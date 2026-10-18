'use strict';
/**
 * CLAUDE CODE — processo persistente em stream-json (um por pista), reaproveitado entre turnos.
 *
 * COMPORTAMENTO NATIVO (medido com o modelo real, 2026-10-18 — não é regra nossa):
 *  - mensagem escrita no stdin com o claude trabalhando: ele lê no fim da ferramenta em andamento
 *    e ela ENTRA NO MESMO TURNO (uma resposta só); escrita enquanto ele só escreve texto, vira o
 *    PRÓXIMO turno, que ele começa sozinho; várias juntas podem virar um turno só.
 *  - `--replay-user-messages`: o claude ECOA cada mensagem (com o uuid que mandamos, isReplay)
 *    no instante em que a lê — é isso que diz, sem adivinhar, de qual turno ela é.
 *  - STOP = control_request `interrupt`: o turno fecha (result error_during_execution); as
 *    mensagens que esperavam CONTINUAM e rodam em seguida.
 *  - continuação: depois do result o claude pode acordar sozinho (task-notification) e seguir
 *    trabalhando — essa saída é do turno anterior, que espera ela acabar.
 *
 * Identidade do processo (respawn): systemPrompt, maxTurns, effort, MCP, forma de auth, chave.
 * Modelo NÃO é identidade: troca no lugar (control_request set_model, o /model do interativo).
 * Token novo da assinatura também não: reescreve .credentials.json no lugar.
 * Reiniciar com trabalho em andamento não dá — aí (e só aí) a mensagem espera o claude ficar
 * ocioso. ROTAÇÃO por memória fecha o processo e a próxima mensagem volta com --resume.
 */

const fs = require('node:fs');
const path = require('node:path');
const { Engine, foldedMessage, rawMessage } = require('./engine');
const { CredentialWatch } = require('../lib/credential-watch');
const { TURN_ENV, newTurnMark, spawnEngine } = require('../lib/turn-quiet');
const { reserveMemoryFor } = require('../lib/lane');
const { nowMs, logInfo, logError, secretHash, sleep } = require('../lib/log');

const BACKGROUND_CAP_MS = 10 * 60 * 1000;
const BUSY_TYPES = new Set(['assistant', 'stream_event', 'user']);

class ClaudeEngine extends Engine {
	static get kind() { return 'claude'; }

	constructor(lane) {
		super(lane);
		this.proc = null;
		this.sig = null; this.model = null; this.authKind = null; this.secret = null;
		this.mark = null;
		this.lineBuf = '';
		/** mensagens entregues no stdin que o claude ainda não leu (ordem de entrega) */
		this.pending = [];
		/** o turno anterior acordou sozinho e voltou a trabalhar (continuação) */
		this.continuing = null;
		this.working = false; this.lastOut = 0;
		this.bgTasks = 0; this.bgTaskNames = [];
		this.sessionId = null; this.rotated = false;
		/** o processo acabou de nascer SEM a sessão do chat: a próxima mensagem leva o histórico */
		this.needsHistory = false;
		/** voltou com --resume: a sessão já tem tudo, nada de histórico dobrado */
		this.resumed = false;
		this.waiters = new Map();
		this.creds = new CredentialWatch({
			label: 'claude',
			file: () => this.credFile,
			route: '/internal/cc-credentials',
			payload: (raw) => {
				const c = JSON.parse(raw).claudeAiOauth || {};
				return c.accessToken ? { credentials: { accessToken: c.accessToken, refreshToken: c.refreshToken || null, expiresAt: c.expiresAt || null } } : null;
			},
		});
	}

	get credFile() { return path.join(this.lane.claudeDir, '.credentials.json'); }
	alive() { return !!this.proc && this.proc.exitCode === null; }
	pendingCount() { return this.pending.length; }
	busy() { return !!this.turn || !!this.continuing || this.pending.length > 0; }
	/** Mensagem entregue, claude vivo e parado sem ler nada há 30s: algo quebrou (server.js desiste em 60s). */
	stalled() { return this.pending.length > 0 && !this.turn && !this.continuing && !this.working && this.alive() && nowMs() - this.lastOut > 30_000; }
	processInfo() { return { sid: this.proc ? this.proc.pid : null, mark: this.mark, pid: this.proc ? this.proc.pid : null }; }
	workInEngine(turn) {
		// continuação do claude (acordou com task-notification) e tarefas em background que ele diz ter
		const continuing = this.continuing === turn || (this.lastTurn === turn && !this.turn && (this.working || nowMs() - this.lastOut < 1500));
		const tasks = turn.until == null ? this.bgTasks : 0;
		return tasks + (continuing ? 1 : 0);
	}

	// ── identidade do processo ───────────────────────────────────────────────────────────
	static signature(e) {
		return JSON.stringify({ systemPrompt: e.systemPrompt || null, maxTurns: e.maxTurns || null, ccEffort: e.ccEffort || null, mcpConfig: e.mcpConfig || null });
	}
	static args(e, resumeId) {
		const args = ['--output-format', 'stream-json', '--input-format', 'stream-json', '--verbose', '--include-partial-messages',
			'--replay-user-messages', '--permission-mode', 'bypassPermissions'];
		if (resumeId) args.push('--resume', resumeId);
		if (e.model) args.push('--model', e.model);
		if (e.ccEffort) args.push('--effort', String(e.ccEffort)); // flag de sessão: está na assinatura
		if (e.systemPrompt) args.push('--system-prompt', e.systemPrompt);
		if (e.maxTurns) args.push('--max-turns', String(e.maxTurns));
		if (e.mcpConfig) {
			const f = `/tmp/mcp-${process.pid}-${Date.now()}.json`;
			fs.writeFileSync(f, JSON.stringify(e.mcpConfig), { mode: 0o600 });
			args.push('--mcp-config', f);
		}
		return args;
	}

	// ── credencial ───────────────────────────────────────────────────────────────────────
	/** Só os lockfiles de sessão saem (caches e credencial ficam: é o que mantém o claude quente). */
	wipeStaleSessions() {
		const projects = path.join(this.lane.claudeDir, 'projects');
		try {
			if (!fs.existsSync(projects)) return;
			for (const p of fs.readdirSync(projects)) {
				const s = path.join(projects, p, 'sessions');
				if (fs.existsSync(s)) { try { fs.rmSync(s, { recursive: true, force: true }); } catch (e) { logError('wipe sessions failed', { msg: e && e.message }); } }
			}
		} catch (e) { logError('wipe .claude sessions failed', { msg: e && e.message }); }
	}
	/** O PAR completo da assinatura (o claude precisa do refresh pra renovar sozinho — doutrina). */
	writeOauth(e) {
		fs.mkdirSync(this.lane.claudeDir, { recursive: true, mode: 0o700 });
		const exp = e.ccExpiresAt ? (typeof e.ccExpiresAt === 'number' ? e.ccExpiresAt : Date.parse(e.ccExpiresAt)) : null;
		fs.writeFileSync(this.credFile, JSON.stringify({ claudeAiOauth: {
			accessToken: e.ccToken, ...(e.ccRefreshToken ? { refreshToken: e.ccRefreshToken } : {}), ...(exp ? { expiresAt: exp } : {}), scopes: ['user:inference'],
		} }), { mode: 0o600 });
		this.creds.markCurrent();
	}
	/** Chave de API: vai no env do spawn, nunca no disco; sem renovação, sem observador. */
	dropOauth() {
		try { fs.rmSync(this.credFile, { force: true }); } catch { /* best-effort */ }
		this.creds.disarm();
	}
	sessionOnDisk() {
		if (!this.sessionId) return false;
		const root = path.join(this.lane.claudeDir, 'projects');
		try { for (const p of fs.readdirSync(root)) if (fs.existsSync(path.join(root, p, `${this.sessionId}.jsonl`))) return true; } catch { /* sem projects */ }
		return false;
	}

	// ── processo ─────────────────────────────────────────────────────────────────────────
	spawn(e, resumeId) {
		this.mark = newTurnMark();
		const proc = spawnEngine('claude', ClaudeEngine.args(e, resumeId), {
			detached: true, // setsid: o turno é a sessão do engine (lib/turn-quiet)
			cwd: this.lane.cwd,
			env: { ...process.env, ...(this.lane.claudeConfigDir ? { CLAUDE_CONFIG_DIR: this.lane.claudeConfigDir } : {}), ...(e.ccApiKey ? { ANTHROPIC_API_KEY: e.ccApiKey } : {}), [TURN_ENV]: this.mark },
			stdio: ['pipe', 'pipe', 'pipe'],
		});
		this.proc = proc;
		this.sig = ClaudeEngine.signature(e);
		this.model = e.model || null;
		this.authKind = e.ccApiKey ? 'apiKey' : 'oauth';
		this.secret = secretHash(e.ccApiKey || e.ccToken);
		this.lineBuf = '';
		this.needsHistory = !resumeId;
		this.resumed = !!resumeId;
		logInfo('claude spawned', { pid: proc.pid, lane: this.lane.key, resumed: !!resumeId });
		proc.stdout.on('data', (c) => this.onStdout(c));
		const tail = [];
		proc.stderr.on('data', (c) => { tail.push(c); let n = 0; for (const b of tail) n += b.length; while (n > 65536 && tail.length > 1) n -= tail.shift().length; });
		proc.on('exit', (code) => {
			const t = Buffer.concat(tail).toString('utf8').slice(-1024);
			logError('claude exited', { code, lane: this.lane.key, stderrTail: t.replace(/\n/g, ' | ') });
			if (this.proc !== proc) return; // fechado de propósito (close): quem fechou trata
			this.proc = null; this.sig = null;
			this.failEverything({ code: 'exit_nonzero', message: `claude exited ${code}${t ? `\nstderr (tail): ${t.slice(-512)}` : ''}`, retryable: false });
		});
		return proc;
	}

	/** O processo morreu: o turno fecha com erro e as mensagens que ele ainda não leu falham. */
	failEverything(err) {
		const live = this.turn || this.continuing;
		this.continuing = null; this.working = false; this.bgTasks = 0; this.bgTaskNames = [];
		if (live) { try { live.emit('error', err); } catch { /* sink fechado */ } this.endTurn(live, null); }
		for (const m of this.pending.splice(0)) m._fail(Object.assign(new Error(err.message), { code: err.code }));
	}

	async close(reason, opts = {}) {
		const proc = this.proc;
		if (!proc) return;
		logInfo('closing claude', { reason, pid: proc.pid, lane: this.lane.key });
		this.proc = null; this.sig = null; this.model = null; this.authKind = null; this.secret = null;
		if (opts.keepSession) this.rotated = true; // a sessão fica no disco: volta com --resume
		try { proc.kill('SIGTERM'); } catch { /* já saiu */ }
		await new Promise((res) => {
			const timer = setTimeout(() => { try { proc.kill('SIGKILL'); } catch { /* já saiu */ } res(); }, 3000);
			proc.once('exit', () => { clearTimeout(timer); res(); });
		});
		if (this.busy()) this.failEverything(opts.failWith || { code: 'engine_restarted', message: `claude fechado (${reason})`, retryable: true });
	}

	/** control_request no canal stream-json. Resolve a resposta ou null (timeout / processo morto). */
	control(request, timeoutMs) {
		return new Promise((resolve) => {
			if (!this.alive()) return resolve(null);
			const id = `ctl_${Date.now()}_${Math.floor(Math.random() * 1e6)}`;
			const timer = setTimeout(() => { this.waiters.delete(id); resolve(null); }, timeoutMs);
			this.waiters.set(id, (resp) => { clearTimeout(timer); this.waiters.delete(id); resolve(resp); });
			try { this.proc.stdin.write(JSON.stringify({ type: 'control_request', request_id: id, request }) + '\n'); }
			catch { clearTimeout(timer); this.waiters.delete(id); resolve(null); }
		});
	}
	async setModel(model, timeoutMs) {
		const r = await this.control({ subtype: 'set_model', model }, timeoutMs);
		const sub = r && r.response ? r.response.subtype : undefined;
		return !!r && (sub === 'success' || (!r.error && sub !== 'error'));
	}

	// ── contrato ─────────────────────────────────────────────────────────────────────────
	async prepare(e, emit) {
		if (!e.ccToken && !e.ccApiKey) throw new Error('ccToken or ccApiKey required for the claude engine');
		const sig = ClaudeEngine.signature(e);
		const model = e.model || null;
		const authKind = e.ccApiKey ? 'apiKey' : 'oauth';
		const secret = secretHash(e.ccApiKey || e.ccToken);
		const restartWanted = e.forceRespawn === true || e.freshSession === true;

		// só o modelo mudou: troca no lugar (nativo), sem perder nada
		if (this.alive() && this.sig === sig && this.model !== model && !restartWanted) {
			if (await this.setModel(model, 2000)) { this.model = model; emit('phase', { name: 'claude_model_swapped', ts: nowMs(), model }); }
			else emit('phase', { name: 'claude_model_swap_refused', ts: nowMs(), model });
		}
		const respawn = !this.alive() || this.sig !== sig || this.model !== model || this.authKind !== authKind
			|| (authKind === 'apiKey' && this.secret !== secret) || restartWanted;

		if (!respawn) {
			if (authKind === 'oauth') {
				if (this.secret !== secret) { this.writeOauth(e); this.secret = secret; emit('phase', { name: 'claude_token_swapped', ts: nowMs() }); }
				this.creds.arm(e.callback);
			}
			emit('phase', { name: 'claude_reused', ts: nowMs() });
			return;
		}

		if (this.alive()) {
			// não dá pra trocar a identidade do processo no meio de um turno: espera ele ficar ocioso
			await this.waitIdle(emit, e.backgroundWaitMs || BACKGROUND_CAP_MS, 'respawn');
			const reason = e.freshSession === true ? 'provider_switch' : e.forceRespawn === true ? 'post_resume'
				: this.sig !== sig ? 'args_changed' : this.authKind !== authKind ? 'auth_changed' : this.model !== model ? 'model_swap_failed' : 'token_rotated';
			emit('phase', { name: 'claude_respawn', ts: nowMs(), reason });
			await this.close('respawn');
		}
		let resumeId = null;
		if (this.rotated && this.sessionOnDisk() && e.freshSession !== true) resumeId = this.sessionId;
		this.rotated = false;
		if (e.freshSession === true) { this.sessionId = null; emit('phase', { name: 'fresh_session', ts: nowMs(), engine: 'claude' }); }
		await reserveMemoryFor(this.lane, emit, e.backgroundWaitMs || BACKGROUND_CAP_MS);
		this.wipeStaleSessions();
		if (authKind === 'apiKey') this.dropOauth();
		else { this.writeOauth(e); this.creds.arm(e.callback); }
		const t0 = nowMs();
		emit('phase', { name: 'pre_claude_spawn', ts: t0, ...(resumeId ? { resumed: true } : {}) });
		this.spawn(e, resumeId);
		emit('phase', { name: 'claude_spawned', ts: nowMs(), since_spawn_call_ms: nowMs() - t0 });
	}

	async submit(message) {
		const e = message.envelope;
		if (!e.prompt) throw new Error('prompt required');
		// sessão nova sem a conversa: o histórico vai dobrado nesta mensagem (uma vez)
		let content;
		if (this.resumed) content = rawMessage(e.prompt);
		else if (this.needsHistory) {
			content = foldedMessage(e);
			if (content !== e.prompt) message.emit('phase', { name: 'context_refolded', ts: nowMs(), chars: (e.priorContext || '').length });
		} else content = e.prompt;
		this.needsHistory = false; this.resumed = false;
		if (this.busy()) message.emit('queued', { engine: 'claude' }); // o claude decide: entra no turno ou vem depois
		this.pending.push(message);
		try {
			this.proc.stdin.write(JSON.stringify({ type: 'user', uuid: message.id, message: { role: 'user', content } }) + '\n');
		} catch (err) {
			this.pending = this.pending.filter((m) => m !== message);
			message._fail(new Error(`stdin write failed: ${err && err.message}`));
		}
		return message.attached;
	}

	async interrupt() {
		const live = this.turn || this.continuing;
		if (!this.alive() || !live) return { interrupted: false, promptIds: [], engine: 'claude' };
		const promptIds = live.promptIds();
		live.stopped = true; // o result que vier agora é do STOP, não erro do provedor
		const ack = await this.control({ subtype: 'interrupt' }, 3000);
		const t0 = nowMs();
		while ((this.turn === live || this.continuing === live) && nowMs() - t0 < 3000) await sleep(100);
		let how = 'interrupted';
		if (!ack || this.turn === live || this.continuing === live) {
			// não respondeu: mata, e a próxima mensagem volta na MESMA sessão (--resume)
			await this.close('stop', { keepSession: true, failWith: { code: 'cancelled', message: 'turno parado pelo STOP', retryable: false } });
			how = 'killed';
		}
		this.continuing = null;
		return { interrupted: true, promptIds, engine: 'claude', how };
	}

	async warmup(e, emit) {
		if (!e.ccToken && !e.ccApiKey) throw new Error('ccToken or ccApiKey required for warmupOnly');
		if (!this.alive() || this.sig !== ClaudeEngine.signature(e)) {
			if (this.alive()) await this.close('warmup_respawn');
			this.wipeStaleSessions();
			if (e.ccApiKey) this.dropOauth(); else { this.writeOauth(e); this.creds.arm(e.callback); }
			emit('phase', { name: 'pre_claude_spawn', ts: nowMs() });
			this.spawn(e, null);
		}
		// prova de vida sem inferência: set_model pro mesmo modelo (no-op) tem que voltar ack
		const ok = await this.setModel(e.model || this.model || 'claude-opus-4-8', 15_000);
		return { ok };
	}

	async flushCredentials() { await this.creds.report(); }

	// ── stdout: de qual turno é cada linha ──────────────────────────────────────────────
	onStdout(chunk) {
		this.lastOut = nowMs();
		this.lane.lastUsed = this.lastOut;
		this.lineBuf += chunk.toString('utf8');
		let nl;
		while ((nl = this.lineBuf.indexOf('\n')) >= 0) {
			const line = this.lineBuf.slice(0, nl).trim();
			this.lineBuf = this.lineBuf.slice(nl + 1);
			if (line) this.onLine(line);
		}
	}

	onLine(line) {
		let o = null;
		try { o = JSON.parse(line); } catch { /* linha não-JSON: vai pro turno vivo, se houver */ }
		const type = o && o.type;
		if (type === 'control_response') {
			const rid = (o.response && o.response.request_id) || o.request_id;
			const w = rid && this.waiters.get(rid);
			if (w) w(o);
			return;
		}
		if (type === 'system' && o.subtype === 'background_tasks_changed' && Array.isArray(o.tasks)) {
			this.bgTasks = o.tasks.length;
			this.bgTaskNames = o.tasks.slice(0, 8).map((x) => String(x.description || x.task_type || x.task_id || '?'));
		}
		if (o && (type === 'system' || type === 'result') && typeof o.session_id === 'string' && o.session_id) this.sessionId = o.session_id;
		// ECO da mensagem: o claude acabou de LER ela — é isso que diz de qual turno ela é
		if (type === 'user' && o.isReplay === true) { this.onEcho(o); return; }

		let target = this.turn || this.continuing;
		const startsWork = (type === 'system' && o.subtype === 'init') || BUSY_TYPES.has(type);
		if (!target && startsWork) {
			if (this.pending.length) target = this.startTurn(this.pending.shift()); // o claude lê em ordem
			else if (this.lastTurn && !this.lastTurn.relay.done) { this.continuing = this.lastTurn; target = this.continuing; }
		}
		if (BUSY_TYPES.has(type)) this.working = true;
		if (!target) return;

		if (target === this.turn) this.turnPhases(target, type);
		// turno parado pelo STOP: o claude devolve o texto parcial marcado como erro — pro Worker e pro
		// app isso é PARADO (o que já saiu no stream fica como resposta), não falha do provedor
		if (type === 'result' && target.stopped && !target.done) {
			o.subtype = 'error_during_execution'; o.is_error = true; o.result = '';
			line = JSON.stringify(o);
		}
		target.relay.write(line + '\n');

		if (type === 'result') {
			this.working = false;
			const res = {
				text: typeof o.result === 'string' ? o.result : '',
				usage: o.usage || null,
				isError: o.is_error === true,
				subtype: typeof o.subtype === 'string' ? o.subtype : undefined,
			};
			if (target === this.turn) this.endTurn(target, res);
			else if (target === this.continuing) {
				// result da CONTINUAÇÃO: soma na resposta do mesmo turno
				const pr = target.relay.pendingResult || { text: '', usage: null };
				pr.text = [pr.text, res.text].filter(Boolean).join('\n\n');
				if (res.usage) pr.usage = { input_tokens: ((pr.usage && pr.usage.input_tokens) || 0) + (res.usage.input_tokens || 0), output_tokens: ((pr.usage && pr.usage.output_tokens) || 0) + (res.usage.output_tokens || 0) };
				target.relay.pendingResult = pr;
				target.emit('phase', { name: 'claude_continuation_result', ts: nowMs(), chars: res.text.length });
				this.continuing = null;
			}
		}
	}

	onEcho(o) {
		const i = this.pending.findIndex((m) => m.id === o.uuid);
		if (i < 0) return; // eco da dona do turno (já atribuída) ou de outra coisa
		const msg = this.pending.splice(i, 1)[0];
		const live = this.turn || this.continuing;
		if (live) this.mergeInto(live, msg);
		else this.startTurn(msg);
	}

	turnPhases(turn, type) {
		turn.phases = turn.phases || {};
		const P = turn.phases;
		const t = nowMs();
		if (!P.stdout) { P.stdout = true; turn.emit('phase', { name: 'claude_first_stdout', ts: t }); }
		if (!P.system && type === 'system') { P.system = true; turn.emit('phase', { name: 'claude_first_system', ts: t }); }
		if (!P.assistant && (type === 'assistant' || type === 'stream_event')) { P.assistant = true; turn.emit('phase', { name: 'claude_first_assistant', ts: t }); }
	}
}

module.exports = { ClaudeEngine };
