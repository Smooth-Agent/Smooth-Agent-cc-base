'use strict';
/**
 * CODEX — `codex app-server` persistente por pista (o mesmo protocolo que o app do codex usa).
 * Antes era `codex exec` (um processo por mensagem): sem turno vivo, não havia como uma mensagem
 * entrar no turno em andamento nem como parar o turno pelo canal do codex.
 *
 * COMPORTAMENTO NATIVO (medido com a sub real, codex 0.163.0, 2026-10-18):
 *  - `turn/start` com um turno ATIVO não abre outro: a mensagem ENTRA no turno que está rodando
 *    (a resposta devolve o MESMO turn id) e ele responde tudo junto. É isso que diz se ela é dona
 *    de um turno novo ou entrou no da outra — sem adivinhar.
 *  - STOP = `turn/interrupt`: o turno fecha `interrupted`; um comando que já estava rodando segue
 *    até acabar (é o codex quem decide) — a box espera ele como qualquer trabalho em background.
 *  - avisos "Reconnecting… n/5" chegam como `error` com willRetry=true: são progresso, não erro.
 *
 * Conversa: uma THREAD do codex por chat, guardada em $CODEX_HOME/smooth-thread.json (no volume).
 * Processo novo numa box que já rodou: `thread/resume` (a conversa volta inteira). Box que acabou
 * de nascer, troca de provedor ou thread sumida: thread nova com o histórico dobrado na 1ª mensagem.
 * O prompt do sistema do agente vai como `developerInstructions` (o `codex exec` NUNCA recebia).
 *
 * Auth — prioridade assinatura > chave: a assinatura é o auth.json do ChatGPT (o codex renova
 * sozinho; o observador captura); a chave vira um auth.json de API key (o app-server ignora a
 * variável de ambiente que o exec usava — provado). MCP: `-c mcp_servers.<k>={url,
 * env_http_headers}` no spawn, o token pelo ambiente (nunca na linha de comando).
 */

const fs = require('node:fs');
const path = require('node:path');
const { Engine, foldedMessage, rawMessage } = require('./engine');
const { CredentialWatch } = require('../lib/credential-watch');
const { TURN_ENV, newTurnMark, spawnEngine } = require('../lib/turn-quiet');
const { reserveMemoryFor, boxState } = require('../lib/lane');
const { nowMs, logInfo, logError, secretHash } = require('../lib/log');

const BACKGROUND_CAP_MS = 10 * 60 * 1000;
const CLIENT_INFO = { name: 'smoothagent', title: 'SmoothAgent', version: '2.0.0' };

/** MCP do envelope ({mcpServers:{nome:{url,headers}}}) → flags -c do codex + env com os tokens. */
function mcpFlags(mcpConfig) {
	const flags = []; const env = {};
	const servers = mcpConfig && mcpConfig.mcpServers;
	if (!servers || typeof servers !== 'object') return { flags, env, count: 0 };
	const bare = (k) => (/^[A-Za-z0-9_-]+$/.test(k) ? k : JSON.stringify(k));
	let n = 0;
	for (const [name, s] of Object.entries(servers)) {
		if (!s || !s.url) continue;
		const key = String(name).replace(/[^A-Za-z0-9_]/g, '_') || `mcp${n}`;
		const headers = s.headers && typeof s.headers === 'object' ? s.headers : {};
		const map = {};
		for (const [h, v] of Object.entries(headers)) {
			if (v == null) continue;
			const ev = `MCP_HDR_${key}_${String(h).replace(/[^A-Za-z0-9_]/g, '_')}`.toUpperCase();
			env[ev] = String(v); map[h] = ev;
		}
		const parts = [`url=${JSON.stringify(s.url)}`];
		if (Object.keys(map).length) parts.push(`env_http_headers={${Object.entries(map).map(([h, ev]) => `${bare(h)}=${JSON.stringify(ev)}`).join(', ')}}`);
		flags.push('-c', `mcp_servers.${key}={${parts.join(', ')}}`);
		n++;
	}
	return { flags, env, count: n };
}

// ── itens do codex → eventos canônicos (stream-json do Claude Code, o que o Worker traduz) ──
const TOOL_ITEMS = new Set(['commandExecution', 'mcpToolCall', 'fileChange', 'webSearch', 'dynamicToolCall', 'imageView']);
const toolName = (it) => {
	switch (it.type) {
		case 'commandExecution': return 'bash';
		case 'mcpToolCall': return it.server && it.tool ? `mcp__${it.server}__${it.tool}` : String(it.tool || it.server || 'mcp_tool');
		case 'fileChange': return 'edit';
		case 'webSearch': return 'web_search';
		case 'imageView': return 'view_image';
		case 'dynamicToolCall': return String(it.tool || 'tool');
		default: return String(it.type || 'tool');
	}
};
const toolInput = (it) => {
	switch (it.type) {
		case 'commandExecution': return { command: String(it.command || '') };
		case 'mcpToolCall': case 'dynamicToolCall': return it.arguments ?? {};
		case 'fileChange': return { changes: (it.changes || []).map((c) => ({ path: c.path, kind: c.kind })) };
		case 'webSearch': return { query: String(it.query || '') };
		case 'imageView': return { path: String(it.path || '') };
		default: return {};
	}
};
const toolFailed = (it) => it.status === 'failed' || it.status === 'declined' || it.error != null || it.success === false || (it.exitCode != null && it.exitCode !== 0);
const toolOutput = (it) => {
	let v;
	if (it.type === 'commandExecution') v = it.aggregatedOutput;
	else if (it.type === 'mcpToolCall') v = it.error ? it.error.message : it.result ? it.result.content : null;
	else if (it.type === 'dynamicToolCall') v = it.contentItems;
	else if (it.type === 'fileChange') v = (it.changes || []).map((c) => `${c.kind} ${c.path}`).join('\n');
	const s = typeof v === 'string' ? v : v == null ? '' : JSON.stringify(v);
	return s.slice(0, 2000);
};

class CodexEngine extends Engine {
	static get kind() { return 'codex'; }

	constructor(lane) {
		super(lane);
		this.proc = null; this.sig = null; this.mark = null;
		this.buf = ''; this.rpcId = 10; this.waiters = new Map();
		this.threadId = null; this.threadReady = false; this.needsHistory = false;
		this.authWritten = null; // hash do auth que gravamos por último (não pisar numa rotação do codex)
		this.inflight = null;    // a mensagem cujo turn/start está no ar
		this.submitChain = Promise.resolve();
		this.early = new Map();  // notificações que chegaram antes da resposta do turn/start
		this.usageTotal = null;
		this.auth = new CredentialWatch({
			label: 'codex', file: () => this.authFile, route: '/internal/codex-credentials',
			payload: (raw) => (raw.trim() ? { authJson: raw } : null),
		});
	}

	get authFile() { return path.join(this.lane.codexHome, 'auth.json'); }
	get threadFile() { return path.join(this.lane.codexHome, 'smooth-thread.json'); }
	alive() { return !!this.proc && this.proc.exitCode === null; }
	pendingCount() { return this.inflight ? 1 : 0; }
	// o app-server sobe auxiliares PERMANENTES dele (ex.: `codex-code-mode`, provado em 2026-10-18 com a
	// sub real: segurava o 1º turno até o teto) — não são trabalho do turno; comandos do agente não se chamam codex*
	processInfo() { return { sid: this.proc ? this.proc.pid : null, mark: this.mark, pid: this.proc ? this.proc.pid : null, ignore: (name) => /^codex/.test(name) }; }

	static signature(e) {
		const authKind = e.codexAuth ? 'sub' : 'key';
		return JSON.stringify({ mcpConfig: e.mcpConfig || null, authKind, key: authKind === 'key' ? secretHash(e.codexApiKey) : null, systemPrompt: e.systemPrompt || null });
	}

	// ── credencial ───────────────────────────────────────────────────────────────────────
	writeAuth(e) {
		fs.mkdirSync(this.lane.codexHome, { recursive: true, mode: 0o700 });
		const raw = e.codexAuth
			? (typeof e.codexAuth === 'string' ? e.codexAuth : JSON.stringify(e.codexAuth))
			: e.codexApiKey ? JSON.stringify({ OPENAI_API_KEY: e.codexApiKey }) : null;
		if (!raw) throw new Error('codexAuth (sub) or codexApiKey required for the codex engine');
		const h = secretHash(raw);
		if (h !== this.authWritten) {
			fs.writeFileSync(this.authFile, raw, { mode: 0o600 });
			this.authWritten = h;
			this.auth.markCurrent();
		}
		if (e.codexAuth) this.auth.arm(e.callback); else this.auth.disarm();
	}

	// ── JSON-RPC ─────────────────────────────────────────────────────────────────────────
	request(method, params, timeoutMs = 60_000) {
		return new Promise((resolve) => {
			if (!this.alive()) return resolve({ error: { message: 'codex app-server not running' } });
			const id = this.rpcId++;
			const timer = setTimeout(() => { this.waiters.delete(id); resolve({ error: { message: `${method} timeout` } }); }, timeoutMs);
			this.waiters.set(id, (o) => { clearTimeout(timer); this.waiters.delete(id); resolve(o); });
			try { this.proc.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n'); }
			catch (err) { clearTimeout(timer); this.waiters.delete(id); resolve({ error: { message: String(err && err.message) } }); }
		});
	}
	notify(method, params) { try { this.proc.stdin.write(JSON.stringify({ jsonrpc: '2.0', method, ...(params ? { params } : {}) }) + '\n'); } catch { /* morto */ } }

	async start(e, emit) {
		const mcp = mcpFlags(e.mcpConfig);
		if (mcp.count) emit('phase', { name: 'codex_mcp', ts: nowMs(), servers: mcp.count });
		this.mark = newTurnMark();
		const proc = spawnEngine('codex', ['app-server', ...mcp.flags], {
			detached: true, cwd: this.lane.cwd,
			env: { ...process.env, CODEX_HOME: this.lane.codexHome, ...mcp.env, [TURN_ENV]: this.mark },
			stdio: ['pipe', 'pipe', 'pipe'],
		});
		this.proc = proc; this.buf = ''; this.threadReady = false;
		this.sig = CodexEngine.signature(e);
		logInfo('codex app-server spawned', { pid: proc.pid, lane: this.lane.key });
		proc.stdout.on('data', (c) => this.onStdout(c));
		const tail = [];
		proc.stderr.on('data', (c) => { tail.push(c); let n = 0; for (const b of tail) n += b.length; while (n > 32768 && tail.length > 1) n -= tail.shift().length; });
		proc.on('exit', (code) => {
			const t = Buffer.concat(tail).toString('utf8').slice(-800);
			logError('codex app-server exited', { code, lane: this.lane.key, stderrTail: t.replace(/\n/g, ' | ') });
			if (this.proc !== proc) return;
			this.proc = null; this.sig = null; this.threadReady = false;
			for (const w of this.waiters.values()) w({ error: { message: `codex exited ${code}` } });
			const live = this.turn;
			if (live) { try { live.emit('error', { code: 'codex_exit', message: `codex exited ${code}${t ? `\nstderr: ${t.slice(-400)}` : ''}`, retryable: false }); } catch { /* fechado */ } this.endTurn(live, null); }
		});
		const init = await this.request('initialize', { clientInfo: CLIENT_INFO, capabilities: null }, 30_000);
		if (init.error) throw new Error(`codex initialize: ${init.error.message}`);
		this.notify('initialized');
	}

	threadParams(e) {
		return { cwd: this.lane.cwd, approvalPolicy: 'never', sandbox: 'danger-full-access', ...(e.systemPrompt ? { developerInstructions: e.systemPrompt } : {}) };
	}
	savedThread() { try { return JSON.parse(fs.readFileSync(this.threadFile, 'utf8')).threadId || null; } catch { return null; } }
	saveThread(id) { try { fs.writeFileSync(this.threadFile, JSON.stringify({ threadId: id }), { mode: 0o600 }); } catch { /* best-effort */ } }

	async openThread(e, emit) {
		const saved = this.savedThread();
		// box que acabou de nascer NÃO retoma: o volume traz a sessão da box anterior e o engine novo
		// voltava vazio (provado 2026-09-19 no exec). Thread nova com o histórico — o Worker manda.
		if (saved && e.freshSession !== true && boxState.runsCompleted > 0) {
			const r = await this.request('thread/resume', { threadId: saved, ...this.threadParams(e) });
			if (!r.error && r.result && r.result.thread) {
				this.threadId = r.result.thread.id; this.threadReady = true; this.needsHistory = false;
				emit('phase', { name: 'codex_thread_resumed', ts: nowMs() });
				return;
			}
			emit('phase', { name: 'codex_thread_resume_failed', ts: nowMs(), err: r.error ? String(r.error.message).slice(0, 120) : 'no thread' });
		}
		if (e.freshSession === true) emit('phase', { name: 'fresh_session', ts: nowMs(), engine: 'codex' });
		const r = await this.request('thread/start', { ...this.threadParams(e), ...(e.model ? { model: e.model } : {}) });
		if (r.error || !r.result || !r.result.thread) throw new Error(`codex thread/start: ${r.error ? r.error.message : 'no thread'}`);
		this.threadId = r.result.thread.id; this.threadReady = true; this.needsHistory = true;
		this.saveThread(this.threadId);
		emit('phase', { name: 'codex_thread_started', ts: nowMs() });
	}

	// ── contrato ─────────────────────────────────────────────────────────────────────────
	async prepare(e, emit) {
		const sig = CodexEngine.signature(e);
		if (this.alive() && (this.sig !== sig || e.forceRespawn === true)) {
			await this.waitIdle(emit, e.backgroundWaitMs || BACKGROUND_CAP_MS, 'respawn');
			emit('phase', { name: 'codex_respawn', ts: nowMs(), reason: this.sig !== sig ? 'args_changed' : 'post_resume' });
			await this.close('respawn', { keepSession: true });
		}
		this.writeAuth(e);
		if (!this.alive()) {
			await reserveMemoryFor(this.lane, emit, e.backgroundWaitMs || BACKGROUND_CAP_MS);
			emit('phase', { name: 'codex_spawn', ts: nowMs(), auth: e.codexAuth ? 'sub' : 'key' });
			await this.start(e, emit);
		}
		if (e.freshSession === true && this.threadReady) {
			await this.waitIdle(emit, e.backgroundWaitMs || BACKGROUND_CAP_MS, 'fresh_session');
			this.threadReady = false;
		}
		if (!this.threadReady) await this.openThread(e, emit);
	}

	submit(message) {
		// transporte em ordem (um turn/start por vez): quem decide o que fazer com cada um é o codex
		this.submitChain = this.submitChain.then(() => this.deliver(message)).catch(() => {});
		return message.attached;
	}

	async deliver(message) {
		const e = message.envelope;
		if (!e.prompt) { message._fail(new Error('prompt required')); return; }
		let text;
		if (this.needsHistory) {
			text = foldedMessage(e);
			if (text !== e.prompt || e.priorContext) message.emit('phase', { name: 'context_refolded', ts: nowMs(), chars: (e.priorContext || '').length });
		} else text = rawMessage(e.prompt);
		this.needsHistory = false;
		this.inflight = message;
		const r = await this.request('turn/start', {
			threadId: this.threadId,
			input: [{ type: 'text', text, text_elements: [] }],
			clientUserMessageId: message.id,
			...(e.model ? { model: e.model } : {}),
			...(e.codexReasoning ? { effort: e.codexReasoning } : {}),
		});
		this.inflight = null;
		if (r.error || !r.result || !r.result.turn) { message._fail(new Error(`codex turn/start: ${r.error ? r.error.message : 'no turn'}`)); return; }
		const id = r.result.turn.id;
		if (this.turn && this.turn.codexTurnId === id) { this.mergeInto(this.turn, message); return; }
		const turn = this.startTurn(message);
		turn.codexTurnId = id; turn.lastText = ''; turn.agentItem = null; turn.streamed = new Set(); turn.tools = new Set();
		turn.usageAtStart = this.usageTotal;
		const early = this.early.get(id) || [];
		this.early.delete(id);
		for (const n of early) this.onNotification(n.method, n.params);
	}

	async interrupt() {
		const live = this.turn;
		if (!this.alive() || !live) return { interrupted: false, promptIds: [], engine: 'codex' };
		const promptIds = live.promptIds();
		const r = await this.request('turn/interrupt', { threadId: this.threadId, turnId: live.codexTurnId }, 15_000);
		return { interrupted: !r.error, promptIds, engine: 'codex', how: r.error ? `error: ${r.error.message}` : 'interrupted' };
	}

	async warmup(e, emit) {
		this.writeAuth(e);
		if (!this.alive()) await this.start(e, emit);
		return { ok: this.alive() };
	}

	async close(reason) {
		const proc = this.proc;
		if (!proc) return;
		logInfo('closing codex', { reason, pid: proc.pid, lane: this.lane.key });
		this.proc = null; this.sig = null; this.threadReady = false; // a thread fica salva: volta com thread/resume
		try { proc.kill('SIGTERM'); } catch { /* já saiu */ }
		await new Promise((res) => {
			const timer = setTimeout(() => { try { proc.kill('SIGKILL'); } catch { /* já saiu */ } res(); }, 3000);
			proc.once('exit', () => { clearTimeout(timer); res(); });
		});
		const live = this.turn;
		if (live) { try { live.emit('error', { code: 'engine_restarted', message: `codex fechado (${reason})`, retryable: true }); } catch { /* fechado */ } this.endTurn(live, null); }
	}

	async flushCredentials() { await this.auth.report(); }

	// ── notificações ────────────────────────────────────────────────────────────────────
	onStdout(chunk) {
		this.lane.lastUsed = nowMs();
		this.buf += chunk.toString('utf8');
		let nl;
		while ((nl = this.buf.indexOf('\n')) >= 0) {
			const line = this.buf.slice(0, nl).trim();
			this.buf = this.buf.slice(nl + 1);
			if (!line) continue;
			let o; try { o = JSON.parse(line); } catch { continue; }
			if (o.id != null && !o.method) { const w = this.waiters.get(o.id); if (w) w(o); continue; }
			if (o.method) this.onNotification(o.method, o.params || {});
		}
	}

	turnOf(turnId) {
		if (this.turn && this.turn.codexTurnId === turnId) return this.turn;
		if (this.lastTurn && this.lastTurn.codexTurnId === turnId && !this.lastTurn.relay.done) return this.lastTurn;
		return null;
	}

	onNotification(method, p) {
		if (method === 'thread/tokenUsage/updated' && p.tokenUsage) { this.usageTotal = p.tokenUsage.total; }
		const turnId = p.turnId || (p.turn && p.turn.id);
		if (!turnId) return;
		const t = this.turnOf(turnId);
		if (!t) {
			// chegou antes da resposta do turn/start que abriu este turno: guarda e entrega depois
			if (this.inflight) { const arr = this.early.get(turnId) || []; arr.push({ method, params: p }); this.early.set(turnId, arr); }
			return;
		}
		switch (method) {
			case 'turn/started':
				t.write({ type: 'system', subtype: 'init' });
				t.emit('phase', { name: 'claude_first_system', ts: nowMs(), engine: 'codex' });
				break;
			case 'item/agentMessage/delta':
				this.text(t, p.itemId, String(p.delta || ''));
				break;
			case 'item/reasoning/summaryTextDelta':
			case 'item/reasoning/textDelta':
				t.streamed.add(p.itemId);
				t.write({ type: 'content_block_delta', delta: { type: 'thinking_delta', thinking: String(p.delta || '') } });
				break;
			case 'item/started':
				if (p.item && TOOL_ITEMS.has(p.item.type)) this.toolCall(t, p.item, false);
				break;
			case 'item/completed': this.itemDone(t, p.item || {}); break;
			case 'error':
				if (p.willRetry) t.emit('phase', { name: 'codex_retrying', ts: nowMs(), message: String((p.error && p.error.message) || '').slice(0, 160) });
				else t.emit('error', { code: 'codex_failed', message: String((p.error && (p.error.additionalDetails || p.error.message)) || 'codex error').slice(0, 500), retryable: false });
				break;
			case 'turn/completed': this.turnDone(t, p.turn || {}); break;
			default: break;
		}
	}

	text(t, itemId, delta) {
		if (!delta) return;
		if (t.agentItem !== itemId) {
			// mensagem nova do agente no mesmo turno: separa da anterior
			if (t.agentItem) t.write({ type: 'content_block_delta', delta: { type: 'text_delta', text: '\n\n' } });
			t.agentItem = itemId;
		}
		t.streamed.add(itemId);
		t.write({ type: 'content_block_delta', delta: { type: 'text_delta', text: delta } });
	}

	toolCall(t, it, force) {
		if (t.tools.has(it.id)) return;
		const input = toolInput(it);
		const hasInput = Object.values(input).some((v) => v != null && v !== '' && !(Array.isArray(v) && !v.length) && !(typeof v === 'object' && !Array.isArray(v) && !Object.keys(v).length));
		if (!hasInput && !force) return; // alguns itens só trazem os argumentos no completed
		t.tools.add(it.id);
		t.write({ type: 'content_block_start', content_block: { type: 'tool_use', id: it.id, name: toolName(it), input } });
	}

	itemDone(t, it) {
		if (it.type === 'agentMessage') {
			if (!t.streamed.has(it.id)) this.text(t, it.id, String(it.text || ''));
			t.lastText = String(it.text || '');
		} else if (it.type === 'reasoning') {
			if (!t.streamed.has(it.id)) {
				const s = [...(it.summary || []), ...(it.content || [])].join('\n');
				if (s) t.write({ type: 'content_block_delta', delta: { type: 'thinking_delta', thinking: s } });
			}
		} else if (TOOL_ITEMS.has(it.type)) {
			this.toolCall(t, it, true); // garante o par chamada+resultado
			t.write({ type: 'tool_result', id: it.id, name: toolName(it), is_error: toolFailed(it), content: toolOutput(it) });
			if (it.type === 'fileChange') t.emit('phase', { name: 'file_changed', ts: nowMs(), tool: 'edit', error: toolFailed(it) });
		} else if (it.type !== 'userMessage' && it.type !== 'plan' && it.type !== 'contextCompaction') {
			t.emit('phase', { name: 'codex_item_unmapped', ts: nowMs(), item_type: String(it.type || '?'), keys: Object.keys(it).slice(0, 15) });
		}
	}

	async turnDone(t, turn) {
		const status = turn.status || 'completed';
		const total = this.usageTotal; const start = t.usageAtStart;
		const usage = {
			input_tokens: total ? Math.max(0, total.inputTokens - ((start && start.inputTokens) || 0)) : 0,
			output_tokens: total ? Math.max(0, total.outputTokens - ((start && start.outputTokens) || 0)) : 0,
		};
		const failed = status === 'failed';
		const text = failed && turn.error ? String(turn.error.message || 'codex error') : t.lastText;
		// rotação do codex ANTES de fechar o turno (o refresh token é de uso único)
		try { await this.flushCredentials(); } catch { /* nunca derruba o turno */ }
		t.write({ type: 'result', subtype: status === 'completed' ? 'success' : status === 'interrupted' ? 'error_during_execution' : 'error', is_error: status !== 'completed', result: text, ...(this.threadId ? { session_id: this.threadId } : {}), usage });
		t.emit('phase', { name: 'codex_turn_completed', ts: nowMs(), status });
		this.endTurn(t, { text, usage, isError: status !== 'completed', subtype: status === 'completed' ? 'success' : status });
	}
}

module.exports = { CodexEngine, mcpFlags };
