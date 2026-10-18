#!/usr/bin/env node
/**
 * SmoothAgent runtime HTTP server (port 8080) — the box-side adapter.
 *
 * Runs PERSISTENT inside a Detona microVM (snapshot/resume physics: everything
 * in process globals gets frozen into golden forks — see the SNAPSHOT TRAP notes
 * inline). One /run per turn (concurrent /run → 409), streamed back chunked;
 * the box survives across turns (Detona pauses it, resume restores byte-intact).
 *
 * MODES (envelope.mode):
 *   cc-cli — an AGENT ENGINE runs the turn:
 *     engine 'claude' (default): persistent claude process, --input-format
 *       stream-json; reused across turns when the core signature matches
 *       (respawn on args_changed/auth_changed/model_swap_failed/dead).
 *     engine 'codex' (2026-07-18): OpenAI Codex CLI, ONE-SHOT per turn
 *       (codex exec --json); runCodexTurn translates its JSONL into claude
 *       stream-json so everything downstream is engine-agnostic.
 *   slot   — the CLIENT's persistent server (SLOT_CONTRACT.md).
 *   build/exec — plain command runs (image prep, layer authoring).
 *
 * AUTH (never logged, never echoed):
 *   claude sub  — ccToken/ccRefreshToken/ccExpiresAt → .credentials.json (0600);
 *     claude is the ONLY rotator (token doctrine, core/V2_TOKEN_REFRESH.md);
 *     the credWatcher reports rotations back to the Worker.
 *   claude key  — ccApiKey → ANTHROPIC_API_KEY at spawn (static, no watcher).
 *   codex key   — codexApiKey → CODEX_API_KEY at spawn (static, no watcher).
 *
 * Streaming: each stdout chunk is forwarded to the wire immediately via the
 * TurnRelay (buffers for F5 replay, fans out to live sinks, fires the save
 * callback on completion).
 */

const http = require('node:http');
const { spawn } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { TurnRelay } = require('./relay');

const PORT = 8080;
const HOST = '0.0.0.0';

// FILA NO ADAPTER (2026-09-26): a box ACEITA mensagem com turno rodando — antes era 409
// 'already_running', a fila morava no Worker (teto de 175s) e, passado 3 min, o Worker
// despachava direto, levava 409 e o retry DESTRUIA a box com o turno no meio. Agora cada
// /run entra na fila do ENGINE: a mensagem 2 comeca assim que o engine termina a 1 (o
// result do claude / o exit do codex) — nao espera trabalho em background, sem teto. E o
// comportamento nativo dos dois: o Claude Code processa a mensagem seguinte depois do
// result; o codex roda a proxima exec na mesma sessao.
let openRuns = 0;                     // /run abertos (inclui os esperando a vez) — /health inFlight
let runsCompleted = 0; // 0 = este processo nunca fechou um turno = box COLD (nasceu da base agora)

// ─── PISTAS (BOX COMPARTILHADA, core/BOX_SPEC.md, 2026-10-17) ────────────────────
// Toda box e compartilhavel; a box de um chat e a box com UMA pista. Uma PISTA e o estado de
// engine de um chat: o processo do claude dele, o turno ativo, a continuacao, a fila (a vez do
// engine), o cwd e a pasta de credencial (claude e codex). Box propria (envelope sem
// sharedBox) = a pista '_own' com os caminhos de SEMPRE — /workspace, ~/.claude,
// /workspace/.codex — identica ao adapter de um chat so. Box compartilhada = uma pista por
// chat: cwd /workspace/chats/<chat>, CLAUDE_CONFIG_DIR e CODEX_HOME proprios em
// /workspace/.lanes/<chat> (as contas de cada agente nao se misturam e cada uma renova o
// proprio token), e dois chats rodam ao mesmo tempo.
const OWN_LANE = '_own';
const lanes = new Map(); // key -> Lane
class Lane {
	constructor(key, shared) {
		this.key = key;
		this.shared = shared;
		const base = shared ? path.join('/workspace/.lanes', key) : null;
		this.cwd = shared ? path.join('/workspace/chats', key) : (process.env.RUN_CWD || '/workspace');
		this.claudeConfigDir = shared ? path.join(base, 'claude') : null; // null = ~/.claude (box propria)
		this.codexHome = shared ? path.join(base, 'codex') : (process.env.CODEX_HOME || '/workspace/.codex');
		// processo do claude
		this.proc = null; this.sig = null; this.model = null; this.tokenHash = null; this.authKind = null;
		this.activeTurn = null; this.lingerTurn = null; this.busy = false; this.lastOut = 0; this.lineBuf = '';
		this.bgTasks = 0; this.bgTaskNames = []; this.waiters = new Map();
		this.sid = null; this.mark = null;
		// sessao do Claude Code (pra voltar com --resume depois de uma rotacao por memoria)
		this.sessionId = null; this.rotated = false;
		// a vez do engine DESTA pista
		this.chain = Promise.resolve(); this.engineBusy = false; this.lastTurnCtx = null; this.openRuns = 0;
		this.lastUsed = Date.now();
		// watchers de credencial (por pista: cada uma tem a propria conta)
		this.cred = { callback: null, started: false, last: '', report: null };
		this.cx = { callback: null, started: false, last: '', report: null };
		// STOP do usuario (dono, 2026-10-17: a box so encerra no idle — o STOP para o TURNO do chat,
		// nunca a box). stopAt = quando o ultimo STOP chegou; /run que chegou antes e ainda esperava
		// a vez e descartado, e a espera por background do turno parado acaba na hora.
		this.stopAt = 0; this.cxProc = null; this.lastChatId = null;
	}
	get claudeDir() { return this.claudeConfigDir || path.join(process.env.HOME || '/workspace', '.claude'); }
	get credFile() { return path.join(this.claudeDir, '.credentials.json'); }
	get codexAuthFile() { return path.join(this.codexHome, 'auth.json'); }
}
/** A pista do envelope: chat de box compartilhada → a dele; senao a pista unica da box. */
function laneFor(envelope) {
	const chat = envelope && (envelope.chatId || (envelope.workspaceProxy && envelope.workspaceProxy.chatId));
	const shared = !!(envelope && envelope.sharedBox === true && typeof chat === 'string' && /^[A-Za-z0-9_-]{1,64}$/.test(chat));
	const key = shared ? chat : OWN_LANE;
	let l = lanes.get(key);
	if (!l) {
		l = new Lane(key, shared);
		if (shared) for (const d of [l.cwd, l.claudeConfigDir, l.codexHome]) { try { fs.mkdirSync(d, { recursive: true, mode: 0o700 }); } catch { /* spawn falha com mensagem clara */ } }
		lanes.set(key, l);
	}
	return l;
}
const ownLane = () => laneFor(null);
/** The current turn's relay (retain/send/save). GET /stream attaches to it. */
let currentRelay = null;
/** Relays dos turnos ABERTOS, em ordem de chegada (com a fila no adapter pode haver mais de
 *  um: o que esta rodando + os que esperam a vez), e os ultimos fechados por promptId. */
const openRelays = [];
const doneRelays = new Map(); // promptId -> relay (so os 8 mais recentes)
/** Relay pro reattach: o do promptId pedido; sem promptId, o turno mais ANTIGO ainda aberto
 *  (= o que esta rodando), nao o ultimo que chegou (que pode estar so esperando a vez). */
// chatId: so turnos DAQUELE chat (box compartilhada: o reattach de um chat nunca pode cair no
// turno de outro). Sem chatId = quem chama e o Worker (chave da box), que manda promptId.
function relayFor(promptId, chatId) {
	const mine = (r) => !chatId || r.chatId === chatId;
	if (promptId) { const r = openRelays.find((x) => x.promptId === promptId) || doneRelays.get(promptId) || null; return r && mine(r) ? r : null; }
	return openRelays.find(mine) || (currentRelay && mine(currentRelay) ? currentRelay : null);
}

// ---------------------------------------------------------------------------
// PERSISTENT CLAUDE PROCESS — the heart of the long-running optimization.
//
// First /run: spawn claude with --input-format stream-json. Boots Node,
// loads @anthropic-ai/claude-code, does OAuth handshake with Anthropic.
// ~20-25s cold cost paid ONCE per machine lifetime.
//
// Subsequent /run on the SAME machine (same containerKey → pool reuse):
// just write a new user message to claude's stdin. No spawn, no Node load,
// no OAuth handshake. ~2-3s warm cost.
//
// When the args that materially affect Claude's behavior change between
// turns (model swap, system-prompt change, MCP config change), the prior
// process is killed and a new one spawned. Cold cost again on that turn, but
// rare. A rotated ccToken does NOT respawn — it's a credential, not a behavior
// change, so it's swapped in place (.credentials.json rewritten, process kept).
// ---------------------------------------------------------------------------

// ESTADO DO CLAUDE: vive na PISTA (Lane acima) — lane.proc (o processo), lane.sig (assinatura
// systemPrompt+maxTurns+mcpConfig com que subiu), lane.model (modelo atual), lane.tokenHash
// (ccToken gravado; token novo = troca no lugar, nao respawn), lane.authKind ('oauth' =
// .credentials.json que se renova sozinho, com watcher | 'apiKey' = ANTHROPIC_API_KEY no env
// do spawn, estatica, sem watcher, trocar a chave exige respawn), lane.activeTurn (o turno
// do /run em andamento) e lane.lineBuf (linhas NDJSON parciais do stdout).
// CONTINUACAO DO CLAUDE DEPOIS DO RESULT (2026-10-03). O Claude Code encerra a resposta
// ("estou esperando o CI") com tarefas em background e, quando elas terminam
// (task-notification), ACORDA SOZINHO e continua o trabalho — chamando MCP, sem processo
// filho. Antes: (1) a box fechava o turno quando as tarefas zeravam, o Worker pausava e o
// claude congelava NO MEIO da continuacao (chat_795256c4, 05:49); (2) tudo que o claude
// escrevia depois do result era DESCARTADO (o "2o result"). Agora o turno fica "lingering"
// (lane.lingerTurn) ate o claude ficar ocioso (lane.busy false e lane.lastOut ha 1.5s): a
// continuacao vai pro relay do mesmo turno, o result dela entra na resposta, e a proxima
// mensagem da pista espera o claude ocioso.
/** Time of last /run completion (for the idle-exit watchdog). */
let lastRunAt = Date.now(); // (so informativo desde que o idle watchdog saiu)
// ---------------------------------------------------------------------------
// R2 workspace sync subsystem REMOVED 2026-07-07 (was ~220 dead lines behind
// `if (false && proxy)` — see git history). rclone left the image with it.

function nowMs() {
	return Date.now();
}

// ─── TURN = ATE O TRABALHO ACABAR (nao ate o CLI sair) ───────────────────────
// O stream do /run e o que segura a box acordada: quando ele fecha, o Worker
// pausa. Antes, ele fechava quando o CLI dizia "result" — e um `npm run build &`,
// um subagente, um curl esperando resposta ficavam vivos e CONGELAVAM no pause
// (caso real: chat_add755f9e469dd26, 2026-09-14 04:46). Quem sabe se o trabalho
// acabou e a BOX, olhando a arvore de processos do turno:
//   - o engine sobe com `detached:true` (= setsid): ele e todo descendente dele
//     ficam na MESMA sessao, mesmo se o pai morrer e o filho for reparentado;
//   - so contam processos que NASCERAM depois do inicio do turno (starttime do
//     /proc), pra um helper persistente do claude nao segurar todo turno;
//   - teto configuravel (envelope.backgroundWaitMs, default 10 min): estourou,
//     a box avisa `background_left_running` e fecha — o Worker entao NAO pausa e
//     deixa o idlePauseSeconds decidir.
// Nenhum sandbox do mercado faz isto (Fly = conexoes do proxy, E2B = gap entre
// comandos, Daytona = API): todos cegam pra trabalho em background.
// CGROUP, nao sessao. A 1a versao (sha-f185b70) usava a SESSAO do engine — e o codex
// roda a tool de shell com setsid proprio: o `sleep 40 &` do turno ficou fora da sessao,
// o stream fechou, a box pausou e o sleep CONGELOU (provado em prod 2026-09-19, chat
// chat_9b18d1fa94981237: sleep vivo com sid=641 != sid do codex). Sessao e furavel por
// qualquer setsid/nohup — do engine ou do user. Cgroup nao: filho herda o cgroup do pai
// SEMPRE, e reparenting nao muda. O engine entra em /sys/fs/cgroup/turn no spawn e todo
// descendente (bash, subagente, curl, build) nasce la dentro. Provado no kernel do
// Detona: `setsid nohup sleep 30 &` + pai sai -> segue em cgroup.procs, populated=1.
// A sessao fica como FALLBACK se o cgroup nao estiver disponivel.
// MARCADOR DE AMBIENTE (o criterio primario). O server.js roda como UID 996 e o Detona
// sobe o node direto, sem fase root: nao da pra criar/delegar cgroup de dentro (mkdir em
// /sys/fs/cgroup falha — medido, turnCgOk=false em prod). O que NAO precisa de privilegio
// e sobrevive a setsid/nohup/reparenting e o AMBIENTE herdado: todo descendente do engine
// carrega SMOOTH_TURN_ID=<nonce>; /proc/<pid>/environ e legivel porque tudo e o mesmo UID.
// So `env -i` explicito fura — e ai a sessao e o cgroup (se um dia delegado) sao o fallback.
// TRABALHO DENTRO DO PROCESSO DO ENGINE. Subagente do Claude Code em background nao e um
// processo filho ate chamar uma tool: e uma promise dentro do claude, invisivel no /proc.
// Provado em prod (chat_6ffcebd7c4d470d4, 09:09): result em 7s com "aguardando subagente",
// zero processos, stream fechou, box pausou em 6s, o sleep do subagente congelou — o caso
// original do tester. O sinal certo e do proprio Claude Code: `system/background_tasks_changed`
// traz a LISTA de tarefas vivas (agentes e bash em background) e chega ANTES do result;
// `[]` quando tudo acabou. Parse ACIMA do gate que descarta linhas pos-result.
// (lane.bgTasks / lane.bgTaskNames: tarefas em background que o Claude Code diz ter vivas)
const TURN_ENV = 'SMOOTH_TURN_ID';
// lane.mark = nonce do engine da pista (claude: por spawn; codex: por turno)
const newTurnMark = () => `t${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
function procHasTurnMark(pid, mark) {
	if (!mark) return false;
	try { return fs.readFileSync(`/proc/${pid}/environ`).toString('latin1').split('\0').includes(`${TURN_ENV}=${mark}`); } catch { return false; }
}
const TURN_CG = '/sys/fs/cgroup/turn';
let turnCgOk = false;
try { fs.mkdirSync(TURN_CG, { recursive: true }); fs.accessSync(TURN_CG + '/cgroup.procs', fs.constants.W_OK); turnCgOk = true; }
catch (e) { logError('turn cgroup indisponivel — cai pra sessao', { msg: e && e.message }); }
/**
 * Spawn do engine JA DENTRO do cgroup do turno. Mover o pid depois do spawn perdia a
 * corrida: o sh forkava o filho antes do write em cgroup.procs (medido: sleep em 0::/).
 * O wrapper entra no cgroup ANTES do exec, no mesmo pid — o engine nasce la dentro e
 * todo descendente herda. Sem cgroup, spawn normal (fallback por sessao).
 */
function spawnInTurnCgroup(cmd, args, opts) {
	if (!turnCgOk) return spawn(cmd, args, opts);
	return spawn('/bin/sh', ['-c', `echo $$ > ${TURN_CG}/cgroup.procs 2>/dev/null; exec "$0" "$@"`, cmd, ...args], opts);
}
// lane.sid = sessao (= pid) do engine da pista; claude persiste, codex e por turno
function uptimeTicks() {
	try { return Math.floor(parseFloat(fs.readFileSync('/proc/uptime', 'utf8').split(' ')[0]) * 100); } catch { return 0; }
}
/** Processos vivos (nao-zumbis) na sessao `sid`, nascidos apos `sinceTicks`, exceto `exceptPid`. */
function liveTurnProcs(sid, sinceTicks, exceptPid, mark, untilTicks) {
	const out = [];
	// candidatos = quem esta no cgroup do turno (a verdade) U quem esta na sessao (fallback)
	const cands = new Set();
	_cgSet = new Set();
	if (turnCgOk) { try { for (const l of fs.readFileSync(TURN_CG + '/cgroup.procs', 'utf8').split('\n')) { if (/^\d+$/.test(l)) { cands.add(Number(l)); _cgSet.add(Number(l)); } } } catch { /* fallback abaixo */ } }
	let dirs = []; try { dirs = fs.readdirSync('/proc'); } catch { /* sem /proc: so o cgroup */ }
	for (const d of dirs) if (/^\d+$/.test(d)) cands.add(Number(d));
	for (const pid of cands) {
		const d = String(pid);
		if (pid === exceptPid || pid === process.pid) continue;
		let stat; try { stat = fs.readFileSync(`/proc/${d}/stat`, 'utf8'); } catch { continue; }
		const close = stat.lastIndexOf(')');
		const name = stat.slice(stat.indexOf('(') + 1, close);
		const f = stat.slice(close + 2).split(' '); // f[0]=state f[3]=session f[19]=starttime
		if (f[0] === 'Z') continue;
		// esta no turno se: esta no cgroup do turno, OU (fallback) esta na sessao do engine.
		// O cgroup e UM so pra box: com mais de uma pista ele nao diz de qual chat o processo e —
		// ai so contam a sessao e o marcador de ambiente (que sao por pista).
		const inCg = turnCgOk && lanes.size <= 1 && cgHas(pid);
		if (!inCg && Number(f[3]) !== sid && !procHasTurnMark(pid, mark)) continue;
		// 2 ticks (20ms) de folga: starttime e /proc/uptime podem divergir em 1 tick na borda —
		// contar um processo que nasceu 20ms antes do turno e inofensivo; deixar de contar o do turno nao.
		if (Number(f[19]) < sinceTicks - 2) continue;
		// nasceu depois que o PROXIMO turno comecou = e trabalho do proximo, nao deste
		if (untilTicks != null && Number(f[19]) >= untilTicks) continue;
		out.push({ pid, name });
	}
	return out;
}
let _cgSet = new Set();
function cgHas(pid) { return _cgSet.has(pid); }
/** Segura o turno enquanto houver processo do turno vivo. Devolve o que sobrou no teto. */
async function waitTurnQuiet(emit, maxMs, ctx) {
	if (!ctx || !ctx.sid) return { waitedMs: 0, left: [] };
	const t0 = nowMs(); let lastPhase = 0; let seen = false;
	for (;;) {
		const live = liveTurnProcs(ctx.sid, ctx.since, ctx.sid, ctx.mark, ctx.until);
		// tarefas em background do Claude Code sao da SESSAO: quando o proximo turno ja comecou,
		// elas seguem com ele — este turno nao espera por elas.
		// A continuacao do claude (acordou com task-notification) e DESTE turno enquanto ele for
		// o lingering — mesmo que a proxima mensagem ja tenha chegado (ela espera o claude ocioso).
		const L = ctx.lane;
		// STOP: o usuario parou este turno — nada mais a esperar (interruptLane ja matou o que sobrou)
		if (L.stopAt >= (ctx.startedAt || 0)) {
			emit('phase', { name: 'background_stopped', ts: nowMs(), waited_ms: nowMs() - t0 });
			return { waitedMs: nowMs() - t0, left: [] };
		}
		const mine = ctx.engine === 'claude' && L.lingerTurn && L.lingerTurn.relay === ctx.relay;
		const claudeWorking = mine && (L.busy || nowMs() - L.lastOut < 1500);
		const bg = (ctx.until == null ? L.bgTasks : 0) + (claudeWorking ? 1 : 0);
		const waitedMs = nowMs() - t0;
		if (!live.length && bg === 0) {
			if (seen) emit('phase', { name: 'background_done', ts: nowMs(), waited_ms: waitedMs });
			return { waitedMs, left: [] };
		}
		seen = true;
		if (waitedMs >= maxMs) {
			emit('phase', { name: 'background_left_running', ts: nowMs(), waited_ms: waitedMs, procs: live.length, names: live.slice(0, 8).map((p) => p.name), tasks: bg, taskNames: L.bgTaskNames });
			logError('turn left background work running (cap)', { waitedMs, procs: live.length, names: live.slice(0, 8).map((p) => `${p.pid}:${p.name}`) });
			return { waitedMs, left: live };
		}
		if (nowMs() - lastPhase >= 5000) {
			lastPhase = nowMs();
			emit('phase', { name: 'background_pending', ts: nowMs(), waited_ms: waitedMs, procs: live.length, names: live.slice(0, 8).map((p) => p.name), tasks: bg, taskNames: L.bgTaskNames });
		}
		await new Promise((r) => setTimeout(r, 1000));
	}
}

function logInfo(msg, extra = {}) {
	// Stderr so it goes to Fly logs without polluting stdout (which is the SSE stream).
	process.stderr.write(JSON.stringify({ ts: nowMs(), level: 'info', msg, ...extra }) + '\n');
}

function logError(msg, extra = {}) {
	process.stderr.write(JSON.stringify({ ts: nowMs(), level: 'error', msg, ...extra }) + '\n');
}

/**
 * Remove stale Claude Code session state on the persistent volume.
 *
 * /workspace is mounted from a Fly Volume that persists across turns. If a
 * previous turn's claude process died without releasing its session lockfile
 * (machine destroyed mid-run, server crash, etc), the next turn would hit
 * "Session ID X is already in use". We nuke the sessions/ subdir on every
 * turn — claude rebuilds it on demand, conversation continuity comes from
 * the system prompt context bridge in core (chats-api.ts).
 *
 * We keep /workspace/.claude/CLAUDE.md and other non-session config because
 * it's customer-authored project rules, not state.
 */
function wipeClaudeState(lane) {
	// HOME for the `agent` user is /workspace (Dockerfile useradd --home-dir),
	// so ~/.claude resolves into the persistent Fly Volume / tmpfs. (Pista de box
	// compartilhada: o CLAUDE_CONFIG_DIR dela — lane.claudeDir.)
	//
	// PREVIOUSLY: this wiped the entire .claude dir. That fixed "Session ID X
	// is already in use" but ALSO blew away anything Claude caches between
	// runs (MCP server caches, OAuth session caches inside Anthropic SDK,
	// model/tokenizer caches), forcing a full cold OAuth handshake every turn.
	//
	// NOW: we wipe ONLY the session lockfiles. caches and credentials survive.
	// configureCcAuth still overwrites .credentials.json right after, so any
	// stale OAuth token gets replaced — but the SDK-level session cache is
	// preserved, which lets the second turn skip part of the Anthropic
	// handshake.
	const projectsDir = path.join(lane.claudeDir, 'projects');
	try {
		if (!fs.existsSync(projectsDir)) return;
		// Walk projects/* and remove only sessions/ subdirs. Keeps project
		// indices, MCP caches, agent settings.
		for (const proj of fs.readdirSync(projectsDir)) {
			const sessionsDir = path.join(projectsDir, proj, 'sessions');
			if (fs.existsSync(sessionsDir)) {
				try { fs.rmSync(sessionsDir, { recursive: true, force: true }); }
				catch (e) { logError('failed to wipe sessions dir', { proj, msg: e && e.message }); }
			}
		}
	} catch (err) {
		logError('failed to wipe .claude sessions', { msg: err && err.message });
	}
}

/** Configure Claude Code OAuth credentials. Call AFTER wipeClaudeState. */
/**
 * Write the FULL OAuth pair. The doctrine (owner-defined 2026-07-05): the ONLY
 * thing that ever refreshes a CC token is CLAUDE ITSELF, in here, naturally —
 * the platform never calls Anthropic's refresh endpoint. That only works if
 * claude HAS the refresh token (this file used to write access-only, which made
 * self-refresh impossible and silently forced the platform into worker-side
 * rotation). expiresAt lets claude know when to rotate.
 */
function configureCcAuth(lane, token, refreshToken, expiresAt) {
	fs.mkdirSync(lane.claudeDir, { recursive: true, mode: 0o700 });
	fs.writeFileSync(
		lane.credFile,
		JSON.stringify({
			claudeAiOauth: {
				accessToken: token,
				...(refreshToken ? { refreshToken } : {}),
				...(expiresAt ? { expiresAt: typeof expiresAt === 'number' ? expiresAt : Date.parse(expiresAt) } : {}),
				scopes: ['user:inference'],
			},
		}),
		{ mode: 0o600 },
	);
	lane.cred.last = credFileFingerprint(lane);
}

/** Cheap change detector for the credentials file (never logs contents). */
function credFileFingerprint(lane) {
	try {
		const raw = fs.readFileSync(lane.credFile, 'utf8');
		return crypto.createHash('sha256').update(raw).digest('hex');
	} catch { return ''; }
}

/**
 * CREDENTIAL WATCHER — the capture half of the doctrine. claude rotates the
 * token by rewriting .credentials.json; the moment that happens we report the
 * new pair to the Worker (dedicated endpoint, signed callback JWT) so the
 * SOURCE OF TRUTH is updated immediately — not at turn end, not never. The
 * Worker also re-forks the golden with the new key, in parallel, on its side.
 * fs.watch + a post-turn sweep (belt and suspenders; fs.watch can miss on
 * overlayfs). Token contents never logged.
 *
 * SNAPSHOT TRAP (2026-07-10, the "Not logged in" death): credWatcherStarted
 * and the report closure are process state — a golden fork restores them
 * FROZEN with the bake-time callback jwt. Every report from a forked box then
 * 401'd at the Worker (silently) and claude's rotation EVAPORATED from the
 * source while the old refresh token was already burned → the next chat
 * cloned a dead pair. Fix: the callback is REBOUND on every /run into
 * credWatcherCallback (module-level), and the report always reads the CURRENT
 * turn's jwt from there. Never capture callback params in this closure.
 */
// (lane.cred.callback = o callback do turno ATUAL da pista; lane.cred.last = fingerprint ja reportado)
function startCredWatcher(lane, callback) {
	const W = lane.cred;
	if (callback && callback.url) W.callback = callback; // rebind EVERY turn — see snapshot trap above
	if (W.started || !W.callback) return;
	W.started = true;
	const report = async () => {
		try {
			const cb = W.callback;
			if (!cb || !cb.url) return;
			const fp = credFileFingerprint(lane);
			if (!fp || fp === W.last) return;
			const cred = JSON.parse(fs.readFileSync(lane.credFile, 'utf8')).claudeAiOauth || {};
			if (!cred.accessToken) return;
			W.last = fp;
			const url = cb.url.replace(/\/internal\/chat-result$/, '/internal/cc-credentials');
			const r = await fetch(url, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json', ...(cb.jwt ? { Authorization: `Bearer ${cb.jwt}` } : {}) },
				body: JSON.stringify({ credentials: { accessToken: cred.accessToken, refreshToken: cred.refreshToken || null, expiresAt: cred.expiresAt || null } }),
			});
			// A failed report must NOT be marked as reported — the post-turn sweep
			// (next turn, fresh jwt) retries it. Losing a rotation kills the pair.
			if (!r.ok) W.last = null;
			logInfo('cred rotation reported', { status: r.status, lane: lane.key });
		} catch (e) {
			W.last = null; // retry on the next sweep
			logError('cred report failed', { msg: e && e.message });
		}
	};
	try {
		fs.watch(lane.claudeDir, { persistent: false }, (_evt, fname) => {
			if (fname === '.credentials.json') setTimeout(report, 150); // debounce partial writes
		});
	} catch (e) { logError('cred watcher failed to start', { msg: e && e.message }); }
	W.report = report;
}

/**
 * API-key mode: static key, nothing EVER rotates — unbind the callback so a
 * stray fs event (or the post-turn sweep) can never POST a phantom "rotation".
 * The report closure re-reads credWatcherCallback at call time, so nulling it
 * here disarms reporting; the next oauth turn's startCredWatcher rebinds it.
 */
function stopCredWatcher(lane) {
	lane.cred.callback = null;
}

// ─── CODEX SUB AUTH WATCHER (2026-07-26) ────────────────────────────────────
// Mirror of the CC credWatcher for the codex subscription. The codex CLI
// self-rotates $CODEX_HOME/auth.json in place (it IS the only rotator, same
// doctrine); when it does, we mirror the new auth.json back to the Worker so
// the stored sub never drifts stale. Reports to /internal/codex-credentials.
// (por pista: lane.cx = { callback, started, last, report } — cada pista tem o proprio CODEX_HOME)
function codexAuthFingerprint(lane) {
	try { const s = fs.statSync(lane.codexAuthFile); return `${s.size}:${s.mtimeMs}`; } catch { return ''; }
}
function startCodexAuthWatcher(lane, callback) {
	const W = lane.cx;
	if (callback && callback.url) W.callback = callback; // rebind every turn (snapshot trap)
	if (W.started || !W.callback) return;
	W.started = true;
	const report = async () => {
		try {
			const cb = W.callback;
			if (!cb || !cb.url) return;
			const fp = codexAuthFingerprint(lane);
			if (!fp || fp === W.last) return;
			const raw = fs.readFileSync(lane.codexAuthFile, 'utf8');
			if (!raw.trim()) return;
			W.last = fp;
			const url = cb.url.replace(/\/internal\/chat-result$/, '/internal/codex-credentials');
			const r = await fetch(url, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json', ...(cb.jwt ? { Authorization: `Bearer ${cb.jwt}` } : {}) },
				body: JSON.stringify({ authJson: raw }), // the full auth.json (never logged)
			});
			if (!r.ok) W.last = ''; // failed → retry next sweep
			logInfo('codex auth rotation reported', { status: r.status, lane: lane.key });
		} catch (e) { W.last = ''; logError('codex auth report failed', { msg: e && e.message }); }
	};
	try {
		fs.watch(lane.codexHome, { persistent: false }, (_evt, fname) => {
			if (fname === 'auth.json') setTimeout(report, 150);
		});
	} catch (e) { logError('codex auth watcher failed to start', { msg: e && e.message }); }
	W.report = report;
}

/**
 * Build claude argv for LONG-RUNNING mode (stream-json on stdin).
 *
 * Critical differences from one-shot mode:
 *   - --input-format stream-json: accept user messages on stdin as NDJSON.
 *     This is the ONLY way to feed a single claude process multiple prompts
 *     without re-paying the 20s OAuth handshake on every turn.
 *   - NO prompt in argv (it comes via stdin).
 *   - NO --session-id: stream-json mode manages the session lifecycle within
 *     a single process; passing a stale id makes claude refuse with
 *     "Session already in use".
 *   - --max-turns kept (it's an upper bound per user turn, not per session).
 */
function buildClaudeArgs(envelope, resumeId) {
	const args = [
		'--output-format', 'stream-json',
		'--input-format', 'stream-json',
		'--verbose',
		'--include-partial-messages',
		'--permission-mode', 'bypassPermissions',
	];
	// ROTACAO (box compartilhada): o claude desta pista foi fechado por falta de memoria e
	// volta na MESMA sessao do Claude Code (o arquivo dela mora no volume). Provado em metal
	// 2026-10-14: processo novo com --resume em stream-json retoma o mesmo session_id.
	if (resumeId) args.push('--resume', resumeId);
	if (envelope.model) args.push('--model', envelope.model);
	// `--effort` e flag de SESSAO: entra no coreSignature, senao trocar de nivel
	// reusaria o processo antigo e o user nao veria diferenca nenhuma.
	if (envelope.ccEffort) args.push('--effort', String(envelope.ccEffort));
	if (envelope.systemPrompt) args.push('--system-prompt', envelope.systemPrompt);
	if (envelope.maxTurns) args.push('--max-turns', String(envelope.maxTurns));
	if (envelope.mcpConfig) {
		const tmpFile = `/tmp/mcp-${Date.now()}.json`;
		fs.writeFileSync(tmpFile, JSON.stringify(envelope.mcpConfig));
		args.push('--mcp-config', tmpFile);
	}
	return args;
}

// (sessionSignature — the model-inclusive variant — was removed 2026-07-07: it was
// orphaned once the in-flight set_model switch made coreSignature the only reuse
// identity. One source of truth for "what is process identity": coreSignature.)

/**
 * Signature WITHOUT the model. The model is a per-REQUEST parameter to the
 * Anthropic API, not process identity — same CC, same context, any model
 * (interactive claude has /model for exactly this). When ONLY the model differs
 * between turns we attempt an IN-FLIGHT switch via a stream-json control_request
 * (subtype set_model) instead of killing the warm process; kill+respawn remains
 * the fallback when the CLI doesn't ack. systemPrompt/maxTurns/mcp still respawn
 * (those ARE process identity: spawn args that rebuild the session).
 */
function coreSignature(envelope) {
	return JSON.stringify({
		// engine is part of process identity (defensive: a claude proc must never
		// be "reused" by a codex turn or vice-versa — runCodexTurn also kills).
		engine: envelope.engine || 'claude',
		systemPrompt: envelope.systemPrompt || null,
		maxTurns: envelope.maxTurns || null,
		ccEffort: envelope.ccEffort || null,
		mcpConfig: envelope.mcpConfig || null,
	});
}

// (lane.waiters: control_request acks pendentes, request_id → resolve. Ver trySetModel.)

/**
 * Ask the running claude to switch model in-flight (stream-json control channel).
 * Resolves true on an acked success, false on error/timeout — caller falls back
 * to the legacy kill+respawn, so an older CLI that ignores the subtype only costs
 * `timeoutMs` once per model switch, never correctness.
 */
function trySetModel(lane, model, timeoutMs) {
	return new Promise((resolve) => {
		if (!lane.proc || lane.proc.exitCode !== null) return resolve(false);
		const id = `sm_${Date.now()}_${Math.floor(Math.random() * 1e6)}`;
		const timer = setTimeout(() => { lane.waiters.delete(id); resolve(false); }, timeoutMs);
		lane.waiters.set(id, (resp) => {
			clearTimeout(timer);
			lane.waiters.delete(id);
			const sub = resp && resp.response ? resp.response.subtype : undefined;
			resolve(sub === 'success' || (!!resp && !resp.error && sub !== 'error'));
		});
		try {
			lane.proc.stdin.write(JSON.stringify({ type: 'control_request', request_id: id, request: { subtype: 'set_model', model } }) + '\n');
		} catch {
			clearTimeout(timer);
			lane.waiters.delete(id);
			resolve(false);
		}
	});
}

/**
 * Cheap fingerprint of the ccToken — we never store the token raw in any
 * variable that could end up in stderr/error chains; just a short hash so
 * we can detect rotation between turns.
 */
function tokenHash(token) {
	if (!token) return null;
	let h = 0x811c9dc5;
	for (let i = 0; i < token.length; i++) {
		h ^= token.charCodeAt(i);
		h = Math.imul(h, 0x01000193);
	}
	return (h >>> 0).toString(16);
}

/** Send SIGTERM to the running claude and wait for it to exit. */
async function killClaude(lane, reason) {
	if (!lane.proc) return;
	logInfo('killing persistent claude', { reason, pid: lane.proc.pid, lane: lane.key });
	const proc = lane.proc;
	lane.proc = null;
	lane.sig = null;
	lane.model = null;
	lane.tokenHash = null;
	lane.authKind = null;
	lane.lineBuf = '';
	try { proc.kill('SIGTERM'); } catch { /* already dead */ }
	await new Promise((resolve) => {
		const timer = setTimeout(() => { try { proc.kill('SIGKILL'); } catch {} resolve(); }, 3000);
		proc.once('exit', () => { clearTimeout(timer); resolve(); });
	});
}

/**
 * Spawn the persistent claude process. Caller must have already wiped and
 * written /workspace/.claude/.credentials.json with the user's OAuth token.
 *
 * Returns the proc handle; also wires stdout to the streaming pipeline.
 */
function spawnPersistentClaude(lane, envelope, resumeId) {
	const args = buildClaudeArgs(envelope, resumeId);
	lane.mark = newTurnMark();
	const proc = spawnInTurnCgroup('claude', args, {
		detached: true, // setsid: o turno e a SESSAO (ver waitTurnQuiet)
		cwd: lane.cwd,
		// API-key auth: the key rides the SPAWN env (claude reads ANTHROPIC_API_KEY
		// natively) — never written to disk, never in .credentials.json.
		// Pista de box compartilhada: CLAUDE_CONFIG_DIR proprio (credencial + sessoes do chat).
		env: { ...process.env, ...(lane.claudeConfigDir ? { CLAUDE_CONFIG_DIR: lane.claudeConfigDir } : {}), ...(envelope.ccApiKey ? { ANTHROPIC_API_KEY: envelope.ccApiKey } : {}), [TURN_ENV]: lane.mark },
		stdio: ['pipe', 'pipe', 'pipe'],
	});
	lane.sid = proc.pid;
	logInfo('persistent claude spawned', { pid: proc.pid, args: args.length, turnCgroup: turnCgOk, lane: lane.key, resumed: !!resumeId });

	proc.stdout.on('data', (chunk) => handleClaudeStdout(lane, chunk));

	const stderrTail = [];
	proc.stderr.on('data', (chunk) => {
		// Cap to last 64KB so we can surface useful context on crash without
		// growing unbounded.
		stderrTail.push(chunk);
		let total = 0;
		for (const b of stderrTail) total += b.length;
		while (total > 65536 && stderrTail.length > 1) {
			const removed = stderrTail.shift();
			total -= removed.length;
		}
	});

	proc.on('exit', (code) => {
		const tail = Buffer.concat(stderrTail).toString('utf8').slice(-1024);
		logError('persistent claude exited', { code, stderrTail: tail.replace(/\n/g, ' | ') });
		// If a turn was waiting on this process, surface the failure. (Uma rotacao por memoria
		// so fecha pista OCIOSA — sem activeTurn — entao nunca cai aqui.)
		if (lane.activeTurn && lane.proc === proc) {
			const t = lane.activeTurn;
			try {
				t.emit('error', {
					code: 'exit_nonzero',
					message: `claude exited ${code}${tail ? `\nstderr (tail): ${tail.slice(-512)}` : ''}`,
					retryable: false,
				});
				t.resolve();
			} catch {}
			lane.activeTurn = null;
		}
		// Clear lane state — next /run will respawn fresh.
		if (lane.proc === proc) {
			lane.proc = null;
			lane.sig = null;
			lane.model = null;
			lane.tokenHash = null;
			lane.authKind = null;
			lane.lineBuf = '';
			lane.bgTasks = 0; lane.bgTaskNames = []; lane.busy = false;
		}
	});

	lane.proc = proc;
	lane.sig = coreSignature(envelope);
	lane.model = envelope.model || null;
	lane.authKind = envelope.ccApiKey ? 'apiKey' : 'oauth';
	lane.tokenHash = tokenHash(envelope.ccApiKey || envelope.ccToken);
	return proc;
}

/**
 * Handle a chunk of stdout from the persistent claude. Forwards raw bytes to
 * the active /run response AND parses NDJSON line-by-line to detect:
 *   - first stdout (CLI booted)
 *   - first 'system' event (CLI ready for inference)
 *   - first 'assistant' event (model emitting output)
 *   - 'result' event (current turn finished — release the request)
 *
 * Phase markers are emitted as 'phase' events so the worker telemetry can
 * record them in /timing/cc.
 */
function handleClaudeStdout(lane, chunk) {
	lane.lastOut = nowMs();
	lane.lastUsed = lane.lastOut;
	// control_response acks (e.g. set_model) can arrive BETWEEN turns — resolve
	// waiters before the activeTurn gate or they'd be dropped and time out.
	if (lane.waiters.size > 0) {
		for (const line of chunk.toString('utf8').split('\n')) {
			const s = line.trim();
			if (!s || s.indexOf('control_response') === -1) continue;
			try {
				const obj = JSON.parse(s);
				const rid = (obj.response && obj.response.request_id) || obj.request_id;
				const w = rid && lane.waiters.get(rid);
				if (w) w(obj);
			} catch { /* partial line — the waiter's timeout covers it */ }
		}
	}
	// background_tasks_changed: pode chegar DEPOIS do result (turno ja resolvido) — e e
	// exatamente ai que importa. Le antes do gate.
	if (chunk.indexOf('background_tasks_changed') !== -1) {
		for (const line of chunk.toString('utf8').split('\n')) {
			if (line.indexOf('background_tasks_changed') === -1) continue;
			try {
				const obj = JSON.parse(line.trim());
				if (obj.type === 'system' && obj.subtype === 'background_tasks_changed' && Array.isArray(obj.tasks)) {
					lane.bgTasks = obj.tasks.length;
					lane.bgTaskNames = obj.tasks.slice(0, 8).map((x) => String(x.description || x.task_type || x.task_id || '?'));
				}
			} catch { /* linha parcial */ }
		}
	}
	// Sem turno nenhum o parse SEGUE rodando: o estado do claude (ocupado/ocioso) tem que ser
	// lido sempre, senao o result de uma continuacao sem dono deixa lane.busy preso em true.
	const t = lane.activeTurn || lane.lingerTurn;

	if (t) {
		// Phase: first stdout in this turn
		if (!t.firstStdoutSeen) {
			t.firstStdoutSeen = true;
			t.emit('phase', { name: 'claude_first_stdout', ts: nowMs(), since_spawn_call_ms: nowMs() - t.t_spawn });
		}
		// Forward raw chunk through the relay (buffers for F5 replay + fans out to
		// every live sink — preserves frame alignment for the translator).
		t.relay.write(chunk);
	}

	// Parse line-by-line for terminal/phase detection.
	lane.lineBuf += chunk.toString('utf8');
	let nl;
	while ((nl = lane.lineBuf.indexOf('\n')) >= 0) {
		const line = lane.lineBuf.slice(0, nl).trim();
		lane.lineBuf = lane.lineBuf.slice(nl + 1);
		if (!line) continue;
		let obj;
		try { obj = JSON.parse(line); } catch { continue; }
		if (!obj || typeof obj !== 'object') continue;
		const type = obj.type;
		// sessao do Claude Code desta pista (system/init e result trazem) — e por ela que a pista
		// volta com --resume depois de uma rotacao por memoria
		// (so do claude: o codex passa por aqui via feed() com o thread_id DELE no session_id)
		if (lane.proc && (type === 'system' || type === 'result') && typeof obj.session_id === 'string' && obj.session_id) lane.sessionId = obj.session_id;

		if (t && !t.firstSystemSeen && type === 'system') {
			t.firstSystemSeen = true;
			t.emit('phase', { name: 'claude_first_system', ts: nowMs(), since_spawn_call_ms: nowMs() - t.t_spawn });
		}
		if (t && !t.firstAssistantSeen && (type === 'assistant' || type === 'text' || type === 'content_block_start')) {
			t.firstAssistantSeen = true;
			t.emit('phase', { name: 'claude_first_assistant', ts: nowMs(), since_spawn_call_ms: nowMs() - t.t_spawn });
		}
		if (type === 'assistant' || type === 'stream_event' || type === 'user') lane.busy = true;
		if (type === 'result') {
			lane.busy = false;
			if (lane.activeTurn) {
				// Claude finished. Capture the final text + usage for the SAVE
				// callback, but don't complete the relay yet — sync_up + post_claude
				// events still stream after this. complete() fires at end of /run.
				const turn = lane.activeTurn;
				lane.activeTurn = null;
				turn.relay.pendingResult = {
					text: typeof obj.result === 'string' ? obj.result : '',
					usage: obj.usage || null,
					// o turno TERMINOU EM ERRO (chave inválida, limite, API fora)? O texto aqui é a
					// mensagem de erro, não fala do agente — o Worker salva como erro.
					isError: obj.is_error === true,
					subtype: typeof obj.subtype === 'string' ? obj.subtype : undefined,
				};
				lane.lingerTurn = turn;
				turn.resolve();
			} else if (lane.lingerTurn) {
				// result da CONTINUACAO: soma na resposta do mesmo turno
				const lt = lane.lingerTurn;
				const pr = lt.relay.pendingResult || { text: '', usage: null };
				const more = typeof obj.result === 'string' ? obj.result : '';
				pr.text = [pr.text, more].filter(Boolean).join('\n\n');
				if (obj.usage) pr.usage = { input_tokens: ((pr.usage && pr.usage.input_tokens) || 0) + (obj.usage.input_tokens || 0), output_tokens: ((pr.usage && pr.usage.output_tokens) || 0) + (obj.usage.output_tokens || 0) };
				lt.relay.pendingResult = pr;
				lt.emit('phase', { name: 'claude_continuation_result', ts: nowMs(), chars: more.length });
			}
		}
	}
}

// ROTACAO POR MEMORIA (BOX_SPEC, 2026-10-17). So existe com mais de uma pista (box compartilhada):
// antes de subir um claude, a box confere a RAM livre; faltando, fecha o claude da pista OCIOSA
// usada ha mais tempo (LRU) — ela volta depois pelo --resume da sessao dela. Sem ociosa pra
// fechar, o turno ESPERA a vez (com ping no stream), nunca arrisca matar a box por falta de
// memoria. Box de um chat: nada disso roda (a pausa ja libera a RAM, como sempre).
const LANE_SPAWN_MB = Number(process.env.SMOOTH_LANE_SPAWN_MB) || 300;
const MEMINFO_PATH = process.env.SMOOTH_MEMINFO_PATH || '/proc/meminfo'; // trocavel so pra teste
function memAvailableMb() {
	try {
		const m = fs.readFileSync(MEMINFO_PATH, 'utf8').match(/^MemAvailable:\s+(\d+)\s+kB/m);
		return m ? Math.floor(Number(m[1]) / 1024) : Infinity;
	} catch { return Infinity; }
}
function laneIdle(l) { return !!l.proc && !l.activeTurn && !l.lingerTurn && !l.busy && l.openRuns === 0 && !l.engineBusy; }
async function reserveMemoryFor(lane, emit, maxWaitMs) {
	if (lanes.size <= 1) return;
	const t0 = nowMs(); let lastPhase = 0;
	for (;;) {
		const avail = memAvailableMb();
		if (avail >= LANE_SPAWN_MB) return;
		const victim = [...lanes.values()].filter((l) => l !== lane && laneIdle(l)).sort((a, b) => a.lastUsed - b.lastUsed)[0];
		if (victim) {
			emit('phase', { name: 'lane_rotated', ts: nowMs(), lane: victim.key, avail_mb: avail });
			victim.rotated = true;
			await killClaude(victim, 'memory_rotation');
			// respiro antes de medir de novo: a RAM do processo fechado nao aparece livre no mesmo
			// instante — medir na hora fechava uma pista A MAIS do que o necessario (provado no teste)
			await new Promise((r) => setTimeout(r, 700));
			continue;
		}
		if (nowMs() - t0 >= maxWaitMs) { emit('phase', { name: 'memory_wait_gave_up', ts: nowMs(), avail_mb: avail }); return; }
		if (nowMs() - lastPhase >= 5000) { lastPhase = nowMs(); emit('phase', { name: 'waiting_memory', ts: nowMs(), avail_mb: avail, need_mb: LANE_SPAWN_MB }); }
		await new Promise((r) => setTimeout(r, 1000));
	}
}
/**
 * STOP de UM chat (dono, 2026-10-17: "o stop so deve acontecer agora sempre no idle"). Antes o
 * STOP destruia a box inteira; agora para so o turno da pista e o trabalho que ele deixou, e a box
 * segue a regra de sempre (pausa quando nada mais roda). claude: interrupt nativo (control_request);
 * sem resposta em 3s → mata o processo e a proxima mensagem volta na MESMA sessao (--resume).
 * codex: mata o exec do turno. Background do turno (sessao/marcador da pista): SIGTERM.
 */
function sendClaudeInterrupt(lane, timeoutMs) {
	return new Promise((resolve) => {
		if (!lane.proc || lane.proc.exitCode !== null) return resolve(false);
		const id = `int_${Date.now()}_${Math.floor(Math.random() * 1e6)}`;
		const timer = setTimeout(() => { lane.waiters.delete(id); resolve(false); }, timeoutMs);
		lane.waiters.set(id, () => { clearTimeout(timer); lane.waiters.delete(id); resolve(true); });
		try { lane.proc.stdin.write(JSON.stringify({ type: 'control_request', request_id: id, request: { subtype: 'interrupt' } }) + '\n'); }
		catch { clearTimeout(timer); lane.waiters.delete(id); resolve(false); }
	});
}
async function interruptLane(lane) {
	lane.stopAt = nowMs();
	const out = { lane: lane.key, interrupted: false, claude: null, codex: false, killed: 0 };
	if (lane.cxProc && lane.cxProc.exitCode === null) {
		try { process.kill(-lane.cxProc.pid, 'SIGTERM'); } catch { try { lane.cxProc.kill('SIGTERM'); } catch { /* ja saiu */ } }
		out.codex = true; out.interrupted = true;
	}
	if (lane.proc && lane.proc.exitCode === null && (lane.activeTurn || lane.lingerTurn || lane.busy)) {
		const acked = await sendClaudeInterrupt(lane, 3000);
		const t0 = nowMs();
		while ((lane.activeTurn || lane.busy) && nowMs() - t0 < 3000) await new Promise((r) => setTimeout(r, 100));
		if (lane.activeTurn || lane.busy || !acked) {
			lane.rotated = true; // sessao no disco: a proxima mensagem volta nela com --resume
			await killClaude(lane, 'stop');
			// killClaude solta o processo antes de matar, entao o exit nao fecha o turno: fecha aqui
			if (lane.activeTurn) {
				const t = lane.activeTurn; lane.activeTurn = null;
				try { t.emit('error', { code: 'cancelled', message: 'turno parado pelo STOP', retryable: false }); } catch { /* sink fechado */ }
				if (t.resolve) t.resolve();
			}
			lane.busy = false;
			out.claude = 'killed';
		} else out.claude = 'interrupted';
		lane.lingerTurn = null;
		out.interrupted = true;
	}
	// o que o turno deixou rodando (servidor, build, subagente) — so desta pista
	const keep = lane.proc && lane.proc.exitCode === null ? lane.proc.pid : null;
	// so o que nasceu depois que o ultimo turno da pista comecou (filhos do claude de antes, como
	// MCP stdio, ficam — sao do processo, nao do turno; mesma janela do waitTurnQuiet)
	const since = lane.lastTurnCtx ? lane.lastTurnCtx.since : uptimeTicks();
	const strays = lane.sid ? liveTurnProcs(lane.sid, since, keep, lane.mark, null) : [];
	for (const p of strays) { try { process.kill(p.pid, 'SIGTERM'); out.killed++; } catch { /* ja saiu */ } }
	if (out.killed) out.interrupted = true;
	lane.bgTasks = 0; lane.bgTaskNames = [];
	logInfo('lane interrupted (STOP)', out);
	return out;
}
/** A sessao do Claude Code desta pista ainda esta no disco? (--resume de sessao sumida = erro) */
function laneSessionOnDisk(lane) {
	if (!lane.sessionId) return false;
	const root = path.join(lane.claudeDir, 'projects');
	try { for (const p of fs.readdirSync(root)) if (fs.existsSync(path.join(root, p, `${lane.sessionId}.jsonl`))) return true; } catch { /* sem projects */ }
	return false;
}

/**
 * Drive one /run against the persistent claude process OF THIS LANE.
 *
 * Spawns claude if it's not running OR if the session args / token have
 * changed since last turn. Otherwise reuses the existing process — that's
 * where the time savings come from.
 */
async function runPersistentClaude(lane, envelope, relay, emit) {
	// O claude pode estar CONTINUANDO o turno anterior (acordou com task-notification depois
	// do result). Mandar a mensagem nova no meio faria o proximo result (o da continuacao)
	// fechar ESTE turno com a resposta errada. Espera ele ficar ocioso (teto = o do background).
	if (lane.proc && lane.proc.exitCode === null && lane.busy) {
		emit('phase', { name: 'waiting_claude_idle', ts: nowMs() });
		const until = nowMs() + (envelope.backgroundWaitMs || 10 * 60 * 1000);
		while (lane.busy && lane.proc && lane.proc.exitCode === null && nowMs() < until) await new Promise((r) => setTimeout(r, 500));
	}
	// este turno assume o claude: o anterior deixa de ser dono da saida dele
	lane.lingerTurn = null;
	const newCore = coreSignature(envelope);
	const newModel = envelope.model || null;
	// Auth kind: ccApiKey (static Anthropic API key → spawn env) beats the OAuth
	// trio when present — the Worker sends one OR the other, never both.
	const newAuthKind = envelope.ccApiKey ? 'apiKey' : 'oauth';
	const newTokenHash = tokenHash(envelope.ccApiKey || envelope.ccToken);

	// MODEL SWAP (not respawn): the model is a per-request API parameter — same CC,
	// same context, any model. When ONLY the model differs, ask the running claude to
	// switch in-flight (control_request set_model, the same channel interactive
	// /model uses). Ack → warm reuse on the new model; no ack (older CLI) → the
	// mismatch below falls through to the legacy respawn. Worst case = one 2s
	// timeout per switch; best case kills the model-switch respawn entirely.
	if (
		lane.proc && lane.proc.exitCode === null &&
		lane.sig === newCore && lane.model !== newModel &&
		envelope.forceRespawn !== true
	) {
		const swapped = await trySetModel(lane, newModel, 2000);
		if (swapped) {
			lane.model = newModel;
			emit('phase', { name: 'claude_model_swapped', ts: nowMs(), model: newModel });
		} else {
			emit('phase', { name: 'claude_model_swap_refused', ts: nowMs(), model: newModel });
		}
	}

	// Decide: reuse, or kill+respawn?
	// forceRespawn: set by the Worker on the FIRST /run after a Detona resume.
	// A resumed microVM restores the claude PROCESS but its open HTTPS connection
	// to Anthropic is dead — reusing it hangs ~20-56s. So we kill+respawn (cheap:
	// auth + workspace survive on disk, only the process is rebuilt ~1.4s).
	// A rotated ccToken is NOT a reason to respawn — it's a credential, not a
	// behavior change. Respawning to re-key threw away the warm in-memory session
	// (forcing a context refold, tokIn ~2470) for nothing. The token is now swapped
	// in place in the reuse branch below. Only systemPrompt/maxTurns/mcp changes
	// (core signature), a failed model swap, a dead process, or the Worker's
	// post-resume forceRespawn actually respawn.
	const needsRespawn =
		!lane.proc ||
		lane.proc.exitCode !== null ||
		lane.sig !== newCore ||
		lane.model !== newModel ||
		// Auth transport changed (oauth↔apiKey), or the STATIC key itself changed —
		// ANTHROPIC_API_KEY lives in the spawn env, it can't be swapped in place.
		lane.authKind !== newAuthKind ||
		(newAuthKind === 'apiKey' && lane.tokenHash !== newTokenHash) ||
		envelope.forceRespawn === true ||
		// TROCA DE PROVEDOR (dono, 2026-10-17: "a troca sempre e um novo chat que recebe historico"):
		// o chat passou por outro engine desde a ultima vez que este claude falou — a sessao dele
		// nao tem o que o outro respondeu. Sessao NOVA com o historico inteiro (refold abaixo).
		envelope.freshSession === true;

	// Volta da ROTACAO: a pista foi fechada por falta de memoria e a sessao dela segue no disco →
	// sobe com --resume (conversa, ferramentas e arquivos lidos voltam inteiros; sem refold).
	let resumeId = null;
	if (needsRespawn) {
		if (lane.proc) {
			emit('phase', { name: 'claude_respawn', ts: nowMs(), reason: envelope.freshSession === true ? 'provider_switch' : envelope.forceRespawn === true ? 'post_resume' : (lane.sig !== newCore ? 'args_changed' : (lane.authKind !== newAuthKind ? 'auth_changed' : (lane.model !== newModel ? 'model_swap_failed' : (lane.tokenHash !== newTokenHash ? 'token_rotated' : 'dead')))) });
			await killClaude(lane, 'respawn');
		}
		if (lane.rotated && laneSessionOnDisk(lane) && envelope.freshSession !== true) resumeId = lane.sessionId;
		lane.rotated = false;
		if (envelope.freshSession === true) { lane.sessionId = null; emit('phase', { name: 'fresh_session', ts: nowMs(), engine: 'claude' }); }
		await reserveMemoryFor(lane, emit, envelope.backgroundWaitMs || 10 * 60 * 1000);
		// Refresh credentials on disk and (only on first ever turn) wipe stale
		// session lockfiles. Token may have rotated even when session args are
		// the same, so always rewrite credentials before spawn.
		wipeClaudeState(lane);
		if (newAuthKind === 'apiKey') {
			// API-key auth: the key rides the spawn env (spawnPersistentClaude).
			// Remove any OAuth credentials left on disk (a golden baked with a sub,
			// or a prior oauth turn) so claude can't prefer the stale OAuth pair —
			// and DO NOT start the credWatcher: static keys never rotate, there is
			// nothing to capture (doctrine untouched — no rotation exists here).
			try { fs.rmSync(lane.credFile, { force: true }); } catch { /* best-effort */ }
			stopCredWatcher(lane);
		} else {
			configureCcAuth(lane, envelope.ccToken, envelope.ccRefreshToken, envelope.ccExpiresAt);
			startCredWatcher(lane, envelope.callback);
		}
		const t_spawn_called = nowMs();
		emit('phase', { name: 'pre_claude_spawn', ts: t_spawn_called, ...(resumeId ? { resumed: true } : {}) });
		spawnPersistentClaude(lane, envelope, resumeId);
		emit('phase', { name: 'claude_spawned', ts: nowMs(), since_spawn_call_ms: nowMs() - t_spawn_called });
		lane.activeTurn = {
			relay, emit, t_spawn: t_spawn_called,
			firstStdoutSeen: false, firstSystemSeen: false, firstAssistantSeen: false,
			resolve: null,
		};
	} else {
		// Warm path — reuse existing process. No spawn cost.
		// TOKEN SWAP (not respawn): the ccToken rotates ~hourly, but rotating it does
		// NOT change claude's behavior. Rewrite .credentials.json in place and update the
		// tracked hash; claude re-reads it on its next self-refresh, and its still-valid
		// in-memory token keeps working until then. Do NOT wipeClaudeState() here — the
		// session lockfiles/caches are exactly the warmth we're keeping (that's the win).
		if (newAuthKind === 'oauth' && lane.tokenHash !== newTokenHash) {
			configureCcAuth(lane, envelope.ccToken, envelope.ccRefreshToken, envelope.ccExpiresAt);
			lane.tokenHash = newTokenHash;
			emit('phase', { name: 'claude_token_swapped', ts: nowMs() });
		}
		// apiKey mode: static key (a changed key respawned above); keep the
		// watcher DISARMED — there is no rotation to capture.
		if (newAuthKind === 'oauth') startCredWatcher(lane, envelope.callback);
		emit('phase', { name: 'claude_reused', ts: nowMs() });
		lane.activeTurn = {
			relay, emit, t_spawn: nowMs(),
			firstStdoutSeen: false, firstSystemSeen: false, firstAssistantSeen: false,
			resolve: null,
		};
	}

	// CONTEXT REFOLD AUTOMATION (owner design, 2026-07-07): the box, not the Worker,
	// knows the TRUTH about cold-vs-warm. On a Detona REBASE (base image changed →
	// instance re-inits cold, volume kept) the Worker still sees "resume" (its marker
	// exists) and skips the context fold — a fresh claude would answer with amnesia.
	// So: whenever WE had to spawn a fresh claude (needsRespawn) and the envelope
	// carries the chat's priorContext, fold it into the prompt HERE. The sentinel
	// check makes it idempotent (the Worker pre-folds on clone/base-cold paths — do
	// not fold twice). Warm reuse skips: claude remembers natively. --resume tambem:
	// a sessao voltou inteira, refoldar duplicaria a conversa.
	let promptContent = envelope.prompt;
	if (needsRespawn && !resumeId && envelope.priorContext && !String(promptContent).includes('## Mensagem atual do usuário')) {
		promptContent = `${envelope.priorContext}\n\n## Mensagem atual do usuário\n\n${promptContent}`;
		emit('phase', { name: 'context_refolded', ts: nowMs(), chars: envelope.priorContext.length });
	}

	// Send the user message via stdin as an NDJSON event. stream-json input
	// format expects {"type":"user","message":{"role":"user","content":"..."}}.
	const userEvent = {
		type: 'user',
		message: { role: 'user', content: promptContent },
	};
	try {
		lane.proc.stdin.write(JSON.stringify(userEvent) + '\n');
		lane.busy = true;
	} catch (err) {
		emit('error', { code: 'internal', message: `stdin write failed: ${err && err.message}`, retryable: true });
		lane.activeTurn = null;
		return;
	}

	// Wait for the 'result' event (set by handleClaudeStdout) or process exit.
	await new Promise((resolve) => {
		if (lane.activeTurn) lane.activeTurn.resolve = resolve;
		else resolve(); // already settled by an exit
	});
}

// ─── CODEX ENGINE (2026-07-18) ──────────────────────────────────────────────
// The "second base" that isn't a second base: same box, same machinery, a
// different engine. codex runs ONE-SHOT per turn (`codex exec --json` emits
// JSONL and exits — the binary is fast; warmth comes from the box itself).
// The adapter translates codex events INTO claude stream-json and feeds them
// through handleClaudeStdout — so the relay, the Worker translator, the save
// callback and usage metering all work with ZERO downstream changes ("codex
// just speaks the same format"). Continuity = the same engine-agnostic refold
// (priorContext) claude uses on respawn; files continuity = the chat volume.
// AUTH — two modes, priority SUB > key:
//  · sub  (envelope.codexAuth): the ChatGPT-subscription auth.json, written to
//    $CODEX_HOME/auth.json. codex self-rotates it (same doctrine as the CC sub:
//    the ENGINE is the only rotator; a watcher captures the rotation back).
//  · key  (envelope.codexApiKey): CODEX_API_KEY at spawn — static, no rotation.
// CODEX_HOME e da PISTA (lane.codexHome): box propria = $CODEX_HOME ou /workspace/.codex (como
// sempre); pista de box compartilhada = /workspace/.lanes/<chat>/codex (conta e sessoes do chat).

async function runCodexTurn(lane, envelope, relay, emit) {
	const CODEX_HOME = lane.codexHome;
	// A live claude from a previous engine on this lane (chat switched engines)
	// must not linger — kill it; the codex golden lives in its own namespace.
	if (lane.proc) await killClaude(lane, 'engine_switch');

	// Feed claude-format lines through the NORMAL stdout pipeline (relay write,
	// phase markers, result capture). This is the whole adapter trick.
	const feed = (obj) => handleClaudeStdout(lane, Buffer.from(JSON.stringify(obj) + '\n', 'utf8'));
	let turnDone;
	const done = new Promise((r) => { turnDone = r; });
	lane.activeTurn = {
		relay, emit, t_spawn: nowMs(),
		firstStdoutSeen: false, firstSystemSeen: false, firstAssistantSeen: false,
		resolve: turnDone,
	};

	// SESSION REUSE ("como usar o codex normal", 2026-07-26): the per-chat volume
	// persists $CODEX_HOME across turns (volume-as-upper — every path the box writes
	// lands on the volume), so codex's OWN session rollout survives. When one exists
	// we RESUME it (`codex exec resume --last`) and send ONLY the new message — codex
	// keeps the conversation, the files it read, and its plan (tokIn stays flat, no
	// manual transcript refold, no re-derivation). Only a fresh chat — or a reset that
	// wiped the volume — starts a new session. Mirrors claude's warm-resume, but via
	// codex's session files instead of a resident process (codex exec is one-shot).
	const RUN_CWD = lane.cwd;
	const sessionsDir = path.join(CODEX_HOME, 'sessions');
	let hasSession = false;
	try {
		hasSession = fs.existsSync(sessionsDir) &&
			fs.readdirSync(sessionsDir).some((f) => /\.jsonl$/.test(f) || (() => {
				try { return fs.statSync(path.join(sessionsDir, f)).isDirectory(); } catch { return false; }
			})());
	} catch { /* no sessions dir yet → fresh */ }
	// BOX COLD NAO RESUME. O CODEX_HOME mora no VOLUME e o volume sobrevive a box: uma box
	// que acabou de nascer da base ve o sessions/ da box ANTERIOR e tentava `resume --last`
	// num processo que nunca teve aquela sessao — o turno voltava VAZIO (4s, 0 tokens, sem
	// erro; provado em prod 2026-09-19, todo 1o turno apos agentRecycle). Cold = sessao nova
	// com o historico refolded (o Worker ja manda o fold quando sabe que e cold). Golden e
	// resume de pause restauram a RAM com runsCompleted >= 1, entao NAO caem aqui.
	if (hasSession && envelope.freshSession === true) {
		// troca de provedor: a sessao do codex no disco nao tem o que o outro engine respondeu
		emit('phase', { name: 'fresh_session', ts: nowMs(), engine: 'codex' });
		hasSession = false;
	}
	if (hasSession && runsCompleted === 0) {
		emit('phase', { name: 'codex_cold_ignores_session', ts: nowMs() });
		logInfo('codex: box cold — ignorando sessao do volume, sessao nova com fold');
		hasSession = false;
	}

	// The RAW new message, even if the Worker pre-folded the transcript into the
	// prompt (sentinel split): resume must NOT re-send the history codex already has.
	const rawUserMessage = (p) => {
		const marker = '## Mensagem atual do usuário\n\n';
		const s = String(p == null ? '' : p);
		const i = s.lastIndexOf(marker);
		return i >= 0 ? s.slice(i + marker.length) : s;
	};

	const lastMsgFile = `/tmp/codex-last-${Date.now()}.txt`;
	// Flags accepted by BOTH `exec` and `exec resume`.
	const commonFlags = ['--json', '--ignore-user-config', '--skip-git-repo-check', '--output-last-message', lastMsgFile];

	// MCP (streamable-HTTP): the Worker sends the SAME mcpConfig for codex as for
	// claude ({mcpServers:{<name>:{type:'http',url,headers}}}), but codex reads MCP
	// from CONFIG, not a --mcp-config file (claude's path). So translate each HTTP
	// server into a `-c mcp_servers.<key>={url, env_http_headers}` override — the
	// reliable form under --ignore-user-config, accepted by BOTH exec and resume
	// (rides commonFlags). The header VALUE (the token) goes via ENV, never argv —
	// same doctrine as CC (creds never on the command line / in logs). Before this,
	// mcpConfig was silently dropped on codex: the Worker sent it, the codex adapter
	// never wired it. (owner: "mcp não funciona no codex", 2026-08-01)
	const mcpEnv = {};
	const _mcpServers = envelope.mcpConfig && envelope.mcpConfig.mcpServers;
	if (_mcpServers && typeof _mcpServers === 'object') {
		const bareKey = (k) => /^[A-Za-z0-9_-]+$/.test(k) ? k : JSON.stringify(k);
		let idx = 0;
		for (const [name, s] of Object.entries(_mcpServers)) {
			if (!s || !s.url) continue; // codex speaks HTTP MCP here; skip malformed
			// codex config key: a bare TOML key. The unquoted `-c` dotted path is the
			// only form the override parser reliably accepts (a hyphen/dot in the raw
			// name breaks it), so sanitize. This is codex's INTERNAL server label only
			// — tool names come from the server itself, so it never reaches tool ids.
			const key = String(name).replace(/[^A-Za-z0-9_]/g, '_') || `mcp${idx}`;
			const headers = (s.headers && typeof s.headers === 'object') ? s.headers : {};
			const envMap = {};
			for (const [h, v] of Object.entries(headers)) {
				if (v == null) continue;
				const ev = `MCP_HDR_${key}_${String(h).replace(/[^A-Za-z0-9_]/g, '_')}`.toUpperCase();
				mcpEnv[ev] = String(v); // token lives in env, read by codex at connect
				envMap[h] = ev;         // header name → env var to read
			}
			const parts = [`url=${JSON.stringify(s.url)}`];
			if (Object.keys(envMap).length) {
				parts.push(`env_http_headers={${Object.entries(envMap).map(([h, ev]) => `${bareKey(h)}=${JSON.stringify(ev)}`).join(', ')}}`);
			}
			commonFlags.push('-c', `mcp_servers.${key}={${parts.join(', ')}}`);
			idx++;
		}
		if (idx) emit('phase', { name: 'codex_mcp', ts: nowMs(), servers: idx });
	}
	// Model + REASONING EFFORT — independent. A ChatGPT-account sub can't pick a
	// model (OpenAI 400s every explicit id — only the account default works), so we
	// OMIT -m there and drive the sole real knob, reasoning effort, via
	// `-c model_reasoning_effort=<minimal|low|medium|high>`. A key-based codex (API)
	// still gets -m for its real model. Both honor codexReasoning when set.
	const pushModel = (a) => {
		if (envelope.model) a.push('-m', String(envelope.model));
		if (envelope.codexReasoning) a.push('-c', `model_reasoning_effort=${envelope.codexReasoning}`);
	};
	let args;
	if (hasSession) {
		// resume the chat's session — `resume` inherits the session's cwd + sandbox
		// (it doesn't accept -C/--sandbox), so we bypass approvals+sandbox for the
		// same full-access-in-the-microVM behavior as the fresh path below.
		args = ['exec', 'resume', '--last', ...commonFlags, '--dangerously-bypass-approvals-and-sandbox'];
		pushModel(args);
		// `--` ANTES do prompt: o texto do usuário é posicional, e clap trata qualquer
		// coisa iniciada por '-' como flag → `codex exited 2: unexpected argument '- '`
		// e o TURNO SOME (o usuário manda uma lista em markdown e não recebe nada).
		// Visto em prod 2026-09-11, 2 turnos seguidos perdidos. O próprio codex sugere
		// a saída no erro ("to pass '- ' as a value, use '-- - '"). Provado em metal que
		// depois do `--` o texto vai pro [PROMPT] e NÃO pro [SESSION_ID] do resume
		// (o thread_id do --last seguiu o mesmo). claude não precisa: prompt vai por stdin.
		args.push('--', rawUserMessage(envelope.prompt));
		emit('phase', { name: 'codex_resume', ts: nowMs() });
	} else {
		// fresh session. Fold prior context if present (cold box mid-chat with the
		// session gone — the correctness safety net; idempotent via the sentinel).
		let promptContent = envelope.prompt;
		if (envelope.priorContext && !String(promptContent).includes('## Mensagem atual do usuário')) {
			promptContent = `${envelope.priorContext}\n\n## Mensagem atual do usuário\n\n${promptContent}`;
			emit('phase', { name: 'context_refolded', ts: nowMs(), chars: envelope.priorContext.length });
		}
		// The microVM IS the sandbox → codex's own sandbox off (full access), and
		// --skip-git-repo-check because /workspace is not a git repo.
		args = ['exec', ...commonFlags, '--sandbox', 'danger-full-access', '-C', RUN_CWD];
		pushModel(args);
		args.push('--', promptContent); // ver comentário no ramo de resume: prompt começando com '-' matava o turno

	}

	// AUTH: sub (auth.json) wins over the static key. The sub is written to
	// $CODEX_HOME/auth.json where the CLI reads it (and self-rotates it in place —
	// the watcher below captures the rotation back, mirroring the CC credWatcher).
	const codexEnv = { ...process.env, CODEX_HOME, ...mcpEnv };
	if (envelope.codexAuth) {
		try {
			fs.mkdirSync(CODEX_HOME, { recursive: true, mode: 0o700 });
			fs.writeFileSync(lane.codexAuthFile, typeof envelope.codexAuth === 'string' ? envelope.codexAuth : JSON.stringify(envelope.codexAuth), { mode: 0o600 });
			lane.cx.last = codexAuthFingerprint(lane);
			startCodexAuthWatcher(lane, envelope.callback);
		} catch (e) { logError('codex auth.json write failed', { msg: e && e.message }); }
	} else if (envelope.codexApiKey) {
		// static key mode — no auth.json, no watcher.
		try { fs.rmSync(lane.codexAuthFile, { force: true }); } catch {}
		codexEnv.CODEX_API_KEY = envelope.codexApiKey;
		codexEnv.OPENAI_API_KEY = envelope.codexApiKey;
	}
	emit('phase', { name: 'codex_spawn', ts: nowMs(), auth: envelope.codexAuth ? 'sub' : 'key' });
	lane.mark = newTurnMark();
	const proc = spawnInTurnCgroup('codex', args, {
		detached: true, // setsid: o turno e a SESSAO (ver waitTurnQuiet)
		cwd: lane.cwd,
		env: { ...codexEnv, [TURN_ENV]: lane.mark },
		stdio: ['ignore', 'pipe', 'pipe'],
	});
	lane.sid = proc.pid;
	lane.cxProc = proc; proc.once('exit', () => { if (lane.cxProc === proc) lane.cxProc = null; });
	lane.bgTasks = 0; lane.bgTaskNames = []; // codex nao tem subagente
	feed({ type: 'system', subtype: 'init' }); // claude_first_system phase (adapter alive)

	let buf = '';
	let finalText = '';
	let usage = null;
	let sessionId = '';
	const stderrTail = [];
	// ── ADAPTER DE ITENS DO CODEX (2026-09-11) ──────────────────────────────────
	// Antes so 3 tipos viravam evento (agent_message, reasoning, command_execution)
	// e TODO o resto sumia em silencio — inclusive mcp_tool_call, que passou a
	// existir de verdade quando o MCP voltou a funcionar no codex (fix do 202).
	// Efeito pro usuario: o agente CHAMAVA a ferramenta e a UI nao mostrava nada
	// (provado: turno respondeu "TOOLS=5" com ZERO eventos tool-call no stream).
	// Tipos que o binario do codex conhece: agent_message, reasoning,
	// command_execution, mcp_tool_call, patch_apply, file_change, web_search,
	// todo_list. Extracao DEFENSIVA (cada campo tem candidatos) porque o schema do
	// CLI muda entre versoes; e o que nao for reconhecido vira um phase com as
	// CHAVES (nunca os valores) em vez de desaparecer.
	const startedItems = new Set();
	const TOOLISH = new Set(['command_execution', 'mcp_tool_call', 'web_search', 'patch_apply', 'file_change', 'todo_list']);
	const KNOWN_EVENTS = new Set(['thread.started', 'turn.started', 'turn.completed', 'turn.failed', 'item.started', 'item.completed', 'item.updated', 'error']);
	const pick = (o, ...names) => { for (const n of names) { const v = o && o[n]; if (v != null && v !== '') return v; } return undefined; };
	const toolNameOf = (it) => {
		switch (it.type) {
			case 'command_execution': return 'bash';
			case 'mcp_tool_call': {
				const srv = pick(it, 'server', 'server_name', 'serverLabel', 'server_label');
				const tool = pick(it, 'tool', 'tool_name', 'name');
				return srv && tool ? `mcp__${srv}__${tool}` : String(tool || srv || 'mcp_tool');
			}
			case 'web_search': return 'web_search';
			case 'patch_apply': case 'file_change': return 'edit';
			case 'todo_list': return 'todo';
			default: return String(it.type || 'tool');
		}
	};
	const toolInputOf = (it) => {
		switch (it.type) {
			case 'command_execution': return { command: String(pick(it, 'command', 'cmd') || '') };
			case 'mcp_tool_call': return pick(it, 'arguments', 'args', 'input', 'params') ?? {};
			case 'web_search': return { query: String(pick(it, 'query', 'q', 'search') || '') };
			case 'patch_apply': case 'file_change': return { changes: pick(it, 'changes', 'files', 'path', 'diff') ?? {} };
			case 'todo_list': return { items: pick(it, 'items', 'todos') ?? [] };
			default: return {};
		}
	};
	// ERRO de ferramenta: cobre as varias formas que o codex usa (status textual,
	// campo error, exit_code != 0). O dono pediu erro visivel tanto do agente
	// quanto das tools do usuario — este predicado e o que marca is_error.
	const isErr = (it) => it.is_error === true || it.error != null
		|| it.status === 'failed' || it.status === 'error'
		|| (it.exit_code != null && it.exit_code !== 0);
	const outputOf = (it) => {
		const v = pick(it, 'aggregated_output', 'output', 'result', 'content', 'text', 'error', 'message');
		return (typeof v === 'string' ? v : v == null ? '' : JSON.stringify(v)).slice(0, 2000);
	};
	// id do item vira o id do par call/result: sem ele a UI recebe a chamada e o
	// resultado SOLTOS (toolCallId vazio no result) e nao consegue casar os dois.
	const idOf = (it) => { const v = pick(it, 'id', 'item_id', 'call_id'); return v == null ? '' : String(v); };
	const emitToolCall = (it) => {
		const id = idOf(it);
		if (id) { if (startedItems.has(id)) return; startedItems.add(id); }
		feed({ type: 'content_block_start', content_block: { type: 'tool_use', id, name: toolNameOf(it), input: toolInputOf(it) } });
	};
	// Alguns itens so trazem os argumentos no item.completed (provado com
	// mcp_tool_call: no started o input vinha vazio). Entao no started so emitimos
	// quando ja ha input util; senao esperamos o completed, que emite o par.
	const hasInput = (it) => { const i = toolInputOf(it); return !!i && Object.values(i).some((v) => v != null && v !== '' && !(Array.isArray(v) && !v.length) && !(typeof v === 'object' && v && !Array.isArray(v) && !Object.keys(v).length)); };
	proc.stdout.on('data', (chunk) => {
		buf += chunk.toString('utf8');
		let nl;
		while ((nl = buf.indexOf('\n')) >= 0) {
			const line = buf.slice(0, nl).trim();
			buf = buf.slice(nl + 1);
			if (!line) continue;
			let ev;
			try { ev = JSON.parse(line); } catch { continue; }
			if (!ev || typeof ev !== 'object') continue;
			if (ev.type === 'thread.started' && ev.thread_id) sessionId = String(ev.thread_id);
			if (ev.type === 'turn.completed' && ev.usage && typeof ev.usage === 'object') usage = ev.usage;
			const item = ev.item && typeof ev.item === 'object' ? ev.item : null;
			if (ev.type === 'item.started' && item) {
				if (TOOLISH.has(item.type) && hasInput(item)) emitToolCall(item);
			} else if (ev.type === 'item.completed' && item) {
				if (item.type === 'agent_message' && typeof item.text === 'string') {
					finalText = item.text; // last agent_message wins (pre -o fallback)
					feed({ type: 'content_block_delta', delta: { type: 'text_delta', text: item.text } });
				} else if (item.type === 'reasoning' && typeof item.text === 'string') {
					feed({ type: 'content_block_delta', delta: { type: 'thinking_delta', thinking: item.text } });
				} else if (TOOLISH.has(item.type)) {
					// Garante o PAR call+result: item curto pode nunca emitir item.started,
					// e resultado sem chamada aparece como resposta surgida do nada.
					emitToolCall(item);
					feed({ type: 'tool_result', id: idOf(item), name: toolNameOf(item), is_error: isErr(item), content: outputOf(item) });
					if (item.type === 'patch_apply' || item.type === 'file_change') {
						emit('phase', { name: 'file_changed', ts: nowMs(), tool: toolNameOf(item), error: isErr(item) });
					}
				} else {
					// NADA some calado: tipo novo do CLI vira log com as CHAVES, sem valores.
					emit('phase', { name: 'codex_item_unmapped', ts: nowMs(), item_type: String(item.type || '?'), keys: Object.keys(item).slice(0, 15) });
				}
			} else if (!KNOWN_EVENTS.has(ev.type)) {
				emit('phase', { name: 'codex_event_unmapped', ts: nowMs(), event_type: String(ev.type || '?') });
			}
			if (ev.type === 'error' || ev.type === 'turn.failed') {
				const msg = (ev.error && (ev.error.message || ev.error)) || ev.message || 'codex error';
				emit('error', { code: 'codex_failed', message: String(msg).slice(0, 500), retryable: false });
			}
		}
	});
	proc.stderr.on('data', (c) => {
		stderrTail.push(c);
		let total = 0;
		for (const b of stderrTail) total += b.length;
		while (total > 65536 && stderrTail.length > 1) total -= stderrTail.shift().length;
	});

	proc.on('exit', async (code) => {
		// --output-last-message is the AUTHORITATIVE final text — survives any
		// schema drift in the --json events across codex versions.
		try {
			const last = fs.readFileSync(lastMsgFile, 'utf8').trim();
			if (last) finalText = last;
		} catch { /* no file — keep what the events gave us */ }
		try { fs.rmSync(lastMsgFile, { force: true }); } catch { /* best-effort */ }
		if (code !== 0 && !finalText) {
			const tail = Buffer.concat(stderrTail).toString('utf8').slice(-512);
			emit('error', { code: 'codex_exit', message: `codex exited ${code}${tail ? `\nstderr: ${tail}` : ''}`, retryable: false });
		}
		// FLUSH DA ROTAÇÃO DO CODEX — espelho do `await credWatcherReport()` do claude.
		// O refresh token do sub é ROTATIVO DE USO ÚNICO: o codex refresha aqui dentro,
		// reescreve auth.json e o token velho morre no servidor da OpenAI. O watcher só
		// tinha o caminho do fs.watch com setTimeout(report,150) — e esses 150ms PERDEM
		// a corrida contra o pauseAfter (a box congela quando o stream fecha) ou contra
		// o destroy do one-shot. Resultado provado em prod: ZERO rotações capturadas,
		// auth.json guardado parado desde 2026-08-06, e todo turno depois da primeira
		// rotação morrendo com "refresh token was already used". Reportar ANTES de
		// fechar o turno é o que torna a captura determinística.
		if (lane.cx.report) { try { await lane.cx.report(); } catch { /* nunca derruba o turno */ } }
		// claude-format result closes the turn: pendingResult (save callback) +
		// the Worker translator's finish, exactly like a claude turn.
		feed({
			type: 'result',
			result: finalText,
			...(sessionId ? { session_id: sessionId } : {}),
			usage: {
				input_tokens: (usage && Number(usage.input_tokens)) || 0,
				output_tokens: (usage && Number(usage.output_tokens)) || 0,
			},
		});
		emit('phase', { name: 'codex_exit', ts: nowMs(), code });
		turnDone(); // idempotent — feed(result) normally resolved it already
	});

	await done;
}

// CODEX SUB CONNECT (device flow via the official CLI — passes OpenAI's WAF).
// Streams the device URL + code out as normal assistant text (so the Studio shows
// them with no translator change), keeps the CLI polling, and on success mirrors
// the resulting auth.json to the Worker via the callback.
async function runCodexLogin(envelope, relay, emit) {
	const lane = ownLane(); const CODEX_HOME = lane.codexHome; // box one-shot: a pista unica
	if (lane.proc) await killClaude(lane, 'codex_login'); // free the box's engine slot
	const feed = (obj) => handleClaudeStdout(lane, Buffer.from(JSON.stringify(obj) + '\n', 'utf8'));
	let done; const p = new Promise((r) => { done = r; });
	lane.activeTurn = { relay, emit, t_spawn: nowMs(), firstStdoutSeen: false, firstSystemSeen: false, firstAssistantSeen: false, resolve: done };
	feed({ type: 'system', subtype: 'init' });

	try { fs.mkdirSync(CODEX_HOME, { recursive: true, mode: 0o700 }); } catch {}
	emit('phase', { name: 'codex_login_spawn', ts: nowMs() });
	const proc = spawn('codex', ['login', '--device-auth'], {
		cwd: '/workspace', env: { ...process.env, CODEX_HOME }, stdio: ['ignore', 'pipe', 'pipe'],
	});

	let buf = '', url = '', code = '', announced = false;
	const strip = (s) => s.replace(/\x1b\[[0-9;]*m/g, ''); // drop ANSI colors
	const scan = (chunk) => {
		buf += strip(chunk.toString('utf8'));
		const u = buf.match(/https:\/\/auth\.openai\.com\/[^\s]+/);
		const c = buf.match(/\b([A-Z0-9]{4}-[A-Z0-9]{4,6})\b/);
		if (u) url = u[0];
		if (c) code = c[1];
		if (!announced && url && code) {
			announced = true;
			emit('phase', { name: 'codex_device_code', ts: nowMs(), url, code });
			feed({ type: 'content_block_delta', delta: { type: 'text_delta', text:
				`🔗 **Conectar Codex (sua sub ChatGPT)**\n\n1. Abra: ${url}\n2. Digite o código: **${code}**\n3. Autorize com sua conta ChatGPT\n\n_Aguardando autorização (expira em 15 min)…_\n` } });
		}
	};
	proc.stdout.on('data', scan);
	proc.stderr.on('data', scan);

	proc.on('exit', async (exitCode) => {
		let ok = false;
		try {
			const raw = fs.readFileSync(lane.codexAuthFile, 'utf8');
			if (raw.trim() && envelope.callback && envelope.callback.url) {
				const cbUrl = envelope.callback.url.replace(/\/internal\/chat-result$/, '/internal/codex-credentials');
				const r = await fetch(cbUrl, { method: 'POST', headers: { 'Content-Type': 'application/json', ...(envelope.callback.jwt ? { Authorization: `Bearer ${envelope.callback.jwt}` } : {}) }, body: JSON.stringify({ authJson: raw }) });
				ok = r.ok;
			}
		} catch (e) { logError('codex login capture failed', { msg: e && e.message }); }
		feed({ type: 'content_block_delta', delta: { type: 'text_delta', text: ok ? '\n✅ **Codex conectado!** Sua sub está pronta — escolha um modelo Codex e mande uma mensagem.' : `\n❌ Login não completou${exitCode ? ` (código ${exitCode})` : ''}. Tente de novo.` } });
		feed({ type: 'result', result: ok ? 'codex_connected' : 'codex_login_failed', usage: { input_tokens: 0, output_tokens: 0 } });
		emit('phase', { name: 'codex_login_exit', ts: nowMs(), ok, code: exitCode });
		done();
	});

	// Safety: the CLI polls up to 15min; cap our wait a touch under Detona's turn timeout.
	setTimeout(() => { try { proc.kill('SIGKILL'); } catch {} }, 14 * 60 * 1000);
	await p;
}

// ---------------------------------------------------------------------------
// EXTRACT the codex model catalog from codex's OWN app-server (`model/list` RPC).
// This is the source of truth — new models appear without any code change. Runs
// authed when envelope.codexAuth is present (→ the account's entitled models), and
// posts the parsed list [{id,displayName,description,isDefault,reasoningEfforts}]
// to /internal/codex-models via the callback. Never a hardcoded list.
async function runCodexModels(envelope, relay, emit) {
	const lane = ownLane(); const CODEX_HOME = lane.codexHome; // box one-shot: a pista unica
	if (lane.proc) await killClaude(lane, 'codex_models');
	const feed = (obj) => handleClaudeStdout(lane, Buffer.from(JSON.stringify(obj) + '\n', 'utf8'));
	let done; const p = new Promise((r) => { done = r; });
	lane.activeTurn = { relay, emit, t_spawn: nowMs(), firstStdoutSeen: false, firstSystemSeen: false, firstAssistantSeen: false, resolve: done };
	feed({ type: 'system', subtype: 'init' });

	try { fs.mkdirSync(CODEX_HOME, { recursive: true, mode: 0o700 }); } catch {}
	if (envelope.codexAuth) {
		try { fs.writeFileSync(lane.codexAuthFile, typeof envelope.codexAuth === 'string' ? envelope.codexAuth : JSON.stringify(envelope.codexAuth), { mode: 0o600 }); } catch {}
	}
	emit('phase', { name: 'codex_models_spawn', ts: nowMs() });
	const proc = spawn('codex', ['app-server'], { cwd: '/workspace', env: { ...process.env, CODEX_HOME }, stdio: ['pipe', 'pipe', 'ignore'] });
	let buf = '';
	proc.stdout.on('data', (d) => { buf += d.toString('utf8'); });
	const send = (o) => { try { proc.stdin.write(JSON.stringify(o) + '\n'); } catch {} };
	send({ jsonrpc: '2.0', id: 1, method: 'initialize', params: { clientInfo: { name: 'smoothagent', title: 'SmoothAgent', version: '1.0.0' } } });
	setTimeout(() => send({ jsonrpc: '2.0', id: 2, method: 'model/list', params: {} }), 2000);

	setTimeout(async () => {
		let models = [];
		for (const line of buf.split('\n')) {
			if (!line.trim()) continue;
			try {
				const o = JSON.parse(line);
				if (o.id === 2 && o.result && Array.isArray(o.result.data)) {
					models = o.result.data.filter((m) => !m.hidden).map((m) => ({
						id: m.id, model: m.model, displayName: m.displayName, description: m.description,
						isDefault: !!m.isDefault, defaultReasoningEffort: m.defaultReasoningEffort,
						reasoningEfforts: (m.supportedReasoningEfforts || []).map((e) => e.reasoningEffort),
					}));
				}
			} catch { /* skip non-JSON */ }
		}
		let posted = false;
		try {
			if (envelope.callback && envelope.callback.url) {
				const cbUrl = envelope.callback.url.replace(/\/internal\/chat-result$/, '/internal/codex-models');
				const r = await fetch(cbUrl, { method: 'POST', headers: { 'Content-Type': 'application/json', ...(envelope.callback.jwt ? { Authorization: `Bearer ${envelope.callback.jwt}` } : {}) }, body: JSON.stringify({ models }) });
				posted = r.ok;
			}
		} catch (e) { logError('codex model/list post failed', { msg: e && e.message }); }
		emit('phase', { name: 'codex_models_result', ts: nowMs(), count: models.length, posted });
		feed({ type: 'content_block_delta', delta: { type: 'text_delta', text: JSON.stringify(models) } });
		feed({ type: 'result', result: 'codex_models', usage: { input_tokens: 0, output_tokens: 0 } });
		try { proc.kill('SIGKILL'); } catch {}
		done();
	}, 6000);
	setTimeout(() => { try { proc.kill('SIGKILL'); } catch {} }, 30_000);
	await p;
}

// ---------------------------------------------------------------------------
// AUTH — trust-on-first-RUN key pinning (2026-07-07 audit: /run had NO auth at
// all; anything with network reach could run claude with bypassPermissions).
//
// The image is generic and boots with no secrets, so there is nothing baked to
// compare against. The pin therefore comes from the first POST /run — which is
// by construction the CREATOR's (Detona delivers the worker's run.http at spawn)
// — and is demanded (timing-safe) on every mutating/sensitive request after.
// Golden bakes pin the worker's key, so clones inherit the pin via the snapshot.
// If the worker's key rotates, old boxes 401 and the worker's transport retry
// destroys + respawns them — self-healing.
//
// Why pin ONLY on POST /run (not any-first-request): Detona's template build
// boots the VM and its readiness probe GETs the port BEFORE any worker request
// exists. Pinning on that probe would pin EMPTY into the template snapshot —
// auth permanently open — and 401-ing it breaks the build (AGENT_READY timeout,
// observed live on import). GETs are safe pre-pin: /health is static, /stream
// has nothing to replay before the first run.
//
// An empty first-run key pins '' (auth effectively open) — loudly logged so a
// misconfigured DETONA_CC_KEY is visible instead of silently unprotected.
let pinnedApiKey = null; // null = nothing pinned yet; '' = pinned open
function apiKeyMatches(req) {
	const got = String(req.headers['x-api-key'] ?? '');
	const a = Buffer.from(got), b = Buffer.from(pinnedApiKey);
	return a.length === b.length && crypto.timingSafeEqual(a, b);
}

// ─── USER STREAM TOKEN (direct user→box on GET /stream) ─────────────────────
// The Worker mints a short Ed25519 token per turn so the END-USER can attach to
// /stream DIRECTLY through the exposed URL (no Worker in the byte path). The box
// only ever holds the PUBLIC key → harmless inside a golden fork. Both values
// are REBOUND on every /run (SNAPSHOT TRAP: process globals freeze into forks;
// a stale pubkey would reject every token after a key rotation).
// The token opens ONLY GET /stream. /run keeps demanding the pinned x-api-key.
// Chats que rodaram turno NESTA box (o token tem que ser de um deles). Box propria = um chat;
// box compartilhada = varios. Rebind a cada /run (snapshot trap: o conjunto cresce com a box).
const streamChats = new Set();
let streamPubKey = null;   // Ed25519 SPKI DER (Buffer) or null = feature off
function b64urlDecode(str) {
	const pad = str.length % 4 === 0 ? '' : '='.repeat(4 - (str.length % 4));
	return Buffer.from(str.replace(/-/g, '+').replace(/_/g, '/') + pad, 'base64');
}
/** Returns null when valid, else a short reason. Valid token → req.streamChat = o chat dele. */
function userStreamTokenWhy(req) {
	if (!streamPubKey) return 'no_pubkey';
	let raw = '';
	const authz = String(req.headers['authorization'] ?? '');
	if (authz.startsWith('Bearer ')) raw = authz.slice(7).trim();
	if (!raw) { const q = req.url.indexOf('?'); if (q >= 0) raw = new URLSearchParams(req.url.slice(q + 1)).get('token') || ''; }
	if (!raw) return 'no_token';
	const dot = raw.indexOf('.');
	if (dot <= 0) return 'malformed';
	let payloadBuf, sig, payload;
	try { payloadBuf = b64urlDecode(raw.slice(0, dot)); sig = b64urlDecode(raw.slice(dot + 1)); payload = JSON.parse(payloadBuf.toString('utf8')); }
	catch { return 'malformed'; }
	if (!payload || typeof payload.c !== 'string' || typeof payload.e !== 'number') return 'malformed';
	let ok = false;
	try { ok = crypto.verify(null, payloadBuf, { key: streamPubKey, format: 'der', type: 'spki' }, sig); } catch { ok = false; }
	if (!ok) return 'bad_sig';
	if (payload.e <= Math.floor(Date.now() / 1000)) return 'expired';
	if (!streamChats.has(payload.c)) return 'wrong_chat';
	req.streamChat = payload.c;
	return null;
}

// ─── SLOT RUNTIME (SLOT_CONTRACT.md, Fase 1 2026-07-11) ─────────────────────
// The client's agent server: a PERSISTENT process speaking GET /ready +
// POST /agent-run on localhost. Managed exactly like claude: spawned once,
// kept across turns, captured in the golden snapshot — ONE photo holds
// adapter + client SDK (the "um cold só"). slot {cmd, port} arrives in the
// envelope but is agent-level PRE-CONFIGURATION upstream; the envelope comes
// from our Worker (pinned x-api-key), same trust as build/exec cmd.
// SNAPSHOT RULE (the credWatcher lesson): nothing per-turn lives here —
// slotProc/slotKey only hold the process identity, which the photo SHOULD
// carry. Per-turn data (prompt, secrets, context) rides each /agent-run body.
let slotProc = null;
let slotKey = null; // cmd|port the running process was started with

function slotFetch(port, path, opts, inactivityMs) {
	return new Promise((resolve, reject) => {
		const r = http.request(
			{ host: '127.0.0.1', port, path, method: (opts && opts.method) || 'GET', headers: (opts && opts.headers) || {} },
			resolve,
		);
		r.on('error', reject);
		// Inactivity timeout (socket idle), NOT a total cap — a slow SDK streaming
		// tokens keeps the socket busy and lives; a dead one gets reaped.
		r.setTimeout(inactivityMs || 5_000, () => r.destroy(new Error('slot inactivity timeout')));
		if (opts && opts.body) r.write(opts.body);
		r.end();
	});
}

/** Spawn the slot process if missing/dead/config-changed, then poll /ready. */
async function ensureSlotReady(slot, emit, bootTimeoutMs) {
	const key = `${slot.cmd}|${slot.port}`;
	if (!slotProc || slotProc.exitCode !== null || slotKey !== key) {
		if (slotProc && slotProc.exitCode === null) { try { slotProc.kill('SIGTERM'); } catch { /* already gone */ } }
		emit('phase', { name: 'slot_spawn', ts: nowMs() });
		slotProc = spawn('/bin/sh', ['-c', slot.cmd], {
			cwd: process.env.RUN_CWD || '/workspace',
			env: { ...process.env, SLOT_PORT: String(slot.port) },
			stdio: ['ignore', 'pipe', 'pipe'],
		});
		slotProc.stdout.on('data', (d) => logInfo('slot stdout', { line: String(d).slice(0, 200) }));
		slotProc.stderr.on('data', (d) => logError('slot stderr', { line: String(d).slice(0, 200) }));
		slotKey = key;
	}
	const deadline = nowMs() + (bootTimeoutMs || 60_000);
	let lastErr = 'not_attempted';
	while (nowMs() < deadline) {
		try {
			const r = await slotFetch(slot.port, '/ready', {}, 2_000);
			if (r.statusCode === 200) {
				let body = '';
				for await (const c of r) body += c;
				return { ok: true, ready: body.slice(0, 200) };
			}
			lastErr = `http_${r.statusCode}`;
			r.resume(); // drain
		} catch (e) { lastErr = e && e.message ? e.message : 'x'; }
		await new Promise((rs) => setTimeout(rs, 150));
	}
	return { ok: false, ready: null, err: lastErr };
}

/** One turn against the slot: POST /agent-run, relay its NDJSON stream. */
async function runSlotTurn(envelope, relay, emit) {
	const slot = envelope.slot;
	const up = await ensureSlotReady(slot, emit, 60_000);
	emit('phase', { name: 'slot_ready', ts: nowMs(), ack: up.ok, info: up.ready || up.err });
	if (!up.ok) { emit('error', { code: 'slot_not_ready', message: String(up.err), retryable: true }); return; }
	const body = JSON.stringify({
		prompt: envelope.prompt || '',
		context: envelope.priorContext || '',
		secrets: envelope.secrets || {},
		sessionId: envelope.sessionId || envelope.chatId || null,
		meta: { chatId: envelope.chatId || null, promptId: envelope.promptId || null },
	});
	const resp = await slotFetch(slot.port, '/agent-run', {
		method: 'POST',
		headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
		body,
	}, 300_000);
	emit('phase', { name: 'slot_run_started', ts: nowMs() });
	let buf = '';
	let sawFirst = false;
	for await (const chunk of resp) {
		buf += chunk;
		let i;
		while ((i = buf.indexOf('\n')) >= 0) {
			const line = buf.slice(0, i).trim();
			buf = buf.slice(i + 1);
			if (!line) continue;
			if (!sawFirst) { sawFirst = true; emit('phase', { name: 'slot_first_byte', ts: nowMs() }); }
			let ev = null;
			try { ev = JSON.parse(line); } catch { continue; } // contract: NDJSON only
			if (ev.type === 'text') {
				emit('text', { text: String(ev.data ?? '') });
			} else if (ev.type === 'result') {
				const d = ev.data || {};
				// pendingResult feeds relay.complete's Worker callback (persistence).
				relay.pendingResult = { text: d.text || '', usage: d.usage || null, sessionId: envelope.sessionId || null, context: typeof d.context === 'string' ? d.context : null };
				emit('result', { text: d.text || '', usage: d.usage || null, context: typeof d.context === 'string' ? d.context : null });
			} else if (ev.type === 'error') {
				emit('error', { code: 'slot_error', message: String((ev.data && ev.data.message) || 'slot error'), retryable: false });
			}
		}
	}
}

// TURNO VIVO = BOX NAO OCIOSA (2026-09-27). O idle-pause do Detona (idlePauseSeconds=1800)
// NAO enxerga turno: com run.http aberto ele marca busy:false e o idle segue contando ate um
// exec pela API. Provado em prod: chat_795256c4, turno de build com a aba ABERTA e heartbeat
// correndo, pausado aos 30:31 no meio de 107 tools (claude congelado). A box nao fala com o
// Detona; avisa o Worker (mesmo JWT do callback do turno, 24h) e o Worker zera o idle.
const ALIVE_EVERY_MS = 4 * 60 * 1000;
setInterval(() => {
	if (openRuns <= 0) return;
	const r = openRelays[0] || currentRelay;
	const cb = r && r.callback;
	if (!cb || !cb.url) return;
	const url = cb.url.replace(/\/internal\/chat-result$/, '/internal/turn-alive');
	fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json', ...(cb.jwt ? { Authorization: `Bearer ${cb.jwt}` } : {}) }, body: JSON.stringify({ chatId: r.chatId, promptId: r.promptId }) })
		.then((x) => { if (!x.ok) logError('turn-alive recusado', { status: x.status }); })
		.catch((e) => logError('turn-alive falhou', { msg: e && e.message }));
}, ALIVE_EVERY_MS);

const server = http.createServer(async (req, res) => {
	if (req.method === 'GET' && (req.url === '/health' || req.url === '/')) {
		// Open on purpose: static liveness for Detona's build/readiness probes.
		res.writeHead(200, { 'Content-Type': 'application/json' });
		// busyChats: quais chats tem turno aberto (rodando, na vez ou segurando background). Box
		// compartilhada: o Worker so pausa quando nenhum OUTRO chat da box esta aqui.
		const busyChats = [...lanes.values()].filter((l) => l.openRuns > 0).map((l) => l.lastChatId || l.key);
		res.end(JSON.stringify({ status: 'ok', inFlight: openRuns > 0, busyChats }));
		return;
	}

	// Past this point everything is sensitive (/run runs claude, /stream replays
	// the whole turn). Once a key is pinned, demand it. Before any pin exists the
	// box has nothing to protect (no turn ever ran) — /stream 204s below and
	// POST /run performs the pinning.
	// GET /stream is the ONE route a user stream token may open (direct user→box).
	// Everything else (notably /run) still demands the pinned x-api-key.
	const isStreamGet = req.method === 'GET' && (req.url === '/stream' || req.url.startsWith('/stream?'));
	let userTokenWhy = null;
	if (pinnedApiKey !== null && !apiKeyMatches(req)) {
		userTokenWhy = isStreamGet ? userStreamTokenWhy(req) : 'not_stream';
		if (userTokenWhy !== null) {
			res.writeHead(401, { 'Content-Type': 'application/json' });
			res.end(JSON.stringify(isStreamGet ? { error: 'unauthorized', why: userTokenWhy } : { error: 'unauthorized' }));
			return;
		}
	}

	// F5 / reconnect: attach to the current turn's relay — replay everything
	// buffered so far, then tail live. No active turn → 204, Worker reads D1.
	// Agent-agnostic: works for any adapter that drives the relay.
	if (isStreamGet) {
		const q = req.url.indexOf('?');
		const wantPrompt = q >= 0 ? new URLSearchParams(req.url.slice(q + 1)).get('promptId') : null;
		const qs = q >= 0 ? new URLSearchParams(req.url.slice(q + 1)) : null;
		// token de usuario = so o chat dele; o Worker (chave) pode filtrar por ?chatId=
		const target = relayFor(wantPrompt, req.streamChat || (qs && qs.get('chatId')) || null);
		if (!target) { res.writeHead(204).end(); return; }
		res.writeHead(200, {
			'Content-Type': 'application/x-ndjson',
			'Cache-Control': 'no-cache, no-store',
			'X-Accel-Buffering': 'no',
		});
		target.addSink(res);
		// KEEPALIVE — /stream sinks only (never sink #0, the Worker's /run). The
		// Detona expose path drops a connection after ~30s of silence; a thinking
		// model can be quiet longer than that. One NDJSON line every 20s.
		const ka = setInterval(() => { try { res.write(JSON.stringify({ type: 'ping' }) + '\n'); } catch { clearInterval(ka); } }, 20_000);
		const stopKa = () => clearInterval(ka);
		res.on('close', stopKa); res.on('error', stopKa); res.on('finish', stopKa);
		return;
	}

	if (req.method === 'POST' && req.url === '/interrupt') {
		let b = '';
		for await (const c of req) { b += c; if (b.length > 4096) break; }
		let chatId = null; try { chatId = JSON.parse(b || '{}').chatId || null; } catch { /* corpo invalido = sem chat */ }
		const lane = chatId && lanes.get(chatId) ? lanes.get(chatId) : [...lanes.values()].find((l) => l.lastChatId && l.lastChatId === chatId) || null;
		// os turnos DESTE chat abertos agora (rodando ou na vez): o Worker cancela exatamente esses no
		// D1, qualquer que seja o status que eles tenham lá (um aviso de erro no meio do turno, um
		// callback que gravou antes) — o STOP tem que valer sempre
		const openPromptIds = openRelays.filter((r) => r.chatId === chatId && r.promptId && !r.done).map((r) => r.promptId);
		const out = lane ? await interruptLane(lane) : { lane: null, interrupted: false };
		out.promptIds = openPromptIds;
		res.writeHead(200, { 'Content-Type': 'application/json' });
		res.end(JSON.stringify({ ok: true, chatId, ...out }));
		return;
	}

	if (req.method !== 'POST' || req.url !== '/run') {
		res.writeHead(404, { 'Content-Type': 'application/json' });
		res.end(JSON.stringify({ error: 'not_found' }));
		return;
	}

	// First /run ever = the creator's (worker run.http at spawn) → PIN its key.
	if (pinnedApiKey === null) {
		pinnedApiKey = String(req.headers['x-api-key'] ?? '');
		if (!pinnedApiKey) logError('auth pin EMPTY — first /run had no x-api-key, auth is OPEN');
		else logInfo('auth pinned', { keyHash: tokenHash(pinnedApiKey) });
	}

	openRuns++;

	// Read body (capped at 1 MiB).
	const chunks = [];
	let bytes = 0;
	for await (const chunk of req) {
		bytes += chunk.length;
		if (bytes > 1_048_576) {
			res.writeHead(413).end();
			openRuns--;
			return;
		}
		chunks.push(chunk);
	}

	let envelope;
	try {
		envelope = JSON.parse(Buffer.concat(chunks).toString('utf8'));
	} catch (err) {
		res.writeHead(400, { 'Content-Type': 'application/json' });
		res.end(JSON.stringify({ error: 'invalid_json' }));
		openRuns--;
		return;
	}

	const mode = envelope.mode || 'cc-cli';

	// REBIND the user-stream identity every turn (never trust the frozen global).
	{ const c = envelope.chatId || (envelope.workspaceProxy && envelope.workspaceProxy.chatId) || null; if (c) streamChats.add(c); }
	streamPubKey = (typeof envelope.streamPubKey === 'string' && envelope.streamPubKey) ? Buffer.from(envelope.streamPubKey, 'base64') : null;

	// Open streaming response. Chunked transfer encoding by default in Node.
	res.writeHead(200, {
		'Content-Type': 'application/x-ndjson',
		'Cache-Control': 'no-cache, no-store',
		'X-Accel-Buffering': 'no',
	});

	// Per-turn relay: RETAIN (buffer for F5 replay) · SEND (fan-out to sinks) ·
	// SAVE (callback to the Worker on completion). Agent-agnostic — the claude
	// path below just feeds it. The /run caller is sink #0; /stream adds more.
	const relay = new TurnRelay({
		chatId: envelope.chatId || (envelope.workspaceProxy && envelope.workspaceProxy.chatId) || null,
		promptId: envelope.promptId || null,
		callback: envelope.callback && envelope.callback.url ? envelope.callback : null,
	});
	currentRelay = relay;
	openRelays.push(relay);
	relay.addSink(res);

	function emit(type, data) {
		relay.write(JSON.stringify({ type, data, ts: nowMs() }) + '\n');
	}

	// Always emit a leading 'ready' event so client knows server is alive.
	emit('ready', { mode, cwd: process.cwd() });

	// HEARTBEAT do /run (2026-09-26). O Cloudflare derruba conexao ~100s SEM bytes (doc do
	// Detona: "mande um heartbeat a cada <=30s"). Mensagem esperando a vez na fila e tool
	// longa (build, deploy) ficam mudas — no chat_795256c4 a 2a mensagem caiu com "Network
	// connection lost" 2 min depois de entrar na fila. So escreve em fronteira de linha
	// (o claude manda chunks crus; um ping no meio de uma linha corromperia o NDJSON).
	const heartbeat = setInterval(() => {
		if (relay.done) return;
		if (Date.now() - relay.lastWriteAt >= 20_000 && relay.aligned) emit('ping', {});
	}, 5_000);

	// A VEZ DO ENGINE (ver FILA NO ADAPTER). release() e idempotente e roda no ponto em que
	// o engine terminou (antes da espera por background) ou, no pior caso, no finally.
	// A fila e POR PISTA: na box compartilhada, chats diferentes rodam ao mesmo tempo; dentro de
	// um chat, a mensagem 2 espera a 1 (como sempre). Box propria = uma pista = a fila de antes.
	const lane = laneFor(envelope);
	const arrivedAt = nowMs();
	lane.openRuns++;
	lane.lastUsed = arrivedAt;
	lane.lastChatId = relay.chatId || lane.lastChatId;
	const prevEngine = lane.chain;
	let releaseEngine = () => {};
	lane.chain = new Promise((r) => { releaseEngine = r; });
	let engineReleased = false;
	const release = () => { if (engineReleased) return; engineReleased = true; lane.engineBusy = false; runsCompleted++; releaseEngine(); };
	if (lane.engineBusy) emit('phase', { name: 'queued_in_box', ts: nowMs() });
	await prevEngine;
	lane.engineBusy = true;
	// STOP chegou enquanto esta mensagem esperava a vez: o usuario cancelou a conversa ate aqui
	// (o Worker ja marcou os prompts como cancelled) — nao roda.
	const cancelledInQueue = lane.stopAt >= arrivedAt;
	// contexto do turno pra espera por background: janela [since, until) + sessao/marcador do engine
	const turnCtx = { sid: null, mark: null, since: uptimeTicks(), until: null, lane, startedAt: arrivedAt };
	if (lane.lastTurnCtx) lane.lastTurnCtx.until = turnCtx.since;
	lane.lastTurnCtx = turnCtx;

	// Dispatch.
	try {
		// Phase timestamps emitted as events for the worker-side benchmark
		// instrumentation. These are inner-container measurements that the
		// translator forwards as 'phase' events for telemetry only.
		const t_run_received = nowMs();
		emit('phase', { name: 'run_received', ts: t_run_received });

		if (cancelledInQueue && mode === 'cc-cli') {
			emit('phase', { name: 'cancelled_in_queue', ts: nowMs() });
			emit('error', { code: 'cancelled', message: 'turno cancelado pelo STOP antes de comecar', retryable: false });
		} else if (mode === 'cc-cli' && envelope.warmupOnly === true) {
			// WARMUP-ONLY (inline fork, 2026-07-07): boot claude — wipe stale sessions,
			// write the FULL cred pair, spawn, start the cred watcher — but send NO user
			// message. The Worker forks this box into the golden right after, so the
			// snapshot carries a booted+authed claude with a CLEAN session (no phantom
			// 'ok' — the old bake's warmup message leaked into every clone's history).
			// Liveness proof without inference: a set_model control ping to the SAME
			// model (no-op) — the ack proves the CLI is up and serving its stdin
			// protocol. Auth itself is exercised on first real inference (measured
			// ~52ms — the entire thing the 'ok' used to pre-pay).
			if (envelope.engine === 'codex') {
				// CODEX WARMUP: one-shot engine — nothing persistent to boot. We BAKE the
				// SUB auth (auth.json) OR the API key into the golden photo, with a CLEAN
				// session (the seed's throwaway volume — no chat data), so every clone of
				// this golden is codex-authed and ready. Accepts EITHER credential shape
				// (sub > key) — the standardization point: a new engine's warmup lives here.
				if (!envelope.codexAuth && !envelope.codexApiKey) throw new Error('codex warmup needs codexAuth (sub) or codexApiKey');
				const codexEnvW = { ...process.env, CODEX_HOME: lane.codexHome };
				try { fs.mkdirSync(lane.codexHome, { recursive: true, mode: 0o700 }); } catch {}
				if (envelope.codexAuth) {
					try { fs.writeFileSync(lane.codexAuthFile, typeof envelope.codexAuth === 'string' ? envelope.codexAuth : JSON.stringify(envelope.codexAuth), { mode: 0o600 }); } catch (e) { logError('codex warmup auth.json write failed', { msg: e && e.message }); }
				} else if (envelope.codexApiKey) {
					try { fs.rmSync(lane.codexAuthFile, { force: true }); } catch {}
					codexEnvW.CODEX_API_KEY = envelope.codexApiKey; codexEnvW.OPENAI_API_KEY = envelope.codexApiKey;
				}
				const ok = await new Promise((resolve) => {
					const p = spawn('codex', ['--version'], { env: codexEnvW, stdio: ['ignore', 'ignore', 'ignore'] });
					const t = setTimeout(() => { try { p.kill('SIGKILL'); } catch {} resolve(false); }, 15_000);
					p.on('exit', (code) => { clearTimeout(t); resolve(code === 0); });
					p.on('error', () => { clearTimeout(t); resolve(false); });
				});
				emit('phase', { name: 'claude_warmup_ready', ts: nowMs(), ack: ok, engine: 'codex' });
				if (!ok) emit('error', { code: 'warmup_no_ack', message: 'codex binary did not answer', retryable: true });
				return; // no claude state to set
			}
			if (!envelope.ccToken && !envelope.ccApiKey) throw new Error('ccToken or ccApiKey required for warmupOnly');
			if (!lane.proc || lane.proc.exitCode !== null || lane.sig !== coreSignature(envelope)) {
				if (lane.proc) await killClaude(lane, 'warmup_respawn');
				wipeClaudeState(lane);
				if (envelope.ccApiKey) {
					// API-key warmup (akey: goldens): key via spawn env, no creds file, no watcher.
					try { fs.rmSync(lane.credFile, { force: true }); } catch { /* best-effort */ }
					stopCredWatcher(lane);
				} else {
					configureCcAuth(lane, envelope.ccToken, envelope.ccRefreshToken, envelope.ccExpiresAt);
					startCredWatcher(lane, envelope.callback);
				}
				emit('phase', { name: 'pre_claude_spawn', ts: nowMs() });
				spawnPersistentClaude(lane, envelope, null); // define sig/model/tokenHash da pista
			}
			const alive = await trySetModel(lane, envelope.model || lane.model || 'claude-opus-4-8', 15_000);
			emit('phase', { name: 'claude_warmup_ready', ts: nowMs(), ack: alive });
			if (!alive) emit('error', { code: 'warmup_no_ack', message: 'claude did not ack the control ping', retryable: true });
		} else if (mode === 'codex-login') {
			// CODEX SUB CONNECT: run `codex login --device-auth` (the official CLI
			// passes OpenAI's WAF where our Worker gets 403). Stream the device code +
			// URL out so the Studio shows them; keep polling; when the user authorizes
			// in THEIR browser, the CLI writes auth.json → we mirror it to the Worker.
			await runCodexLogin(envelope, relay, emit);
			emit('phase', { name: 'codex_login_done', ts: nowMs() });
		} else if (mode === 'codex-models') {
			// EXTRACT the model catalog from codex ITSELF (its app-server `model/list`
			// RPC) — the source of truth, so new models appear with zero maintenance.
			// Runs authed (envelope.codexAuth) so the list is the account's entitled set.
			await runCodexModels(envelope, relay, emit);
			emit('phase', { name: 'codex_models_done', ts: nowMs() });
		} else if (mode === 'cc-cli' && envelope.engine === 'codex') {
			// CODEX ENGINE: same lane, different brain. One-shot per turn; the
			// adapter feeds claude-format events so everything downstream is shared.
			if (!envelope.prompt) throw new Error('prompt required for engine=codex');
			if (!envelope.codexAuth && !envelope.codexApiKey) throw new Error('codexAuth (sub) or codexApiKey required for engine=codex');
			await runCodexTurn(lane, envelope, relay, emit);
			turnCtx.sid = lane.sid; turnCtx.mark = lane.mark;
			release(); // o engine terminou: a proxima mensagem ja pode rodar
			relay.backgroundLeft = (await waitTurnQuiet(emit, envelope.backgroundWaitMs || 10 * 60 * 1000, turnCtx)).left.length;
			emit('phase', { name: 'post_claude_exit', ts: nowMs(), since_run_received_ms: nowMs() - t_run_received, engine: 'codex' });
		} else if (mode === 'cc-cli') {
			if (!envelope.prompt) throw new Error('prompt required for mode=cc-cli');
			if (!envelope.ccToken && !envelope.ccApiKey) throw new Error('ccToken or ccApiKey required for mode=cc-cli');

			// R2 workspace sync REMOVED (2026-07-07). It was disabled behind
			// `if (false && ...)` since the D1-priorContext model made it pure
			// latency, and the dead subsystem (~200 lines + rclone in the image)
			// only invited drift. If file sync ever returns, rebuild it against
			// the workspace-proxy JWT model (see git history for the old code).

			// Run claude. runPersistentClaude reuses the same process across
			//    turns when args + token match — only the first turn pays the
			//    ~20-25s OAuth handshake cost. Subsequent turns are ~2-3s.
			//    Credentials wipe + write happens INSIDE runPersistentClaude
			//    when (and only when) a respawn is needed.
			await runPersistentClaude(lane, envelope, relay, emit);
			turnCtx.sid = lane.sid; turnCtx.mark = lane.mark; turnCtx.engine = 'claude'; turnCtx.relay = relay;
			release(); // o engine terminou (result): a proxima mensagem ja pode rodar
			relay.backgroundLeft = (await waitTurnQuiet(emit, envelope.backgroundWaitMs || 10 * 60 * 1000, turnCtx)).left.length;
			if (lane.lingerTurn && lane.lingerTurn.relay === relay) lane.lingerTurn = null;
			emit('phase', { name: 'post_claude_exit', ts: nowMs(), since_run_received_ms: nowMs() - t_run_received });
		} else if (mode === 'slot' && envelope.warmupOnly === true) {
			// SLOT WARMUP (Fase 1): boot the client's server, NO turn — the Worker
			// forks this box right after, so the snapshot carries adapter + client
			// SDK already booted ("um cold só"). Mirror of the cc-cli warmup.
			if (!envelope.slot || !envelope.slot.cmd || !envelope.slot.port) throw new Error('slot {cmd, port} required for mode=slot');
			const up = await ensureSlotReady(envelope.slot, emit, 60_000);
			emit('phase', { name: 'slot_warmup_ready', ts: nowMs(), ack: up.ok, info: up.ready || up.err });
			if (!up.ok) emit('error', { code: 'warmup_no_ack', message: 'slot did not become ready', retryable: true });
		} else if (mode === 'slot') {
			if (!envelope.slot || !envelope.slot.cmd || !envelope.slot.port) throw new Error('slot {cmd, port} required for mode=slot');
			if (!envelope.prompt) throw new Error('prompt required for mode=slot');
			await runSlotTurn(envelope, relay, emit);
			emit('phase', { name: 'post_slot_exit', ts: nowMs(), since_run_received_ms: nowMs() - t_run_received });
		} else if (mode === 'build' || mode === 'exec') {
			if (!Array.isArray(envelope.cmd) || envelope.cmd.length === 0) {
				throw new Error(`cmd required for mode=${mode}`);
			}
			emit('build_start', { cmd: envelope.cmd });
			const exitCode = await runProcLogs(envelope.cmd[0], envelope.cmd.slice(1), envelope.env || {}, emit);
			emit('build_done', { exitCode });
		} else {
			emit('error', { code: 'mode_unknown', message: `unknown mode: ${mode}`, retryable: false });
		}
	} catch (err) {
		emit('error', {
			code: 'internal',
			message: err && err.message ? String(err.message) : 'unknown',
			retryable: false,
		});
	} finally {
		// Release the turn lock BEFORE closing the stream. relay.complete() ends the
		// `res` sink, and Detona's pauseAfter freezes the box the instant that stream
		// closes. If inFlight were still true at that moment, the box would FREEZE
		// with the lock held → on resume the next /run sees inFlight=true and returns
		// 409 'already_running' (the turn comes back empty). The turn is logically
		// done here — claude has finished; relay.complete is just persistence + close
		// — so clearing the lock first is correct AND makes the freeze always catch
		// the box idle. (This was the empty-2nd-message bug under pauseAfter.)
		openRuns--;
		release();
		lastRunAt = nowMs();
		clearInterval(heartbeat);
		// SAVE + close: complete the relay — ends every sink (incl. this res) and
		// fires the Worker callback to persist text+usage (the single D1 writer).
		// Awaited so the ephemeral container never drops the save. currentRelay is
		// left pointing at the finished relay (done=true): a late F5 hitting
		// /stream still gets the whole turn replayed from its buffer until the
		// next /run replaces it. Never let teardown throw.
		lane.openRuns--;
		lane.lastUsed = nowMs();
		if (lane.cred.report) { try { await lane.cred.report(); } catch {} }
		try {
			const out = await relay.complete(relay.pendingResult || {});
			if (out && out.ok === false) logError('save callback failed', { status: out.status, error: out.error });
		} catch (e) {
			logError('relay.complete threw', { msg: e && e.message });
		}
		const at = openRelays.indexOf(relay); if (at >= 0) openRelays.splice(at, 1);
		if (relay.promptId) { doneRelays.set(relay.promptId, relay); while (doneRelays.size > 8) doneRelays.delete(doneRelays.keys().next().value); }
	}
});

// (Legacy one-shot runProc removed — long-running flow uses runPersistentClaude.
// build/exec modes still use runProcLogs below for non-claude commands.)

/** Spawn for build/exec modes — stdout/stderr both wrapped as `log` events. */
function runProcLogs(command, args, env, emit) {
	return new Promise((resolve) => {
		const proc = spawn(command, args, {
			cwd: process.env.RUN_CWD || '/workspace',
			env: { ...process.env, ...env },
			stdio: ['ignore', 'pipe', 'pipe'],
		});
		const flushLines = (buf, isErr) => {
			let s = buf.toString('utf8');
			for (const line of s.split('\n')) {
				if (line.length === 0) continue;
				emit('log', { line, stream: isErr ? 'stderr' : 'stdout' });
			}
		};
		proc.stdout.on('data', (c) => flushLines(c, false));
		proc.stderr.on('data', (c) => flushLines(c, true));
		proc.on('error', (err) => {
			emit('error', { code: 'spawn_failed', message: err.message, retryable: false });
			resolve(1);
		});
		proc.on('exit', (code) => resolve(code ?? 1));
	});
}

server.listen(PORT, HOST, () => {
	logInfo('SmoothAgent runtime server listening', { port: PORT });
});

// (Idle watchdog de 35 min REMOVIDO 2026-09-26 — sobra da era Fly: numa box pausada ha
// mais de 35 min o tick de 60s podia cair entre o resume e o /run e matar o server. Quem
// pausa/destroi box e o Detona + o Worker.)

// Graceful shutdown — let inflight finish.
const onSig = (sig) => {
	logInfo('signal received', { sig });
	if (openRuns === 0) process.exit(0);
};
process.on('SIGTERM', onSig);
process.on('SIGINT', onSig);
