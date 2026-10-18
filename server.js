#!/usr/bin/env node
'use strict';
/**
 * SmoothAgent runtime HTTP server (port 8080) — the box-side adapter.
 *
 * Roda PERSISTENTE numa microVM do Detona (física de snapshot/resume: tudo que mora em variável do
 * processo congela no golden — ver SNAPSHOT TRAP em lib/worker.js e lib/credential-watch.js).
 *
 * ROTAS
 *   GET  /health     — aberto: { inFlight, busyChats } (o Worker pausa a box só com tudo quieto)
 *   GET  /stream     — reattach no turno (replay + ao vivo); chave da box OU token do usuário
 *   POST /run        — um envelope: mensagem de conversa, warmup, one-shot do codex, slot, exec
 *   POST /interrupt  — STOP de um chat: o interrupt NATIVO do provedor dele
 *
 * MODOS (envelope.mode)
 *   cc-cli      — conversa com um PROVEDOR (engines/: claude, codex). A box NÃO enfileira nem
 *                 ordena: a mensagem vai na hora pro engine e ele decide (ver engines/engine.js).
 *   codex-login / codex-models — one-shots do codex (engines/codex-tools.js)
 *   slot        — o servidor do cliente (runtimes/slot.js)
 *   build/exec  — comando simples (prep de imagem, smoke do canário)
 *
 * Cada /run tem o próprio TurnRelay (relay.js): guarda tudo pro reattach, entrega pros sinks e,
 * no fim, SALVA via callback do Worker. Mensagem que entrou no turno de outra recebe `merged` e
 * fecha; a resposta segue no stream (e no salvamento) da dona do turno.
 */

const http = require('node:http');
const { spawn } = require('node:child_process');
const { TurnRelay } = require('./relay');
const { nowMs, logInfo, logError, sleep } = require('./lib/log');
const { postToWorker } = require('./lib/worker');
const { lanes, laneFor, boxState } = require('./lib/lane');
const { waitTurnQuiet } = require('./lib/turn-quiet');
const relays = require('./lib/relays');
const auth = require('./lib/auth');
const { Message } = require('./engines/engine');
const { engineKindOf } = require('./engines');
const { runCodexLogin, runCodexModels } = require('./engines/codex-tools');
const { runSlotTurn, warmupSlot } = require('./runtimes/slot');

const PORT = 8080;
const HOST = '0.0.0.0';
const BACKGROUND_CAP_MS = 10 * 60 * 1000;
const BODY_MAX = 1_048_576;
let openRuns = 0; // /run abertos — /health inFlight

const json = (res, status, body) => { res.writeHead(status, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(body)); };
const NDJSON_HEADERS = { 'Content-Type': 'application/x-ndjson', 'Cache-Control': 'no-cache, no-store', 'X-Accel-Buffering': 'no' };

async function readBody(req, max) {
	const chunks = []; let n = 0;
	for await (const c of req) { n += c.length; if (n > max) return null; chunks.push(c); }
	return Buffer.concat(chunks).toString('utf8');
}

/** A pista de um chat (a dele na box compartilhada, ou a única da box própria). */
function laneOfChat(chatId) {
	if (!chatId) return null;
	return lanes.get(chatId) || [...lanes.values()].find((l) => l.lastChatId === chatId) || null;
}

// ─── conversa: uma mensagem pro provedor ─────────────────────────────────────────────────
/**
 * Espera o engine dizer o que fez com a mensagem. Guarda-chuva: engine vivo, sem turno e sem ler
 * a mensagem por muito tempo = algo quebrou (nunca deixar um /run pendurado pra sempre).
 */
async function attachment(engine, message) {
	let stalledSince = null;
	for (;;) {
		const r = await Promise.race([message.attached.then((v) => ({ v })), sleep(5000).then(() => null)]);
		if (r) return r.v;
		if (engine.stalled && engine.stalled()) {
			stalledSince = stalledSince || nowMs();
			if (nowMs() - stalledSince > 60_000) throw Object.assign(new Error('o engine não leu a mensagem'), { code: 'engine_stalled' });
		} else stalledSince = null;
	}
}

async function runMessage(lane, envelope, relay, emit) {
	if (!envelope.prompt) throw new Error('prompt required for mode=cc-cli');
	const kind = engineKindOf(envelope);
	const cap = envelope.backgroundWaitMs || BACKGROUND_CAP_MS;
	// UMA conversa, um engine por vez: trocar de provedor espera o outro terminar o que faz
	const other = lane.busyEngine(kind);
	if (other) await other.waitIdle(emit, cap, 'engine_switch');
	for (const e of lane.engines.values()) if (e.kind !== kind && e.alive()) await e.close('engine_switch');

	const engine = lane.engine(kind);
	await engine.prepare(envelope, emit);
	const message = new Message({ envelope, relay, emit });
	Promise.resolve().then(() => engine.submit(message)).catch((err) => message._fail(err));
	const { role, turn } = await attachment(engine, message);

	if (role === 'merged') {
		// o provedor juntou esta mensagem ao turno que já rodava: a resposta sai no turno da dona
		emit('merged', { intoPromptId: turn.owner.promptId, engine: kind });
		return;
	}
	emit('started', { engine: kind });
	await turn.ended;
	// TURNO ATÉ O TRABALHO ACABAR: só fecha quando nada que ele começou ainda roda
	const info = engine.processInfo();
	const quiet = await waitTurnQuiet(emit, cap, {
		sid: info.sid, mark: info.mark, since: turn.since, get until() { return turn.until; },
		exceptPid: info.pid, useCgroup: lanes.size <= 1, working: () => engine.workInEngine(turn),
	});
	relay.backgroundLeft = quiet.left.length;
	// quem entrou no turno (até durante a continuação) vai junto no salvamento da dona
	if (relay.pendingResult && turn.merged.length) relay.pendingResult.mergedPromptIds = turn.merged.map((m) => m.promptId).filter(Boolean);
	emit('phase', { name: 'post_claude_exit', ts: nowMs(), engine: kind });
}

async function runWarmup(lane, envelope, emit) {
	const kind = engineKindOf(envelope);
	const { ok } = await lane.engine(kind).warmup(envelope, emit);
	emit('phase', { name: 'claude_warmup_ready', ts: nowMs(), ack: ok, engine: kind });
	if (!ok) emit('error', { code: 'warmup_no_ack', message: `${kind} did not answer the warmup`, retryable: true });
}

/** build/exec: comando simples, stdout/stderr como eventos `log`. */
function runProcLogs(command, args, env, emit) {
	return new Promise((resolve) => {
		const proc = spawn(command, args, { cwd: process.env.RUN_CWD || '/workspace', env: { ...process.env, ...env }, stdio: ['ignore', 'pipe', 'pipe'] });
		const lines = (buf, isErr) => { for (const line of buf.toString('utf8').split('\n')) if (line) emit('log', { line, stream: isErr ? 'stderr' : 'stdout' }); };
		proc.stdout.on('data', (c) => lines(c, false));
		proc.stderr.on('data', (c) => lines(c, true));
		proc.on('error', (err) => { emit('error', { code: 'spawn_failed', message: err.message, retryable: false }); resolve(1); });
		proc.on('exit', (code) => resolve(code ?? 1));
	});
}

// ─── POST /run ───────────────────────────────────────────────────────────────────────────
async function handleRun(req, res) {
	auth.pinIfFirst(req);
	openRuns++;
	const raw = await readBody(req, BODY_MAX);
	if (raw == null) { openRuns--; res.writeHead(413).end(); return; }
	let envelope;
	try { envelope = JSON.parse(raw); } catch { openRuns--; json(res, 400, { error: 'invalid_json' }); return; }
	const mode = envelope.mode || 'cc-cli';
	auth.rebindStream(envelope);

	res.writeHead(200, NDJSON_HEADERS);
	const relay = new TurnRelay({
		chatId: envelope.chatId || (envelope.workspaceProxy && envelope.workspaceProxy.chatId) || null,
		promptId: envelope.promptId || null,
		callback: envelope.callback && envelope.callback.url ? envelope.callback : null,
	});
	relays.opened(relay);
	relay.addSink(res);
	const emit = (type, data) => relay.write(JSON.stringify({ type, data, ts: nowMs() }) + '\n');
	emit('ready', { mode, cwd: process.cwd() });
	// HEARTBEAT: o Cloudflare derruba conexão muda ~100s. Só em fronteira de linha.
	const heartbeat = setInterval(() => { if (!relay.done && Date.now() - relay.lastWriteAt >= 20_000 && relay.aligned) emit('ping', {}); }, 5_000);

	const lane = laneFor(envelope);
	lane.openRuns++;
	lane.lastUsed = nowMs();
	lane.lastChatId = relay.chatId || lane.lastChatId;
	try {
		emit('phase', { name: 'run_received', ts: nowMs() });
		if (mode === 'cc-cli' && envelope.warmupOnly === true) await runWarmup(lane, envelope, emit);
		else if (mode === 'cc-cli') await runMessage(lane, envelope, relay, emit);
		else if (mode === 'codex-login') { await runCodexLogin(envelope, relay, emit); emit('phase', { name: 'codex_login_done', ts: nowMs() }); }
		else if (mode === 'codex-models') { await runCodexModels(envelope, relay, emit); emit('phase', { name: 'codex_models_done', ts: nowMs() }); }
		else if (mode === 'slot' && envelope.warmupOnly === true) await warmupSlot(envelope, emit);
		else if (mode === 'slot') await runSlotTurn(envelope, relay, emit);
		else if (mode === 'build' || mode === 'exec') {
			if (!Array.isArray(envelope.cmd) || !envelope.cmd.length) throw new Error(`cmd required for mode=${mode}`);
			emit('build_start', { cmd: envelope.cmd });
			emit('build_done', { exitCode: await runProcLogs(envelope.cmd[0], envelope.cmd.slice(1), envelope.env || {}, emit) });
		} else emit('error', { code: 'mode_unknown', message: `unknown mode: ${mode}`, retryable: false });
	} catch (err) {
		emit('error', { code: (err && err.code) || 'internal', message: err && err.message ? String(err.message) : 'unknown', retryable: false });
	} finally {
		// libera o "em voo" ANTES de fechar o stream: a box pode ser pausada no instante em que ele
		// fecha, e congelar com o contador preso fazia o próximo /run achar que havia turno rodando
		openRuns--;
		lane.openRuns--;
		lane.lastUsed = nowMs();
		boxState.turnCompleted();
		clearInterval(heartbeat);
		// rotação de credencial ANTES do salvamento (refresh token de uso único)
		for (const e of lane.engines.values()) { try { if (e.flushCredentials) await e.flushCredentials(); } catch { /* nunca derruba */ } }
		try {
			const out = await relay.complete(relay.pendingResult || {});
			if (out && out.ok === false) logError('save callback failed', { status: out.status, error: out.error });
		} catch (e) { logError('relay.complete threw', { msg: e && e.message }); }
		relays.closed(relay);
	}
}

// ─── POST /interrupt (STOP de um chat) ───────────────────────────────────────────────────
async function handleInterrupt(req, res) {
	const raw = await readBody(req, 4096);
	let chatId = null;
	try { chatId = JSON.parse(raw || '{}').chatId || null; } catch { /* sem chat */ }
	const lane = laneOfChat(chatId);
	let out = { interrupted: false, promptIds: [] };
	if (lane) for (const e of lane.engines.values()) if (e.busy()) { out = await e.interrupt(); break; }
	json(res, 200, { ok: true, chatId, lane: lane ? lane.key : null, ...out });
}

// ─── GET /stream (reattach) ──────────────────────────────────────────────────────────────
function handleStream(req, res) {
	const q = req.url.indexOf('?');
	const qs = q >= 0 ? new URLSearchParams(req.url.slice(q + 1)) : null;
	const target = relays.relayFor(qs && qs.get('promptId'), req.streamChat || (qs && qs.get('chatId')) || null);
	if (!target) { res.writeHead(204).end(); return; }
	res.writeHead(200, NDJSON_HEADERS);
	target.addSink(res);
	// KEEPALIVE: o expose do Detona derruba conexão muda ~30s (sinks de /stream só)
	const ka = setInterval(() => { try { res.write(JSON.stringify({ type: 'ping' }) + '\n'); } catch { clearInterval(ka); } }, 20_000);
	const stop = () => clearInterval(ka);
	res.on('close', stop); res.on('error', stop); res.on('finish', stop);
}

const server = http.createServer(async (req, res) => {
	if (req.method === 'GET' && (req.url === '/health' || req.url === '/')) {
		// aberto (probes de prontidão). busyChats: chats com /run aberto (rodando ou segurando background)
		json(res, 200, { status: 'ok', inFlight: openRuns > 0, busyChats: [...lanes.values()].filter((l) => l.openRuns > 0).map((l) => l.lastChatId || l.key) });
		return;
	}
	const isStreamGet = req.method === 'GET' && (req.url === '/stream' || req.url.startsWith('/stream?'));
	if (auth.pinned() && !auth.apiKeyMatches(req)) {
		const why = isStreamGet ? auth.userStreamTokenWhy(req) : 'not_stream';
		if (why !== null) { json(res, 401, isStreamGet ? { error: 'unauthorized', why } : { error: 'unauthorized' }); return; }
	}
	try {
		if (isStreamGet) return handleStream(req, res);
		if (req.method === 'POST' && req.url === '/interrupt') return await handleInterrupt(req, res);
		if (req.method === 'POST' && req.url === '/run') return await handleRun(req, res);
		json(res, 404, { error: 'not_found' });
	} catch (e) {
		logError('request failed', { url: req.url, msg: e && e.message });
		try { if (!res.headersSent) json(res, 500, { error: 'internal' }); else res.end(); } catch { /* fechado */ }
	}
});

// TURNO VIVO = BOX NÃO OCIOSA. O idle-pause do Detona não enxerga run.http aberto (busy:false e o
// idle contando): turno de mais de 30 min congelava com a aba aberta. A box não fala com o Detona;
// avisa o Worker (JWT do callback do turno) e o Worker zera o idle.
setInterval(() => {
	if (openRuns <= 0) return;
	const r = relays.firstOpen();
	if (!r || !r.callback) return;
	postToWorker(r.callback, '/internal/turn-alive', { chatId: r.chatId, promptId: r.promptId })
		.then((x) => { if (!x.ok) logError('turn-alive recusado', { status: x.status }); });
}, 4 * 60 * 1000);

server.listen(PORT, HOST, () => logInfo('SmoothAgent runtime server listening', { port: PORT }));

const onSig = (sig) => { logInfo('signal received', { sig }); if (openRuns === 0) process.exit(0); };
process.on('SIGTERM', onSig);
process.on('SIGINT', onSig);
