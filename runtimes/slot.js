'use strict';
/**
 * SLOT RUNTIME (SLOT_CONTRACT.md): o servidor de agente DO CLIENTE — processo persistente que fala
 * GET /ready + POST /agent-run em localhost. Gerido como o claude: sobe uma vez, fica entre turnos
 * e entra na foto do golden ("um cold só"). SNAPSHOT RULE: nada por turno mora aqui — só a
 * identidade do processo (cmd|port); prompt, segredos e contexto vão em cada /agent-run.
 */

const http = require('node:http');
const { spawn } = require('node:child_process');
const { nowMs, logInfo, logError, sleep } = require('../lib/log');

let slotProc = null;
let slotKey = null; // cmd|port com que o processo subiu

function slotFetch(port, path, opts, inactivityMs) {
	return new Promise((resolve, reject) => {
		const r = http.request({ host: '127.0.0.1', port, path, method: (opts && opts.method) || 'GET', headers: (opts && opts.headers) || {} }, resolve);
		r.on('error', reject);
		// inatividade do socket, não teto total: SDK lento que streama segue vivo; um morto é colhido
		r.setTimeout(inactivityMs || 5_000, () => r.destroy(new Error('slot inactivity timeout')));
		if (opts && opts.body) r.write(opts.body);
		r.end();
	});
}

/** Sobe o slot se faltar/morreu/mudou a config e espera o /ready. */
async function ensureSlotReady(slot, emit, bootTimeoutMs) {
	const key = `${slot.cmd}|${slot.port}`;
	if (!slotProc || slotProc.exitCode !== null || slotKey !== key) {
		if (slotProc && slotProc.exitCode === null) { try { slotProc.kill('SIGTERM'); } catch { /* já saiu */ } }
		emit('phase', { name: 'slot_spawn', ts: nowMs() });
		slotProc = spawn('/bin/sh', ['-c', slot.cmd], { cwd: process.env.RUN_CWD || '/workspace', env: { ...process.env, SLOT_PORT: String(slot.port) }, stdio: ['ignore', 'pipe', 'pipe'] });
		slotProc.stdout.on('data', (d) => logInfo('slot stdout', { line: String(d).slice(0, 200) }));
		slotProc.stderr.on('data', (d) => logError('slot stderr', { line: String(d).slice(0, 200) }));
		slotKey = key;
	}
	const deadline = nowMs() + (bootTimeoutMs || 60_000);
	let lastErr = 'not_attempted';
	while (nowMs() < deadline) {
		try {
			const r = await slotFetch(slot.port, '/ready', {}, 2_000);
			if (r.statusCode === 200) { let body = ''; for await (const c of r) body += c; return { ok: true, ready: body.slice(0, 200) }; }
			lastErr = `http_${r.statusCode}`;
			r.resume();
		} catch (e) { lastErr = e && e.message ? e.message : 'x'; }
		await sleep(150);
	}
	return { ok: false, ready: null, err: lastErr };
}

async function warmupSlot(envelope, emit) {
	if (!envelope.slot || !envelope.slot.cmd || !envelope.slot.port) throw new Error('slot {cmd, port} required for mode=slot');
	const up = await ensureSlotReady(envelope.slot, emit, 60_000);
	emit('phase', { name: 'slot_warmup_ready', ts: nowMs(), ack: up.ok, info: up.ready || up.err });
	if (!up.ok) emit('error', { code: 'warmup_no_ack', message: 'slot did not become ready', retryable: true });
}

/** Um turno no slot: POST /agent-run e repassa o NDJSON dele. */
async function runSlotTurn(envelope, relay, emit) {
	if (!envelope.slot || !envelope.slot.cmd || !envelope.slot.port) throw new Error('slot {cmd, port} required for mode=slot');
	if (!envelope.prompt) throw new Error('prompt required for mode=slot');
	const slot = envelope.slot;
	const up = await ensureSlotReady(slot, emit, 60_000);
	emit('phase', { name: 'slot_ready', ts: nowMs(), ack: up.ok, info: up.ready || up.err });
	if (!up.ok) { emit('error', { code: 'slot_not_ready', message: String(up.err), retryable: true }); return; }
	const body = JSON.stringify({
		prompt: envelope.prompt || '', context: envelope.priorContext || '', secrets: envelope.secrets || {},
		sessionId: envelope.sessionId || envelope.chatId || null, meta: { chatId: envelope.chatId || null, promptId: envelope.promptId || null },
	});
	const resp = await slotFetch(slot.port, '/agent-run', { method: 'POST', headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) }, body }, 300_000);
	emit('phase', { name: 'slot_run_started', ts: nowMs() });
	let buf = ''; let sawFirst = false;
	for await (const chunk of resp) {
		buf += chunk;
		let i;
		while ((i = buf.indexOf('\n')) >= 0) {
			const line = buf.slice(0, i).trim();
			buf = buf.slice(i + 1);
			if (!line) continue;
			if (!sawFirst) { sawFirst = true; emit('phase', { name: 'slot_first_byte', ts: nowMs() }); }
			let ev = null;
			try { ev = JSON.parse(line); } catch { continue; } // contrato: só NDJSON
			if (ev.type === 'text') emit('text', { text: String(ev.data ?? '') });
			else if (ev.type === 'result') {
				const d = ev.data || {};
				relay.pendingResult = { text: d.text || '', usage: d.usage || null, sessionId: envelope.sessionId || null, context: typeof d.context === 'string' ? d.context : null };
				emit('result', { text: d.text || '', usage: d.usage || null, context: typeof d.context === 'string' ? d.context : null });
			} else if (ev.type === 'error') emit('error', { code: 'slot_error', message: String((ev.data && ev.data.message) || 'slot error'), retryable: false });
		}
	}
	emit('phase', { name: 'post_slot_exit', ts: nowMs() });
}

module.exports = { runSlotTurn, warmupSlot };
