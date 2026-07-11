#!/usr/bin/env node
/**
 * SmoothAgent runtime HTTP server (port 8080).
 *
 * Single-purpose: receives one POST /run with an envelope, spawns claude
 * (or the customer-configured agent), pipes stdout chunks back to the
 * client in real-time via HTTP chunked transfer encoding, then exits so
 * the Fly machine auto-destroys.
 *
 * Why HTTP server vs Fly's REST exec:
 *   Fly REST exec is buffered — it waits for the command to exit before
 *   returning stdout. claude emits stream-json events as it generates each
 *   token, but with REST exec the user sees nothing until the whole turn
 *   completes. With this server, each chunk claude emits is forwarded to
 *   the wire immediately — true streaming.
 *
 * Token handling:
 *   ccToken arrives in POST body (memory only). Written to a 0600 mode
 *   credentials file under $HOME/.claude/. Never logged, never echoed,
 *   never persisted on volume (lives in tmpfs of HOME).
 *
 * Lifecycle:
 *   server.listen on 0.0.0.0:8080 → /run handles ONE request → process.exit
 *   when claude exits. Fly auto_destroy=true cleans up the machine.
 *   If a second /run arrives during the first, it's rejected 409.
 */

const http = require('node:http');
const { spawn } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { TurnRelay } = require('./relay');

const PORT = 8080;
const HOST = '0.0.0.0';

let inFlight = false;
/** The current turn's relay (retain/send/save). GET /stream attaches to it. */
let currentRelay = null;

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

/** Currently-running claude process. Null when no claude has been spawned (or it crashed/was killed). */
let claudeProc = null;
/** Core signature (systemPrompt+maxTurns+mcpConfig) the running claude was started with. */
let claudeSig = null;
/** Model the running claude is CURRENTLY on (spawn arg, or last in-flight set_model). */
let claudeModel = null;
/** Last ccToken written to .credentials.json. A new turn with a different token is
 *  swapped in place (credentials rewritten, process reused) — NOT a respawn. */
let claudeTokenHash = null;
/** Per-turn handler binding — null when no /run is in flight. */
let activeTurn = null;
/** Rolling stdout buffer so we can split partial NDJSON lines across data chunks. */
let stdoutLineBuffer = '';
/** Time of last /run completion (for the idle-exit watchdog). */
let lastRunAt = Date.now();
// ---------------------------------------------------------------------------
// R2 workspace sync subsystem REMOVED 2026-07-07 (was ~220 dead lines behind
// `if (false && proxy)` — see git history). rclone left the image with it.

function nowMs() {
	return Date.now();
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
function wipeClaudeState() {
	// HOME for the `agent` user is /workspace (Dockerfile useradd --home-dir),
	// so ~/.claude resolves into the persistent Fly Volume / tmpfs.
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
	const homeDir = process.env.HOME || '/workspace';
	const projectsDir = path.join(homeDir, '.claude', 'projects');
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
function configureCcAuth(token, refreshToken, expiresAt) {
	const homeDir = process.env.HOME || '/workspace';
	const ccDir = path.join(homeDir, '.claude');
	fs.mkdirSync(ccDir, { recursive: true, mode: 0o700 });
	fs.writeFileSync(
		CRED_FILE_PATH(),
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
	credFileLastReported = credFileFingerprint();
}

function CRED_FILE_PATH() {
	return path.join(process.env.HOME || '/workspace', '.claude', '.credentials.json');
}

/** Cheap change detector for the credentials file (never logs contents). */
function credFileFingerprint() {
	try {
		const raw = fs.readFileSync(CRED_FILE_PATH(), 'utf8');
		return crypto.createHash('sha256').update(raw).digest('hex');
	} catch { return ''; }
}

let credFileLastReported = '';
let credWatcherStarted = false;

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
let credWatcherCallback = null;
function startCredWatcher(callback) {
	if (callback && callback.url) credWatcherCallback = callback; // rebind EVERY turn — see snapshot trap above
	if (credWatcherStarted || !credWatcherCallback) return;
	credWatcherStarted = true;
	const report = async () => {
		try {
			const cb = credWatcherCallback;
			if (!cb || !cb.url) return;
			const fp = credFileFingerprint();
			if (!fp || fp === credFileLastReported) return;
			const cred = JSON.parse(fs.readFileSync(CRED_FILE_PATH(), 'utf8')).claudeAiOauth || {};
			if (!cred.accessToken) return;
			credFileLastReported = fp;
			const url = cb.url.replace(/\/internal\/chat-result$/, '/internal/cc-credentials');
			const r = await fetch(url, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json', ...(cb.jwt ? { Authorization: `Bearer ${cb.jwt}` } : {}) },
				body: JSON.stringify({ credentials: { accessToken: cred.accessToken, refreshToken: cred.refreshToken || null, expiresAt: cred.expiresAt || null } }),
			});
			// A failed report must NOT be marked as reported — the post-turn sweep
			// (next turn, fresh jwt) retries it. Losing a rotation kills the pair.
			if (!r.ok) credFileLastReported = null;
			logInfo('cred rotation reported', { status: r.status });
		} catch (e) {
			credFileLastReported = null; // retry on the next sweep
			logError('cred report failed', { msg: e && e.message });
		}
	};
	try {
		fs.watch(path.dirname(CRED_FILE_PATH()), { persistent: false }, (_evt, fname) => {
			if (fname === '.credentials.json') setTimeout(report, 150); // debounce partial writes
		});
	} catch (e) { logError('cred watcher failed to start', { msg: e && e.message }); }
	credWatcherReport = report;
}
let credWatcherReport = null;

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
function buildClaudeArgs(envelope) {
	const args = [
		'--output-format', 'stream-json',
		'--input-format', 'stream-json',
		'--verbose',
		'--include-partial-messages',
		'--permission-mode', 'bypassPermissions',
	];
	if (envelope.model) args.push('--model', envelope.model);
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
		systemPrompt: envelope.systemPrompt || null,
		maxTurns: envelope.maxTurns || null,
		mcpConfig: envelope.mcpConfig || null,
	});
}

/** Pending control_request acks (request_id → resolve). See trySetModel. */
const controlWaiters = new Map();

/**
 * Ask the running claude to switch model in-flight (stream-json control channel).
 * Resolves true on an acked success, false on error/timeout — caller falls back
 * to the legacy kill+respawn, so an older CLI that ignores the subtype only costs
 * `timeoutMs` once per model switch, never correctness.
 */
function trySetModel(model, timeoutMs) {
	return new Promise((resolve) => {
		if (!claudeProc || claudeProc.exitCode !== null) return resolve(false);
		const id = `sm_${Date.now()}_${Math.floor(Math.random() * 1e6)}`;
		const timer = setTimeout(() => { controlWaiters.delete(id); resolve(false); }, timeoutMs);
		controlWaiters.set(id, (resp) => {
			clearTimeout(timer);
			controlWaiters.delete(id);
			const sub = resp && resp.response ? resp.response.subtype : undefined;
			resolve(sub === 'success' || (!!resp && !resp.error && sub !== 'error'));
		});
		try {
			claudeProc.stdin.write(JSON.stringify({ type: 'control_request', request_id: id, request: { subtype: 'set_model', model } }) + '\n');
		} catch {
			clearTimeout(timer);
			controlWaiters.delete(id);
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
async function killClaude(reason) {
	if (!claudeProc) return;
	logInfo('killing persistent claude', { reason, pid: claudeProc.pid });
	const proc = claudeProc;
	claudeProc = null;
	claudeSig = null;
	claudeModel = null;
	claudeTokenHash = null;
	stdoutLineBuffer = '';
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
function spawnPersistentClaude(envelope) {
	const args = buildClaudeArgs(envelope);
	const proc = spawn('claude', args, {
		cwd: process.env.RUN_CWD || '/workspace',
		env: process.env,
		stdio: ['pipe', 'pipe', 'pipe'],
	});
	logInfo('persistent claude spawned', { pid: proc.pid, args: args.length });

	proc.stdout.on('data', handleClaudeStdout);

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
		// If a turn was waiting on this process, surface the failure.
		if (activeTurn) {
			try {
				activeTurn.emit('error', {
					code: 'exit_nonzero',
					message: `claude exited ${code}${tail ? `\nstderr (tail): ${tail.slice(-512)}` : ''}`,
					retryable: false,
				});
				activeTurn.resolve();
			} catch {}
			activeTurn = null;
		}
		// Clear top-level state — next /run will respawn fresh.
		if (claudeProc === proc) {
			claudeProc = null;
			claudeSig = null;
			claudeModel = null;
			claudeTokenHash = null;
			stdoutLineBuffer = '';
		}
	});

	claudeProc = proc;
	claudeSig = coreSignature(envelope);
	claudeModel = envelope.model || null;
	claudeTokenHash = tokenHash(envelope.ccToken);
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
function handleClaudeStdout(chunk) {
	// control_response acks (e.g. set_model) can arrive BETWEEN turns — resolve
	// waiters before the activeTurn gate or they'd be dropped and time out.
	if (controlWaiters.size > 0) {
		for (const line of chunk.toString('utf8').split('\n')) {
			const s = line.trim();
			if (!s || s.indexOf('control_response') === -1) continue;
			try {
				const obj = JSON.parse(s);
				const rid = (obj.response && obj.response.request_id) || obj.request_id;
				const w = rid && controlWaiters.get(rid);
				if (w) w(obj);
			} catch { /* partial line — the waiter's timeout covers it */ }
		}
	}
	if (!activeTurn) return;
	const t = activeTurn;

	// Phase: first stdout in this turn
	if (!t.firstStdoutSeen) {
		t.firstStdoutSeen = true;
		t.emit('phase', { name: 'claude_first_stdout', ts: nowMs(), since_spawn_call_ms: nowMs() - t.t_spawn });
	}

	// Forward raw chunk through the relay (buffers for F5 replay + fans out to
	// every live sink — preserves frame alignment for the translator).
	t.relay.write(chunk);

	// Parse line-by-line for terminal/phase detection.
	stdoutLineBuffer += chunk.toString('utf8');
	let nl;
	while ((nl = stdoutLineBuffer.indexOf('\n')) >= 0) {
		const line = stdoutLineBuffer.slice(0, nl).trim();
		stdoutLineBuffer = stdoutLineBuffer.slice(nl + 1);
		if (!line) continue;
		let obj;
		try { obj = JSON.parse(line); } catch { continue; }
		if (!obj || typeof obj !== 'object') continue;
		const type = obj.type;

		if (!t.firstSystemSeen && type === 'system') {
			t.firstSystemSeen = true;
			t.emit('phase', { name: 'claude_first_system', ts: nowMs(), since_spawn_call_ms: nowMs() - t.t_spawn });
		}
		if (!t.firstAssistantSeen && (type === 'assistant' || type === 'text' || type === 'content_block_start')) {
			t.firstAssistantSeen = true;
			t.emit('phase', { name: 'claude_first_assistant', ts: nowMs(), since_spawn_call_ms: nowMs() - t.t_spawn });
		}
		if (type === 'result') {
			// Claude finished. Capture the final text + usage for the SAVE
			// callback, but don't complete the relay yet — sync_up + post_claude
			// events still stream after this. complete() fires at end of /run.
			const turn = activeTurn;
			activeTurn = null;
			turn.relay.pendingResult = {
				text: typeof obj.result === 'string' ? obj.result : '',
				usage: obj.usage || null,
			};
			turn.resolve();
		}
	}
}

/**
 * Drive one /run against the persistent claude process.
 *
 * Spawns claude if it's not running OR if the session args / token have
 * changed since last turn. Otherwise reuses the existing process — that's
 * where the time savings come from.
 */
async function runPersistentClaude(envelope, relay, emit) {
	const newCore = coreSignature(envelope);
	const newModel = envelope.model || null;
	const newTokenHash = tokenHash(envelope.ccToken);

	// MODEL SWAP (not respawn): the model is a per-request API parameter — same CC,
	// same context, any model. When ONLY the model differs, ask the running claude to
	// switch in-flight (control_request set_model, the same channel interactive
	// /model uses). Ack → warm reuse on the new model; no ack (older CLI) → the
	// mismatch below falls through to the legacy respawn. Worst case = one 2s
	// timeout per switch; best case kills the model-switch respawn entirely.
	if (
		claudeProc && claudeProc.exitCode === null &&
		claudeSig === newCore && claudeModel !== newModel &&
		envelope.forceRespawn !== true
	) {
		const swapped = await trySetModel(newModel, 2000);
		if (swapped) {
			claudeModel = newModel;
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
		!claudeProc ||
		claudeProc.exitCode !== null ||
		claudeSig !== newCore ||
		claudeModel !== newModel ||
		envelope.forceRespawn === true;

	if (needsRespawn) {
		if (claudeProc) {
			emit('phase', { name: 'claude_respawn', ts: nowMs(), reason: envelope.forceRespawn === true ? 'post_resume' : (claudeSig !== newCore ? 'args_changed' : (claudeModel !== newModel ? 'model_swap_failed' : (claudeTokenHash !== newTokenHash ? 'token_rotated' : 'dead'))) });
			await killClaude('respawn');
		}
		// Refresh credentials.json on disk and (only on first ever turn) wipe
		// stale session lockfiles. Token may have rotated even when session
		// args are the same, so always rewrite credentials before spawn.
		wipeClaudeState();
		configureCcAuth(envelope.ccToken, envelope.ccRefreshToken, envelope.ccExpiresAt);
		startCredWatcher(envelope.callback);
		const t_spawn_called = nowMs();
		emit('phase', { name: 'pre_claude_spawn', ts: t_spawn_called });
		spawnPersistentClaude(envelope);
		emit('phase', { name: 'claude_spawned', ts: nowMs(), since_spawn_call_ms: nowMs() - t_spawn_called });
		activeTurn = {
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
		if (claudeTokenHash !== newTokenHash) {
			configureCcAuth(envelope.ccToken, envelope.ccRefreshToken, envelope.ccExpiresAt);
			claudeTokenHash = newTokenHash;
			emit('phase', { name: 'claude_token_swapped', ts: nowMs() });
		}
		startCredWatcher(envelope.callback);
		emit('phase', { name: 'claude_reused', ts: nowMs() });
		activeTurn = {
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
	// not fold twice). Warm reuse skips: claude remembers natively.
	let promptContent = envelope.prompt;
	if (needsRespawn && envelope.priorContext && !String(promptContent).includes('## Mensagem atual do usuário')) {
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
		claudeProc.stdin.write(JSON.stringify(userEvent) + '\n');
	} catch (err) {
		emit('error', { code: 'internal', message: `stdin write failed: ${err && err.message}`, retryable: true });
		activeTurn = null;
		return;
	}

	// Wait for the 'result' event (set by handleClaudeStdout) or process exit.
	await new Promise((resolve) => {
		if (activeTurn) activeTurn.resolve = resolve;
		else resolve(); // already settled by an exit
	});
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

const server = http.createServer(async (req, res) => {
	if (req.method === 'GET' && (req.url === '/health' || req.url === '/')) {
		// Open on purpose: static liveness for Detona's build/readiness probes.
		res.writeHead(200, { 'Content-Type': 'application/json' });
		res.end(JSON.stringify({ status: 'ok', inFlight }));
		return;
	}

	// Past this point everything is sensitive (/run runs claude, /stream replays
	// the whole turn). Once a key is pinned, demand it. Before any pin exists the
	// box has nothing to protect (no turn ever ran) — /stream 204s below and
	// POST /run performs the pinning.
	if (pinnedApiKey !== null && !apiKeyMatches(req)) {
		res.writeHead(401, { 'Content-Type': 'application/json' });
		res.end(JSON.stringify({ error: 'unauthorized' }));
		return;
	}

	// F5 / reconnect: attach to the current turn's relay — replay everything
	// buffered so far, then tail live. No active turn → 204, Worker reads D1.
	// Agent-agnostic: works for any adapter that drives the relay.
	if (req.method === 'GET' && req.url === '/stream') {
		if (!currentRelay) { res.writeHead(204).end(); return; }
		res.writeHead(200, {
			'Content-Type': 'application/x-ndjson',
			'Cache-Control': 'no-cache, no-store',
			'X-Accel-Buffering': 'no',
		});
		currentRelay.addSink(res);
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

	if (inFlight) {
		res.writeHead(409, { 'Content-Type': 'application/json' });
		res.end(JSON.stringify({ error: 'already_running' }));
		return;
	}
	inFlight = true;

	// Read body (capped at 1 MiB).
	const chunks = [];
	let bytes = 0;
	for await (const chunk of req) {
		bytes += chunk.length;
		if (bytes > 1_048_576) {
			res.writeHead(413).end();
			inFlight = false;
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
		inFlight = false;
		return;
	}

	const mode = envelope.mode || 'cc-cli';

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
	relay.addSink(res);

	function emit(type, data) {
		relay.write(JSON.stringify({ type, data, ts: nowMs() }) + '\n');
	}

	// Always emit a leading 'ready' event so client knows server is alive.
	emit('ready', { mode, cwd: process.cwd() });

	// Dispatch.
	try {
		// Phase timestamps emitted as events for the worker-side benchmark
		// instrumentation. These are inner-container measurements that the
		// translator forwards as 'phase' events for telemetry only.
		const t_run_received = nowMs();
		emit('phase', { name: 'run_received', ts: t_run_received });

		if (mode === 'cc-cli' && envelope.warmupOnly === true) {
			// WARMUP-ONLY (inline fork, 2026-07-07): boot claude — wipe stale sessions,
			// write the FULL cred pair, spawn, start the cred watcher — but send NO user
			// message. The Worker forks this box into the golden right after, so the
			// snapshot carries a booted+authed claude with a CLEAN session (no phantom
			// 'ok' — the old bake's warmup message leaked into every clone's history).
			// Liveness proof without inference: a set_model control ping to the SAME
			// model (no-op) — the ack proves the CLI is up and serving its stdin
			// protocol. Auth itself is exercised on first real inference (measured
			// ~52ms — the entire thing the 'ok' used to pre-pay).
			if (!envelope.ccToken) throw new Error('ccToken required for warmupOnly');
			if (!claudeProc || claudeProc.exitCode !== null || claudeSig !== coreSignature(envelope)) {
				if (claudeProc) await killClaude('warmup_respawn');
				wipeClaudeState();
				configureCcAuth(envelope.ccToken, envelope.ccRefreshToken, envelope.ccExpiresAt);
				startCredWatcher(envelope.callback);
				emit('phase', { name: 'pre_claude_spawn', ts: nowMs() });
				spawnPersistentClaude(envelope);
				claudeSig = coreSignature(envelope);
				claudeModel = envelope.model || null;
				claudeTokenHash = tokenHash(envelope.ccToken);
			}
			const alive = await trySetModel(envelope.model || claudeModel || 'claude-opus-4-8', 15_000);
			emit('phase', { name: 'claude_warmup_ready', ts: nowMs(), ack: alive });
			if (!alive) emit('error', { code: 'warmup_no_ack', message: 'claude did not ack the control ping', retryable: true });
		} else if (mode === 'cc-cli') {
			if (!envelope.prompt) throw new Error('prompt required for mode=cc-cli');
			if (!envelope.ccToken) throw new Error('ccToken required for mode=cc-cli');

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
			await runPersistentClaude(envelope, relay, emit);
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
		inFlight = false;
		lastRunAt = nowMs();
		// SAVE + close: complete the relay — ends every sink (incl. this res) and
		// fires the Worker callback to persist text+usage (the single D1 writer).
		// Awaited so the ephemeral container never drops the save. currentRelay is
		// left pointing at the finished relay (done=true): a late F5 hitting
		// /stream still gets the whole turn replayed from its buffer until the
		// next /run replaces it. Never let teardown throw.
		if (credWatcherReport) { try { await credWatcherReport(); } catch {} }
		try {
			const out = await relay.complete(relay.pendingResult || {});
			if (out && out.ok === false) logError('save callback failed', { status: out.status, error: out.error });
		} catch (e) {
			logError('relay.complete threw', { msg: e && e.message });
		}
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

// Idle watchdog — defensive fallback. The Worker-side PoolManagerDO is the
// primary mechanism that destroys idle Fly Machines, but in case it misses us
// (DO restart, alarm drift, network blip) we self-exit after 35 minutes of
// no /run traffic. This is slightly longer than the pool's default 30-min
// idleTimeoutMs so the pool's stop() lands first under normal conditions.
const IDLE_EXIT_MS = 35 * 60 * 1000;
setInterval(() => {
	if (inFlight) return;
	const idle = nowMs() - lastRunAt;
	if (idle > IDLE_EXIT_MS) {
		logError('idle for too long, exiting', { idleMs: idle });
		if (claudeProc) { try { claudeProc.kill('SIGTERM'); } catch {} }
		setTimeout(() => process.exit(2), 1000);
	}
}, 60_000);

// Graceful shutdown — let inflight finish.
const onSig = (sig) => {
	logInfo('signal received', { sig });
	if (!inFlight) process.exit(0);
};
process.on('SIGTERM', onSig);
process.on('SIGINT', onSig);
