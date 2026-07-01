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
/** Hash of model+systemPrompt+maxTurns+mcpConfig the running claude was started with. */
let claudeSig = null;
/** Last ccToken written to .credentials.json. A new turn with a different token is
 *  swapped in place (credentials rewritten, process reused) — NOT a respawn. */
let claudeTokenHash = null;
/** Per-turn handler binding — null when no /run is in flight. */
let activeTurn = null;
/** Rolling stdout buffer so we can split partial NDJSON lines across data chunks. */
let stdoutLineBuffer = '';
/** Time of last /run completion (for the idle-exit watchdog). */
let lastRunAt = Date.now();
/**
 * Fingerprint of the workspace files synced into THIS container. A warm container
 * already holds the workspace locally, so we only re-sync from R2 when this is a
 * fresh container (null) or the fingerprint changed (a new/removed upload). null
 * until the first successful sync, so a cold container always syncs.
 */
let lastSyncedFingerprint = null;

// ---------------------------------------------------------------------------
// Workspace proxy sync.
//
// Container talks to the Worker (`workspaceProxy.url`) using a short-lived
// JWT (`workspaceProxy.jwt`) — NEVER directly to R2. The Worker enforces
// that every op is rooted at workspaces/<jwt.chatId>/, so cross-tenant
// leaks are impossible by construction even if claude is jailbroken and
// extracts the JWT (it would only get access to its own chat).
//
// We use Node 22 built-in fetch — no S3 client, no rclone, no long-lived
// credentials. The JWT is the ONLY thing in container memory that touches
// R2, and it's chat-scoped + 10-min expiring.
// ---------------------------------------------------------------------------

const SYNC_CONCURRENCY = 8;

// Excluded path patterns. Apply both ways so node_modules / .git / claude
// internals never traverse R2 even if a stale upload sneaks through.
//
// HOME dotfiles (.bashrc, .profile, etc) appear at the workspace root because
// HOME=/workspace for the agent user. They're useradd defaults — not user
// content — so they should never appear in R2. .npmrc / .ssh / .gnupg get
// special exclusion because they may contain credentials.
const HOME_DOTFILES = new Set([
	'.bashrc',
	'.bash_logout',
	'.bash_history',
	'.bash_profile',
	'.profile',
	'.npmrc',
	'.viminfo',
	'.lesshst',
	'.wget-hsts',
]);

function shouldExclude(rel) {
	if (rel.startsWith('node_modules/') || rel.includes('/node_modules/')) return true;
	if (rel.startsWith('.claude/') || rel.startsWith('.claude.json')) return true;
	if (rel.startsWith('.git/') || rel.includes('/.git/')) return true;
	if (rel.startsWith('.ssh/') || rel.startsWith('.gnupg/')) return true;
	if (rel.endsWith('.log')) return true;
	if (rel.startsWith('.cache/') || rel.includes('/.cache/')) return true;
	// HOME dotfiles only at the workspace ROOT (no slash before name).
	// Subpaths like myproject/.bashrc are user content and should sync.
	if (!rel.includes('/') && HOME_DOTFILES.has(rel)) return true;
	return false;
}

async function proxyFetch(proxy, urlPath, init = {}) {
	const headers = { ...(init.headers || {}), Authorization: `Bearer ${proxy.jwt}` };
	return fetch(`${proxy.url}${urlPath}`, { ...init, headers });
}

async function listWorkspace(proxy) {
	const res = await proxyFetch(proxy, '/internal/workspace/list');
	if (!res.ok) {
		const txt = await res.text().catch(() => '');
		throw new Error(`list ${res.status}: ${txt.slice(0, 200)}`);
	}
	const data = await res.json();
	return Array.isArray(data?.files) ? data.files : [];
}

async function getFile(proxy, rel) {
	const res = await proxyFetch(proxy, `/internal/workspace/get?path=${encodeURIComponent(rel)}`);
	if (res.status === 404) return null;
	if (!res.ok) throw new Error(`get ${rel} ${res.status}`);
	const buf = await res.arrayBuffer();
	return Buffer.from(buf);
}

async function putFile(proxy, rel, buffer) {
	const res = await proxyFetch(proxy, `/internal/workspace/put?path=${encodeURIComponent(rel)}`, {
		method: 'PUT',
		headers: {
			'Content-Type': 'application/octet-stream',
			'Content-Length': String(buffer.length),
		},
		body: buffer,
	});
	if (!res.ok) {
		const txt = await res.text().catch(() => '');
		throw new Error(`put ${rel} ${res.status}: ${txt.slice(0, 200)}`);
	}
}

async function deleteFile(proxy, rel) {
	const res = await proxyFetch(proxy, `/internal/workspace/delete?path=${encodeURIComponent(rel)}`, {
		method: 'DELETE',
	});
	if (!res.ok && res.status !== 404) {
		throw new Error(`delete ${rel} ${res.status}`);
	}
}

// Walk a local directory into a flat list of relative paths (with excludes).
async function walkLocal(workspaceDir) {
	const out = [];
	async function rec(dir, base) {
		let entries;
		try {
			entries = await fs.promises.readdir(dir, { withFileTypes: true });
		} catch (err) {
			if (err && err.code === 'ENOENT') return;
			throw err;
		}
		for (const e of entries) {
			const full = path.join(dir, e.name);
			const rel = base ? `${base}/${e.name}` : e.name;
			if (e.isSymbolicLink()) continue;
			if (e.isDirectory()) {
				if (shouldExclude(rel + '/')) continue;
				await rec(full, rel);
			} else if (e.isFile()) {
				if (shouldExclude(rel)) continue;
				out.push(rel);
			}
		}
	}
	await rec(workspaceDir, '');
	return out;
}

// Bounded-concurrency pool. Returns {results, errors}.
async function pool(items, fn, concurrency) {
	const out = new Array(items.length);
	let next = 0;
	let errors = 0;
	async function worker() {
		while (next < items.length) {
			const i = next++;
			try {
				out[i] = await fn(items[i], i);
			} catch (err) {
				errors++;
				out[i] = { __error: err && err.message ? err.message : String(err) };
			}
		}
	}
	const workers = Array.from({ length: Math.min(concurrency, items.length) }, () => worker());
	await Promise.all(workers);
	return { results: out, errors };
}

/**
 * Sync R2 → /workspace via the Worker proxy. Called BEFORE claude spawn.
 * First turn returns empty manifest from the proxy, so this is near-noop.
 */
async function syncFromWorker(proxy, workspaceDir) {
	if (!proxy?.url || !proxy?.jwt) return { skipped: true, reason: 'no_proxy' };

	fs.mkdirSync(workspaceDir, { recursive: true });
	const t0 = nowMs();

	let files;
	try {
		files = await listWorkspace(proxy);
	} catch (err) {
		return { ok: false, ms: nowMs() - t0, error: err.message };
	}
	// Defensive client-side excludes too — in case the bucket has historical
	// junk that pre-dates the current excludes list.
	files = files.filter((f) => !shouldExclude(f.key));

	if (files.length === 0) {
		return { ok: true, ms: nowMs() - t0, count: 0 };
	}

	const { errors } = await pool(
		files,
		async (f) => {
			const buf = await getFile(proxy, f.key);
			if (!buf) return { skipped: true };
			const dest = path.join(workspaceDir, f.key);
			fs.mkdirSync(path.dirname(dest), { recursive: true });
			await fs.promises.writeFile(dest, buf);
			return { wrote: f.key, bytes: buf.length };
		},
		SYNC_CONCURRENCY,
	);

	return { ok: errors === 0, ms: nowMs() - t0, count: files.length, errors };
}

/**
 * Sync /workspace → R2 via the Worker proxy. Called AFTER claude exits.
 *
 * Behaves like `rclone sync` (mirror): uploads every local file and deletes
 * remote files that no longer exist locally. Failures are logged but never
 * mask claude's own exit/result event.
 */
async function syncToWorker(proxy, workspaceDir) {
	if (!proxy?.url || !proxy?.jwt) return { skipped: true, reason: 'no_proxy' };
	if (!fs.existsSync(workspaceDir)) return { skipped: true, reason: 'no_workspace_dir' };

	const t0 = nowMs();

	const [localList, remoteList] = await Promise.all([
		walkLocal(workspaceDir),
		listWorkspace(proxy).catch(() => []),
	]);
	const localSet = new Set(localList);
	const remoteRels = remoteList.map((f) => f.key).filter((k) => !shouldExclude(k));

	const upload = await pool(
		localList,
		async (rel) => {
			const full = path.join(workspaceDir, rel);
			const buf = await fs.promises.readFile(full);
			await putFile(proxy, rel, buf);
			return { rel, bytes: buf.length };
		},
		SYNC_CONCURRENCY,
	);

	const toDelete = remoteRels.filter((rel) => !localSet.has(rel));
	let deleteErrors = 0;
	if (toDelete.length) {
		const del = await pool(toDelete, async (rel) => {
			await deleteFile(proxy, rel);
			return { rel };
		}, SYNC_CONCURRENCY);
		deleteErrors = del.errors;
	}

	return {
		ok: upload.errors === 0 && deleteErrors === 0,
		ms: nowMs() - t0,
		uploaded: localList.length,
		deleted: toDelete.length,
		errors: upload.errors + deleteErrors,
	};
}

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
function configureCcAuth(token) {
	const homeDir = process.env.HOME || '/workspace';
	const ccDir = path.join(homeDir, '.claude');
	fs.mkdirSync(ccDir, { recursive: true, mode: 0o700 });
	fs.writeFileSync(
		path.join(ccDir, '.credentials.json'),
		JSON.stringify({
			claudeAiOauth: {
				accessToken: token,
				scopes: ['user:inference'],
			},
		}),
		{ mode: 0o600 },
	);
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

/**
 * Signature of the args that materially change Claude's behavior. If a
 * subsequent /run has a different signature, the persistent process must be
 * restarted (it's already locked into the prior model/system-prompt/MCPs).
 */
function sessionSignature(envelope) {
	return JSON.stringify({
		model: envelope.model || null,
		systemPrompt: envelope.systemPrompt || null,
		maxTurns: envelope.maxTurns || null,
		mcpConfig: envelope.mcpConfig || null,
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
			claudeTokenHash = null;
			stdoutLineBuffer = '';
		}
	});

	claudeProc = proc;
	claudeSig = sessionSignature(envelope);
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
	const newSig = sessionSignature(envelope);
	const newTokenHash = tokenHash(envelope.ccToken);

	// Decide: reuse, or kill+respawn?
	// forceRespawn: set by the Worker on the FIRST /run after a Detona resume.
	// A resumed microVM restores the claude PROCESS but its open HTTPS connection
	// to Anthropic is dead — reusing it hangs ~20-56s. So we kill+respawn (cheap:
	// auth + workspace survive on disk, only the process is rebuilt ~1.4s).
	// A rotated ccToken is NOT a reason to respawn — it's a credential, not a
	// behavior change. Respawning to re-key threw away the warm in-memory session
	// (forcing a context refold, tokIn ~2470) for nothing. The token is now swapped
	// in place in the reuse branch below. Only model/system/mcp changes (claudeSig),
	// a dead process, or the Worker's post-resume forceRespawn actually respawn.
	const needsRespawn =
		!claudeProc ||
		claudeProc.exitCode !== null ||
		claudeSig !== newSig ||
		envelope.forceRespawn === true;

	if (needsRespawn) {
		if (claudeProc) {
			emit('phase', { name: 'claude_respawn', ts: nowMs(), reason: envelope.forceRespawn === true ? 'post_resume' : (claudeSig !== newSig ? 'args_changed' : (claudeTokenHash !== newTokenHash ? 'token_rotated' : 'dead')) });
			await killClaude('respawn');
		}
		// Refresh credentials.json on disk and (only on first ever turn) wipe
		// stale session lockfiles. Token may have rotated even when session
		// args are the same, so always rewrite credentials before spawn.
		wipeClaudeState();
		configureCcAuth(envelope.ccToken);
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
			configureCcAuth(envelope.ccToken);
			claudeTokenHash = newTokenHash;
			emit('phase', { name: 'claude_token_swapped', ts: nowMs() });
		}
		emit('phase', { name: 'claude_reused', ts: nowMs() });
		activeTurn = {
			relay, emit, t_spawn: nowMs(),
			firstStdoutSeen: false, firstSystemSeen: false, firstAssistantSeen: false,
			resolve: null,
		};
	}

	// Send the user message via stdin as an NDJSON event. stream-json input
	// format expects {"type":"user","message":{"role":"user","content":"..."}}.
	const userEvent = {
		type: 'user',
		message: { role: 'user', content: envelope.prompt },
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

const server = http.createServer(async (req, res) => {
	if (req.method === 'GET' && req.url === '/health') {
		res.writeHead(200, { 'Content-Type': 'application/json' });
		res.end(JSON.stringify({ status: 'ok', inFlight }));
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
	const workspaceDir = process.env.RUN_CWD || '/workspace';
	// Workspace proxy: container talks to the Worker (not R2). Worker scopes
	// every op to this chat's prefix using the chatId baked into the JWT,
	// so even if the JWT is exfiltrated by a jailbroken claude it can only
	// access this chat's files. No S3 token exists in this process.
	const proxy = envelope.workspaceProxy && envelope.workspaceProxy.url && envelope.workspaceProxy.jwt
		? envelope.workspaceProxy
		: null;

	try {
		// Phase timestamps emitted as events for the worker-side benchmark
		// instrumentation. These are inner-container measurements that the
		// translator forwards as 'phase' events for telemetry only.
		const t_run_received = nowMs();
		emit('phase', { name: 'run_received', ts: t_run_received });

		if (mode === 'cc-cli') {
			if (!envelope.prompt) throw new Error('prompt required for mode=cc-cli');
			if (!envelope.ccToken) throw new Error('ccToken required for mode=cc-cli');

			// 1. Pull last-turn workspace from R2 via Worker.
			// ── SYNC DISABLED (decisão temporária) ────────────────────────────
			// R2 workspace sync is OFF for now. CC chat carries context via D1
			// priorContext, not files, so the sync was pure latency. To re-enable,
			// change `false && proxy` back to `proxy` here AND in the sync_up below.
			// (The skip-when-warm logic + wsFingerprint stay wired for when it's on.)
			if (false && proxy) {
				const wsFp = envelope.wsFingerprint || '';
				if (lastSyncedFingerprint === wsFp) {
					emit('sync_down_done', { ok: true, ms: 0, skipped: 'warm_unchanged' });
				} else {
					emit('sync_down_start', {});
					const downResult = await syncFromWorker(proxy, workspaceDir);
					if (downResult.ok === false) {
						logError('sync_down failed', { error: downResult.error, errors: downResult.errors });
						emit('sync_down_done', { ok: false, ms: downResult.ms, error: downResult.error });
						// Don't fail the turn — claude can still run with empty workspace.
					} else {
						lastSyncedFingerprint = wsFp;
						emit('sync_down_done', { ok: true, ms: downResult.ms, count: downResult.count });
					}
				}
			}

			// 2. Run claude. runPersistentClaude reuses the same process across
			//    turns when args + token match — only the first turn pays the
			//    ~20-25s OAuth handshake cost. Subsequent turns are ~2-3s.
			//    Credentials wipe + write happens INSIDE runPersistentClaude
			//    when (and only when) a respawn is needed.
			await runPersistentClaude(envelope, relay, emit);
			emit('phase', { name: 'post_claude_exit', ts: nowMs(), since_run_received_ms: nowMs() - t_run_received });

			// 4. Push workspace back to R2 via Worker so the next turn sees it.
			// ── SYNC DISABLED (decisão temporária) — pair of the sync_down above.
			//    Re-enable by changing `false && proxy` back to `proxy`.
			if (false && proxy) {
				emit('sync_up_start', {});
				try {
					const upResult = await syncToWorker(proxy, workspaceDir);
					if (upResult.ok === false) {
						logError('sync_up failed', { errors: upResult.errors });
						emit('sync_up_done', { ok: false, ms: upResult.ms, errors: upResult.errors });
					} else {
						emit('sync_up_done', {
							ok: true,
							ms: upResult.ms,
							uploaded: upResult.uploaded,
							deleted: upResult.deleted,
						});
					}
				} catch (err) {
					logError('sync_up threw', { msg: err && err.message });
					emit('sync_up_done', { ok: false, error: 'exception' });
				}
			}
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
