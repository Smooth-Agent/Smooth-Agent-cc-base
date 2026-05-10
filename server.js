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

const PORT = 8080;
const HOST = '0.0.0.0';

let inFlight = false;

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
function shouldExclude(rel) {
	if (rel.startsWith('node_modules/') || rel.includes('/node_modules/')) return true;
	if (rel.startsWith('.claude/') || rel.startsWith('.claude.json')) return true;
	if (rel.startsWith('.git/') || rel.includes('/.git/')) return true;
	if (rel.endsWith('.log')) return true;
	if (rel.startsWith('.cache/') || rel.includes('/.cache/')) return true;
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
	// so ~/.claude resolves into the persistent Fly Volume. Claude stores
	// per-project session state under .claude/projects/<hash>/sessions/<id>.jsonl.
	// If a prior turn died mid-run, the lockfile/state survives and the next
	// turn errors with "Session ID X is already in use". Wipe the whole
	// .claude dir before each turn — auth credentials are re-written by
	// configureCcAuth right after.
	//
	// Note: project files in /workspace/* (NOT under .claude/) are untouched;
	// CLAUDE.md and similar live at the project root, not under .claude/, so
	// they survive.
	const homeDir = process.env.HOME || '/workspace';
	const ccDir = path.join(homeDir, '.claude');
	try {
		fs.rmSync(ccDir, { recursive: true, force: true });
	} catch (err) {
		logError('failed to wipe .claude state', { msg: err && err.message });
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

/** Build claude argv from the envelope. */
function buildClaudeArgs(envelope) {
	const args = ['--output-format', 'stream-json', '--verbose', '--include-partial-messages'];
	// Sandbox is the trust boundary, not the CLI prompt: container is per-turn
	// ephemeral, /workspace lives in tmpfs, R2 is scoped to one chat. Without
	// bypass, claude refuses every Write/Edit/Bash call and just narrates
	// "I need permission" — useless in headless mode. Override with
	// --permission-mode default if a customer image needs interactive prompts.
	args.push('--permission-mode', 'bypassPermissions');
	if (envelope.model) args.push('--model', envelope.model);
	if (envelope.sessionId) args.push('--session-id', envelope.sessionId);
	if (envelope.systemPrompt) args.push('--system-prompt', envelope.systemPrompt);
	if (envelope.maxTurns) args.push('--max-turns', String(envelope.maxTurns));
	if (envelope.mcpConfig) {
		const tmpFile = `/tmp/mcp-${Date.now()}.json`;
		fs.writeFileSync(tmpFile, JSON.stringify(envelope.mcpConfig));
		args.push('--mcp-config', tmpFile);
	}
	args.push(envelope.prompt);
	return args;
}

const server = http.createServer(async (req, res) => {
	if (req.method === 'GET' && req.url === '/health') {
		res.writeHead(200, { 'Content-Type': 'application/json' });
		res.end(JSON.stringify({ status: 'ok', inFlight }));
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

	function emit(type, data) {
		try {
			res.write(JSON.stringify({ type, data, ts: nowMs() }) + '\n');
		} catch {
			// Connection closed; ignore.
		}
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
		if (mode === 'cc-cli') {
			if (!envelope.prompt) throw new Error('prompt required for mode=cc-cli');
			if (!envelope.ccToken) throw new Error('ccToken required for mode=cc-cli');

			// 1. Pull last-turn workspace from R2 via Worker (noop on first turn).
			if (proxy) {
				emit('sync_down_start', {});
				const downResult = await syncFromWorker(proxy, workspaceDir);
				if (downResult.ok === false) {
					logError('sync_down failed', { error: downResult.error, errors: downResult.errors });
					emit('sync_down_done', { ok: false, ms: downResult.ms, error: downResult.error });
					// Don't fail the turn — claude can still run with empty workspace.
				} else {
					emit('sync_down_done', { ok: true, ms: downResult.ms, count: downResult.count });
				}
			}

			// 2. Wipe and write fresh CC credentials. Order matters: wipe FIRST
			//    (kills stale state from R2 sync if any sneaked through the
			//    exclusion list), then write fresh creds.
			wipeClaudeState();
			configureCcAuth(envelope.ccToken);

			// 3. Run claude. Spawned with default env (process.env) — workspace
			//    JWT is held in a JS variable scoped to this request handler,
			//    not in the env. claude can read its OWN env via bash, but not
			//    this server's JS heap.
			const args = buildClaudeArgs(envelope);
			await runProc('claude', args, res, emit);

			// 4. Push workspace back to R2 via Worker so the next turn sees it.
			//    Best-effort: failures are logged + emitted but never mask
			//    claude's own output.
			if (proxy) {
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
		try {
			res.end();
		} catch { /* connection closed */ }
		// Allow response to flush, then exit. Machine auto-destroys.
		setTimeout(() => process.exit(0), 250);
	}
});

/** Spawn a process, pipe stdout chunks raw to the response (claude case). */
function runProc(command, args, res, emit) {
	return new Promise((resolve) => {
		const proc = spawn(command, args, {
			cwd: process.env.RUN_CWD || '/workspace',
			env: process.env,
			stdio: ['ignore', 'pipe', 'pipe'],
		});

		// claude already emits stream-json (ndjson). Forward raw — saves CPU + preserves
		// the chatagentico-style frame structure that the runner translator expects.
		proc.stdout.on('data', (chunk) => {
			try {
				res.write(chunk);
			} catch { /* ignore */ }
		});

		// Stderr: emit as warn-level event so client sees it but doesn't fail.
		const stderrBuf = [];
		proc.stderr.on('data', (chunk) => {
			stderrBuf.push(chunk);
			// Cap to 64KB to avoid runaway memory.
			let total = 0;
			for (const b of stderrBuf) total += b.length;
			while (total > 65536 && stderrBuf.length > 1) {
				const removed = stderrBuf.shift();
				total -= removed.length;
			}
		});

		proc.on('error', (err) => {
			emit('error', { code: 'spawn_failed', message: err.message, retryable: false });
			resolve();
		});
		proc.on('exit', (code) => {
			if (code !== 0) {
				const tail = Buffer.concat(stderrBuf).toString('utf8').slice(-512);
				emit('error', {
					code: 'exit_nonzero',
					message: `${command} exited ${code}${tail ? `\nstderr (tail): ${tail}` : ''}`,
					retryable: false,
				});
			}
			resolve();
		});
	});
}

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

// If we don't get a request within 5 minutes of boot, exit (defensive).
setTimeout(() => {
	if (!inFlight) {
		logError('No /run after 5 min, exiting');
		process.exit(2);
	}
}, 5 * 60 * 1000);

// Graceful shutdown — let inflight finish.
const onSig = (sig) => {
	logInfo('signal received', { sig });
	if (!inFlight) process.exit(0);
};
process.on('SIGTERM', onSig);
process.on('SIGINT', onSig);
