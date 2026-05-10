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
// R2 sync state.
//
// Holds rclone env vars for the duration of one /run. Module-scoped so it is
// reachable by syncFromR2/syncToR2, but DELIBERATELY NOT placed on process.env
// — claude is spawned with the default env (process.env), so by never setting
// these globals we guarantee the model can't read R2 credentials via the bash
// tool. rclone is spawned with `{ env: { ...process.env, ...r2Env } }` only.
// ---------------------------------------------------------------------------
let r2Env = null;

function setR2Credentials(r2Config) {
	if (!r2Config) return false;
	const { accessKey, secretKey, endpoint } = r2Config;
	if (!accessKey || !secretKey || !endpoint) {
		logError('r2Config provided but incomplete; skipping sync');
		return false;
	}
	r2Env = {
		RCLONE_CONFIG_R2_TYPE: 's3',
		RCLONE_CONFIG_R2_PROVIDER: 'Cloudflare',
		RCLONE_CONFIG_R2_ACCESS_KEY_ID: accessKey,
		RCLONE_CONFIG_R2_SECRET_ACCESS_KEY: secretKey,
		RCLONE_CONFIG_R2_ENDPOINT: endpoint,
		RCLONE_CONFIG_R2_REGION: 'auto',
		// Suppress slow checksum operations rclone tries on S3-compat backends
		// that R2 doesn't fully support; checksum mode is set per-call instead.
		RCLONE_S3_NO_CHECK_BUCKET: 'true',
	};
	return true;
}

function rcloneSpawnEnv() {
	return r2Env ? { ...process.env, ...r2Env } : { ...process.env };
}

/**
 * Sync R2 prefix → /workspace (download). Called BEFORE claude spawn so the
 * model sees prior turn's files. Empty prefix on first turn = noop fast path.
 *
 * Excludes node_modules and .claude on the way down — node_modules is bundled
 * into the customer image (Option D from architecture doc), and .claude is
 * always wiped+rewritten by configureCcAuth.
 */
async function syncFromR2(r2Config, workspaceDir) {
	if (!r2Config) return { skipped: true };
	const { bucket, prefix } = r2Config;
	if (!bucket) return { skipped: true, reason: 'no_bucket' };

	fs.mkdirSync(workspaceDir, { recursive: true });

	const remote = `r2:${bucket}/${prefix || ''}`;
	const args = [
		'sync',
		remote,
		workspaceDir,
		'--transfers', '8',
		'--checkers', '16',
		'--fast-list',
		'--exclude', 'node_modules/**',
		'--exclude', '.claude/**',
		'--exclude', '.git/**',
		'--exclude', '*.log',
		'--exclude', '.cache/**',
		'--quiet',
	];
	const t0 = nowMs();
	const result = await spawnRclone(args);
	return { ...result, ms: nowMs() - t0, remote };
}

/**
 * Sync /workspace → R2 prefix (upload). Called AFTER claude exits so its
 * generated files are durable across turns.
 *
 * Same exclusions as the down direction. Failures here are logged but do
 * not mask claude's own exit code — caller decides whether to bubble up.
 */
async function syncToR2(r2Config, workspaceDir) {
	if (!r2Config) return { skipped: true };
	const { bucket, prefix } = r2Config;
	if (!bucket) return { skipped: true, reason: 'no_bucket' };
	if (!fs.existsSync(workspaceDir)) return { skipped: true, reason: 'no_workspace_dir' };

	const remote = `r2:${bucket}/${prefix || ''}`;
	const args = [
		'sync',
		workspaceDir,
		remote,
		'--transfers', '8',
		'--checkers', '16',
		'--fast-list',
		'--exclude', 'node_modules/**',
		'--exclude', '.claude/**',
		'--exclude', '.git/**',
		'--exclude', '*.log',
		'--exclude', '.cache/**',
		'--quiet',
	];
	const t0 = nowMs();
	const result = await spawnRclone(args);
	return { ...result, ms: nowMs() - t0, remote };
}

function spawnRclone(args) {
	return new Promise((resolve) => {
		const proc = spawn('/usr/local/bin/rclone', args, {
			stdio: ['ignore', 'pipe', 'pipe'],
			env: rcloneSpawnEnv(),
		});
		const stderrBuf = [];
		proc.stderr.on('data', (c) => {
			stderrBuf.push(c);
			let total = 0;
			for (const b of stderrBuf) total += b.length;
			while (total > 8192 && stderrBuf.length > 1) {
				const removed = stderrBuf.shift();
				total -= removed.length;
			}
		});
		proc.on('error', (err) => {
			resolve({ ok: false, code: -1, err: err.message });
		});
		proc.on('exit', (code) => {
			if (code === 0) {
				resolve({ ok: true, code: 0 });
			} else {
				const stderrTail = Buffer.concat(stderrBuf).toString('utf8').slice(-512);
				resolve({ ok: false, code, stderrTail });
			}
		});
	});
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
	// Configure R2 ONCE, before sync. setR2Credentials is the only call that
	// stores the creds in module scope; nothing else reads them and they are
	// never copied to process.env, so claude's spawned child can't see them.
	const haveR2 = setR2Credentials(envelope.r2Config);

	try {
		if (mode === 'cc-cli') {
			if (!envelope.prompt) throw new Error('prompt required for mode=cc-cli');
			if (!envelope.ccToken) throw new Error('ccToken required for mode=cc-cli');

			// 1. Pull last-turn workspace from R2 (noop if first turn / no creds).
			if (haveR2) {
				emit('sync_down_start', { remote: `r2:${envelope.r2Config.bucket}/${envelope.r2Config.prefix || ''}` });
				const downResult = await syncFromR2(envelope.r2Config, workspaceDir);
				if (downResult.ok === false) {
					logError('sync_down failed', { code: downResult.code, stderr: downResult.stderrTail });
					emit('sync_down_done', { ok: false, code: downResult.code, ms: downResult.ms });
					// Don't fail the turn — claude can still run with empty workspace.
				} else {
					emit('sync_down_done', { ok: true, ms: downResult.ms });
				}
			}

			// 2. Wipe and write fresh CC credentials. Order matters: wipe FIRST
			//    (kills stale state from R2 sync if any sneaked through .claude
			//    exclusion), then write fresh creds.
			wipeClaudeState();
			configureCcAuth(envelope.ccToken);

			// 3. Run claude. Spawned with default env (process.env) — R2 creds
			//    are NOT in there, so the model can't exfiltrate them via bash.
			const args = buildClaudeArgs(envelope);
			await runProc('claude', args, res, emit);

			// 4. Push workspace back to R2 so next turn sees it. Best-effort:
			//    failures here are logged + emitted but don't reverse claude's
			//    output. If the user's connection already closed, emit() noops.
			if (haveR2) {
				emit('sync_up_start', {});
				try {
					const upResult = await syncToR2(envelope.r2Config, workspaceDir);
					if (upResult.ok === false) {
						logError('sync_up failed', { code: upResult.code, stderr: upResult.stderrTail });
						emit('sync_up_done', { ok: false, code: upResult.code, ms: upResult.ms });
					} else {
						emit('sync_up_done', { ok: true, ms: upResult.ms });
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
