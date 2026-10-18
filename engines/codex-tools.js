'use strict';
/**
 * Boxes ONE-SHOT do codex (o Worker destrói no fim): conectar a assinatura ChatGPT e ler o
 * catálogo de modelos da conta. Não são turno de conversa — escrevem direto no relay no formato
 * canônico e fecham.
 */

const fs = require('node:fs');
const { spawn } = require('node:child_process');
const { postToWorker } = require('../lib/worker');
const { ownLane } = require('../lib/lane');
const { nowMs, logError } = require('../lib/log');

/** Escritor de linhas canônicas pra um relay de one-shot. */
function writer(relay) {
	const line = (o) => relay.write(JSON.stringify(o) + '\n');
	return {
		text: (t) => line({ type: 'content_block_delta', delta: { type: 'text_delta', text: t } }),
		result: (text) => {
			line({ type: 'result', result: text, usage: { input_tokens: 0, output_tokens: 0 } });
			relay.pendingResult = { text, usage: { input_tokens: 0, output_tokens: 0 } };
		},
		init: () => line({ type: 'system', subtype: 'init' }),
	};
}

async function closeLaneEngines(lane, why) { for (const e of lane.engines.values()) await e.close(why); }

/**
 * Conectar a assinatura: `codex login --device-auth` (o CLI oficial passa o WAF da OpenAI onde o
 * Worker leva 403). Mostra a URL + o código, o usuário autoriza no navegador DELE, o CLI grava o
 * auth.json e a box manda pro Worker.
 */
async function runCodexLogin(envelope, relay, emit) {
	const lane = ownLane();
	await closeLaneEngines(lane, 'codex_login');
	const w = writer(relay);
	w.init();
	try { fs.mkdirSync(lane.codexHome, { recursive: true, mode: 0o700 }); } catch { /* o CLI cria */ }
	emit('phase', { name: 'codex_login_spawn', ts: nowMs() });
	const proc = spawn('codex', ['login', '--device-auth'], { cwd: '/workspace', env: { ...process.env, CODEX_HOME: lane.codexHome }, stdio: ['ignore', 'pipe', 'pipe'] });
	let buf = '', url = '', code = '', announced = false;
	const scan = (chunk) => {
		buf += chunk.toString('utf8').replace(/\x1b\[[0-9;]*m/g, '');
		const u = buf.match(/https:\/\/auth\.openai\.com\/[^\s]+/);
		const c = buf.match(/\b([A-Z0-9]{4}-[A-Z0-9]{4,6})\b/);
		if (u) url = u[0];
		if (c) code = c[1];
		if (!announced && url && code) {
			announced = true;
			emit('phase', { name: 'codex_device_code', ts: nowMs(), url, code });
			w.text(`🔗 **Conectar Codex (sua sub ChatGPT)**\n\n1. Abra: ${url}\n2. Digite o código: **${code}**\n3. Autorize com sua conta ChatGPT\n\n_Aguardando autorização (expira em 15 min)…_\n`);
		}
	};
	proc.stdout.on('data', scan);
	proc.stderr.on('data', scan);
	const kill = setTimeout(() => { try { proc.kill('SIGKILL'); } catch { /* já saiu */ } }, 14 * 60 * 1000);
	const exitCode = await new Promise((res) => proc.on('exit', res));
	clearTimeout(kill);
	let ok = false;
	try {
		const raw = fs.readFileSync(`${lane.codexHome}/auth.json`, 'utf8');
		if (raw.trim()) ok = (await postToWorker(envelope.callback, '/internal/codex-credentials', { authJson: raw })).ok;
	} catch (e) { logError('codex login capture failed', { msg: e && e.message }); }
	w.text(ok ? '\n✅ **Codex conectado!** Sua sub está pronta — escolha um modelo Codex e mande uma mensagem.' : `\n❌ Login não completou${exitCode ? ` (código ${exitCode})` : ''}. Tente de novo.`);
	w.result(ok ? 'codex_connected' : 'codex_login_failed');
	emit('phase', { name: 'codex_login_exit', ts: nowMs(), ok, code: exitCode });
}

/**
 * Catálogo de modelos DO PRÓPRIO codex (`model/list` do app-server), autenticado com a conta —
 * a lista que a conta tem direito. Nunca uma lista fixa nossa.
 */
async function runCodexModels(envelope, relay, emit) {
	const lane = ownLane();
	await closeLaneEngines(lane, 'codex_models');
	const w = writer(relay);
	w.init();
	try { fs.mkdirSync(lane.codexHome, { recursive: true, mode: 0o700 }); } catch { /* o CLI cria */ }
	if (envelope.codexAuth) {
		try { fs.writeFileSync(`${lane.codexHome}/auth.json`, typeof envelope.codexAuth === 'string' ? envelope.codexAuth : JSON.stringify(envelope.codexAuth), { mode: 0o600 }); } catch { /* sem conta: lista pública */ }
	}
	emit('phase', { name: 'codex_models_spawn', ts: nowMs() });
	const proc = spawn('codex', ['app-server'], { cwd: '/workspace', env: { ...process.env, CODEX_HOME: lane.codexHome }, stdio: ['pipe', 'pipe', 'ignore'] });
	let buf = '';
	const models = await new Promise((resolve) => {
		const done = (list) => { clearTimeout(cap); try { proc.kill('SIGKILL'); } catch { /* já saiu */ } resolve(list); };
		const cap = setTimeout(() => done([]), 30_000);
		proc.stdout.on('data', (d) => {
			buf += d.toString('utf8');
			let nl;
			while ((nl = buf.indexOf('\n')) >= 0) {
				const l = buf.slice(0, nl); buf = buf.slice(nl + 1);
				let o; try { o = JSON.parse(l); } catch { continue; }
				if (o.id === 1 && !o.error) proc.stdin.write(JSON.stringify({ jsonrpc: '2.0', id: 2, method: 'model/list', params: {} }) + '\n');
				if (o.id === 2) done(o.result && Array.isArray(o.result.data) ? o.result.data.filter((m) => !m.hidden).map((m) => ({
					id: m.id, model: m.model, displayName: m.displayName, description: m.description,
					isDefault: !!m.isDefault, defaultReasoningEffort: m.defaultReasoningEffort,
					reasoningEfforts: (m.supportedReasoningEfforts || []).map((x) => x.reasoningEffort),
				})) : []);
			}
		});
		proc.stdin.write(JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'initialize', params: { clientInfo: { name: 'smoothagent', title: 'SmoothAgent', version: '2.0.0' }, capabilities: null } }) + '\n');
	});
	const posted = (await postToWorker(envelope.callback, '/internal/codex-models', { models })).ok;
	emit('phase', { name: 'codex_models_result', ts: nowMs(), count: models.length, posted });
	w.text(JSON.stringify(models));
	w.result('codex_models');
}

module.exports = { runCodexLogin, runCodexModels };
