'use strict';
/**
 * TURNO ATÉ O TRABALHO ACABAR (provado nos dois engines, 2026-09-19). O turno só fecha — e a box
 * só pode pausar — quando nada que ele começou ainda roda: comando em background, build,
 * servidor, subagente. Pausar antes CONGELA esse trabalho no meio.
 *
 * Como achar "o que este turno começou", do mais forte pro fallback:
 *  1. MARCADOR DE AMBIENTE: o engine sobe com SMOOTH_TURN_ID=<nonce>; todo descendente herda
 *     (sobrevive a setsid/nohup/reparenting; /proc/<pid>/environ é legível — mesmo UID).
 *  2. CGROUP do turno, quando delegável (nem sempre é: o server roda como UID 996). Com mais de
 *     uma pista ele não diz de qual chat o processo é — aí só valem marcador e sessão.
 *  3. SESSÃO do engine (setsid no spawn).
 * Janela [since, until): processo que nasceu depois que o PRÓXIMO turno começou é trabalho dele.
 * Trabalho DENTRO do processo do engine (subagente do Claude Code antes de chamar tool, a
 * continuação depois do result) não aparece no /proc — o engine informa via `working()`.
 */

const fs = require('node:fs');
const { spawn } = require('node:child_process');
const { nowMs, logError, logInfo, sleep } = require('./log');

const TURN_ENV = 'SMOOTH_TURN_ID';
const newTurnMark = () => `t${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;

function procHasTurnMark(pid, mark) {
	if (!mark) return false;
	try { return fs.readFileSync(`/proc/${pid}/environ`).toString('latin1').split('\0').includes(`${TURN_ENV}=${mark}`); } catch { return false; }
}

const TURN_CG = '/sys/fs/cgroup/turn';
let turnCgOk = false;
try { fs.mkdirSync(TURN_CG, { recursive: true }); fs.accessSync(TURN_CG + '/cgroup.procs', fs.constants.W_OK); turnCgOk = true; }
// esperado no Detona (sem delegação de cgroup): info, não erro — o dev busca level:error nos logs da box
catch (e) { logInfo('turn cgroup indisponivel — cai pro marcador/sessao', { err: e && e.message }); }

/**
 * Spawn do engine JÁ DENTRO do cgroup do turno (mover o pid depois perdia a corrida: o filho
 * nascia antes). O wrapper entra no cgroup ANTES do exec, no mesmo pid. Sem cgroup, spawn normal.
 */
function spawnEngine(cmd, args, opts) {
	if (!turnCgOk) return spawn(cmd, args, opts);
	return spawn('/bin/sh', ['-c', `echo $$ > ${TURN_CG}/cgroup.procs 2>/dev/null; exec "$0" "$@"`, cmd, ...args], opts);
}

function uptimeTicks() {
	try { return Math.floor(parseFloat(fs.readFileSync('/proc/uptime', 'utf8').split(' ')[0]) * 100); } catch { return 0; }
}

/**
 * Processos vivos (não-zumbis) do turno: na sessão `sid` OU com o marcador `mark` (OU no cgroup,
 * se `useCgroup`), nascidos em [since, until), exceto `exceptPid` e o próprio server.
 */
function liveTurnProcs({ sid, mark, since, until = null, exceptPid = null, useCgroup = false, ignore = null }) {
	const out = [];
	const cands = new Set();
	const inCg = new Set();
	if (turnCgOk && useCgroup) {
		try { for (const l of fs.readFileSync(TURN_CG + '/cgroup.procs', 'utf8').split('\n')) if (/^\d+$/.test(l)) { cands.add(Number(l)); inCg.add(Number(l)); } } catch { /* fallback abaixo */ }
	}
	let dirs = []; try { dirs = fs.readdirSync('/proc'); } catch { /* sem /proc: só o cgroup */ }
	for (const d of dirs) if (/^\d+$/.test(d)) cands.add(Number(d));
	for (const pid of cands) {
		if (pid === exceptPid || pid === process.pid) continue;
		let stat; try { stat = fs.readFileSync(`/proc/${pid}/stat`, 'utf8'); } catch { continue; }
		const close = stat.lastIndexOf(')');
		const name = stat.slice(stat.indexOf('(') + 1, close);
		const f = stat.slice(close + 2).split(' '); // f[0]=state f[3]=session f[19]=starttime
		if (f[0] === 'Z') continue;
		if (!inCg.has(pid) && Number(f[3]) !== sid && !procHasTurnMark(pid, mark)) continue;
		// 2 ticks de folga na borda: contar um processo de 20ms antes é inofensivo; perder um do turno, não
		if (Number(f[19]) < since - 2) continue;
		if (until != null && Number(f[19]) >= until) continue;
		// processo PERMANENTE do próprio engine (não é trabalho do turno — ver Engine.processInfo)
		if (ignore && ignore(name)) continue;
		out.push({ pid, name });
	}
	return out;
}

/**
 * Segura o turno enquanto houver trabalho dele vivo. `ctx`:
 *   sid, mark, since, until  — a janela do turno (until = quando o próximo turno começou)
 *   exceptPid                — o processo do engine (persistente: não é "trabalho do turno")
 *   useCgroup                — pista única na box
 *   ignore(name)             — processos permanentes do engine (auxiliares dele, não do turno)
 *   working()                — trabalho dentro do engine (tarefas em background, continuação)
 * Devolve { waitedMs, left } — `left` = o que ainda rodava quando o teto estourou.
 */
async function waitTurnQuiet(emit, maxMs, ctx) {
	if (!ctx || !ctx.sid) return { waitedMs: 0, left: [] };
	const t0 = nowMs(); let lastPhase = 0; let seen = false;
	for (;;) {
		const live = liveTurnProcs(ctx);
		const inEngine = ctx.working ? ctx.working() : 0;
		const waitedMs = nowMs() - t0;
		if (!live.length && !inEngine) {
			if (seen) emit('phase', { name: 'background_done', ts: nowMs(), waited_ms: waitedMs });
			return { waitedMs, left: [] };
		}
		seen = true;
		const info = { procs: live.length, names: live.slice(0, 8).map((p) => p.name), tasks: inEngine };
		if (waitedMs >= maxMs) {
			emit('phase', { name: 'background_left_running', ts: nowMs(), waited_ms: waitedMs, ...info });
			logError('turn left background work running (cap)', { waitedMs, ...info });
			return { waitedMs, left: live };
		}
		if (nowMs() - lastPhase >= 5000) { lastPhase = nowMs(); emit('phase', { name: 'background_pending', ts: nowMs(), waited_ms: waitedMs, ...info }); }
		await sleep(1000);
	}
}

module.exports = { TURN_ENV, newTurnMark, spawnEngine, uptimeTicks, liveTurnProcs, waitTurnQuiet, turnCgroupAvailable: () => turnCgOk };
