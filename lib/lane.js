'use strict';
/**
 * PISTAS (box compartilhada, core/BOX_SPEC.md). Toda box é compartilhável; a box de um chat é a
 * box com UMA pista. Uma pista é o estado de um chat dentro da box: o cwd, as pastas de conta de
 * cada provedor e os engines dele (um processo por provedor usado).
 *   Box própria (envelope sem sharedBox) = a pista '_own' com os caminhos de SEMPRE: /workspace,
 *   ~/.claude, /workspace/.codex.
 *   Box compartilhada = uma pista por chat: cwd /workspace/chats/<chat>; conta e sessões de cada
 *   provedor em /workspace/.lanes/<chat>/<provedor> (cada uma renova o próprio token). Chats
 *   diferentes rodam ao mesmo tempo.
 *
 * ROTAÇÃO POR MEMÓRIA (só com mais de uma pista): antes de subir um engine a box confere a RAM;
 * faltando, fecha os engines da pista OCIOSA usada há mais tempo (LRU) — ela volta depois na
 * mesma sessão. Sem ociosa pra fechar, espera (com ping no stream); nunca arrisca OOM.
 */

const fs = require('node:fs');
const path = require('node:path');
const { nowMs, sleep } = require('./log');

const OWN_LANE = '_own';
const lanes = new Map(); // key -> Lane

class Lane {
	constructor(key, shared) {
		this.key = key;
		this.shared = shared;
		const base = shared ? path.join('/workspace/.lanes', key) : null;
		this.cwd = shared ? path.join('/workspace/chats', key) : (process.env.RUN_CWD || '/workspace');
		/** null = ~/.claude (box própria) */
		this.claudeConfigDir = shared ? path.join(base, 'claude') : null;
		this.codexHome = shared ? path.join(base, 'codex') : (process.env.CODEX_HOME || '/workspace/.codex');
		/** @type {Map<string, import('../engines/engine').Engine>} */
		this.engines = new Map();
		this.openRuns = 0;
		this.lastChatId = null;
		this.lastUsed = nowMs();
		/** fechando os engines ociosos (box compartilhada, server.js releaseIfIdle) */
		this.releasing = null;
	}
	get claudeDir() { return this.claudeConfigDir || path.join(process.env.HOME || '/workspace', '.claude'); }

	/** O engine `kind` desta pista (criado na primeira vez). */
	engine(kind) {
		let e = this.engines.get(kind);
		if (!e) {
			const { engineClass } = require('../engines');
			const Cls = engineClass(kind);
			e = new Cls(this);
			this.engines.set(kind, e);
		}
		return e;
	}
	/** Algum engine da pista com trabalho em andamento (outro que não `exceptKind`). */
	busyEngine(exceptKind) {
		for (const e of this.engines.values()) if (e.kind !== exceptKind && !e.idle()) return e;
		return null;
	}
	idle() { return this.openRuns === 0 && [...this.engines.values()].every((e) => e.idle()); }
	hasLiveEngine() { return [...this.engines.values()].some((e) => e.alive()); }
}

/** A pista do envelope: chat de box compartilhada → a dele; senão a pista única da box. */
function laneFor(envelope) {
	const chat = envelope && (envelope.chatId || (envelope.workspaceProxy && envelope.workspaceProxy.chatId));
	const shared = !!(envelope && envelope.sharedBox === true && typeof chat === 'string' && /^[A-Za-z0-9_-]{1,64}$/.test(chat));
	const key = shared ? chat : OWN_LANE;
	let l = lanes.get(key);
	if (!l) {
		l = new Lane(key, shared);
		if (shared) for (const d of [l.cwd, l.claudeConfigDir, l.codexHome]) { try { fs.mkdirSync(d, { recursive: true, mode: 0o700 }); } catch { /* o spawn falha com mensagem clara */ } }
		lanes.set(key, l);
	}
	return l;
}
const ownLane = () => laneFor(null);

const LANE_SPAWN_MB = Number(process.env.SMOOTH_LANE_SPAWN_MB) || 300;
const MEMINFO_PATH = process.env.SMOOTH_MEMINFO_PATH || '/proc/meminfo'; // trocável só pra teste
function memAvailableMb() {
	try {
		const m = fs.readFileSync(MEMINFO_PATH, 'utf8').match(/^MemAvailable:\s+(\d+)\s+kB/m);
		return m ? Math.floor(Number(m[1]) / 1024) : Infinity;
	} catch { return Infinity; }
}

/** Garante RAM pra um engine novo desta pista (box compartilhada), fechando pista ociosa (LRU). */
async function reserveMemoryFor(lane, emit, maxWaitMs) {
	if (lanes.size <= 1) return;
	const t0 = nowMs(); let lastPhase = 0;
	for (;;) {
		const avail = memAvailableMb();
		if (avail >= LANE_SPAWN_MB) return;
		const victim = [...lanes.values()].filter((l) => l !== lane && l.idle() && l.hasLiveEngine()).sort((a, b) => a.lastUsed - b.lastUsed)[0];
		if (victim) {
			emit('phase', { name: 'lane_rotated', ts: nowMs(), lane: victim.key, avail_mb: avail });
			for (const e of victim.engines.values()) await e.close('memory_rotation', { keepSession: true });
			// respiro antes de medir de novo: a RAM do processo fechado não aparece livre na hora
			await sleep(700);
			continue;
		}
		if (nowMs() - t0 >= maxWaitMs) { emit('phase', { name: 'memory_wait_gave_up', ts: nowMs(), avail_mb: avail }); return; }
		if (nowMs() - lastPhase >= 5000) { lastPhase = nowMs(); emit('phase', { name: 'waiting_memory', ts: nowMs(), avail_mb: avail, need_mb: LANE_SPAWN_MB }); }
		await sleep(1000);
	}
}

/** Quantos turnos a box já fechou desde que o processo subiu (0 = box acabou de nascer: cold). */
let runsCompleted = 0;
const boxState = {
	get runsCompleted() { return runsCompleted; },
	turnCompleted() { runsCompleted++; },
};

module.exports = { Lane, lanes, laneFor, ownLane, OWN_LANE, reserveMemoryFor, memAvailableMb, boxState };
