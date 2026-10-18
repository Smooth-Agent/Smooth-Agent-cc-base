'use strict';
/**
 * O CONTRATO DE UM PROVEDOR (engine). Dono, 2026-10-18: "cada provedor lida com envio seguido e
 * cancel a sua maneira e o comportamento tem q ser o do provedor escolhido" — e "tem q ser fácil
 * adicionar outros provedores".
 *
 * Por isso a box NÃO tem fila nem regra de ordem própria. Cada mensagem (um /run) é ENTREGUE ao
 * engine na hora, pelo canal nativo dele, e é o ENGINE quem decide:
 *   - começar um turno com ela                    → a mensagem é a DONA do turno;
 *   - juntá-la ao turno que já está rodando       → MERGED (a resposta segue no turno da dona);
 *   - deixá-la pra depois do turno atual          → vira dona do próximo turno quando ele começar.
 * O STOP é o interrupt nativo; o que o provedor mantém (mensagens esperando, comando rodando) fica.
 *
 * Mensagem ≠ turno: `Message` é o que o usuário mandou; `Turn` é o que o engine rodou — a dona +
 * as que entraram nele. A saída do turno vai pro relay da DONA (o stream e o salvamento dela); as
 * que entraram recebem `merged` e fecham.
 *
 * PRA ADICIONAR UM PROVEDOR: uma classe que estende `Engine` (engines/<nome>.js) + uma linha em
 * engines/index.js. Ela escreve a saída no formato canônico (stream-json do Claude Code — o que o
 * Worker traduz) via `turn.write()` / `this.writeLine()`.
 */

const crypto = require('node:crypto');
const { nowMs, sleep } = require('../lib/log');
const { uptimeTicks } = require('../lib/turn-quiet');

/** Sentinela do histórico dobrado no prompt (o Worker e a box usam a mesma). */
const FOLD_MARK = '## Mensagem atual do usuário\n\n';
/** Só a mensagem do usuário (sem o histórico que o Worker possa ter dobrado na frente). */
function rawMessage(prompt) {
	const s = String(prompt == null ? '' : prompt);
	const i = s.lastIndexOf(FOLD_MARK);
	return i >= 0 ? s.slice(i + FOLD_MARK.length) : s;
}
/** A mensagem com o histórico dobrado na frente (sessão nova do engine). Idempotente. */
function foldedMessage(envelope) {
	const p = String(envelope.prompt == null ? '' : envelope.prompt);
	if (!envelope.priorContext || p.includes(FOLD_MARK)) return p;
	return `${envelope.priorContext}\n\n${FOLD_MARK}${p}`;
}

class Message {
	constructor({ envelope, relay, emit }) {
		this.id = crypto.randomUUID(); // id que o engine conhece (o claude ecoa ele)
		this.promptId = envelope.promptId || null;
		this.chatId = relay.chatId || null;
		this.envelope = envelope;
		this.relay = relay;
		this.emit = emit;
		this.arrivedAt = nowMs();
		this.role = null; // 'owner' | 'merged'
		this.turn = null;
		this.attached = new Promise((res, rej) => { this._attach = res; this._fail = rej; });
		this.attached.catch(() => {}); // quem espera trata; sem rejeição solta
	}
}

class Turn {
	constructor(owner, engineKind) {
		this.owner = owner;
		this.engine = engineKind;
		this.merged = [];
		this.relay = owner.relay;
		this.emit = owner.emit;
		this.since = uptimeTicks();
		this.until = null; // ticks em que o próximo turno começou
		this.result = null;
		this.done = false;
		this.ended = new Promise((res) => { this._end = res; });
	}
	/** Uma linha (objeto) no formato canônico pro stream da dona. */
	write(obj) { this.relay.write(JSON.stringify(obj) + '\n'); }
	promptIds() { return [this.owner.promptId, ...this.merged.map((m) => m.promptId)].filter(Boolean); }
}

class Engine {
	/** @param {import('../lib/lane').Lane} lane */
	constructor(lane) {
		this.lane = lane;
		/** turno rodando agora */
		this.turn = null;
		/** último turno fechado (o engine ainda pode estar terminando coisas dele) */
		this.lastTurn = null;
	}

	static get kind() { throw new Error('engine kind'); }
	get kind() { return this.constructor.kind; }

	// ── o que cada provedor implementa ────────────────────────────────────────────────────
	/** Deixa o processo pronto pra ESTE envelope (spawn, troca de credencial, modelo, sessão nova). */
	async prepare(_envelope, _emit) { throw new Error('prepare'); }
	/** Entrega a mensagem pelo canal nativo. Resolve `message.attached` com { role, turn }. */
	async submit(_message) { throw new Error('submit'); }
	/** STOP nativo do turno atual. → { interrupted, promptIds } */
	async interrupt() { return { interrupted: false, promptIds: [] }; }
	/** Boot sem mensagem (golden: a foto leva o engine de pé e autenticado). → { ok } */
	async warmup(_envelope, _emit) { return { ok: true }; }
	/** Fecha o processo (memória, troca de identidade). `keepSession` = a próxima vez volta na mesma sessão. */
	async close(_reason, _opts) {}
	/** Trabalho DENTRO do engine que o /proc não vê (o turno espera). */
	workInEngine(_turn) { return 0; }
	/** Sessão/marcador/pid pra achar os processos do turno (lib/turn-quiet). `ignore(name)` = processos
	 *  PERMANENTES do engine (ele sobe auxiliares próprios) — não são trabalho do turno. */
	processInfo() { return { sid: null, mark: null, pid: null, ignore: null }; }
	/** Tem processo vivo (rotação por memória só fecha engine ocioso). */
	alive() { return false; }

	// ── comum a todos ─────────────────────────────────────────────────────────────────────
	/** Algo em andamento: turno rodando ou mensagem entregue esperando o engine ler. */
	busy() { return !!this.turn || this.pendingCount() > 0; }
	pendingCount() { return 0; }
	/** Ocioso de verdade (inclui o trabalho dentro do engine depois do fim do turno). */
	idle() { return !this.busy() && !(this.lastTurn && this.workInEngine(this.lastTurn)); }

	/** Espera ficar ocioso (só quando o processo PRECISA reiniciar: não dá pra trocar a identidade
	 *  de um engine no meio de um turno). Teto = o do trabalho em background. */
	async waitIdle(emit, maxMs, why) {
		if (this.idle()) return true;
		emit('phase', { name: 'waiting_engine_idle', ts: nowMs(), engine: this.kind, why });
		const until = nowMs() + maxMs;
		while (!this.idle() && nowMs() < until) await sleep(300);
		return this.idle();
	}

	/** A mensagem é a dona de um turno novo. */
	startTurn(message) {
		const turn = new Turn(message, this.kind);
		if (this.lastTurn && this.lastTurn.until == null) this.lastTurn.until = turn.since;
		this.turn = turn;
		message.role = 'owner'; message.turn = turn;
		message._attach({ role: 'owner', turn });
		return turn;
	}
	/** A mensagem entrou no turno que está rodando. */
	mergeInto(turn, message) {
		turn.merged.push(message);
		message.role = 'merged'; message.turn = turn;
		message._attach({ role: 'merged', turn });
	}
	/** O turno acabou (result do engine). */
	endTurn(turn, result) {
		if (!turn || turn.done) return;
		turn.done = true;
		turn.result = result;
		// o salvamento (relay.complete → callback do Worker) lê daqui
		if (result) turn.relay.pendingResult = { ...result, ...(turn.merged.length ? { mergedPromptIds: turn.merged.map((m) => m.promptId).filter(Boolean) } : {}) };
		if (this.turn === turn) this.turn = null;
		this.lastTurn = turn;
		turn._end(result);
	}
}

module.exports = { Engine, Message, Turn, rawMessage, foldedMessage, FOLD_MARK };
