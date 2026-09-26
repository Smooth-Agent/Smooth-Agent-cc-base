'use strict';
/**
 * relay.js — per-turn transport layer. AGENT-AGNOSTIC.
 *
 * The three jobs of a turn's I/O, in one reusable place:
 *   RETAIN — buffer every byte emitted this turn, so a client that joins late
 *            (F5 / reconnect) can be handed the whole turn so far.
 *   SEND   — fan a turn's output out to N live sinks (HTTP responses). The
 *            original /run caller is sink #0; a /stream reconnect adds another.
 *   SAVE   — when the turn finishes, POST the result to the Worker (the single
 *            D1 writer) so persistence never depends on a client staying online.
 *
 * It knows NOTHING about claude. An agent adapter drives it:
 *   relay.write(bytes)          // forward agent output (raw or framed)
 *   relay.complete({text,usage})// turn finished → flush sinks + fire callback
 * Adding a new agent (e.g. Codex) = a new adapter that drives this same relay.
 * The Worker side (/internal/chat-result, /stream proxy) is equally agnostic.
 */

class TurnRelay {
  /** @param {{chatId?:string, promptId?:string, callback?:{url:string, jwt?:string}}} opts */
  constructor(opts = {}) {
    this.chatId = opts.chatId || null;
    this.promptId = opts.promptId || null;
    this.callback = opts.callback && opts.callback.url ? opts.callback : null;
    /** @type {Buffer[]} every byte sent this turn — the F5 replay log */
    this.buffer = [];
    /** @type {import('http').ServerResponse[]} live responses receiving the stream */
    this.sinks = [];
    this.done = false;
    this.result = null;
    /** ultimo byte escrito (heartbeat) e se o stream esta numa fronteira de linha */
    this.lastWriteAt = Date.now();
    this.aligned = true;
  }

  /** Attach a (re)connecting client: replay everything so far, then tail live. */
  addSink(res) {
    try {
      for (const b of this.buffer) res.write(b);
    } catch { /* peer gone mid-replay */ }
    if (this.done) {
      try { res.end(); } catch { /* already closed */ }
      return;
    }
    this.sinks.push(res);
    res.on('close', () => { this.sinks = this.sinks.filter((s) => s !== res); });
  }

  /** Buffer a chunk and fan it out to every live sink. */
  write(chunk) {
    if (this.done) return;
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk));
    if (!buf.length) return;
    this.lastWriteAt = Date.now();
    this.aligned = buf[buf.length - 1] === 0x0a;
    this.buffer.push(buf);
    for (const res of this.sinks) {
      try { res.write(buf); } catch { /* drop dead sink; 'close' will prune it */ }
    }
  }

  /**
   * Turn finished. End live sinks and SAVE via the Worker callback. Best-effort
   * and idempotent: returns the callback outcome; never throws into the caller.
   */
  async complete(result) {
    if (this.done) return { ok: true, already: true };
    this.done = true;
    this.result = result || null;
    // ORDER INVARIANT (owner, 2026-08-01): SAVE **before** closing the sinks.
    // Detona's pauseAfter freezes the box the instant the /run stream (a sink)
    // closes — so if we ended sinks first (the old order), the box could pause
    // MID-CALLBACK and the answer would never persist. The client already got the
    // result via the streamed 'result' event, so holding the sink open for the ~1
    // RTT of the save costs nothing and makes the box pausable ONLY once the answer
    // is durable. The empty-claim guard still gates the call (no answer → no POST).
    let outcome = { ok: true, saved: false };
    // EMPTY-CLAIM GUARD (2026-07-11): a run with NOTHING to persist (no text,
    // no usage) must NOT fire the completion callback — the warmupOnly run of
    // the inline fork inherits the TURN's callback jwt, and its empty POST was
    // CLAIMING the prompt row ('done', response '') before the real turn's
    // result could land. The callback exists to persist an answer; no answer,
    // no call. (Cred rotation capture is unaffected — the watcher has its own
    // dedicated endpoint.)
    if (this.callback && result && (result.text || result.usage)) {
      try {
        const r = await fetch(this.callback.url, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            ...(this.callback.jwt ? { Authorization: `Bearer ${this.callback.jwt}` } : {}),
          },
          body: JSON.stringify({
            chatId: this.chatId,
            promptId: this.promptId,
            text: (result && result.text) || '',
            usage: (result && result.usage) || null,
            // SLOT runtime (SLOT_CONTRACT.md): the slot's opaque conversation
            // state — the Worker persists it (chats.slot_context) and returns it
            // as `context` on the next turn. Absent for cc-cli turns.
            ...(result && typeof result.context === 'string' ? { context: result.context } : {}),
            // TURNO-ATE-QUIETO: quantos processos/tarefas do turno AINDA rodavam quando o
            // teto estourou (0 = tudo acabou). O Worker pausa a box neste callback — e so
            // se isto for 0. E o unico sinal de fim de turno que sobrevive a um stream
            // longo (o drain do Worker morre no limite do waitUntil).
            backgroundLeft: Number(this.backgroundLeft || 0),
          }),
        });
        outcome = { ok: r.ok, status: r.status, saved: true };
      } catch (e) {
        outcome = { ok: false, error: (e && e.message) || 'callback_failed', saved: false };
      }
    }
    // Answer is durable (or there was nothing to save) → NOW close the live sinks.
    // This is the signal Detona waits on to pauseAfter, so it must come last.
    for (const res of this.sinks) { try { res.end(); } catch { /* closed */ } }
    this.sinks = [];
    return outcome;
  }
}

module.exports = { TurnRelay };
