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
    for (const res of this.sinks) { try { res.end(); } catch { /* closed */ } }
    this.sinks = [];
    if (!this.callback) return { ok: true, saved: false };
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
        }),
      });
      return { ok: r.ok, status: r.status, saved: true };
    } catch (e) {
      return { ok: false, error: (e && e.message) || 'callback_failed', saved: false };
    }
  }
}

module.exports = { TurnRelay };
