'use strict';
/**
 * DOUTRINA DO TOKEN: o ENGINE é o único que renova a própria credencial (o claude reescreve
 * .credentials.json; o codex reescreve auth.json). A plataforma nunca renova — ela CAPTURA: no
 * instante em que o arquivo muda, a rotação vai pro Worker (fonte da verdade). O refresh token é
 * de uso único: rotação perdida = conta morta. Por isso também há o flush explícito (report())
 * antes de fechar cada turno — o fs.watch sozinho perde a corrida contra a pausa da box.
 *
 * Um observador por arquivo de credencial de uma pista. O callback é REBINDADO a cada turno
 * (snapshot trap: ver lib/worker.js). Conteúdo nunca vai pra log.
 */

const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { postToWorker } = require('./worker');
const { logInfo, logError } = require('./log');

class CredentialWatch {
	/**
	 * @param {{ file: () => string, route: string, payload: (raw: string) => object | null, label: string }} spec
	 *   file: caminho do arquivo (função: a pista resolve); route: rota interna do Worker;
	 *   payload: monta o corpo a partir do conteúdo (null = nada a reportar).
	 */
	constructor(spec) {
		this.spec = spec;
		this.callback = null;
		this.started = false;
		this.last = '';
	}

	fingerprint() {
		try { return crypto.createHash('sha256').update(fs.readFileSync(this.spec.file())).digest('hex'); } catch { return ''; }
	}

	/** O conteúdo atual é o que a plataforma já tem (acabamos de gravar). */
	markCurrent() { this.last = this.fingerprint(); }

	/** Liga (uma vez) e rebinda o callback do turno. */
	arm(callback) {
		if (callback && callback.url) this.callback = callback;
		if (this.started || !this.callback) return;
		this.started = true;
		const file = this.spec.file();
		try {
			fs.watch(path.dirname(file), { persistent: false }, (_evt, fname) => {
				if (fname === path.basename(file)) setTimeout(() => this.report(), 150); // escrita parcial
			});
		} catch (e) { logError('credential watch failed to start', { label: this.spec.label, msg: e && e.message }); }
	}

	/** Credencial estática (chave de API): nada roda, nada a reportar. */
	disarm() { this.callback = null; }

	/** Reporta se mudou. Falha = não marca como reportado (o próximo turno tenta de novo). */
	async report() {
		if (!this.callback) return;
		const fp = this.fingerprint();
		if (!fp || fp === this.last) return;
		let body = null;
		try { body = this.spec.payload(fs.readFileSync(this.spec.file(), 'utf8')); } catch { body = null; }
		if (!body) return;
		this.last = fp;
		const r = await postToWorker(this.callback, this.spec.route, body);
		if (!r.ok) this.last = '';
		logInfo('credential rotation reported', { label: this.spec.label, status: r.status });
	}
}

module.exports = { CredentialWatch };
