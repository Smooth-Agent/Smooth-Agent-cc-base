'use strict';
/**
 * Chamadas da box pro Worker. Toda chamada usa o callback DO TURNO ATUAL ({url, jwt}): a url é a
 * de /internal/chat-result e as outras rotas internas são irmãs dela. O jwt é rebindado a cada
 * /run (SNAPSHOT TRAP: um golden congela as variáveis do processo — um jwt guardado do bake
 * daria 401 em toda chamada de uma box clonada).
 */

const { logError } = require('./log');

/** POST pra uma rota interna do Worker. Devolve {ok, status} e nunca lança. */
async function postToWorker(callback, route, body) {
	if (!callback || !callback.url) return { ok: false, status: 0, skipped: true };
	const url = callback.url.replace(/\/internal\/chat-result$/, route);
	try {
		const r = await fetch(url, {
			method: 'POST',
			headers: { 'Content-Type': 'application/json', ...(callback.jwt ? { Authorization: `Bearer ${callback.jwt}` } : {}) },
			body: JSON.stringify(body),
		});
		return { ok: r.ok, status: r.status };
	} catch (e) {
		logError('worker call failed', { route, msg: e && e.message });
		return { ok: false, status: 0 };
	}
}

module.exports = { postToWorker };
