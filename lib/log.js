'use strict';
/** Logs da box: stderr (o stdout de cada /run é o stream do turno). Nunca logar credencial. */

const nowMs = () => Date.now();

function logInfo(msg, extra = {}) {
	process.stderr.write(JSON.stringify({ ts: nowMs(), level: 'info', msg, ...extra }) + '\n');
}

function logError(msg, extra = {}) {
	process.stderr.write(JSON.stringify({ ts: nowMs(), level: 'error', msg, ...extra }) + '\n');
}

/** Impressão digital curta de um segredo (detectar troca sem guardar o valor). FNV-1a. */
function secretHash(value) {
	if (!value) return null;
	let h = 0x811c9dc5;
	for (let i = 0; i < value.length; i++) {
		h ^= value.charCodeAt(i);
		h = Math.imul(h, 0x01000193);
	}
	return (h >>> 0).toString(16);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

module.exports = { nowMs, logInfo, logError, secretHash, sleep };
