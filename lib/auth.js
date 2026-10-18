'use strict';
/**
 * AUTH DA BOX.
 *
 * PIN NO PRIMEIRO /run: a imagem é genérica e sobe sem segredo; o primeiro POST /run é por
 * construção o do CRIADOR (o run.http que o Detona entrega no spawn) e fixa a chave, exigida
 * (comparação em tempo constante) em toda rota sensível depois. GET não fixa: o probe de prontidão
 * do build do template faz GET antes de existir qualquer pedido do Worker — fixar ali gravaria
 * chave VAZIA no snapshot. Chave vazia no 1º /run = aberto, com log alto.
 *
 * TOKEN DE STREAM DO USUÁRIO (GET /stream direto do navegador): token Ed25519 curto que o Worker
 * assina por turno. A box só guarda a chave PÚBLICA (inofensiva num golden). Abre SÓ GET /stream,
 * e só dos chats que rodaram turno nesta box. Chave e chats são rebindados a cada /run.
 */

const crypto = require('node:crypto');
const { logInfo, logError, secretHash } = require('./log');

let pinnedApiKey = null; // null = nada fixado; '' = fixado aberto
const streamChats = new Set();
let streamPubKey = null;

function pinIfFirst(req) {
	if (pinnedApiKey !== null) return;
	pinnedApiKey = String(req.headers['x-api-key'] ?? '');
	if (!pinnedApiKey) logError('auth pin EMPTY — first /run had no x-api-key, auth is OPEN');
	else logInfo('auth pinned', { keyHash: secretHash(pinnedApiKey) });
}
const pinned = () => pinnedApiKey !== null;
function apiKeyMatches(req) {
	const a = Buffer.from(String(req.headers['x-api-key'] ?? '')), b = Buffer.from(pinnedApiKey || '');
	return a.length === b.length && crypto.timingSafeEqual(a, b);
}

function rebindStream(envelope) {
	const c = envelope.chatId || (envelope.workspaceProxy && envelope.workspaceProxy.chatId) || null;
	if (c) streamChats.add(c);
	streamPubKey = typeof envelope.streamPubKey === 'string' && envelope.streamPubKey ? Buffer.from(envelope.streamPubKey, 'base64') : null;
}

const b64url = (s) => Buffer.from(s.replace(/-/g, '+').replace(/_/g, '/') + (s.length % 4 === 0 ? '' : '='.repeat(4 - (s.length % 4))), 'base64');

/** null = válido (req.streamChat = o chat do token); senão o motivo. */
function userStreamTokenWhy(req) {
	if (!streamPubKey) return 'no_pubkey';
	let raw = '';
	const authz = String(req.headers['authorization'] ?? '');
	if (authz.startsWith('Bearer ')) raw = authz.slice(7).trim();
	if (!raw) { const q = req.url.indexOf('?'); if (q >= 0) raw = new URLSearchParams(req.url.slice(q + 1)).get('token') || ''; }
	if (!raw) return 'no_token';
	const dot = raw.indexOf('.');
	if (dot <= 0) return 'malformed';
	let payloadBuf, sig, payload;
	try { payloadBuf = b64url(raw.slice(0, dot)); sig = b64url(raw.slice(dot + 1)); payload = JSON.parse(payloadBuf.toString('utf8')); } catch { return 'malformed'; }
	if (!payload || typeof payload.c !== 'string' || typeof payload.e !== 'number') return 'malformed';
	let ok = false;
	try { ok = crypto.verify(null, payloadBuf, { key: streamPubKey, format: 'der', type: 'spki' }, sig); } catch { ok = false; }
	if (!ok) return 'bad_sig';
	if (payload.e <= Math.floor(Date.now() / 1000)) return 'expired';
	if (!streamChats.has(payload.c)) return 'wrong_chat';
	req.streamChat = payload.c;
	return null;
}

module.exports = { pinIfFirst, pinned, apiKeyMatches, rebindStream, userStreamTokenWhy };
