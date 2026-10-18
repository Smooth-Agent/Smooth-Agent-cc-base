'use strict';
/**
 * Relays dos /run da box (um por mensagem): os ABERTOS em ordem de chegada e os últimos fechados
 * por promptId. É por aqui que o reattach (GET /stream) acha o turno certo.
 */

const open = [];
const done = new Map(); // promptId -> relay (os 8 mais recentes)
let current = null;

function opened(relay) { current = relay; open.push(relay); }
function closed(relay) {
	const at = open.indexOf(relay);
	if (at >= 0) open.splice(at, 1);
	if (relay.promptId) { done.set(relay.promptId, relay); while (done.size > 8) done.delete(done.keys().next().value); }
}

/**
 * O relay pro reattach. promptId → o daquele turno (aberto ou recém-fechado); sem promptId → o
 * mais ANTIGO ainda aberto (o que está rodando). chatId filtra: na box compartilhada o reattach de
 * um chat nunca pode cair no turno de outro.
 */
function relayFor(promptId, chatId) {
	const mine = (r) => !chatId || r.chatId === chatId;
	if (promptId) { const r = open.find((x) => x.promptId === promptId) || done.get(promptId) || null; return r && mine(r) ? r : null; }
	return open.find(mine) || (current && mine(current) ? current : null);
}

/** Os abertos de um chat (o STOP diz ao Worker quais cancelar). */
const openFor = (chatId) => open.filter((r) => r.chatId === chatId && !r.done);
const firstOpen = () => open[0] || current;

module.exports = { opened, closed, relayFor, openFor, firstOpen };
