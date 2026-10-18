'use strict';
/**
 * REGISTRO DE PROVEDORES. Adicionar um provedor = escrever engines/<nome>.js (uma classe que
 * estende Engine — ver engines/engine.js) e registrar aqui. O envelope escolhe pelo `engine`
 * (ausente = claude, o histórico). Nada mais na box precisa saber qual provedor é.
 */

const { ClaudeEngine } = require('./claude');
const { CodexEngine } = require('./codex');

const ENGINES = {
	[ClaudeEngine.kind]: ClaudeEngine,
	[CodexEngine.kind]: CodexEngine,
};

function engineKindOf(envelope) {
	return (envelope && envelope.engine) || ClaudeEngine.kind;
}

function engineClass(kind) {
	const Cls = ENGINES[kind];
	if (!Cls) throw new Error(`unknown engine: ${kind}`);
	return Cls;
}

module.exports = { ENGINES, engineKindOf, engineClass };
