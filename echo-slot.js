#!/usr/bin/env node
// echo-slot.js — a MENOR implementação válida do SLOT_CONTRACT (referência + PoC).
// Servidor persistente (invariante 1): sobe uma vez, atende N turnos.
// Nada per-turn em global (invariante 2): tudo vem do body, nada é cacheado.
// Stateless (invariante 3): o "estado" é um contador que viaja no context.
// Prova do "um cold só": se este processo responder WARM num clone de golden,
// o snapshot capturou adaptador + slot juntos.
'use strict';
const http = require('http');

const PORT = Number(process.env.SLOT_PORT || 8200);
const BOOT_TS = Date.now(); // estampa do BOOT — num clone warm, vem da FOTO (prova o snapshot)

const server = http.createServer((req, res) => {
	if (req.method === 'GET' && req.url === '/ready') {
		res.writeHead(200, { 'Content-Type': 'application/json' });
		res.end(JSON.stringify({ ok: true, bootTs: BOOT_TS }));
		return;
	}
	if (req.method === 'POST' && req.url === '/agent-run') {
		let body = '';
		req.on('data', (c) => { body += c; });
		req.on('end', () => {
			let e = {};
			try { e = JSON.parse(body); } catch { /* eco de nada */ }
			res.writeHead(200, { 'Content-Type': 'application/x-ndjson' });
			// context = contador de turnos (prova que o estado viaja no request, não no processo)
			let turns = 0;
			try { turns = (JSON.parse(e.context || '{}').turns || 0); } catch { }
			const secretKeys = Object.keys(e.secrets || {});
			const text = `echo[turn ${turns + 1}, boot ${BOOT_TS}]: ${e.prompt || ''}` +
				(secretKeys.length ? ` (secrets: ${secretKeys.join(',')})` : '');
			res.write(JSON.stringify({ type: 'text', data: text }) + '\n');
			res.write(JSON.stringify({
				type: 'result',
				data: { text, context: JSON.stringify({ turns: turns + 1 }), usage: { tokensIn: 0, tokensOut: 0 } },
			}) + '\n');
			res.end();
		});
		return;
	}
	res.writeHead(404, { 'Content-Type': 'application/json' });
	res.end(JSON.stringify({ error: 'not_found' }));
});

server.listen(PORT, () => console.log(`[echo-slot] ready on :${PORT} boot=${BOOT_TS}`));
