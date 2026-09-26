#!/usr/bin/env node
/**
 * os-stub.js — o server.js da BASE FIXA (cc-os). Nao roda turno nenhum.
 *
 * Existe porque o import do Detona sobe a VM e espera a porta responder antes de
 * marcar o template READY — base sem server nao importa. O runtime de verdade
 * (server.js, relay.js, claude, codex) chega pelo layer `smooth-runtime`, que monta
 * por cima deste arquivo no MESMO caminho (/opt/smoothagent/server.js) e ganha.
 *
 * Box sem o layer montado cai aqui: /health responde (o probe passa) e todo o
 * resto devolve 503 `runtime_layer_missing` — falha alta, nunca um turno vazio.
 */
const http = require('node:http');

http.createServer((req, res) => {
	if (req.method === 'GET' && (req.url === '/health' || req.url === '/')) {
		res.writeHead(200, { 'Content-Type': 'application/json' });
		res.end(JSON.stringify({ status: 'ok', inFlight: false, stub: true }));
		return;
	}
	res.writeHead(503, { 'Content-Type': 'application/json' });
	res.end(JSON.stringify({ error: 'runtime_layer_missing', message: 'box sem o layer smooth-runtime montado' }));
}).listen(8080, '0.0.0.0');
