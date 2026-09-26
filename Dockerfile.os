# cc-os — a BASE FIXA das boxes do SmoothAgent (2026-09-26).
#
# So o que quase nunca muda: SO, node, o user `agent` (996) e um stub que responde
# /health (o import do Detona exige o server de pe). Importada UMA vez como template
# e publicada num alias de app. O nosso runtime (server.js, relay.js, Claude Code,
# codex) NAO mora aqui: vem pelo layer `smooth-runtime` (o Dockerfile ao lado,
# FROM esta imagem), que se atualiza re-publicando o mesmo nome — toda box converge
# no proximo acordar, /data intacto.
#
# Mudou algo aqui? Nova tag os-vN + reimport + publish-base no alias. Raro.

FROM node:22-slim

RUN groupadd --system --gid 996 agent \
 && useradd  --system --uid 996 --gid agent \
             --home-dir /workspace --shell /bin/bash --create-home agent

# curl + jq + git ficam pro bash do AGENTE; tini como init.
RUN apt-get update \
 && apt-get install --yes --no-install-recommends \
    ca-certificates curl jq tini git \
 && rm -rf /var/lib/apt/lists/*

# Diretorio criado EXPLICITO com o bit x (ver a nota no Dockerfile do runtime:
# dir implicito nasceu 0644 uma vez e o node do user 996 nao entrava).
RUN mkdir -p -m 0755 /opt/smoothagent
COPY --chown=root:root --chmod=0755 os-stub.js /opt/smoothagent/server.js

EXPOSE 8080
WORKDIR /workspace
USER agent
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/node", "/opt/smoothagent/server.js"]
CMD []
