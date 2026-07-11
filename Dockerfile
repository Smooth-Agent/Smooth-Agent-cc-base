# SmoothAgent base image for ephemeral agent runs.
#
# Customers extend this with their own tools/scaffolds:
#
#   FROM smoothagent/cc-base:latest
#   RUN npm install -g vite @your/sdk
#   COPY scaffolds/ /scaffolds/
#
# Contract: see README.md and ENTRYPOINT.md
# License: Apache-2.0

FROM node:22-slim AS base

# Non-root user. uid/gid 996 matches host conventions for service users.
RUN groupadd --system --gid 996 agent \
 && useradd  --system --uid 996 --gid agent \
             --home-dir /workspace --shell /bin/bash --create-home agent

# Minimal packages: ca-certs for HTTPS, curl + jq kept for the AGENT's own bash
# tool calls (not for our boot path), git for repo work, tini as init for proper
# signal handling on container teardown. (unzip removed with rclone, 2026-07-07.)
RUN apt-get update \
 && apt-get install --yes --no-install-recommends \
    ca-certificates curl jq tini git \
 && rm -rf /var/lib/apt/lists/*

# Claude Code CLI (official npm package).
# Pinned via build arg so customers can rebuild against a known version.
ARG CC_VERSION=latest
RUN npm install --global --no-fund --no-audit "@anthropic-ai/claude-code@${CC_VERSION}" \
 && npm cache clean --force

# (rclone REMOVED 2026-07-07 — server.js's R2 sync was dead code behind
# `if (false && ...)`; the ~50 MB binary only bloated the image and slowed
# Detona template imports/pulls. See git history to resurrect.)
# Create the dir EXPLICITLY with the x bit. It used to be born 0755 as a side
# effect of the first COPY being entrypoint.sh --chmod=0755; when that file was
# removed (2026-07-07) the first COPY became contract.json --chmod=0644 and the
# implicitly-created dir inherited 0644 — no x bit, so the non-root agent user
# couldn't path-lookup INTO it: node died with "Cannot find module
# /opt/smoothagent/server.js" on Detona's ext4 rootfs (Docker's overlay happened
# to forgive it, which is why CI smoke passed — never trust implicit dir modes).
RUN mkdir -p -m 0755 /opt/smoothagent
COPY --chown=root:root --chmod=0644 contract.json /opt/smoothagent/contract.json
COPY --chown=root:root --chmod=0755 server.js /opt/smoothagent/server.js
# relay.js — required by server.js (per-turn retain/send/save transport). MUST be
# copied or the HTTP /run path fails with "Cannot find module './relay'".
COPY --chown=root:root --chmod=0644 relay.js /opt/smoothagent/relay.js
# echo-slot.js — the SLOT_CONTRACT reference implementation (Fase 1 PoC). Tiny;
# ships in the base so the slot pipeline is testable on ANY box without a custom
# image. Real clients bring their own server (own image/layer) instead.
COPY --chown=root:root --chmod=0644 echo-slot.js /opt/smoothagent/echo-slot.js

# HTTP server port. Runner spawns the machine with init.cmd pointing at server.js;
# Fly routes app traffic to this port via [services] config.
EXPOSE 8080

# Default workspace is the mount point Fly Volume / Docker volume hits.
WORKDIR /workspace

# Drop privileges. The runner spawns this image as user 996 already (--user 996)
# but we set USER too for any standalone / docker-run usage.
USER agent

# ONE entry mode: the HTTP server. Listens on :8080, accepts POST /run with the
# envelope JSON, streams claude output back chunked. (Production/Detona overrides
# with its own SERVER_CMD, but the default now matches production instead of the
# legacy stdin-pipe entrypoint.sh — REMOVED 2026-07-07: it re-implemented the boot
# divergently and wrote ACCESS-ONLY credentials, violating the token doctrine
# [claude can't self-rotate without the refresh token]. A doctrine-violating
# loaded gun as default entry was exactly the class of latent bug the audit hunts.)
#
# tini handles SIGTERM so claude / node child processes shut down cleanly.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/node", "/opt/smoothagent/server.js"]
CMD []
