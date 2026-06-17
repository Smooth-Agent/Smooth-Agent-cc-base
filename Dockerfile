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

# Minimal packages: ca-certs for HTTPS, curl for healthchecks + rclone install,
# jq for stdin parsing, unzip for rclone release zip, tini as init for proper
# signal handling on container teardown.
RUN apt-get update \
 && apt-get install --yes --no-install-recommends \
    ca-certificates curl jq unzip tini git \
 && rm -rf /var/lib/apt/lists/*

# Claude Code CLI (official npm package).
# Pinned via build arg so customers can rebuild against a known version.
ARG CC_VERSION=latest
RUN npm install --global --no-fund --no-audit "@anthropic-ai/claude-code@${CC_VERSION}" \
 && npm cache clean --force

# rclone — used by server.js to sync /workspace to/from Cloudflare R2 each turn.
# We replace the previous Fly Volume model: the volume was per-project persistent
# state (~30 MB per chat), now /workspace lives in tmpfs and is synced to R2 at
# turn boundaries. R2 storage is ~10× cheaper than Fly Volumes and we don't pay
# for idle space.
#
# Multi-arch: dpkg --print-architecture returns 'amd64' or 'arm64'; rclone
# releases publish both. Pinned version for reproducibility.
ARG RCLONE_VERSION=v1.69.0
RUN ARCH=$(dpkg --print-architecture) \
 && curl -fsSL "https://github.com/rclone/rclone/releases/download/${RCLONE_VERSION}/rclone-${RCLONE_VERSION}-linux-${ARCH}.zip" -o /tmp/rclone.zip \
 && unzip -q /tmp/rclone.zip -d /tmp \
 && mv "/tmp/rclone-${RCLONE_VERSION}-linux-${ARCH}/rclone" /usr/local/bin/rclone \
 && chmod 0755 /usr/local/bin/rclone \
 && rm -rf /tmp/rclone*

# Entrypoint script (legacy stdin-pipe path) + HTTP server (preferred path).
COPY --chown=root:root --chmod=0755 entrypoint.sh /opt/smoothagent/entrypoint.sh
COPY --chown=root:root --chmod=0644 contract.json /opt/smoothagent/contract.json
COPY --chown=root:root --chmod=0755 server.js /opt/smoothagent/server.js
# relay.js — required by server.js (per-turn retain/send/save transport). MUST be
# copied or the HTTP /run path fails with "Cannot find module './relay'".
COPY --chown=root:root --chmod=0644 relay.js /opt/smoothagent/relay.js

# HTTP server port. Runner spawns the machine with init.cmd pointing at server.js;
# Fly routes app traffic to this port via [services] config.
EXPOSE 8080

# Default workspace is the mount point Fly Volume / Docker volume hits.
WORKDIR /workspace

# Drop privileges. The runner spawns this image as user 996 already (--user 996)
# but we set USER too for any standalone / docker-run usage.
USER agent

# Two entry modes (selected by Fly init.cmd or `docker run` cmd):
#
#   1. HTTP server (production):
#      init.cmd = ["/usr/local/bin/node", "/opt/smoothagent/server.js"]
#      → Listens on :8080, accepts POST /run with envelope JSON,
#        streams claude output back chunked.
#
#   2. Legacy stdin-pipe (smoke tests, docker run -i):
#      no override → ENTRYPOINT below runs entrypoint.sh which reads
#      stdin envelope and exits. Used by test/smoke.sh.
#
# tini handles SIGTERM so claude / node child processes shut down cleanly.
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/smoothagent/entrypoint.sh"]
CMD []
