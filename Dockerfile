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

# Minimal packages: ca-certs for HTTPS, curl for healthchecks, jq for stdin parsing,
# tini as init for proper signal handling on container teardown.
RUN apt-get update \
 && apt-get install --yes --no-install-recommends \
    ca-certificates curl jq tini git \
 && rm -rf /var/lib/apt/lists/*

# Claude Code CLI (official npm package).
# Pinned via build arg so customers can rebuild against a known version.
ARG CC_VERSION=latest
RUN npm install --global --no-fund --no-audit "@anthropic-ai/claude-code@${CC_VERSION}" \
 && npm cache clean --force

# Entrypoint script. Read-only at runtime.
COPY --chown=root:root --chmod=0755 entrypoint.sh /opt/smoothagent/entrypoint.sh
COPY --chown=root:root --chmod=0644 contract.json /opt/smoothagent/contract.json

# Default workspace is the mount point Fly Volume / Docker volume hits.
WORKDIR /workspace

# Drop privileges. The runner spawns this image as user 996 already (--user 996)
# but we set USER too for any standalone / docker-run usage.
USER agent

# tini handles SIGTERM correctly so graceful shutdown of CC subprocess works.
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/smoothagent/entrypoint.sh"]
CMD []
