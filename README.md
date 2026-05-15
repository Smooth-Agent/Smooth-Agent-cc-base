# smoothagent/cc-base

The base container image for SmoothAgent agent runs.

> **Architecture v2 (2026-05-14)**: this image is no longer ephemeral-per-turn.
> The Fly Machine lives for many turns (managed by `core`'s `PoolManagerDO`),
> and inside it a single `claude` CLI process is kept alive across `/run`
> calls via `--input-format stream-json`. The ~20s OAuth handshake is paid
> ONCE per machine boot, not per turn. See `server.js:runPersistentClaude`
> for the lifecycle and `core/PLAN.md` for the full architecture.

This image runs an HTTP server (`/opt/smoothagent/server.js`) on port 8080
that accepts envelopes over `POST /run` and drives the long-running claude
process internally. Customers extend it to bring their own tools and scaffolds.

```
┌──────────────────────────────────────────────┐
│ smoothagent/cc-base:latest                   │
│   FROM node:22-slim                          │
│   + Claude Code CLI                          │
│   + non-root user (uid 996)                  │
│   + /opt/smoothagent/server.js (HTTP :8080)  │
│   + tini, jq, ca-certs, git                  │
└──────────────────────────────────────────────┘
                    │
                    │ FROM
                    ▼
┌──────────────────────────────────────────────┐
│ yourorg/your-builder:v1                      │
│   FROM smoothagent/cc-base:latest            │
│   RUN npm install -g vite your-sdk           │
│   COPY scaffolds/ /scaffolds/                │
│   ENV BUILD_CMD="npm run build"              │
└──────────────────────────────────────────────┘
                    │
                    │ pulled by
                    ▼
       SmoothAgent core (RunnerDO + PoolManagerDO)
       Fly Machines pool with sticky containerKey;
       long-running claude per machine.
```

## Why image-as-contract

Most agent platforms ship an SDK and ask customers to integrate against it in JS/Python/etc. SmoothAgent inverts that: **your container is the integration**. You bring the environment your agent needs (build tools, scaffolds, language runtimes, MCP servers, whatever). SmoothAgent runs it ephemerally, streams output back, persists files in a per-project volume.

What this gets you:

- **No SDK lock-in.** Your image runs anywhere Docker runs. Leave SmoothAgent and you keep your image.
- **Language-agnostic.** Use Node, Python, Go, Rust — SmoothAgent only cares about the entrypoint contract.
- **Versioning is your registry.** Your image at `v1`, `v2`, `v3` is your release lifecycle.
- **Customization is a `RUN` away.** Need a new tool? Extend the Dockerfile.

## Extending this image (the typical case)

```dockerfile
FROM smoothagent/cc-base:latest

# Add your tools.
RUN npm install --global vite typescript @your/sdk

# Drop in scaffolds the agent will start from.
COPY --chown=agent:agent scaffolds/ /scaffolds/

# Optional: customize default env so build mode picks them up.
ENV BUILD_CMD="npm run build" \
    PROJECT_TYPE="react"

# Do NOT change USER, WORKDIR, or ENTRYPOINT — the contract depends on them.
```

Build and push to any registry SmoothAgent can pull from:

```bash
docker build -t docker.io/yourorg/your-builder:v1 .
docker push docker.io/yourorg/your-builder:v1
```

Then register the image in your SmoothAgent agent config and start sending turns.

## The entrypoint contract

The entrypoint reads a single JSON object from stdin (≤1 MiB) and dispatches by `mode`. It emits newline-delimited JSON events on stdout. See [contract.json](./contract.json) for the full schema.

### Mode: `cc-cli`

Run Claude Code with a prompt. The runner uses this for normal agent turns.

```json
{
  "mode": "cc-cli",
  "prompt": "Add a dark mode toggle to the App component",
  "ccToken": "sk-ant-oat01-...",
  "model": "claude-sonnet-4-6",
  "sessionId": "abc-123",
  "systemPrompt": "You are working on a React app...",
  "maxTurns": 15,
  "mcpConfig": {
    "mcpServers": {
      "memory": { "type": "http", "url": "https://...", "headers": { "Authorization": "Bearer ..." } }
    }
  }
}
```

The entrypoint:
1. Validates input.
2. Writes `ccToken` to `~/.claude/.credentials.json` (mode 600). The token is never in argv, env, or logs.
3. Execs `claude --output-format stream-json --verbose --include-partial-messages …`.
4. The CLI's stream-json output is the rest of stdout. The runner reads, translates, and forwards as SSE.

### Mode: `build`

Run an arbitrary argv (typically the customer's build command). Output is wrapped as `log` events.

```json
{
  "mode": "build",
  "cmd": ["sh", "-c", "npm ci && npm run build"]
}
```

Emits `build_start`, then one `log` event per stdout/stderr line, then `build_done` with the exit code.

### Mode: `exec`

Same as `build` but for one-off commands (e.g. `git status`, `ls -la /workspace`).

## Filesystem layout

```
/workspace      ← per-(user, project) volume mounted by runner. Writable. Persists.
/tmp            ← tmpfs, cleared on container exit.
/opt/smoothagent ← image-bundled, read-only. Don't write here.
~/.claude       ← scratch dir for CC OAuth credentials. Cleared with /tmp on exit.
```

Customer images should put their scaffolds in `/scaffolds/` (or any non-`/workspace` path) and copy them into `/workspace` from a setup hook if needed.

## Security model

The runner spawns this image with hardening flags (memory limits, dropped capabilities, read-only root, network egress firewall). The image itself contributes:

| Layer | What |
|---|---|
| User | uid/gid 996 (`agent`). Never root. |
| Secrets | `ccToken` only via stdin, written to a 600-mode credentials file. Never echoed. |
| Init | `tini` so SIGTERM propagates to the CC subprocess for graceful shutdown. |
| Packages | Minimal: `ca-certificates`, `curl`, `jq`, `tini`, `git`. No build-essential, no setuid binaries beyond defaults. |

What this image does NOT control (the runner does):

- Memory and CPU limits.
- Network egress allowlist.
- Mount of `/workspace` from a Fly Volume.
- Concurrency caps per (user, project).
- Lifetime cap on the container.

Don't add features here that belong at the runner. Keep the image lean.

## Building cc-base itself

```bash
docker build --build-arg CC_VERSION=latest -t smoothagent/cc-base:dev .

# Smoke test: feed a build-mode envelope, expect events on stdout.
echo '{"mode":"build","cmd":["echo","hello from cc-base"]}' \
  | docker run --rm -i smoothagent/cc-base:dev
```

You should see `ready`, `build_start`, `log{line:"hello from cc-base"}`, `build_done{exitCode:0}` on stdout, one event per line.

For end-to-end verification with CC, you need a real OAuth token; see [test/cc-cli.sh](./test/cc-cli.sh).

## Versioning

`smoothagent/cc-base` follows the upstream Claude Code CLI version, with a small monotonically-increasing patch number for image-side changes:

```
smoothagent/cc-base:1.0.4-cc1.2.3
                     ▲       ▲
                     │       └── Claude Code CLI version
                     └────────── image rev (this Dockerfile + entrypoint)
```

`latest` always points to the most recent stable build.

## License

Apache-2.0. See [LICENSE](./LICENSE).

## Repository

This image's Dockerfile and entrypoint live at <https://github.com/Smooth-Agent/Smooth-Agent-cc-base>. Issues and PRs welcome.
