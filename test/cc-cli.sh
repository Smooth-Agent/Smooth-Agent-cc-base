#!/bin/bash
# End-to-end test for mode=cc-cli.
#
# Requires a real Claude Code OAuth token in CC_TOKEN env var.
# DO NOT commit your token. Pass it inline:
#
#   CC_TOKEN='sk-ant-oat01-...' ./test/cc-cli.sh

set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
readonly IMAGE="${IMAGE:-smoothagent/cc-base:smoke-test}"

: "${CC_TOKEN:?CC_TOKEN env var required (Claude Code OAuth access token)}"

if [[ "${NO_BUILD:-0}" != "1" ]]; then
  docker build --tag "$IMAGE" "$REPO_ROOT" >&2
fi

# Send a trivial prompt. CC should respond with stream-json events.
input=$(jq -nc --arg t "$CC_TOKEN" '{
  mode: "cc-cli",
  prompt: "Say the single word ACK and nothing else.",
  ccToken: $t,
  maxTurns: 1
}')

echo "$input" | docker run --rm -i "$IMAGE" | tee /tmp/cc-cli-out.log

# Validate: should have at least the "ready" event from entrypoint plus CC events.
grep -q '"type":"ready"' /tmp/cc-cli-out.log || { echo "FAIL: no ready event"; exit 1; }
grep -qi 'ack' /tmp/cc-cli-out.log           || { echo "FAIL: CC didn't echo ACK"; exit 1; }

echo "PASS: cc-cli mode produced expected output"
