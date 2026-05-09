#!/bin/bash
# SmoothAgent base entrypoint.
#
# Reads a JSON envelope from stdin describing the turn, dispatches to the
# correct execution mode, streams stream-json events on stdout.
#
# This script is intentionally short. All security guarantees come from the
# container runtime flags (memory/cpu limits, read-only root, dropped caps,
# egress firewall) that the runner sets at spawn time. This script must NOT
# be trusted with secrets handling beyond what stdin provides.
#
# Input format (JSON, single object on stdin):
#   {
#     "mode":        "cc-cli" | "build" | "exec",
#     "prompt":      "<user prompt>",          // mode=cc-cli
#     "ccToken":     "sk-ant-oat01-...",       // mode=cc-cli, OAuth access token
#     "model":       "claude-sonnet-4-6",      // optional, mode=cc-cli
#     "sessionId":   "<resume id>",            // optional, mode=cc-cli
#     "systemPrompt":"<system prompt>",        // optional, mode=cc-cli
#     "maxTurns":    15,                        // optional, mode=cc-cli
#     "mcpConfig":   { ... },                   // optional, mode=cc-cli
#     "cmd":         ["sh", "-c", "npm run build"], // mode=build|exec
#     "env":         { "KEY": "val" }           // optional, all modes
#   }
#
# Output: stream-json events on stdout, one per line. See contract.json.

set -euo pipefail

readonly CONTRACT_PATH="/opt/smoothagent/contract.json"
readonly CC_CONFIG_DIR="${HOME}/.claude"

# ---- Helpers --------------------------------------------------------------

# Emit a JSON event line on stdout.
emit() {
  local type="$1"; shift
  local data="${1:-{}}"
  printf '{"type":"%s","data":%s,"ts":%s}\n' "$type" "$data" "$(date +%s%3N)"
}

# Emit an error event and exit non-zero.
fatal() {
  local code="$1"; shift
  local msg="$1"; shift
  emit error "$(jq -nc --arg code "$code" --arg msg "$msg" '{code:$code,message:$msg}')"
  exit 1
}

# Apply env overrides from input.
apply_env() {
  local input="$1"
  local keys
  keys=$(printf '%s' "$input" | jq -r '.env // {} | keys[]?' 2>/dev/null || true)
  while IFS= read -r key; do
    [[ -z "$key" ]] && continue
    local val
    val=$(printf '%s' "$input" | jq -r --arg k "$key" '.env[$k]')
    export "$key=$val"
  done <<< "$keys"
}

# Configure CC OAuth credentials from token. Token never written to logs.
configure_cc_auth() {
  local token="$1"
  [[ -z "$token" || "$token" == "null" ]] && fatal auth_missing "ccToken required for mode=cc-cli"

  mkdir -p "$CC_CONFIG_DIR"
  chmod 700 "$CC_CONFIG_DIR"

  # Credentials file format expected by Claude Code CLI.
  # The CLI looks for OAuth tokens here when ANTHROPIC_API_KEY is unset.
  jq -nc --arg t "$token" '{
    claudeAiOauth: {
      accessToken: $t,
      scopes: ["user:inference"]
    }
  }' > "$CC_CONFIG_DIR/.credentials.json"
  chmod 600 "$CC_CONFIG_DIR/.credentials.json"
}

# ---- Main -----------------------------------------------------------------

# Read entire stdin as JSON envelope. 1MB cap to bound memory.
INPUT=$(head --bytes=1048576)
[[ -z "$INPUT" ]] && fatal input_empty "no input on stdin"

# Validate envelope.
echo "$INPUT" | jq empty 2>/dev/null || fatal input_invalid "stdin is not valid JSON"

MODE=$(echo "$INPUT" | jq -r '.mode // "cc-cli"')

emit ready "$(jq -nc --arg mode "$MODE" --arg cwd "$(pwd)" '{mode:$mode,cwd:$cwd}')"

apply_env "$INPUT"

case "$MODE" in
  cc-cli)
    PROMPT=$(echo "$INPUT" | jq -r '.prompt // ""')
    [[ -z "$PROMPT" ]] && fatal prompt_missing "prompt required for mode=cc-cli"

    CC_TOKEN=$(echo "$INPUT" | jq -r '.ccToken // ""')
    configure_cc_auth "$CC_TOKEN"

    # Build CLI args from optional fields.
    args=(--output-format stream-json --verbose --include-partial-messages)

    MODEL=$(echo "$INPUT" | jq -r '.model // ""')
    [[ -n "$MODEL" && "$MODEL" != "null" ]] && args+=(--model "$MODEL")

    SESSION=$(echo "$INPUT" | jq -r '.sessionId // ""')
    [[ -n "$SESSION" && "$SESSION" != "null" ]] && args+=(--session-id "$SESSION")

    SYSTEM=$(echo "$INPUT" | jq -r '.systemPrompt // ""')
    [[ -n "$SYSTEM" && "$SYSTEM" != "null" ]] && args+=(--system-prompt "$SYSTEM")

    MAX_TURNS=$(echo "$INPUT" | jq -r '.maxTurns // ""')
    [[ -n "$MAX_TURNS" && "$MAX_TURNS" != "null" ]] && args+=(--max-turns "$MAX_TURNS")

    # MCP config goes via temp file (CLI accepts a path).
    MCP_CONFIG=$(echo "$INPUT" | jq -c '.mcpConfig // null')
    if [[ "$MCP_CONFIG" != "null" ]]; then
      MCP_FILE=$(mktemp)
      printf '%s' "$MCP_CONFIG" > "$MCP_FILE"
      args+=(--mcp-config "$MCP_FILE")
      trap 'rm -f "$MCP_FILE"' EXIT
    fi

    # Run CC. stdout from claude is the SSE stream we forward as-is.
    # stderr is captured for diagnostics but not inlined into protocol.
    exec claude "${args[@]}" "$PROMPT"
    ;;

  build|exec)
    # Cmd is an argv array. Execute directly without shell unless customer asked.
    CMD=$(echo "$INPUT" | jq -c '.cmd // []')
    if [[ "$CMD" == "[]" || -z "$CMD" ]]; then
      fatal cmd_missing "cmd required for mode=$MODE"
    fi

    # Convert JSON array to bash array.
    mapfile -t cmd_array < <(echo "$CMD" | jq -r '.[]')

    emit build_start "$(jq -nc --argjson cmd "$CMD" '{cmd:$cmd}')"

    # Execute, capture exit code, stream stdout/stderr line by line as events.
    set +e
    "${cmd_array[@]}" 2>&1 | while IFS= read -r line; do
      emit log "$(jq -nc --arg line "$line" '{line:$line}')"
    done
    rc=${PIPESTATUS[0]}
    set -e

    emit build_done "$(jq -nc --argjson code "$rc" '{exitCode:$code}')"
    exit "$rc"
    ;;

  *)
    fatal mode_unknown "unknown mode: $MODE (expected cc-cli|build|exec)"
    ;;
esac
