#!/bin/bash
# Smoke test for smoothagent/cc-base.
#
# Builds the image and verifies the entrypoint dispatches modes correctly.
# Does NOT exercise mode=cc-cli (that requires a real OAuth token; see test/cc-cli.sh).
#
# Usage:
#   ./test/smoke.sh           # build and test
#   IMAGE=...   ./test/smoke.sh   # test an existing image
#   NO_BUILD=1  ./test/smoke.sh   # skip docker build (use existing IMAGE)

set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
readonly IMAGE="${IMAGE:-smoothagent/cc-base:smoke-test}"

red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
blue()  { printf '\033[34m%s\033[0m\n' "$*"; }

fail() { red "FAIL: $*"; exit 1; }
pass() { green "PASS: $*"; }

# ---- Build (unless skipped) -----------------------------------------------

if [[ "${NO_BUILD:-0}" != "1" ]]; then
  blue "==> Building $IMAGE"
  docker build --tag "$IMAGE" "$REPO_ROOT" >&2
fi

# ---- Test 1: rejects empty stdin ------------------------------------------

blue "==> Test 1: rejects empty stdin"
out=$(echo -n '' | docker run --rm -i "$IMAGE" 2>&1 || true)
echo "$out" | grep -q '"code":"input_empty"' || fail "expected input_empty error, got: $out"
pass "rejects empty stdin"

# ---- Test 2: rejects malformed JSON ---------------------------------------

blue "==> Test 2: rejects malformed JSON"
out=$(echo 'not-json' | docker run --rm -i "$IMAGE" 2>&1 || true)
echo "$out" | grep -q '"code":"input_invalid"' || fail "expected input_invalid error, got: $out"
pass "rejects malformed JSON"

# ---- Test 3: build mode runs cmd and emits log events ---------------------

blue "==> Test 3: build mode echoes via log events"
out=$(echo '{"mode":"build","cmd":["echo","hello-cc-base"]}' | docker run --rm -i "$IMAGE")
echo "$out" | grep -q '"type":"ready"'        || fail "expected ready event"
echo "$out" | grep -q '"type":"build_start"'  || fail "expected build_start event"
echo "$out" | grep -q 'hello-cc-base'         || fail "expected echo output as log"
echo "$out" | grep -q '"type":"build_done"'   || fail "expected build_done event"
echo "$out" | grep -q '"exitCode":0'          || fail "expected exit code 0"
pass "build mode emits expected events"

# ---- Test 4: build mode propagates non-zero exit code ---------------------

blue "==> Test 4: build mode propagates failure"
out=$(echo '{"mode":"build","cmd":["sh","-c","exit 7"]}' | docker run --rm -i "$IMAGE" || true)
echo "$out" | grep -q '"exitCode":7' || fail "expected exit code 7, got: $out"
pass "propagates non-zero exit"

# ---- Test 5: exec mode honors env overrides --------------------------------

blue "==> Test 5: exec mode honors env"
out=$(echo '{"mode":"exec","cmd":["sh","-c","echo $GREETING"],"env":{"GREETING":"olá"}}' | docker run --rm -i "$IMAGE")
echo "$out" | grep -q 'olá' || fail "expected env override to apply"
pass "env overrides applied"

# ---- Test 6: rejects unknown mode -----------------------------------------

blue "==> Test 6: rejects unknown mode"
out=$(echo '{"mode":"telepathy"}' | docker run --rm -i "$IMAGE" 2>&1 || true)
echo "$out" | grep -q '"code":"mode_unknown"' || fail "expected mode_unknown error, got: $out"
pass "rejects unknown mode"

# ---- Test 7: cc-cli without token fails fast -------------------------------

blue "==> Test 7: cc-cli without token fails"
out=$(echo '{"mode":"cc-cli","prompt":"hi"}' | docker run --rm -i "$IMAGE" 2>&1 || true)
echo "$out" | grep -q '"code":"auth_missing"' || fail "expected auth_missing error, got: $out"
pass "cc-cli without token fails fast"

# ---- Test 8: runs as non-root (uid 996) -----------------------------------

blue "==> Test 8: runs as uid 996"
uid=$(echo '{"mode":"exec","cmd":["id","-u"]}' | docker run --rm -i "$IMAGE" | grep '"line":"' | head -1 | sed 's/.*"line":"\([0-9]*\)".*/\1/')
[[ "$uid" == "996" ]] || fail "expected uid 996, got: $uid"
pass "runs as uid 996"

# ---- Test 9: workdir is /workspace ----------------------------------------

blue "==> Test 9: workdir is /workspace"
out=$(echo '{"mode":"exec","cmd":["pwd"]}' | docker run --rm -i "$IMAGE")
echo "$out" | grep -q '"line":"/workspace"' || fail "expected workdir /workspace, got: $out"
pass "workdir is /workspace"

# ---- Test 10: claude binary is on PATH ------------------------------------

blue "==> Test 10: claude binary is installed"
out=$(echo '{"mode":"exec","cmd":["sh","-c","command -v claude"]}' | docker run --rm -i "$IMAGE" 2>&1)
echo "$out" | grep -q '/claude\|claude$' || fail "expected claude on PATH, got: $out"
pass "claude binary present"

# ---- Done -----------------------------------------------------------------

echo
green "All 10 smoke tests passed against $IMAGE"
