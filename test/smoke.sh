#!/bin/bash
# Smoke test for smoothagent/cc-base — exercises the HTTP SERVER path, which is
# what production (Detona) actually runs. (The legacy stdin-pipe entrypoint.sh
# was removed 2026-07-07; the old smoke test only covered that dead path.)
#
# Covers: /health, auth pin (TOFU: first request pins X-API-Key, wrong/no key
# 401s, /stream protected too), mode=build|exec via POST /run, malformed JSON,
# unknown mode, non-root uid, workdir.
# Does NOT exercise mode=cc-cli (needs a real OAuth token; see test/cc-cli.sh).
#
# Usage:
#   ./test/smoke.sh              # build and test
#   IMAGE=...   ./test/smoke.sh  # test an existing image
#   NO_BUILD=1  ./test/smoke.sh  # skip docker build (use existing IMAGE)

set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
readonly IMAGE="${IMAGE:-smoothagent/cc-base:smoke-test}"
readonly KEY="smoke-test-key-123"
readonly PORT="${PORT:-18080}"
readonly URL="http://localhost:$PORT"

red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
blue()  { printf '\033[34m%s\033[0m\n' "$*"; }

cleanup() { docker rm -f cc-smoke >/dev/null 2>&1 || true; }
fail() { red "FAIL: $*"; cleanup; exit 1; }
pass() { green "PASS: $*"; }

# One /run per curl; the server serializes turns (409 while in flight), so each
# test waits for the previous curl to finish — sequential curls are safe.
run() { curl -s -X POST "$URL/run" -H "X-API-Key: $KEY" -H 'Content-Type: application/json' -d "$1"; }

# ---- Build (unless skipped) -----------------------------------------------

if [[ "${NO_BUILD:-0}" != "1" ]]; then
  blue "==> Building $IMAGE"
  docker build --tag "$IMAGE" "$REPO_ROOT" >&2
fi

# ---- Boot the server (the production path — image default ENTRYPOINT) ------

blue "==> Booting server container on :$PORT"
cleanup
docker run --detach --rm --name cc-smoke --publish "$PORT:8080" "$IMAGE" >/dev/null

for i in $(seq 1 30); do
  if curl -sf "$URL/health" >/dev/null 2>&1; then break; fi
  [[ $i == 30 ]] && fail "server never became healthy"
  sleep 0.5
done
pass "server boots (default ENTRYPOINT) and /health responds"

# ---- Test 0: pre-pin surface (Detona probe compatibility) --------------------
# Before the first POST /run, no key is pinned: GET / and /health answer openly
# (Detona's build readiness probe), and /stream 204s (nothing to protect yet).

blue "==> Test 0: pre-pin — GET / open (probe), /stream 204"
code=$(curl -s -o /dev/null -w '%{http_code}' "$URL/")
[[ "$code" == "200" ]] || fail "expected 200 on GET / (probe path), got $code"
code=$(curl -s -o /dev/null -w '%{http_code}' "$URL/stream")
[[ "$code" == "204" ]] || fail "expected 204 on pre-pin /stream (no turn), got $code"
pass "pre-pin: GET / 200, /stream 204"

# ---- Test 1: first POST /run pins the key + exec works -----------------------

blue "==> Test 1: first /run pins the key; mode=exec honors env"
out=$(run '{"mode":"exec","cmd":["sh","-c","echo $GREETING"],"env":{"GREETING":"ola-smoke"}}')
echo "$out" | grep -q '"type":"ready"' || fail "expected ready event, got: $out"
echo "$out" | grep -q 'ola-smoke'      || fail "expected env echo, got: $out"
pass "exec via POST /run with pinned key"

# ---- Test 2: wrong key → 401 -------------------------------------------------

blue "==> Test 2: wrong key rejected"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$URL/run" \
  -H 'X-API-Key: wrong-key' -H 'Content-Type: application/json' \
  -d '{"mode":"exec","cmd":["echo","x"]}')
[[ "$code" == "401" ]] || fail "expected 401 with wrong key, got $code"
pass "wrong key → 401"

# ---- Test 3: NO key → 401 ----------------------------------------------------

blue "==> Test 3: missing key rejected"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$URL/run" \
  -H 'Content-Type: application/json' -d '{"mode":"exec","cmd":["echo","x"]}')
[[ "$code" == "401" ]] || fail "expected 401 with no key, got $code"
pass "no key → 401"

# ---- Test 4: /stream protected too -------------------------------------------

blue "==> Test 4: /stream requires the key"
code=$(curl -s -o /dev/null -w '%{http_code}' "$URL/stream")
[[ "$code" == "401" ]] || fail "expected 401 on unauthenticated /stream, got $code"
# authed /stream after a turn 200-replays the retained relay — both fine here.
code=$(curl -s -o /dev/null -w '%{http_code}' -H "X-API-Key: $KEY" "$URL/stream")
[[ "$code" == "200" || "$code" == "204" ]] || fail "expected 200/204 on authed /stream, got $code"
pass "/stream: 401 without key, authed ok"

# ---- Test 5: malformed JSON ---------------------------------------------------

blue "==> Test 5: malformed JSON rejected"
out=$(curl -s -X POST "$URL/run" -H "X-API-Key: $KEY" -d 'not-json')
echo "$out" | grep -q 'invalid_json' || fail "expected invalid_json, got: $out"
pass "malformed JSON → invalid_json"

# ---- Test 6: build mode + exit code -------------------------------------------

blue "==> Test 6: build mode events + non-zero exit"
out=$(run '{"mode":"build","cmd":["echo","hello-cc-base"]}')
echo "$out" | grep -q '"type":"build_start"' || fail "expected build_start, got: $out"
echo "$out" | grep -q 'hello-cc-base'        || fail "expected echo output, got: $out"
echo "$out" | grep -q '"exitCode":0'         || fail "expected exitCode 0, got: $out"
out=$(run '{"mode":"build","cmd":["sh","-c","exit 7"]}')
echo "$out" | grep -q '"exitCode":7' || fail "expected exitCode 7, got: $out"
pass "build events + exit code propagated"

# ---- Test 7: unknown mode ------------------------------------------------------

blue "==> Test 7: unknown mode rejected"
out=$(run '{"mode":"telepathy"}')
echo "$out" | grep -q 'mode_unknown' || fail "expected mode_unknown, got: $out"
pass "unknown mode → mode_unknown"

# ---- Test 8: runs as non-root (uid 996) + workdir ------------------------------

blue "==> Test 8: uid 996 + /workspace"
out=$(run '{"mode":"exec","cmd":["sh","-c","echo uid=$(id -u) pwd=$PWD"]}')
echo "$out" | grep -q 'uid=996'        || fail "expected uid 996, got: $out"
echo "$out" | grep -q 'pwd=/workspace' || fail "expected /workspace, got: $out"
pass "non-root uid 996, workdir /workspace"

# ---- Done -----------------------------------------------------------------------

cleanup
green "ALL SMOKE TESTS PASSED"
