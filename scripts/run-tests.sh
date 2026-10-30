#!/usr/bin/env bash
# Run the test suite with the dependencies its modules actually need.
#
# Bare `python3 -m pytest` on this repo silently skips ~20 backup-worker
# modules (azure / aiohttp missing) — including the chat-drain-honesty and
# shard-failure-accounting suites — and still exits 0 as long as one other
# test passes. The repo-root conftest.py now fails the run in that case;
# this script is how you make it pass properly.
#
# Usage:
#   ./scripts/run-tests.sh                       # whole suite, unit only
#   ./scripts/run-tests.sh tests/workers -q      # any pytest args
#
# Env:
#   VENV=/path/to/venv   reuse a different venv (default .venv-test)
set -euo pipefail

cd "$(dirname "$0")/.."
VENV="${VENV:-.venv-test}"
PY="$VENV/bin/python"

if [ ! -x "$PY" ]; then
  echo "==> creating $VENV"
  python3 -m venv "$VENV"
fi

# -r requirements-test.txt is cheap to re-check and keeps a stale venv honest.
echo "==> syncing test dependencies"
"$VENV/bin/pip" install --quiet --disable-pip-version-check -r requirements-test.txt

# shared/config.py refuses to import without a non-placeholder JWT secret.
# Ephemeral per-run value: never reused, never a real secret.
export ALLOW_DEV_JWT_SECRETS=true
export JWT_SECRET="${JWT_SECRET:-$("$PY" -c 'import secrets;print(secrets.token_hex(32))')}"

# Default to the unit suite; the marked suites need docker services.
if [ "$#" -eq 0 ]; then
  set -- tests/ -q -m "not integration and not slow and not chaos and not worm"
fi

echo "==> pytest $*"
exec "$PY" -m pytest "$@"
