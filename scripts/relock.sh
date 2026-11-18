#!/usr/bin/env bash
# Regenerate every requirements.lock from its requirements.txt (the
# human-edited source). Dockerfiles install from the .lock only.
# Usage: scripts/relock.sh [--upgrade]   (needs uv: https://docs.astral.sh/uv/)
set -euo pipefail
cd "$(dirname "$0")/.."
for src in requirements.base.txt requirements-test.txt api-gateway/requirements.txt \
           services/*/requirements.txt workers/*/requirements.txt; do
  py=3.11
  [ "$src" = workers/chat-export-worker/requirements.txt ] && py=3.12  # python:3.12-slim image
  uv pip compile -q "$@" "$src" -o "${src%.txt}.lock" \
    --python-version "$py" --python-platform x86_64-unknown-linux-gnu \
    --custom-compile-command scripts/relock.sh
done
