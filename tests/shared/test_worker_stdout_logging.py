"""2026-08-27 monitoring-gap fix: shared.* INFO logs must reach worker stdout.

shared/database.py designates the "[CARRY_FORWARD] waiting" logger.info line
as the REPLACEMENT monitoring signal for try-lock slot waiters (they are
invisible in pg_locks). But backup-worker and restore-worker configure no
logging at all — Python's last-resort handler drops INFO — so the designed
early-warning line never reached pod logs while the 8/26 + 8/27 archive
runs starved for 4h. ensure_worker_stdout_logging() closes the gap:

  * attaches ONE stdout StreamHandler to the "shared" logger tree at INFO,
  * is idempotent (workers restart handlers must not stack),
  * defers to an existing configuration (root or "shared" handlers) —
    services that run basicConfig/uvicorn logging must not get duplicates,
  * sets propagate=False so a later root configuration cannot double-emit.
"""
from __future__ import annotations

import io
import logging
import os
import pathlib

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

from shared.worker_logging import ensure_worker_stdout_logging  # noqa: E402

REPO = pathlib.Path(__file__).resolve().parents[2]


def _clean():
    root = logging.getLogger()
    shared = logging.getLogger("shared")
    saved = (root.handlers[:], shared.handlers[:], shared.level, shared.propagate)
    root.handlers[:] = []
    shared.handlers[:] = []
    shared.setLevel(logging.NOTSET)
    shared.propagate = True
    return saved


def _restore(saved):
    root = logging.getLogger()
    shared = logging.getLogger("shared")
    root.handlers[:], shared.handlers[:], level, prop = saved
    shared.setLevel(level)
    shared.propagate = prop


def test_attaches_stdout_info_handler_and_emits():
    saved = _clean()
    try:
        assert ensure_worker_stdout_logging() is True
        shared = logging.getLogger("shared")
        assert len(shared.handlers) == 1
        handler = shared.handlers[0]
        assert isinstance(handler, logging.StreamHandler)
        # The whole point: INFO from shared.database must come out.
        buf = io.StringIO()
        handler.setStream(buf)
        logging.getLogger("shared.database").info(
            "[CARRY_FORWARD] waiting for slot bucket=%d", 4
        )
        assert "[CARRY_FORWARD] waiting for slot bucket=4" in buf.getvalue()
        assert shared.propagate is False
    finally:
        _restore(saved)


def test_idempotent_across_calls():
    saved = _clean()
    try:
        assert ensure_worker_stdout_logging() is True
        assert ensure_worker_stdout_logging() is False
        assert len(logging.getLogger("shared").handlers) == 1
    finally:
        _restore(saved)


def test_defers_to_existing_root_configuration():
    saved = _clean()
    try:
        logging.getLogger().addHandler(logging.NullHandler())
        assert ensure_worker_stdout_logging() is False
        assert logging.getLogger("shared").handlers == []
    finally:
        _restore(saved)


def test_workers_call_it_at_startup():
    # Source pins: both print()-convention workers must opt in near the top
    # of main — the signal is useless if nothing installs the handler.
    for rel in ("workers/backup-worker/main.py", "workers/restore-worker/main.py"):
        src = (REPO / rel).read_text()
        i = src.find("ensure_worker_stdout_logging()")
        assert i != -1, f"{rel} never enables shared.* INFO visibility"
        assert i < 6000, (
            f"{rel}: ensure_worker_stdout_logging() must run at import/startup, "
            "before any carry-forward wait can begin"
        )
