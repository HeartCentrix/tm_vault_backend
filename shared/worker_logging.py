"""Stdout visibility for shared.* INFO logs in print()-convention workers.

backup-worker and restore-worker never configure logging, so Python's
last-resort handler drops everything below WARNING. That silenced the
"[CARRY_FORWARD] waiting" line — the designed replacement monitoring signal
for advisory try-lock slot waiters (invisible in pg_locks) — while the
8/26 and 8/27 archive runs starved for 4h before dying on the wait ceiling.

Workers call ensure_worker_stdout_logging() at import time. Services that
configure logging themselves (basicConfig/uvicorn/dictConfig) are left
alone: an existing root or "shared" handler means someone already decided
where logs go.
"""
from __future__ import annotations

import logging
import sys

_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def ensure_worker_stdout_logging(level: int = logging.INFO) -> bool:
    """Attach one stdout handler to the "shared" logger tree.

    Returns True if a handler was attached, False if an existing
    configuration (root or "shared" handlers) made this a no-op.
    Idempotent: safe to call from every worker entrypoint.
    """
    shared_logger = logging.getLogger("shared")
    if shared_logger.handlers or logging.getLogger().handlers:
        return False
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(_FORMAT))
    shared_logger.addHandler(handler)
    shared_logger.setLevel(level)
    # A later root configuration must not double-emit these records.
    shared_logger.propagate = False
    return True
