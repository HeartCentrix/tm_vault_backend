"""Regression: the DLQ consumer must watch every backup DLQ, not just the
four priority lanes.

MessageBus._declare_queue creates a ``<queue>.dlq`` for EVERY queue it
declares, and the main queue's x-dead-letter-exchange routes rejected
messages there (two delivery attempts, then park — see
shared/message_bus.consume). ``consume_backup_dlq`` in
services/backup-scheduler/main.py watched four of them:

    ["backup.urgent.dlq", "backup.high.dlq", "backup.normal.dlq",
     "backup.low.dlq"]

so anything parked off the heavy lane or any of the six partition lanes had
ZERO consumers and produced no alert. Confirmed on prod 2026-08-11: three
real poison USER_CHATS shard messages had been sitting in
``backup.chats_partition.dlq`` since 08-08/08-11 — a missing slice of a
user's chat backup that nothing would ever have reported, because
work_dead_letter is not read either.

The partition set is derived from shared.reconciler._PARTITION_QUEUE_BY_TYPE
(the single mirror of the publish-side routing, itself pinned by
tests/shared/test_reconciler_step_a_guard.py against the partition types the
workers actually persist) so a new partition type cannot land with a queue
nobody watches.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "services" / "backup-scheduler" / "main.py"
_spec = importlib.util.spec_from_file_location("bs_main_dlq_coverage", _MAIN_PATH)
_bs = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bs_main_dlq_coverage"] = _bs
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bs)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import-env guard
    pytest.skip(
        f"backup-scheduler module failed to import: {exc}",
        allow_module_level=True,
    )


def test_every_partition_dlq_is_consumed():
    """A poisoned shard is a hole in a backup. If nothing consumes its DLQ,
    nothing alerts and the hole is permanent and unrecorded."""
    from shared.reconciler import _PARTITION_QUEUE_BY_TYPE

    watched = set(_bs.backup_dlq_queue_names())
    for ptype, queue in _PARTITION_QUEUE_BY_TYPE.items():
        assert f"{queue}.dlq" in watched, (
            f"{ptype} shards dead-letter to {queue}.dlq and nothing consumes "
            f"it — exactly the state backup.chats_partition.dlq was found in "
            f"on 2026-08-11 (3 parked messages, no consumer, no alert)"
        )


def test_the_chats_partition_dlq_specifically_is_consumed():
    """Named on purpose: this is the queue that had real parked messages."""
    assert "backup.chats_partition.dlq" in _bs.backup_dlq_queue_names()


def test_priority_and_heavy_lanes_are_still_consumed():
    """The four lanes that were already covered must not be lost in the
    rewrite, and the heavy lane (oversized OneDrive drives — the biggest
    single backups in the fleet) has to join them."""
    from shared.config import settings

    watched = set(_bs.backup_dlq_queue_names())
    for queue in ("backup.urgent", "backup.high", "backup.normal",
                  "backup.low", settings.BACKUP_HEAVY_QUEUE,
                  settings.BACKUP_WORKER_QUEUE):
        assert f"{queue}.dlq" in watched, f"{queue}.dlq is unwatched"


def test_the_queue_list_has_no_duplicates():
    """BACKUP_WORKER_QUEUE defaults to backup.normal, which is already in the
    list — a duplicate would start two consumers on one queue and double
    every alert."""
    names = _bs.backup_dlq_queue_names()
    assert len(names) == len(set(names)), f"duplicate DLQ queues: {names}"
    assert all(n.endswith(".dlq") for n in names), names


def test_the_consumer_uses_the_derived_list():
    """Pins the wiring, not just the helper: a hand-written literal inside
    consume_backup_dlq is how the four-queue blind spot happened."""
    import inspect

    src = inspect.getsource(_bs.consume_backup_dlq)
    assert "backup_dlq_queue_names()" in src, (
        "consume_backup_dlq no longer derives its queue set — it can drift "
        "away from the declared queues again"
    )
    assert '"backup.urgent.dlq"' not in src, (
        "the hand-listed four-queue literal is back inside the consumer"
    )
