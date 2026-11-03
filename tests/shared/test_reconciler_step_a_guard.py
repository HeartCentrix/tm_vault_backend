"""CI guard for STEP A's never-claimed-partition protection.

Row-level semantics live in tests/shared/test_reconciler_unclaimed_partitions.py,
which needs a real Postgres (CTE + FOR UPDATE SKIP LOCKED + enum casts)
and skips without RECONCILER_TEST_PG_URL. This file runs everywhere and
pins the two pieces that must not silently regress:

  * STEP A binds a separate, longer horizon for never-claimed shards, and
  * the poison-pill branch requires evidence that a worker actually
    claimed the shard before it may mark it FAILED.

Without them a QUEUED shard whose RabbitMQ message hasn't been popped yet
(lease_owner_id and lease_expires_at both NULL — the lease guard matches
vacuously) is requeue-counted every 15 min and poisoned to FAILED after
~45 min of simply waiting in a saturated queue.
"""
from __future__ import annotations

import json
import asyncio
from typing import Any, Dict, List, Optional, Tuple


class _StubResult:
    def first(self):
        return None

    def all(self):
        return []


class _RecordingSession:
    """Captures (sql, binds) for every statement the sweep issues."""

    def __init__(self):
        self.executed: List[Tuple[str, Dict]] = []
        self.committed = False

    async def execute(self, statement, binds: Optional[Dict] = None):
        self.executed.append((str(statement), dict(binds or {})))
        return _StubResult()

    async def commit(self):
        self.committed = True

    async def rollback(self):
        pass


def _run(coro: Any):
    """Run on a live loop, installing one if the suite left none behind.

    Other modules in this suite use asyncio.run(), which uninstalls the
    current event loop; get_event_loop() then raises depending on test
    order. Leave the loop installed (not closed) so later modules find a
    usable one too.
    """
    try:
        loop = asyncio.get_event_loop_policy().get_event_loop()
        if loop.is_closed():
            raise RuntimeError("closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


def _set_clause(sql: str) -> str:
    """The SET ... (FROM|WHERE|RETURNING) span of an UPDATE, for column checks."""
    import re
    m = re.search(
        r"\bSET\b(.*?)\n\s*(FROM|WHERE|RETURNING)\b", sql, re.DOTALL,
    )
    assert m, f"could not locate a SET clause in: {sql}"
    return m.group(1)


def test_step_a_binds_a_longer_horizon_for_never_claimed_partitions():
    import shared.reconciler as reconciler

    session = _RecordingSession()
    _run(reconciler.sweep_orphans(session))

    # The never-claimed republish is its own statement (see
    # test_never_claimed_republish_does_not_spend_the_requeue_budget), so
    # locate it by the bind it owns rather than by CTE name.
    cte_sql, cte_binds = next(
        (s, b) for s, b in session.executed if ":unclaimed_stale" in s
    )
    claimed_sql, claimed_binds = next(
        (s, b) for s, b in session.executed if "WITH stale AS" in s
    )
    assert cte_binds["unclaimed_stale"] == (
        reconciler.RECONCILER_PART_UNCLAIMED_STALE_MIN
    )
    assert cte_binds["unclaimed_stale"] > claimed_binds["part_stale"], (
        "never-claimed shards need a longer republish horizon than shards "
        "a worker claimed and abandoned"
    )
    assert ":unclaimed_stale" in cte_sql


def test_poison_pill_branch_requires_evidence_of_a_claim():
    import shared.reconciler as reconciler

    session = _RecordingSession()
    _run(reconciler.sweep_orphans(session))

    poison_sql, _ = next(
        (s, b) for s, b in session.executed
        if "SET status = 'FAILED'" in s and "requeue_count >= :max_rq" in s
    )
    for token in ("worker_id IS NOT NULL", "started_at IS NOT NULL",
                  "retry_count > 0"):
        assert token in poison_sql, (
            f"poison-pill branch lost its claim-evidence guard ({token}) — "
            f"it can again FAIL partitions no worker ever claimed"
        )


def test_every_partition_type_the_workers_persist_can_be_republished():
    """Every partition_type written to snapshot_partitions must be routable.

    STEP A no longer poisons never-claimed shards, so a type missing from
    the routing maps has no way out at all: republish_partition_messages
    skips it and the poison branch can't fail it, leaving the row QUEUED,
    the snapshot IN_PROGRESS and the job RUNNING forever with no DLQ row
    and no alert. GROUPS_CHANNELS and ENTRA_CATEGORIES are persisted by
    workers/backup-worker/main.py (_fanout_groups_partitions /
    _fanout_entra_partitions) and have their own queues declared in
    shared/message_bus.py.
    """
    import shared.reconciler as reconciler

    for ptype, queue, msg_type in (
        ("ONEDRIVE_FILES", "backup.onedrive_partition",
         "BACKUP_ONEDRIVE_PARTITION"),
        ("CHATS", "backup.chats_partition", "BACKUP_CHATS_PARTITION"),
        ("MAIL_FOLDERS", "backup.mail_partition", "BACKUP_MAIL_PARTITION"),
        ("SHAREPOINT_DRIVES", "backup.sharepoint_partition",
         "BACKUP_SHAREPOINT_PARTITION"),
        ("GROUPS_CHANNELS", "backup.groups_partition",
         "BACKUP_GROUPS_PARTITION"),
        ("ENTRA_CATEGORIES", "backup.entra_partition",
         "BACKUP_ENTRA_PARTITION"),
    ):
        assert reconciler._queue_for_partition_type(ptype) == queue, (
            f"{ptype} has no queue mapping — a never-claimed shard of this "
            f"type can be neither republished nor failed (permanent zombie)"
        )
        assert reconciler._message_type_for_partition_type(ptype) == msg_type, (
            f"{ptype} has no messageType mapping — the republished envelope "
            f"would fall through the worker dispatcher to the jobId path"
        )


def test_never_claimed_republish_does_not_spend_the_requeue_budget():
    """requeue_count is the circuit breaker for claimed-then-died shards.

    Bumping it while a shard merely waits in a saturated queue means the
    first genuine worker death after a long queue wait finds the budget
    already spent and poisons the shard to FAILED with zero recovery
    requeues — exactly the failure this fix exists to prevent.
    """
    import shared.reconciler as reconciler

    session = _RecordingSession()
    _run(reconciler.sweep_orphans(session))

    unclaimed_sql = next(
        s for s, _ in session.executed if ":unclaimed_stale" in s
    )
    claimed_sql = next(
        s for s, _ in session.executed if "WITH stale AS" in s
    )
    assert unclaimed_sql is not claimed_sql and unclaimed_sql != claimed_sql, (
        "the never-claimed republish shares a statement (and therefore its "
        "SET clause) with the claimed-then-abandoned requeue"
    )
    assert "requeue_count  = sp.requeue_count + 1" in claimed_sql, (
        "claimed-then-abandoned requeue stopped bumping requeue_count — the "
        "poison-pill circuit breaker can no longer trip"
    )
    assert "requeue_count" not in _set_clause(unclaimed_sql), (
        "never-claimed republish still spends the shared requeue_count "
        "budget: SET clause = " + _set_clause(unclaimed_sql)
    )


def test_never_claimed_republish_is_bounded_and_expires_loudly():
    """Unbounded republish = message amplification with no terminal state.

    The original AMQP message is still in RabbitMQ, so each republish adds
    a copy. Bound the copies with a counter that is NOT requeue_count, and
    when the bound is reached mark the shard FAILED with its own
    work_dead_letter reason so ops sees "nobody is draining this queue"
    instead of a silent forever-hang.
    """
    import shared.reconciler as reconciler

    assert reconciler.RECONCILER_PART_UNCLAIMED_MAX_REPUBLISH > 0

    class _OneExpiredRow(_RecordingSession):
        """Return one row from the unclaimed-expiry UPDATE so its
        work_dead_letter INSERT actually gets issued."""

        async def execute(self, statement, binds=None):
            await super().execute(statement, binds)
            sql = str(statement)
            if "SET status = 'FAILED'" in sql and ">= :max_unclaimed_rq" in sql:
                class _R:
                    # Mirrors the branch's RETURNING, widened 2026-08-11 from
                    # `sp.id` to the full republish envelope so the
                    # work_dead_letter row stores a replayable payload.
                    id = "e1b8f0c2-0000-0000-0000-000000000001"
                    snapshot_id = "e1b8f0c2-0000-0000-0000-000000000002"
                    partition_type = "CHATS"
                    partition_index = 4
                    drive_id = None
                    file_ids = None
                    payload = {"chat_ids": ["19:abc@thread.v2"]}

                class _Res:
                    def all(self):
                        return [_R()]

                    def first(self):
                        return None
                return _Res()
            return _StubResult()

    session = _OneExpiredRow()
    _run(reconciler.sweep_orphans(session))

    unclaimed_sql, unclaimed_binds = next(
        (s, b) for s, b in session.executed
        if ":unclaimed_stale" in s and "WITH unclaimed AS" in s
    )
    assert unclaimed_binds["max_unclaimed_rq"] == (
        reconciler.RECONCILER_PART_UNCLAIMED_MAX_REPUBLISH
    )
    assert "< :max_unclaimed_rq" in unclaimed_sql, (
        "never-claimed republish is unbounded — copies accumulate at one "
        "per shard per horizon forever"
    )

    expire_sql, expire_binds = next(
        (s, b) for s, b in session.executed
        if "SET status = 'FAILED'" in s and ">= :max_unclaimed_rq" in s
    )
    assert expire_binds["max_unclaimed_rq"] == (
        reconciler.RECONCILER_PART_UNCLAIMED_MAX_REPUBLISH
    )
    # Must stay disjoint from the poison branch: only never-claimed rows.
    for token in ("worker_id IS NULL", "started_at IS NULL",
                  "lease_owner_id IS NULL", "retry_count = 0"):
        assert token in expire_sql, (
            f"unclaimed-expiry branch is missing {token} — it could fail a "
            f"shard a worker actually claimed"
        )

    # The reason moved from a SQL literal to a bind when both dead-letter
    # sites were folded into _dead_letter_partition (2026-08-11), so assert
    # on the bind — and on last_payload, which is what makes the row
    # replayable at all.
    dlq_sql, dlq_binds = next(
        (s, b) for s, b in session.executed
        if "INSERT INTO work_dead_letter" in s
    )
    assert "'partition'" in dlq_sql
    assert dlq_binds["reason"] == "unclaimed_expired"
    assert "last_payload" in dlq_sql, (
        "a dead-letter row without the republish envelope cannot be replayed "
        "— all 12 prod rows from 2026-06 are unrecoverable for exactly this "
        "reason"
    )
    _body = json.loads(dlq_binds["body"])
    assert _body["kind"] == "partition"
    assert _body["partition_type"] == "CHATS"
    assert _body["payload"] == {"chat_ids": ["19:abc@thread.v2"]}
