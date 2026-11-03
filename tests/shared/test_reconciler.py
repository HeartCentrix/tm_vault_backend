"""Unit tests for the distributed-reconciliation primitives.

Pure-Python tests against stubbed AsyncSession — no DB required. The
sweeper SQL itself is integration-tested in the per-service suite
(needs a real Postgres for the CTEs to execute), but we cover here
the slicing, dataclass shape, and the routing-fence + lease logic.

Spec: docs/superpowers/specs/2026-05-16-distributed-reconciliation-design.md.
"""
from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import pytest


# ─── Stub harness ─────────────────────────────────────────────────────


@dataclass
class _Row:
    """Tiny SQLAlchemy-Row stand-in: attribute access + indexable."""
    id: uuid.UUID
    snapshot_id: Optional[uuid.UUID] = None
    partition_type: Optional[str] = None
    partition_index: Optional[int] = None
    drive_id: Optional[str] = None
    file_ids: Optional[List] = None
    payload: Optional[Dict] = None
    requeue_count: int = 0
    status: Optional[str] = None


class _StubResult:
    def __init__(self, rows: List[Any], rowcount: int = None):
        self._rows = rows
        self.rowcount = rowcount if rowcount is not None else len(rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return list(self._rows)

    def scalar_one_or_none(self):
        return self._rows[0] if self._rows else None


class _StubSession:
    """SQL string → rows registry. Tests stage rows by SQL substring."""

    def __init__(self, registry: Dict[str, List[Any]]):
        self._registry = registry
        self.executed: List[Tuple[str, Dict]] = []
        self.committed = False
        self.rolled_back = False

    async def execute(self, statement, binds: Optional[Dict] = None):
        sql = str(statement)
        self.executed.append((sql, dict(binds or {})))
        for needle, rows in self._registry.items():
            if needle in sql:
                return _StubResult(rows)
        return _StubResult([], rowcount=0)

    async def commit(self):
        self.committed = True

    async def rollback(self):
        self.rolled_back = True


def _run(coro):
    return asyncio.run(coro)


# ─── reconciler.sweep_orphans ─────────────────────────────────────────


def test_sweep_returns_empty_stats_when_nothing_orphan():
    from shared.reconciler import sweep_orphans
    session = _StubSession({})
    stats = _run(sweep_orphans(session))
    assert stats.total() == 0
    assert stats.partitions_requeued == 0
    assert stats.snapshots_finalized == 0
    assert stats.jobs_finalized == 0
    assert session.committed is True


def test_sweep_collects_requeue_payloads_for_orphan_partitions():
    from shared.reconciler import sweep_orphans
    pid = uuid.uuid4()
    sid = uuid.uuid4()
    session = _StubSession({
        # Step A is the only sweep step that does UPDATE-RETURNING on
        # snapshot_partitions; matching on "UPDATE snapshot_partitions"
        # disambiguates from the partition poison-pill UPDATE which
        # only has a RETURNING id (no extra columns).
        "WITH stale AS": [
            _Row(
                id=pid, snapshot_id=sid,
                partition_type="ONEDRIVE_FILES", partition_index=0,
                drive_id="drive-1", file_ids=["a", "b"], payload=None,
                requeue_count=1,
            ),
        ],
    })
    stats = _run(sweep_orphans(session))
    assert stats.partitions_requeued == 1
    assert stats.requeue_payloads[0]["partition_type"] == "ONEDRIVE_FILES"
    assert stats.requeue_payloads[0]["snapshot_id"] == str(sid)


def test_sweep_finalizes_orphan_snapshots():
    from shared.reconciler import sweep_orphans
    snap_id = uuid.uuid4()
    session = _StubSession({
        # Substring unique to STEP B (the orphan-snapshot UPDATE): STEP D's
        # integrity correction also starts "UPDATE snapshots s SET status =
        # CASE", so key on STEP B's non-partitioned-orphan comment.
        "-- Non-partitioned orphan": [
            _Row(id=snap_id, status="FAILED"),
        ],
    })
    stats = _run(sweep_orphans(session))
    assert stats.snapshots_finalized == 1


def test_step_b_orphan_without_partitions_derives_failed_not_completed():
    """A non-partitioned IN_PROGRESS snapshot whose lease died (WHERE
    precondition of STEP B) is a worker that died mid-run. Deriving
    COMPLETED manufactured false successes — a crashed multi-hour
    archive walk became a COMPLETED 0-item snapshot that read as a
    successful empty backup and fed retention deletion."""
    import inspect
    import shared.reconciler as reconciler
    src = inspect.getsource(reconciler)
    import re
    m = re.search(
        r"WHEN NOT EXISTS \(\s*SELECT 1 FROM snapshot_partitions sp\s*"
        r"WHERE sp\.snapshot_id = s\.id\s*\) THEN '(\w+)'::snapshotstatus",
        src,
    )
    assert m, "STEP B's non-partitioned CASE branch not found"
    assert m.group(1) == "FAILED", (
        "non-partitioned orphans must derive FAILED — COMPLETED is a "
        "false success for a run that died mid-walk"
    )


def test_sweep_finalizes_orphan_jobs():
    from shared.reconciler import sweep_orphans
    job_id = uuid.uuid4()
    session = _StubSession({
        "UPDATE jobs j": [
            _Row(id=job_id, status="COMPLETED"),
        ],
    })
    stats = _run(sweep_orphans(session))
    assert stats.jobs_finalized == 1


def test_sweep_dry_run_rolls_back_and_does_not_commit(monkeypatch):
    import shared.reconciler as reconciler
    monkeypatch.setattr(reconciler, "DRY_RUN", True)
    session = _StubSession({})
    _run(reconciler.sweep_orphans(session))
    assert session.rolled_back is True
    assert session.committed is False


# ─── reconciler.republish_partition_messages ─────────────────────────


class _StubBus:
    def __init__(self):
        self.published: List[Tuple[str, Dict]] = []

    async def publish(self, queue: str, body: Dict):
        self.published.append((queue, body))


def test_republish_routes_each_partition_type():
    from shared.reconciler import republish_partition_messages, SweepStats
    bus = _StubBus()
    stats = SweepStats(requeue_payloads=[
        {"id": "1", "snapshot_id": "s1", "partition_type": "ONEDRIVE_FILES",
         "partition_index": 0, "drive_id": "d", "file_ids": ["x"], "payload": None},
        {"id": "2", "snapshot_id": "s2", "partition_type": "CHATS",
         "partition_index": 1, "drive_id": None, "file_ids": None,
         "payload": {"chat_ids": ["c"]}},
        {"id": "3", "snapshot_id": "s3", "partition_type": "MAIL_FOLDERS",
         "partition_index": 0, "drive_id": None, "file_ids": None,
         "payload": {"folder_ids": ["f"]}},
        {"id": "4", "snapshot_id": "s4", "partition_type": "SHAREPOINT_DRIVES",
         "partition_index": 0, "drive_id": None, "file_ids": None,
         "payload": {"drive_ids": ["sp"]}},
        {"id": "5", "snapshot_id": "s5", "partition_type": "GROUPS_CHANNELS",
         "partition_index": 0, "drive_id": None, "file_ids": None,
         "payload": {"channel_ids": ["ch"], "team_id": "t"}},
        {"id": "6", "snapshot_id": "s6", "partition_type": "ENTRA_CATEGORIES",
         "partition_index": 0, "drive_id": None, "file_ids": None,
         "payload": {"category_ids": ["USERS"], "directory_id": "d"}},
    ])
    n = _run(republish_partition_messages(stats, message_bus=bus))
    assert n == 6
    queues = [q for q, _ in bus.published]
    assert "backup.onedrive_partition" in queues
    assert "backup.chats_partition" in queues
    assert "backup.mail_partition" in queues
    assert "backup.sharepoint_partition" in queues
    assert "backup.groups_partition" in queues
    assert "backup.entra_partition" in queues


def test_republish_skips_unknown_partition_type():
    from shared.reconciler import republish_partition_messages, SweepStats
    bus = _StubBus()
    stats = SweepStats(requeue_payloads=[
        {"id": "1", "snapshot_id": "s1", "partition_type": "FUTURE_TYPE",
         "partition_index": 0, "drive_id": None, "file_ids": None, "payload": None},
    ])
    n = _run(republish_partition_messages(stats, message_bus=bus))
    assert n == 0
    assert bus.published == []


# ─── routing_fence ────────────────────────────────────────────────────


def test_wrap_outgoing_wraps_only_once():
    from shared.routing_fence import wrap_outgoing
    inner = {"foo": "bar"}
    out = wrap_outgoing(inner, expected_workload="USER_CHATS")
    assert out["expected_workload"] == "USER_CHATS"
    assert out["route_hops"] == 0
    # Double-wrap is a no-op.
    twice = wrap_outgoing(out, expected_workload="USER_CHATS")
    assert twice is out


def test_unwrap_returns_payload_when_workload_matches():
    from shared.routing_fence import wrap_outgoing, unwrap_incoming
    wrapped = wrap_outgoing({"snap": "X"}, expected_workload="USER_MAIL")
    inner = unwrap_incoming(wrapped, handled_workloads={"USER_MAIL", "USER_CONTACTS"})
    assert inner == {"snap": "X"}


def test_unwrap_returns_none_when_workload_mismatched():
    from shared.routing_fence import wrap_outgoing, unwrap_incoming
    wrapped = wrap_outgoing({"snap": "X"}, expected_workload="USER_CHATS")
    inner = unwrap_incoming(wrapped, handled_workloads={"USER_MAIL"})
    assert inner is None


def test_unwrap_passes_through_legacy_unwrapped():
    """Pre-fence messages don't carry expected_workload; we must NOT
    drop them. Backwards-compatible during rollout."""
    from shared.routing_fence import unwrap_incoming
    legacy = {"snap": "Y", "old": True}
    inner = unwrap_incoming(legacy, handled_workloads={"USER_MAIL"})
    assert inner == legacy


def test_reroute_or_dlq_routes_to_correct_queue():
    from shared.routing_fence import reroute_or_dlq
    bus = _StubBus()

    class _SFactory:
        def __call__(self):
            return self
        async def __aenter__(self):
            return _StubSession({})
        async def __aexit__(self, *a):
            return False

    msg = {"expected_workload": "USER_CHATS", "route_hops": 0, "payload": {"x": 1}}
    out = _run(reroute_or_dlq(msg, message_bus=bus, session_factory=_SFactory()))
    assert out == "rerouted"
    assert bus.published[0][0] == "backup.heavy"
    assert bus.published[0][1]["route_hops"] == 1


def test_reroute_or_dlq_dlqs_after_max_hops():
    from shared.routing_fence import reroute_or_dlq
    bus = _StubBus()

    class _SFactory:
        def __call__(self):
            return self
        async def __aenter__(self):
            return _StubSession({})
        async def __aexit__(self, *a):
            return False

    msg = {"expected_workload": "USER_CHATS", "route_hops": 5, "payload": {"x": 1}}
    out = _run(reroute_or_dlq(msg, message_bus=bus, session_factory=_SFactory()))
    assert out == "dlq"
    assert bus.published == []


# ─── lease primitives ────────────────────────────────────────────────


def test_lease_dataclass_fence_clause_shape():
    from shared.lease import Lease
    lease = Lease(table="snapshots", work_id="abc", owner_id="owner", token=42)
    assert "lease_token = :__lease_token" in lease.fence_clause()
    assert lease.fence_params() == {"__lease_token": 42}


def test_lease_rejects_unsupported_table():
    from shared.lease import claim
    with pytest.raises(ValueError):
        _run(claim(
            session=_StubSession({}),
            table="nope",
            work_id=uuid.uuid4(),
            owner_id=str(uuid.uuid4()),
            in_progress_status="IN_PROGRESS",
        ))


def test_lease_claim_returns_none_when_no_row_updated():
    from shared.lease import claim
    session = _StubSession({})  # registry empty → no rows returned
    out = _run(claim(
        session=session,
        table="snapshots",
        work_id=uuid.uuid4(),
        owner_id=str(uuid.uuid4()),
        in_progress_status="IN_PROGRESS",
    ))
    assert out is None
