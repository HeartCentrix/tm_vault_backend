"""A duplicate partition message must not steal a LIVE worker's claim.

``_claim_partition`` accepts an already-IN_PROGRESS row whose
``started_at`` is older than ONEDRIVE_PARTITION_STALE_SWEEP_MIN (30 min)
with no lease check at all. That was tolerable while the only source of a
second delivery was a coordinator redelivery, but the reconciler's
never-claimed republish path deliberately puts extra copies of a
partition message on the queue while the original is still there — so a
duplicate arriving 30+ min into a legitimate multi-hour drain would take
the shard away from the worker that is actively draining it. Two workers
then write the same shard and the loser's terminal write is fenced,
which loses whichever half finished second.

The lease is the live-ness signal and it is renewed every LEASE_RENEW_S
by shared.lease.LeaseExtender (which covers snapshot_partitions), so the
steal must additionally require an expired/absent lease — the same guard
the backup-scheduler's stale-sweep already carries
(services/backup-scheduler/main.py, "Lease guard (2026-07-14)").

Recovery is unaffected: a dead worker stops renewing, so its lease
expires within LEASE_TTL_S (10 min) and the 30-min steal still fires.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import uuid
from typing import Any, Dict, List, Optional, Tuple

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_claim_under_test", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_claim_under_test"] = _bw
assert _spec and _spec.loader
_spec.loader.exec_module(_bw)  # type: ignore[union-attr]


def _run(coro):
    """Run on a live loop, installing one if the suite left none behind."""
    try:
        loop = asyncio.get_event_loop_policy().get_event_loop()
        if loop.is_closed():
            raise RuntimeError("closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


# ─── CI guard: inspect the statement the claim actually issues ─────────


class _RecordingResult:
    def first(self):
        return None


class _RecordingSession:
    def __init__(self):
        self.executed: List[Tuple[str, Dict]] = []

    async def execute(self, statement, binds: Optional[Dict] = None):
        self.executed.append((str(statement), dict(binds or {})))
        return _RecordingResult()

    async def commit(self):
        pass


class _RecordingFactory:
    def __init__(self, session):
        self._session = session

    def __call__(self):
        session = self._session

        class _Ctx:
            async def __aenter__(self):
                return session

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


class _FakeSelf:
    """Minimal stand-in: _claim_partition only reads these two."""
    worker_id = "worker-test"
    _heartbeat = None


def _claim_sql(monkeypatch) -> str:
    session = _RecordingSession()
    monkeypatch.setattr(_bw, "async_session_factory", _RecordingFactory(session))
    _run(_bw.BackupWorker._claim_partition(
        _FakeSelf(), uuid.uuid4(), "worker-test",
    ))
    sql, _ = next(
        (s, b) for s, b in session.executed
        if "UPDATE snapshot_partitions" in s and "SET status        = 'IN_PROGRESS'" in s
    )
    return sql


def test_claim_cannot_steal_a_partition_whose_lease_is_still_live(monkeypatch):
    sql = _claim_sql(monkeypatch)
    # The QUEUED arm must stay unguarded — that is the normal claim.
    assert "status = 'QUEUED'" in sql
    # The IN_PROGRESS steal arm must require an expired/absent lease.
    steal_arm = sql.split("status = 'IN_PROGRESS'", 1)[1].split("RETURNING", 1)[0]
    assert "lease_expires_at IS NULL" in steal_arm, (
        "the IN_PROGRESS steal arm has no lease guard: a duplicate delivery "
        "can take a shard from a worker that is still actively draining it "
        "(reconciler republish puts duplicates on the queue by design). "
        f"steal arm = {steal_arm}"
    )
    assert "lease_expires_at < NOW()" in steal_arm, (
        "the steal arm must still allow recovery once the lease has expired "
        f"(a dead worker stops renewing). steal arm = {steal_arm}"
    )
    assert "started_at < NOW()" in steal_arm, (
        "the 30-min idle requirement must be kept in addition to the lease "
        f"guard. steal arm = {steal_arm}"
    )


# ─── Row-level semantics against a real Postgres ──────────────────────

PG_URL = os.getenv("RECONCILER_TEST_PG_URL", "").strip()

_SCHEMA = [
    "DROP TABLE IF EXISTS snapshot_partitions CASCADE",
    """CREATE TABLE snapshot_partitions (
        id               uuid PRIMARY KEY,
        snapshot_id      uuid NOT NULL,
        tenant_id        uuid,
        resource_id      uuid,
        job_id           uuid,
        partition_type   text NOT NULL DEFAULT 'CHATS',
        drive_id         text,
        partition_index  int  NOT NULL,
        file_ids         json,
        payload          json,
        total_files      int  NOT NULL DEFAULT 0,
        total_bytes_est  bigint NOT NULL DEFAULT 0,
        status           text NOT NULL DEFAULT 'QUEUED',
        worker_id        text,
        worker_region    text,
        retry_count      int  NOT NULL DEFAULT 0,
        enqueued_at      timestamptz NOT NULL DEFAULT NOW(),
        started_at       timestamptz,
        lease_owner_id   uuid,
        lease_expires_at timestamptz,
        lease_token      bigint NOT NULL DEFAULT 0,
        requeue_count    int  NOT NULL DEFAULT 0
    )""",
]


@pytest.mark.skipif(
    not PG_URL, reason="RECONCILER_TEST_PG_URL not set (needs a throwaway PG)",
)
@pytest.mark.parametrize(
    "lease_offset_min,expect_claim,label",
    [
        (5, False, "live lease (worker still draining) — must NOT be stolen"),
        (-5, True, "expired lease (worker died) — recovery must still work"),
        (None, True, "no lease (heartbeat init failed) — legacy path kept"),
    ],
)
def test_claim_steal_respects_the_lease(lease_offset_min, expect_claim, label,
                                        monkeypatch):
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import (
        async_sessionmaker, create_async_engine,
    )

    async def _body():
        engine = create_async_engine(PG_URL, poolclass=None)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        pid = uuid.uuid4()
        async with engine.begin() as conn:
            for stmt in _SCHEMA:
                await conn.execute(text(stmt))
            lease = (
                "NULL" if lease_offset_min is None
                else f"NOW() + ({lease_offset_min} * INTERVAL '1 minute')"
            )
            await conn.execute(text(
                f"""INSERT INTO snapshot_partitions
                    (id, snapshot_id, partition_index, partition_type, status,
                     worker_id, started_at, lease_owner_id, lease_expires_at)
                    VALUES (:pid, :sid, 0, 'CHATS', 'IN_PROGRESS',
                            'worker-live',
                            NOW() - INTERVAL '40 minutes',
                            gen_random_uuid(), {lease})"""
            ), {"pid": str(pid), "sid": str(uuid.uuid4())})

        monkeypatch.setattr(_bw, "async_session_factory", factory)
        claim = await _bw.BackupWorker._claim_partition(
            _FakeSelf(), pid, "worker-duplicate",
        )
        async with factory() as s:
            owner = (await s.execute(text(
                "SELECT worker_id FROM snapshot_partitions WHERE id = :pid"
            ), {"pid": str(pid)})).scalar()
        await engine.dispose()
        return claim, owner

    claim, owner = _run(_body())
    if expect_claim:
        assert claim is not None, f"claim was refused but should succeed: {label}"
        assert owner == "worker-duplicate", label
    else:
        assert claim is None, f"claim stole a live shard: {label}"
        assert owner == "worker-live", label
