"""Regression: dead-lettering a partition must be replayable and visible.

Verified on prod 2026-08-11, three separate holes in the same path:

  1. UNREPLAYABLE. Both reconciler DLQ inserts wrote only
     (work_kind, work_id, reason) and left ``last_payload`` NULL. All 12
     rows in the table (2026-06-17/18, reason=poison_pill) are NULL, and
     snapshot_partitions is ON DELETE CASCADE from snapshots — so once
     retention pruned those snapshots the entries became permanently
     uninterpretable: the work_id points at a row that no longer exists and
     nothing else ever recorded what the shard was supposed to do. A backup
     product must at minimum be able to say what work it lost, and ideally
     re-run it.
  2. UNMONITORED. Nothing in the codebase has ever READ work_dead_letter,
     and the ``alerts`` table held 0 rows over its entire history — so
     abandoning a shard produced no operator signal of any kind.
  3. The alert insert must never be able to abort the sweep transaction.
     alerts.tenant_id carries an FK; a shard whose tenant row is gone would
     otherwise roll back the whole tick — every snapshot and job
     finalization in it, not just this row — and do so again every 60 s.

The fix routes both sites through ``_dead_letter_partition``, which stores
the envelope ``republish_partition_messages`` consumes (so replay is "read
last_payload, publish it"), raises one HIGH alert per abandoned shard, and
resolves tenant_id through a subselect so a missing tenant yields NULL
instead of an error.
"""
from __future__ import annotations

import asyncio
import json
import os
import uuid
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

PG_URL = os.getenv("RECONCILER_TEST_PG_URL", "").strip()

_TENANT = "11111111-1111-1111-1111-111111111111"
_RESOURCE = "22222222-2222-2222-2222-222222222222"
_SNAPSHOT = "33333333-3333-3333-3333-333333333333"


def _row(pid: str, *, ptype: str = "CHATS", idx: int = 2):
    """One row as the DLQ branches' RETURNING clauses hand it over."""
    return SimpleNamespace(
        id=pid,
        snapshot_id=_SNAPSHOT,
        partition_type=ptype,
        partition_index=idx,
        drive_id=None,
        file_ids=None,
        payload={"chat_ids": ["19:aaa", "19:bbb"]},
        tenant_id=_TENANT,
        resource_id=_RESOURCE,
    )


class _Result:
    def __init__(self, rows):
        self._rows = list(rows)

    def all(self):
        return list(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None


class _Session:
    """Feeds one poison row and one never-claimed-expired row into the two
    dead-letter branches, and records every statement."""

    POISON = "1a1a1a1a-0000-0000-0000-00000000aaaa"
    EXPIRED = "1a1a1a1a-0000-0000-0000-00000000bbbb"

    def __init__(self):
        self.executed: List[Tuple[str, Dict]] = []

    async def execute(self, statement, binds: Optional[Dict] = None):
        sql = str(statement)
        self.executed.append((sql, dict(binds or {})))
        if "SET status = 'FAILED'" in sql and "requeue_count >= :max_rq" in sql:
            return _Result([_row(self.POISON)])
        if "SET status = 'FAILED'" in sql and ">= :max_unclaimed_rq" in sql:
            return _Result([_row(self.EXPIRED, ptype="MAIL_FOLDERS", idx=0)])
        return _Result([])

    async def commit(self):
        pass

    async def rollback(self):
        pass


def _run(coro: Any):
    try:
        loop = asyncio.get_event_loop_policy().get_event_loop()
        if loop.is_closed():
            raise RuntimeError("closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


def _swept() -> _Session:
    import shared.reconciler as reconciler

    session = _Session()
    _run(reconciler.sweep_orphans(session))
    return session


def _dlq_inserts(session: _Session) -> Dict[str, Dict]:
    """reason → binds, for every work_dead_letter INSERT the tick issued."""
    out = {}
    for sql, binds in session.executed:
        if "INSERT INTO work_dead_letter" in sql:
            assert "last_payload" in sql, (
                "the dead-letter INSERT has no last_payload column — the "
                "abandoned shard is unreplayable the moment its partition "
                "row is CASCADE-deleted"
            )
            out[binds.get("reason")] = binds
    return out


def test_both_dead_letter_sites_record_a_replayable_payload():
    session = _swept()
    inserts = _dlq_inserts(session)
    assert set(inserts) == {"poison_pill", "unclaimed_expired"}, (
        f"not every dead-letter site records a payload: {sorted(inserts)}"
    )
    for reason, binds in inserts.items():
        body = json.loads(binds["body"])
        assert body["kind"] == "partition"
        assert body["snapshot_id"] == _SNAPSHOT
        assert body["reason"] == reason
        assert body["payload"] == {"chat_ids": ["19:aaa", "19:bbb"]}, (
            "the shard's work-set (payload) was not recorded, so replaying "
            "it would back up the wrong chats"
        )


def test_a_dead_letter_payload_replays_through_the_republish_path():
    """The whole point of storing the envelope: recovery is a read + a
    publish, using the code that already exists."""
    import shared.reconciler as reconciler

    session = _swept()
    body = json.loads(_dlq_inserts(session)["poison_pill"]["body"])

    published: List[Tuple[str, Dict]] = []

    class _Bus:
        async def publish(self, queue, message):
            published.append((queue, message))

    stats = reconciler.SweepStats()
    stats.requeue_payloads.append(body)
    n = _run(reconciler.republish_partition_messages(stats, message_bus=_Bus()))

    assert n == 1, (
        "republish_partition_messages could not consume the stored "
        "last_payload — the DLQ shape has drifted from the requeue shape"
    )
    queue, msg = published[0]
    assert queue == "backup.chats_partition"
    assert msg["messageType"] == "BACKUP_CHATS_PARTITION"
    assert msg["partitionId"] == _Session.POISON
    assert msg["snapshotId"] == _SNAPSHOT
    assert msg["payload"] == {"chat_ids": ["19:aaa", "19:bbb"]}


def test_dead_lettering_raises_an_operator_alert():
    """work_dead_letter has never been read by anything. An abandoned shard
    is a hole in someone's backup — it has to surface where operators
    already look."""
    session = _swept()
    alerts = [
        (sql, binds) for sql, binds in session.executed
        if "INSERT INTO alerts" in sql
    ]
    assert len(alerts) == 2, (
        f"expected one alert per dead-lettered shard, got {len(alerts)} — "
        f"dead-lettering is silent again"
    )
    for sql, binds in alerts:
        assert "'WORK_DEAD_LETTER'" in sql and "'HIGH'" in sql
        assert "SELECT id FROM tenants" in sql, (
            "tenant_id is bound straight into an FK column — a shard whose "
            "tenant row is gone would abort the whole sweep transaction "
            "every 60 s"
        )
        details = json.loads(binds["details"])
        assert details["work_kind"] == "partition"
        assert details["reason"] in ("poison_pill", "unclaimed_expired")
        assert details["work_id"] in (_Session.POISON, _Session.EXPIRED)
        assert "replay" in binds["message"].lower(), (
            "the alert does not tell the operator how to recover the shard"
        )


def test_dead_letter_log_line_is_greppable():
    """Fallback signal for anyone without the alerts UI: one structured
    line, not a sentence."""
    import shared.reconciler as reconciler

    printed: List[str] = []
    real_print = print

    session = _Session()
    import builtins
    builtins.print = lambda *a, **kw: printed.append(" ".join(str(x) for x in a))
    try:
        _run(reconciler.sweep_orphans(session))
    finally:
        builtins.print = real_print

    dlq_lines = [p for p in printed if "[RECONCILER][DLQ]" in p]
    assert len(dlq_lines) == 2, f"DLQ log lines: {printed}"
    for line in dlq_lines:
        for token in ("work_kind=partition", "work_id=", "reason=",
                      "snapshot_id=", "replayable=true"):
            assert token in line, f"missing {token} in {line!r}"


def test_poison_branch_returns_the_columns_the_envelope_needs():
    """RETURNING id alone is what made the payload impossible to build.
    Pinned so a future edit to that clause fails here instead of silently
    writing NULL payloads again."""
    session = _swept()
    poison_sql = next(
        s for s, _ in session.executed
        if "SET status = 'FAILED'" in s and "requeue_count >= :max_rq" in s
    )
    returning = poison_sql.split("RETURNING", 1)[1]
    for col in ("snapshot_id", "partition_type", "partition_index",
                "drive_id", "file_ids", "payload", "tenant_id"):
        assert col in returning, (
            f"poison-pill RETURNING lost {col} — last_payload can no longer "
            f"describe the shard it abandoned"
        )


# ── behaviour against a real Postgres ───────────────────────────────────

pg_only = pytest.mark.skipif(
    not PG_URL,
    reason="RECONCILER_TEST_PG_URL not set (needs a throwaway Postgres)",
)

_SCHEMA = [
    "DROP TABLE IF EXISTS snapshot_partitions, worker_heartbeats, "
    "work_dead_letter, snapshot_items, snapshots, jobs, alerts, "
    "tenants CASCADE",
    "DROP TYPE IF EXISTS snapshotstatus, jobstatus CASCADE",
    "CREATE TYPE snapshotstatus AS ENUM ('IN_PROGRESS','COMPLETED','FAILED',"
    "'PARTIAL','PENDING_DELETION')",
    "CREATE TYPE jobstatus AS ENUM ('QUEUED','PENDING','RUNNING','COMPLETED',"
    "'FAILED','CANCELLED','CANCELLING','RETRYING')",
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
        status           text NOT NULL DEFAULT 'QUEUED',
        worker_id        text,
        retry_count      int  NOT NULL DEFAULT 0,
        enqueued_at      timestamptz NOT NULL DEFAULT NOW(),
        started_at       timestamptz,
        completed_at     timestamptz,
        failure_state    json,
        lease_owner_id   uuid,
        lease_expires_at timestamptz,
        lease_token      bigint NOT NULL DEFAULT 0,
        requeue_count    int  NOT NULL DEFAULT 0
    )""",
    """CREATE TABLE worker_heartbeats (
        worker_id    uuid PRIMARY KEY,
        last_seen_at timestamptz NOT NULL DEFAULT NOW()
    )""",
    """CREATE TABLE work_dead_letter (
        id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
        work_kind    text NOT NULL,
        work_id      uuid NOT NULL,
        reason       text NOT NULL,
        last_payload json,
        created_at   timestamptz NOT NULL DEFAULT NOW()
    )""",
    """CREATE TABLE snapshots (
        id               uuid PRIMARY KEY,
        job_id           uuid,
        status           snapshotstatus NOT NULL DEFAULT 'IN_PROGRESS',
        started_at       timestamptz DEFAULT NOW(),
        completed_at     timestamptz,
        item_count       int,
        bytes_total      bigint,
        lease_owner_id   uuid,
        lease_expires_at timestamptz,
        lease_token      bigint NOT NULL DEFAULT 0
    )""",
    """CREATE TABLE snapshot_items (
        id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
        snapshot_id  uuid NOT NULL,
        content_size bigint
    )""",
    """CREATE TABLE jobs (
        id               uuid PRIMARY KEY,
        status           jobstatus NOT NULL DEFAULT 'RUNNING',
        completed_at     timestamptz,
        lease_owner_id   uuid,
        lease_expires_at timestamptz,
        lease_token      bigint NOT NULL DEFAULT 0
    )""",
    "CREATE TABLE tenants (id uuid PRIMARY KEY)",
    """CREATE TABLE alerts (
        id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
        tenant_id     uuid REFERENCES tenants(id),
        type          text NOT NULL,
        severity      text NOT NULL DEFAULT 'MEDIUM',
        message       text NOT NULL,
        resource_id   uuid,
        triggered_by  text,
        details       json DEFAULT '{}',
        created_at    timestamptz NOT NULL DEFAULT NOW()
    )""",
]


async def _fresh_db():
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    engine = create_async_engine(PG_URL, poolclass=None)
    async with engine.begin() as conn:
        for stmt in _SCHEMA:
            await conn.execute(text(stmt))
    return engine, async_sessionmaker(engine, expire_on_commit=False)


async def _insert_poison_shard(session, *, tenant_exists: bool):
    """A CHATS shard a worker claimed and abandoned at the requeue cap —
    the population the poison branch dead-letters."""
    from sqlalchemy import text
    from shared.reconciler import RECONCILER_MAX_REQUEUE

    if tenant_exists:
        await session.execute(
            text("INSERT INTO tenants (id) VALUES (:t)"), {"t": _TENANT},
        )
    pid = uuid.uuid4()
    await session.execute(
        text(
            """
            INSERT INTO snapshot_partitions
                (id, snapshot_id, tenant_id, resource_id, job_id,
                 partition_type, partition_index, payload, status,
                 worker_id, requeue_count, enqueued_at, started_at)
            VALUES (:id, :sid, :tid, :rid, :jid, 'CHATS', 3,
                    :payload, 'IN_PROGRESS', 'worker-dead-1', :rq,
                    NOW() - INTERVAL '90 minutes',
                    NOW() - INTERVAL '60 minutes')
            """
        ),
        {
            "id": str(pid),
            "sid": _SNAPSHOT,
            "tid": _TENANT,
            "rid": _RESOURCE,
            "jid": str(uuid.uuid4()),
            "payload": json.dumps({"chat_ids": ["19:aaa"]}),
            "rq": RECONCILER_MAX_REQUEUE,
        },
    )
    return pid


@pg_only
def test_dead_letter_row_and_alert_land_in_postgres():
    from shared.reconciler import sweep_orphans
    from sqlalchemy import text

    async def _body():
        engine, factory = await _fresh_db()
        try:
            async with factory() as session:
                pid = await _insert_poison_shard(session, tenant_exists=True)
                await session.commit()
                await sweep_orphans(session)

                dlq = (await session.execute(
                    text("SELECT reason, last_payload FROM work_dead_letter "
                         "WHERE work_id = :id"),
                    {"id": str(pid)},
                )).first()
                alert = (await session.execute(
                    text("SELECT type, severity, tenant_id::text, "
                         "resource_id::text, details FROM alerts"),
                )).first()
        finally:
            await engine.dispose()

        assert dlq is not None and dlq.reason == "poison_pill"
        payload = dlq.last_payload
        if isinstance(payload, str):
            payload = json.loads(payload)
        assert payload and payload["id"] == str(pid), (
            "last_payload is NULL/unusable — same unreplayable DLQ row as the "
            "12 already in prod"
        )
        assert payload["payload"] == {"chat_ids": ["19:aaa"]}
        assert alert is not None, "no alert row — abandonment is invisible"
        assert (alert.type, alert.severity) == ("WORK_DEAD_LETTER", "HIGH")
        assert alert.tenant_id == _TENANT
        assert alert.resource_id == _RESOURCE

    _run(_body())


@pg_only
def test_dead_letter_alert_survives_a_missing_tenant():
    """alerts.tenant_id is an FK. Binding a since-deleted tenant would abort
    the sweep transaction — killing every snapshot/job finalization in the
    tick — and repeat every 60 s. The subselect degrades to NULL instead."""
    from shared.reconciler import sweep_orphans
    from sqlalchemy import text

    async def _body():
        engine, factory = await _fresh_db()
        try:
            async with factory() as session:
                pid = await _insert_poison_shard(session, tenant_exists=False)
                await session.commit()
                stats = await sweep_orphans(session)
                row = (await session.execute(
                    text("SELECT status FROM snapshot_partitions "
                         "WHERE id = :id"),
                    {"id": str(pid)},
                )).first()
                alert = (await session.execute(
                    text("SELECT tenant_id, message FROM alerts"),
                )).first()
        finally:
            await engine.dispose()

        assert stats.partitions_dlq == 1
        assert row.status == "FAILED", (
            "the sweep rolled back — one orphaned tenant row now blocks "
            "every finalization the reconciler performs"
        )
        assert alert is not None and alert.tenant_id is None

    _run(_body())
