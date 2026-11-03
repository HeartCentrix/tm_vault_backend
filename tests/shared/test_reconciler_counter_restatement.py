"""Regression: when the reconciler settles a snapshot it must restate
item_count / bytes_total from snapshot_items.

THE BUG (measured on prod 2026-08-11). Both counters are written MID-walk
by the stream-persist handlers as a per-process running total, and only the
success path restates them from a real COUNT(*). A snapshot whose owning
process died therefore keeps whatever the last flush left — routinely 0 —
while holding every snapshot_items row it committed: 25 rows read
item_count=0 over 270,885 real items (9 FAILED ROOM_MAILBOX = 189,692 and
16 COMPLETED USER_CHATS = 81,193), none partitioned, all with
reuse_chain_root_id NULL.

Why that is data loss and not cosmetics: those counters are the only
evidence shared/retention_cleanup._holds_persisted_content consults, so a
lying 0 costs the row its GFS bucket — and under GFS the buckets ARE the
keep set — after which _delete_snapshots hard-deletes the snapshot together
with its items. Retention now fails safe on its own side too
(tests/shared/test_retention_counter_lie.py); this file pins the other
half: the settle writes the truth, so the DB stops manufacturing liars.

The reconciler settles a snapshot in two places and both are covered:
STEP B (orphan settle) and STEP D (terminal-status integrity sweep).

The shape tests run everywhere. The behaviour tests need a real Postgres
(GREATEST + correlated aggregates + enum casts have no sqlite equivalent);
point RECONCILER_TEST_PG_URL at a throwaway cluster to run them, exactly as
tests/shared/test_reconciler_unclaimed_partitions.py documents. The schema
below is deliberately a local copy rather than an import from that module —
tests/ is not a package, so a cross-module import is not addressable.
"""
from __future__ import annotations

import asyncio
import os
import re
import uuid
from typing import Any, Dict, List, Optional, Tuple

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

PG_URL = os.getenv("RECONCILER_TEST_PG_URL", "").strip()


# ── SQL shape (runs everywhere) ─────────────────────────────────────────

class _StubResult:
    def first(self):
        return None

    def all(self):
        return []


class _RecordingSession:
    def __init__(self):
        self.executed: List[Tuple[str, Dict]] = []

    async def execute(self, statement, binds: Optional[Dict] = None):
        self.executed.append((str(statement), dict(binds or {})))
        return _StubResult()

    async def commit(self):
        pass

    async def rollback(self):
        pass


def _run(coro: Any):
    """Run on a live loop, installing one if the suite left none behind."""
    try:
        loop = asyncio.get_event_loop_policy().get_event_loop()
        if loop.is_closed():
            raise RuntimeError("closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


def _sweep_statements() -> List[str]:
    import shared.reconciler as reconciler

    session = _RecordingSession()
    _run(reconciler.sweep_orphans(session))
    return [s for s, _ in session.executed]


def _flat(sql: str) -> str:
    return " ".join(sql.split())


_RESTATE_ITEMS = _flat("""
    item_count = GREATEST( COALESCE(s.item_count, 0), (SELECT COUNT(*) FROM
    snapshot_items si WHERE si.snapshot_id = s.id) )
""")
_RESTATE_BYTES = _flat("""
    bytes_total = GREATEST( COALESCE(s.bytes_total, 0), (SELECT
    COALESCE(SUM(si.content_size), 0) FROM snapshot_items si WHERE
    si.snapshot_id = s.id) )
""")


def test_step_b_orphan_settle_restates_the_counters():
    """STEP B is where a dead worker's snapshot becomes terminal — the first
    moment the item truth is knowable without that worker."""
    step_b = next(
        s for s in _sweep_statements()
        if "UPDATE snapshots s" in s
        and "s.status = 'IN_PROGRESS'::snapshotstatus" in s
    )
    flat = _flat(step_b)
    assert _RESTATE_ITEMS in flat, (
        "STEP B settles a snapshot without restating item_count — a "
        "FAILED-mid-walk row keeps item_count=0 over its real "
        "snapshot_items and retention reads that as 'holds nothing'"
    )
    assert _RESTATE_BYTES in flat, "STEP B does not restate bytes_total"


def test_step_d_integrity_sweep_restates_the_counters():
    """STEP D corrects the status of an already-terminal snapshot; the path
    that produced that wrong status could not vouch for its counters
    either."""
    step_d = next(
        s for s in _sweep_statements() if "WITH part_agg AS" in s
    )
    flat = _flat(step_d)
    assert _RESTATE_ITEMS in flat, (
        "STEP D corrects a snapshot's status but leaves its counters lying"
    )
    assert _RESTATE_BYTES in flat, "STEP D does not restate bytes_total"


def test_restatement_is_greatest_never_an_assignment():
    """GREATEST is load-bearing twice over: it makes the write idempotent
    under the 60 s re-run, and it can never LOWER a counter a handler wrote
    (a handler may legitimately count work that never became a
    snapshot_items row — pointer / dedup writes)."""
    for sql in _sweep_statements():
        # Strip `-- ...` comments: they quote the prod symptom
        # ("item_count=0") and would match as if it were an assignment.
        code = "\n".join(
            line.split("--", 1)[0] for line in sql.splitlines()
        )
        for m in re.finditer(r"(item_count|bytes_total)\s*=\s*(\S+)", code):
            assert m.group(2).startswith("GREATEST"), (
                f"{m.group(1)} is assigned directly ({m.group(2)}) — a "
                f"re-run or a partial handler flush can now LOWER a real "
                f"counter"
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
        started_at       timestamptz DEFAULT NOW() - INTERVAL '2 hours',
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


async def _make_orphan_snapshot(session, *, item_count, n_items, size=1000):
    """An IN_PROGRESS snapshot with no live lease and no partitions — the
    shape STEP B settles — holding `n_items` real snapshot_items rows."""
    from sqlalchemy import text

    sid = uuid.uuid4()
    await session.execute(
        text(
            """
            INSERT INTO snapshots (id, status, started_at, item_count,
                                   bytes_total)
            VALUES (:id, 'IN_PROGRESS', NOW() - INTERVAL '2 hours',
                    :ic, 0)
            """
        ),
        {"id": str(sid), "ic": item_count},
    )
    for _ in range(n_items):
        await session.execute(
            text(
                "INSERT INTO snapshot_items (snapshot_id, content_size) "
                "VALUES (:sid, :sz)"
            ),
            {"sid": str(sid), "sz": size},
        )
    return sid


async def _counters(session, sid):
    from sqlalchemy import text

    return (
        await session.execute(
            text(
                "SELECT status::text, item_count, bytes_total "
                "FROM snapshots WHERE id = :id"
            ),
            {"id": str(sid)},
        )
    ).first()


@pg_only
def test_settled_orphan_gets_its_real_item_count():
    """The prod shape: the walk died, the counter still reads 0, the items
    are all there. After the settle the row must state what it holds."""
    from shared.reconciler import sweep_orphans

    async def _body():
        engine, factory = await _fresh_db()
        try:
            async with factory() as session:
                sid = await _make_orphan_snapshot(
                    session, item_count=0, n_items=7, size=1_000,
                )
                await session.commit()
                await sweep_orphans(session)
                row = await _counters(session, sid)
            assert row.status == "FAILED", (
                "premise moved: a non-partitioned orphan settles FAILED"
            )
            assert row.item_count == 7, (
                f"settled with item_count={row.item_count} while holding 7 "
                f"snapshot_items — retention will read this row as empty and "
                f"hard-delete it with its items"
            )
            assert row.bytes_total == 7_000
        finally:
            await engine.dispose()

    _run(_body())


@pg_only
def test_settle_never_lowers_a_counter_the_handler_wrote():
    """A handler that reported more than snapshot_items can account for is
    not overwritten — GREATEST keeps the write monotonic, which is also what
    makes the 60 s re-run idempotent."""
    from shared.reconciler import sweep_orphans

    async def _body():
        engine, factory = await _fresh_db()
        try:
            async with factory() as session:
                sid = await _make_orphan_snapshot(
                    session, item_count=999, n_items=3, size=10,
                )
                await session.commit()
                await sweep_orphans(session)
                row = await _counters(session, sid)
            assert row.item_count == 999, (
                f"settle lowered item_count 999 → {row.item_count}"
            )
        finally:
            await engine.dispose()

    _run(_body())


@pg_only
def test_restatement_is_idempotent_across_ticks():
    """The sweep runs every 60 s over the same rows. Two ticks must not
    double a counter (which is what a `+=` style restatement would do)."""
    from shared.reconciler import sweep_orphans

    async def _body():
        engine, factory = await _fresh_db()
        try:
            async with factory() as session:
                sid = await _make_orphan_snapshot(
                    session, item_count=0, n_items=4, size=5,
                )
                await session.commit()
                await sweep_orphans(session)
                first = await _counters(session, sid)
                # Re-open the row so STEP B sees it again, as a re-delivered
                # worker message would.
                from sqlalchemy import text
                await session.execute(
                    text("UPDATE snapshots SET status = 'IN_PROGRESS' "
                         "WHERE id = :id"),
                    {"id": str(sid)},
                )
                await session.commit()
                await sweep_orphans(session)
                second = await _counters(session, sid)
            assert (first.item_count, first.bytes_total) == (4, 20)
            assert (second.item_count, second.bytes_total) == (4, 20), (
                f"second tick changed the counters to "
                f"{second.item_count}/{second.bytes_total}"
            )
        finally:
            await engine.dispose()

    _run(_body())
