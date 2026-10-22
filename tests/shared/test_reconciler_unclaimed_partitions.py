"""STEP A must not poison partitions that were never claimed.

A QUEUED partition whose RabbitMQ message simply hasn't been popped yet
has lease_owner_id NULL and lease_expires_at NULL, so the lease guard
protects nothing: the only gate is `enqueued_at` age vs
RECONCILER_PART_STALE_MIN (15 min). Every sweep past that horizon bumps
requeue_count, and at RECONCILER_MAX_REQUEUE (3, so ~45 min in a
saturated queue) the poison branch flips the shard to FAILED and writes
a work_dead_letter row — for work that is perfectly healthy and still
waiting in the queue. Under the observed worker OOMKill churn and large
fan-outs that is routine, and it fails backups that never even started.

These tests run the real `sweep_orphans` SQL against a real Postgres
(the CTEs, FOR UPDATE SKIP LOCKED and enum casts have no sqlite
equivalent). Point RECONCILER_TEST_PG_URL at a throwaway cluster to run
them, e.g.:

    initdb -D /tmp/pg -U tmv --auth=trust
    pg_ctl -D /tmp/pg -o "-p 55432 -c unix_socket_directories='' \
        -c listen_addresses=127.0.0.1" start
    createdb -h 127.0.0.1 -p 55432 -U tmv tmvtest
    export RECONCILER_TEST_PG_URL=postgresql+asyncpg://tmv@127.0.0.1:55432/tmvtest

Never point it at a real environment: the fixture drops and recreates
the tables it uses.
"""
from __future__ import annotations

import asyncio
import os
import uuid

import pytest

PG_URL = os.getenv("RECONCILER_TEST_PG_URL", "").strip()

pytestmark = pytest.mark.skipif(
    not PG_URL,
    reason="RECONCILER_TEST_PG_URL not set (needs a throwaway Postgres)",
)


# Minimal stand-ins for the columns the sweeper SQL touches. Hand-written
# rather than models.Base.metadata so the fixture doesn't have to satisfy
# the whole FK graph (tenants → resources → jobs → snapshots → ...).
_SCHEMA = [
    "DROP TABLE IF EXISTS snapshot_partitions, worker_heartbeats, "
    "work_dead_letter, snapshots, jobs CASCADE",
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
        worker_region    text,
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
        lease_owner_id   uuid,
        lease_expires_at timestamptz,
        lease_token      bigint NOT NULL DEFAULT 0
    )""",
    """CREATE TABLE jobs (
        id               uuid PRIMARY KEY,
        status           jobstatus NOT NULL DEFAULT 'RUNNING',
        completed_at     timestamptz,
        lease_owner_id   uuid,
        lease_expires_at timestamptz,
        lease_token      bigint NOT NULL DEFAULT 0
    )""",
]


def _run(coro):
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


async def _fresh_db():
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    engine = create_async_engine(PG_URL, poolclass=None)
    async with engine.begin() as conn:
        for stmt in _SCHEMA:
            await conn.execute(text(stmt))
    return engine, async_sessionmaker(engine, expire_on_commit=False)


async def _insert_partition(session, **over):
    """Insert one snapshot_partitions row. Ages are minutes-ago ints."""
    from sqlalchemy import text

    pid = over.pop("id", uuid.uuid4())
    enqueued_min_ago = over.pop("enqueued_min_ago", 0)
    started_min_ago = over.pop("started_min_ago", None)
    lease_expired_min_ago = over.pop("lease_expired_min_ago", None)
    await session.execute(
        text(
            """
            INSERT INTO snapshot_partitions
                (id, snapshot_id, partition_index, partition_type, status,
                 worker_id, retry_count, requeue_count, lease_owner_id,
                 enqueued_at,
                 started_at,
                 lease_expires_at)
            VALUES
                (:id, :snapshot_id, :idx, :ptype, :status,
                 :worker_id, :retry_count, :requeue_count,
                 CAST(:lease_owner_id AS uuid),
                 NOW() - (:enq * INTERVAL '1 minute'),
                 CASE WHEN CAST(:started AS int) IS NULL THEN NULL
                      ELSE NOW() - (CAST(:started AS int)
                                    * INTERVAL '1 minute') END,
                 CASE WHEN CAST(:lease AS int) IS NULL THEN NULL
                      ELSE NOW() - (CAST(:lease AS int)
                                    * INTERVAL '1 minute') END)
            """
        ),
        {
            "id": str(pid),
            "snapshot_id": str(over.pop("snapshot_id", uuid.uuid4())),
            "idx": over.pop("partition_index", 0),
            "ptype": over.pop("partition_type", "CHATS"),
            "status": over.pop("status", "QUEUED"),
            "worker_id": over.pop("worker_id", None),
            "retry_count": over.pop("retry_count", 0),
            "requeue_count": over.pop("requeue_count", 0),
            "lease_owner_id": over.pop("lease_owner_id", None),
            "enq": enqueued_min_ago,
            "started": started_min_ago,
            "lease": lease_expired_min_ago,
        },
    )
    assert not over, f"unused kwargs: {over}"
    return pid


async def _state(session, pid):
    from sqlalchemy import text

    row = (
        await session.execute(
            text(
                """
                SELECT sp.status, sp.requeue_count,
                       (SELECT count(*) FROM work_dead_letter dl
                         WHERE dl.work_id = sp.id) AS dlq_rows
                  FROM snapshot_partitions sp WHERE sp.id = :id
                """
            ),
            {"id": str(pid)},
        )
    ).first()
    return row


def test_never_claimed_queued_partition_is_not_failed_at_requeue_cap():
    """The bug: a healthy, never-claimed QUEUED shard is poisoned."""
    from shared.reconciler import RECONCILER_MAX_REQUEUE, sweep_orphans

    async def _body():
        engine, factory = await _fresh_db()
        try:
            async with factory() as session:
                # Never claimed: no worker_id, no started_at, no lease,
                # retry_count 0 — it is simply still sitting in RabbitMQ
                # behind a saturated queue. requeue_count is at the cap
                # because earlier sweeps already bumped it.
                pid = await _insert_partition(
                    session,
                    status="QUEUED",
                    requeue_count=RECONCILER_MAX_REQUEUE,
                    enqueued_min_ago=90,
                )
                await session.commit()

                stats = await sweep_orphans(session)
                row = await _state(session, pid)

            assert row.status == "QUEUED", (
                f"never-claimed QUEUED partition was poisoned to "
                f"{row.status}: nothing is wrong with it, its message just "
                f"hasn't been popped yet"
            )
            assert row.dlq_rows == 0, (
                "never-claimed partition was dead-lettered as a poison pill"
            )
            assert [p["id"] for p in stats.requeue_payloads] == [str(pid)], (
                "never-claimed partition past the horizon should be "
                "re-published, not failed — "
                f"requeue_payloads={stats.requeue_payloads}"
            )
        finally:
            await engine.dispose()

    _run(_body())


def test_claimed_then_abandoned_partition_still_poisons_at_cap():
    """The circuit breaker must survive for shards a worker really took."""
    from shared.reconciler import RECONCILER_MAX_REQUEUE, sweep_orphans

    async def _body():
        engine, factory = await _fresh_db()
        try:
            async with factory() as session:
                pid = await _insert_partition(
                    session,
                    status="IN_PROGRESS",
                    worker_id="worker-dead-1",
                    lease_owner_id=str(uuid.uuid4()),  # no heartbeat row
                    lease_expired_min_ago=30,
                    started_min_ago=60,
                    requeue_count=RECONCILER_MAX_REQUEUE,
                    enqueued_min_ago=90,
                )
                await session.commit()

                await sweep_orphans(session)
                row = await _state(session, pid)

            assert row.status == "FAILED", (
                f"a claimed-then-abandoned shard at the requeue cap must "
                f"still be failed, got {row.status}"
            )
            assert row.dlq_rows == 1, "poison pill was not dead-lettered"
        finally:
            await engine.dispose()

    _run(_body())


def test_never_claimed_partition_uses_longer_republish_horizon(monkeypatch):
    """Unclaimed shards get a longer horizon so a saturated queue isn't
    amplified with duplicate publishes every RECONCILER_PART_STALE_MIN."""
    import shared.reconciler as rec

    async def _body():
        engine, factory = await _fresh_db()
        try:
            async with factory() as session:
                # 20 min old: past the 15-min claimed horizon, well short
                # of the unclaimed horizon.
                young = await _insert_partition(
                    session, status="QUEUED", enqueued_min_ago=20,
                )
                old = await _insert_partition(
                    session, status="QUEUED", enqueued_min_ago=120,
                )
                await session.commit()

                stats = await rec.sweep_orphans(session)

            ids = {p["id"] for p in stats.requeue_payloads}
            assert str(young) not in ids, (
                "a never-claimed shard only 20 min into a saturated queue "
                "should not be re-published yet"
            )
            assert str(old) in ids, (
                "a never-claimed shard past the unclaimed horizon should be "
                "re-published"
            )
        finally:
            await engine.dispose()

    assert rec.RECONCILER_PART_UNCLAIMED_STALE_MIN > rec.RECONCILER_PART_STALE_MIN
    _run(_body())


async def _unclaimed_rq(session, pid):
    """The never-claimed republish counter STEP A keeps in failure_state."""
    from sqlalchemy import text

    return (
        await session.execute(
            text(
                "SELECT (failure_state->>'unclaimed_republish_count')::int "
                "FROM snapshot_partitions WHERE id = :id"
            ),
            {"id": str(pid)},
        )
    ).scalar()


def test_never_claimed_partition_of_unroutable_type_does_not_hang_forever():
    """No partition type may end up with neither recovery nor a terminal.

    GROUPS_CHANNELS / ENTRA_CATEGORIES rows are real (the backup-worker
    fan-outs persist them), and if a type is missing from
    _PARTITION_QUEUE_BY_TYPE the republish is a silent no-op. With the
    poison branch now refusing never-claimed rows, such a shard would sit
    QUEUED forever — snapshot IN_PROGRESS, job RUNNING, no
    work_dead_letter row, nothing to alert on. It must instead exhaust a
    bounded republish budget and then fail loudly.
    """
    import shared.reconciler as rec

    async def _body():
        engine, factory = await _fresh_db()
        try:
            async with factory() as session:
                pid = await _insert_partition(
                    session,
                    status="QUEUED",
                    partition_type="GROUPS_CHANNELS",
                    enqueued_min_ago=999,
                )
                await session.commit()

                seen = []
                # One sweep per horizon: each sweep resets enqueued_at, so
                # age it back past the horizon before the next tick.
                for _ in range(rec.RECONCILER_PART_UNCLAIMED_MAX_REPUBLISH + 2):
                    await rec.sweep_orphans(session)
                    row = await _state(session, pid)
                    seen.append((row.status, row.requeue_count, row.dlq_rows))
                    await session.execute(
                        __import__("sqlalchemy").text(
                            "UPDATE snapshot_partitions SET enqueued_at = "
                            "NOW() - INTERVAL '999 minutes' WHERE id = :id"
                        ),
                        {"id": str(pid)},
                    )
                    await session.commit()

                final = await _state(session, pid)

            assert final.status == "FAILED", (
                f"an unroutable never-claimed shard never reached a terminal "
                f"state — permanent zombie. sweeps={seen}"
            )
            assert final.dlq_rows == 1, (
                f"no work_dead_letter row, so ops has no signal. sweeps={seen}"
            )
            assert final.requeue_count == 0, (
                "the never-claimed path must not have touched requeue_count"
            )
            # Bounded: exactly MAX_REPUBLISH republishes, then terminal.
            assert sum(1 for s in seen if s[0] == "QUEUED") == (
                rec.RECONCILER_PART_UNCLAIMED_MAX_REPUBLISH
            ), f"republish was not bounded at the budget. sweeps={seen}"
        finally:
            await engine.dispose()

    _run(_body())


def test_queue_wait_does_not_spend_the_claimed_death_requeue_budget():
    """Waiting in a queue then dying once must still get full recovery.

    The cited prod scenario is a saturated queue plus worker OOMKill
    churn: the shard waits hours, is finally claimed, and the worker dies.
    If the waiting spent requeue_count, that first genuine death poisons
    the shard to FAILED immediately with zero recovery requeues.
    """
    import shared.reconciler as rec
    from sqlalchemy import text

    async def _body():
        engine, factory = await _fresh_db()
        try:
            async with factory() as session:
                pid = await _insert_partition(
                    session, status="QUEUED", enqueued_min_ago=999,
                )
                await session.commit()

                # Long queue wait: republish up to the unclaimed budget.
                for _ in range(rec.RECONCILER_PART_UNCLAIMED_MAX_REPUBLISH):
                    await rec.sweep_orphans(session)
                    await session.execute(
                        text(
                            "UPDATE snapshot_partitions SET enqueued_at = "
                            "NOW() - INTERVAL '999 minutes' WHERE id = :id"
                        ),
                        {"id": str(pid)},
                    )
                    await session.commit()

                after_wait = await _state(session, pid)
                unclaimed_n = await _unclaimed_rq(session, pid)

                # Now a worker really claims it and dies (lease lapsed).
                await session.execute(
                    text(
                        """
                        UPDATE snapshot_partitions
                           SET status = 'IN_PROGRESS',
                               worker_id = 'worker-oomkilled',
                               started_at = NOW() - INTERVAL '40 minutes',
                               lease_owner_id = gen_random_uuid(),
                               lease_expires_at = NOW() - INTERVAL '5 minutes',
                               enqueued_at = NOW() - INTERVAL '30 minutes'
                         WHERE id = :id
                        """
                    ),
                    {"id": str(pid)},
                )
                await session.commit()

                stats = await rec.sweep_orphans(session)
                after_death = await _state(session, pid)

            assert after_wait.requeue_count == 0, (
                f"queue waiting spent {after_wait.requeue_count} of the "
                f"claimed-death requeue budget"
            )
            assert unclaimed_n == rec.RECONCILER_PART_UNCLAIMED_MAX_REPUBLISH, (
                f"unclaimed attempts were not counted separately: "
                f"{unclaimed_n}"
            )
            assert after_death.status == "QUEUED", (
                f"first genuine worker death was poisoned straight to "
                f"{after_death.status} instead of being requeued"
            )
            assert after_death.dlq_rows == 0
            assert after_death.requeue_count == 1
            assert stats.partitions_requeued == 1
        finally:
            await engine.dispose()

    _run(_body())
