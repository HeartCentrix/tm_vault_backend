"""Retained-reference ledger against an explicitly disposable PostgreSQL database."""

import importlib
import os
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture(params=[False, True], ids=["plain", "partitioned"])
async def ledger(request):
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires an explicitly disposable PostgreSQL database")
    schema = "protected_size_test_" + uuid4().hex
    engine = create_async_engine(url, connect_args={"statement_cache_size": 0})

    @event.listens_for(engine.sync_engine, "begin")
    def isolated(conn):
        conn.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')

    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await conn.execute(
            text("CREATE TABLE resources(id uuid PRIMARY KEY,tenant_id uuid,type text)")
        )
        await conn.execute(
            text(
                "CREATE TABLE snapshots(id uuid PRIMARY KEY,resource_id uuid REFERENCES resources(id) ON DELETE CASCADE,status text,created_at timestamp)"
                + (" PARTITION BY HASH(id)" if request.param else "")
            )
        )
        if request.param:
            for n in range(2):
                await conn.execute(
                    text(
                        f"CREATE TABLE snapshots_p{n} PARTITION OF snapshots FOR VALUES WITH(MODULUS 2,REMAINDER {n})"
                    )
                )
        await conn.execute(
            text(
                "CREATE TABLE snapshot_items(id uuid NOT NULL,snapshot_id uuid NOT NULL,external_id text,item_type text,content_size bigint,is_deleted boolean,created_at timestamp)"
                + (" PARTITION BY HASH(snapshot_id)" if request.param else "")
            )
        )
        if request.param:
            for n in range(2):
                await conn.execute(
                    text(
                        f"CREATE TABLE snapshot_items_p{n} PARTITION OF snapshot_items FOR VALUES WITH(MODULUS 2,REMAINDER {n})"
                    )
                )
        await conn.execute(
            text(
                "CREATE TABLE resource_protected_bytes(resource_id uuid PRIMARY KEY REFERENCES resources(id) ON DELETE CASCADE,latest_snapshot_id uuid NOT NULL,bytes bigint NOT NULL DEFAULT 0,computed_at timestamptz NOT NULL DEFAULT now())"
            )
        )
        module = importlib.import_module("shared.protected_size_schema")
        await module.install(conn)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    tenant, resource = uuid4(), uuid4()
    async with factory.begin() as db:
        await db.execute(
            text("INSERT INTO resources VALUES(:r,:t,'USER_MAIL')"),
            {"r": resource, "t": tenant},
        )

    async def sql(query, **params):
        async with factory.begin() as db:
            return await db.execute(text(query), params)

    async def snapshot(status="COMPLETED", resource_id=resource):
        sid = uuid4()
        await sql(
            "INSERT INTO snapshots VALUES(:s,:r,:status,now())",
            s=sid,
            r=resource_id,
            status=status,
        )
        return sid

    async def item(
        sid,
        key="a",
        size=10,
        *,
        item_id=None,
        deleted=False,
        when="2026-01-01",
        kind="mail",
        table="snapshot_items",
    ):
        iid = item_id or uuid4()
        await sql(
            f"INSERT INTO {table} VALUES(:i,:s,:k,:kind,:z,:d,:when)",
            i=iid,
            s=sid,
            k=key,
            kind=kind,
            z=size,
            d=deleted,
            when=__import__("datetime").datetime.fromisoformat(when),
        )
        return iid

    async def summary(tenants=(tenant,), types=None):
        from shared.protected_size import read_summary

        async with factory.begin() as db:
            return await read_summary(db, tenant_ids=tenants, service_types=types)

    async def drain(limit=100, page_size=2):
        from shared.protected_size import refresh_once

        turns = []
        for _ in range(limit):
            turn = await refresh_once(factory, page_size=page_size)
            turns.append(turn)
            if turn["status"] == "idle":
                return turns
        pytest.fail(f"ledger did not converge: {turns[-5:]}")

    yield SimpleNamespace(
        factory=factory,
        sql=sql,
        snapshot=snapshot,
        item=item,
        summary=summary,
        drain=drain,
        resource=resource,
        tenant=tenant,
        schema=schema,
        partitioned=request.param,
    )
    async with engine.begin() as conn:
        # Schema-specific event trigger must be removed before its function.
        from shared.protected_size_schema import event_trigger_names

        for trigger in event_trigger_names(schema):
            await conn.execute(text(f'DROP EVENT TRIGGER IF EXISTS "{trigger}"'))
        await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    await engine.dispose()


async def test_retained_history_parity_and_live_deltas(ledger):
    a, b = await ledger.snapshot(), await ledger.snapshot("PARTIAL")
    first = await ledger.item(a, "same", 100)
    newer = await ledger.item(b, "same", 25, when="2026-02-01")
    await ledger.item(b, "sparse", 7)
    await ledger.item(b, "same", 999, deleted=True, when="2026-03-01")
    # Identity deliberately ignores item_type, like existing protected metric.
    await ledger.item(a, "sparse", 4, kind="other", when="2025-01-01")
    assert (await ledger.summary())["totalBytes"] is None
    turns = await ledger.drain()
    result = await ledger.summary()
    assert result["totalBytes"] == 32 and result["freshness"] == "fresh"
    assert max(t.get("referencesTouched", 0) for t in turns) <= 2
    await ledger.sql("DELETE FROM snapshot_items WHERE id=:i", i=newer)
    stale = await ledger.summary()
    assert stale["totalBytes"] == 32 and stale["stale"] is True
    await ledger.drain()
    assert (await ledger.summary())["totalBytes"] == 107
    await ledger.sql("UPDATE snapshot_items SET content_size=3 WHERE id=:i", i=first)
    await ledger.drain()
    assert (await ledger.summary())["totalBytes"] == 10
    from shared.protected_size import optimized_rollup

    expected = (await ledger.sql(optimized_rollup, rid=ledger.resource)).scalar()
    assert expected == 10


async def test_snapshot_eligibility_reparent_and_rollback(ledger):
    s = await ledger.snapshot("IN_PROGRESS")
    await ledger.item(s, "x", 9)
    await ledger.drain()
    assert (await ledger.summary())["eligibleCount"] == 0
    await ledger.sql("UPDATE snapshots SET status='COMPLETED' WHERE id=:s", s=s)
    assert (await ledger.summary())["freshness"] in ("missing", "stale")
    await ledger.drain()
    assert (await ledger.summary())["totalBytes"] == 9
    before = (await ledger.sql("SELECT count(*) FROM protected_size_journal")).scalar()
    async with ledger.factory() as db:
        await db.execute(
            text("UPDATE snapshot_items SET content_size=123 WHERE snapshot_id=:s"),
            {"s": s},
        )
        await db.rollback()
    assert (
        await ledger.sql("SELECT count(*) FROM protected_size_journal")
    ).scalar() == before
    await ledger.sql("UPDATE snapshots SET status=status WHERE id=:s", s=s)
    assert (
        await ledger.sql("SELECT count(*) FROM protected_size_journal")
    ).scalar() == before
    r2 = uuid4()
    await ledger.sql(
        "INSERT INTO resources VALUES(:r,:t,'USER_CHATS')", r=r2, t=ledger.tenant
    )
    await ledger.sql("UPDATE snapshots SET resource_id=:r WHERE id=:s", r=r2, s=s)
    await ledger.drain()
    assert (await ledger.summary(types=("USER_MAIL",)))["totalBytes"] == 0
    assert (await ledger.summary(types=("USER_CHATS",)))["totalBytes"] == 9
    assert (await ledger.summary(tenants=()))["totalBytes"] == 0
    await ledger.sql("DELETE FROM resources WHERE id=:r", r=r2)
    assert (await ledger.summary())["totalBytes"] == 0


async def test_ties_null_identity_and_journal_batches(ledger):
    s = await ledger.snapshot()
    lo, hi = __import__("uuid").UUID(int=1), __import__("uuid").UUID(int=2)
    await ledger.item(s, "tie", 2, item_id=lo)
    await ledger.item(s, "tie", 8, item_id=hi)
    await ledger.item(s, None, 5)
    await ledger.drain()
    assert (await ledger.summary())["totalBytes"] == 13
    await ledger.sql(
        "INSERT INTO snapshot_items SELECT gen_random_uuid(),:s,'bulk-'||n,'mail',1,false,now() FROM generate_series(1,2001) n",
        s=s,
    )
    assert (
        await ledger.sql(
            "SELECT max(cardinality(item_ids)) FROM protected_size_journal"
        )
    ).scalar() <= 1000
    await ledger.drain(limit=1100, page_size=1000)
    assert (await ledger.summary())["totalBytes"] == 2014


async def test_future_partition_direct_writes_are_captured(ledger):
    if not ledger.partitioned:
        return
    await ledger.sql("ALTER TABLE snapshot_items DETACH PARTITION snapshot_items_p0")
    await ledger.sql("DROP TABLE snapshot_items_p0")
    await ledger.sql(
        "CREATE TABLE snapshot_items_p0 PARTITION OF snapshot_items FOR VALUES WITH(MODULUS 2,REMAINDER 0)"
    )
    s = await ledger.snapshot()
    table = (
        await ledger.sql("SELECT tableoid::regclass::text FROM snapshot_items LIMIT 1")
    ).scalar()
    # Find routing without inserting, then perform a direct leaf write.
    table = (
        await ledger.sql(
            "SELECT CASE WHEN satisfies_hash_partition('snapshot_items'::regclass,2,0,CAST(:s AS uuid)) THEN 'snapshot_items_p0' ELSE 'snapshot_items_p1' END",
            s=s,
        )
    ).scalar()
    await ledger.item(s, "direct", 19, table=table)
    await ledger.drain()
    assert (await ledger.summary())["totalBytes"] == 19


async def test_uncommitted_low_event_id_is_not_skipped(ledger):
    s = await ledger.snapshot()
    a = await ledger.item(s, "a", 1)
    b = await ledger.item(s, "b", 2)
    await ledger.drain()
    async with ledger.factory() as delayed:
        await delayed.execute(
            text("UPDATE snapshot_items SET content_size=10 WHERE id=:i"), {"i": a}
        )
        # A later sequence number commits first; the earlier transaction remains invisible.
        await ledger.sql("UPDATE snapshot_items SET content_size=20 WHERE id=:i", i=b)
        await ledger.drain()
        assert (await ledger.summary())["totalBytes"] == 21
        await delayed.commit()
    assert (await ledger.summary())["stale"]
    await ledger.drain()
    assert (await ledger.summary())["totalBytes"] == 30


async def test_pages_survive_churn_crash_and_old_owner_fencing(ledger):
    from shared.protected_size import _claim, _source_page, _apply, refresh_once

    s = await ledger.snapshot()
    await ledger.item(s, "a", 1)
    await ledger.item(s, "b", 2)
    # Complete discovery first, then abandon a fenced claim as if the process died.
    while (
        await ledger.sql("SELECT discovery_done FROM protected_size_control")
    ).scalar() is False:
        await refresh_once(ledger.factory, page_size=1)
    owner = uuid4()
    old = await _claim(ledger.factory, owner, 5000)
    assert (await refresh_once(ledger.factory, page_size=1))["status"] == "busy"
    page = await _source_page(ledger.factory, old, 1, 5000, False)
    await ledger.sql(
        "UPDATE protected_size_control SET lease_until=clock_timestamp()-interval '1 second'"
    )
    new = await _claim(ledger.factory, uuid4(), 5000)
    assert (await _apply(ledger.factory, old, page, 1, 5000))["status"] == "lost_lease"
    newpage = await _source_page(ledger.factory, new, 1, 5000, False)
    await _apply(ledger.factory, new, newpage, 1, 5000)
    await ledger.item(s, "late", 4, when="2026-04-01")
    await ledger.drain(page_size=1)
    assert (await ledger.summary())["totalBytes"] == 7
    # Replaying an acknowledged batch is idempotent; it reads current scalar refs.
    await ledger.sql(
        "UPDATE protected_size_targets SET acknowledged_at=NULL,processed_offset=0"
    )
    await ledger.drain(page_size=1)
    assert (await ledger.summary())["totalBytes"] == 7


async def test_resource_turns_are_fair_and_winner_probe_uses_index(ledger):
    from shared.protected_size import refresh_once

    r2 = uuid4()
    await ledger.sql("INSERT INTO resources VALUES(:r,:t,'USER_MAIL')", r=r2, t=uuid4())
    a, b = await ledger.snapshot(), await ledger.snapshot(resource_id=r2)
    for n in range(8):
        await ledger.item(a, str(n), 1)
    await ledger.item(b, "small", 3)
    while not (
        await ledger.sql("SELECT discovery_done FROM protected_size_control")
    ).scalar():
        await refresh_once(ledger.factory, page_size=1)
    turns = [await refresh_once(ledger.factory, page_size=1) for _ in range(4)]
    assert len({t.get("resourceId") for t in turns}) == 2
    await ledger.drain(page_size=1)
    assert (await ledger.summary())["totalBytes"] == 8
    async with ledger.factory.begin() as db:
        await db.execute(text("SET LOCAL enable_seqscan=off"))
        plan = "\n".join(
            (
                await db.execute(
                    text(
                        "EXPLAIN SELECT * FROM snapshot_items WHERE snapshot_id=:s AND id>:i ORDER BY id LIMIT 1"
                    ),
                    {"s": a, "i": __import__("uuid").UUID(int=0)},
                )
            ).scalars()
        )
    assert (
        "ix_protected_size_snapshot_items_page" in plan or "_snapshot_id_id_idx" in plan
    )


async def test_source_read_then_new_commit_never_publishes_fresh_obsolete_total(ledger):
    from shared.protected_size import _claim, _source_page, _apply

    s = await ledger.snapshot()
    i = await ledger.item(s, "x", 1)
    await ledger.drain()
    await ledger.sql("UPDATE snapshot_items SET content_size=2 WHERE id=:i", i=i)
    work = await _claim(ledger.factory, uuid4(), 5000)
    page = await _source_page(ledger.factory, work, 2, 5000, False)
    await ledger.sql("UPDATE snapshot_items SET content_size=3 WHERE id=:i", i=i)
    await _apply(ledger.factory, work, page, 2, 5000)
    assert (await ledger.summary())["freshness"] == "stale"
    await ledger.drain()
    assert (await ledger.summary())["totalBytes"] == 3


async def test_prepopulated_partition_attach_backfills_retained_rows(ledger):
    if not ledger.partitioned:
        return
    s = await ledger.snapshot()
    await ledger.drain()
    # Select a snapshot hashing to partition 0; detached data must be discovered on attach.
    while not (
        await ledger.sql(
            "SELECT satisfies_hash_partition('snapshot_items'::regclass,2,0,CAST(:s AS uuid))",
            s=s,
        )
    ).scalar():
        s = await ledger.snapshot()
    await ledger.drain()
    await ledger.sql("ALTER TABLE snapshot_items DETACH PARTITION snapshot_items_p0")
    # Drop capture on this standalone table to simulate an imported prepopulated partition.
    for op in ("insert", "update", "delete"):
        await ledger.sql(f"DROP TRIGGER protected_size_{op} ON snapshot_items_p0")
    await ledger.item(s, "attached", 23, table="snapshot_items_p0")
    await ledger.sql(
        "ALTER TABLE snapshot_items ATTACH PARTITION snapshot_items_p0 FOR VALUES WITH(MODULUS 2,REMAINDER 0)"
    )
    assert (await ledger.summary())["freshness"] != "fresh"
    await ledger.drain()
    assert (await ledger.summary())["totalBytes"] == 23


async def test_known_sum_remains_visible_with_missing_resource(ledger):
    s = await ledger.snapshot()
    await ledger.item(s, "known", 11)
    await ledger.drain()
    other = uuid4()
    await ledger.sql(
        "INSERT INTO resources VALUES(:r,:t,'USER_MAIL')", r=other, t=ledger.tenant
    )
    await ledger.snapshot(resource_id=other)
    result = await ledger.summary()
    assert result["totalBytes"] == 11 and result["partial"]
    assert result["missingCount"] == 1 and result["freshCount"] == 1


async def test_bounded_cleanup_runs_after_resource_deleted_and_journal_retention(
    ledger,
):
    s = await ledger.snapshot()
    for n in range(6):
        await ledger.item(s, str(n), 1)
    await ledger.drain()
    await ledger.sql("DELETE FROM resources WHERE id=:r", r=ledger.resource)
    assert (await ledger.summary())["totalBytes"] == 0
    turns = await ledger.drain(page_size=1)
    assert (await ledger.sql("SELECT count(*) FROM protected_size_refs")).scalar() == 0
    assert (
        await ledger.sql("SELECT count(*) FROM protected_size_winners")
    ).scalar() == 0
    assert max(t.get("cleanupRows", 0) for t in turns) <= 6
    await ledger.sql(
        "UPDATE protected_size_journal SET created_at=now()-interval '2 days'"
    )
    await ledger.drain(page_size=1)
    assert (
        await ledger.sql("SELECT count(*) FROM protected_size_journal")
    ).scalar() == 0


async def test_nonmetric_metadata_and_content_noops_do_not_append_events(ledger):
    s = await ledger.snapshot()
    i = await ledger.item(s, "x", 1)
    await ledger.drain()
    before = (await ledger.sql("SELECT count(*) FROM protected_size_journal")).scalar()
    await ledger.sql(
        "UPDATE snapshot_items SET item_type='new-type',content_size=content_size WHERE id=:i",
        i=i,
    )
    await ledger.sql(
        "UPDATE snapshots SET created_at=now(),status='PARTIAL' WHERE id=:s", s=s
    )
    assert (
        await ledger.sql("SELECT count(*) FROM protected_size_journal")
    ).scalar() == before
    assert (await ledger.summary())["freshness"] == "fresh"


async def test_snapshot_deletion_restores_prior_version_and_item_reparent(ledger):
    a, b = await ledger.snapshot(), await ledger.snapshot("PENDING_DELETION")
    await ledger.item(a, "x", 12)
    await ledger.item(b, "x", 3, when="2026-02-01")
    await ledger.drain()
    await ledger.sql("DELETE FROM snapshots WHERE id=:s", s=b)
    await ledger.drain()
    assert (await ledger.summary())["totalBytes"] == 12
    # A raw SQL move from a retained snapshot must invalidate both resources.
    r2 = uuid4()
    await ledger.sql(
        "INSERT INTO resources VALUES(:r,:t,'USER_CHATS')", r=r2, t=ledger.tenant
    )
    c = await ledger.snapshot(resource_id=r2)
    await ledger.sql(
        "UPDATE snapshot_items SET snapshot_id=:s WHERE snapshot_id=:old", s=c, old=a
    )
    await ledger.drain()
    assert (await ledger.summary(types=("USER_MAIL",)))["totalBytes"] == 0
    assert (await ledger.summary(types=("USER_CHATS",)))["totalBytes"] == 12


async def test_detach_partition_removes_old_refs_and_reattach_restores_them(ledger):
    if not ledger.partitioned:
        return
    s = await ledger.snapshot()
    await ledger.item(s, "x", 14)
    await ledger.drain()
    part = (
        await ledger.sql(
            "SELECT tableoid::regclass::text FROM snapshot_items WHERE snapshot_id=:s",
            s=s,
        )
    ).scalar()
    remainder = 0 if part.endswith("p0") else 1
    await ledger.sql(f"ALTER TABLE snapshot_items DETACH PARTITION {part}")
    await ledger.drain()
    assert (await ledger.summary())["totalBytes"] == 0
    await ledger.sql(
        f"ALTER TABLE snapshot_items ATTACH PARTITION {part} FOR VALUES WITH(MODULUS 2,REMAINDER {remainder})"
    )
    await ledger.drain()
    assert (await ledger.summary())["totalBytes"] == 14


async def test_unbounded_source_identity_and_hash_collisions_preserve_exact_identity(
    ledger, monkeypatch
):
    from shared import protected_size

    monkeypatch.setattr(
        protected_size, "_identity_hash", lambda _: "forced-collision", raising=False
    )
    s = await ledger.snapshot()
    # Incompressible IDs exceed a B-tree tuple's limit and remain legal source text.
    key = "".join(uuid4().hex for _ in range(150))
    await ledger.item(s, key, 3)
    await ledger.item(s, key + "different", 7)
    await ledger.drain()
    assert (await ledger.summary())["totalBytes"] == 10


async def test_discovery_page_cannot_resurrect_deleted_snapshot(ledger):
    from shared.protected_size import _claim, _source_page, _apply

    s = await ledger.snapshot()
    await ledger.item(s, "x", 10)
    work = await _claim(ledger.factory, uuid4(), 5000)
    page = await _source_page(ledger.factory, work, 2, 5000, False)
    await ledger.sql("DELETE FROM snapshots WHERE id=:s", s=s)
    await _apply(ledger.factory, work, page, 2, 5000)
    assert (await ledger.summary())["eligibleCount"] == 0
    await ledger.drain()
    assert (await ledger.summary())["totalBytes"] == 0


async def test_snapshot_partition_detach_reconciles_known_eligibility(ledger):
    if not ledger.partitioned:
        return
    s = await ledger.snapshot()
    await ledger.item(s, "x", 15)
    await ledger.drain()
    part = (
        await ledger.sql(
            "SELECT tableoid::regclass::text FROM snapshots WHERE id=:s", s=s
        )
    ).scalar()
    await ledger.sql(f"ALTER TABLE snapshots DETACH PARTITION {part}")
    await ledger.drain()
    result = await ledger.summary()
    assert result["eligibleCount"] == 0 and result["totalBytes"] == 0


async def test_partition_attach_restarts_existing_snapshot_job_cursor(ledger):
    if not ledger.partitioned:
        return
    from shared.protected_size import refresh_once

    s = await ledger.snapshot()
    await ledger.item(s, "existing", 30)
    for _ in range(30):
        await refresh_once(ledger.factory, page_size=1)
        cursor = (
            await ledger.sql(
                "SELECT cursor FROM protected_size_jobs WHERE snapshot_id=:s AND phase='source'",
                s=s,
            )
        ).scalar()
        if cursor is not None:
            break
    assert cursor is not None
    part = (
        await ledger.sql(
            "SELECT tableoid::regclass::text FROM snapshot_items WHERE snapshot_id=:s",
            s=s,
        )
    ).scalar()
    remainder = 0 if part.endswith("p0") else 1
    await ledger.sql(f"ALTER TABLE snapshot_items DETACH PARTITION {part}")
    for op in ("insert", "update", "delete"):
        await ledger.sql(f"DROP TRIGGER protected_size_{op} ON {part}")
    await ledger.item(
        s, "behind-cursor", 30, item_id=__import__("uuid").UUID(int=1), table=part
    )
    await ledger.sql(
        f"ALTER TABLE snapshot_items ATTACH PARTITION {part} FOR VALUES WITH(MODULUS 2,REMAINDER {remainder})"
    )
    await ledger.drain(page_size=1)
    assert (await ledger.summary())["totalBytes"] == 60


async def test_item_committing_after_snapshot_reparent_routes_to_current_owner(ledger):
    s = await ledger.snapshot()
    await ledger.item(s, "base", 10)
    await ledger.drain()
    other = uuid4()
    await ledger.sql(
        "INSERT INTO resources VALUES(:r,:t,'USER_CHATS')", r=other, t=ledger.tenant
    )
    async with ledger.factory() as delayed:
        await delayed.execute(
            text(
                "INSERT INTO snapshot_items VALUES(:i,:s,'late','mail',99,false,now())"
            ),
            {"i": uuid4(), "s": s},
        )
        await ledger.sql(
            "UPDATE snapshots SET resource_id=:r WHERE id=:s", r=other, s=s
        )
        await ledger.drain()
        assert (await ledger.summary(types=("USER_CHATS",)))["totalBytes"] == 10
        await delayed.commit()
    result = await ledger.summary(types=("USER_CHATS",))
    assert result["freshness"] == "stale"
    await ledger.drain()
    assert (await ledger.summary(types=("USER_CHATS",)))["totalBytes"] == 109


async def test_drop_populated_partition_invalidates_and_prunes_retained_references(
    ledger,
):
    if not ledger.partitioned:
        return
    s = await ledger.snapshot()
    await ledger.item(s, "x", 10)
    await ledger.drain()
    part = (
        await ledger.sql(
            "SELECT tableoid::regclass::text FROM snapshot_items WHERE snapshot_id=:s",
            s=s,
        )
    ).scalar()
    await ledger.sql(f"DROP TABLE {part}")
    assert (await ledger.summary())["freshness"] != "fresh"
    await ledger.drain()
    assert (await ledger.summary())["totalBytes"] == 0


async def test_repeated_physical_uuid_across_snapshots_stays_one_identity_per_page(
    ledger,
):
    from shared.protected_size import _claim, _source_page, _apply

    same = uuid4()
    snapshots = [await ledger.snapshot() for _ in range(5)]
    for n, s in enumerate(snapshots):
        await ledger.item(s, str(n), 1, item_id=same)
    await ledger.drain(page_size=1)
    await ledger.sql(
        "UPDATE snapshot_items SET content_size=2 WHERE snapshot_id=:s AND id=:i",
        s=snapshots[0],
        i=same,
    )
    work = await _claim(ledger.factory, uuid4(), 5000)
    assert work["event"]["kind"] == "items"
    page = await _source_page(ledger.factory, work, 1, 5000, False)
    assert len(page["rows"]) == 1
    await _apply(ledger.factory, work, page, 1, 5000)
    await ledger.drain(page_size=1)
    assert (await ledger.summary())["totalBytes"] == 6


async def test_backfill_page_gets_turn_under_continuous_item_events(ledger):
    from shared.protected_size import refresh_once

    s = await ledger.snapshot()
    await ledger.sql(
        "INSERT INTO snapshot_items SELECT gen_random_uuid(),:s,'bulk-'||n,'mail',1,false,now() FROM generate_series(1,30) n",
        s=s,
    )
    while not (
        await ledger.sql("SELECT discovery_done FROM protected_size_control")
    ).scalar():
        await refresh_once(ledger.factory, page_size=1)
    phases = []
    for n in range(8):
        await ledger.item(s, "new-" + str(n), 1)
        phases.append((await refresh_once(ledger.factory, page_size=1)).get("phase"))
    assert "job" in phases


async def test_truncate_sources_fails_closed(ledger):
    from sqlalchemy.exc import DBAPIError

    for table in ("snapshot_items", "snapshots"):
        with pytest.raises(DBAPIError, match="TRUNCATE is unsupported"):
            await ledger.sql(f"TRUNCATE {table}")
    if ledger.partitioned:
        with pytest.raises(DBAPIError, match="TRUNCATE is unsupported"):
            await ledger.sql("TRUNCATE snapshot_items_p0")


async def test_old_pending_journal_cleanup_sweeps_bounded_candidates(ledger):
    from shared.protected_size import refresh_once

    s = await ledger.snapshot()
    for n in range(9):
        await ledger.item(s, str(n), 1)
    await ledger.sql("UPDATE protected_size_journal SET created_at='2020-01-01'")
    turns = []
    for _ in range(6):
        turns.append(await refresh_once(ledger.factory, page_size=1))
    assert any(t["status"] == "progress" for t in turns)
    # Sweep moves one candidate at a time instead of rescanning an old pending prefix.
    assert (
        await ledger.sql("SELECT retirement_id FROM protected_size_control")
    ).scalar() > 0
    await ledger.drain(page_size=1)
    assert (await ledger.summary())["totalBytes"] == 9


async def test_real_replica_lag_defers_without_primary_fallback(ledger):
    import asyncio
    from shared.protected_size import refresh_once

    url = os.environ.get("TMVAULT_TEST_REPLICA_URL")
    if not url:
        pytest.skip("requires an explicitly disposable streaming replica")
    s = await ledger.snapshot()
    i = await ledger.item(s, "x", 9)
    await ledger.drain()
    engine = create_async_engine(
        url,
        connect_args={"statement_cache_size": 0, "prepared_statement_cache_size": 0},
    )

    @event.listens_for(engine.sync_engine, "begin")
    def schema(conn):
        conn.exec_driver_sql(f'SET LOCAL search_path TO "{ledger.schema}"')

    ro = async_sessionmaker(engine, expire_on_commit=False)

    async def caught_up():
        lsn = (await ledger.sql("SELECT pg_current_wal_insert_lsn()::text")).scalar()
        for _ in range(300):
            async with ro.begin() as db:
                ready = await db.scalar(
                    text(
                        "SELECT pg_last_wal_replay_lsn()>=CAST(CAST(:lsn AS text) AS pg_lsn)"
                    ),
                    {"lsn": str(lsn)},
                )
            if ready:
                return
            await asyncio.sleep(0.01)
        pytest.fail("disposable replica did not catch up")

    try:
        await caught_up()
        async with ro.begin() as db:
            assert await db.scalar(text("SELECT pg_is_in_recovery()"))
            await db.execute(text("SELECT pg_wal_replay_pause()"))
        try:
            await ledger.sql(
                "UPDATE snapshot_items SET content_size=20 WHERE id=:i", i=i
            )
            result = await refresh_once(ledger.factory, source_factory=ro, page_size=1)
            assert result["status"] == "replica_deferred"
            assert (await ledger.summary())["freshness"] == "stale"
            assert (
                await ledger.sql(
                    "SELECT working_bytes FROM resource_protected_bytes WHERE resource_id=:r",
                    r=ledger.resource,
                )
            ).scalar() == 9
        finally:
            async with ro.begin() as db:
                await db.execute(text("SELECT pg_wal_replay_resume()"))
        await caught_up()
        await ledger.sql("UPDATE resource_protected_bytes SET next_attempt='epoch'")
        for _ in range(100):
            turn = await refresh_once(ledger.factory, source_factory=ro, page_size=1)
            if (await ledger.summary())["freshness"] == "fresh":
                break
            if turn["status"] == "replica_deferred":
                await caught_up()
                await ledger.sql(
                    "UPDATE resource_protected_bytes SET next_attempt='epoch'"
                )
        assert (await ledger.summary())["totalBytes"] == 20
    finally:
        await engine.dispose()


async def test_real_deadlock_returns_retry_and_new_transaction_completes(ledger):
    import asyncio
    from sqlalchemy.exc import DBAPIError
    from shared.protected_size import refresh_once

    s = await ledger.snapshot()
    i = await ledger.item(s, "x", 1)
    await ledger.drain()
    await ledger.sql("UPDATE snapshot_items SET content_size=2 WHERE id=:i", i=i)

    def fast_deadlock(conn):
        conn.exec_driver_sql("SET LOCAL deadlock_timeout='100ms'")

    event.listen(ledger.factory.kw["bind"].sync_engine, "begin", fast_deadlock)
    async with ledger.factory() as blocker:
        await blocker.execute(text("SET LOCAL deadlock_timeout='2s'"))
        await blocker.execute(
            text(
                "SELECT * FROM resource_protected_bytes WHERE resource_id=:r FOR UPDATE"
            ),
            {"r": ledger.resource},
        )
        task = asyncio.create_task(refresh_once(ledger.factory, page_size=1))
        await asyncio.sleep(0.05)
        # Complete the opposite lock order. The worker's bounded lock timeout or
        # PostgreSQL deadlock detector must free it and leave a usable next call.
        try:
            await blocker.execute(
                text("SELECT * FROM protected_size_control FOR UPDATE")
            )
        except DBAPIError:
            await blocker.rollback()
        result = await task
        await blocker.rollback()
    assert result["status"] == "retry"
    assert result["sqlstate"] == "40P01"
    event.remove(ledger.factory.kw["bind"].sync_engine, "begin", fast_deadlock)
    await ledger.drain()
    assert (await ledger.summary())["totalBytes"] == 2


async def test_real_serialization_conflict_retries_whole_page_without_delta_duplication(
    ledger,
):
    from sqlalchemy.ext.asyncio import AsyncSession
    from shared.protected_size import refresh_once

    s = await ledger.snapshot()
    i = await ledger.item(s, "x", 1)
    await ledger.drain()
    await ledger.sql("UPDATE snapshot_items SET content_size=2 WHERE id=:i", i=i)
    ordinary = ledger.factory.kw["bind"]
    injected = False

    class SerializableSession(AsyncSession):
        async def execute(self, statement, *args, **kwargs):
            nonlocal injected
            result = await super().execute(statement, *args, **kwargs)
            if not injected and str(statement).startswith(
                "SELECT DISTINCT f.identity_id"
            ):
                injected = True
                # This real concurrent update changes the tuple version after
                # the serializable page has read it, before its DELETE.
                await ledger.sql(
                    "UPDATE protected_size_refs SET size=size WHERE resource_id=:r",
                    r=ledger.resource,
                )
            return result

    factory = async_sessionmaker(
        ordinary.execution_options(isolation_level="SERIALIZABLE"),
        class_=SerializableSession,
        expire_on_commit=False,
    )
    result = await refresh_once(factory, page_size=1)
    assert injected and result["status"] == "retry" and result["sqlstate"] == "40001"
    assert (await ledger.summary())["freshness"] == "stale"
    await ledger.sql("UPDATE resource_protected_bytes SET next_attempt='epoch'")
    await ledger.drain()
    assert (await ledger.summary())["totalBytes"] == 2


async def test_replica_snapshot_must_include_claim_marker_not_only_volatile_lsn(ledger):
    import asyncio
    from sqlalchemy.ext.asyncio import AsyncSession
    from shared.protected_size import _claim, _source_page, _release

    url = os.environ.get("TMVAULT_TEST_REPLICA_URL")
    if not url:
        pytest.skip("requires an explicitly disposable streaming replica")
    s = await ledger.snapshot()
    i = await ledger.item(s, "x", 9)
    await ledger.drain()
    engine = create_async_engine(
        url,
        connect_args={"statement_cache_size": 0, "prepared_statement_cache_size": 0},
    )

    @event.listens_for(engine.sync_engine, "begin")
    def schema(conn):
        conn.exec_driver_sql(f'SET LOCAL search_path TO "{ledger.schema}"')

    plain = async_sessionmaker(engine, expire_on_commit=False)

    async def catchup(lsn):
        for _ in range(300):
            async with plain.begin() as db:
                if await db.scalar(
                    text(
                        "SELECT pg_last_wal_replay_lsn()>=CAST(CAST(:lsn AS text) AS pg_lsn)"
                    ),
                    {"lsn": lsn},
                ):
                    return
            await asyncio.sleep(0.01)
        pytest.fail("replica did not catch up")

    try:
        await catchup(
            (await ledger.sql("SELECT pg_current_wal_insert_lsn()::text")).scalar()
        )
        async with plain.begin() as db:
            await db.execute(text("SELECT pg_wal_replay_pause()"))
        await ledger.sql("UPDATE snapshot_items SET content_size=20 WHERE id=:i", i=i)
        work = await _claim(ledger.factory, uuid4(), 5000)

        class SnapshotRaceSession(AsyncSession):
            async def scalar(self, statement, *args, **kwargs):
                if str(statement).startswith("SELECT instance_id="):
                    # Establish the old RR snapshot, then allow replay past the
                    # barrier before the original readiness SELECT evaluates.
                    await super().execute(
                        text("SELECT singleton FROM protected_size_control")
                    )
                    async with plain.begin() as resume:
                        await resume.execute(text("SELECT pg_wal_replay_resume()"))
                    await catchup(work["barrier"])
                return await super().scalar(statement, *args, **kwargs)

        racing = async_sessionmaker(
            engine, class_=SnapshotRaceSession, expire_on_commit=False
        )
        page = await _source_page(racing, work, 1, 5000, True)
        assert page is None
        await _release(ledger.factory, work, 5000)
        await ledger.sql("UPDATE resource_protected_bytes SET next_attempt='epoch'")
        await ledger.drain()
        assert (await ledger.summary())["totalBytes"] == 20
    finally:
        async with plain.begin() as db:
            await db.execute(text("SELECT pg_wal_replay_resume()"))
        await engine.dispose()


async def test_completed_routes_leave_fast_path_while_audit_journal_is_retained(ledger):
    s = await ledger.snapshot()
    await ledger.item(s, "x", 1)
    await ledger.drain()
    assert (
        await ledger.sql("SELECT count(*) FROM protected_size_journal")
    ).scalar() > 0
    assert (
        await ledger.sql("SELECT count(*) FROM protected_size_event_snapshots")
    ).scalar() == 0


async def test_fast_summary_buffers_do_not_scale_with_acknowledged_journal_history(
    ledger,
):
    import json
    from shared.protected_size import SUMMARY_SQL

    s = await ledger.snapshot()
    await ledger.item(s, "x", 1)
    await ledger.drain()
    await ledger.sql(
        """WITH history AS(INSERT INTO protected_size_journal(kind,snapshot_ids,item_ids)
      SELECT 'items',ARRAY[CAST(:s AS uuid)],ARRAY[gen_random_uuid()] FROM generate_series(1,100001) RETURNING id)
      INSERT INTO protected_size_targets(event_id,resource_id,acknowledged_at)
      SELECT id,:r,now() FROM history""",
        s=s,
        r=ledger.resource,
    )
    await ledger.sql("ANALYZE protected_size_targets")
    await ledger.sql("ANALYZE protected_size_journal")
    plan = (
        await ledger.sql(
            "EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) " + SUMMARY_SQL,
            all_tenants=False,
            tenants=[ledger.tenant],
            all_types=True,
            types=[],
        )
    ).scalar()
    if isinstance(plan, str):
        plan = json.loads(plan)
    assert plan[0]["Plan"]["Shared Hit Blocks"] < 150
    assert (await ledger.summary())["totalBytes"] == 1


async def test_retired_cleanup_pages_candidates_before_dependent_row_checks(ledger):
    import json
    from shared.protected_size import _cleanup

    s = await ledger.snapshot()
    await ledger.drain()
    await ledger.sql(
        """WITH history AS(INSERT INTO protected_size_journal(kind,snapshot_ids,item_ids)
      SELECT 'items',ARRAY[CAST(:s AS uuid)],ARRAY[gen_random_uuid()] FROM generate_series(1,10000) RETURNING id)
      INSERT INTO protected_size_retired SELECT id FROM history""",
        s=s,
    )
    await ledger.sql(
        """INSERT INTO protected_size_event_snapshots SELECT CAST(:s AS uuid),event_id FROM protected_size_retired
      UNION ALL SELECT CAST(:other AS uuid),event_id FROM protected_size_retired""",
        s=s,
        other=uuid4(),
    )
    await ledger.sql("ANALYZE protected_size_retired")
    await ledger.sql("ANALYZE protected_size_event_snapshots")
    captured = []

    def capture(conn, clause, multiparams, params, execution_options):
        if str(clause).startswith("WITH ready AS"):
            captured.append((str(clause), dict(params)))

    engine = ledger.factory.kw["bind"]
    event.listen(engine.sync_engine, "before_execute", capture)
    try:
        async with ledger.factory.begin() as db:
            await _cleanup(db, 1)
    finally:
        event.remove(engine.sync_engine, "before_execute", capture)
    assert captured
    query, args = captured[-1]
    plan = (
        await ledger.sql("EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) " + query, **args)
    ).scalar()
    if isinstance(plan, str):
        plan = json.loads(plan)
    assert plan[0]["Plan"]["Shared Hit Blocks"] < 100
    assert len(args["ids"]) == 1


async def test_ddl_between_source_read_and_apply_preserves_last_published_bytes(ledger):
    from shared.protected_size import _claim, _source_page, _apply

    s = await ledger.snapshot()
    item = await ledger.item(s, "x", 1)
    await ledger.drain()
    await ledger.sql("UPDATE snapshot_items SET content_size=2 WHERE id=:i", i=item)
    work = await _claim(ledger.factory, uuid4(), 5000)
    page = await _source_page(ledger.factory, work, 1, 5000, False)
    await ledger.sql("ALTER TABLE snapshot_items ADD COLUMN unrelated_metadata text")
    await _apply(ledger.factory, work, page, 1, 5000)
    assert (
        await ledger.sql(
            "SELECT bytes FROM resource_protected_bytes WHERE resource_id=:r",
            r=ledger.resource,
        )
    ).scalar() == 1
    await ledger.drain()
    assert (await ledger.summary())["totalBytes"] == 2


async def test_legacy_cache_writer_cannot_replace_authoritative_ledger_publication(
    ledger,
):
    s = await ledger.snapshot()
    item = await ledger.item(s, "x", 25)
    await ledger.drain()
    before = await ledger.summary()
    # Exact old dashboard cache-upsert shape from the rolling predecessor.
    await ledger.sql(
        """INSERT INTO resource_protected_bytes(resource_id,latest_snapshot_id,bytes,computed_at)
      VALUES(:r,:s,100,now()) ON CONFLICT(resource_id) DO UPDATE SET
      latest_snapshot_id=EXCLUDED.latest_snapshot_id,bytes=EXCLUDED.bytes,computed_at=EXCLUDED.computed_at""",
        r=ledger.resource,
        s=s,
    )
    assert (await ledger.drain())[-1]["status"] == "idle"
    after = await ledger.summary()
    assert after["totalBytes"] == 25 and after["freshness"] == "fresh"
    assert after["asOf"] == before["asOf"]
    await ledger.sql("UPDATE snapshot_items SET content_size=7 WHERE id=:i", i=item)
    await ledger.drain()
    assert (await ledger.summary())["totalBytes"] == 7


@pytest_asyncio.fixture
async def runtime_store():
    """DBA installs into existing sources owned by an ordinary application role."""
    url = os.environ.get("TMVAULT_TEST_RUNTIME_DATABASE_URL") or os.environ.get(
        "TMVAULT_TEST_DATABASE_URL"
    )
    if not url:
        pytest.skip("requires an explicitly disposable PostgreSQL database")
    token = uuid4().hex
    schema = "ps-runtime-schema_" + token
    other_schema = "ps_other_schema_" + token
    source_role, app_role, other_role = [
        "ps_" + prefix + "_" + token for prefix in ("source", "app", "other")
    ]
    engine = create_async_engine(
        url,
        connect_args={"statement_cache_size": 0, "prepared_statement_cache_size": 0},
    )

    @event.listens_for(engine.sync_engine, "begin")
    def isolated(conn):
        conn.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')

    async with engine.begin() as db:
        for role in (source_role, app_role, other_role):
            await db.execute(
                text(
                    f'CREATE ROLE "{role}" NOSUPERUSER NOCREATEDB NOCREATEROLE NOLOGIN'
                )
            )
        await db.execute(
            text(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{source_role}"')
        )
        await db.execute(
            text(f'CREATE SCHEMA "{other_schema}" AUTHORIZATION "{other_role}"')
        )
        await db.execute(text(f'SET LOCAL ROLE "{source_role}"'))
        for ddl in (
            "CREATE TABLE resources(id uuid PRIMARY KEY,tenant_id uuid,type text)",
            "CREATE TABLE snapshots(id uuid PRIMARY KEY,resource_id uuid REFERENCES resources(id) ON DELETE CASCADE,status text,created_at timestamp)",
            "CREATE TABLE snapshot_items(id uuid NOT NULL,snapshot_id uuid NOT NULL,external_id text,item_type text,content_size bigint,is_deleted boolean,created_at timestamp) PARTITION BY HASH(snapshot_id)",
            "CREATE TABLE snapshot_items_p0 PARTITION OF snapshot_items FOR VALUES WITH(MODULUS 2,REMAINDER 0)",
            "CREATE TABLE snapshot_items_p1 PARTITION OF snapshot_items FOR VALUES WITH(MODULUS 2,REMAINDER 1)",
            "CREATE TABLE resource_protected_bytes(resource_id uuid PRIMARY KEY REFERENCES resources(id) ON DELETE CASCADE,latest_snapshot_id uuid NOT NULL,bytes bigint NOT NULL DEFAULT 0,computed_at timestamptz NOT NULL DEFAULT now())",
        ):
            await db.execute(text(ddl))
    admin = async_sessionmaker(engine, expire_on_commit=False)
    role_engines = []

    def factory(role, *, target_schema=schema):
        role_engine = create_async_engine(
            url,
            connect_args={
                "statement_cache_size": 0,
                "prepared_statement_cache_size": 0,
            },
        )

        @event.listens_for(role_engine.sync_engine, "begin")
        def scope(conn):
            conn.exec_driver_sql(f'SET LOCAL ROLE "{role}"')
            conn.exec_driver_sql(f'SET LOCAL search_path TO "{target_schema}"')

        role_engines.append(role_engine)
        return async_sessionmaker(role_engine, expire_on_commit=False)

    async def install(**kwargs):
        from shared.protected_size_schema import install

        async with admin.begin() as db:
            await install(await db.connection(), **kwargs)

    try:
        yield SimpleNamespace(
            admin=admin,
            install=install,
            factory=factory,
            schema=schema,
            other_schema=other_schema,
            source_role=source_role,
            app_role=app_role,
            other_role=other_role,
        )
    finally:
        from shared.protected_size_schema import event_trigger_names

        for role_engine in role_engines:
            await role_engine.dispose()
        async with engine.begin() as db:
            for trigger in event_trigger_names(schema):
                await db.execute(text(f'DROP EVENT TRIGGER IF EXISTS "{trigger}"'))
            await db.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
            await db.execute(text(f'DROP SCHEMA "{other_schema}" CASCADE'))
            for role in (source_role, app_role, other_role):
                await db.execute(text(f'DROP ROLE "{role}"'))
        await engine.dispose()


@pytest.mark.parametrize("explicit_runtime", [False, True])
async def test_dba_migration_preserves_ordinary_source_owner_capture_read_and_refresh(
    runtime_store,
    explicit_runtime,
):
    from shared.protected_size import read_summary, refresh_once

    if explicit_runtime:
        async with runtime_store.admin.begin() as db:
            await db.execute(
                text(
                    f'GRANT USAGE ON SCHEMA "{runtime_store.schema}" TO "{runtime_store.app_role}"'
                )
            )
            await db.execute(
                text(
                    f'GRANT SELECT ON resources,snapshots,snapshot_items TO "{runtime_store.app_role}"'
                )
            )
    await runtime_store.install(
        **({"runtime_role": runtime_store.app_role} if explicit_runtime else {})
    )
    factory = runtime_store.factory(runtime_store.source_role)
    worker = (
        runtime_store.factory(runtime_store.app_role) if explicit_runtime else factory
    )
    tenant, resource, snapshot, item = [uuid4() for _ in range(4)]
    async with factory.begin() as db:
        await db.execute(
            text("INSERT INTO resources VALUES(:r,:t,'USER_MAIL')"),
            {"r": resource, "t": tenant},
        )
        await db.execute(
            text("INSERT INTO snapshots VALUES(:s,:r,'IN_PROGRESS',now())"),
            {"s": snapshot, "r": resource},
        )
        await db.execute(
            text("INSERT INTO snapshot_items VALUES(:i,:s,'x','mail',9,false,now())"),
            {"i": item, "s": snapshot},
        )
        await db.execute(
            text("UPDATE snapshots SET status='COMPLETED' WHERE id=:s"), {"s": snapshot}
        )
    for _ in range(30):
        if (await refresh_once(worker, page_size=1))["status"] == "idle":
            break
    async with worker.begin() as db:
        result = await read_summary(db, tenant_ids=(tenant,), service_types=None)
        assert result["totalBytes"] == 9 and result["freshness"] == "fresh"
        if explicit_runtime:
            assert not await db.scalar(
                text(
                    "SELECT has_table_privilege(current_user,'snapshot_items','INSERT')"
                )
            )
    async with factory.begin() as db:
        # Ordinary source-owner DDL creates a future covered direct-write leaf.
        await db.execute(
            text("ALTER TABLE snapshot_items DETACH PARTITION snapshot_items_p0")
        )
        await db.execute(text("DROP TABLE snapshot_items_p0"))
        await db.execute(
            text(
                "CREATE TABLE snapshot_items_p0 PARTITION OF snapshot_items FOR VALUES WITH(MODULUS 2,REMAINDER 0)"
            )
        )

        leaf = (
            "snapshot_items_p0"
            if await db.scalar(
                text(
                    "SELECT satisfies_hash_partition('snapshot_items'::regclass,2,0,CAST(:s AS uuid))"
                ),
                {"s": snapshot},
            )
            else "snapshot_items_p1"
        )
        await db.execute(
            text(f"INSERT INTO {leaf} VALUES(:i,:s,'new','mail',3,false,now())"),
            {"i": uuid4(), "s": snapshot},
        )
        assert await db.scalar(text("SELECT count(*) FROM protected_size_journal")) > 0
        await db.execute(text("DELETE FROM resources WHERE id=:r"), {"r": resource})
    for _ in range(30):
        if (await refresh_once(worker, page_size=1))["status"] == "idle":
            break
    async with worker.begin() as db:
        assert (await read_summary(db, tenant_ids=(tenant,)))["totalBytes"] == 0
        assert await db.scalar(text("SELECT count(*) FROM protected_size_orphans")) == 0


async def test_unrelated_ordinary_schema_ddl_does_not_need_ledger_schema_usage(
    runtime_store,
):
    await runtime_store.install()
    async with runtime_store.admin.begin() as db:
        before = await db.scalar(
            text("SELECT discovery_epoch FROM protected_size_control")
        )
    factory = runtime_store.factory(
        runtime_store.other_role, target_schema=runtime_store.other_schema
    )
    async with factory.begin() as db:
        assert not await db.scalar(
            text("SELECT has_schema_privilege(current_user,:s,'USAGE')"),
            {"s": runtime_store.schema},
        )
        await db.execute(text("CREATE TABLE unrelated(id integer)"))
        await db.execute(text("ALTER TABLE unrelated ADD COLUMN more integer"))
        await db.execute(text("DROP TABLE unrelated"))
    async with runtime_store.admin.begin() as db:
        assert (
            await db.scalar(text("SELECT discovery_epoch FROM protected_size_control"))
            == before
        )


@pytest.mark.parametrize("failure", ["missing", "usage", "select", "split_owner"])
async def test_runtime_role_preflight_rejects_before_ledger_ddl(runtime_store, failure):
    from shared.protected_size_schema import statements

    selected = runtime_store.app_role
    expected = {
        "missing": "does not exist",
        "usage": "schema USAGE",
        "select": "existing SELECT",
        "split_owner": "split ownership",
    }[failure]
    async with runtime_store.admin.begin() as db:
        if failure == "missing":
            selected = "missing_" + uuid4().hex
        elif failure == "select":
            await db.execute(
                text(f'GRANT USAGE ON SCHEMA "{runtime_store.schema}" TO "{selected}"')
            )
        elif failure == "split_owner":
            await db.execute(text(f'ALTER TABLE snapshots OWNER TO "{selected}"'))
        # Even an executor committing each yielded statement must fail before schema changes.
        entries = iter(statements(runtime_store.schema, runtime_role=selected))
        await db.execute(text(next(entries)))
        with pytest.raises(Exception, match=expected):
            async with db.begin_nested():
                await db.execute(text(next(entries)))
        assert (
            await db.scalar(text("SELECT to_regclass('protected_size_control')"))
            is None
        )
        assert not await db.scalar(
            text(
                "SELECT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='resource_protected_bytes'::regclass AND attname='ledger_bytes')"
            )
        )
        assert await db.scalar(
            text(
                "SELECT attnotnull FROM pg_attribute WHERE attrelid='resource_protected_bytes'::regclass AND attname='latest_snapshot_id'"
            )
        )


async def test_ddl_helpers_are_private_fixed_path_and_migration_owned(runtime_store):
    await runtime_store.install()
    async with runtime_store.admin.begin() as db:
        rows = (
            (
                await db.execute(
                    text("""SELECT p.proname,p.prosecdef,p.proconfig,p.proowner=(SELECT oid FROM pg_roles WHERE rolname=current_user) migration_owned,
          EXISTS(SELECT 1 FROM aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) a WHERE a.grantee=0 AND a.privilege_type='EXECUTE') public_execute
          FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=:schema AND p.proname LIKE 'protected_size_%'"""),
                    {"schema": runtime_store.schema},
                )
            )
            .mappings()
            .all()
        )
    helpers = {
        "protected_size_install_triggers",
        "protected_size_partition_ddl",
        "protected_size_partition_drop",
    }
    assert {r["proname"] for r in rows if r["prosecdef"]} == helpers
    for row in rows:
        assert row["migration_owned"]
        if row["proname"] in helpers:
            assert not row["public_execute"]
            assert row["proconfig"] == [
                f'search_path=pg_catalog, "{runtime_store.schema}", pg_temp'
            ]
    factory = runtime_store.factory(runtime_store.source_role)
    async with factory.begin() as db:
        with pytest.raises(Exception, match="permission denied for function"):
            async with db.begin_nested():
                await db.execute(text("SELECT protected_size_install_triggers()"))
