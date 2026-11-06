"""Migration 0014's snapshot_items/snapshots indexes are built without blocking writes. A plain
CREATE INDEX locks writes for the whole build; TM's snapshot_items has ~532M rows in 16 hash
partitions, and PostgreSQL cannot build an index on a partitioned parent CONCURRENTLY, so each
partition is built concurrently and attached to an ON ONLY parent index."""
import os
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from shared.protected_size_schema import ONLINE_INDEXES, build_index_online

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture(params=[False, True], ids=["plain", "partitioned"])
async def db(request):
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires explicitly configured disposable PostgreSQL")
    schema = "online_idx_" + uuid4().hex
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}, "statement_cache_size": 0})
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await conn.execute(text("CREATE TABLE snapshots(id uuid PRIMARY KEY)"))
        part = " PARTITION BY HASH(snapshot_id)" if request.param else ""
        await conn.execute(text(f"CREATE TABLE snapshot_items(id uuid NOT NULL, snapshot_id uuid NOT NULL,"
                                f" PRIMARY KEY(id, snapshot_id)){part}"))
        if request.param:
            for n in range(3):
                await conn.execute(text(f"CREATE TABLE snapshot_items_p{n} PARTITION OF snapshot_items"
                                        f" FOR VALUES WITH(MODULUS 3,REMAINDER {n})"))
        await conn.execute(text("INSERT INTO snapshot_items SELECT gen_random_uuid(), gen_random_uuid()"
                                " FROM generate_series(1,50)"))
    yield engine, request.param
    async with engine.begin() as conn:
        await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    await engine.dispose()


async def _build_all(engine):
    async with engine.connect() as conn:
        conn = await conn.execution_options(isolation_level="AUTOCOMMIT")
        for name, table, columns in ONLINE_INDEXES:
            await conn.run_sync(lambda c, n=name, t=table, cols=columns: build_index_online(c, n, t, cols))


async def _state(engine):
    async with engine.connect() as conn:
        rows = (await conn.execute(text(
            "SELECT c.relname, i.indisvalid, (SELECT count(*) FROM pg_inherits h WHERE h.inhparent = c.oid) AS kids "
            "FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
            "WHERE c.relnamespace = current_schema()::regnamespace AND c.relname LIKE 'ix_protected_size_%' "
            "ORDER BY 1"))).all()
    return {r.relname: (r.indisvalid, r.kids) for r in rows}


async def test_indexes_are_built_valid_and_attached_per_partition(db):
    engine, partitioned = db
    await _build_all(engine)
    await _build_all(engine)  # idempotent
    state = await _state(engine)
    for name, table, _ in ONLINE_INDEXES:
        valid, kids = state[name]
        assert valid, (name, state)
        assert kids == (3 if partitioned and table == "snapshot_items" else 0), (name, state)
    if partitioned:
        children = {k: v for k, v in state.items() if k not in {n for n, _, _ in ONLINE_INDEXES}}
        assert len(children) == 6 and all(v for v, _ in children.values()), state


async def test_an_invalid_index_left_by_an_interrupted_build_is_rebuilt(db):
    engine, partitioned = db
    await _build_all(engine)
    async with engine.begin() as conn:
        if partitioned:
            # Interrupted while building partition p1, before the attach: parent invalid, child invalid.
            await conn.execute(text("DROP INDEX ix_protected_size_snapshot_items_page"))
            await conn.execute(text("CREATE INDEX ix_protected_size_snapshot_items_page_p1 ON snapshot_items_p1 (snapshot_id,id)"))
            victim = "ix_protected_size_snapshot_items_page_p1"
        else:
            victim = "ix_protected_size_snapshot_items_page"
        await conn.execute(text("UPDATE pg_index SET indisvalid = false WHERE indexrelid = to_regclass(:n)"), {"n": victim})
    await _build_all(engine)
    state = await _state(engine)
    assert state[victim][0] is True and state["ix_protected_size_snapshot_items_page"][0] is True, state
    if partitioned:
        assert state["ix_protected_size_snapshot_items_page"][1] == 3, state


async def test_an_invalid_attached_partition_index_is_reindexed(db):
    engine, partitioned = db
    if not partitioned:
        pytest.skip("partition indexes only")
    await _build_all(engine)
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE pg_index SET indisvalid = false WHERE indexrelid = to_regclass(:n)"),
                           {"n": "ix_protected_size_snapshot_items_identity_p2"})
    await _build_all(engine)
    state = await _state(engine)
    assert state["ix_protected_size_snapshot_items_identity_p2"][0] is True, state
