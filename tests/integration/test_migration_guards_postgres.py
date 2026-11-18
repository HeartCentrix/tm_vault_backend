"""Migration guards for TM's online upgrade: hot-table DDL gives up after MIGRATION_LOCK_TIMEOUT
with an operator message instead of queueing every writer behind it, and a concurrent index
build that left an INVALID index fails loudly instead of IF NOT EXISTS silently keeping it."""
import importlib.util
import os
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

pytestmark = pytest.mark.integration
VERSIONS = Path(__file__).resolve().parents[2] / "alembic/versions"


def _upgrade(conn, filename):
    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    spec = importlib.util.spec_from_file_location("guarded_" + filename[:4], VERSIONS / filename)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    context = MigrationContext.configure(conn)
    with Operations.context(context):
        with context.begin_transaction():
            migration.upgrade()


@pytest.fixture
async def schema():
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires an explicitly disposable PostgreSQL database")
    name = "mig_guard_" + uuid4().hex[:12]
    engine = create_async_engine(url, connect_args={"statement_cache_size": 0,
                                                    "server_settings": {"search_path": name}})
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{name}"'))
    yield engine
    async with engine.begin() as conn:
        await conn.execute(text(f'DROP SCHEMA "{name}" CASCADE'))
    await engine.dispose()


async def test_lock_timeout_names_the_blocker_instead_of_queueing(schema, monkeypatch):
    from shared.migration_guard import lock_timeout
    monkeypatch.setenv("MIGRATION_LOCK_TIMEOUT", "200ms")
    async with schema.begin() as conn:
        await conn.execute(text("CREATE TABLE snapshots(id uuid PRIMARY KEY)"))

    def alter(sync):
        with lock_timeout(sync, "test ALTER"):
            sync.execute(text("ALTER TABLE snapshots ADD COLUMN x int"))

    async with schema.connect() as holder, schema.connect() as migrator:
        await holder.execute(text("SELECT 1 FROM snapshots"))  # idle in transaction, ACCESS SHARE held
        with pytest.raises(RuntimeError, match="idle in transaction"):
            await migrator.run_sync(alter)
        await migrator.rollback()
        await holder.rollback()
        await migrator.run_sync(alter)  # retry succeeds once the blocker is gone
        assert await migrator.scalar(text("SHOW lock_timeout")) == "0"  # restored after the block


async def test_0020_refuses_an_invalid_index_left_by_an_interrupted_build(schema):
    async with schema.begin() as conn:
        await conn.execute(text("CREATE TABLE jobs (id uuid PRIMARY KEY, spec json)"))
        await conn.execute(text("CREATE TABLE snapshots (id uuid PRIMARY KEY, job_id uuid)"))
        await conn.execute(text("CREATE TABLE snapshot_partitions (id uuid PRIMARY KEY, job_id uuid)"))
        await conn.execute(text("CREATE INDEX ix_snapshots_job_id ON snapshots(job_id)"))
        await conn.execute(text("UPDATE pg_index SET indisvalid=false WHERE indexrelid=to_regclass('ix_snapshots_job_id')"))
    async with schema.connect() as conn:
        with pytest.raises(RuntimeError, match="DROP INDEX CONCURRENTLY ix_snapshots_job_id"):
            await conn.run_sync(_upgrade, "0020_activity_rollup_indexes.py")


async def test_0009_refuses_an_invalid_index_left_by_an_interrupted_build(schema):
    async with schema.begin() as conn:
        await conn.execute(text("CREATE TABLE resources (id uuid PRIMARY KEY, tenant_id uuid, type text,"
                                " parent_resource_id uuid)"))
        await conn.execute(text("CREATE INDEX uq_resource_tier2_child_per_parent ON resources(tenant_id)"))
        await conn.execute(text("UPDATE pg_index SET indisvalid=false "
                                "WHERE indexrelid=to_regclass('uq_resource_tier2_child_per_parent')"))
    async with schema.connect() as conn:
        with pytest.raises(RuntimeError, match="DROP INDEX CONCURRENTLY uq_resource_tier2_child_per_parent"):
            await conn.run_sync(_upgrade, "0009_uq_tier2_child_per_parent.py")
