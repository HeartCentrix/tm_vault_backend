"""Migration 0020 builds the Activity rollup lookup indexes CONCURRENTLY, so TM's large snapshots
table is never write-locked, and it must install for the table-owner role TM's migrate job uses,
which is not a superuser."""
import importlib.util
import os
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

pytestmark = pytest.mark.integration
INDEXES = {"ix_snapshots_job_id", "ix_snapshot_partitions_job_id", "ix_jobs_spec_batch_id"}


def _upgrade(conn):
    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    path = Path(__file__).resolve().parents[2] / "alembic/versions/0020_activity_rollup_indexes.py"
    spec = importlib.util.spec_from_file_location("activity_rollup_indexes_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    context = MigrationContext.configure(conn)
    with Operations.context(context):
        with context.begin_transaction():
            migration.upgrade()


async def test_indexes_build_concurrently_as_a_plain_table_owner():
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires an explicitly disposable PostgreSQL database")
    suffix = uuid4().hex[:12]
    schema, role = f"ar_idx_{suffix}", f"ar_idx_role_{suffix}"
    admin = create_async_engine(url, connect_args={"statement_cache_size": 0})
    owner = create_async_engine(url, connect_args={"statement_cache_size": 0,
                                                   "server_settings": {"search_path": schema}})
    try:
        async with admin.begin() as conn:
            await conn.execute(text(f'CREATE ROLE "{role}" LOGIN NOSUPERUSER'))
            await conn.execute(text(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{role}"'))
        async with owner.connect() as conn:
            await conn.execute(text(f'SET ROLE "{role}"'))
            await conn.execute(text("CREATE TABLE jobs (id uuid PRIMARY KEY, spec json)"))
            await conn.execute(text("CREATE TABLE snapshots (id uuid PRIMARY KEY, job_id uuid)"))
            await conn.execute(text("CREATE TABLE snapshot_partitions (id uuid PRIMARY KEY, job_id uuid)"))
            await conn.commit()
            await conn.run_sync(_upgrade)
            await conn.run_sync(_upgrade)  # re-runnable: IF NOT EXISTS
            built = dict((await conn.execute(text(
                "SELECT c.relname, i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
                "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = :s"), {"s": schema})).all())
            superuser = await conn.scalar(text("SELECT rolsuper FROM pg_roles WHERE rolname = current_user"))
        assert superuser is False
        assert {name: built.get(name) for name in INDEXES} == dict.fromkeys(INDEXES, True)
    finally:
        async with admin.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            await conn.execute(text(f'DROP ROLE IF EXISTS "{role}"'))
        await admin.dispose()
        await owner.dispose()
