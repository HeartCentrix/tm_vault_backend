"""Taylor Morrison's production database was built by the old bootstrap (tm_vault_backend 98e1398
init_db) and stamped alembic 0006; snapshot_items is a plain table with PK btree(id). From that
frozen schema (tests/fixtures/tm_0006_bootstrap_schema.sql), `alembic upgrade head` must reach a
schema the runtime accepts, with no INVALID index and no redundant SI(id) index build."""
import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import create_async_engine

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests/fixtures/tm_0006_bootstrap_schema.sql"


def _run(env, *args):
    result = subprocess.run([sys.executable, *args], cwd=ROOT, env=env, capture_output=True, text=True, timeout=600)
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout


async def test_tm_0006_bootstrap_schema_upgrades_to_head_and_verifies():
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires an explicitly disposable PostgreSQL database")
    name = "tm0006_" + uuid4().hex[:12]
    admin = create_async_engine(url, isolation_level="AUTOCOMMIT", connect_args={"statement_cache_size": 0})
    db_url = make_url(url).set(database=name)
    async with admin.connect() as conn:
        await conn.execute(text(f'CREATE DATABASE "{name}"'))
    engine = create_async_engine(db_url, poolclass=NullPool, connect_args={"statement_cache_size": 0})
    try:
        async with engine.connect() as conn:
            raw = await conn.get_raw_connection()
            await raw.driver_connection.execute(FIXTURE.read_text())  # simple protocol: whole script
        env = {**os.environ, "DATABASE_URL": db_url.render_as_string(hide_password=False),
               "DB_SCHEMA": "public", "DB_PGBOUNCER": "false"}
        _run(env, "-m", "alembic", "stamp", "0006")
        _run(env, "-m", "alembic", "upgrade", "head")
        heads = _run(env, "-m", "alembic", "heads").split()[0]
        current = _run(env, "-m", "alembic", "current").split()
        assert heads in current, current
        verify = ("import asyncio\nfrom shared.database import verify_runtime_schema\n"
                  "print('VERIFY', asyncio.run(verify_runtime_schema()))")
        assert "VERIFY True" in _run(env, "-c", verify)

        async with engine.begin() as conn:
            invalid = (await conn.execute(text(
                "SELECT indexrelid::regclass::text FROM pg_index WHERE NOT indisvalid"))).scalars().all()
            indexes = set((await conn.execute(text(
                "SELECT indexname FROM pg_indexes WHERE schemaname='public'"))).scalars())
            # Capture still journals snapshot_items writes without the SI(id) index.
            tenant, resource, snapshot = uuid4(), uuid4(), uuid4()
            await conn.execute(text("INSERT INTO tenants(id,type,display_name,status) "
                                    "VALUES(:t,'M365','t','ACTIVE')"), {"t": tenant})
            await conn.execute(text("INSERT INTO resources(id,tenant_id,type,external_id,display_name,status) "
                                    "VALUES(:r,:t,'MAILBOX','x','r','ACTIVE')"), {"r": resource, "t": tenant})
            await conn.execute(text("INSERT INTO snapshots(id,resource_id,type,status) "
                                    "VALUES(:s,:r,'FULL','COMPLETED')"), {"s": snapshot, "r": resource})
            await conn.execute(text("INSERT INTO snapshot_items(id,snapshot_id,tenant_id,external_id,item_type,name) "
                                    "VALUES(gen_random_uuid(),:s,:t,'e','EMAIL','n')"), {"s": snapshot, "t": tenant})
            journaled = await conn.scalar(text(
                "SELECT count(*) FROM protected_size_journal WHERE kind='items' AND :s=ANY(snapshot_ids)"),
                {"s": snapshot})
        assert invalid == []
        assert "ix_protected_size_snapshot_items_identity" not in indexes  # PK btree(id) reused
        assert "ix_protected_size_snapshots_page" not in indexes
        assert "ix_protected_size_snapshot_items_page" in indexes
        assert {"tenant_dek", "encryption_config"} <= set(await _tables(engine))
        assert journaled == 1
    finally:
        await engine.dispose()
        async with admin.connect() as conn:
            await conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        await admin.dispose()


async def _tables(engine):
    async with engine.connect() as conn:
        return (await conn.execute(text("SELECT tablename FROM pg_tables WHERE schemaname='public'"))).scalars().all()
