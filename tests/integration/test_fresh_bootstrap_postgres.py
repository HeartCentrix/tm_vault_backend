"""Greenfield install (tmvault-migrate FRESH arm, local compose): DB_SCHEMA_MODE=bootstrap init_db
on an empty database, then `alembic stamp 0006` + `upgrade head`, must leave a schema verify-mode
services accept, including migration-only objects a `stamp head` would have skipped."""
import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]


def _run(env, *args):
    result = subprocess.run([sys.executable, *args], cwd=ROOT, env=env, capture_output=True, text=True, timeout=600)
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout


async def test_fresh_bootstrap_then_upgrade_head_verifies():
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires an explicitly disposable PostgreSQL database")
    name = "fresh_" + uuid4().hex[:12]
    admin = create_async_engine(url, isolation_level="AUTOCOMMIT", connect_args={"statement_cache_size": 0})
    async with admin.connect() as conn:
        await conn.execute(text(f'CREATE DATABASE "{name}"'))
    try:
        env = {**os.environ, "DATABASE_URL": make_url(url).set(database=name).render_as_string(hide_password=False),
               "DB_SCHEMA": "public", "DB_PGBOUNCER": "false"}
        # Exactly the migrate job's FRESH arm: only shared.database is imported.
        _run({**env, "DB_SCHEMA_MODE": "bootstrap"}, "-c",
             "import asyncio\nfrom shared.database import init_db\nasyncio.run(init_db())")
        _run(env, "-m", "alembic", "stamp", "0006")
        _run(env, "-m", "alembic", "upgrade", "head")
        assert "VERIFY True" in _run({**env, "DB_SCHEMA_MODE": "verify"}, "-c",
            "import asyncio\nfrom shared.database import verify_runtime_schema\n"
            "print('VERIFY', asyncio.run(verify_runtime_schema()))")
        fresh = create_async_engine(env["DATABASE_URL"])
        try:
            async with fresh.connect() as conn:
                missing = (await conn.execute(text(
                    "SELECT t FROM unnest(ARRAY['protected_size_control','protected_size_journal',"
                    "'resource_protected_bytes','tenant_dek']) t WHERE to_regclass(t) IS NULL"))).scalars().all()
        finally:
            await fresh.dispose()
        assert missing == []
    finally:
        async with admin.connect() as conn:
            await conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        await admin.dispose()
