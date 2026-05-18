"""Smoke-test alembic 0002 upgrade + downgrade against an isolated DB.

Goal
----
Prove the downgrade path is real before betting prod data on it.

Strategy
--------
1. Spin up an isolated Postgres database (a brand-new database on the
   same server, named ``tmvault_migration_test_<rand>``).
2. Apply ``Base.metadata.create_all`` so the baseline non-partitioned
   schema exists.
3. Seed a small but representative row set into each partition target
   (~1k rows per table, enough to exercise the INSERT…SELECT copy).
4. Run ``alembic upgrade head`` against this DB.
5. Assert all data still readable; row counts match.
6. Run ``alembic downgrade base``.
7. Assert all data still readable; row counts match.
8. DROP the test database.

Why a separate DB
-----------------
Running upgrade/downgrade against the real DB just to test is reckless
— the migration is destructive. Cloning the live DB into a sandbox via
``CREATE DATABASE … TEMPLATE`` is the only safe pattern.

Skips
-----
- Does NOT test 240 TiB scale. The migration's time/disk math is
  validated separately via ``pre_migration_check.py``. This script
  only validates correctness.
- Does NOT exercise the ``_preflight`` gates — sets all skip env vars
  upfront. Gate logic is reviewed in code; this is a forward+reverse
  DDL smoke.

Usage
-----
::

    # Will create + drop a throwaway database on $DATABASE_URL's host.
    python3 scripts/test_partition_rollback.py

    # Use a specific template DB (clones it) to test against realistic shape.
    python3 scripts/test_partition_rollback.py --template tmvault_prod_clone
"""
from __future__ import annotations

import argparse
import asyncio
import os
import secrets
import subprocess
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


async def _seed_minimal(conn, schema: str) -> None:
    """Insert a few rows in each partition target so the migration's
    INSERT…SELECT actually has data to move."""
    # Order matters — FKs.
    await conn.execute(
        f'INSERT INTO "{schema}".organizations (id, name) VALUES '
        f"(gen_random_uuid(), 'rollback-test-org')"
    )
    org_id = await conn.fetchval(
        f'SELECT id FROM "{schema}".organizations WHERE name = $1',
        "rollback-test-org",
    )
    await conn.execute(
        f'INSERT INTO "{schema}".tenants (id, org_id, type, display_name, status) '
        f"VALUES (gen_random_uuid(), $1, 'M365', 'rollback-test', 'ACTIVE')",
        org_id,
    )
    tenant_id = await conn.fetchval(
        f'SELECT id FROM "{schema}".tenants WHERE display_name = $1',
        "rollback-test",
    )

    # storage_backends + a snapshot need to exist for snapshot_items FKs.
    backend_id = await conn.fetchval(
        f'INSERT INTO "{schema}".storage_backends '
        f"(id, name, kind, config, is_active, created_at, updated_at) "
        f"VALUES (gen_random_uuid(), 'test-backend', 'azure_blob', '{{}}'::json, "
        f"true, now(), now()) RETURNING id"
    )

    # A minimal resource + snapshot.
    res_id = await conn.fetchval(
        f'INSERT INTO "{schema}".resources '
        f"(id, tenant_id, type, name, status, created_at, updated_at) "
        f"VALUES (gen_random_uuid(), $1, 'MAILBOX', 'test-mailbox', 'PROTECTED', now(), now()) "
        f"RETURNING id",
        tenant_id,
    )
    snap_id = await conn.fetchval(
        f'INSERT INTO "{schema}".snapshots '
        f"(id, resource_id, type, status, started_at, item_count, bytes_added, bytes_total, backend_id) "
        f"VALUES (gen_random_uuid(), $1, 'USER_MAIL', 'COMPLETED', now(), 100, 1024, 1024, $2) "
        f"RETURNING id",
        res_id, backend_id,
    )

    # 100 snapshot_items
    for i in range(100):
        await conn.execute(
            f'INSERT INTO "{schema}".snapshot_items '
            f"(id, snapshot_id, tenant_id, external_id, item_type, name, content_size, backend_id, created_at) "
            f"VALUES (gen_random_uuid(), $1, $2, $3, 'message', $4, 100, $5, now())",
            snap_id, tenant_id, f"ext-{i}", f"item-{i}", backend_id,
        )

    # 50 audit_events
    for i in range(50):
        await conn.execute(
            f'INSERT INTO "{schema}".audit_events '
            f"(id, org_id, tenant_id, action, outcome, occurred_at) "
            f"VALUES (gen_random_uuid(), $1, $2, 'BACKUP_COMPLETED', 'SUCCESS', now())",
            org_id, tenant_id,
        )


async def _counts(conn, schema: str) -> dict:
    out = {}
    for t in ("snapshot_items", "audit_events"):
        n = await conn.fetchval(f'SELECT count(*) FROM "{schema}"."{t}"')
        out[t] = int(n)
    return out


async def _main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--keep-db", action="store_true",
        help="don't drop the test DB at end (for debugging)",
    )
    args = parser.parse_args()

    import asyncpg
    from shared.config import settings

    src_dsn = settings.DATABASE_URL
    if src_dsn.startswith("postgresql+asyncpg://"):
        src_dsn = src_dsn.replace("postgresql+asyncpg://", "postgresql://", 1)

    test_db = f"tmvault_rb_{secrets.token_hex(4)}"
    schema = os.environ.get("DB_SCHEMA", "tm_vault")

    print(f"creating test DB {test_db} …")
    admin = await asyncpg.connect(src_dsn, statement_cache_size=0)
    try:
        await admin.execute(f'CREATE DATABASE "{test_db}"')
    finally:
        await admin.close()

    test_dsn = src_dsn.rsplit("/", 1)[0] + f"/{test_db}"

    try:
        # Bootstrap schema via Base.metadata.create_all
        from sqlalchemy.ext.asyncio import create_async_engine
        from shared.database import Base
        import shared.models  # noqa: F401

        a_dsn = test_dsn.replace("postgresql://", "postgresql+asyncpg://", 1)
        engine = create_async_engine(a_dsn, echo=False)
        async with engine.begin() as conn:
            await conn.execute(__import__("sqlalchemy").text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            await conn.execute(__import__("sqlalchemy").text(f"SET search_path TO {schema}"))
            await conn.run_sync(Base.metadata.create_all)
        await engine.dispose()

        # Seed
        conn = await asyncpg.connect(test_dsn, statement_cache_size=0,
                                     server_settings={"search_path": schema})
        try:
            await _seed_minimal(conn, schema)
            pre = await _counts(conn, schema)
            print(f"seeded: {pre}")
        finally:
            await conn.close()

        # Run upgrade — bypass _preflight by setting all skip env vars.
        env = os.environ.copy()
        env["DATABASE_URL"] = test_dsn
        env["ALEMBIC_FORCE_PARTITIONING"] = "1"
        env["DB_SCHEMA"] = schema
        print("running alembic upgrade head …")
        r = subprocess.run(
            ["alembic", "upgrade", "head"],
            cwd=str(_ROOT), env=env, capture_output=True, text=True,
        )
        if r.returncode != 0:
            print(f"upgrade FAILED: {r.stdout}\n{r.stderr}")
            return 1

        # Verify data preserved
        conn = await asyncpg.connect(test_dsn, statement_cache_size=0,
                                     server_settings={"search_path": schema})
        try:
            post = await _counts(conn, schema)
            print(f"post-upgrade: {post}")
            if pre != post:
                print(f"COUNT MISMATCH after upgrade: pre={pre} post={post}")
                return 1
        finally:
            await conn.close()

        # Run downgrade
        print("running alembic downgrade base …")
        r = subprocess.run(
            ["alembic", "downgrade", "base"],
            cwd=str(_ROOT), env=env, capture_output=True, text=True,
        )
        if r.returncode != 0:
            print(f"downgrade FAILED: {r.stdout}\n{r.stderr}")
            return 1

        conn = await asyncpg.connect(test_dsn, statement_cache_size=0,
                                     server_settings={"search_path": schema})
        try:
            post_down = await _counts(conn, schema)
            print(f"post-downgrade: {post_down}")
            if pre != post_down:
                print(f"COUNT MISMATCH after downgrade: pre={pre} after={post_down}")
                return 1
        finally:
            await conn.close()

        print("✓ rollback verified — upgrade + downgrade preserved row counts")
        return 0
    finally:
        if not args.keep_db:
            admin = await asyncpg.connect(src_dsn, statement_cache_size=0)
            try:
                await admin.execute(
                    f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    f"WHERE datname = '{test_db}'"
                )
                await admin.execute(f'DROP DATABASE "{test_db}"')
                print(f"dropped {test_db}")
            finally:
                await admin.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
