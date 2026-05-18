"""Alembic env.py — async-aware, schema-routed, model-aware.

Run order
---------
1. `alembic/versions/` holds migrations. First migration is the
   baseline (20260517_baseline_0001), which is a deliberate no-op:
   it stamps the current `Base.metadata.create_all()` schema as
   revision 0001 without touching DDL. After that, every schema
   change is a new revision file.

2. `online` mode (the only mode we run): drives the async engine
   from `shared.database.engine` using `run_sync(do_run_migrations)`
   — Alembic's async pattern. Falls back to a one-off engine when
   imported standalone (e.g. CI runs `alembic upgrade head` against
   a fresh DB with no app loaded).

3. `offline` mode: emits SQL to stdout against the configured DSN
   without a live connection. Useful for review (PR-time) and for
   running migrations manually via psql.

Why we route through `shared.database.engine` when available
------------------------------------------------------------
The app uses `statement_cache_size=0` (see `shared/database.py`)
because asyncpg's prepared-statement cache survives DDL, producing
`cache lookup failed for type ...` errors after a schema change.
Migrations must use the SAME engine config so the prepared cache
is consistent across migration + app workloads.

Schema isolation
----------------
The app lives in schema `tm_vault` (env: `DB_SCHEMA`). Alembic's
`version_table_schema` is set to the same schema so the
`alembic_version` row sits alongside the tables it tracks — not
in `public`, which we treat as read-only/unused.
"""
from __future__ import annotations

import asyncio
import os
from logging.config import fileConfig
from typing import Optional

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

# Make `shared.*` importable when alembic is invoked from the repo root.
import sys
from pathlib import Path
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Import the app's metadata so `alembic revision --autogenerate` can
# diff models against the live DB. Importing `shared.models` triggers
# the table registrations on `Base.metadata`.
from shared.database import Base  # noqa: E402
import shared.models  # noqa: F401, E402  – needed for side-effect registration

target_metadata = Base.metadata

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)


def _get_dsn() -> str:
    """Resolve the DSN the way the app does — DATABASE_URL wins, then
    DB_HOST/PORT/USERNAME/PASSWORD/NAME. Alembic doesn't get
    `sqlalchemy.url` from alembic.ini (we left it blank) so this
    function is the single source of truth."""
    from shared.config import settings
    return settings.DATABASE_URL


def _schema() -> str:
    return os.getenv("DB_SCHEMA", "tm_vault")


def run_migrations_offline() -> None:
    """Emit SQL to stdout against the configured DSN, no live conn."""
    url = _get_dsn()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        version_table_schema=_schema(),
        include_schemas=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        version_table_schema=_schema(),
        include_schemas=True,
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    """Run migrations against an async engine.

    Tries to reuse `shared.database.engine` so we share pool / cache
    config. If that isn't importable in this context (CI bootstrap),
    falls back to building a one-off engine from the resolved DSN.
    """
    engine = None
    try:
        from shared.database import engine as _shared_engine
        engine = _shared_engine
    except Exception:
        engine = None

    if engine is None:
        engine = async_engine_from_config(
            {"sqlalchemy.url": _get_dsn()},
            prefix="sqlalchemy.",
            poolclass=pool.NullPool,
            future=True,
        )

    async with engine.connect() as connection:
        await connection.run_sync(do_run_migrations)


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
