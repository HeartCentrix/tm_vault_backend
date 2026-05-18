# Alembic — schema migrations for TMvault

The app's schema is owned by SQLAlchemy models in `shared/models.py`. Until 2026-05-17 the schema was bootstrapped at runtime via `Base.metadata.create_all`; that only emits `CREATE TABLE` for *new* tables and silently no-ops on column / index / enum / FK changes. Safe for greenfield demos, **unsafe in prod** once data is real.

This directory adds Alembic so schema changes can be:

- **Versioned** — every migration is a file under `alembic/versions/`.
- **Reviewable** — diffs read like normal SQLAlchemy DSL.
- **Reversible** — every revision has `upgrade()` and `downgrade()`.
- **Stamped** — running databases can be marked at a revision without re-running DDL.

## Layout

```
tm_backend/
├── alembic.ini                  # config: location + log levels
├── alembic/
│   ├── env.py                   # async-aware runner; reuses shared.database.engine
│   ├── script.py.mako           # template for `alembic revision`
│   └── versions/
│       └── 20260517_0001_baseline.py   # no-op baseline (stamp target)
```

## Cutover plan

1. **Today** — `init_db()` still runs `Base.metadata.create_all`. Existing Railway DB is unchanged.
2. Run `alembic stamp head` once per environment to seed `tm_vault.alembic_version` with `0001`. No DDL fires.
3. From this point on, any schema change is authored as a new revision file. The next migration to add (`0002_*`) will partition the big tables (see PR for #44).
4. Once every env is at >= 0001 and we have one real revision merged, `init_db()` becomes a thin shim that calls `alembic upgrade head` instead of `create_all`.

## Common operations

```bash
# Apply all pending migrations against $DATABASE_URL
alembic upgrade head

# Show current revision
alembic current

# History — what migrations exist
alembic history --verbose

# Author a new migration by diffing models vs DB
alembic revision --autogenerate -m "add foo column to bar"

# Stamp without running (idempotent on already-migrated DBs)
alembic stamp head

# Preview SQL without executing (review-time)
alembic upgrade head --sql
```

## Async caveats

`alembic/env.py` uses `async_engine_from_config` with `NullPool` and reuses `shared.database.engine` when importable. `statement_cache_size=0` is inherited from `shared.config` to avoid asyncpg cache-after-DDL crashes.

## Versioning conventions

- Revision IDs are simple zero-padded ints (`0001`, `0002`, ...) — easier to read than UUIDs in logs.
- File names: `YYYYMMDD_NNNN_slug.py`.
- One concern per revision. Cross-table changes are allowed but every revision must leave the DB in a writable state (no half-applied indexes).
- `downgrade()` must be implemented and tested. Skipping it locks us out of rollback.

## Multi-tenant / future

When TMvault adds tenants the version table stays per-DB. Per-tenant schema is enforced by `DB_SCHEMA` + `search_path`; Alembic respects this via `version_table_schema` in `env.py`.
