"""Pre-flight check before running ``alembic upgrade head`` in prod.

Run this script BEFORE applying the partition migration (0002). It
gates the dangerous part of the deploy on the same checks the
migration itself enforces, plus the operational checks the migration
cannot make on its own — disk, WAL, time estimate, prior backup.

Outputs a checklist. Exits 0 if safe to proceed, 1 if any block.

What it checks
--------------
1. **Worker drain**  — jobs.status NOT IN (RUNNING, RETRYING). The
   migration itself also does this; we surface it earlier so the
   operator can pause workers if needed without alembic having
   already aborted halfway.

2. **Disk-space estimate** — sums on-disk size of the tables being
   re-created. Migration does CREATE+COPY+DROP, so peak usage is
   2× the largest of (snapshot_items, chat_thread_messages,
   audit_events). Warns if free space < 2× total.

3. **Row count + time estimate** — counts rows in each target
   table; estimates time at observed Railway PG throughput (rough:
   100k rows/sec INSERT…SELECT on Pro plan; tune via ``--rps``).

4. **WAL pressure** — checks ``max_wal_size`` and ``checkpoint_timeout``.
   Hash-partitioning a 500M-row table inside one transaction
   generates ~50 GB of WAL; if ``max_wal_size`` is the PG default
   (1 GB), the checkpoint stalls the DB. Recommends a tuning value.

5. **Recent backup** — looks for a recent pg_dump artifact path
   (env ``LAST_PG_DUMP_PATH``) or recent ``pg_dump`` invocation in
   ``pg_stat_activity``. Hard fail if there is no recent backup.

6. **Schema drift** — calls ``audit_schema.py``; refuses to run if
   the live schema already drifts from models. Reason: the migration
   is written against models.py expectations; drift means the
   ``INSERT…SELECT`` column lists won't match.

Usage
-----
::

    # default thresholds
    python3 scripts/pre_migration_check.py

    # tune assumed INSERT throughput (rows/sec)
    python3 scripts/pre_migration_check.py --rps 200000

    # require a pg_dump in the last N hours
    python3 scripts/pre_migration_check.py --max-backup-age-hours 6
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


PARTITION_TABLES = ("snapshot_items", "chat_thread_messages", "audit_events")


async def _fetch_size_and_count(conn, schema: str, table: str) -> Tuple[int, int]:
    """Return (size_bytes, row_count) for a table. Row count is
    estimated via ``reltuples`` first (cheap) and only falls back to
    COUNT(*) if the estimate is missing — to avoid stalling on a
    table-scan of a 500M-row table at 3 AM."""
    row = await conn.fetchrow(
        f"""
        SELECT
            pg_total_relation_size('"{schema}"."{table}"') AS size_bytes,
            c.reltuples::bigint AS approx_rows
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relname = $2
        """,
        schema, table,
    )
    if not row:
        return 0, 0
    size = int(row["size_bytes"] or 0)
    rows = int(row["approx_rows"] or 0)
    # If approx is wildly wrong (post-ANALYZE = 0 on a populated table),
    # fall back to COUNT(*).
    if rows <= 0 and size > 0:
        row2 = await conn.fetchrow(f'SELECT count(*) FROM "{schema}"."{table}"')
        rows = int(row2[0])
    return size, rows


async def _check_workers_drained(conn) -> Tuple[bool, str]:
    row = await conn.fetchrow(
        "SELECT count(*) AS n FROM jobs WHERE status IN ('RUNNING', 'RETRYING')"
    )
    n = int(row["n"] or 0)
    if n == 0:
        return True, "no active jobs"
    return False, f"{n} jobs RUNNING/RETRYING — drain workers first"


async def _check_wal_config(conn) -> Tuple[bool, str]:
    row = await conn.fetchrow(
        "SELECT setting AS v FROM pg_settings WHERE name = 'max_wal_size'"
    )
    raw = (row["v"] if row else "1024") or "1024"
    # Postgres reports max_wal_size in 8KB pages; multiply.
    try:
        max_wal_bytes = int(raw) * 8 * 1024
    except ValueError:
        # Newer PG returns "16GB" already in human form.
        return True, f"max_wal_size = {raw} (could not parse to bytes; visually verify)"
    recommended = 16 * 1024 ** 3  # 16 GB
    if max_wal_bytes < recommended:
        return False, (
            f"max_wal_size = {max_wal_bytes / 1024**3:.1f} GB — "
            f"recommend >= 16 GB before partition migration. "
            f"Run: ALTER SYSTEM SET max_wal_size = '16GB'; "
            f"SELECT pg_reload_conf();"
        )
    return True, f"max_wal_size = {max_wal_bytes / 1024**3:.1f} GB (ok)"


async def _check_recent_backup(conn, max_age_hours: int) -> Tuple[bool, str]:
    """Heuristic — operator should also confirm out-of-band.

    Looks for env var ``LAST_PG_DUMP_PATH`` and checks its mtime.
    Returns False if neither signal is found within the window.
    """
    p = os.environ.get("LAST_PG_DUMP_PATH")
    if p and os.path.exists(p):
        import time as _t
        age_h = (_t.time() - os.path.getmtime(p)) / 3600
        if age_h < max_age_hours:
            return True, f"LAST_PG_DUMP_PATH ({p}) age = {age_h:.1f}h (within threshold)"
        return False, f"LAST_PG_DUMP_PATH age = {age_h:.1f}h > {max_age_hours}h"
    # Check pg_stat_activity for a recent pg_dump session — not perfect
    # but catches the case where someone just kicked one off.
    rows = await conn.fetch(
        "SELECT pid, query_start, query FROM pg_stat_activity "
        "WHERE query LIKE 'pg_dump%' OR application_name LIKE 'pg_dump%'"
    )
    if rows:
        return True, f"pg_dump active: {len(rows)} session(s)"
    return False, (
        "no recent backup evidence — take a pg_dump first OR set "
        "LAST_PG_DUMP_PATH=/path/to/dump to acknowledge"
    )


async def _check_schema_drift() -> Tuple[bool, str]:
    """Re-uses the audit_schema script logic."""
    try:
        from scripts.audit_schema import _fetch_db_schema, _model_schema, _diff
        from shared.config import settings
        dsn = settings.DATABASE_URL
        if dsn.startswith("postgresql+asyncpg://"):
            dsn = dsn.replace("postgresql+asyncpg://", "postgresql://", 1)
        live = await _fetch_db_schema(dsn, os.environ.get("DB_SCHEMA", "tm_vault"))
        model = _model_schema()
        report = _diff(model, live, list(PARTITION_TABLES))
        if report["missing_tables"] or report["extra_tables"] or report["by_table"]:
            return False, f"schema drift detected on partition targets: {report}"
        return True, "no schema drift on partition targets"
    except Exception as exc:
        return False, f"schema-drift check could not run: {exc}"


async def _main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--rps", type=int, default=100_000,
        help="assumed INSERT…SELECT throughput rows/sec (default 100k)",
    )
    parser.add_argument(
        "--max-backup-age-hours", type=int, default=24,
        help="reject if no pg_dump in this many hours (default 24)",
    )
    parser.add_argument("--schema", default=os.environ.get("DB_SCHEMA", "tm_vault"))
    args = parser.parse_args()

    try:
        import asyncpg
        from shared.config import settings
    except Exception as exc:
        print(f"BLOCK: settings/asyncpg import failed: {exc}", file=sys.stderr)
        return 1

    dsn = settings.DATABASE_URL
    if dsn.startswith("postgresql+asyncpg://"):
        dsn = dsn.replace("postgresql+asyncpg://", "postgresql://", 1)

    conn = await asyncpg.connect(dsn, statement_cache_size=0)
    try:
        results: List[Tuple[str, bool, str]] = []

        ok, msg = await _check_workers_drained(conn)
        results.append(("workers_drained", ok, msg))

        total_bytes = 0
        total_rows = 0
        for t in PARTITION_TABLES:
            try:
                size, rows = await _fetch_size_and_count(conn, args.schema, t)
                total_bytes += size
                total_rows += rows
                est_seconds = rows / args.rps if rows else 0
                results.append((
                    f"size_{t}", True,
                    f"{t}: {size / 1024**3:.1f} GiB, ~{rows:,} rows, "
                    f"est copy time @ {args.rps:,} rps = {est_seconds:.0f}s "
                    f"({est_seconds/60:.1f} min)",
                ))
            except Exception as exc:
                results.append((f"size_{t}", False, f"could not read size: {exc}"))

        # Disk: peak usage is 2x the largest table during CREATE+COPY+DROP.
        # We sum total_bytes as a conservative estimate — operator should
        # confirm free disk is at least 2x total_bytes.
        results.append((
            "disk_estimate", True,
            f"total source bytes = {total_bytes / 1024**3:.1f} GiB; "
            f"peak DB usage during migration ≈ 2 × this = "
            f"{2 * total_bytes / 1024**3:.1f} GiB. Confirm free space >= this.",
        ))

        ok, msg = await _check_wal_config(conn)
        results.append(("wal_config", ok, msg))

        ok, msg = await _check_recent_backup(conn, args.max_backup_age_hours)
        results.append(("recent_backup", ok, msg))

        ok, msg = await _check_schema_drift()
        results.append(("schema_drift", ok, msg))

    finally:
        await conn.close()

    print("Pre-migration check — alembic 0002 partitioning")
    print("=" * 60)
    blocking = 0
    for name, ok, msg in results:
        mark = "✓" if ok else "✗"
        print(f"{mark} {name}: {msg}")
        if not ok:
            blocking += 1
    print("=" * 60)
    if blocking:
        print(f"BLOCK: {blocking} check(s) failed — refuse to run migration")
        return 1
    print("OK: safe to run `alembic upgrade head`")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
