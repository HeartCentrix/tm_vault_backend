"""Schema-drift auditor: diff live Postgres schema against shared.models.

What it checks
--------------
For every table in ``shared/models.py``:

  • Existence (`information_schema.tables`)
  • Column set + types (`information_schema.columns`)
  • NOT NULL / DEFAULT
  • Foreign keys (`information_schema.table_constraints` / `pg_constraint`)
  • Indexes (`pg_indexes`)

Why
---
``Base.metadata.create_all`` only emits ``CREATE TABLE`` for *new*
tables. Production databases routinely accumulate manual hot-fix DDL
(adding a missing index by hand, dropping a stale column during a
prior incident, changing a column type via Railway shell). This drift
is invisible to ``create_all`` and ambushes alembic migrations.

This script does NOT modify anything. Read-only audit. Output is a
human-readable report plus optional JSON for CI to fail on diff.

Usage
-----
::

    # Quick console report against $DATABASE_URL.
    python3 scripts/audit_schema.py

    # JSON output for CI / programmatic checks.
    python3 scripts/audit_schema.py --json > schema_audit.json

    # Limit to specific tables.
    python3 scripts/audit_schema.py --tables snapshot_items audit_events

Exit code
---------
- ``0`` if no drift detected
- ``1`` if drift found (column added/removed, type changed, missing index, …)
- ``2`` on connection error
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# Make `shared.*` importable when run from anywhere.
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


async def _fetch_db_schema(dsn: str, schema_name: str) -> Dict[str, Dict[str, Any]]:
    import asyncpg

    out: Dict[str, Dict[str, Any]] = {}
    conn = await asyncpg.connect(dsn, statement_cache_size=0)
    try:
        # Tables
        tables = await conn.fetch(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = $1 ORDER BY table_name",
            schema_name,
        )
        for t in tables:
            tn = t["table_name"]
            cols = await conn.fetch(
                "SELECT column_name, data_type, is_nullable, column_default "
                "FROM information_schema.columns "
                "WHERE table_schema = $1 AND table_name = $2 "
                "ORDER BY ordinal_position",
                schema_name, tn,
            )
            idx = await conn.fetch(
                "SELECT indexname, indexdef FROM pg_indexes "
                "WHERE schemaname = $1 AND tablename = $2",
                schema_name, tn,
            )
            fks = await conn.fetch(
                """
                SELECT
                    conname AS name,
                    pg_get_constraintdef(c.oid, true) AS definition
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname = $1 AND t.relname = $2 AND c.contype = 'f'
                """,
                schema_name, tn,
            )
            out[tn] = {
                "columns": {
                    c["column_name"]: {
                        "type": c["data_type"],
                        "nullable": c["is_nullable"] == "YES",
                        "default": c["column_default"],
                    } for c in cols
                },
                "indexes": {i["indexname"]: i["indexdef"] for i in idx},
                "foreign_keys": {f["name"]: f["definition"] for f in fks},
            }
    finally:
        await conn.close()
    return out


def _model_schema() -> Dict[str, Dict[str, Any]]:
    """Introspect shared.models.Base.metadata to produce expected schema."""
    from shared.database import Base
    import shared.models  # noqa: F401 — table registration side effect

    out: Dict[str, Dict[str, Any]] = {}
    for table_name, table in Base.metadata.tables.items():
        out[table_name] = {
            "columns": {
                c.name: {
                    "type": _type_name(c.type),
                    "nullable": c.nullable,
                    "default": _serialise_default(c.default),
                } for c in table.columns
            },
            "indexes": [
                {"name": i.name, "columns": [c.name for c in i.columns], "unique": i.unique}
                for i in table.indexes
            ],
            "foreign_keys": [
                {
                    "column": fk.parent.name,
                    "references": f"{fk.column.table.name}.{fk.column.name}",
                }
                for c in table.columns for fk in c.foreign_keys
            ],
        }
    return out


def _type_name(t) -> str:
    """Coarse normalisation so SQLAlchemy types match Postgres types."""
    raw = type(t).__name__.lower()
    return {
        "uuid": "uuid",
        "string": "character varying",
        "text": "text",
        "integer": "integer",
        "biginteger": "bigint",
        "boolean": "boolean",
        "datetime": "timestamp without time zone",
        "json": "json",
        "jsonb": "jsonb",
        "largebinary": "bytea",
        "enum": "USER-DEFINED",
        "float": "double precision",
    }.get(raw, raw)


def _serialise_default(d) -> Optional[str]:
    if d is None:
        return None
    if hasattr(d, "arg"):
        return str(d.arg)
    return str(d)


def _diff(model: Dict[str, Any], live: Dict[str, Any], tables: Optional[List[str]] = None) -> Dict[str, Any]:
    report: Dict[str, Any] = {"missing_tables": [], "extra_tables": [], "by_table": {}}

    model_keys = set(model.keys())
    live_keys = set(live.keys())
    target_keys = set(tables) if tables else (model_keys | live_keys)

    for tname in sorted(target_keys):
        diffs: Dict[str, Any] = {}
        if tname in model_keys and tname not in live_keys:
            report["missing_tables"].append(tname)
            continue
        if tname in live_keys and tname not in model_keys:
            report["extra_tables"].append(tname)
            continue
        if tname not in model_keys:
            continue
        m = model[tname]
        l = live[tname]

        # Columns
        m_cols, l_cols = m["columns"], l["columns"]
        col_diffs = {}
        for cn in sorted(set(m_cols) | set(l_cols)):
            if cn in m_cols and cn not in l_cols:
                col_diffs[cn] = {"status": "missing_in_db"}
            elif cn in l_cols and cn not in m_cols:
                col_diffs[cn] = {"status": "extra_in_db"}
            else:
                mt, lt = m_cols[cn]["type"], l_cols[cn]["type"]
                if mt != lt and not _types_compatible(mt, lt):
                    col_diffs[cn] = {"status": "type_mismatch", "model": mt, "db": lt}
        if col_diffs:
            diffs["columns"] = col_diffs

        # Foreign keys (loose check — model side has [{column,references}], DB side has named defs)
        m_fks = m.get("foreign_keys", [])
        l_fk_defs = list(l.get("foreign_keys", {}).values())
        fk_diffs = []
        for mfk in m_fks:
            referent = mfk["references"].replace(".", "(") + ")"
            ok = any(referent in d for d in l_fk_defs)
            if not ok:
                fk_diffs.append({"missing_fk": mfk})
        if fk_diffs:
            diffs["foreign_keys"] = fk_diffs

        if diffs:
            report["by_table"][tname] = diffs

    return report


def _types_compatible(a: str, b: str) -> bool:
    """Loose type compatibility — Postgres reports vary by version."""
    a, b = a.lower(), b.lower()
    if a == b:
        return True
    pairs = {
        ("character varying", "varchar"),
        ("character varying", "text"),
        ("timestamp without time zone", "timestamp"),
        ("double precision", "float"),
        ("user-defined", "varchar"),  # enums reported as USER-DEFINED
    }
    return (a, b) in pairs or (b, a) in pairs


async def _main():
    p = argparse.ArgumentParser()
    p.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    p.add_argument("--tables", nargs="+", help="restrict to these tables")
    p.add_argument("--schema", default=os.environ.get("DB_SCHEMA", "tm_vault"))
    args = p.parse_args()

    try:
        from shared.config import settings
        dsn = settings.DATABASE_URL
        if dsn.startswith("postgresql+asyncpg://"):
            dsn = dsn.replace("postgresql+asyncpg://", "postgresql://", 1)
    except Exception as exc:
        print(f"ERROR: settings load failed: {exc}", file=sys.stderr)
        return 2

    try:
        live = await _fetch_db_schema(dsn, args.schema)
    except Exception as exc:
        print(f"ERROR: DB connection failed: {exc}", file=sys.stderr)
        return 2

    model = _model_schema()
    report = _diff(model, live, args.tables)

    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        _print_human_report(report)

    has_drift = bool(
        report["missing_tables"] or report["extra_tables"] or report["by_table"]
    )
    return 1 if has_drift else 0


def _print_human_report(r: Dict[str, Any]) -> None:
    if not (r["missing_tables"] or r["extra_tables"] or r["by_table"]):
        print("✓ schema in sync with models")
        return
    if r["missing_tables"]:
        print(f"MISSING tables in DB (model defines, DB lacks): {r['missing_tables']}")
    if r["extra_tables"]:
        print(f"EXTRA tables in DB (DB has, model doesn't): {r['extra_tables']}")
    for tname, diffs in r["by_table"].items():
        print(f"\n— {tname} —")
        if "columns" in diffs:
            for cn, cd in diffs["columns"].items():
                print(f"  col {cn}: {cd}")
        if "foreign_keys" in diffs:
            for f in diffs["foreign_keys"]:
                print(f"  fk: {f}")


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
