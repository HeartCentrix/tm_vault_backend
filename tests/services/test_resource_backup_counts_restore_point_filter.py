"""Regression: resource-service backup counts must count REAL recovery points.

Bug (found 2026-08-04): every snapshot-counting query in
``services/resource-service/main.py`` filtered ``status = 'COMPLETED'`` in raw
SQL. Five sites, all feeding the Protection table's per-resource
``usage.backups`` count and the parent's ``last_backup`` timestamp:

  * ``list_resources``      — direct count, child roll-up
  * ``get_resources_by_type`` — direct count, child roll-up, child last-backup

Both halves of the filter were wrong:

1. PARTIAL was excluded. A PARTIAL snapshot lost some shards but still holds
   every item it *did* capture, so it is a real recovery point — in production
   the only backup of Jason Rue's 1,114,848-item / 152 GB Exchange Online
   Archive is PARTIAL, and the lossy-shard derivation deployed the same day
   makes PARTIAL the normal outcome for any SharePoint/OneDrive run with
   files_failed > 0. A resource whose recovery points are all PARTIAL reported
   ``backups: 0``, and a parent whose children only ever went PARTIAL reported
   ``last_backup: null`` — i.e. "never backed up" for an identity holding
   152 GB of restorable data.
2. ``skipped_concurrent`` placeholders were counted. backup-worker's cross-job
   concurrency guard (SKIP_CONCURRENT in workers/backup-worker/main.py) creates
   a snapshot row purely so a skipped job's FANOUT/FINALIZE counters balance,
   then marks it COMPLETED with item_count=0 in ~19 ms. Six exist in
   production. They inflated backup counts and — worse for the last-backup
   site — could date a "backup" from a run that captured nothing, making a
   stalled resource look freshly protected.

Widening these to PARTIAL is safe for health reporting because it is NOT a
health signal: ``last_backup_status`` is derived independently from the latest
BACKUP Job's status via the LATERAL join in both list queries, so the
clean/degraded distinction is carried elsewhere and cannot be blurred here.

Pins:
  * all five sites route through the single ``_REAL_RESTORE_POINT_SQL``
    constant and no bare ``status = 'COMPLETED'`` fragment survives, so they
    cannot drift apart again;
  * the constant is semantically identical to snapshot-service's
    ``_real_restore_point()`` — the two services must agree on what a recovery
    point is;
  * behaviourally, the fragment admits PARTIAL and pre-flag NULL extra_data
    and rejects placeholders and non-terminal states;
  * the two byte-sum queries still include PENDING_DELETION — deliberately
    different semantics (on-disk footprint, not recovery points).

The behavioural tests execute the fragment **verbatim** as the WHERE clause of
a query against in-memory SQLite, with ``json_extract_path_text`` registered
as a Python shim; COALESCE, IN and NOT IN (including NOT IN's NULL
propagation) behave identically in both engines, so only that one JSON
accessor is emulated. ``= ANY(:ids)`` is Postgres-only, so the surrounding
query shapes are pinned by source inspection instead, anchored on the query
variable names.
"""
from __future__ import annotations

import importlib.util
import inspect
import json
import os
import pathlib
import re
import sqlite3
import sys

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _load(mod_name: str, rel_path: str):
    path = _REPO_ROOT / rel_path
    spec = importlib.util.spec_from_file_location(mod_name, path)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[mod_name] = mod
    assert spec and spec.loader
    try:
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
    except Exception as exc:  # pragma: no cover - import-env guard
        pytest.skip(f"{rel_path} failed to import: {exc}", allow_module_level=True)
    return mod


_rs = _load("rs_main_backup_counts", "services/resource-service/main.py")
_ss = _load("ss_main_for_rs_drift", "services/snapshot-service/main.py")

_FRAGMENT = _rs._REAL_RESTORE_POINT_SQL


# ── the five sites all route through the one constant ─────────────────────

def _query_block(func, var_name: str) -> str:
    """Slice one named raw-SQL query out of a function's source.

    Anchored on the query's variable name so a rename fails the test loudly
    instead of silently making it assert nothing.
    """
    src = inspect.getsource(func)
    marker = f"{var_name} = text("
    assert marker in src, (
        f"{func.__name__} no longer defines {var_name} — retarget this test"
    )
    block = src.split(marker, 1)[1].split('""")', 1)[0]
    assert "FROM snapshots" in block or "snapshots s" in block, (
        f"{var_name} in {func.__name__} no longer reads snapshots — "
        "retarget this test"
    )
    return block


_SITES = [
    (_rs.list_resources, "counts_query", "per-resource backup count"),
    (_rs.list_resources, "child_backup_query", "child roll-up into parent count"),
    (_rs.get_resources_by_type, "counts_query", "per-resource backup count"),
    (_rs.get_resources_by_type, "child_backup_query",
     "child roll-up into parent count"),
    (_rs.get_resources_by_type, "child_last_query",
     "parent last-backup time derived from children"),
]


@pytest.mark.parametrize(
    "func,var_name,what",
    _SITES,
    ids=[f"{f.__name__}.{v}" for f, v, _ in _SITES],
)
def test_site_uses_the_shared_recovery_point_fragment(func, var_name, what):
    block = _query_block(func, var_name)
    assert "_REAL_RESTORE_POINT_SQL" in block, (
        f"{func.__name__}.{var_name} ({what}) must use the shared fragment — "
        "a COMPLETED-only filter here reports 0 backups for resources whose "
        "recovery points are all PARTIAL and counts 0-item placeholders"
    )
    assert "status = 'COMPLETED'" not in block, (
        f"a bare COMPLETED-only filter is back in "
        f"{func.__name__}.{var_name} ({what})"
    )
    # A non-f-string here would ship the literal "{_REAL_RESTORE_POINT_SQL}"
    # to Postgres.
    assert 'text(f"""' in inspect.getsource(func).split(
        f"{var_name} = ", 1)[1][:12], (
        f"{func.__name__}.{var_name} must be an f-string for the fragment to "
        "interpolate"
    )


def test_no_completed_only_snapshot_filter_survives_in_the_module():
    """Whole-module sweep: catches a new counting query added later that
    copies the old pattern instead of the shared fragment."""
    src = (_REPO_ROOT / "services" / "resource-service" / "main.py").read_text()
    offenders = [
        line.strip() for line in src.splitlines()
        if re.search(r"status\s*=\s*'COMPLETED'", line)
    ]
    assert not offenders, (
        "COMPLETED-only snapshot filtering reappeared: " + repr(offenders)
    )


def test_byte_sum_queries_keep_their_different_semantics():
    """The subtree size / delta queries intentionally include
    PENDING_DELETION: they measure bytes still on disk, not recovery points.
    Pinned so a future sweep doesn't 'unify' them onto the recovery-point
    fragment and silently under-report the storage footprint."""
    src = (_REPO_ROOT / "services" / "resource-service" / "main.py").read_text()
    footprint_filters = src.count(
        "s.status IN ('COMPLETED', 'PARTIAL', 'PENDING_DELETION')"
    )
    assert footprint_filters == 2, (
        "expected both subtree byte-sum queries (list_resources and "
        f"get_resources_by_type) to keep the footprint filter; found "
        f"{footprint_filters}"
    )


# ── the two services must agree on what a recovery point is ───────────────

def _conjuncts(sql: str) -> list:
    """Canonical form of a pure-AND predicate: the sorted list of its
    conjuncts, case- and whitespace-normalised, with the table prefix
    unified.

    Compared as conjuncts rather than as one string because both predicates
    are conjunctions with no OR, so grouping parentheses are semantically
    irrelevant — and SQLAlchemy's choice of where to emit them is a
    version detail, not a behaviour we want to pin.
    """
    # snapshot-service compiles with the real table name; resource-service's
    # raw SQL aliases it `s`.
    sql = re.sub(r"\s+", " ", sql.strip()).lower().replace("snapshots.", "s.")
    sql = _strip_outer_parens(sql)
    return sorted(_strip_outer_parens(p) for p in sql.split(" and "))


def _strip_outer_parens(sql: str) -> str:
    """Drop parentheses that wrap the whole expression, and only those."""
    sql = sql.strip()
    while sql.startswith("(") and sql.endswith(")"):
        depth = 0
        for i, ch in enumerate(sql):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    break
        if i != len(sql) - 1:  # the opening paren closes early: "(a) and (b)"
            break
        sql = sql[1:-1].strip()
    return sql


def test_fragment_matches_snapshot_services_predicate_exactly():
    from sqlalchemy.dialects import postgresql

    compiled = str(
        _ss._real_restore_point().compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    mine, theirs = _conjuncts(_FRAGMENT), _conjuncts(compiled)
    assert len(mine) == 2, (
        f"expected a status test AND a placeholder test; got {mine}"
    )
    assert mine == theirs, (
        "resource-service and snapshot-service disagree on the recovery-point "
        "predicate; Protection's backup count and Recovery's list would then "
        "describe different populations\n"
        f"  resource-service: {mine}\n"
        f"  snapshot-service: {theirs}"
    )


def test_fragment_uses_the_json_accessor_and_is_null_safe():
    assert "json_extract_path_text" in _FRAGMENT, (
        "extra_data is a json column; the json accessor is required"
    )
    assert "jsonb_extract_path_text" not in _FRAGMENT, (
        "jsonb_extract_path_text on a json column raises at runtime — "
        "snapshots.extra_data is Column(JSON), not JSONB"
    )
    assert "COALESCE" in _FRAGMENT.upper(), (
        "rows written before the flag hold NULL here; without COALESCE the "
        "NOT IN yields NULL and the row is filtered out, which would zero "
        "the backup count for every pre-flag snapshot"
    )
    assert "NOT IN" in _FRAGMENT.upper(), (
        "placeholder rejection must be a negative match so real snapshots "
        "(flag absent) still count"
    )
    assert _FRAGMENT.startswith("(") and _FRAGMENT.endswith(")"), (
        "the fragment is AND-ed into larger WHERE clauses — it must be "
        "self-parenthesised or OR-containing neighbours would rebind it"
    )


# ── behavioural: run the fragment verbatim ────────────────────────────────

def _pg_json_extract_path_text(doc, key):
    """Shim for Postgres json_extract_path_text(json, text): NULL doc or
    absent key -> NULL, JSON booleans render as 'true'/'false'."""
    if doc is None:
        return None
    try:
        parsed = json.loads(doc)
    except (TypeError, ValueError):  # pragma: no cover - malformed fixture
        return None
    if not isinstance(parsed, dict) or key not in parsed:
        return None
    value = parsed[key]
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return value
    return json.dumps(value)


def _admits(status: str, extra_data) -> bool:
    """True if the fragment keeps a snapshot with this status/extra_data."""
    conn = sqlite3.connect(":memory:")
    conn.create_function("json_extract_path_text", 2, _pg_json_extract_path_text)
    conn.execute("CREATE TABLE snapshots (status TEXT, extra_data TEXT)")
    conn.execute(
        "INSERT INTO snapshots VALUES (?, ?)",
        (status, None if extra_data is None else json.dumps(extra_data)),
    )
    kept = conn.execute(
        f"SELECT COUNT(*) FROM snapshots s WHERE {_FRAGMENT}"
    ).fetchone()[0]
    conn.close()
    return bool(kept)


def test_partial_counts_as_a_recovery_point():
    assert _admits("PARTIAL", None), (
        "a PARTIAL run holds every item it captured — the only backup of the "
        "1.1M-item archive is PARTIAL, and excluding it reported 0 backups"
    )
    assert _admits("PARTIAL", {"files_failed": 4})


def test_clean_completed_still_counts():
    assert _admits("COMPLETED", None), "pre-flag NULL extra_data must survive"
    assert _admits("COMPLETED", {})
    assert _admits("COMPLETED", {"skipped_concurrent": False})
    assert _admits("COMPLETED", {"some_other_key": "x"})


def test_skipped_concurrent_placeholder_is_rejected():
    assert not _admits("COMPLETED", {"skipped_concurrent": True}), (
        "a 0-item ~19ms concurrency placeholder is a fake success — counting "
        "it inflates backup counts and can date a 'last backup' from a run "
        "that captured nothing"
    )
    # The flag is written as a JSON boolean today; tolerate the text forms a
    # backfill or a different writer could produce.
    for value in ("true", "t", "1"):
        assert not _admits("COMPLETED", {"skipped_concurrent": value})
    assert not _admits("PARTIAL", {"skipped_concurrent": True})


def test_non_recovery_point_states_are_rejected():
    for status in ("IN_PROGRESS", "FAILED", "PENDING_DELETION"):
        assert not _admits(status, None), f"{status} is not a recovery point"
