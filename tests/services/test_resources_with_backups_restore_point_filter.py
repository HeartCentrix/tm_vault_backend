"""Regression: GET /resources/with-backups must count REAL recovery points.

Bug (found 2026-08-04): the stats query in
``services/snapshot-service/main.py::list_resources_with_backups`` filtered
``Snapshot.status == SnapshotStatus.COMPLETED``. The endpoint feeds the
Recovery resource list, and a resource only appears at all when its rolled-up
``snapshot_count > 0``, so the filter produced two opposite lies:

1. PARTIAL was excluded, so a resource whose backups are ALL PARTIAL was
   dropped from the Recovery list entirely — indistinguishable from a
   resource that had never been backed up, with no UI path to restore it. A
   PARTIAL snapshot lost some shards but still holds every item it *did*
   capture: in production the only backup of Jason Rue's 1,114,848-item /
   152 GB Exchange Online Archive is PARTIAL, and the lossy-shard derivation
   deployed the same day makes PARTIAL the normal outcome for any
   SharePoint/OneDrive run with files_failed > 0.
2. ``skipped_concurrent`` placeholders were counted, so a resource whose only
   "backup" is a 0-item placeholder was listed as protected with nothing
   behind it. backup-worker's cross-job concurrency guard (SKIP_CONCURRENT in
   workers/backup-worker/main.py) creates a snapshot row purely so a skipped
   job's FANOUT/FINALIZE counters balance, then marks it COMPLETED with
   item_count=0 in ~19 ms. Six exist in production.

The fix routes the query through the shared ``_real_restore_point()``
predicate that ``get_content_snapshots`` already uses.

Pins:
  * the stats query — the one that decides list membership and the
    snapshot_count / total_items shown per row — uses the shared predicate,
    and no bare status comparison survives anywhere in the endpoint;
  * behaviourally, running the compiled predicate over rows modelling the
    exact production population keeps the all-PARTIAL resource in the result
    (with the right count) and drops the placeholder-only resource.

The behavioural half executes the *compiled Postgres SQL* of the predicate
against an in-memory SQLite database with ``json_extract_path_text``
registered as a Python shim (COALESCE, IN and NOT IN — including NOT IN's
NULL propagation — are identical in both engines). That runs the real
predicate text rather than a re-implementation of it; only the one JSON
accessor is emulated.
"""
from __future__ import annotations

import importlib.util
import inspect
import json
import os
import pathlib
import sqlite3
import sys

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "services" / "snapshot-service" / "main.py"
_spec = importlib.util.spec_from_file_location("ss_main_with_backups", _MAIN_PATH)
_ss = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["ss_main_with_backups"] = _ss
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_ss)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import-env guard
    pytest.skip(
        f"snapshot-service module failed to import: {exc}",
        allow_module_level=True,
    )


def _compiled_predicate() -> str:
    """Render _real_restore_point() as literal Postgres SQL."""
    from sqlalchemy.dialects import postgresql

    return str(
        _ss._real_restore_point().compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )


def _stats_query_source() -> str:
    """Just the stats query, anchored on strings unique to it.

    If the SELECT list is ever rewritten these anchors fail loudly rather
    than letting the test quietly assert nothing.
    """
    src = inspect.getsource(_ss.list_resources_with_backups)
    assert "stats_stmt = (" in src, "stats query was renamed — retarget this test"
    assert 'label("snapshot_count")' in src, (
        "snapshot_count is what gates Recovery list membership — anchor lost"
    )
    assert 'label("total_items")' in src, "total_items anchor lost"
    body = src.split("stats_stmt = (", 1)[1]
    return body.split(".group_by(Snapshot.resource_id)", 1)[0]


def test_stats_query_uses_the_shared_recovery_point_predicate():
    assert "_real_restore_point()" in _stats_query_source(), (
        "the query that decides which resources appear in Recovery must use "
        "_real_restore_point(); a COMPLETED-only filter hides every resource "
        "whose backups are all PARTIAL and admits 0-item placeholders"
    )


def test_endpoint_has_no_bare_status_comparison_left():
    """Any surviving direct status comparison in this endpoint reintroduces
    the bug for whichever value it feeds."""
    src = inspect.getsource(_ss.list_resources_with_backups)
    assert "Snapshot.status ==" not in src, (
        "a bare status equality filter is back in list_resources_with_backups"
    )
    assert "SnapshotStatus.COMPLETED" not in src, (
        "COMPLETED-only filtering is back in list_resources_with_backups"
    )


# ── behavioural: run the real predicate text over a modelled population ──

_A = "aaaaaaaa-0000-0000-0000-000000000001"  # only PARTIAL snapshots
_B = "bbbbbbbb-0000-0000-0000-000000000002"  # only a skipped_concurrent row
_C = "cccccccc-0000-0000-0000-000000000003"  # one clean COMPLETED snapshot
_D = "dddddddd-0000-0000-0000-000000000004"  # nothing terminal-with-data


def _pg_json_extract_path_text(doc, key):
    """Shim for Postgres json_extract_path_text(json, text).

    NULL doc or absent key -> NULL; JSON booleans render as 'true'/'false',
    matching Postgres' text rendering of the extracted value.
    """
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


def _rollup_with_predicate() -> dict:
    """Run the endpoint's stats shape (count + sum, grouped by resource)
    with the compiled predicate as the WHERE clause."""
    conn = sqlite3.connect(":memory:")
    conn.create_function("json_extract_path_text", 2, _pg_json_extract_path_text)
    conn.execute(
        "CREATE TABLE snapshots (resource_id TEXT, status TEXT, "
        "item_count INTEGER, extra_data TEXT)"
    )
    rows = [
        # Resource A — the production archive case: real data, PARTIAL only.
        (_A, "PARTIAL", 1_114_848, None),
        (_A, "PARTIAL", 12, json.dumps({"files_failed": 3})),
        # Resource B — a fake success and nothing else.
        (_B, "COMPLETED", 0, json.dumps({
            "skipped_concurrent": True,
            "concurrent_job_id": "x",
        })),
        # Resource C — clean run; extra_data predates the flag (NULL).
        (_C, "COMPLETED", 7, None),
        # Resource C — and a placeholder alongside it, which must not add.
        (_C, "COMPLETED", 0, json.dumps({"skipped_concurrent": True})),
        # Resource D — never produced a recovery point.
        (_D, "IN_PROGRESS", 0, None),
        (_D, "FAILED", 0, None),
        (_D, "PENDING_DELETION", 5, None),
    ]
    conn.executemany("INSERT INTO snapshots VALUES (?, ?, ?, ?)", rows)
    sql = (
        "SELECT resource_id, COUNT(*), COALESCE(SUM(item_count), 0) "
        f"FROM snapshots WHERE {_compiled_predicate()} GROUP BY resource_id"
    )
    out = {r[0]: {"snapshot_count": r[1], "total_items": r[2]}
           for r in conn.execute(sql).fetchall()}
    conn.close()
    return out


def test_all_partial_resource_is_listed_with_every_partial_counted():
    stats = _rollup_with_predicate()
    assert _A in stats, (
        "a resource whose backups are all PARTIAL was dropped from Recovery "
        "entirely — it holds real restorable items"
    )
    assert stats[_A]["snapshot_count"] == 2
    assert stats[_A]["total_items"] == 1_114_860


def test_placeholder_only_resource_is_not_listed_as_protected():
    stats = _rollup_with_predicate()
    assert _B not in stats, (
        "a 0-item skipped_concurrent placeholder is a fake success — it must "
        "not put a resource on the Recovery list with nothing behind it"
    )


def test_placeholder_does_not_inflate_a_real_resource_count():
    """Resource C has one genuine COMPLETED snapshot plus a placeholder; the
    Recovery row must say 1 recovery point, not 2. Its real row also carries
    NULL extra_data, so this doubles as the COALESCE / pre-flag check."""
    stats = _rollup_with_predicate()
    assert stats[_C]["snapshot_count"] == 1
    assert stats[_C]["total_items"] == 7


def test_non_recovery_point_states_never_make_a_resource_protected():
    """IN_PROGRESS has no complete item set, FAILED leftovers are not
    restorable, and PENDING_DELETION blobs are being torn down right now."""
    stats = _rollup_with_predicate()
    assert _D not in stats
