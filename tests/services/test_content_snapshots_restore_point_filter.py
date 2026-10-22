"""Regression: /resources/{id}/content-snapshots must anchor on a REAL
recovery point — COMPLETED *or* PARTIAL, never a concurrency placeholder.

Bug (found 2026-08-04): all three snapshot queries in
``get_content_snapshots`` filtered ``status == COMPLETED``. Two distinct
failures followed.

1. PARTIAL was excluded. A PARTIAL snapshot lost some shards but still
   holds every item it *did* capture — in production the only backup of
   Jason Rue's 1,114,848-item / 152 GB Exchange Online Archive is PARTIAL,
   and the lossy-shard derivation added the same day makes PARTIAL the
   normal outcome for any SharePoint/OneDrive run with files_failed > 0.
   A child whose only good runs are PARTIAL rendered as a tab with no
   snapshot at all. Worse: because the sibling roll-up counts items with
   ``created_at <= anchor``, anchoring to an OLDER COMPLETED snapshot
   silently truncated everything the newest PARTIAL run added — Recovery
   showed stale contents with no indication anything was missing.

2. ``skipped_concurrent`` placeholders were counted. backup-worker's
   cross-job concurrency guard creates a snapshot row purely so a skipped
   job's FANOUT/FINALIZE counters balance, then immediately marks it
   COMPLETED with item_count=0 (~19 ms end to end). Six exist in
   production and Jason Rue's *newest* COMPLETED archive snapshot is one,
   so the newest-COMPLETED anchor resolved to a 0-item placeholder in
   preference to a 1.1M-item real backup.

Pins:
  * the predicate admits COMPLETED and PARTIAL;
  * the predicate rejects skipped_concurrent placeholders;
  * it reads extra_data with the *json* accessor (the column is json, not
    jsonb — jsonb_extract_path_text would raise at runtime) and is
    NULL-safe for rows predating the flag;
  * all three query sites in get_content_snapshots use it, so none can
    drift back to a bare COMPLETED filter.
"""
from __future__ import annotations

import importlib.util
import inspect
import os
import pathlib
import sys

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "services" / "snapshot-service" / "main.py"
_spec = importlib.util.spec_from_file_location("ss_main_restore_point", _MAIN_PATH)
_ss = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["ss_main_restore_point"] = _ss
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


def test_predicate_admits_both_completed_and_partial():
    sql = _compiled_predicate()
    assert "COMPLETED" in sql, "a clean run must remain a recovery point"
    assert "PARTIAL" in sql, (
        "PARTIAL snapshots hold real captured items — the only backup of "
        "the 1.1M-item archive is PARTIAL. Excluding it hides real data."
    )


def test_predicate_rejects_skipped_concurrent_placeholders():
    sql = _compiled_predicate()
    assert "skipped_concurrent" in sql, (
        "the 0-item ~19ms concurrency placeholder must never win the "
        "anchor or count as a version"
    )
    # Rejection must be expressed as a NOT-match so absent/false keeps the
    # row; an equality test would drop every real snapshot instead.
    assert "NOT IN" in sql.upper(), (
        "placeholder rejection must be a negative match so real snapshots "
        "(flag absent) still qualify"
    )


def test_predicate_uses_json_accessor_and_is_null_safe():
    sql = _compiled_predicate()
    assert "json_extract_path_text" in sql, (
        "extra_data is a json column; the json accessor is required"
    )
    assert "jsonb_extract_path_text" not in sql, (
        "jsonb_extract_path_text on a json column raises at runtime — "
        "Snapshot.extra_data is Column(JSON), not JSONB"
    )
    assert "coalesce" in sql.lower(), (
        "rows written before the flag existed have NULL here; without "
        "COALESCE the NOT IN yields NULL and the row is filtered out, "
        "which would hide every pre-flag snapshot"
    )


def test_predicate_does_not_admit_in_progress_or_pending_deletion():
    """A live run has no complete item set, and PENDING_DELETION rows are
    mid-teardown — their items and blobs are being deleted right now."""
    sql = _compiled_predicate()
    assert "IN_PROGRESS" not in sql, "an in-flight run is not a recovery point"
    assert "PENDING_DELETION" not in sql, (
        "PENDING_DELETION snapshots are being torn down by the sweep claim"
    )
    assert "FAILED" not in sql, "a failed run's leftover rows are not a recovery point"


def test_all_content_snapshot_queries_use_the_predicate():
    """The anchor, snapshotCount and versionCount queries must all route
    through the shared predicate — a bare COMPLETED filter surviving in
    any one of them reintroduces the bug for that field."""
    src = inspect.getsource(_ss.get_content_snapshots)
    assert src.count("_real_restore_point()") >= 3, (
        "expected the anchor + snapshotCount + versionCount queries to use "
        f"_real_restore_point(); found {src.count('_real_restore_point()')}"
    )
    assert "Snapshot.status == SnapshotStatus.COMPLETED" not in src, (
        "a bare COMPLETED filter is back in get_content_snapshots — it "
        "excludes PARTIAL recovery points and admits 0-item placeholders"
    )
