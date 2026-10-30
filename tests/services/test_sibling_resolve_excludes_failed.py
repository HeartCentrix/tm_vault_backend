"""Regression: sibling-snapshot unions must exclude FAILED snapshots.

Bug (2026-07-14): jlucas's two reaped 348GB force-fulls left ~199k
blobless coordinator metadata rows on FAILED snapshots. Both
``_resolve_sibling_snapshot_ids`` (Recovery browse/folders) and the
content-snapshots per-tab rollup unioned snapshots of EVERY status, so
those dead rows rendered as recoverable "metadata only" items and the
OneDrive tab badge read 198,945 instead of the real recoverable count.

Pins (source inspection — the queries live inside async endpoint
bodies; importing the module for a fake-db behavioral test would need
a multi-call session fake for little extra proof):
  * the sibling-resolve WHERE excludes SnapshotStatus.FAILED;
  * the explicitly-passed snapshot stays viewable (post-mortem deep
    links into a failed run still resolve to at least itself);
  * the content-snapshots sib_ids query carries the same exclusion.
"""
from __future__ import annotations

import importlib.util
import inspect
import os
import pathlib
import sys

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "services" / "snapshot-service" / "main.py"
_spec = importlib.util.spec_from_file_location("ss_main_failed_excl_under_test", _MAIN_PATH)
_ss = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["ss_main_failed_excl_under_test"] = _ss
assert _spec and _spec.loader
_spec.loader.exec_module(_ss)  # type: ignore[union-attr]


def test_sibling_resolve_excludes_failed_snapshots():
    src = inspect.getsource(_ss._resolve_sibling_snapshot_ids)
    # FAILED (blobless leftovers) AND PENDING_DELETION (mid-teardown sweep
    # claim; blobs being deleted right now) must both stay out of unions.
    assert "SnapshotStatus.FAILED" in src and "SnapshotStatus.PENDING_DELETION" in src
    assert ".notin_(" in src


def test_sibling_resolve_keeps_requested_snapshot_viewable():
    src = inspect.getsource(_ss._resolve_sibling_snapshot_ids)
    assert "snap_uuid not in sibling_ids" in src


def test_content_snapshots_rollup_excludes_failed_snapshots():
    src = inspect.getsource(_ss.get_content_snapshots)
    # the sib_ids query feeding _rollup_latest_items must carry the filter
    assert "SnapshotStatus.FAILED" in src and "SnapshotStatus.PENDING_DELETION" in src
    assert ".notin_(" in src
