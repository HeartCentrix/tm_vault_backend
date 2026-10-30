"""Regression pins for the 2026-07-14 jlucas force-full #3 post-mortem.

Two worker defects let a 348GB force-full 'complete' with 0 items:

1. The _fetch_for blanket handler degraded ANY enumeration exception to
   ``return []`` — a Graph 401 on an expired /root/delta page token
   finalized as COMPLETED-empty (which would also promote a stashed
   pending deltaLink and strand the baseline).
2. Loops serving a dead run never noticed: a swept run's shard consumer
   kept resolving 49,494 download URLs for 16.8 hours under
   429 retry-after-300 storms, starving the single app registration.

Source-inspection pins (worker main.py is a 24k-line module with import
side effects; the SQL/flow shapes here mirror the scheduler sweep tests).
"""
from __future__ import annotations

import pathlib

_MAIN = (
    pathlib.Path(__file__).resolve().parents[2]
    / "workers" / "backup-worker" / "main.py"
).read_text()


def test_enumeration_failure_raises_not_empty_list():
    idx = _MAIN.find("Graph fetch failed for user")
    assert idx != -1
    blk = _MAIN[idx: idx + 400]
    assert "raise EnumerationFailed" in blk, (
        "enumeration exceptions must surface, not degrade to 'no items'"
    )


def test_backup_one_marks_snapshot_failed_on_enumeration_error():
    """Settlement moved into `_record_enumeration_failure` (2026-07-31) so a
    PARTITION SHARD can fail its own row without flipping the SHARED parent
    out of create_snapshot's dedup set. The property pinned here is
    unchanged: a whole-resource enumeration failure ends FAILED with a
    failure_reason, never COMPLETED-empty. Behavioural coverage lives in
    tests/workers/test_shard_failure_accounting.py.
    """
    idx = _MAIN.find("except EnumerationFailed as enum_exc:")
    assert idx != -1
    blk = _MAIN[idx: idx + 2600]
    assert "_record_enumeration_failure(" in blk
    # honest return shape for the gather sums
    assert '"failed": True' in blk

    ridx = _MAIN.find("async def _record_enumeration_failure(")
    assert ridx != -1
    rec = _MAIN[ridx: ridx + 5000]
    # The terminal status is decided by reconcile_enumeration_failure_status
    # rather than an inline `SnapshotStatus.FAILED` assignment (2026-08-05):
    # the old inline write was fenced on status == IN_PROGRESS and silently
    # no-opped when the inline finalize won the race, leaving COMPLETED-empty
    # snapshots while the log claimed FAILED. The property pinned here is
    # unchanged — behaviour lives in
    # tests/workers/test_enumeration_failure_reconcile.py.
    assert "reconcile_enumeration_failure_status(" in rec
    assert "enumeration_failure_extra_data(" in rec
    assert "failure_reason" in _MAIN[
        _MAIN.find("def enumeration_failure_extra_data("):
    ][:1500]


def test_prefetch_aborts_when_snapshot_terminal():
    idx = _MAIN.find("url pre-fetch")
    assert idx != -1
    region = _MAIN[_MAIN.find("async def _useronedrive_backup_files"):]
    assert "_snapshot_still_live(snapshot.id)" in region
    assert "raise RunAbandoned" in region
    # the non-fatal prefetch fallback must NOT swallow the abort
    assert "except RunAbandoned:" in region


def test_download_loop_periodic_liveness_check():
    idx = _MAIN.find("async def _process_one(f: Dict[str, Any]):")
    assert idx != -1
    blk = _MAIN[idx - 1200: idx + 1600]
    assert "_live_check" in blk
    assert "% 200 == 0" in blk
    assert "RunAbandoned" in blk


def test_liveness_helper_fails_open_on_db_errors():
    idx = _MAIN.find("async def _snapshot_still_live")
    assert idx != -1
    blk = _MAIN[idx: idx + 1400]
    assert "return True" in blk  # DB hiccup must not kill live work
    assert "SnapshotStatus.IN_PROGRESS" in blk
