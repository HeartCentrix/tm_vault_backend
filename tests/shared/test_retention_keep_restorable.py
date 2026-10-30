"""P3 — retention must ALWAYS keep >=1 RESTORABLE (COMPLETED) snapshot.

The pre-P3 selectors used "keep the newest snapshot by time" as the safety net.
That's unsafe: the newest snapshot can be FAILED / IN_PROGRESS / PARTIAL and NOT
restorable — so a resource whose latest run failed could have every COMPLETED
(restorable) snapshot pruned, leaving nothing to restore. P3 changes the safety
net to "newest COMPLETED", falling back to newest-by-time only when nothing is
restorable at all (so a resource is never silently emptied either way).

Pure stand-in objects (same attribute shape as the ORM rows the code reads) —
fast, no DB, mirrors tests/shared/test_retention_archived_branch.py.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace

from shared.retention_cleanup import (
    _latest_restorable_id,
    _flat_keep_ids,
    _gfs_keep_ids,
    _archived_keep_ids,
)


def _snap(id_str: str, days_ago: int, status: str = "COMPLETED"):
    ts = datetime.utcnow() - timedelta(days=days_ago)
    return SimpleNamespace(id=id_str, started_at=ts, created_at=ts, status=status)


# ── _latest_restorable_id ─────────────────────────────────────────────────
def test_latest_restorable_prefers_completed_over_newer_failed():
    snaps = [
        _snap("failed_today", 0, "FAILED"),
        _snap("completed_yesterday", 1, "COMPLETED"),
        _snap("completed_old", 10, "COMPLETED"),
    ]
    assert _latest_restorable_id(snaps) == "completed_yesterday"


def test_latest_restorable_ignores_in_progress_and_partial():
    snaps = [
        _snap("inprog", 0, "IN_PROGRESS"),
        _snap("partial", 1, "PARTIAL"),
        _snap("good", 2, "COMPLETED"),
    ]
    assert _latest_restorable_id(snaps) == "good"


def test_latest_restorable_falls_back_to_newest_when_none_completed():
    # nothing restorable exists — keep the newest by time so the resource is
    # not silently left with zero snapshots.
    snaps = [_snap("newest_failed", 0, "FAILED"), _snap("older_failed", 5, "FAILED")]
    assert _latest_restorable_id(snaps) == "newest_failed"


def test_latest_restorable_empty_is_none():
    assert _latest_restorable_id([]) is None


# ── the safety net is applied by the real selectors ───────────────────────
def test_flat_keeps_newest_completed_not_newer_failed():
    # retention_days=1 → only "today" is within window by time, but today FAILED.
    # The safety net must still retain the newest COMPLETED (yesterday).
    policy = SimpleNamespace(
        retention_days=1, retention_hot_days=0, retention_cool_days=0,
        retention_archive_days=0,
    )
    snaps = [
        _snap("failed_today", 0, "FAILED"),
        _snap("completed_yesterday", 1, "COMPLETED"),
        _snap("completed_week", 7, "COMPLETED"),
    ]
    keep = _flat_keep_ids(snaps, policy)
    assert "completed_yesterday" in keep  # restorable safety net survives


def test_archived_keep_last_keeps_newest_completed_not_newer_failed():
    policy = SimpleNamespace(archived_retention_mode="KEEP_LAST", archived_retention_days=None)
    snaps = [_snap("failed_today", 0, "FAILED"), _snap("completed_yesterday", 1, "COMPLETED")]
    assert "completed_yesterday" in _archived_keep_ids(snaps, policy)


def test_archived_custom_safety_net_is_restorable():
    # window=1 day excludes the 5-day-old COMPLETED by time; newest is a FAILED run.
    # The CUSTOM safety net must still retain the restorable (COMPLETED) snapshot.
    policy = SimpleNamespace(archived_retention_mode="CUSTOM", archived_retention_days=1)
    snaps = [_snap("failed_today", 0, "FAILED"), _snap("completed_old", 5, "COMPLETED")]
    keep = _archived_keep_ids(snaps, policy)
    assert "completed_old" in keep      # restorable safety net survives


def test_gfs_keeps_newest_completed_as_safety_net():
    # Only 1 daily bucket + no weekly/monthly/yearly. The daily bucket would keep
    # just the newest (a FAILED run); the ONLY thing that retains the restorable
    # 30-day-old COMPLETED snapshot is the P3 safety net.
    policy = SimpleNamespace(
        gfs_daily_count=1, gfs_weekly_count=0, gfs_monthly_count=0, gfs_yearly_count=0,
    )
    snaps = [
        _snap("failed_today", 0, "FAILED"),
        _snap("failed_recent", 1, "FAILED"),
        _snap("completed_old", 30, "COMPLETED"),
    ]
    keep = _gfs_keep_ids(snaps, policy)
    assert "completed_old" in keep          # restorable safety net survives
    assert "failed_recent" not in keep      # buckets did NOT keep everything
