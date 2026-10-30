"""Retention must ALWAYS keep the most-recent FULL (baseline) snapshot.

Regression tests for the 2026-07-30 data-loss defect: retention selected
snapshots by calendar recency only, so a resource's FULL baseline aged out
of the GFS daily window (~14 days) and was hard-deleted while incrementals
kept advancing the deltaLink — stranding OneDrive drives on tiny
incrementals (jrue OneDrive: 124GB / 8,109 files -> 63 files).

Every keep-selector (_gfs_keep_ids, _flat_keep_ids, _archived_keep_ids)
must include the newest FULL snapshot's id in its keep set, no matter how
far outside the recency window it has fallen. This is a keep-MORE rule:
it can only preserve additional snapshots, never delete more.

Uses lightweight stand-in objects with the same attribute shape as the
SQLAlchemy ORM rows the production code reads (same pattern as
test_retention_archived_branch.py) — fast, no DB dependency.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from shared.models import SnapshotType
from shared.retention_cleanup import (
    _archived_keep_ids,
    _flat_keep_ids,
    _gfs_keep_ids,
)


def _snap(id_str: str, days_ago: float, snap_type=SnapshotType.INCREMENTAL,
          started=True):
    """Stand-in Snapshot ORM object — only the fields the keep-selectors
    actually read. `started=False` leaves started_at as None so the
    created_at fallback path is exercised."""
    ts = datetime.utcnow() - timedelta(days=days_ago)
    return SimpleNamespace(
        id=id_str,
        type=snap_type,
        started_at=ts if started else None,
        created_at=ts,
    )


def _gfs_policy(daily=14, weekly=0, monthly=0, yearly=0):
    return SimpleNamespace(
        retention_mode="GFS",
        gfs_daily_count=daily,
        gfs_weekly_count=weekly,
        gfs_monthly_count=monthly,
        gfs_yearly_count=yearly,
    )


def _flat_policy(days=14):
    return SimpleNamespace(
        retention_mode="FLAT",
        retention_days=days,
        retention_hot_days=7,
        retention_cool_days=30,
        retention_archive_days=None,
    )


def _archived_policy(mode: str, days=None):
    return SimpleNamespace(
        archived_retention_mode=mode,
        archived_retention_days=days,
    )


def _aged_out_chain():
    """The defect scenario: one FULL baseline far outside the daily window
    (60 days ago) + a run of recent daily INCREMENTALs. Recency-only
    selection keeps only incrementals and hard-deletes the baseline."""
    full = _snap("full-baseline", 60, SnapshotType.FULL)
    incrementals = [
        _snap(f"incr-{d}", d, SnapshotType.INCREMENTAL) for d in range(0, 30)
    ]
    return full, incrementals


# ── GFS ─────────────────────────────────────────────────────────────────

def test_gfs_keeps_aged_out_full_baseline():
    full, incrementals = _aged_out_chain()
    keep = _gfs_keep_ids([full] + incrementals, _gfs_policy(daily=14))
    assert full.id in keep, (
        "GFS retention dropped the FULL baseline — this is the "
        "OneDrive 124GB->63-files data-loss bug"
    )


def test_gfs_recency_alone_would_have_dropped_the_baseline():
    """Prove the baseline is kept by the baseline rule, NOT by recency:
    every other kept snapshot is a newer INCREMENTAL, and the FULL sits
    far outside the 14-day daily window."""
    full, incrementals = _aged_out_chain()
    keep = _gfs_keep_ids([full] + incrementals, _gfs_policy(daily=14))
    cutoff = datetime.utcnow() - timedelta(days=14)
    assert full.started_at < cutoff  # baseline is outside the window
    others = keep - {full.id}
    assert others  # recency did keep recent snapshots
    assert all(oid.startswith("incr-") for oid in others), (
        "recency-only selection picked only incrementals — without the "
        "baseline rule the FULL would have been deleted"
    )


def test_gfs_keeps_only_most_recent_full():
    """Two FULLs: only the newest anchors the live chain; the older one
    may age out normally."""
    old_full = _snap("full-old", 90, SnapshotType.FULL)
    new_full = _snap("full-new", 60, SnapshotType.FULL)
    incrementals = [_snap(f"incr-{d}", d) for d in range(0, 10)]
    keep = _gfs_keep_ids([old_full, new_full] + incrementals,
                         _gfs_policy(daily=5))
    assert new_full.id in keep
    assert old_full.id not in keep


def test_gfs_no_full_exists_does_not_crash():
    incrementals = [_snap(f"incr-{d}", d) for d in range(0, 5)]
    keep = _gfs_keep_ids(incrementals, _gfs_policy(daily=3))
    assert "incr-0" in keep  # newest always kept, behavior unchanged


# ── FLAT ────────────────────────────────────────────────────────────────

def test_flat_keeps_aged_out_full_baseline():
    full, incrementals = _aged_out_chain()
    keep = _flat_keep_ids([full] + incrementals, _flat_policy(days=14))
    assert full.id in keep


def test_flat_recency_alone_would_have_dropped_the_baseline():
    full, incrementals = _aged_out_chain()
    keep = _flat_keep_ids([full] + incrementals, _flat_policy(days=14))
    cutoff = datetime.utcnow() - timedelta(days=14)
    assert full.started_at < cutoff
    assert all(oid.startswith("incr-") for oid in keep - {full.id})


def test_flat_no_full_exists_does_not_crash():
    incrementals = [_snap(f"incr-{d}", d) for d in range(0, 5)]
    keep = _flat_keep_ids(incrementals, _flat_policy(days=3))
    assert "incr-0" in keep


# ── ARCHIVED ────────────────────────────────────────────────────────────

def test_archived_custom_keeps_aged_out_full_baseline():
    full, incrementals = _aged_out_chain()
    keep = _archived_keep_ids([full] + incrementals,
                              _archived_policy("CUSTOM", days=14))
    assert full.id in keep


def test_archived_keep_last_also_keeps_full_baseline():
    full, incrementals = _aged_out_chain()
    keep = _archived_keep_ids([full] + incrementals,
                              _archived_policy("KEEP_LAST"))
    # Most-recent snapshot AND the baseline that anchors it.
    assert keep == {"incr-0", full.id}


def test_archived_keep_all_still_keeps_everything():
    full, incrementals = _aged_out_chain()
    snaps = [full] + incrementals
    keep = _archived_keep_ids(snaps, _archived_policy("KEEP_ALL"))
    assert keep == {s.id for s in snaps}


def test_archived_same_sentinel_is_preserved():
    """SAME must still return the None sentinel so the caller falls
    through to FLAT/GFS (which apply the baseline rule themselves)."""
    full, incrementals = _aged_out_chain()
    assert _archived_keep_ids([full] + incrementals,
                              _archived_policy("SAME")) is None


# ── Type robustness ─────────────────────────────────────────────────────

def test_full_type_as_plain_string_is_recognized():
    """Snapshot.type may surface as a plain string instead of the
    SnapshotType enum depending on how the row was loaded."""
    full = _snap("full-str", 60, "FULL")
    incrementals = [_snap(f"incr-{d}", d, "INCREMENTAL") for d in range(0, 20)]
    keep = _gfs_keep_ids([full] + incrementals, _gfs_policy(daily=7))
    assert full.id in keep


def test_full_with_null_started_at_uses_created_at():
    full = _snap("full-nostart", 60, SnapshotType.FULL, started=False)
    incrementals = [_snap(f"incr-{d}", d) for d in range(0, 20)]
    keep = _flat_keep_ids([full] + incrementals, _flat_policy(days=14))
    assert full.id in keep


def test_manual_and_preemptive_types_are_not_baselines():
    """Only FULL anchors the incremental chain — MANUAL/PREEMPTIVE
    snapshots outside the window are not force-kept."""
    manual = _snap("manual-old", 60, SnapshotType.MANUAL)
    incrementals = [_snap(f"incr-{d}", d) for d in range(0, 20)]
    keep = _gfs_keep_ids([manual] + incrementals, _gfs_policy(daily=7))
    assert manual.id not in keep
