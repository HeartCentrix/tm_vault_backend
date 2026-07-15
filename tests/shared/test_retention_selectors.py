"""Core selection math for retention — the keep-set logic itself (not just the P3
safety net): _gfs_keep_ids bucket counts, _flat_keep_ids window + tiered fallback,
and _is_on_hold (legal-hold / WORM skip). Pure functions, deterministic dates.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace

from shared.retention_cleanup import _gfs_keep_ids, _flat_keep_ids, _is_on_hold


def _snap(id_str, dt, status="COMPLETED"):
    return SimpleNamespace(id=id_str, started_at=dt, created_at=dt,
                           status=SimpleNamespace(value=status))


def _gfs_policy(d=0, w=0, m=0, y=0):
    return SimpleNamespace(gfs_daily_count=d, gfs_weekly_count=w,
                           gfs_monthly_count=m, gfs_yearly_count=y)


# ── GFS daily bucket: keep newest snapshot per calendar day, up to N days ──
def test_gfs_daily_keeps_newest_per_day_up_to_count():
    snaps = [
        _snap("a", datetime(2026, 3, 10, 9)),   # day 03-10 (newest of that day)
        _snap("b", datetime(2026, 3, 10, 8)),   # day 03-10 (older same day)
        _snap("c", datetime(2026, 3, 9, 12)),    # day 03-09
        _snap("d", datetime(2026, 1, 5, 12)),    # day 01-05 (3rd distinct day)
    ]
    keep = _gfs_keep_ids(snaps, _gfs_policy(d=2))   # only 2 daily buckets
    assert "a" in keep          # newest of newest day
    assert "c" in keep          # newest of 2nd day
    assert "b" not in keep      # older snapshot of an already-counted day
    assert "d" not in keep      # 3rd day exceeds daily=2


def test_gfs_weekly_keeps_newest_per_iso_week():
    snaps = [
        _snap("w12b", datetime(2026, 3, 18)),   # ISO week 12 (newer)
        _snap("w12a", datetime(2026, 3, 16)),   # ISO week 12 (older, same week)
        _snap("w11", datetime(2026, 3, 9)),      # ISO week 11
        _snap("w10", datetime(2026, 3, 2)),      # ISO week 10
    ]
    keep = _gfs_keep_ids(snaps, _gfs_policy(w=2))
    assert "w12b" in keep       # newest of newest week
    assert "w11" in keep        # newest of 2nd week
    assert "w12a" not in keep   # older same-week snapshot
    assert "w10" not in keep    # 3rd week exceeds weekly=2


def test_gfs_monthly_keeps_newest_per_month():
    snaps = [
        _snap("m03b", datetime(2026, 3, 20)),   # month 03 (newer)
        _snap("m03a", datetime(2026, 3, 15)),   # month 03 (older)
        _snap("m02", datetime(2026, 2, 10)),
        _snap("m01", datetime(2026, 1, 5)),
    ]
    keep = _gfs_keep_ids(snaps, _gfs_policy(m=2))
    assert "m03b" in keep
    assert "m02" in keep
    assert "m03a" not in keep   # older same-month snapshot
    assert "m01" not in keep    # 3rd month exceeds monthly=2


def test_gfs_yearly_keeps_first_per_year():
    snaps = [
        _snap("y26b", datetime(2026, 6, 1)),
        _snap("y26a", datetime(2026, 2, 1)),
        _snap("y25", datetime(2025, 8, 1)),
        _snap("y24", datetime(2024, 8, 1)),
    ]
    keep = _gfs_keep_ids(snaps, _gfs_policy(y=2))   # 2 yearly buckets
    assert "y26b" in keep       # newest of 2026
    assert "y25" in keep        # newest (only) of 2025
    assert "y24" not in keep    # 3rd year exceeds yearly=2
    assert "y26a" not in keep   # older 2026 snapshot not the yearly pick


# ── FLAT window: keep within retention_days, drop older ────────────────────
def _flat_policy(days=None, hot=0, cool=0, archive=0):
    return SimpleNamespace(retention_days=days, retention_hot_days=hot,
                           retention_cool_days=cool, retention_archive_days=archive)


def _rel(days_ago, status="COMPLETED"):
    ts = datetime.utcnow() - timedelta(days=days_ago)
    return _snap(f"d{days_ago}", ts, status)


def test_flat_keeps_within_window_drops_older():
    snaps = [_rel(2), _rel(10), _rel(20)]
    keep = _flat_keep_ids(snaps, _flat_policy(days=7))
    assert "d2" in keep          # within 7-day window
    assert "d10" not in keep     # older than window
    assert "d20" not in keep


def test_flat_tiered_fallback_sums_hot_cool_archive():
    # retention_days unset → keep_days = hot+cool+archive = 3+4+0 = 7
    snaps = [_rel(2), _rel(10)]
    keep = _flat_keep_ids(snaps, _flat_policy(days=None, hot=3, cool=4, archive=0))
    assert "d2" in keep
    assert "d10" not in keep


def test_flat_unlimited_archive_keeps_everything():
    # archive_days=None means "no delete rule" → keep all
    snaps = [_rel(2), _rel(400)]
    keep = _flat_keep_ids(snaps, _flat_policy(days=None, hot=3, cool=4, archive=None))
    assert keep == {"d2", "d400"}


# ── legal hold / immutability: never prune ─────────────────────────────────
def _hold_policy(legal=False, until=None, immut=None):
    return SimpleNamespace(legal_hold_enabled=legal, legal_hold_until=until,
                           immutability_mode=immut)


def test_on_hold_active_legal_hold():
    assert _is_on_hold(_hold_policy(legal=True, until=None)) is True                 # indefinite
    assert _is_on_hold(_hold_policy(legal=True, until=datetime.utcnow() + timedelta(days=1))) is True


def test_not_on_hold_when_legal_hold_expired():
    assert _is_on_hold(_hold_policy(legal=True, until=datetime.utcnow() - timedelta(days=1))) is False


def test_on_hold_when_immutability_locked():
    assert _is_on_hold(_hold_policy(immut="Locked")) is True
    assert _is_on_hold(_hold_policy(immut="locked")) is True     # case-insensitive


def test_not_on_hold_unlocked_or_none():
    assert _is_on_hold(_hold_policy(immut="Unlocked")) is False
    assert _is_on_hold(_hold_policy(immut=None)) is False
