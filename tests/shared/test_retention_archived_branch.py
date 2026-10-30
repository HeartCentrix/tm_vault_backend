"""Tests for the archived-resource branch in retention_cleanup.

Covers the four archived_retention_mode variants: SAME / KEEP_ALL /
KEEP_LAST / CUSTOM. Uses lightweight stand-in objects with the same
attribute shape as the SQLAlchemy ORM rows the production code reads —
keeps tests fast and avoids a DB dependency.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from shared.retention_cleanup import (
    _archived_keep_ids,
    _is_archived,
)


def _snap(id_str: str, days_ago: int):
    """Stand-in Snapshot ORM object — only the fields _archived_keep_ids
    actually reads."""
    return SimpleNamespace(
        id=id_str,
        started_at=datetime.utcnow() - timedelta(days=days_ago),
        created_at=datetime.utcnow() - timedelta(days=days_ago),
    )


def _policy(mode: str, days=None):
    return SimpleNamespace(
        archived_retention_mode=mode,
        archived_retention_days=days,
    )


def _data_snap(id_str: str, days_ago: int, *, item_count: int = 400,
               bytes_total: int = 4_000_000, snap_type: str = "INCREMENTAL"):
    """Stand-in Snapshot ORM object carrying the counters the anchor rule
    reads. `type` is INCREMENTAL because production types EVERY snapshot
    INCREMENTAL — there are zero FULL rows, which is precisely why a
    FULL-only anchor test is a no-op there."""
    ts = datetime.utcnow() - timedelta(days=days_ago)
    return SimpleNamespace(
        id=id_str,
        started_at=ts,
        created_at=ts,
        status="COMPLETED",
        type=snap_type,
        item_count=item_count,
        bytes_total=bytes_total,
        extra_data={},
        reuse_of_snapshot_id=None,
        reuse_chain_root_id=None,
        lease_expires_at=None,
        hc_drain_status="NOT_APPLICABLE",
    )


# ── KEEP_ALL ────────────────────────────────────────────────────────────

def test_keep_all_returns_every_snapshot():
    snaps = [_snap("a", 1), _snap("b", 30), _snap("c", 365)]
    keep = _archived_keep_ids(snaps, _policy("KEEP_ALL"))
    assert keep == {"a", "b", "c"}


def test_keep_all_with_no_snapshots():
    assert _archived_keep_ids([], _policy("KEEP_ALL")) == set()


# ── KEEP_LAST ───────────────────────────────────────────────────────────

def test_keep_last_keeps_the_most_recent_plus_the_chain_anchor():
    """KEEP_LAST means "keep the latest backup" — that intent is intact:
    "newest" is kept and "middle" is pruned.

    It also keeps the anchor the latest backup resolves against, because
    every snapshot in production is an INCREMENTAL. These stand-ins record
    no counters, so all three broadened anchors collapse onto the oldest
    data-bearing row ("oldest"); with counters present it would be the
    largest-by-item_count row instead. Keeping the chain root does not
    violate "keep the last" — deleting it would make the last backup
    unrestorable, which is the 2026-07-30 incident."""
    snaps = [_snap("oldest", 100), _snap("middle", 50), _snap("newest", 1)]
    keep = _archived_keep_ids(snaps, _policy("KEEP_LAST"))
    assert keep == {"newest", "oldest"}
    assert "middle" not in keep


def test_keep_last_with_single_snapshot():
    snaps = [_snap("only", 5)]
    assert _archived_keep_ids(snaps, _policy("KEEP_LAST")) == {"only"}


# ── CUSTOM ──────────────────────────────────────────────────────────────

def test_custom_keeps_within_window_plus_latest_and_the_chain_anchor():
    """CUSTOM's window still governs: "very-old" (365d) is outside a 30-day
    window and is pruned. What survives besides the in-window row is the
    chain anchor ("ancient", the oldest data-bearing row here) — an
    in-window incremental with no baseline is not a restore point."""
    snaps = [
        _snap("very-old", 365),
        _snap("recent", 5),
        _snap("ancient", 1000),
    ]
    keep = _archived_keep_ids(snaps, _policy("CUSTOM", days=30))
    assert keep == {"recent", "ancient"}
    assert "very-old" not in keep


def test_custom_with_null_days_is_unlimited():
    snaps = [_snap("any", 999)]
    assert _archived_keep_ids(snaps, _policy("CUSTOM", days=None)) == {"any"}


def test_custom_always_keeps_latest_even_if_outside_window():
    snaps = [_snap("ancient", 5000)]
    keep = _archived_keep_ids(snaps, _policy("CUSTOM", days=7))
    # Latest safety net guarantees the resource never goes empty.
    assert keep == {"ancient"}


# ── ARCHIVED + the de-facto baseline (2026-07-31) ───────────────────────
#
# An ARCHIVED resource no longer exists in M365, so the vault holds the
# LAST remaining copy of that data — losing the chain root there is worse
# than anywhere else. `archived_retention_mode` says how much history to
# keep; it was never a statement that an unrestorable delta is acceptable.
# Production types every snapshot INCREMENTAL, so the FULL-typed anchor
# these modes used to union in is a provable no-op and the real baseline
# (largest / oldest data-bearing) was deletable.

def test_keep_last_keeps_the_de_facto_baseline_when_nothing_is_typed_full():
    """Modelled on jlucas: a 198,992-item / 199 GB baseline behind a tiny
    older stub, with nothing typed FULL. Offboarding the user flips the
    resource ARCHIVED; KEEP_LAST must not leave the operator holding
    incrementals whose chain root has been hard-deleted."""
    snaps = [
        _data_snap("stub", 120, item_count=2, bytes_total=31_000_000),
        _data_snap("baseline", 90, item_count=198_992, bytes_total=199_000_000_000),
        *[_data_snap(f"incr-{d}", d) for d in range(5)],
    ]
    keep = _archived_keep_ids(snaps, _policy("KEEP_LAST"))
    assert "baseline" in keep, (
        "KEEP_LAST hard-deletes the de-facto baseline of an ARCHIVED "
        "resource — the vault is the last copy and the kept incrementals "
        "are unrestorable without it"
    )
    assert "incr-0" in keep, "KEEP_LAST must still keep the latest backup"


def test_custom_window_keeps_the_de_facto_baseline_too():
    """Same hole on the CUSTOM branch: the baseline sits outside
    archived_retention_days."""
    snaps = [
        _data_snap("baseline", 200, item_count=8_239, bytes_total=124_000_000_000),
        *[_data_snap(f"incr-{d}", d) for d in range(3)],
    ]
    keep = _archived_keep_ids(snaps, _policy("CUSTOM", days=30))
    assert "baseline" in keep
    assert "incr-0" in keep


def test_keep_last_still_prunes_everything_but_the_latest_and_the_anchor():
    """Guard against over-correcting KEEP_LAST into KEEP_ALL: a mid-history
    row that is neither the latest nor a plausible chain root is still
    pruned."""
    snaps = [
        _data_snap("baseline", 90, item_count=200_000, bytes_total=199_000_000_000),
        _data_snap("mid", 45, item_count=500),
        _data_snap("latest", 1, item_count=700),
    ]
    keep = _archived_keep_ids(snaps, _policy("KEEP_LAST"))
    assert keep == {"latest", "baseline"}, (
        "KEEP_LAST should keep the last backup plus the anchor it resolves "
        f"against, nothing else — got {sorted(keep)}"
    )


def test_archived_anchor_never_lands_on_a_zero_item_placeholder():
    """The anchor rule stays data-aware on the archived path: a
    SKIP_CONCURRENT placeholder and an empty FAILED row can win nothing."""
    skip = _data_snap("skip-oldest", 200, item_count=0, bytes_total=0)
    skip.extra_data = {"skipped_concurrent": True}
    failed = _data_snap("failed-empty", 190, item_count=0, bytes_total=0)
    failed.status = "FAILED"
    snaps = [skip, failed,
             _data_snap("real-oldest", 150, item_count=9_000),
             _data_snap("latest", 1, item_count=300)]
    keep = _archived_keep_ids(snaps, _policy("KEEP_LAST"))
    assert "skip-oldest" not in keep and "failed-empty" not in keep
    assert "real-oldest" in keep and "latest" in keep


# ── SAME ────────────────────────────────────────────────────────────────

def test_same_returns_sentinel_none():
    """SAME means 'caller falls through to FLAT/GFS'. The function
    returns None as a sentinel — caller branches on it."""
    snaps = [_snap("a", 1)]
    keep = _archived_keep_ids(snaps, _policy("SAME"))
    assert keep is None


# ── _is_archived helper ─────────────────────────────────────────────────

def test_is_archived_with_string_status():
    res = SimpleNamespace(status="ARCHIVED")
    assert _is_archived(res) is True


def test_is_archived_with_enum_value():
    # Mimic SAEnum behavior — has .value attribute.
    enum_like = SimpleNamespace(value="ARCHIVED")
    res = SimpleNamespace(status=enum_like)
    assert _is_archived(res) is True


def test_is_archived_returns_false_for_other_statuses():
    for s in ("ACTIVE", "DISCOVERED", "INACCESSIBLE"):
        res = SimpleNamespace(status=s)
        assert _is_archived(res) is False, f"expected False for {s}"
