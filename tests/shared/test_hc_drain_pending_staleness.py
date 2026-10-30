"""A lost hc_drain_status write must not pin a snapshot forever.

2026-08-11: the USER_CHATS handler stamps `hc_drain_status='PENDING'`
before it detaches the hostedContent finalizer, and only that finalizer's
one fire-and-forget UPDATE ever clears it. That UPDATE died on the same
dropped connection that cost CHATS/4 of snapshot 6807df97 its partition
row, its except arm only prints, and nothing re-derives the column — so
the snapshot is PENDING forever. Consequences, both verified in this
repo at the time:
  * shared/retention_cleanup.py skips it in BOTH guards (`_is_snapshot_live`
    and the `_delete_snapshots` SELECT ... hc_drain_status <> 'PENDING'),
    so its blobs are never reclaimed;
  * `snapshot_is_restore_ready` refuses the recovery point, and tells the
    operator to "retry later" — advice that can never come true.
One prod snapshot was found stuck this way during the 2026-08-11 review.

These tests pin the re-derivation: a PENDING older than the horizon is
abandoned, not in flight, and the honest value is FAILED (may be missing
inline images), never COMPLETE — the finalizer records its evidence in the
same statement that failed, so nothing in the row can prove the drain
finished.
"""
from __future__ import annotations

import os
import pathlib
import sys
from datetime import datetime, timedelta, timezone

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from shared.models import (  # noqa: E402
    HC_DRAIN_PENDING_STALE_AFTER_S,
    hc_drain_pending_is_stale,
    rederive_stale_hc_drain_status,
    snapshot_is_restore_ready,
)


class _Snap:
    def __init__(self, hc_drain_status="PENDING", age_hours=None,
                 completed_at="auto", started_at=None):
        self.hc_drain_status = hc_drain_status
        if completed_at == "auto":
            completed_at = (
                datetime.utcnow() - timedelta(hours=age_hours)
                if age_hours is not None else None
            )
        self.completed_at = completed_at
        self.started_at = started_at
        self.created_at = None


def test_a_fresh_pending_is_still_in_flight():
    snap = _Snap(age_hours=0.5)
    assert hc_drain_pending_is_stale(snap) is False
    assert rederive_stale_hc_drain_status(snap) is None
    ready, reason = snapshot_is_restore_ready(snap)
    assert ready is False
    assert "still in flight" in reason


def test_a_pending_older_than_the_horizon_is_abandoned():
    snap = _Snap(age_hours=(HC_DRAIN_PENDING_STALE_AFTER_S / 3600) + 1)
    assert hc_drain_pending_is_stale(snap) is True


def test_the_rederived_value_is_failed_not_complete():
    """COMPLETE would be a lie: whether every hostedContent item landed is
    unknowable from the row."""
    snap = _Snap(age_hours=48)
    assert rederive_stale_hc_drain_status(snap) == "FAILED"


def test_a_stale_pending_stops_telling_the_operator_to_retry_later():
    snap = _Snap(age_hours=48)
    ready, reason = snapshot_is_restore_ready(snap)
    assert ready is False
    assert "stale" in reason
    assert "retry later" not in reason
    assert "rederive_stale_hc_drain_status" in reason, (
        "the reason must name the recovery, or the operator has nothing to do"
    )


def test_other_statuses_are_never_touched():
    for status in ("NOT_APPLICABLE", "COMPLETE", "FAILED"):
        snap = _Snap(hc_drain_status=status, age_hours=100)
        assert hc_drain_pending_is_stale(snap) is False, status
        assert rederive_stale_hc_drain_status(snap) is None, status
    assert snapshot_is_restore_ready(_Snap("COMPLETE", age_hours=100))[0] is True
    assert snapshot_is_restore_ready(_Snap("NOT_APPLICABLE", age_hours=100))[0] is True


def test_a_row_with_no_timestamp_is_never_guessed_stale():
    snap = _Snap(completed_at=None)
    assert hc_drain_pending_is_stale(snap) is False


def test_started_at_is_the_fallback_when_the_snapshot_never_completed():
    snap = _Snap(completed_at=None,
                 started_at=datetime.utcnow() - timedelta(hours=72))
    assert hc_drain_pending_is_stale(snap) is True


def test_timezone_aware_timestamps_do_not_raise():
    """lease/dr columns come back tz-aware; started_at/completed_at are
    naive. A mixed comparison must not blow up in the restore path."""
    snap = _Snap(completed_at=datetime.now(timezone.utc) - timedelta(hours=72))
    assert hc_drain_pending_is_stale(snap) is True
    snap_fresh = _Snap(completed_at=datetime.now(timezone.utc))
    assert hc_drain_pending_is_stale(snap_fresh) is False


def test_horizon_is_overridable_for_a_sweep():
    snap = _Snap(age_hours=2)
    assert hc_drain_pending_is_stale(snap, stale_after_s=3600) is True
    assert hc_drain_pending_is_stale(snap, stale_after_s=24 * 3600) is False


def test_now_can_be_injected():
    snap = _Snap(age_hours=100)
    assert hc_drain_pending_is_stale(
        snap, now=snap.completed_at + timedelta(minutes=5),
    ) is False
