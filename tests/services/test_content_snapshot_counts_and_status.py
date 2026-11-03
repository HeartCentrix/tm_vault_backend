"""Regression: tab badge, item list, and snapshot status tell one story.

Two findings from the QA sweep of 2026-08-11, both on
`GET /api/v1/resources/{id}/content-snapshots` and the content list
endpoints it points at.

TMV-009 — "Calendar tab badge (5,416) disagrees with sidebar total (5,418)."
Real, and the badge was the correct number. `_CONTENT_ROLLUP_SQL` (which
computes the badge) has always carried
``AND COALESCE(si.is_deleted, false) = false``; `/calendar` called
`_paged_latest_items` WITHOUT ``exclude_deleted``, so Graph @removed
tombstones were listed as recoverable events. Measured on prod for the test
resource's USER_CALENDAR child over its anchor snapshot's 23 siblings:

    dedup incl. deleted = 5,418     dedup excl. deleted = 5,416
    is_deleted rows     = 2         both named "(deleted event)", 232 B

`/onedrive` already passed ``exclude_deleted=True``; calendar now matches.

TMV-010 — "Chats badge (719,826) disagrees with snapshot itemCount
(598,807); PARTIAL snapshot status is never surfaced." The counts half is
NOT a bug: they are different metrics and both are right. `itemCount` is
the cross-sibling newest-wins rollup (distinct messages recoverable as of
this snapshot, over 18 siblings); `snapshots.item_count` is what that one
delta run captured. Verified on prod: rollup 719,826, anchor item_count
598,807, zero deleted rows — the 121k gap is delta overlap, not loss.

The real half is that the anchor snapshot's status is PARTIAL and the
payload had no field to say so, so Recovery rendered a knowingly-incomplete
backup as complete. `_real_restore_point()` deliberately ADMITS PARTIAL, so
surfacing the status is mandatory, not optional.
"""
from __future__ import annotations

import importlib.util
import inspect
import pathlib
import sys

import pytest

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "services" / "snapshot-service" / "main.py"
_spec = importlib.util.spec_from_file_location("snap_main_counts", _MAIN_PATH)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["snap_main_counts"] = _mod
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_mod)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import-env guard
    pytest.skip(f"snapshot-service failed to import: {exc}",
                allow_module_level=True)


# ── TMV-009: badge and list count the same rows ──────────────────────────


def test_calendar_list_excludes_deleted_tombstones():
    src = inspect.getsource(_mod.list_snapshot_calendar)
    assert "exclude_deleted=True" in src, (
        "the calendar list must drop is_deleted rows — the tab badge "
        "(_CONTENT_ROLLUP_SQL) always has, and the 2-event gap the QA sweep "
        "saw was exactly the 2 tombstones under that calendar"
    )


def test_the_badge_rollup_still_excludes_deleted():
    """The other side of the same equality — if this ever stops excluding
    them, calendar must stop too or the numbers diverge again."""
    assert "COALESCE(si.is_deleted, false) = false" in _mod._CONTENT_ROLLUP_SQL


def test_every_content_list_that_feeds_a_badge_agrees_on_deleted():
    """Tombstones are not restorable content anywhere. Any list endpoint
    whose count is mirrored by a tab badge has to exclude them."""
    for func in (_mod.list_snapshot_calendar, _mod.list_snapshot_onedrive):
        assert "exclude_deleted=True" in inspect.getsource(func), (
            f"{func.__name__} lists deletion tombstones as recoverable items"
        )


# ── TMV-010: the snapshot's status reaches the UI ────────────────────────


def test_content_snapshots_exposes_the_anchor_status():
    src = inspect.getsource(_mod.get_content_snapshots)
    assert '"status": _snap_status' in src, (
        "each byContent tab must carry its anchor snapshot's status — a "
        "PARTIAL chats snapshot rendered as complete is a silently "
        "incomplete backup"
    )
    assert '"isPartial"' in src


def test_content_snapshots_reports_both_counters_separately():
    """The rollup and the snapshot's own count are different metrics; ship
    both so they are never mistaken for a contradiction again."""
    src = inspect.getsource(_mod.get_content_snapshots)
    assert '"itemCount": agg_count' in src
    assert '"snapshotItemCount"' in src


def test_partial_is_still_a_real_restore_point():
    """The status is surfaced, NOT used to hide the snapshot: a PARTIAL run
    holds everything it captured and is jrue's only archive baseline."""
    from shared.models import SnapshotStatus

    assert SnapshotStatus.PARTIAL in _mod._READABLE_SNAPSHOT_STATES
    assert SnapshotStatus.COMPLETED in _mod._READABLE_SNAPSHOT_STATES


def test_ispartial_is_derived_from_completed_not_from_a_status_list():
    """`isPartial` must be true for FAILED-ish anchors too, not just the
    literal PARTIAL enum — anything that is not a clean COMPLETED warrants
    the banner."""
    src = inspect.getsource(_mod.get_content_snapshots)
    assert '_snap_status != SnapshotStatus.COMPLETED.value' in src
