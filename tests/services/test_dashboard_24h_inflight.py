"""Regression: the 24-hour card must not read Success while jobs are stuck.

TMV-022 (QA sweep 2026-08-11). "Two BACKUP jobs have been In Progress since
Aug 11, 3:07 AM. The 24-hour card shows Success with 2 success / 0 failures
and does not count them."

Cause: `_roll_up_group_outcome` returns None for any batch group holding a
RUNNING/QUEUED child ("in-flight — not an outcome yet"), and
`get_24hour_status` tallied only success / warnings / failures. An in-flight
batch was therefore counted NOWHERE — not as a failure, not as a warning,
not even as a total — so the headline read Success.

Reproduced live before the fix (prod tenant 163c487b, 2026-08-11 21:28 UTC):

    GET /api/v1/dashboard/status/24hour
        → {"success": 2, "warnings": 0, "failures": 0}
    jobs table, same window
        → 26 COMPLETED + 2 RUNNING, both RUNNING rows in batch
          2123e84f-2765-4766-8857-498542f57f8e, created 18:07:01 UTC

The response now also carries `inProgress` / `stalled` so the frontend can
stop calling that Success.

On the threshold: `jobs.updated_at` is NOT a heartbeat — job ee9bac4f ran
13.6 h to a clean COMPLETED with updated_at last touched 4 minutes in — so
staleness is measured from the group's earliest `created_at`. Over 30 days
of terminal BACKUP jobs on prod (n=832) p50 = 327 s, p90 = 4,846 s,
p99 = 60,651 s, longest clean COMPLETED = 54.6 h; the 24 h default sits
above p99.
"""
from __future__ import annotations

import importlib.util
import inspect
import pathlib
import sys
from datetime import datetime, timedelta

import pytest

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "services" / "dashboard-service" / "main.py"
_spec = importlib.util.spec_from_file_location("dashboard_main_inflight", _MAIN_PATH)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["dashboard_main_inflight"] = _mod
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_mod)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import-env guard
    pytest.skip(f"dashboard-service failed to import: {exc}",
                allow_module_level=True)

from shared.models import JobStatus  # noqa: E402

_state = _mod._group_inflight_state
_NOW = datetime(2026, 8, 11, 21, 28, 0)


def _children(*statuses):
    return [(s, {}) for s in statuses]


# ── the stuck batch is visible at all ────────────────────────────────────


def test_running_batch_is_reported_in_flight():
    """The exact prod shape: batch 2123e84f, two RUNNING children."""
    started = _NOW - timedelta(hours=3, minutes=21)
    assert _state(
        _children(JobStatus.RUNNING, JobStatus.RUNNING), started, _NOW,
    ) == "inflight"


def test_queued_batch_is_reported_in_flight():
    assert _state(
        _children(JobStatus.QUEUED), _NOW - timedelta(minutes=5), _NOW,
    ) == "inflight"


def test_long_running_batch_is_reported_stalled():
    started = _NOW - timedelta(seconds=_mod._STALL_AFTER_SECONDS + 1)
    assert _state(_children(JobStatus.RUNNING), started, _NOW) == "stalled"


def test_a_batch_just_under_the_threshold_is_not_stalled_yet():
    started = _NOW - timedelta(seconds=_mod._STALL_AFTER_SECONDS - 60)
    assert _state(_children(JobStatus.RUNNING), started, _NOW) == "inflight"


def test_threshold_sits_above_the_measured_p99():
    """p99 of terminal BACKUP duration over 30 days on prod was 60,651 s.

    A default under that would flag healthy nightly runs as stalled, which
    is how a staleness signal gets ignored.
    """
    assert _mod._STALL_AFTER_SECONDS >= 60651


def test_missing_started_at_still_counts_as_in_flight():
    """A NULL created_at must not make a running batch disappear again."""
    assert _state(_children(JobStatus.RUNNING), None, _NOW) == "inflight"


# ── it does not steal groups from the terminal buckets ───────────────────


def test_a_failed_batch_is_not_double_counted_as_in_flight():
    """FAILED already wins in _roll_up_group_outcome; counting it here too
    would inflate the card."""
    assert _state(
        _children(JobStatus.FAILED, JobStatus.RUNNING),
        _NOW - timedelta(days=3), _NOW,
    ) is None


def test_a_finished_batch_is_not_in_flight():
    assert _state(
        _children(JobStatus.COMPLETED, JobStatus.COMPLETED),
        _NOW - timedelta(hours=1), _NOW,
    ) is None


def test_a_cancelled_batch_stays_uncounted():
    assert _state(
        _children(JobStatus.CANCELLED), _NOW - timedelta(days=5), _NOW,
    ) is None


# ── the endpoint actually reports it ─────────────────────────────────────


def test_endpoint_returns_the_inflight_counters():
    src = inspect.getsource(_mod.get_24hour_status)
    for key in ('"inProgress"', '"stalled"', '"oldestInProgressStartedAt"'):
        assert key in src, (
            f"GET /api/v1/dashboard/status/24hour must return {key} — "
            "without it the frontend has no way to stop rendering Success "
            "while a batch is hung"
        )
    assert "_group_inflight_state(" in src


def test_terminal_buckets_are_unchanged():
    """success / warnings / failures keep their existing meaning: the fix is
    additive, so the 7-day chart and every current consumer are untouched."""
    src = inspect.getsource(_mod.get_24hour_status)
    assert '"success": success' in src
    assert '"warnings": warnings' in src
    assert '"failures": failures' in src
    # The 7-day endpoint must NOT have grown an in-flight bucket — its
    # daily_tally dict only has the three terminal keys.
    seven = inspect.getsource(_mod.get_7day_status)
    assert "_group_inflight_state" not in seven


def test_roll_up_still_declines_to_bucket_in_flight_groups():
    """The in-flight count is layered on top of _roll_up_group_outcome, not
    baked into it — the 7-day chart shares that helper."""
    assert _mod._roll_up_group_outcome(
        _children(JobStatus.RUNNING, JobStatus.COMPLETED)
    ) is None
