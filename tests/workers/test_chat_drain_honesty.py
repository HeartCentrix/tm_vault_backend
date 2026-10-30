"""USER_CHATS drain-failure honesty gate.

Bug (adversarial review, 2026-07-31): the USER_CHATS handler recorded
per-chat drain failures into ``chat_threads.drain_failure_state`` and then
let the snapshot finalize COMPLETED regardless. Three real paths produce a
snapshot that captured nothing while claiming success:

  * consent revoked / 403 on every chat,
  * a 429 storm exhausting CHAT_DRAIN_RETRIES on every chat,
  * the 12h ``asyncio.wait_for`` firing and cancelling the remaining
    chats mid-fanout.

The mailbox handler already refuses that finalize (see the
``totals["drain_failures"]`` gate). These tests pin the equivalent
decision for chats plus the terminal-status choice both finalize sites
make, so a backup that captured nothing can never report success again.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_chat_honesty", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_chat_honesty"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )


# ─── the all-drains-failed refusal (mailbox parity) ───────────────────────

def test_every_drain_failed_with_zero_items_refuses_completed():
    """Consent revoked mid-run: 12 chats planned, all 12 403'd, nothing
    persisted. Must be FAILED, never an empty COMPLETED."""
    status, reason = _bw.evaluate_drain_honesty(
        planned=12,
        drained_ok=0,
        failed=12,
        items_persisted=0,
    )
    assert status is _bw.SnapshotStatus.FAILED
    assert reason and "12" in reason


def test_throttle_storm_exhausting_retries_refuses_completed():
    """A 429 storm burns CHAT_DRAIN_RETRIES on every chat — same shape as
    the consent case, same refusal."""
    status, reason = _bw.evaluate_drain_honesty(
        planned=3,
        drained_ok=0,
        failed=3,
        items_persisted=0,
    )
    assert status is _bw.SnapshotStatus.FAILED
    assert reason


# ─── timeout truncation must report honestly ──────────────────────────────

def test_timeout_cancelled_remaining_chats_reports_partial():
    """The 12h wait_for fired after 40/200 chats: 160 chats never ran.
    Items were captured, so the run is PARTIAL — not COMPLETED."""
    status, reason = _bw.evaluate_drain_honesty(
        planned=200,
        drained_ok=40,
        failed=0,
        items_persisted=9_100,
    )
    assert status is _bw.SnapshotStatus.PARTIAL
    assert reason and "160" in reason


def test_timeout_with_zero_items_and_zero_successes_is_failed():
    """Timeout before any chat finished → nothing captured → FAILED."""
    status, reason = _bw.evaluate_drain_honesty(
        planned=200,
        drained_ok=0,
        failed=0,
        items_persisted=0,
    )
    assert status is _bw.SnapshotStatus.FAILED
    assert reason


def test_some_failures_with_items_is_partial():
    status, reason = _bw.evaluate_drain_honesty(
        planned=10,
        drained_ok=8,
        failed=2,
        items_persisted=500,
    )
    assert status is _bw.SnapshotStatus.PARTIAL
    assert reason and "2" in reason


def test_all_failed_but_reuse_persisted_items_is_partial_not_failed():
    """Cross-user reuse wrote pointer rows before the drains failed. The
    snapshot is not empty, so downgrade to PARTIAL rather than FAILED —
    keeping data beats discarding it."""
    status, reason = _bw.evaluate_drain_honesty(
        planned=4,
        drained_ok=0,
        failed=4,
        items_persisted=77,
    )
    assert status is _bw.SnapshotStatus.PARTIAL
    assert reason


# ─── clean runs stay COMPLETED ────────────────────────────────────────────

def test_clean_run_is_not_degraded():
    status, reason = _bw.evaluate_drain_honesty(
        planned=10,
        drained_ok=10,
        failed=0,
        items_persisted=1_234,
    )
    assert status is None
    assert reason is None


def test_empty_incremental_with_no_failures_is_not_degraded():
    """Every chat drained cleanly and returned nothing new — a legitimate
    quiet incremental, must NOT be flagged."""
    status, reason = _bw.evaluate_drain_honesty(
        planned=6,
        drained_ok=6,
        failed=0,
        items_persisted=0,
    )
    assert status is None
    assert reason is None


def test_no_chats_planned_is_not_degraded():
    """User has no chats at all — zero items is the truth, not a failure."""
    status, reason = _bw.evaluate_drain_honesty(
        planned=0,
        drained_ok=0,
        failed=0,
        items_persisted=0,
    )
    assert status is None
    assert reason is None


# ─── the finalize sites' terminal-status choice ───────────────────────────

def test_finalize_picks_partial_when_handler_recorded_degradation():
    proposed, reason = _bw.terminal_status_for_extra_data(
        {"degraded_reason": "160 of 200 chat(s) did not run"},
    )
    assert proposed is _bw.SnapshotStatus.PARTIAL
    assert reason == "160 of 200 chat(s) did not run"


def test_finalize_picks_completed_for_a_clean_run():
    proposed, reason = _bw.terminal_status_for_extra_data(
        {"partitioned": False},
    )
    assert proposed is _bw.SnapshotStatus.COMPLETED
    assert reason is None


def test_finalize_tolerates_missing_extra_data():
    proposed, reason = _bw.terminal_status_for_extra_data(None)
    assert proposed is _bw.SnapshotStatus.COMPLETED
    assert reason is None


# ─── EnumerationFailed must survive the partition finalize ────────────────
#
# _backup_one marks the snapshot FAILED, but for a partitioned run the
# shard's own wrapper still marks its partition COMPLETED (EnumerationFailed
# never escapes _backup_one), and _finalize_partitioned_snapshot's
# integrity correction re-derives status from partition counts — flipping
# the snapshot back to COMPLETED. Stamping degraded_reason alongside
# failure_reason lets the finalize's degraded check downgrade it to PARTIAL
# instead of claiming success.

def test_enumeration_failure_stamps_both_reasons():
    ed = _bw.enumeration_failure_extra_data(
        {"partitioned": True, "partition_count": 4},
        "USER_CHATS: 40 of 40 chat drain(s) failed — 0 items persisted",
    )
    assert ed["failure_reason"].startswith("USER_CHATS: 40 of 40")
    assert ed["degraded_reason"] == ed["failure_reason"]
    assert ed["partitioned"] is True
    assert ed["partition_count"] == 4


def test_enumeration_failure_tolerates_missing_extra_data():
    ed = _bw.enumeration_failure_extra_data(None, "boom")
    assert ed["failure_reason"] == "boom"
    assert ed["degraded_reason"] == "boom"


def test_enumeration_failure_reason_is_truncated():
    ed = _bw.enumeration_failure_extra_data({}, "x" * 900)
    assert len(ed["failure_reason"]) <= 320
