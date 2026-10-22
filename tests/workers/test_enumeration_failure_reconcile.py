"""A hard enumeration failure must never leave a false recovery point.

Live incident (2026-08-05, Gold 10:07 UTC tick — the run the operator
flagged as "one of the backups failed"):

  worker log : [USER_CHATS] Chats — Corp_Esplanade: 20 of 20 chat drain(s)
               failed — 0 items persisted, refusing empty-COMPLETED finalize
  worker log : snapshot 0e8ed6f6 FAILED — enumeration failed: ...
  DATABASE   : status=COMPLETED  item_count=0  extra_data={}

All 8 USER_CHATS snapshots in that batch showed the same divergence, and
one ROOM_MAILBOX snapshot (f72b7bbc) ended FAILED but ALSO with
extra_data={} — so in every case the failure REASON was never persisted
either.

Mechanism: ``_record_enumeration_failure`` fenced its write on
``status == IN_PROGRESS``. The inline finalize had already flipped the row
COMPLETED (proved by duration_secs being set, which only that path
writes), so the fence silently no-opped — while the function printed
"snapshot X FAILED" and returned "snapshot_failed" unconditionally. The
log claimed the truth was recorded; the database disagreed.

Why it matters: a COMPLETED snapshot with 0 items is offered as a valid
restore point, and COMPLETED is the only status that promotes a delta
cursor — so the next incremental starts *after* content that was never
captured. That converts a visible failure into permanent silent data loss.
"""
from __future__ import annotations

import importlib.util
import pathlib
import sys

import pytest

_WORKER = (
    pathlib.Path(__file__).resolve().parents[2]
    / "workers" / "backup-worker" / "main.py"
)


def _load_worker():
    """backup-worker/main.py is hyphenated and huge; load it the same way
    tests/workers/test_exclusion_matcher.py does."""
    spec = importlib.util.spec_from_file_location("bw_main_enum", _WORKER)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["bw_main_enum"] = mod
    spec.loader.exec_module(mod)
    return mod




bw = None
try:
    bw = _load_worker()
except Exception as exc:  # pragma: no cover - env without worker deps
    pytest.skip(f"backup-worker not importable: {exc}", allow_module_level=True)

S = bw.SnapshotStatus
reconcile = bw.reconcile_enumeration_failure_status


def test_in_progress_becomes_failed():
    """The original, working case must keep working."""
    assert reconcile(S.IN_PROGRESS, 0) is S.FAILED
    assert reconcile(S.IN_PROGRESS, 500) is S.FAILED


def test_completed_with_zero_items_is_corrected_to_failed():
    """THE live bug: 0e8ed6f6 et al. A COMPLETED-empty snapshot after a
    hard enumeration failure is a false recovery point — it would be
    offered for restore and it promotes a delta cursor past content that
    was never captured."""
    assert reconcile(S.COMPLETED, 0) is S.FAILED
    assert reconcile(S.COMPLETED, None) is S.FAILED


def test_completed_with_items_is_downgraded_to_partial_not_failed():
    """Enumeration died mid-walk but content landed. Discarding it would
    be worse than keeping it — PARTIAL keeps the data reachable while
    still blocking the delta-cursor promotion, so the next run
    re-enumerates what this one missed."""
    assert reconcile(S.COMPLETED, 1) is S.PARTIAL
    assert reconcile(S.COMPLETED, 9000) is S.PARTIAL


def test_already_failed_is_left_alone():
    """Idempotent: a second settle attempt must not churn the row."""
    assert reconcile(S.FAILED, 0) is None
    assert reconcile(S.FAILED, 10) is None


def test_partial_with_items_is_left_alone():
    """A handler already recorded a degraded outcome with content — that
    is the honest status; don't escalate it."""
    assert reconcile(S.PARTIAL, 10) is None


def test_partial_is_left_alone_even_at_zero_items():
    """Deliberately NOT corrected. _finalize_partitioned_snapshot owns
    aggregating shard outcomes into PARTIAL, and a parent can legitimately
    sit at PARTIAL with its counts not yet written — overruling it from
    here would race the partition finalizer (pinned by
    test_shard_failure_accounting.py::
    test_whole_resource_failure_does_not_touch_an_already_terminal_snapshot).
    A shard's own failure is already recorded on its snapshot_partitions
    row, so nothing is lost by leaving this alone."""
    assert reconcile(S.PARTIAL, 0) is None


def test_pending_deletion_is_never_resurrected():
    """A snapshot being torn down must not be flipped back to a live
    status — the blobs are already going away."""
    assert reconcile(S.PENDING_DELETION, 0) is None


def test_unknown_status_is_left_alone_rather_than_guessed():
    assert reconcile("SOMETHING_NEW", 0) is None
    assert reconcile(None, 0) is None


def test_string_statuses_are_accepted():
    """SQLAlchemy hands back the enum, but a raw asyncpg/text() read gives
    a plain string. Comparing an enum to a str with `is` would silently
    take the no-op branch and re-introduce the bug."""
    assert reconcile("IN_PROGRESS", 0) is S.FAILED
    assert reconcile("COMPLETED", 0) is S.FAILED
    assert reconcile("COMPLETED", 5) is S.PARTIAL
    assert reconcile("FAILED", 0) is None


# ─── the reason must always be recorded ────────────────────────────────

def test_failure_reason_is_stamped_even_when_status_is_unchanged():
    """Both live snapshots had extra_data={}, so nobody could tell WHY the
    backup failed. The reason must be persisted independently of whether
    the status write applies."""
    out = bw.enumeration_failure_extra_data({}, "20 of 20 chat drain(s) failed")
    assert out["failure_reason"]
    assert out[bw.SNAPSHOT_DEGRADED_REASON_KEY]


def test_reason_stamp_preserves_existing_extra_data():
    out = bw.enumeration_failure_extra_data(
        {"partitioned": True}, "boom",
    )
    assert out["partitioned"] is True and out["failure_reason"] == "boom"


# ─── wiring pins ───────────────────────────────────────────────────────

def _worker_src() -> str:
    return _WORKER.read_text()


def test_record_enumeration_failure_reports_whether_the_write_landed():
    """It used to print 'snapshot X FAILED' and return 'snapshot_failed'
    unconditionally, so the log asserted something the DB never did."""
    src = _worker_src()
    fn = src[src.index("async def _record_enumeration_failure"):]
    fn = fn[:fn.index("\n    async def ", 1)]
    assert "reconcile_enumeration_failure_status" in fn, (
        "the whole-resource path must reconcile an already-terminal "
        "snapshot instead of silently no-opping"
    )
    assert "status NOT recorded" in fn or "not_recorded" in fn, (
        "a failed/skipped status write must say so in the log"
    )


def test_chats_failed_gate_stamps_before_raising():
    """Belt-and-braces for the race: stamping the degraded reason BEFORE
    raising means whichever finalize path wins can already see that this
    run is not clean, so it cannot settle COMPLETED in the first place.
    The PARTIAL branch always did this; the FAILED branch did not."""
    src = _worker_src()
    gate = src[src.index("if _chat_honesty_status is SnapshotStatus.FAILED:"):]
    gate = gate[:gate.index("if _chat_honesty_status is SnapshotStatus.PARTIAL:")]
    assert (
        "_stamp_snapshot_degraded" in gate
        or "_patch_snapshot_extra_data" in gate
    ), "the FAILED branch must persist the reason before raising"
