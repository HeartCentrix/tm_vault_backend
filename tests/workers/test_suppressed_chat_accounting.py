"""Permanently-unreadable chats are SUPPRESSED, not "failed".

Bug (adversarial review, 2026-07-31): the smart-skip arm — which exists
precisely because a chat is deterministically unreadable (prior PERMISSION
failure with an UNCHANGED scope fingerprint, or GONE and no longer in the
chat list) — counted the chat as a drain FAILURE. Cross-tenant
`19:meeting_*` chats are permanently 403 and common, so:

  * any user holding one was PARTIAL on EVERY run, forever;
  * a user whose ONLY chats are such chats was FAILED every run, and
    because FAILED is outside create_snapshot's dedup set that also minted
    a fresh snapshot row + BACKUP_FAILED audit each run.

A false-alarm generator, and it buries the real signal for exactly the
tenants most likely to hit it. Suppressed units are therefore excluded
from the failure math (a run that captured everything it legitimately can
reports success) while still being recorded on the snapshot under a key
that does NOT claim content is missing.

The honesty property must survive: a run where READABLE chats genuinely
failed still may not report COMPLETED.
"""
from __future__ import annotations

import ast
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
_spec = importlib.util.spec_from_file_location("bw_suppressed", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_suppressed"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )

SnapshotStatus = _bw.SnapshotStatus


# ─── the decision ─────────────────────────────────────────────────────────

def test_only_permanently_unreadable_chats_is_an_honest_success():
    """3 cross-tenant meeting chats, all known-403, nothing else to fetch."""
    status, reason = _bw.evaluate_drain_honesty(
        planned=3, drained_ok=0, failed=0, suppressed=3,
        items_persisted=0, unit="chat",
    )
    assert status is None, (
        "a run that captured everything it legitimately can must not report "
        f"{status} ({reason})"
    )
    assert reason is None


def test_suppressed_alongside_real_content_is_still_success():
    status, reason = _bw.evaluate_drain_honesty(
        planned=5, drained_ok=3, failed=0, suppressed=2,
        items_persisted=412, unit="chat",
    )
    assert status is None and reason is None


def test_a_readable_chat_that_failed_still_blocks_completed():
    status, reason = _bw.evaluate_drain_honesty(
        planned=5, drained_ok=3, failed=1, suppressed=1,
        items_persisted=412, unit="chat",
    )
    assert status is SnapshotStatus.PARTIAL
    assert "1 of 5 chat drain(s) failed" in reason
    assert "suppress" not in reason.lower(), (
        "the reason must describe the real failure, not the suppression"
    )


def test_nothing_captured_and_a_real_failure_is_still_failed():
    status, reason = _bw.evaluate_drain_honesty(
        planned=3, drained_ok=0, failed=1, suppressed=2,
        items_persisted=0, unit="chat",
    )
    assert status is SnapshotStatus.FAILED
    assert "0 items persisted" in reason


def test_suppressed_units_are_not_double_counted_as_never_ran():
    """never_ran is DERIVED — suppressed units must be subtracted or every
    suppressed chat re-enters the math as a truncated fanout."""
    status, reason = _bw.evaluate_drain_honesty(
        planned=4, drained_ok=1, failed=0, suppressed=3,
        items_persisted=90, unit="chat",
    )
    assert status is None, reason


def test_truncated_fanout_is_still_visible_with_suppression_present():
    status, reason = _bw.evaluate_drain_honesty(
        planned=6, drained_ok=2, failed=0, suppressed=2,
        items_persisted=90, unit="chat",
    )
    assert status is SnapshotStatus.PARTIAL
    assert "2 of 6 chat(s) did not run" in reason


def test_suppressed_defaults_to_zero_for_the_mail_fanout():
    """USER_MAIL has no smart-skip; its call site must keep working."""
    status, reason = _bw.evaluate_drain_honesty(
        planned=3, drained_ok=2, failed=1, items_persisted=10, unit="folder",
    )
    assert status is SnapshotStatus.PARTIAL
    assert "1 of 3 folder drain(s) failed" in reason


# ─── what the snapshot records ────────────────────────────────────────────

def test_suppression_note_names_the_count_and_stays_short():
    note = _bw.suppressed_units_note(suppressed=2, planned=7, unit="chat")
    assert "2 of 7" in note
    assert "chat" in note
    assert len(note) <= 300


def test_suppression_note_is_empty_when_nothing_was_suppressed():
    assert _bw.suppressed_units_note(suppressed=0, planned=7, unit="chat") == ""


def test_the_suppression_key_does_not_settle_the_snapshot_partial():
    """It records a permanent, known-unreadable gap — not "content that
    should be here is missing", which is what degraded_reason means."""
    assert (
        _bw.SNAPSHOT_SUPPRESSED_REASON_KEY != _bw.SNAPSHOT_DEGRADED_REASON_KEY
    )
    status, reason = _bw.terminal_status_for_extra_data(
        {_bw.SNAPSHOT_SUPPRESSED_REASON_KEY: "2 of 7 chat(s) skipped"},
    )
    assert status is SnapshotStatus.COMPLETED
    assert reason is None
    # and a genuinely degraded run is unaffected
    status2, reason2 = _bw.terminal_status_for_extra_data({
        _bw.SNAPSHOT_SUPPRESSED_REASON_KEY: "2 of 7 chat(s) skipped",
        _bw.SNAPSHOT_DEGRADED_REASON_KEY: "1 of 7 chat drain(s) failed",
    })
    assert status2 is SnapshotStatus.PARTIAL
    assert "failed" in reason2


# ─── call-site wiring ─────────────────────────────────────────────────────

_SRC = _MAIN_PATH.read_text()


def _smart_skip_arm() -> str:
    idx = _SRC.find("if _should_skip_failed:")
    assert idx != -1
    end = _SRC.find("return cid, [], saved_cursor", idx)
    assert end != -1
    return _SRC[idx:end]


def test_smart_skip_arm_tallies_suppressed_not_failed():
    arm = _smart_skip_arm()
    assert '_chat_outcomes["suppressed"] += 1' in arm
    assert '_chat_outcomes["failed"] += 1' not in arm, (
        "a deliberate, permanent skip is not a drain failure"
    )


def test_outcome_dict_declares_the_suppressed_bucket():
    assert '_chat_outcomes = {"ok": 0, "failed": 0, "suppressed": 0}' in _SRC


def test_chat_honesty_gate_receives_the_suppressed_tally():
    idx = _SRC.find("_chat_honesty_status, _chat_honesty_reason")
    assert idx != -1
    blk = _SRC[idx: idx + 900]
    assert 'suppressed=_chat_outcomes["suppressed"]' in blk
    assert 'failed=_chat_outcomes["failed"]' in blk


def test_chat_handler_records_the_suppression_on_the_snapshot():
    idx = _SRC.find("_chat_honesty_status, _chat_honesty_reason")
    blk = _SRC[idx: idx + 3000]
    assert "suppressed_units_note(" in blk
    assert "SNAPSHOT_SUPPRESSED_REASON_KEY" in blk


def test_every_drain_outcome_still_lands_in_exactly_one_bucket():
    """AST pin: the only writers of _chat_outcomes are single-key bumps of
    known buckets, so no exit can double-count or vanish."""
    tree = ast.parse(_SRC)
    buckets = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.AugAssign):
            continue
        tgt = node.target
        if (isinstance(tgt, ast.Subscript)
                and isinstance(tgt.value, ast.Name)
                and tgt.value.id == "_chat_outcomes"):
            assert isinstance(node.op, ast.Add)
            assert isinstance(tgt.slice, ast.Constant)
            buckets.add(tgt.slice.value)
    assert buckets == {"ok", "failed", "suppressed"}, buckets
