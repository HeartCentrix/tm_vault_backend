"""The detached hostedContent / attachment interleave must not swallow
failures.

Bug (adversarial review, 2026-07-31): with USER_CHATS_HC_BARRIER_DETACHED
(the default) Phase-2's retry filter runs BEFORE the kick tasks finish, so
nothing re-reads the `_hc_interleaved` / `_att_interleaved` marks after it.
Meanwhile:

  * _kick_hosted_content_for_chat catches every per-message exception
    internally, so the finalizer's gather sees no exception, and
  * _kick_attachments_for_chat marked every message `_att_interleaved`
    even when BOTH the session.add_all AND the per-row bulk-upsert
    fallback failed — attachments downloaded but never committed.

Result: inline images and attachments permanently lost while
snapshot.hc_drain_status was recorded COMPLETE.
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
_spec = importlib.util.spec_from_file_location("bw_interleave", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_interleave"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )

CHAT_A = "19:aaaaaaaa@thread.v2"
CHAT_B = "19:bbbbbbbb@thread.v2"


# ─── hc_drain_status must never claim COMPLETE over lost content ───────────

def test_clean_detached_drain_is_complete():
    status, reason = _bw.interleave_drain_status(
        gather_errors=0, hc_failed=0, att_failed=0,
    )
    assert status == "COMPLETE"
    assert reason is None


def test_swallowed_per_message_hc_failure_is_not_complete():
    """The exact bug: the gather returned cleanly because the kick caught
    the exception itself."""
    status, reason = _bw.interleave_drain_status(
        gather_errors=0, hc_failed=3, att_failed=0,
    )
    assert status != "COMPLETE"
    assert reason and "3" in reason


def test_uncommitted_attachments_are_not_complete():
    status, reason = _bw.interleave_drain_status(
        gather_errors=0, hc_failed=0, att_failed=7,
    )
    assert status != "COMPLETE"
    assert reason and "7" in reason


def test_gather_level_exception_is_not_complete():
    status, reason = _bw.interleave_drain_status(
        gather_errors=1, hc_failed=0, att_failed=0,
    )
    assert status != "COMPLETE"
    assert reason


def test_all_three_signals_are_reported_together():
    status, reason = _bw.interleave_drain_status(
        gather_errors=2, hc_failed=5, att_failed=9,
    )
    assert status == "FAILED"
    assert "5" in reason and "9" in reason and "2" in reason


# ─── the retry pass groups failures back into per-chat batches ─────────────

def test_failed_entries_group_by_chat():
    grouped = _bw.group_failed_interleaves([
        (CHAT_A, {"id": "m1"}),
        (CHAT_B, {"id": "m2"}),
        (CHAT_A, {"id": "m3"}),
    ])
    assert set(grouped) == {CHAT_A, CHAT_B}
    assert [m["id"] for m in grouped[CHAT_A]] == ["m1", "m3"]
    assert [m["id"] for m in grouped[CHAT_B]] == ["m2"]


def test_duplicate_messages_are_deduped():
    """A message can be recorded twice (hc list fetch + persist). Retrying
    it twice would double the Graph cost for no gain."""
    grouped = _bw.group_failed_interleaves([
        (CHAT_A, {"id": "m1"}),
        (CHAT_A, {"id": "m1"}),
    ])
    assert len(grouped[CHAT_A]) == 1


def test_entries_without_an_id_are_dropped():
    grouped = _bw.group_failed_interleaves([
        (CHAT_A, {"id": None}),
        (CHAT_A, {}),
        (CHAT_A, {"id": "m9"}),
    ])
    assert [m["id"] for m in grouped[CHAT_A]] == ["m9"]


def test_empty_input_groups_to_nothing():
    assert _bw.group_failed_interleaves([]) == {}
    assert _bw.group_failed_interleaves(None) == {}
