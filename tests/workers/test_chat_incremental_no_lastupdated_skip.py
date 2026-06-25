"""Durable fix: incremental chat drains must NOT skip the per-chat message
query based on chat.lastUpdatedDateTime.

Verified LIVE against Graph (2026-06-25): a chat with Rohit + Akshat had a
message at 2026-06-24T14:42Z while Graph reported that chat's
lastUpdatedDateTime as 2026-06-23T14:17Z — i.e. Graph does NOT bump
lastUpdatedDateTime on new 1:1/group messages. The old per-chat skip
(`chat_lu <= saved_cursor` -> return without calling /messages) therefore
silently dropped every message sent after lastUpdatedDateTime last moved.

Two pure helpers pin the durable contract:
  - _chat_should_skip_message_query: NEVER skip the messages call based on
    lastUpdatedDateTime (the messages query, gated by the cheap
    lastModifiedDateTime filter, is the single source of truth for activity).
  - _chat_next_cursor: the saved cursor is derived ONLY from real message
    timestamps, never inflated to chat.lastUpdatedDateTime (inflation pushed
    the next `lastModifiedDateTime gt cursor` filter past unfetched messages).

Loads workers/backup-worker/main.py via importlib (hyphen path) — same
pattern as test_chat_carry_forward_vanished.py.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

_MAIN_PATH = (
    pathlib.Path(__file__).resolve().parents[2]
    / "workers" / "backup-worker" / "main.py"
)
_spec = importlib.util.spec_from_file_location("bw_main_chat_incr", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_main_chat_incr"] = _bw
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )


def test_drains_when_chat_has_new_messages():
    """chat_lu = the chat's LAST MESSAGE time (from lastMessagePreview). When it
    is NEWER than our cursor there are unfetched messages -> MUST NOT skip. This
    is the data-loss fix: a new message is never skipped (the old code keyed on
    lastUpdatedDateTime, which Graph freezes, so it skipped real messages)."""
    assert _bw._chat_should_skip_message_query(
        "2026-06-23T00:00:00Z", "2026-06-25T00:00:00Z"
    ) is False


def test_skips_when_no_new_messages():
    """Last message at-or-before the cursor -> nothing new -> skip with NO
    per-chat call (zero duration cost). Reliable because chat_lu is the real
    last-message time, not the frozen lastUpdatedDateTime."""
    assert _bw._chat_should_skip_message_query(
        "2026-06-25T00:00:00Z", "2026-06-23T00:00:00Z"
    ) is True
    assert _bw._chat_should_skip_message_query(
        "2026-06-23T00:00:00Z", "2026-06-23T00:00:00Z"
    ) is True


def test_no_skip_on_http_cursor_or_missing_signal():
    """An http deltaLink cursor is not ISO-string-comparable, and a missing
    last-message signal is ambiguous -> drain to be safe rather than risk a
    drop."""
    assert _bw._chat_should_skip_message_query(
        "https://graph.microsoft.com/v1.0/x/delta?$deltatoken=abc",
        "2026-06-23T00:00:00Z",
    ) is False
    assert _bw._chat_should_skip_message_query(
        "2026-06-25T00:00:00Z", None
    ) is False


def test_cursor_is_message_derived_not_lastupdated():
    """Old code pinned drain_cursor to max(message_max, chat_lu). When chat_lu
    is AHEAD of the newest real message (a reaction/system bump), that inflated
    the cursor so the next `lastModifiedDateTime gt cursor` filter skipped real
    messages. Durable contract: cursor = newest real message timestamp only."""
    # chat_lu ahead of the newest message → cursor must stay at the message ts.
    assert _bw._chat_next_cursor(
        "2026-06-20T10:00:00Z", "2026-06-23T00:00:00Z"
    ) == "2026-06-20T10:00:00Z"
    # message newer than chat_lu → message ts.
    assert _bw._chat_next_cursor(
        "2026-06-25T10:00:00Z", "2026-06-23T00:00:00Z"
    ) == "2026-06-25T10:00:00Z"
    # no new messages this run → do not advance (None == "keep prior cursor").
    assert _bw._chat_next_cursor(None, "2026-06-23T00:00:00Z") is None


def test_chat_messages_filter_url_has_no_orderby():
    """Graph GET /chats/{id}/messages returns HTTP 400 on
    $orderby=lastModifiedDateTime (verified live 2026-06-25), which failed the
    ENTIRE per-chat drain and stranded the cursor -> new messages never
    captured. The incremental URL must carry $filter ONLY (Graph honors it)."""
    u = _bw._chat_messages_filter_url(
        "https://graph.microsoft.com/v1.0/chats/19:abc@thread.v2/messages",
        "2026-06-23T17:47:43.868Z",
    )
    assert "$orderby" not in u, "Graph 400s on $orderby for chat messages"
    assert "$filter=" in u and "lastModifiedDateTime" in u
