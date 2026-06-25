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


def test_never_skips_message_query_on_lastupdated():
    """The skip used to fire whenever chat_lu <= saved_cursor. Because Graph
    freezes lastUpdatedDateTime, that dropped real new messages. The durable
    contract: never skip the messages call on lastUpdatedDateTime — not when
    it is older than the cursor, not when it is equal."""
    # chat_lu newer than cursor (the rare case Graph DOES advance it) — run.
    assert _bw._chat_should_skip_message_query(
        "2026-06-23T00:00:00Z", "2026-06-25T00:00:00Z"
    ) is False
    # chat_lu == cursor (the live-evidence case: frozen at 06-23) — STILL run.
    assert _bw._chat_should_skip_message_query(
        "2026-06-23T00:00:00Z", "2026-06-23T00:00:00Z"
    ) is False
    # chat_lu older than cursor (stale/frozen) — STILL run.
    assert _bw._chat_should_skip_message_query(
        "2026-06-25T00:00:00Z", "2026-06-23T00:00:00Z"
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
