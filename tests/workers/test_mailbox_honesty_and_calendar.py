"""Regression pins for the Corp_Esplanade room-mailbox post-mortem
(2026-07-14): a room with 241 calendar events + 204 inbox messages
produced two COMPLETED-empty snapshots.

1. get_mail_folder_tree exceptions were downgraded to an empty tree
   (policy-blocked 403/404 already return {} INSIDE the helper) — a
   429-retry-exhaustion during app-pool saturation became "empty
   mailbox". Must raise so the outer handler fail_snapshot()s.
2. If every attempted folder drain raised and zero items persisted,
   the handler still finalized COMPLETED-empty. Must refuse.
3. backup_mailbox never captured calendars at all — a room's primary
   content. CALENDAR_EVENT capture now runs on the Tier-1 path.

Source-inspection pins (same rationale as the other worker tests).
"""
from __future__ import annotations

import pathlib

_MAIN = (
    pathlib.Path(__file__).resolve().parents[2]
    / "workers" / "backup-worker" / "main.py"
).read_text()


def _mailbox_handler() -> str:
    start = _MAIN.find("async def backup_one_mailbox")
    assert start != -1
    end = _MAIN.find("results = await asyncio.gather(\n            *[backup_one_mailbox", start)
    return _MAIN[start:end if end != -1 else start + 40000]


def test_folder_tree_failure_raises_not_empty_tree():
    h = _mailbox_handler()
    idx = h.find("folder tree \"\n                            f\"fetch failed")
    if idx == -1:
        idx = h.find("fetch failed for {resource.external_id}")
    assert idx != -1
    blk = h[idx: idx + 700]
    assert "raise" in blk, "folder-tree fetch failure must fail the snapshot"
    assert "folder_tree = {}" not in blk


def test_all_drains_failed_refuses_empty_finalize():
    h = _mailbox_handler()
    assert "refusing empty-COMPLETED finalize" in h
    assert 'totals.get("drain_failures", 0) > 0' in h


def test_drain_failures_counted():
    h = _mailbox_handler()
    assert 'totals["drain_failures"] = totals.get("drain_failures", 0) + 1' in h


def test_mailbox_path_captures_calendar_events():
    h = _mailbox_handler()
    assert "get_calendar_events_delta" in h
    assert '"CALENDAR_EVENT"' in h
    assert '"/Calendar"' in h
    # calendar failure feeds the empty-finalize guard, never silent
    idx = h.find("calendar \"\n                                f\"capture failed")
    if idx == -1:
        idx = h.find("capture failed for {user_label}")
    assert idx != -1
