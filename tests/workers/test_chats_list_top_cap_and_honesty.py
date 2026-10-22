"""USER_CHATS list enumeration: respect Graph's /chats $top cap, and never
degrade a failed enumeration into "this user has no chats".

Live RCA (2026-08-05, verified in prod after the overlapfix deploy). Every
USER_CHATS snapshot came back COMPLETED / item_count 0 / extra_data {} while
108 chats recorded a fresh failure. The worker log showed why:

    [USER_CHATS] $expand=members unsupported, falling back to $batch:
        400 Bad Request .../users/<id>/chats?$top=200&$select=...&$expand=members
    [USER_CHATS] chats list failed for <id>:
        400 Bad Request .../users/<id>/chats?$top=200&$select=...
    [USER_CHATS] skip-by-activity: 0 reused from cache, 0 need member fetch

Two independent defects stacked:

1. ``$top=200``. **Graph caps $top at 50 on /chats** — anything higher is a
   deterministic 400. This is already established in-repo: commit c0df8f0
   ("cap /chats probe $top at 50; stop treating Graph 400 as permanent
   license failure") fixed $top=999 -> 50 in shared/graph_client.py for the
   DISCOVERY probe. That branch (fix/chats-probe-top50-ito9777) never merged
   into this lineage, so discovery was fixed and the backup path was not —
   which is why all 119 chats have failed since 2026-06-17.

2. The failed list was swallowed into ``chats_raw = []``. With zero chats
   enumerated, ``planned == 0``, so evaluate_drain_honesty short-circuits on
   ``if planned <= 0: return None, None`` and the run settles a clean
   COMPLETED with no items and no reason. A total enumeration failure is
   reported as a successful empty backup — and COMPLETED is the only status
   that promotes a delta cursor, so the next incremental would resume past
   messages that were never captured.

Defect 2 is why the drain-honesty gate could not catch this: the gate
answers "all N planned drains failed", and here nothing was ever planned.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import re
import sys

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_chats_top", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_chats_top"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )

SRC = _MAIN_PATH.read_text()


# ─── 1. the $top cap ───────────────────────────────────────────────────

def test_chats_list_page_size_is_at_most_50():
    """Graph's documented hard cap on /chats. 51+ is a 400, not a
    truncation, so this is a correctness bound and not a tuning knob."""
    assert hasattr(_bw, "CHATS_LIST_PAGE_SIZE"), (
        "the /chats page size must be a named constant so both call sites "
        "and this test cannot drift"
    )
    assert 0 < _bw.CHATS_LIST_PAGE_SIZE <= 50


def test_no_chats_list_call_hardcodes_a_page_size_above_the_cap():
    """Both the $expand=members attempt and the plain fallback sent
    $top=200. Fixing only one leaves the fallback 400ing, which is the
    call whose failure actually zeroed the run."""
    # Every `"$top": "<literal>"` that sits inside a /users/{...}/chats
    # request must be the constant, not a literal.
    chats_calls = [
        m.start() for m in re.finditer(r"/users/\{user_id\}/chats", SRC)
    ]
    assert chats_calls, "could not locate the /chats list call sites"
    for pos in chats_calls:
        window = SRC[pos: pos + 700]
        literal = re.search(r'"\$top":\s*"(\d+)"', window)
        assert literal is None, (
            f"/chats call at offset {pos} hardcodes $top={literal.group(1)}; "
            f"use CHATS_LIST_PAGE_SIZE (Graph caps it at 50)"
        )
        assert "CHATS_LIST_PAGE_SIZE" in window, (
            f"/chats call at offset {pos} must page with CHATS_LIST_PAGE_SIZE"
        )


def test_both_chats_list_sites_were_converted():
    """There are two: the $expand=members attempt and the fallback."""
    assert SRC.count("CHATS_LIST_PAGE_SIZE") >= 3, (
        "expected the constant definition plus both /chats call sites"
    )


# ─── 2. a failed enumeration must not read as "no chats" ───────────────

def test_chats_list_failure_raises_instead_of_returning_empty():
    """``chats_raw = []`` on a failed list is what produced the clean
    COMPLETED-empty snapshots. It must raise EnumerationFailed so
    _backup_one records the failure and the snapshot ends FAILED with a
    reason — the same rule the module already applies to a mid-walk Graph
    error ("Do NOT degrade an enumeration failure to 'no items'")."""
    i = SRC.index("[USER_CHATS] chats list failed for")
    block = SRC[i: i + 900]
    assert "EnumerationFailed" in block, (
        "a failed /chats list must raise EnumerationFailed, not swallow"
    )
    assert not re.search(r"chats_raw\s*=\s*\[\]", block), (
        "the failed-list path must no longer degrade to an empty chat list"
    )


def test_enumeration_failed_from_the_list_reaches_backup_one():
    """EnumerationFailed raised inside _fetch_for is caught by _backup_one,
    which calls _record_enumeration_failure. Pin the nesting so a future
    refactor that moves the list call out of _fetch_for fails loudly."""
    fetch_for = SRC.index("async def _fetch_for(")
    backup_one = SRC.index("async def _backup_one(")
    list_call = SRC.index("[USER_CHATS] chats list failed for")
    assert fetch_for < list_call < backup_one, (
        "the /chats list call must stay inside _fetch_for so its raise is "
        "caught by _backup_one's `except EnumerationFailed`"
    )
    handler = SRC[backup_one: backup_one + 2000]
    assert "except EnumerationFailed" in handler
    assert "_record_enumeration_failure(" in handler


def test_the_honesty_gate_still_cannot_see_a_zero_planned_run():
    """Documents WHY defect 2 needed its own fix rather than relying on the
    drain-honesty gate: with nothing enumerated, planned == 0 and the gate
    deliberately returns a clean verdict."""
    status, reason = _bw.evaluate_drain_honesty(
        planned=0, drained_ok=0, failed=0, items_persisted=0,
    )
    assert status is None and reason is None
