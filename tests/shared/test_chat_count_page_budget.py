"""count_chat_messages must be page-bounded, and must never hand back a
partial tally as if it were a total.

Finding (preflight BLOCKER 9, 2026-07-31): the correct `$top` 999 -> 50 cap
on chatMessage collections multiplies this function's page count ~20x. It
paginates a chat's ENTIRE message list purely to produce one integer, and
its only caller — audit-service's nightly chat-integrity sweep — ran it over
every chat drained in the last 24h with no cap. On this tenant that is
~20,000 Graph requests a night against a 6-app registration pool, which can
throttle-cascade into the 02:07 Gold run.

The fix must not revert the `$top` cap (999 really is a 400), so the walk
itself has to become bounded:

  * `max_pages` caps the round trips for one chat.
  * A capped walk returns None — the count is UNKNOWN, never a partial that
    a caller could compare against a DB count and false-flag as a gap
    (which would force a full re-drain of the 713k-message account).
  * `stats` reports the pages actually spent (so the caller can keep a
    per-run budget) and the partial tally as an explicit LOWER BOUND, which
    can prove a gap but can never disprove one.
"""
from __future__ import annotations

import asyncio
import os

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

import pytest

from shared.graph_client import GraphClient

GRAPH_URL = "https://graph.microsoft.com/v1.0"


def _paged_get(calls, total_pages, per_page=50):
    """Fake _get serving `total_pages` pages of `per_page` messages, chained
    by @odata.nextLink exactly like Graph's chatMessage collection."""

    async def fake_get(url, params=None):
        calls.append((url, dict(params or {})))
        top = (params or {}).get("$top")
        assert top is None or int(top) <= 50, "chatMessage $top caps at 50"
        idx = len(calls)  # 1-based page number
        page = {"value": [{"id": f"p{idx}m{i}"} for i in range(per_page)]}
        if idx < total_pages:
            page["@odata.nextLink"] = f"{GRAPH_URL}/chats/ch1/messages?$skiptoken=p{idx + 1}"
        return page

    return fake_get


def _client(fake_get):
    g = GraphClient.__new__(GraphClient)
    g.GRAPH_URL = GRAPH_URL
    g._get = fake_get
    return g


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def test_max_pages_bounds_the_round_trips():
    calls = []
    g = _client(_paged_get(calls, total_pages=100))

    _run(g.count_chat_messages("ch1", max_pages=4))

    assert len(calls) == 4, (
        f"a 100-page chat must cost 4 requests when max_pages=4, spent {len(calls)}"
    )


def test_capped_walk_returns_unknown_not_a_partial_count():
    """A partial tally compared against the DB count reads as a huge gap and
    triggers a full re-drain of the biggest mailbox in the tenant. Unknown
    (None) makes the caller skip instead."""
    calls = []
    g = _client(_paged_get(calls, total_pages=100))

    assert _run(g.count_chat_messages("ch1", max_pages=4)) is None


def test_stats_report_pages_spent_and_the_lower_bound():
    calls = []
    g = _client(_paged_get(calls, total_pages=100))
    stats: dict = {}

    result = _run(g.count_chat_messages("ch1", max_pages=4, stats=stats))

    assert result is None
    assert stats["pages"] == 4, "the caller charges its per-run budget from this"
    assert stats["capped"] is True
    assert stats["partial_total"] == 200, (
        "200 messages were actually seen — a LOWER bound on the real total"
    )


def test_complete_walk_is_exact_and_not_flagged_capped():
    calls = []
    g = _client(_paged_get(calls, total_pages=3))
    stats: dict = {}

    result = _run(g.count_chat_messages("ch1", max_pages=40, stats=stats))

    assert result == 150
    assert stats["capped"] is False
    assert stats["pages"] == 3


def test_default_call_is_unchanged_for_existing_callers():
    """No max_pages => the pre-existing unbounded walk, so nothing else in
    the tree changes behavior."""
    calls = []
    g = _client(_paged_get(calls, total_pages=7))

    assert _run(g.count_chat_messages("ch1")) == 350
    assert len(calls) == 7


def test_stats_still_report_pages_when_graph_rejects_the_call():
    """Permission / 404 / throttle swallow to None. The budget must still be
    charged for the request that was actually spent, or a tenant that 403s
    every chat would loop the sweep for free and never terminate early."""
    calls = []

    async def fake_get(url, params=None):
        calls.append((url, dict(params or {})))
        raise RuntimeError("403 Forbidden")

    g = _client(fake_get)
    stats: dict = {}

    assert _run(g.count_chat_messages("ch1", max_pages=40, stats=stats)) is None
    assert stats["pages"] == 1
    assert stats["capped"] is False
    assert "RuntimeError" in str(stats.get("error"))
