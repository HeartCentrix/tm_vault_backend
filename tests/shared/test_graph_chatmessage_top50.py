"""Regression: chatMessage endpoints must cap $top at 50.

Same bug class as ITO-9959 (the /chats probe, proven live: $top=999 → HTTP
400, $top=50 → 200). Graph caps $top at 50 on every chatMessage collection;
the Teams channel-message and chat-message readers still sent $top=999.
Since 36b0bcf (the params fix) these values actually reach Graph, so the
call 400s — and get_channel_messages' caller swallows it into "0 channel
messages", i.e. a silently empty Teams channel backup.

The fake _get here mirrors real Graph: 400 on $top>50 for chatMessage
URLs. Pagination via @odata.nextLink still enumerates everything.
"""
from __future__ import annotations

import asyncio
import os

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

import httpx
import pytest

from shared.graph_client import GraphClient

GRAPH_URL = "https://graph.microsoft.com/v1.0"


class _Resp:
    def __init__(self, status):
        self.status_code = status


def _graph_like_get(calls, pages=None):
    """Fake _get that enforces Graph's real chatMessage $top cap of 50 and
    serves two pages so pagination is exercised."""
    pages = pages or {}

    async def fake_get(url, params=None):
        calls.append((url, dict(params or {})))
        top = (params or {}).get("$top")
        if top is not None and int(top) > 50:
            raise httpx.HTTPStatusError(
                f"Client error '400 Bad Request' for url '{url}' ($top={top})",
                request=None, response=_Resp(400),
            )
        if url in pages:
            return pages[url]
        if "$skiptoken" in url:  # terminal page — no further nextLink
            return {"value": [{"id": "m3"}]}
        return {
            "value": [{"id": "m1"}, {"id": "m2"}],
            "@odata.nextLink": f"{url}?$skiptoken=p2",
        }

    return fake_get


def _client(calls, pages=None):
    g = GraphClient.__new__(GraphClient)
    g.GRAPH_URL = GRAPH_URL
    g._get = _graph_like_get(calls, pages)
    return g


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def test_get_channel_messages_survives_graph_top_cap():
    calls = []
    page2 = f"{GRAPH_URL}/teams/t1/channels/c1/messages/delta?$skiptoken=p2"
    g = _client(calls, pages={page2: {"value": [{"id": "m3"}]}})

    result = _run(g.get_channel_messages("t1", "c1"))

    assert [m["id"] for m in result["value"]] == ["m1", "m2", "m3"], (
        "channel messages must come back (and paginate), not 400 into an "
        "empty backup"
    )
    assert calls[0][1]["$top"] == "50"


def test_get_channel_messages_replies_survives_graph_top_cap():
    calls = []
    g = _client(calls)

    # /replies has no pagination loop in the client — one page is all it reads
    result = _run(g.get_channel_messages_replies("t1", "c1", "m1"))

    assert [m["id"] for m in result["value"]] == ["m1", "m2"]
    assert calls[0][1]["$top"] == "50"


def test_get_chat_messages_survives_graph_top_cap():
    calls = []
    page2 = f"{GRAPH_URL}/chats/ch1/messages?$skiptoken=p2"
    g = _client(calls, pages={page2: {"value": [{"id": "m3"}]}})

    result = _run(g.get_chat_messages("ch1"))

    assert [m["id"] for m in result["value"]] == ["m1", "m2", "m3"]
    assert calls[0][1]["$top"] == "50"


def test_count_chat_messages_survives_graph_top_cap():
    """count_chat_messages swallows every exception into None, so a 400 here
    reads as "Graph won't tell us" and the integrity verifier skips the
    chat — silently disabling drop detection."""
    calls = []
    page2 = f"{GRAPH_URL}/chats/ch1/messages?$skiptoken=p2"
    g = _client(calls, pages={page2: {"value": [{"id": "m3"}]}})

    assert _run(g.count_chat_messages("ch1")) == 3
    assert calls[0][1]["$top"] == "50"


@pytest.mark.parametrize(
    "coro_factory",
    [
        lambda g: g.get_channel_messages("t1", "c1"),
        lambda g: g.get_channel_messages_replies("t1", "c1", "m1"),
        lambda g: g.get_chat_messages("ch1"),
        lambda g: g.count_chat_messages("ch1"),
    ],
    ids=["channel_messages", "channel_replies", "chat_messages", "count_chat"],
)
def test_no_chatmessage_call_exceeds_top_50(coro_factory):
    calls = []
    g = _client(calls)

    _run(coro_factory(g))

    for url, params in calls:
        top = params.get("$top")
        assert top is None or int(top) <= 50, (
            f"{url} sent $top={top}; Graph caps chatMessage $top at 50"
        )
