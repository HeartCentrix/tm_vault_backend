"""The per-user chat delta round pages to its deltaLink and reports the chats with changes."""
import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from shared.chat_capture_graph import ChatGraphError, StrictChatGraphSource


def fake(pages):
    calls = []

    async def _json(url, *, params=None):
        calls.append((url, params))
        return pages.pop(0)
    return SimpleNamespace(_client=SimpleNamespace(GRAPH_URL="https://graph.microsoft.com/v1.0"),
                           principal_id="user-1", _json=_json), calls


def test_new_round_starts_at_since_and_collects_chats_across_pages():
    source, calls = fake([
        {"value": [{"id": "m1", "chatId": "19:a"}, {"id": "m2", "chatId": "19:b"}], "@odata.nextLink": "https://graph.microsoft.com/v1.0/next"},
        {"value": [{"id": "m3", "chatId": "19:a"}], "@odata.deltaLink": "https://graph.microsoft.com/v1.0/delta-2"},
    ])
    changed, link = asyncio.run(StrictChatGraphSource.message_delta(
        source, None, since=datetime(2026, 11, 5, 8, 30, tzinfo=timezone.utc)))
    assert changed == {"19:a", "19:b"} and link == "https://graph.microsoft.com/v1.0/delta-2"
    assert calls[0] == ("https://graph.microsoft.com/v1.0/users/user-1/chats/getAllMessages/delta",
                        {"$top": "50", "$filter": "lastModifiedDateTime gt 2026-11-05T08:30:00.000Z"})
    assert calls[1] == ("https://graph.microsoft.com/v1.0/next", None)


def test_saved_cursor_resumes_and_a_round_without_cursor_is_an_error():
    source, calls = fake([{"value": []}])
    with pytest.raises(ChatGraphError):
        asyncio.run(StrictChatGraphSource.message_delta(source, "https://graph.microsoft.com/v1.0/delta-1"))
    assert calls == [("https://graph.microsoft.com/v1.0/delta-1", None)]
