"""Unit tests for shared/archive_graph.py — the Exchange in-place ARCHIVE
adapter over the Graph Mailbox Import/Export API. No network: a FakeGC stubs
_get/_post so we test URL/version handling, delta threading, the 20-id export
batching, and base64 decode without hitting Graph."""
from __future__ import annotations

import base64
import pytest

from shared import archive_graph as ag


class FakeGC:
    """Minimal stand-in for GraphClient: records URLs and returns canned JSON."""
    def __init__(self, get_map=None, post_responses=None):
        self.get_map = get_map or {}
        self.post_responses = list(post_responses or [])
        self.get_calls = []
        self.post_calls = []

    async def _get(self, url, params=None):
        self.get_calls.append(url)
        # exact match first, then prefix (for nextLink chains)
        if url in self.get_map:
            return self.get_map[url]
        for k, v in self.get_map.items():
            if url.startswith(k):
                return v
        return {}

    async def _post(self, url, payload, headers=None):
        self.post_calls.append((url, payload))
        return self.post_responses.pop(0) if self.post_responses else {"value": []}


def test_base_uses_version_env(monkeypatch):
    monkeypatch.setattr(ag, "ARCHIVE_GRAPH_VERSION", "beta")
    assert ag._base() == "https://graph.microsoft.com/beta"
    assert ag._base("v1.0") == "https://graph.microsoft.com/v1.0"  # GA override


def test_batches_caps_at_20():
    ids = [f"id{i}" for i in range(45)]
    got = list(ag._batches(ids))
    assert [len(b) for b in got] == [20, 20, 5]
    assert got[0][0] == "id0" and got[-1][-1] == "id44"


@pytest.mark.asyncio
async def test_get_archive_mailbox_id_extracts_inplace_field():
    gc = FakeGC(get_map={
        "https://graph.microsoft.com/beta/users/U/settings/exchange":
            {"primaryMailboxId": "MBX:prim@t", "inPlaceArchiveMailboxId": "MBX:arch@t"},
    })
    assert await ag.get_archive_mailbox_id(gc, "U") == "MBX:arch@t"


@pytest.mark.asyncio
async def test_get_archive_mailbox_id_none_when_no_archive():
    gc = FakeGC(get_map={
        "https://graph.microsoft.com/beta/users/U/settings/exchange":
            {"primaryMailboxId": "MBX:prim@t"},  # no archive provisioned
    })
    assert await ag.get_archive_mailbox_id(gc, "U") is None


@pytest.mark.asyncio
async def test_list_folders_follows_nextlink():
    base = "https://graph.microsoft.com/beta/admin/exchange/mailboxes/M/folders"
    gc = FakeGC(get_map={
        base: {"value": [{"id": "f1"}], "@odata.nextLink": base + "?page=2"},
        base + "?page=2": {"value": [{"id": "f2"}]},
    })
    folders = await ag.list_archive_folders(gc, "M")
    assert [f["id"] for f in folders] == ["f1", "f2"]


@pytest.mark.asyncio
async def test_list_items_uses_plain_items_and_paginates():
    # NOT the delta endpoint (broken on beta) — plain /items with $select, paged.
    b = "https://graph.microsoft.com/beta/admin/exchange/mailboxes/M/folders/F/items"
    gc = FakeGC(get_map={
        b: {"value": [{"id": "i1", "changeKey": "k1"}], "@odata.nextLink": b + "?p=2"},
        b + "?p=2": {"value": [{"id": "i2", "changeKey": "k2"}]},
    })
    items = await ag.list_archive_items(gc, "M", "F")
    assert [i["id"] for i in items] == ["i1", "i2"]
    # first call hit /items with $select, never /items/delta
    assert gc.get_calls[0].startswith(b + "?$top=200&$select=")
    assert "/items/delta" not in gc.get_calls[0]


def test_select_changed_returns_new_and_changed_only():
    items = [
        {"id": "unchanged", "changeKey": "SAME"},
        {"id": "edited", "changeKey": "NEW"},
        {"id": "brandnew", "changeKey": "X"},
    ]
    prev = {"unchanged": "SAME", "edited": "OLD"}  # brandnew absent
    assert ag.select_changed(items, prev) == ["edited", "brandnew"]
    # first-ever run (no prior state) exports everything
    assert ag.select_changed(items, None) == ["unchanged", "edited", "brandnew"]
    # nothing changed → empty incremental
    assert ag.select_changed(items, {"unchanged": "SAME", "edited": "NEW", "brandnew": "X"}) == []


@pytest.mark.asyncio
async def test_export_items_batches_20_and_decodes_base64():
    payload = base64.b64encode(b"OPAQUE-BLOB-BYTES").decode()
    ids = [f"id{i}" for i in range(25)]
    # two POST responses: first 20, then 5
    gc = FakeGC(post_responses=[
        {"value": [{"itemId": i, "data": payload} for i in ids[:20]]},
        {"value": [{"itemId": i, "data": payload} for i in ids[20:]]},
    ])
    out = [x async for x in ag.export_items(gc, "M", ids)]
    assert len(gc.post_calls) == 2                       # batched into 20 + 5
    assert len(gc.post_calls[0][1]["itemIds"]) == 20
    assert len(out) == 25
    item_id, blob, err = out[0]
    assert item_id == "id0" and blob == b"OPAQUE-BLOB-BYTES" and err is None


@pytest.mark.asyncio
async def test_export_items_surfaces_per_item_error_without_blob():
    gc = FakeGC(post_responses=[{"value": [
        {"itemId": "ok", "data": base64.b64encode(b"x").decode()},
        {"itemId": "bad", "error": {"code": "ErrorArchiveFolderMovedPermanently"}},
    ]}])
    out = [x async for x in ag.export_items(gc, "M", ["ok", "bad"])]
    assert out[0] == ("ok", b"x", None)
    assert out[1][0] == "bad" and out[1][1] == b"" and out[1][2]["code"] == "ErrorArchiveFolderMovedPermanently"
