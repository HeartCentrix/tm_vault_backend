"""Snapshot-service catalogue browse over a chat overlay reads exactly what the same backup written
the copy way (every row its own) reads: inherited chats are part of the overlay's restore point."""
from __future__ import annotations

import pytest
from sqlalchemy import select

from test_chat_catalogue_postgres import store  # noqa: F401  (fixture)
from test_chat_catalogue_entry_postgres import source_adapter  # noqa: F401  (fixture)
from test_chat_catalogue_reads import load
from test_chat_overlay_postgres import overlay_and_twin
from shared import models

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def api():
    return load("overlay_browse_snapshot_api", "services/snapshot-service/main.py")


async def both(store, source_adapter, monkeypatch, read):
    """``read(db, snapshot)`` for the overlay and for its copy twin, snapshot ids normalised."""
    _, overlay, twin = await overlay_and_twin(store, source_adapter, monkeypatch)
    out = []
    for sid in (overlay, twin):
        async with store.factory() as db:
            out.append(norm(await read(db, sid), str(sid)))
    return out


def norm(value, sid):
    if isinstance(value, dict):
        return {k: norm(v, sid) for k, v in value.items() if k != "id"}
    if isinstance(value, (list, tuple)):
        return [norm(v, sid) for v in value]
    return value.replace(sid, "SNAP") if isinstance(value, str) else value


def item_key(item):
    return (item.external_id, item.parent_external_id, item.item_type, item.name, item.folder_path,
            item.content_size, repr(item.extra_data), item.is_deleted, str(item.snapshot_id))


async def test_overlay_browse_page_matches_copy(store, source_adapter, monkeypatch, api):
    async def read(db, sid):
        snapshot = await api._catalogue_snapshot(db, sid)
        items, total = await api._catalogue_page(db, snapshot, page=1, size=50)
        return {"total": total, "items": sorted(item_key(i) for i in items)}
    overlay, twin = await both(store, source_adapter, monkeypatch, read)
    assert overlay["total"] == 8 and overlay == twin


async def test_overlay_browse_groups_match_copy(store, source_adapter, monkeypatch, api):
    async def read(db, sid):
        return await api._catalogue_groups(db, await api._catalogue_snapshot(db, sid))
    overlay, twin = await both(store, source_adapter, monkeypatch, read)
    assert {g["chatId"]: g["count"] for g in overlay} == {"chat-alpha": 1, "chat-beta": 3, "chat-gamma": 3}
    assert overlay == twin


async def test_overlay_browse_folders_match_copy(store, source_adapter, monkeypatch, api):
    async def read(db, sid):
        return await api._catalogue_folders(db, await api._catalogue_snapshot(db, sid), page=1, size=50)
    overlay, twin = await both(store, source_adapter, monkeypatch, read)
    assert sum(f["count"] for f in overlay["content"]) == 7 and overlay == twin


async def test_overlay_browse_messages_match_copy(store, source_adapter, monkeypatch, api):
    async def read(db, sid):
        response = await api._catalogue_message_response(db, await api._catalogue_snapshot(db, sid), page=1, size=50)
        return response | {"content": sorted(response["content"], key=lambda m: m["externalId"])}
    overlay, twin = await both(store, source_adapter, monkeypatch, read)
    assert overlay["totalElements"] == 7 and overlay == twin
    assert {m["snapshotId"] for m in overlay["content"]} == {"SNAP"}


async def test_overlay_browse_messages_of_inherited_chat_match_copy(store, source_adapter, monkeypatch, api):
    # The chat_id filter also validates through shared.chat_presentation.chat_message_statement.
    async def read(db, sid):
        return await api._catalogue_message_response(db, await api._catalogue_snapshot(db, sid),
                                                     page=1, size=50, chat_id="chat-alpha")
    overlay, twin = await both(store, source_adapter, monkeypatch, read)
    assert overlay["totalElements"] == 1 and overlay == twin


async def test_overlay_browse_attachments_match_copy(store, source_adapter, monkeypatch, api):
    async def read(db, sid):
        rows, _ = await api._catalogue_page(db, await api._catalogue_snapshot(db, sid), page=1, size=50,
                                            messages_only=True)
        alpha = next(r for r in rows if (r.extra_data or {}).get("chatId") == "chat-alpha")
        # The page hands out the link row id (for the overlay: the base row it inherits).
        return await api.get_item_attachments(str(sid), str(alpha.id), db=db)
    overlay, twin = await both(store, source_adapter, monkeypatch, read)
    assert len(overlay) == 1 and overlay == twin
