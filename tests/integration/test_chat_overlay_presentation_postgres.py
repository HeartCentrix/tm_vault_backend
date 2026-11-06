"""Chat selection over overlay snapshots answers exactly as over the copy twin, including for the
chats the overlay inherits from its base (rows owned by the base snapshot)."""
from __future__ import annotations

import pytest

from test_chat_catalogue_postgres import store  # noqa: F401  (fixture)
from test_chat_catalogue_entry_postgres import source_adapter  # noqa: F401  (fixture)
from test_chat_overlay_postgres import overlay_and_twin

pytestmark = pytest.mark.integration
CHATS = ("chat-alpha", "chat-beta", "chat-gamma")


def shape(rows, snapshot_id):
    assert {r.snapshot_id for r in rows} == {snapshot_id}   # inherited rows read as the requested snapshot
    return sorted((r.item_type, r.external_id, r.parent_external_id, r.folder_path, r.chat_version_id,
                   (r.extra_data or {}).get("chat_id")) for r in rows)


async def test_overlay_chat_message_statement_matches_copy(store, source_adapter, monkeypatch):
    from shared.chat_presentation import captured_chat_path, chat_message_statement
    base, overlay, twin = await overlay_and_twin(store, source_adapter, monkeypatch)
    seen = {}
    async with store.factory() as session:
        for chat in CHATS:
            path = await captured_chat_path(session, overlay, chat)
            assert path == await captured_chat_path(session, twin, chat)
            for kwargs in ({"chat_id": chat}, {"chat_id": chat, "thread_path": path}):
                got = {}
                for sid in (overlay, twin):
                    stmt = await chat_message_statement(session, snapshot_ids=[sid], **kwargs)
                    rows = list((await session.scalars(stmt)).all())
                    # callers narrowing to ids must use the statement's own columns
                    assert set((await session.scalars(stmt.with_only_columns(
                        stmt.selected_columns.id))).all()) == {r.id for r in rows}
                    got[sid] = shape(rows, sid)
                assert got[overlay] == got[twin] and got[overlay], (chat, kwargs)
            seen[chat] = len(got[overlay])
    assert seen == {"chat-alpha": 1, "chat-beta": 3, "chat-gamma": 3}


async def test_overlay_chat_restore_items_matches_copy(store, source_adapter, monkeypatch):
    from shared.chat_presentation import chat_restore_items
    base, overlay, twin = await overlay_and_twin(store, source_adapter, monkeypatch)
    async with store.factory() as session:
        for chat in CHATS:
            got = {sid: shape(await chat_restore_items(session, snapshot_ids=[sid], chat_id=chat), sid)
                   for sid in (overlay, twin)}
            assert got[overlay] == got[twin] and got[overlay], chat
