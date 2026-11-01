"""Captured presentation and identity selection against disposable PostgreSQL."""
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import select

from test_chat_catalogue_postgres import store, api, message
from test_chat_catalogue_reads import snapshot_api
from shared import models

pytestmark = pytest.mark.integration


async def legacy(store, rows):
    scope = await store.new_scope()
    async with store.factory.begin() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        snapshot.extra_data = {}
        for index, (chat_id, path, metadata) in enumerate(rows):
            db.add(models.SnapshotItem(snapshot_id=scope.snapshot_id, tenant_id=scope.tenant_id,
                backend_id=scope.backend_id, external_id=f"message-{index}",
                parent_external_id=chat_id, item_type="TEAMS_CHAT_MESSAGE", name="Message",
                folder_path=path, extra_data=metadata))
    return scope


async def test_legacy_groups_use_the_snapshots_stored_names_and_keep_equal_names_separate(store, snapshot_api):
    scope = await legacy(store, [("chat-a", "chats/Planning", {}),
        ("chat-b", "chats/Planning", {}), ("chat-a", "chats/Before", {})])
    async with store.factory.begin() as db:
        db.add(models.ChatThread(tenant_id=scope.tenant_id, chat_id="chat-a", chat_topic="Today's different name"))
    async with store.factory.begin() as db:
        groups = await snapshot_api.list_snapshot_chat_groups(str(scope.snapshot_id), db=db)
        by_id = {group["chatId"]: group for group in groups}
        assert set(by_id) == {"chat-a", "chat-b"}
        assert by_id["chat-a"]["displayName"] == "Planning"
        assert by_id["chat-a"]["folderAliases"] == ["chats/Before", "chats/Planning"]
        assert by_id["chat-a"]["presentationSource"] == "legacy_mixed"
        assert by_id["chat-a"]["count"] == 2
        assert by_id["chat-b"]["displayName"] == "Planning"


async def test_identity_selects_all_aliases_but_old_path_selects_its_original_subset(store):
    from shared.chat_presentation import chat_message_statement, ChatSelectionError
    scope = await legacy(store, [("chat-a", "chats/Old", {}),
        ("chat-a", "chats/New", {"chat_id": "chat-a"}),
        ("chat-b", "chats/New", {"chatId": "chat-b"})])
    async with store.factory.begin() as db:
        async def selected(**kwargs):
            stmt = await chat_message_statement(db, snapshot_ids=[scope.snapshot_id], **kwargs)
            return list((await db.scalars(stmt)).all())
        assert len(await selected(chat_id="chat-a")) == 2
        assert len(await selected(thread_path="chats/Old")) == 1
        assert len(await selected(chat_id="chat-a", thread_path="chats/Old")) == 2
        with pytest.raises(ChatSelectionError, match="AMBIGUOUS_CHAT_PATH"):
            await selected(thread_path="chats/New")
        with pytest.raises(ChatSelectionError, match="CHAT_ALIAS_MISMATCH"):
            await selected(chat_id="chat-b", thread_path="chats/Old")


async def test_identity_requires_presence_in_every_requested_snapshot_and_preserves_item_subsets(store):
    from shared.chat_presentation import chat_message_statement, ChatSelectionError
    scope = await legacy(store, [("chat-a", None, {}), ("chat-a", "chats/Current", {})])
    other = await legacy(store, [("chat-b", "chats/Current", {})])
    async with store.factory.begin() as db:
        row = await db.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == scope.snapshot_id))
        stmt = await chat_message_statement(db, snapshot_ids=[scope.snapshot_id], item_ids=[row.id])
        assert len(list((await db.scalars(stmt)).all())) == 1
        with pytest.raises(ChatSelectionError, match="CHAT_NOT_IN_SNAPSHOT"):
            await chat_message_statement(db, snapshot_ids=[scope.snapshot_id, other.snapshot_id], chat_id="chat-a")


async def test_catalogue_equal_titles_keep_separate_chat_identities(store, snapshot_api):
    scope = await store.new_scope()
    for chat_id in ["chat-a", "chat-b"]:
        current = replace(scope, chat_id=chat_id)
        async with store.factory.begin() as db:
            lease = await api().begin_capture(db, scope=current, folder_path="chats/Planning")
        items = [message(graph_id="same-message-id")]
        await store.stage(lease, items)
        await store.observe(current, lease, items)
        await store.publish(lease)
    async with store.factory.begin() as db:
        groups = await snapshot_api.list_snapshot_chat_groups(str(scope.snapshot_id), db=db)
        assert {g["chatId"] for g in groups} == {"chat-a", "chat-b"}
        from shared.chat_presentation import chat_message_statement, ChatSelectionError
        with pytest.raises(ChatSelectionError, match="AMBIGUOUS_CHAT_PATH"):
            await chat_message_statement(db, snapshot_ids=[scope.snapshot_id], thread_path="chats/Planning")
        stmt = await chat_message_statement(db, snapshot_ids=[scope.snapshot_id], chat_id="chat-a")
        rows = list((await db.scalars(stmt)).all())
        assert len(rows) == 1 and rows[0].extra_data["chat_id"] == "chat-a"
