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


async def label_task(store, scope, labels, *, publication=False):
    from shared.copy_work import CopyRequest, enqueue
    async with store.factory.begin() as db:
        task = await enqueue(db, CopyRequest(kind="CHAT_LABELS", lane="micro",
            target_snapshot_id=scope.snapshot_id, params={"labels": labels}))
        if publication:
            publish = await enqueue(db, CopyRequest(kind="CHAT_PRESENTATION", lane="giant",
                target_snapshot_id=scope.snapshot_id))
        workflow = await db.get(models.CopyWorkflow, task.workflow_id)
        workflow.state = "WAITING"
        return (task, publish) if publication else task


async def drain_labels(store, task, limit=2, *, terminal=True):
    from shared.copy_executor import prepare_manifest, copy_manifest_batch
    async with store.factory.begin() as db:
        task = await db.get(models.CopyWorkTask, task.id)
        await prepare_manifest(db, task)
    while True:
        async with store.factory.begin() as db:
            task = await db.get(models.CopyWorkTask, task.id)
            result = await copy_manifest_batch(db, task, limit=limit)
            task.checkpoint = result["cursor"]
            if result["done"]:
                task.state = "COMPLETED"
                workflow = await db.get(models.CopyWorkflow, task.workflow_id)
                if terminal:
                    workflow.state = "COMPLETED"
        if result["done"]:
            break


@pytest.mark.parametrize('migrated', [False, True])
async def test_label_work_changes_only_small_metadata_and_publishes_in_bounded_batches(store, migrated):
    from shared.chat_presentation import presentation_ready, list_chat_presentations
    from shared.copy_executor import prepare_manifest, copy_manifest_batch
    if migrated:
        import importlib.util
        from pathlib import Path
        from alembic.migration import MigrationContext
        from alembic.operations import Operations
        from sqlalchemy import text
        spec = importlib.util.spec_from_file_location('presentation_migration',
            Path(__file__).resolve().parents[2] / 'alembic/versions/0013_chat_presentation.py')
        migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(migration)
        def apply(conn):
            with Operations.context(MigrationContext.configure(conn)):
                migration.upgrade()
                migration.upgrade()
        async with store.engine.begin() as conn:
            for table in ['chat_presentation_drafts', 'snapshot_chat_presentations', 'chat_presentation_plans']:
                await conn.execute(text('DROP TABLE ' + table))
            await conn.run_sync(apply)
    scope = await legacy(store, [(f"chat-{i}", "chats/Fallback", {}) for i in range(3)])
    labels = [{"chat_id": f"chat-{i}", "folder_path": f"chats/Captured {i}",
        "quality": 3, "provenance": "current_topic"} for i in range(3)]
    task, publish = await label_task(store, scope, labels, publication=True)
    await drain_labels(store, task, terminal=False)
    async with store.factory.begin() as db:
        current = await db.get(models.CopyWorkTask, publish.id)
        await prepare_manifest(db, current)
        batch = await copy_manifest_batch(db, current, limit=2)
        assert not batch["done"]
        current.checkpoint = batch["cursor"]
        assert not await presentation_ready(db, scope.snapshot_id)
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        assert not (snapshot.extra_data or {}).get("chat_presentation_format")
    async with store.factory.begin() as db:
        current = await db.get(models.CopyWorkTask, publish.id)
        batch = await copy_manifest_batch(db, current, limit=2)
        assert batch["done"]
        current.checkpoint = batch["cursor"]
        current.state = "COMPLETED"
        assert await presentation_ready(db, scope.snapshot_id)
        assert (await db.get(models.Snapshot, scope.snapshot_id)).extra_data["chat_presentation_format"] == 1
        groups = await list_chat_presentations(db, scope.snapshot_id)
        assert {g["displayName"] for g in groups} == {f"Captured {i}" for i in range(3)}
        assert set((await db.scalars(select(models.SnapshotItem.folder_path).where(
            models.SnapshotItem.snapshot_id == scope.snapshot_id))).all()) == {"chats/Fallback"}
        assert await presentation_ready(db, scope.snapshot_id)


async def test_large_label_inputs_stay_durable_and_stale_capture_cannot_write(store):
    from shared.copy_executor import prepare_manifest, copy_manifest_batch
    scope = await legacy(store, [])
    labels = [{"chat_id": f"chat-{i:04}", "folder_path": f"chats/Name {i}",
        "quality": 2, "provenance": "participants"} for i in range(300)]
    task = await label_task(store, scope, labels)
    async with store.factory.begin() as db:
        current = await db.get(models.CopyWorkTask, task.id)
        assert current.params["labels"] == []
        assert len(list((await db.scalars(select(models.CopyWorkInput).where(
            models.CopyWorkInput.task_id == task.id))).all())) == 300
        await prepare_manifest(db, current)
        workflow = await db.get(models.CopyWorkflow, task.workflow_id)
        workflow.capture_attempt += 1
    async with store.factory.begin() as db:
        current = await db.get(models.CopyWorkTask, task.id)
        with pytest.raises(ValueError, match="capture"):
            await copy_manifest_batch(db, current, limit=5)


async def test_publication_waits_for_other_sources_and_snapshot_deletion_cascades_labels(store):
    from shared.chat_presentation import presentation_ready
    from shared.copy_executor import prepare_manifest
    from sqlalchemy import delete
    scope = await legacy(store, [("chat-a", "chats/Old", {})])
    task, publish = await label_task(store, scope, [{"chat_id": "chat-a", "folder_path": "chats/New",
        "quality": 3, "provenance": "current_topic"}], publication=True)
    async with store.factory.begin() as db:
        assert not await presentation_ready(db, scope.snapshot_id)
        with pytest.raises(ValueError, match="source work"):
            await prepare_manifest(db, await db.get(models.CopyWorkTask, publish.id))
    await drain_labels(store, task, terminal=False)
    await drain_labels(store, publish)
    async with store.factory.begin() as db:
        assert await presentation_ready(db, scope.snapshot_id)
        await db.execute(delete(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == scope.snapshot_id))
        await db.execute(delete(models.Snapshot).where(models.Snapshot.id == scope.snapshot_id))
    async with store.factory.begin() as db:
        assert not list((await db.scalars(select(models.SnapshotChatPresentation).where(
            models.SnapshotChatPresentation.snapshot_id == scope.snapshot_id))).all())


async def test_scheduler_gives_source_labels_a_turn_before_publication(store):
    from shared.copy_work import claim
    scope = await legacy(store, [("chat-a", "chats/Old", {})])
    task, publish = await label_task(store, scope, [{"chat_id": "chat-a", "folder_path": "chats/New",
        "quality": 3, "provenance": "current_topic"}], publication=True)
    async with store.factory.begin() as db:
        picked = await claim(db, owner_id=uuid4())
        assert picked.task_id == task.id
    await drain_labels(store, task, terminal=False)
    async with store.factory.begin() as db:
        picked = await claim(db, owner_id=uuid4())
        assert picked.task_id == publish.id
