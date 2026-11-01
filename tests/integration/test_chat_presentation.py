"""Captured presentation and identity selection against disposable PostgreSQL."""
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import select

from test_chat_catalogue_postgres import store, api, message
from test_chat_catalogue_reads import snapshot_api, export_api, export_scope
from test_chat_catalogue_generic_exports import restore_api
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
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as ambiguous:
            await snapshot_api.list_snapshot_messages(str(scope.snapshot_id), db=db,
                page=1, size=50, chatId=None, folder='chats/Planning', search=None, presentation=True)
        assert ambiguous.value.detail == {'error': 'AMBIGUOUS_CHAT_PATH'}
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


async def test_identity_export_and_estimate_agree_across_legacy_path_aliases(store, export_api, export_scope):
    scope = await legacy(store, [('chat-a', 'chats/Old', {}), ('chat-a', 'chats/New', {}),
        ('chat-b', 'chats/New', {})])
    async with store.factory.begin() as db:
        count, _, layout, _ = await export_api._resolve_counts(db, resource_id=str(scope.resource_id),
            snapshot_ids=[str(scope.snapshot_id)], thread_path=None, item_ids=[], chat_id='chat-a')
        selected = await export_scope.resolve(db, resource_id=scope.resource_id,
            snapshot_ids=[scope.snapshot_id], thread_path=None, item_ids=[], chat_id='chat-a')
        assert count == len(selected.messages) == 2
        assert layout == selected.layout == 'single_thread'
        legacy_subset = await export_scope.resolve(db, resource_id=scope.resource_id,
            snapshot_ids=[scope.snapshot_id], thread_path='chats/Old', item_ids=[])
        assert len(legacy_subset.messages) == 1


async def test_catalogue_identity_exports_one_of_two_equal_titles(store, export_api, export_scope):
    scope = await store.new_scope()
    for chat_id in ['chat-a', 'chat-b']:
        current = replace(scope, chat_id=chat_id)
        async with store.factory.begin() as db:
            lease = await api().begin_capture(db, scope=current, folder_path='chats/Same')
        items = [message()]
        await store.stage(lease, items)
        await store.observe(current, lease, items)
        await store.publish(lease)
    async with store.factory.begin() as db:
        count, _, _, _ = await export_api._resolve_counts(db, resource_id=str(scope.resource_id),
            snapshot_ids=[str(scope.snapshot_id)], thread_path=None, item_ids=[], chat_id='chat-a')
        selected = await export_scope.resolve(db, resource_id=scope.resource_id,
            snapshot_ids=[scope.snapshot_id], thread_path=None, item_ids=[], chat_id='chat-a')
        assert count == len(selected.messages) == 1
        assert selected.messages[0].extra_data['chatId'] == 'chat-a'


async def test_legacy_children_with_a_colliding_message_id_remain_chat_qualified(store, export_api, export_scope):
    scope = await legacy(store, [('chat-a', 'chats/Same', {})])
    other = await legacy(store, [('chat-b', 'chats/Same', {})])
    async with store.factory.begin() as db:
        for captured, size in [(scope, 3), (other, 9)]:
            db.add(models.SnapshotItem(snapshot_id=captured.snapshot_id, tenant_id=captured.tenant_id,
                backend_id=captured.backend_id, external_id='attachment', parent_external_id='message-0',
                item_type='CHAT_ATTACHMENT', name=f'{captured.chat_id}.pdf', content_size=size,
                blob_path=f'objects/{size}', extra_data={'chat_id': 'chat-a' if captured == scope else 'chat-b'}))
    async with store.factory.begin() as db:
        count, size, _, _ = await export_api._resolve_counts(db, resource_id=str(scope.resource_id),
            snapshot_ids=[str(scope.snapshot_id)], thread_path=None, item_ids=[], chat_id='chat-a')
        selected = await export_scope.resolve(db, resource_id=scope.resource_id,
            snapshot_ids=[scope.snapshot_id], thread_path=None, item_ids=[], chat_id='chat-a')
        assert count == len(selected.messages) == 1
        assert size == 3
        assert [a['size'] for a in selected.attachment_map['message-0']] == [3]


async def test_published_legacy_message_page_uses_captured_label_and_exact_identity(store, snapshot_api):
    scope = await legacy(store, [('chat-a', 'chats/Old', {}), ('chat-a', 'chats/Other alias', {}),
        ('chat-b', 'chats/Old', {})])
    labels = [{'chat_id': chat_id, 'folder_path': f'chats/Published {chat_id}', 'quality': 3,
        'provenance': 'current_topic'} for chat_id in ['chat-a', 'chat-b']]
    task, publish = await label_task(store, scope, labels, publication=True)
    await drain_labels(store, task, terminal=False)
    await drain_labels(store, publish)
    async with store.factory.begin() as db:
        thread = models.ChatThread(tenant_id=scope.tenant_id, chat_id='chat-a', chat_topic='Mutable latest name')
        db.add(thread)
        await db.flush()
        db.add(models.ChatThreadMessage(chat_thread_id=thread.id, message_external_id='message-0',
            body_content='Captured pointer body', metadata_raw={'id': 'message-0',
                'body': {'content': 'Captured pointer body', 'contentType': 'text'}}))
        db.add(models.ChatThreadMessage(chat_thread_id=thread.id, message_external_id='not-in-this-backup',
            body_content='A later message'))
    async with store.factory.begin() as db:
        result = await snapshot_api.list_snapshot_messages(str(scope.snapshot_id), page=1, size=50,
            chatId='chat-a', folder='chats/Published chat-a', search=None, db=db)
        assert result['totalElements'] == 2
        assert {row['chatId'] for row in result['content']} == {'chat-a'}
        assert {row['chatTopic'] for row in result['content']} == {'Published chat-a'}
        assert 'not-in-this-backup' not in {row['externalId'] for row in result['content']}
        assert next(row for row in result['content'] if row['externalId'] == 'message-0')['body'] == 'Captured pointer body'
        raw_paths = list((await db.scalars(select(models.SnapshotItem.folder_path).where(
            models.SnapshotItem.snapshot_id == scope.snapshot_id))).all())
        assert 'chats/Published chat-a' not in raw_paths


async def test_legacy_child_without_metadata_uses_qualified_parent_not_its_title(store, export_scope):
    scope = await legacy(store, [('chat-a', 'chats/One', {})])
    async with store.factory.begin() as db:
        db.add(models.SnapshotItem(snapshot_id=scope.snapshot_id, tenant_id=None, backend_id=scope.backend_id,
            external_id='inline', parent_external_id='message-0', item_type='CHAT_HOSTED_CONTENT',
            name='inline.png', content_size=3, blob_path='objects/inline', extra_data={}))
    async with store.factory.begin() as db:
        selected = await export_scope.resolve(db, resource_id=scope.resource_id,
            snapshot_ids=[scope.snapshot_id], thread_path=None, item_ids=[], chat_id='chat-a')
        assert selected.hosted_map['message-0'][0]['blob_path'] == 'objects/inline'


@pytest.mark.parametrize('catalogue', [False, True])
async def test_generic_restore_selection_uses_chat_identity_for_equal_titles(store, restore_api, catalogue):
    if catalogue:
        scope = await store.new_scope()
        for chat_id in ['chat-a', 'chat-b']:
            current = replace(scope, chat_id=chat_id)
            async with store.factory.begin() as db:
                lease = await api().begin_capture(db, scope=current, folder_path='chats/Same')
            items = [message()]
            await store.stage(lease, items)
            await store.observe(current, lease, items)
            await store.publish(lease)
    else:
        scope = await legacy(store, [('chat-a', 'chats/Same', {}), ('chat-b', 'chats/Same', {})])
    async with store.factory.begin() as db:
        (await db.get(models.Snapshot, scope.snapshot_id)).status = models.SnapshotStatus.COMPLETED
    instance = restore_api.RestoreWorker.__new__(restore_api.RestoreWorker)
    async with store.factory.begin() as db:
        selected = await instance.fetch_snapshot_items(db, [str(scope.snapshot_id)], [], chat_id='chat-a')
        assert len(selected) == 1
        from shared.chat_presentation import item_chat_id
        assert item_chat_id(selected[0]) == 'chat-a'


@pytest.mark.parametrize('operation', ['trigger_restore', 'trigger_export'])
async def test_generic_job_persists_the_chat_selector_instead_of_whole_snapshot_scope(store, monkeypatch, operation):
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock
    from uuid import UUID
    from types import SimpleNamespace
    import httpx
    from test_chat_catalogue_reads import load
    module = load('presentation_jobs_' + operation, 'services/job-service/main.py')
    scope = await legacy(store, [('chat-a', 'chats/Same', {}), ('chat-b', 'chats/Same', {})])
    async with store.factory() as db:
        resource = await db.get(models.Resource, scope.resource_id)
    monkeypatch.setattr(module, 'authorize_export_scope', AsyncMock(return_value={resource.id: resource}))
    monkeypatch.setattr(module, 'authorize_restore_destinations', AsyncMock(return_value={}))
    monkeypatch.setattr(module.settings, 'RABBITMQ_ENABLED', False)
    @asynccontextmanager
    async def audit_client(**kwargs):
        yield SimpleNamespace(post=AsyncMock(return_value=SimpleNamespace(status_code=200)))
    monkeypatch.setattr(httpx, 'AsyncClient', audit_client)
    async with store.factory() as db:
        result = await getattr(module, operation)({'snapshotIds': [str(scope.snapshot_id)], 'itemIds': [],
            'selectionScope': 'SELECTED', 'chatId': 'chat-a', 'restoreType': 'EXPORT_ZIP'}, db=db,
            user={'id': str(uuid4()), 'email': 'synthetic@example.invalid', 'roles': ['SUPER_ADMIN']})
        job = await db.get(models.Job, UUID(result['jobId']))
        assert job.spec['chatId'] == 'chat-a'
        assert job.spec.get('item_ids', []) == []


async def test_conversation_pagination_keeps_identical_titles_as_distinct_rows(store, snapshot_api):
    scope = await legacy(store, [('chat-a', 'chats/Same', {}), ('chat-b', 'chats/Same', {})])
    async with store.factory.begin() as db:
        first = await snapshot_api.list_snapshot_chat_groups(str(scope.snapshot_id), db=db, page=1, size=1)
        second = await snapshot_api.list_snapshot_chat_groups(str(scope.snapshot_id), db=db, page=2, size=1)
        assert first['hasMore'] and not second['hasMore']
        assert first['total'] == second['total'] == 2
        assert first['content'][0]['chatId'] != second['content'][0]['chatId']
        assert first['content'][0]['displayName'] == second['content'][0]['displayName'] == 'Same'


async def test_catalogue_snapshots_reject_a_second_legacy_label_representation(store):
    from shared.copy_work import CopyRequest, CopyWorkConflict, enqueue
    scope = await store.new_scope()
    async with store.factory.begin() as db:
        with pytest.raises(CopyWorkConflict, match='catalogue'):
            await enqueue(db, CopyRequest('CHAT_LABELS', 'micro', scope.snapshot_id, params={'labels': []}))


async def test_generic_chat_identity_rejects_an_in_progress_restore_point(store, restore_api):
    scope = await legacy(store, [('chat-a', 'chats/Stored', {})])
    instance = restore_api.RestoreWorker.__new__(restore_api.RestoreWorker)
    async with store.factory() as db:
        with pytest.raises(ValueError, match='restore point'):
            await instance.fetch_snapshot_items(db, [str(scope.snapshot_id)], [], chat_id='chat-a')


async def test_generic_chat_identity_hydrates_thin_pointers_without_changing_stored_rows(store, restore_api):
    scope = await legacy(store, [('chat-a', 'chats/Stored', {})])
    async with store.factory.begin() as db:
        (await db.get(models.Snapshot, scope.snapshot_id)).status = models.SnapshotStatus.COMPLETED
        thread = models.ChatThread(tenant_id=scope.tenant_id, chat_id='chat-a')
        db.add(thread)
        await db.flush()
        db.add(models.ChatThreadMessage(chat_thread_id=thread.id, message_external_id='message-0',
            metadata_raw={'id': 'message-0', 'body': {'content': 'Saved message', 'contentType': 'text'}}))
    instance = restore_api.RestoreWorker.__new__(restore_api.RestoreWorker)
    async with store.factory.begin() as db:
        rows = await instance.fetch_snapshot_items(db, [str(scope.snapshot_id)], [], chat_id='chat-a')
        assert rows[0].extra_data['raw']['body']['content'] == 'Saved message'
    async with store.factory() as db:
        assert await db.scalar(select(models.SnapshotItem.extra_data).where(
            models.SnapshotItem.snapshot_id == scope.snapshot_id)) == {}


async def test_unknown_competing_parent_cannot_prove_a_legacy_childs_chat(store, export_api, export_scope):
    from fastapi import HTTPException
    from shared.chat_presentation import ChatSelectionError
    scope = await legacy(store, [('chat-a', 'chats/Same', {})])
    async with store.factory.begin() as db:
        db.add_all([
            models.SnapshotItem(snapshot_id=scope.snapshot_id, tenant_id=scope.tenant_id,
                backend_id=scope.backend_id, external_id='message-0', parent_external_id='unknown-message',
                item_type='TEAMS_MESSAGE_REPLY', name='Unknown reply', extra_data={}),
            models.SnapshotItem(snapshot_id=scope.snapshot_id, tenant_id=scope.tenant_id,
                backend_id=scope.backend_id, external_id='child', parent_external_id='message-0',
                item_type='CHAT_ATTACHMENT', name='file.pdf', content_size=7, blob_path='objects/file', extra_data={})])
    async with store.factory() as db:
        with pytest.raises(HTTPException) as estimate:
            await export_api._resolve_counts(db, resource_id=str(scope.resource_id),
                snapshot_ids=[str(scope.snapshot_id)], thread_path=None, item_ids=[], chat_id='chat-a')
        assert estimate.value.detail == {'error': 'CHAT_CHILD_IDENTITY_UNAVAILABLE'}
        with pytest.raises(ChatSelectionError, match='CHAT_CHILD_IDENTITY_UNAVAILABLE'):
            await export_scope.resolve(db, resource_id=scope.resource_id,
                snapshot_ids=[scope.snapshot_id], thread_path=None, item_ids=[], chat_id='chat-a')
