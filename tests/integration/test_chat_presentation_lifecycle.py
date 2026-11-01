"""Names survive source retention and never adopt superseded observations."""
from uuid import uuid4

import pytest
from sqlalchemy import delete, select

from test_chat_catalogue_postgres import store
from test_chat_presentation import legacy, label_task, drain_labels
from shared import models, copy_workflow
from shared.copy_work import CopyRequest, enqueue

pytestmark = pytest.mark.integration


@pytest.mark.parametrize('prior_quality,provenance,expected', [
    (3, 'fallback', 'chats/Known'),
    (3, 'current_topic_cleared', 'chats/Fallback'),
    (2, 'current_topic_cleared', 'chats/Known'),
])
async def test_carried_labels_are_snapshot_owned_and_survive_source_deletion(store, prior_quality, provenance, expected):
    old = await legacy(store, [('chat-a', 'chats/Old row path', {})])
    task, publish = await label_task(store, old, [{'chat_id': 'chat-a', 'folder_path': 'chats/Known',
        'quality': prior_quality, 'provenance': 'current_topic' if prior_quality == 3 else 'participants'}], publication=True)
    await drain_labels(store, task, terminal=False)
    await drain_labels(store, publish)
    async with store.factory.begin() as db:
        (await db.get(models.Snapshot, old.snapshot_id)).status = models.SnapshotStatus.COMPLETED
    current = await legacy(store, [('chat-a', 'chats/Old row path', {})])
    async with store.factory.begin() as db:
        carry = await enqueue(db, CopyRequest('CHAT_CARRY', 'giant', current.snapshot_id,
            params={'prior_snapshot_id': str(old.snapshot_id)}))
        labels = await enqueue(db, CopyRequest('CHAT_LABELS', 'micro', current.snapshot_id,
            params={'labels': [{'chat_id': 'chat-a', 'folder_path': 'chats/Fallback',
                'quality': 0, 'provenance': provenance}]}))
        publish = await enqueue(db, CopyRequest('CHAT_PRESENTATION', 'giant', current.snapshot_id))
        (await db.get(models.CopyWorkflow, carry.workflow_id)).state = 'WAITING'
    for task in [carry, labels, publish]:
        await drain_labels(store, task, terminal=False)
    async with store.factory.begin() as db:
        workflow = await db.get(models.CopyWorkflow, carry.workflow_id)
        workflow.state, workflow.parent_settle_pending = 'COMPLETED', True
        snapshot = await db.get(models.Snapshot, current.snapshot_id)
        snapshot.status = models.SnapshotStatus.COMPLETED
        (await db.get(models.Job, snapshot.job_id)).status = models.JobStatus.COMPLETED
    assert await copy_workflow.finish_parent_settlement(store.factory, carry.workflow_id)
    async with store.factory.begin() as db:
        await db.execute(delete(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == old.snapshot_id))
        await db.execute(delete(models.Snapshot).where(models.Snapshot.id == old.snapshot_id))
        from shared.chat_presentation import captured_chat_path
        assert await captured_chat_path(db, current.snapshot_id, 'chat-a') == expected


async def test_equal_quality_overlapping_producers_keep_first_name_and_record_conflict(store):
    scope = await legacy(store, [('chat-a', 'chats/Fallback', {})])
    for index, name in enumerate(['Alpha', 'Beta']):
        pid = uuid4()
        async with store.factory.begin() as db:
            db.add(models.SnapshotPartition(id=pid, snapshot_id=scope.snapshot_id, tenant_id=scope.tenant_id,
                resource_id=scope.resource_id, job_id=(await db.get(models.Snapshot, scope.snapshot_id)).job_id,
                partition_type='CHATS', payload={'chat_ids': ['chat-a']}, partition_index=index,
                file_ids=[], total_files=0, total_bytes_est=0, status='IN_PROGRESS'))
        async with store.factory.begin() as db:
            task = await enqueue(db, CopyRequest('CHAT_LABELS', 'micro', scope.snapshot_id, partition_id=pid,
                params={'labels': [{'chat_id': 'chat-a', 'folder_path': 'chats/' + name,
                    'quality': 3, 'provenance': 'current_topic'}]}))
            (await db.get(models.CopyWorkflow, task.workflow_id)).state = 'WAITING'
        await drain_labels(store, task)
        async with store.factory.begin() as db:
            (await db.get(models.SnapshotPartition, pid)).status = 'COMPLETED'
    async with store.factory.begin() as db:
        task = await enqueue(db, CopyRequest('CHAT_PRESENTATION', 'giant', scope.snapshot_id))
        (await db.get(models.CopyWorkflow, task.workflow_id)).state = 'WAITING'
    await drain_labels(store, task)
    async with store.factory() as db:
        captured = await db.get(models.SnapshotChatPresentation, (scope.snapshot_id, 'chat-a'))
        assert captured.folder_path == 'chats/Alpha'
        assert captured.conflicted


async def test_replacement_capture_reopens_aborted_plan_and_excludes_old_drafts(store, monkeypatch):
    scope = await legacy(store, [('chat-a', 'chats/Stored', {})])
    task = await label_task(store, scope, [{'chat_id': 'chat-a', 'folder_path': 'chats/Stale',
        'quality': 3, 'provenance': 'current_topic'}])
    await drain_labels(store, task)
    async with store.factory.begin() as db:
        (await db.get(models.CopyWorkflow, task.workflow_id)).state = 'FAILED'
        (await db.get(models.ChatPresentationPlan, scope.snapshot_id)).state = 'ABORTED'
        (await db.get(models.Snapshot, scope.snapshot_id)).lease_token += 1
    monkeypatch.setattr(copy_workflow, 'async_session_factory', store.factory)
    monkeypatch.setenv('COPY_WORK_ENABLED', 'true')
    with copy_workflow.copy_collection(scope.snapshot_id):
        await copy_workflow.defer_current_copy('CHAT_LABELS', {'labels': [
            {'chat_id': 'chat-a', 'folder_path': 'chats/Replacement', 'quality': 0, 'provenance': 'fallback'}]}, lane='micro')
        await copy_workflow.seal_current_copy({'resume_kind': 'user_content'})
    async with store.factory() as db:
        tasks = list((await db.scalars(select(models.CopyWorkTask).where(
            models.CopyWorkTask.workflow_id == task.workflow_id, models.CopyWorkTask.capture_attempt == 1))).all())
    assert {item.kind for item in tasks} == {'CHAT_LABELS', 'CHAT_PRESENTATION'}
    for kind in ['CHAT_LABELS', 'CHAT_PRESENTATION']:
        await drain_labels(store, next(item for item in tasks if item.kind == kind), terminal=False)
    async with store.factory() as db:
        assert (await db.get(models.SnapshotChatPresentation, (scope.snapshot_id, 'chat-a'))).folder_path == 'chats/Stored'


@pytest.mark.parametrize('messages', [1, 1000])
async def test_name_manifest_size_depends_on_conversations_not_message_count(store, messages):
    from sqlalchemy import func
    scope = await legacy(store, [('chat-a', 'chats/Stored', {}) for _ in range(messages)])
    label, publish = await label_task(store, scope, [{'chat_id': 'chat-a', 'folder_path': 'chats/Resolved',
        'quality': 3, 'provenance': 'current_topic'}], publication=True)
    await drain_labels(store, label, terminal=False)
    await drain_labels(store, publish)
    async with store.factory() as db:
        assert await db.scalar(select(func.count()).select_from(models.CopyWorkItem).where(
            models.CopyWorkItem.task_id == publish.id)) == 1
        assert await db.scalar(select(func.count()).select_from(models.SnapshotItem).where(
            models.SnapshotItem.snapshot_id == scope.snapshot_id,
            models.SnapshotItem.folder_path == 'chats/Stored')) == messages
        assert (await db.get(models.ChatPresentationPlan, scope.snapshot_id)).expected_count == 1
        assert not list((await db.scalars(select(models.ChatPresentationDraft).where(
            models.ChatPresentationDraft.snapshot_id == scope.snapshot_id))).all())


async def test_published_marker_without_its_receipt_cannot_be_read_as_complete(store):
    from shared.chat_presentation import presentation_ready
    scope = await legacy(store, [('chat-a', 'chats/Stored', {})])
    async with store.factory.begin() as db:
        (await db.get(models.Snapshot, scope.snapshot_id)).extra_data = {'chat_presentation_format': 1}
    async with store.factory() as db:
        assert not await presentation_ready(db, scope.snapshot_id)


@pytest.mark.parametrize('provenance,expected', [('fallback', 'chats/Planning'),
    ('current_topic_cleared', 'chats/Untitled chat #1234')])
async def test_first_presentation_capture_preserves_a_known_unmarked_carried_name(store, provenance, expected):
    old = await legacy(store, [('chat-a', 'chats/Planning', {})])
    async with store.factory.begin() as db:
        (await db.get(models.Snapshot, old.snapshot_id)).status = models.SnapshotStatus.COMPLETED
    current = await legacy(store, [])
    async with store.factory.begin() as db:
        carry = await enqueue(db, CopyRequest('CHAT_CARRY', 'giant', current.snapshot_id,
            params={'prior_snapshot_id': str(old.snapshot_id)}))
        labels = await enqueue(db, CopyRequest('CHAT_LABELS', 'micro', current.snapshot_id,
            params={'labels': [{'chat_id': 'chat-a', 'folder_path': 'chats/Untitled chat #1234',
                'quality': 0, 'provenance': provenance}]}))
        publish = await enqueue(db, CopyRequest('CHAT_PRESENTATION', 'giant', current.snapshot_id))
        (await db.get(models.CopyWorkflow, carry.workflow_id)).state = 'WAITING'
    for task in [carry, labels, publish]:
        await drain_labels(store, task, terminal=False)
    async with store.factory() as db:
        assert (await db.get(models.SnapshotChatPresentation, (current.snapshot_id, 'chat-a'))).folder_path == expected
