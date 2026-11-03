"""Required sources are fixed and pinned before a copy intent can wait."""
import asyncio
from datetime import datetime

import pytest
from sqlalchemy import delete, func, select, text

from test_chat_catalogue_postgres import store
from shared import copy_executor, copy_work, copy_workflow, models
from shared.retention_cleanup import _delete_snapshots

pytestmark = pytest.mark.integration


async def source(store, *, day=1, kind='TEAMS_CHAT_MESSAGE', external='message', metadata=None,
                 name='selected', parent=None):
    scope = await store.new_scope()
    async with store.factory.begin() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        snapshot.status = models.SnapshotStatus.COMPLETED
        snapshot.created_at = datetime(2026, 1, day)
        snapshot.item_count, snapshot.bytes_total = 1, 7
        db.add(models.SnapshotItem(snapshot_id=snapshot.id, tenant_id=store.tenant,
            item_type=kind, external_id=external, parent_external_id=parent, name=name,
            content_size=7, backend_id=store.backend, extra_data=metadata or {}))
    return scope.snapshot_id


async def target(store):
    scope = await store.new_scope()
    async with store.factory.begin() as db:
        (await db.get(models.Snapshot, scope.snapshot_id)).created_at = datetime(2026, 1, 20)
    return scope.snapshot_id


async def enqueue(store, sid, kind='CHAT_CARRY', **params):
    async with store.factory.begin() as db:
        return await copy_work.enqueue(db, copy_work.CopyRequest(kind=kind,
            lane='micro' if kind == 'LEGACY_CHAT_ATTACHMENTS' else 'giant',
            target_snapshot_id=sid, params=params))


async def apply(store, task_id):
    async with store.factory.begin() as db:
        task = await db.get(models.CopyWorkTask, task_id)
        await copy_executor.prepare_manifest(db, task)
    while True:
        async with store.factory.begin() as db:
            task = await db.get(models.CopyWorkTask, task_id)
            batch = await copy_executor.copy_manifest_batch(db, task, limit=50)
            task.checkpoint = batch['cursor']
        if batch['done']:
            return


@pytest.mark.parametrize(('kind', 'item_type'), [('CHAT_CARRY', 'TEAMS_CHAT_MESSAGE'), ('MAIL_CARRY', 'EMAIL')])
async def test_implicit_predecessor_is_pinned_before_first_prepare_and_retention(store, kind, item_type):
    await source(store, day=1, kind=item_type, name='older')
    chosen = await source(store, day=2, kind=item_type)
    sid = await target(store)
    task = await enqueue(store, sid, kind)
    assert task.params.get('prior_snapshot_id') == str(chosen)
    assert not task.prepared
    await copy_workflow.initialize_copy_guards(store.factory)
    async with store.factory.begin() as db:
        pins = (await db.scalars(select(models.CopyWorkItem.source_snapshot_id)
            .where(models.CopyWorkItem.task_id == task.id, models.CopyWorkItem.ordinal < 0))).all()
        assert pins == [chosen]
        assert await _delete_snapshots(db, {chosen}) == 0
    await source(store, day=3, kind=item_type, name='too late')
    await apply(store, task.id)
    async with store.factory() as db:
        assert list(await db.scalars(select(models.SnapshotItem.name)
            .where(models.SnapshotItem.snapshot_id == sid))) == ['selected']


async def test_lost_fixed_source_never_falls_back_to_older_or_empty_history(store):
    await source(store, day=1, name='older')
    chosen = await source(store, day=2)
    sid = await target(store)
    task = await enqueue(store, sid)
    # Simulate externally corrupted/deleted pins: even this must not turn
    # source loss into a successful copy of an older or empty predecessor.
    async with store.factory.begin() as db:
        await db.execute(delete(models.CopyWorkItem).where(models.CopyWorkItem.task_id == task.id))
        await db.execute(delete(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == chosen))
        await db.execute(delete(models.Snapshot).where(models.Snapshot.id == chosen))
    with pytest.raises(ValueError, match='source'):
        await apply(store, task.id)


@pytest.mark.parametrize('kind', ['CHAT_CARRY', 'MAIL_CARRY'])
async def test_verified_no_predecessor_does_not_choose_history_that_arrives_later(store, kind):
    sid = await target(store)
    task = await enqueue(store, sid, kind)
    await source(store, kind='EMAIL' if kind == 'MAIL_CARRY' else 'TEAMS_CHAT_MESSAGE')
    await apply(store, task.id)
    async with store.factory() as db:
        assert await db.scalar(select(func.count()).select_from(models.SnapshotItem)
            .where(models.SnapshotItem.snapshot_id == sid)) == 0
        saved = await db.get(models.CopyWorkTask, task.id)
        assert saved.params['prior_snapshot_id'] is None


async def test_same_workflow_tasks_share_intent_time_source_before_any_preparation(store):
    chosen = await source(store)
    sid = await target(store)
    first = await enqueue(store, sid, selector_label='one')
    await source(store, day=2, name='newer')
    second = await enqueue(store, sid, selector_label='two')
    assert first.params.get('prior_snapshot_id') == second.params.get('prior_snapshot_id') == str(chosen)
    again = await enqueue(store, sid, **first.params)
    assert again.id == first.id


async def test_new_capture_attempt_does_not_inherit_old_intent_source(store):
    chosen = await source(store)
    sid = await target(store)
    first = await enqueue(store, sid)
    assert first.params.get('prior_snapshot_id') == str(chosen)
    await copy_workflow.initialize_copy_guards(store.factory)
    async with store.factory.begin() as db:
        await copy_workflow.cancel_copy_work(db, workflow_id=first.workflow_id, terminal_state='FAILED')
        (await db.get(models.Snapshot, sid)).lease_token += 1
    newer = await source(store, day=2, name='new attempt')
    second = await enqueue(store, sid)
    assert second.capture_attempt > first.capture_attempt
    assert second.params['prior_snapshot_id'] == str(newer)


async def chat_membership(store):
    async with store.factory.begin() as db:
        thread = models.ChatThread(tenant_id=store.tenant, chat_id='chat-a')
        db.add(thread)
        await db.flush()
        db.add(models.ChatThreadMessage(chat_thread_id=thread.id, message_external_id='m1'))


async def test_p8_candidate_snapshots_are_pinned_without_copying_payloads_until_prepare(store):
    await chat_membership(store)
    chosen = await source(store, kind='CHAT_ATTACHMENT', external='m1::attachment', parent='m1',
                          metadata={'chat_id': 'chat-a', 'large': 'x' * 100000})
    sid = await target(store)
    task = await enqueue(store, sid, 'LEGACY_CHAT_ATTACHMENTS', chat_id='chat-a')
    await copy_workflow.initialize_copy_guards(store.factory)
    async with store.factory.begin() as db:
        pins = list(await db.scalars(select(models.CopyWorkItem).where(models.CopyWorkItem.task_id == task.id)))
        assert len(pins) == 1 and pins[0].ordinal < 0
        assert pins[0].source_snapshot_id == chosen and pins[0].payload == {'copy_source_pin': True}
        assert await _delete_snapshots(db, {chosen}) == 0
    await source(store, day=2, kind='CHAT_ATTACHMENT', external='m1::attachment', parent='m1',
                 metadata={'chat_id': 'chat-a'}, name='not in source intent')
    await apply(store, task.id)
    async with store.factory() as db:
        assert list(await db.scalars(select(models.SnapshotItem.name)
            .where(models.SnapshotItem.snapshot_id == sid))) == ['selected']


async def test_p8_initial_empty_candidate_set_does_not_expand_after_wait(store):
    await chat_membership(store)
    sid = await target(store)
    task = await enqueue(store, sid, 'LEGACY_CHAT_ATTACHMENTS', chat_id='chat-a')
    await source(store, kind='CHAT_ATTACHMENT', external='m1::attachment', parent='m1', metadata={'chat_id': 'chat-a'})
    await apply(store, task.id)
    async with store.factory() as db:
        assert await db.scalar(select(func.count()).select_from(models.SnapshotItem)
            .where(models.SnapshotItem.snapshot_id == sid)) == 0


async def test_p8_intent_refuses_candidate_already_claimed_for_deletion(store):
    await chat_membership(store)
    doomed = await source(store, kind='CHAT_ATTACHMENT', external='m1::attachment', parent='m1', metadata={'chat_id': 'chat-a'})
    sid = await target(store)
    async with store.factory.begin() as db:
        (await db.get(models.Snapshot, doomed)).status = models.SnapshotStatus.PENDING_DELETION
    with pytest.raises(ValueError, match='source'):
        await enqueue(store, sid, 'LEGACY_CHAT_ATTACHMENTS', chat_id='chat-a')


async def wait_for_row_lock(store, operation):
    for _ in range(100):
        assert not operation.done(), 'intent registration did not serialize on the source/workflow lock'
        async with store.factory() as db:
            if await db.scalar(text("SELECT EXISTS(SELECT 1 FROM pg_stat_activity "
                "WHERE application_name=:app AND wait_event_type='Lock')"), {'app': store.schema}):
                return
        await asyncio.sleep(0.01)
    pytest.fail('intent registration did not reach the expected PostgreSQL row lock')


async def test_retention_claim_racing_source_registration_never_causes_older_fallback(store):
    await source(store, day=1, name='older')
    chosen = await source(store, day=2)
    sid = await target(store)
    async with store.factory() as holder:
        snapshot = await holder.get(models.Snapshot, chosen, with_for_update=True)
        snapshot.status = models.SnapshotStatus.PENDING_DELETION
        await holder.flush()
        operation = asyncio.create_task(enqueue(store, sid))
        try:
            await wait_for_row_lock(store, operation)
            await holder.commit()
            with pytest.raises(ValueError, match='source'):
                await operation
        finally:
            if not operation.done():
                operation.cancel()
            await asyncio.gather(operation, return_exceptions=True)
    async with store.factory() as db:
        assert await db.scalar(select(func.count()).select_from(models.CopyWorkTask)) == 0


async def test_concurrent_workflow_intents_inherit_first_committed_source_selection(store):
    chosen = await source(store)
    sid = await target(store)
    async with store.factory() as first:
        original = await copy_work.enqueue(first, copy_work.CopyRequest(kind='CHAT_CARRY', lane='giant',
            target_snapshot_id=sid, params={'selector_label': 'first'}))
        await source(store, day=2, name='newer')
        operation = asyncio.create_task(enqueue(store, sid, selector_label='second'))
        try:
            await wait_for_row_lock(store, operation)
            await first.commit()
            later = await operation
        finally:
            if not operation.done():
                operation.cancel()
            await asyncio.gather(operation, return_exceptions=True)
    assert original.params['prior_snapshot_id'] == later.params['prior_snapshot_id'] == str(chosen)


async def test_p8_pin_budget_fails_intent_atomically_instead_of_truncating_candidates(store, monkeypatch):
    await chat_membership(store)
    await source(store, day=1, kind='CHAT_ATTACHMENT', external='m1::one', parent='m1', metadata={'chat_id': 'chat-a'})
    await source(store, day=2, kind='CHAT_ATTACHMENT', external='m1::two', parent='m1', metadata={'chat_id': 'chat-a'})
    sid = await target(store)
    monkeypatch.setenv('COPY_INTENT_SOURCE_PIN_MAX', '1')
    with pytest.raises(ValueError, match='pin limit'):
        await enqueue(store, sid, 'LEGACY_CHAT_ATTACHMENTS', chat_id='chat-a')
    async with store.factory() as db:
        assert await db.scalar(select(func.count()).select_from(models.CopyWorkTask)) == 0
        assert await db.scalar(select(func.count()).select_from(models.CopyWorkItem)) == 0
