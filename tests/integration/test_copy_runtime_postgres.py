"""Worker-hosted copy recovery remains independent of broker delivery."""
import asyncio
from datetime import datetime, timedelta, timezone
import importlib
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text

from test_chat_catalogue_postgres import store
from test_copy_admission_postgres import queued
from shared import copy_executor, copy_workflow, models

pytestmark = pytest.mark.integration


def api():
    try:
        return importlib.import_module('shared.copy_runtime')
    except ModuleNotFoundError:
        pytest.fail('worker-hosted durable copy recovery is not implemented')


async def pending(store, *, task_state='READY', workflow_state='WAITING'):
    target, task_id, claim = await queued(store)
    async with store.factory.begin() as db:
        await db.execute(text('CREATE TABLE IF NOT EXISTS worker_heartbeats '
                              '(worker_id uuid PRIMARY KEY,last_seen_at timestamptz NOT NULL)'))
        task = await db.get(models.CopyWorkTask, task_id)
        workflow = await db.get(models.CopyWorkflow, task.workflow_id)
        task.state, workflow.state = task_state, workflow_state
        if task_state != 'RUNNING':
            task.lease_owner_id = task.lease_expires_at = None
        else:
            task.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        workflow_id = workflow.id
    return target, task_id, workflow_id, claim


def completer(store, calls, completed=None):
    async def resume(workflow_id):
        calls.append(workflow_id)
        owner = uuid4()
        workflow = await copy_workflow.begin_resume(store.factory, workflow_id, owner)
        if workflow is None:
            return
        async with store.factory.begin() as db:
            snapshot = await db.get(models.Snapshot, workflow.target_snapshot_id)
            snapshot.status = models.SnapshotStatus.COMPLETED
        assert await copy_workflow.finish_resume(store.factory, workflow_id, owner)
        if completed is not None:
            completed.set()
    return resume


async def test_polling_executes_and_resumes_without_any_broker_wakeup(store, monkeypatch):
    monkeypatch.setenv('COPY_WORK_ENABLED', 'false')
    _, task_id, workflow_id, _ = await pending(store)
    calls, completed = [], asyncio.Event()
    runner = api().CopyWorkRunner(store.factory, completer(store, calls, completed),
                                  concurrency=1, poll_s=0.02, recovery_s=0.05)
    await runner.start()
    try:
        await asyncio.wait_for(completed.wait(), timeout=3)
    finally:
        await runner.close()
    assert workflow_id in calls
    async with store.factory() as db:
        assert (await db.get(models.CopyWorkTask, task_id)).state == 'COMPLETED'
        assert (await db.get(models.CopyWorkflow, workflow_id)).state == 'COMPLETED'


async def test_run_once_reclaims_expired_task_generation_without_lost_checkpoint(store):
    _, task_id, _, old_claim = await pending(store, task_state='RUNNING')
    runner = api().CopyWorkRunner(store.factory, completer(store, []), concurrency=1)
    result = await runner.run_once()
    assert result.status == 'COMPLETED'
    async with store.factory() as db:
        task = await db.get(models.CopyWorkTask, task_id)
        assert task.generation > old_claim.generation
        assert task.row_count == 2 and task.checkpoint == 2
    await runner.close()


async def test_recovery_finds_settled_workflow_when_final_notification_was_lost(store):
    _, _, workflow_id, _ = await pending(store, task_state='COMPLETED')
    calls = []
    runner = api().CopyWorkRunner(store.factory, completer(store, calls))
    assert await runner.recover_once() == 1
    assert calls == [workflow_id]
    await runner.close()


async def test_recovery_reclaims_expired_resuming_callback(store):
    _, _, workflow_id, _ = await pending(store, task_state='COMPLETED')
    workflow = await copy_workflow.begin_resume(store.factory, workflow_id, uuid4())
    async with store.factory.begin() as db:
        wf = await db.get(models.CopyWorkflow, workflow_id)
        wf.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        (await db.get(models.Snapshot, workflow.target_snapshot_id)).lease_expires_at = wf.lease_expires_at
    calls = []
    runner = api().CopyWorkRunner(store.factory, completer(store, calls))
    assert await runner.recover_once() == 1
    assert calls == [workflow_id]
    await runner.close()


async def test_disabled_database_protocol_neither_claims_nor_resumes(store):
    _, task_id, workflow_id, _ = await pending(store)
    async with store.factory.begin() as db:
        (await db.get(models.CopyWorkControl, 'global')).enabled = False
    calls = []
    runner = api().CopyWorkRunner(store.factory, completer(store, calls))
    assert await runner.run_once() is None
    assert await runner.recover_once() == 0
    assert calls == []
    async with store.factory() as db:
        assert (await db.get(models.CopyWorkTask, task_id)).state == 'READY'
        assert (await db.get(models.CopyWorkflow, workflow_id)).state == 'WAITING'
    await runner.close()


async def unsealed(store, *, live=False, null_lease=False, cancelled=False):
    target, task_id, workflow_id, _ = await pending(store, workflow_state='DRAINING')
    owner = uuid4()
    async with store.factory.begin() as db:
        snapshot = await db.get(models.Snapshot, target.snapshot_id)
        snapshot.lease_owner_id = None if null_lease else owner
        snapshot.lease_expires_at = None if null_lease else datetime.now(timezone.utc) - timedelta(seconds=1)
        if live:
            await db.execute(text('INSERT INTO worker_heartbeats VALUES (:id,NOW())'), {'id': owner})
        # enqueue already pins its fixed predecessor before source drain.
        if cancelled:
            (await db.get(models.Job, snapshot.job_id)).status = models.JobStatus.CANCELLED
    return target, task_id, workflow_id


async def test_dead_unsealed_work_is_abandoned_and_unpins_without_resuming_graph(store):
    _, task_id, workflow_id = await unsealed(store)
    calls = []
    runner = api().CopyWorkRunner(store.factory, completer(store, calls))
    assert await runner.recover_once() == 1
    assert calls == []
    async with store.factory() as db:
        assert (await db.get(models.CopyWorkflow, workflow_id)).state == 'FAILED'
        assert (await db.get(models.CopyWorkTask, task_id)).state == 'CANCELLED'
        assert await db.scalar(select(func.count()).select_from(models.CopyWorkItem)
            .where(models.CopyWorkItem.task_id == task_id)) == 0
    await runner.close()


@pytest.mark.parametrize('mode', ['live_heartbeat', 'new_null_lease'])
async def test_live_or_unproven_unsealed_owner_keeps_required_work_and_pins(store, mode):
    _, task_id, workflow_id = await unsealed(store, live=mode == 'live_heartbeat', null_lease=mode == 'new_null_lease')
    runner = api().CopyWorkRunner(store.factory, completer(store, []))
    assert await runner.recover_once() == 0
    async with store.factory() as db:
        assert (await db.get(models.CopyWorkflow, workflow_id)).state == 'DRAINING'
        assert (await db.get(models.CopyWorkTask, task_id)).state == 'READY'
        assert await db.scalar(select(func.count()).select_from(models.CopyWorkItem)
            .where(models.CopyWorkItem.task_id == task_id)) == 1
    await runner.close()


async def test_terminal_parent_cancels_unsealed_work_even_with_live_heartbeat(store):
    _, _, workflow_id = await unsealed(store, live=True, cancelled=True)
    runner = api().CopyWorkRunner(store.factory, completer(store, []))
    assert await runner.recover_once() == 1
    async with store.factory() as db:
        assert (await db.get(models.CopyWorkflow, workflow_id)).state == 'CANCELLED'
    await runner.close()


async def test_callback_crash_is_recoverable_and_logs_no_source_urls(store, caplog):
    _, _, workflow_id, _ = await pending(store, task_state='COMPLETED')
    async def crash(wid):
        assert await copy_workflow.begin_resume(store.factory, wid, uuid4()) is not None
        raise RuntimeError('https://provider.invalid?signed_secret=must-not-log')
    runner = api().CopyWorkRunner(store.factory, crash)
    assert await runner.recover_once() == 0
    assert 'signed_secret' not in caplog.text
    async with store.factory.begin() as db:
        wf = await db.get(models.CopyWorkflow, workflow_id)
        assert wf.state == 'RESUMING'
        wf.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    await runner.close()
    replacement = api().CopyWorkRunner(store.factory, completer(store, []))
    assert await replacement.recover_once() == 1
    await replacement.close()


async def test_close_cancels_and_drains_sql_then_allows_recovery_without_extra_claims(store, monkeypatch):
    target, task_id, _, _ = await pending(store)
    entered = asyncio.Event()
    async def slow(db, task, **kwargs):
        entered.set()
        await db.execute(text('SELECT pg_sleep(10)'))
    monkeypatch.setattr(copy_executor, 'copy_manifest_batch', slow)
    runner = api().CopyWorkRunner(store.factory, completer(store, []), concurrency=1)
    operation = asyncio.create_task(runner.run_once())
    await asyncio.wait_for(entered.wait(), timeout=3)
    await asyncio.wait_for(runner.close(), timeout=3)
    assert operation.cancelled()
    assert await runner.run_once() is None and await runner.recover_once() == 0
    async with store.factory.begin() as db:
        task = await db.get(models.CopyWorkTask, task_id)
        assert task.state == 'READY' and task.lease_owner_id is None
        assert await db.scalar(select(func.count()).select_from(models.SnapshotItem)
            .where(models.SnapshotItem.snapshot_id == target.snapshot_id)) == 0
        assert await db.scalar(text('SELECT pg_try_advisory_xact_lock(420018,1000000)'))
        assert await db.scalar(text('SELECT pg_try_advisory_xact_lock(420020,0)'))


async def test_failing_recovery_page_does_not_starve_later_workflows(store):
    workflows = [(await pending(store, task_state='COMPLETED'))[2] for _ in range(17)]
    calls = []
    complete = completer(store, calls)
    async def callback(workflow_id):
        if workflow_id != workflows[-1]:
            raise RuntimeError('continuation cannot currently settle')
        await complete(workflow_id)
    runner = api().CopyWorkRunner(store.factory, callback, concurrency=1)
    assert await runner.recover_once() == 0
    assert await runner.recover_once() == 1
    assert calls == [workflows[-1]]
    await runner.close()


async def test_superseded_unsealed_workflow_releases_only_old_work_without_touching_new_owner(store):
    target, task_id, workflow_id = await unsealed(store, live=True)
    replacement = uuid4()
    async with store.factory.begin() as db:
        leaf = await db.get(models.Snapshot, target.snapshot_id)
        leaf.lease_token += 1
        leaf.lease_owner_id = replacement
        leaf.lease_expires_at = datetime.now(timezone.utc) + timedelta(minutes=10)
        token, expires = leaf.lease_token, leaf.lease_expires_at
    runner = api().CopyWorkRunner(store.factory, completer(store, []))
    assert await runner.recover_once() == 1
    async with store.factory() as db:
        assert (await db.get(models.CopyWorkflow, workflow_id)).state == 'FAILED'
        assert (await db.get(models.CopyWorkTask, task_id)).state == 'CANCELLED'
        leaf = await db.get(models.Snapshot, target.snapshot_id)
        assert (leaf.lease_token, leaf.lease_owner_id, leaf.lease_expires_at) == (token, replacement, expires)
        assert leaf.status == models.SnapshotStatus.IN_PROGRESS
    await runner.close()


async def test_future_expected_leaf_fence_fails_closed_and_logs_without_unpinning(store, caplog):
    _, task_id, workflow_id = await unsealed(store)
    async with store.factory.begin() as db:
        (await db.get(models.CopyWorkflow, workflow_id)).parent_lease_token += 1
    runner = api().CopyWorkRunner(store.factory, completer(store, []))
    assert await runner.recover_once() == 0
    assert 'copy_unsealed_future_fence' in caplog.text
    async with store.factory() as db:
        assert (await db.get(models.CopyWorkflow, workflow_id)).state == 'DRAINING'
        assert await db.scalar(select(func.count()).select_from(models.CopyWorkItem)
            .where(models.CopyWorkItem.task_id == task_id)) == 1
    await runner.close()


@pytest.mark.parametrize('parent_state',['RUNNING','COMPLETED','FAILED'])
async def test_recovery_does_not_abandon_a_resume_that_committed_its_leaf(store,parent_state):
    target,task_id,workflow_id,_=await pending(store,task_state='COMPLETED')
    current=await copy_workflow.begin_resume(store.factory,workflow_id,uuid4())
    async with store.factory.begin() as db:
        snapshot=await db.get(models.Snapshot,target.snapshot_id)
        snapshot.status=models.SnapshotStatus.COMPLETED
        (await db.get(models.Job,snapshot.job_id)).status=models.JobStatus(parent_state)
    runner=api().CopyWorkRunner(store.factory,completer(store,[]))
    await runner._initialize()
    assert not await runner._abandon(workflow_id)
    assert workflow_id not in await runner._orphan_candidates()
    async with store.factory() as db:
        assert (await db.get(models.CopyWorkflow,workflow_id)).state=='RESUMING'
        assert (await db.get(models.CopyWorkTask,task_id)).state=='COMPLETED'
    await runner.close()


async def test_terminal_leaf_does_not_hide_explicit_parent_cancellation(store):
    target,_,workflow_id,_=await pending(store,task_state='COMPLETED')
    await copy_workflow.begin_resume(store.factory,workflow_id,uuid4())
    async with store.factory.begin() as db:
        snapshot=await db.get(models.Snapshot,target.snapshot_id)
        snapshot.status=models.SnapshotStatus.COMPLETED
        (await db.get(models.Job,snapshot.job_id)).status=models.JobStatus.CANCELLED
    runner=api().CopyWorkRunner(store.factory,completer(store,[]))
    await runner._initialize()
    assert await runner._abandon(workflow_id)
    async with store.factory() as db:
        assert (await db.get(models.CopyWorkflow,workflow_id)).state=='CANCELLED'
    await runner.close()


@pytest.mark.parametrize('provider_state,expected', [('COMPLETED', True), ('WAITING', False)])
async def test_orphan_recovery_is_not_blocked_by_completed_provider_receipts(store, monkeypatch, provider_state, expected):
    from shared import provider_work
    target, _, workflow_id = await unsealed(store)
    monkeypatch.setattr(provider_work, 'guards_enabled', lambda: True)
    async with store.factory.begin() as db:
        snapshot = await db.get(models.Snapshot, target.snapshot_id)
        receipt = await provider_work.ensure_work(db, snapshot_id=snapshot.id,
            snapshot_token=snapshot.lease_token, job_token=0, continuation={})
        row = await db.get(models.ProviderWork, receipt.work_id)
        row.state, row.parent_settle_pending = provider_state, True
        snapshot.status = models.SnapshotStatus.PARTIAL
    runner = api().CopyWorkRunner(store.factory, completer(store, []))
    assert (workflow_id in await runner._orphan_candidates()) is expected
    await runner.close()


async def test_control_queries_outlast_a_checkpoint_stall(store):
    """Railway 2026-11-05 (08:41, 12:16, 15:59): forced checkpoints stalled every query for 4-9 s, and
    the 5 s control budget cancelled every worker's orphan scan at once (TM log signature #20)."""
    from sqlalchemy import text
    async with store.factory() as session:
        await api().CopyWorkRunner._short_settings(session)
        await session.execute(text("SELECT pg_sleep(6)"))
