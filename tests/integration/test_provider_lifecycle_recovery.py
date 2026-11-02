"""Provider recovery and fairness against disposable PostgreSQL schemas."""
import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock
from uuid import uuid4
import pytest
from sqlalchemy import select, update
from test_chat_catalogue_postgres import store
from shared import models, provider_work as work

pytestmark = pytest.mark.integration

async def enqueue(store, weight=1):
    scope=await store.new_scope()
    async with store.factory.begin() as db:
        return await work.ensure_work(db, snapshot_id=scope.snapshot_id, snapshot_token=0, job_token=0, weight=weight)

async def claim(store):
    async with store.factory.begin() as db:
        return await work.claim_due_work(db, owner_id=uuid4())

@pytest.mark.parametrize('idle_mode',['finished','cooldown'])
async def test_reactivated_idle_policy_cannot_bank_unbounded_priority(store,idle_mode):
    idle=await enqueue(store,8)
    owner=await claim(store)
    async with store.factory.begin() as db:
        if idle_mode=='finished':
            await work.finish_work(db,owner)
        else:
            await work.suspend_current_work(db,owner,next_eligible_at=datetime.now(timezone.utc)+timedelta(hours=1),reason='provider_throttle')
    low=await enqueue(store,1)
    for _ in range(20):
        owner=await claim(store)
        assert owner.work_id == low.work_id
        async with store.factory.begin() as db:
            await work.suspend_current_work(db,owner,next_eligible_at=datetime.now(timezone.utc),reason='checkpoint')
    if idle_mode=='cooldown':
        async with store.factory.begin() as db:
            (await db.get(models.ProviderWork,idle.work_id)).next_eligible_at=datetime.now(timezone.utc)
    seen=[]
    for _ in range(24):
        await enqueue(store,8)
        owner=await claim(store)
        seen.append(owner.work_id)
        async with store.factory.begin() as db:
            if owner.work_id == low.work_id:
                await work.suspend_current_work(db,owner,next_eligible_at=datetime.now(timezone.utc),reason='checkpoint')
            else:
                await work.finish_work(db,owner)
    assert low.work_id in seen, 'Returning high-weight queue consumed all24 turns; existing custom queue got zero'

async def test_original_deadline_exhaustion_settles_instead_of_reclaiming_forever(store):
    from shared.provider_runtime import ProviderWorkRunner, account_provider_attempt, ProviderBudgetExhausted
    receipt=await enqueue(store)
    owner=await claim(store)
    async with store.factory.begin() as db:
        await work.record_unit_checkpoint(db,owner,unit_key='planning',kind='planning',deadline_at=datetime.now(timezone.utc)-timedelta(days=1))
        await work.suspend_current_work(db,owner,next_eligible_at=datetime.now(timezone.utc),reason='checkpoint')
    async def resume(owner):
        await account_provider_attempt()
    runner=ProviderWorkRunner(store.factory,resume,AsyncMock())
    generations=[]
    for _ in range(3):
        try:
            await runner.run_once()
        except ProviderBudgetExhausted:
            pass
        async with store.factory.begin() as db:
            row=await db.get(models.ProviderWork,receipt.work_id)
            generations.append(row.generation)
            row.lease_expires_at=datetime.now(timezone.utc)-timedelta(seconds=1)
    async with store.factory() as db:
        row=await db.get(models.ProviderWork,receipt.work_id)
        assert row.state in work.TERMINAL, f'Expired unit remains {row.state}, claimed generations={generations}'

async def test_parallel_throttles_commit_latest_latched_eligibility(store):
    from shared.provider_runtime import ProviderWorkRunner, provider_request_scope
    from shared.provider_admission import ProviderDeferred, ProviderRequestContext
    receipt=await enqueue(store)
    ready=asyncio.Event()
    count=0
    async def child(delay):
        nonlocal count
        async with provider_request_scope():
            count+=1
            if count==2: ready.set()
            await ready.wait()
            raise ProviderDeferred(ProviderRequestContext('tenant','app','graph'),delay)
    async def resume(owner):
        await asyncio.gather(child(900),child(600))
    runner=ProviderWorkRunner(store.factory,resume,AsyncMock())
    before=datetime.now(timezone.utc)
    assert (await runner.run_once())['provider_waiting']
    async with store.factory() as db:
        row=await db.get(models.ProviderWork,receipt.work_id)
        delay=(row.next_eligible_at-before).total_seconds()
        assert delay>=895, f'Committed shorter {delay:.1f}s handoff although an in-flight sibling latched900s'

async def test_failed_unit_cannot_report_complete_activity(store):
    from shared.provider_runtime import ProviderWorkRunner, unit_scope, checkpoint_unit
    from shared.provider_observability import provider_work_summaries
    receipt=await enqueue(store)
    async def resume(owner):
        async with unit_scope('chat:failed','chat'):
            await checkpoint_unit(state='FAILED',receipt_id='incomplete-coverage')
        async with store.factory.begin() as db:
            snapshot=await db.get(models.Snapshot,owner.snapshot_id)
            snapshot.status=models.SnapshotStatus.PARTIAL
            (await db.get(models.Job,owner.job_id)).status=models.JobStatus.COMPLETED
        return {'outcome':'partial'}
    async def settle(work_id):
        async with store.factory.begin() as db:
            await work.finish_parent_settlement(db,work_id)
    runner=ProviderWorkRunner(store.factory,resume,settle)
    await runner.run_once()
    async with store.factory() as db:
        summary=(await provider_work_summaries(db,job_ids=[receipt.job_id]))[str(receipt.job_id)]
        assert summary['failed_units']==1
        assert summary['state']=='INCOMPLETE', summary

async def test_settlement_receipt_survives_until_job_terminal(store):
    receipt=await enqueue(store)
    owner=await claim(store)
    async with store.factory.begin() as db:
        await work.finish_work(db,owner)
        snapshot=await db.get(models.Snapshot,owner.snapshot_id)
        snapshot.status=models.SnapshotStatus.COMPLETED
        await db.flush()
        assert (await db.get(models.Job,owner.job_id)).status==models.JobStatus.RUNNING
        assert not await work.finish_parent_settlement(db,owner.work_id), 'Released retry obligation before Job settlement'

async def test_failed_turn_joins_provider_children_before_releasing_permit(store):
    from shared.provider_runtime import ProviderWorkRunner, provider_request_scope, ExecutionBudget
    await enqueue(store)
    started, closed=asyncio.Event(),asyncio.Event()
    tasks=[]
    async def child():
        try:
            async with provider_request_scope():
                started.set()
                await asyncio.Event().wait()
        finally:
            closed.set()
    async def resume(owner):
        tasks.append(asyncio.create_task(child()))
        await started.wait()
        raise RuntimeError('ordinary callback failure while sibling is in flight')
    budget=ExecutionBudget(content=1)
    runner=ProviderWorkRunner(store.factory,resume,AsyncMock(),budget=budget)
    try:
        try:
            await runner.run_once()
        except RuntimeError:
            pass
        assert tasks[0].done() and closed.is_set(), 'Turn released process permit with its provider child still running'
    finally:
        for task in tasks: task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)

async def test_crash_after_failed_parent_commit_recovers_truthful_terminal_receipt(store):
    from shared.provider_observability import provider_work_summaries
    receipt=await enqueue(store)
    owner=await claim(store)
    async with store.factory.begin() as db:
        await work.record_unit_checkpoint(db,owner,unit_key='chat:unresolved',kind='chat')
        (await db.get(models.Snapshot,owner.snapshot_id)).status=models.SnapshotStatus.FAILED
        (await db.get(models.Job,owner.job_id)).status=models.JobStatus.FAILED
        (await db.get(models.ProviderWork,owner.work_id)).lease_expires_at=datetime.now(timezone.utc)-timedelta(seconds=1)
    assert await claim(store) is None
    async with store.factory.begin() as db:
        assert await work.finish_parent_settlement(db,owner.work_id)
    async with store.factory() as db:
        summary=(await provider_work_summaries(db,job_ids=[owner.job_id]))[str(owner.job_id)]
        assert summary['state']=='INCOMPLETE' and summary['pending_units']==0, summary
