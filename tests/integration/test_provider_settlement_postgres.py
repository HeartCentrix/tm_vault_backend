"""Durable, bounded parent settlement across independent provider runners."""
import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import select, text, update

from shared import models, provider_work as work
from shared.provider_runtime import ProviderWorkRunner
from test_chat_catalogue_postgres import store
from test_provider_work_postgres import claim, enqueue

pytestmark = pytest.mark.integration


async def completed(store):
    receipt = await enqueue(store)
    owner = await claim(store)
    async with store.factory.begin() as db:
        await work.finish_work(db, owner)
        (await db.get(models.Snapshot, receipt.snapshot_id)).status = models.SnapshotStatus.COMPLETED
    return receipt


async def test_twenty_runners_execute_one_settlement_callback(store):
    receipt = await completed(store)
    started, release, others_done = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls, finished = 0, 0

    async def settle(work_id):
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        return False

    async def contender():
        nonlocal finished
        runner = ProviderWorkRunner(store.factory, None, settle)
        result = await runner.settle_once()
        finished += 1
        if finished == 19:
            others_done.set()
        return result

    tasks = [asyncio.create_task(contender()) for _ in range(20)]
    try:
        await asyncio.wait_for(started.wait(), 3)
        await asyncio.wait_for(others_done.wait(), 3)
        assert calls == 1
    finally:
        release.set()
        results = await asyncio.gather(*tasks, return_exceptions=True)
    assert results.count(True) == 1 and results.count(False) == 19
    async with store.factory() as db:
        row = await db.get(models.ProviderWork, receipt.work_id)
        assert row.parent_settle_pending and row.lease_owner_id is None


@pytest.mark.parametrize('outcome', ['pending', 'error', 'unconfirmed_success'])
async def test_unfinished_settlement_retries_are_durably_delayed(store, outcome):
    receipt = await completed(store)
    calls = 0

    async def settle(work_id):
        nonlocal calls
        calls += 1
        if outcome == 'error':
            raise RuntimeError('synthetic settlement failure')
        return outcome == 'unconfirmed_success'

    runner = ProviderWorkRunner(store.factory, None, settle)
    before = datetime.now(timezone.utc)
    if outcome == 'error':
        with pytest.raises(RuntimeError, match='synthetic settlement failure'):
            await runner.settle_once(receipt.work_id)
    else:
        assert await runner.settle_once(receipt.work_id)
    for _ in range(20):
        assert not await runner.settle_once(receipt.work_id)
    assert calls == 1
    async with store.factory.begin() as db:
        row = await db.get(models.ProviderWork, receipt.work_id)
        assert row.parent_settle_pending and row.lease_owner_id is None and row.lease_expires_at is None
        assert row.next_eligible_at >= before + timedelta(seconds=30)
        row.next_eligible_at = before - timedelta(seconds=1)
    if outcome == 'error':
        with pytest.raises(RuntimeError):
            await runner.settle_once(receipt.work_id)
    else:
        assert await runner.settle_once(receipt.work_id)
    assert calls == 2


@pytest.mark.parametrize('job_status', ['COMPLETED', 'PARTIAL'])
async def test_success_clears_pending_only_after_terminal_parents_without_pinned_session(store, job_status):
    receipt = await completed(store)
    async with store.factory.begin() as db:
        (await db.get(models.Job, receipt.job_id)).status = models.JobStatus[job_status]

    async def settle(work_id):
        assert store.engine.sync_engine.pool.checkedout() == 0
        with work.settling_parent():
            async with store.factory.begin() as db:
                return await work.finish_parent_settlement(db, work_id)

    runner = ProviderWorkRunner(store.factory, None, settle)
    assert await runner.settle_once(receipt.work_id)
    assert not await runner.settle_once(receipt.work_id)
    async with store.factory() as db:
        row = await db.get(models.ProviderWork, receipt.work_id)
        assert not row.parent_settle_pending
        assert row.lease_owner_id is None and row.lease_expires_at is None


async def test_expired_settlement_recovers_and_old_owner_cannot_release_or_finish(store):
    receipt = await completed(store)
    async with store.factory.begin() as db:
        old = await work.claim_parent_settlement(db, owner_id=uuid4(), work_id=receipt.work_id)
    async with store.factory.begin() as db:
        assert await work.claim_parent_settlement(db, owner_id=uuid4()) is None
        await db.execute(update(models.ProviderWork).where(models.ProviderWork.id == receipt.work_id)
            .values(lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1)))
    async with store.factory.begin() as db:
        replacement = await work.claim_parent_settlement(db, owner_id=uuid4())
        assert replacement.generation > old.generation
    async with store.factory.begin() as db:
        assert not await work.release_parent_settlement(db, old)
        (await db.get(models.Job, receipt.job_id)).status = models.JobStatus.COMPLETED
    with work.settling_parent(old):
        async with store.factory.begin() as db:
            with pytest.raises(work.ProviderWorkConflict):
                await work.finish_parent_settlement(db, receipt.work_id)
    async with store.factory() as db:
        row = await db.get(models.ProviderWork, receipt.work_id)
        assert row.parent_settle_pending and row.lease_owner_id == replacement.owner_id
    with work.settling_parent(replacement):
        async with store.factory.begin() as db:
            assert await work.finish_parent_settlement(db, receipt.work_id)


async def test_cancellation_fences_settlement_guards_and_release(store):
    receipt = await completed(store)

    async def settle(work_id):
        async with store.factory.begin() as db:
            await work.cancel_provider_work(db, job_id=receipt.job_id)
        with work.settling_parent():
            async with store.factory.begin() as db:
                assert await work.has_pending_provider(db, job_id=receipt.job_id)
                predicate = work.pending_provider_sql(job_expr=f"'{receipt.job_id}'::uuid")
                assert not await db.scalar(text('SELECT ' + predicate))
                with pytest.raises(work.ProviderWorkConflict):
                    await work.finish_parent_settlement(db, work_id)
        return True

    runner = ProviderWorkRunner(store.factory, None, settle)
    assert await runner.settle_once(receipt.work_id)
    assert not await runner.settle_once(receipt.work_id)
    async with store.factory() as db:
        row = await db.get(models.ProviderWork, receipt.work_id)
        assert row.state == 'CANCELLED' and not row.parent_settle_pending
        assert row.lease_owner_id is None and row.lease_expires_at is None


async def test_immediate_completion_and_periodic_settlement_share_the_same_claim(store):
    receipt = await enqueue(store)
    started, release = asyncio.Event(), asyncio.Event()
    calls = 0

    async def resume(owner):
        return {'completed': True}

    async def settle(work_id):
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        return False

    runner = ProviderWorkRunner(store.factory, resume, settle)
    task = asyncio.create_task(runner.run_once())
    try:
        await asyncio.wait_for(started.wait(), 3)
        other = ProviderWorkRunner(store.factory, None, settle)
        assert not await other.settle_once(receipt.work_id)
    finally:
        release.set()
        result = await task
    assert result == {'completed': True} and calls == 1


async def test_terminal_settlement_lease_does_not_consume_provider_capacity(store):
    receipt = await completed(store)
    pending = await enqueue(store)
    async with store.factory.begin() as db:
        control = await db.get(models.ProviderWorkControl, 'global')
        control.total_capacity = control.tenant_capacity = 1
        assert await work.claim_parent_settlement(db, owner_id=uuid4(), work_id=receipt.work_id)
    assert (await claim(store)).work_id == pending.work_id


async def test_timed_out_callback_releases_lease_and_defers_retry(store, monkeypatch):
    receipt = await completed(store)
    real_timeout = asyncio.timeout
    monkeypatch.setattr('shared.provider_runtime.asyncio.timeout', lambda seconds: real_timeout(.02))
    stopped = asyncio.Event()

    async def settle(work_id):
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    runner = ProviderWorkRunner(store.factory, None, settle)
    with pytest.raises(TimeoutError):
        await runner.settle_once(receipt.work_id)
    assert stopped.is_set()
    assert not await runner.settle_once(receipt.work_id)
    async with store.factory() as db:
        row = await db.get(models.ProviderWork, receipt.work_id)
        assert row.parent_settle_pending and row.lease_owner_id is None and row.lease_expires_at is None


async def test_cancelled_settler_preserves_pending_receipt_and_defers_retry(store):
    receipt = await completed(store)
    started = asyncio.Event()

    async def settle(work_id):
        started.set()
        await asyncio.Event().wait()

    runner = ProviderWorkRunner(store.factory, None, settle)
    task = asyncio.create_task(runner.settle_once(receipt.work_id))
    await asyncio.wait_for(started.wait(), 3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not await runner.settle_once(receipt.work_id)
    async with store.factory() as db:
        row = await db.get(models.ProviderWork, receipt.work_id)
        assert row.parent_settle_pending and row.lease_owner_id is None and row.lease_expires_at is None
