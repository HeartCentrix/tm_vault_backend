"""Parent lock contention and ownership recovery on real PostgreSQL."""
import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import event, select, text, update

from shared import models, provider_work as work, provider_runtime as runtime
from test_chat_catalogue_postgres import store
from test_provider_work_postgres import claim, enqueue

pytestmark = pytest.mark.integration


@pytest.mark.parametrize('guard_kind', ['provider', 'copy'])
async def test_waiting_parent_guard_allows_sibling_foreign_key_write(store, guard_kind):
    """A guard waiting on one snapshot must not block that writer's job FK check."""
    receipt = await enqueue(store)
    reached_snapshot = asyncio.Event()
    task = None

    def record(conn, cursor, statement, parameters, context, executemany):
        sql = statement.lower()
        if 'from snapshots' in sql and 'for ' in sql:
            reached_snapshot.set()

    async def guard():
        async with store.factory.begin() as db:
            if guard_kind == 'provider':
                await work._parents(db, receipt.snapshot_id)
            else:
                from shared.copy_workflow import _lock_parents
                await _lock_parents(db, SimpleNamespace(job_id=receipt.job_id,
                    target_snapshot_id=receipt.snapshot_id, partition_id=None))

    try:
        async with store.factory.begin() as writer:
            await writer.scalar(select(models.Snapshot).where(models.Snapshot.id == receipt.snapshot_id)
                .with_for_update())
            event.listen(store.engine.sync_engine, 'before_cursor_execute', record)
            task = asyncio.create_task(guard())
            await asyncio.wait_for(reached_snapshot.wait(), 2)
            # The FK acquires KEY SHARE on Job, just as the live snapshot UPDATE did.
            writer.add(models.Snapshot(id=uuid4(), resource_id=store.resource,
                job_id=receipt.job_id, backend_id=store.backend, status=models.SnapshotStatus.COMPLETED))
            await asyncio.wait_for(writer.flush(), 2)
        await asyncio.wait_for(task, 2)
    finally:
        event.remove(store.engine.sync_engine, 'before_cursor_execute', record)
        if task:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def sibling(store, first):
    scope = await store.new_scope()
    async with store.factory.begin() as db:
        resource = models.Resource(id=uuid4(), tenant_id=store.tenant, type=models.ResourceType.USER_MAIL,
            external_id=uuid4().hex, display_name='Sibling')
        db.add(resource)
        await db.flush()
        await db.execute(update(models.Snapshot).where(models.Snapshot.id == scope.snapshot_id)
            .values(job_id=first.job_id, resource_id=resource.id))
        return await work.ensure_work(db, snapshot_id=scope.snapshot_id, snapshot_token=0, job_token=0)


@pytest.mark.parametrize('operation', ['validate', 'finish'])
async def test_sibling_lifecycle_does_not_wait_for_unrelated_checkpoint(store, operation):
    first = await enqueue(store)
    second = await sibling(store, first)
    owners = [await claim(store), await claim(store)]
    owner = next(c for c in owners if c.work_id == first.work_id)
    other = next(c for c in owners if c.work_id == second.work_id)

    async def run():
        async with store.factory.begin() as db:
            if operation == 'validate':
                return (await work._owned(db, other))[0].id
            await work.finish_work(db, other)
            return other.work_id

    async with store.factory.begin() as checkpoint:
        await work.record_unit_checkpoint(checkpoint, owner, unit_key='page', kind='mail')
        assert await asyncio.wait_for(run(), 2) == other.work_id


@pytest.mark.parametrize('sqlstate', ['40P01', '40001'])
async def test_renewal_retries_aborted_transaction_without_cancelling_source(store, monkeypatch, sqlstate):
    await enqueue(store)
    original = work.renew_work
    renewed = asyncio.Event()
    transaction_ids = []
    resume_calls = 0

    async def renew(db, owner, **kwargs):
        transaction_ids.append(await db.scalar(text('SELECT txid_current()')))
        result = await original(db, owner, **kwargs)
        await db.flush()
        if len(transaction_ids) == 1:
            await db.execute(text(f"DO $$ BEGIN RAISE EXCEPTION 'abort renewal' USING ERRCODE='{sqlstate}'; END $$"))
        renewed.set()
        return result

    async def resume(owner):
        nonlocal resume_calls
        resume_calls += 1
        await asyncio.wait_for(renewed.wait(), 3)
        return {'source_completed': True}

    async def settle(_):
        return False

    monkeypatch.setattr(work, 'renew_work', renew)
    runner = runtime.ProviderWorkRunner(store.factory, resume, settle, renew_s=.02)
    result = await runner.run_once()
    assert result == {'source_completed': True}, 'Retryable DB abort must not cancel healthy source work'
    assert resume_calls == 1
    assert len(transaction_ids) >= 2 and len(set(transaction_ids)) == len(transaction_ids)


async def test_cancelled_job_still_fences_sibling_validation(store):
    first = await enqueue(store)
    owner = await claim(store)
    async with store.factory.begin() as db:
        await db.execute(update(models.Job).where(models.Job.id == first.job_id)
            .values(status=models.JobStatus.CANCELLED))
    async with store.factory.begin() as db:
        with pytest.raises(work.ProviderWorkConflict):
            await work._owned(db, owner)

