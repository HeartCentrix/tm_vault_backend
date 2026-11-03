"""Durable provider ownership and scheduling against disposable PostgreSQL."""
from datetime import datetime, timedelta, timezone
from uuid import uuid4
import pytest
from sqlalchemy import select, update
from test_chat_catalogue_postgres import store
from shared import models
from shared import provider_work as work

pytestmark = pytest.mark.integration

async def enqueue(store, *, weight=1):
    scope = await store.new_scope()
    async with store.factory.begin() as db:
        receipt = await work.ensure_work(db, snapshot_id=scope.snapshot_id,
            snapshot_token=0, job_token=0, weight=weight, continuation={'kind': 'single'})
    return receipt

async def claim(store):
    async with store.factory.begin() as db:
        return await work.claim_due_work(db, owner_id=uuid4())

async def test_defer_preserves_budget_age_and_fences_duplicate_delivery(store):
    receipt = await enqueue(store)
    first = await claim(store)
    assert first.work_id == receipt.work_id
    assert await claim(store) is None
    async with store.factory.begin() as db:
        unit = await work.record_unit_checkpoint(db, first, unit_key='chat:a', kind='chat',
            attempts=2, waited_s=7, reconciliations=1)
        created, deadline = unit.created_at, unit.deadline_at
        due = datetime.now(timezone.utc) + timedelta(hours=2)
        suspended = await work.suspend_current_work(db, first, next_eligible_at=due, reason='throttled')
        assert await work.has_pending_provider(db, snapshot_id=receipt.snapshot_id)
    assert await claim(store) is None
    async with store.factory.begin() as db:
        assert await work.suspend_current_work(db, first, next_eligible_at=due, reason='throttled') == suspended
        await db.execute(update(models.ProviderWork).values(next_eligible_at=datetime.now(timezone.utc)))
    resumed = await claim(store)
    assert resumed.generation > first.generation
    assert resumed.parent_token > first.parent_token
    async with store.factory.begin() as db:
        with pytest.raises(work.ProviderWorkConflict):
            await work.record_unit_checkpoint(db, first, unit_key='chat:a', kind='chat')
    async with store.factory.begin() as db:
        unit = await work.record_unit_checkpoint(db, resumed, unit_key='chat:a', kind='chat')
        assert (unit.attempts, unit.reconciliations) == (2, 1)
        assert unit.waited_s >= 7206
        assert (unit.created_at, unit.deadline_at) == (created, deadline)

async def test_expired_claim_recovers_without_broker_and_old_owner_cannot_finish(store):
    await enqueue(store)
    first = await claim(store)
    async with store.factory.begin() as db:
        await db.execute(update(models.ProviderWork).values(lease_expires_at=datetime.now(timezone.utc)-timedelta(seconds=1)))
    replacement = await claim(store)
    assert replacement.generation > first.generation
    async with store.factory.begin() as db:
        with pytest.raises(work.ProviderWorkConflict):
            await work.finish_work(db, first)
    async with store.factory.begin() as db:
        await work.finish_work(db, replacement)
        assert await work.has_pending_provider(db, snapshot_id=replacement.snapshot_id)
        assert not await work.finish_parent_settlement(db, replacement.work_id)
        snapshot = await db.get(models.Snapshot, replacement.snapshot_id)
        snapshot.status = models.SnapshotStatus.COMPLETED
        (await db.get(models.Job, replacement.job_id)).status = models.JobStatus.COMPLETED
        await db.flush()
        assert await work.finish_parent_settlement(db, replacement.work_id)
        assert not await work.has_pending_provider(db, snapshot_id=replacement.snapshot_id)

async def test_cancelled_parent_never_claims_and_cancellation_invalidates_owner(store):
    receipt = await enqueue(store)
    first = await claim(store)
    async with store.factory.begin() as db:
        await work.cancel_provider_work(db, snapshot_id=receipt.snapshot_id)
    assert await claim(store) is None
    async with store.factory.begin() as db:
        with pytest.raises(work.ProviderWorkConflict):
            await work.renew_work(db, first)
        assert not await work.has_pending_provider(db, snapshot_id=receipt.snapshot_id)

async def test_positive_weight_and_aging_allow_backlogged_custom_policy_progress(store):
    low = await enqueue(store, weight=1)
    high = await enqueue(store, weight=8)
    seen = []
    for _ in range(25):
        current = await claim(store)
        seen.append(current.work_id)
        async with store.factory.begin() as db:
            await work.suspend_current_work(db, current, next_eligible_at=datetime.now(timezone.utc), reason='checkpoint')
    assert low.work_id in seen and high.work_id in seen
    assert max(i for i, value in enumerate(seen) if value == low.work_id) >= 15

async def test_aggregate_database_capacity_and_pending_receipts(store):
    for _ in range(5):
        await enqueue(store)
    async with store.factory.begin() as db:
        control = await db.get(models.ProviderWorkControl, 'global')
        control.total_capacity = 2
        control.tenant_capacity = 2
    first, second = await claim(store), await claim(store)
    assert first and second and await claim(store) is None
    async with store.factory.begin() as db:
        await work.record_unit_checkpoint(db, first, unit_key='page', kind='planning')
        with pytest.raises(work.ProviderWorkConflict, match='pending units'):
            await work.finish_work(db, first)
    async with store.factory.begin() as db:
        await work.record_unit_checkpoint(db, first, unit_key='page', kind='planning', state='COMPLETED')
        await work.finish_work(db, first)
    assert await claim(store)

async def test_chat_suspend_is_nonterminal_and_exact_attempt_links_are_removed(store):
    from dataclasses import replace
    from test_chat_catalogue_postgres import message
    from shared import chat_catalogue
    scope = await store.new_scope()
    async with store.factory.begin() as db:
        await work.ensure_work(db, snapshot_id=scope.snapshot_id, snapshot_token=0, job_token=0)
    owner = await claim(store)
    scope = replace(scope, snapshot_lease_token=owner.snapshot_token, job_lease_token=owner.job_token)
    lease = await store.begin(scope, observation_hash_version=2)
    await store.stage(lease, [message()])
    due = datetime.now(timezone.utc) + timedelta(hours=1)
    async with store.factory.begin() as db:
        await chat_catalogue.suspend_capture(db, lease=lease, claim=owner, unit_key='chat:a', next_eligible_at=due)
        assert await db.get(models.SnapshotChatInventory, (scope.snapshot_id, scope.chat_id)) is None
        assert not await db.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == scope.snapshot_id))
        assert (await db.get(models.ChatCaptureAttempt, lease.attempt_id)).state == 'SUPERSEDED'
        assert await work.has_pending_provider(db, snapshot_id=scope.snapshot_id)
    fresh = await store.begin(scope, observation_hash_version=2)
    assert fresh.generation > lease.generation
    assert fresh.staged_inventory_id != lease.staged_inventory_id
    with pytest.raises(chat_catalogue.ChatCatalogueConflict):
        await store.stage(lease, [message()])

async def test_runner_commits_long_wait_and_resumes_without_message_publish(store):
    from shared.provider_runtime import ProviderWorkRunner, provider_request_scope, account_provider_attempt
    from shared.provider_admission import ProviderDeferred, ProviderRequestContext
    receipt = await enqueue(store)
    calls = 0
    async def resume(owner):
        nonlocal calls
        calls += 1
        await account_provider_attempt()
        if calls == 1:
            async with provider_request_scope():
                raise ProviderDeferred(ProviderRequestContext('tenant', 'app', 'graph'), 3600)
        return {'completed': True}
    async def settle(work_id):
        async with store.factory.begin() as db:
            await work.finish_parent_settlement(db, work_id)
    runner = ProviderWorkRunner(store.factory, resume, settle, concurrency=1)
    assert (await runner.run_once())['provider_waiting']
    assert await runner.run_once() is None
    async with store.factory.begin() as db:
        row = await db.get(models.ProviderWork, receipt.work_id)
        assert row.lease_owner_id is None
        row.next_eligible_at = datetime.now(timezone.utc)
    assert (await runner.run_once())['completed']
    async with store.factory() as db:
        unit = await db.scalar(select(models.ProviderWorkUnit))
        assert unit.attempts == 32 and unit.state == 'COMPLETED'
        assert (await db.get(models.ProviderWork, receipt.work_id)).state == 'COMPLETED'

async def test_chat_siblings_suspend_without_partial_and_completed_receipts_survive(store, source_adapter):
    from shared.provider_runtime import ProviderWorkRunner, provider_request_scope
    from shared.provider_admission import ProviderDeferred, ProviderRequestContext
    source = source_adapter.source
    source.chats = [{'id': 'aa_done'}, {'id': 'defer'}, {'id': 'sibling'}]
    calls = {'defer': 0}
    async def pages(cid):
        if cid == 'defer' and calls['defer'] == 0:
            calls['defer'] += 1
            async with provider_request_scope():
                raise ProviderDeferred(ProviderRequestContext('tenant', 'app', 'graph'), 600)
        yield []
    source.iter_message_pages = pages
    scope = await store.new_scope()
    async with store.factory.begin() as db:
        await work.ensure_work(db, snapshot_id=scope.snapshot_id, snapshot_token=0, job_token=0)
    async def resume(owner):
        return await source_adapter.run(scope, concurrency=1,
            snapshot_lease_token=owner.snapshot_token, job_lease_token=owner.job_token)
    async def settle(work_id):
        async with store.factory.begin() as db:
            await work.finish_parent_settlement(db, work_id)
    runner = ProviderWorkRunner(store.factory, resume, settle, concurrency=1)
    assert (await runner.run_once())['provider_waiting']
    async with store.factory.begin() as db:
        assert not (await db.scalars(select(models.SnapshotChatInventory).where(
            models.SnapshotChatInventory.coverage_status == 'PARTIAL'))).all()
        for row in (await db.scalars(select(models.ProviderWork))).all():
            row.next_eligible_at = datetime.now(timezone.utc)
    result = await runner.run_once()
    assert result['outcome'] == 'complete'
    async with store.factory() as db:
        assert len((await db.scalars(select(models.SnapshotChatInventory))).all()) == 3
        attempts = (await db.scalars(select(models.ChatCaptureAttempt).order_by(models.ChatCaptureAttempt.generation))).all()
        assert any(attempt.state == 'SUPERSEDED' for attempt in attempts)
        assert all(row.state == 'COMPLETED' for row in (await db.scalars(select(models.ProviderWorkUnit))).all())

from test_chat_catalogue_entry_postgres import source_adapter

async def test_worker_intake_is_durable_and_resume_does_not_reclaim_snapshot(store, monkeypatch, worker_module):
    from shared.provider_runtime import ProviderWorkRunner
    from unittest.mock import AsyncMock
    scope = await store.new_scope()
    async with store.factory() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
    monkeypatch.setenv('PROVIDER_WORK_ENABLED', 'true')
    monkeypatch.setattr(worker_module, 'async_session_factory', store.factory)
    worker = worker_module.BackupWorker.__new__(worker_module.BackupWorker)
    worker.worker_id = 'provider-intake'
    worker._heartbeat = None
    observed = []
    async def execute(job_id, message, resource_id):
        current = work.current_claim()
        async with store.factory() as db:
            resource = await db.get(models.Resource, scope.resource_id)
        resumed = await worker.create_snapshot(resource, message, job_id)
        observed.append((current, resumed.lease_token, message['priority']))
        async with store.factory.begin() as db:
            live = await db.get(models.Snapshot, resumed.id)
            live.status = models.SnapshotStatus.COMPLETED
        return {'completed': True}
    worker._process_single_backup = execute
    first = await worker.process_backup_message({'jobId': str(snapshot.job_id), 'resourceId': str(scope.resource_id)})
    assert first['provider_waiting'] and observed == []
    worker._settle_provider_parent = AsyncMock()
    runner = ProviderWorkRunner(store.factory, worker.resume_provider_work, worker._settle_provider_parent)
    await runner.run_once()
    assert len(observed) == 1
    current, token, priority = observed[0]
    assert current.snapshot_token == token == 1 and priority == 5
    async with store.factory() as db:
        assert (await db.get(models.ProviderWork, current.work_id)).state == 'COMPLETED'

# Keep the expensive worker import behind its existing test fixture.
import importlib.util
from pathlib import Path
_worker_spec = importlib.util.spec_from_file_location('provider_worker_fixture', Path(__file__).parents[1] / 'workers/test_durable_copy_worker_handoff.py')
_worker_fixture = importlib.util.module_from_spec(_worker_spec)
_worker_spec.loader.exec_module(_worker_fixture)
worker_module = _worker_fixture.worker_module

async def test_expired_turn_fences_live_old_capture_and_new_observations_start_now(store):
    from dataclasses import replace
    from test_chat_catalogue_postgres import message
    scope = await store.new_scope()
    async with store.factory.begin() as db:
        await work.ensure_work(db, snapshot_id=scope.snapshot_id, snapshot_token=0, job_token=0)
    first = await claim(store)
    lease = await store.begin(replace(scope, snapshot_lease_token=first.snapshot_token, job_lease_token=first.job_token))
    await store.stage(lease, [message()])
    async with store.factory.begin() as db:
        await db.execute(update(models.ProviderWork).values(lease_expires_at=datetime.now(timezone.utc)-timedelta(seconds=1)))
    fresh = await claim(store)
    current = await store.begin(replace(scope, snapshot_lease_token=fresh.snapshot_token, job_lease_token=fresh.job_token))
    assert current.generation > lease.generation
    async with store.factory() as db:
        assert not await db.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == scope.snapshot_id))

async def test_provider_recovery_cannot_adopt_a_replacement_parent_generation(store):
    pending = await enqueue(store)
    async with store.factory.begin() as db:
        await db.execute(update(models.Snapshot).where(models.Snapshot.id == pending.snapshot_id).values(lease_token=9))
    assert await claim(store) is None
    async with store.factory() as db:
        assert (await db.get(models.Snapshot, pending.snapshot_id)).lease_token == 9

async def test_required_copy_handoff_releases_provider_without_blocking_copy_resume(store, monkeypatch):
    from shared import copy_workflow
    from shared.provider_runtime import ProviderWorkRunner
    from uuid import uuid4
    monkeypatch.setenv('COPY_WORK_ENABLED', 'true')
    monkeypatch.setattr(copy_workflow, 'async_session_factory', store.factory)
    await enqueue(store)
    delegated = []
    async def resume(owner):
        with copy_workflow.copy_collection(owner.snapshot_id, expected_snapshot_token=owner.snapshot_token,
                expected_job_token=owner.job_token):
            delegated.append(await copy_workflow.defer_current_copy('CHAT_CARRY', {}))
            return await copy_workflow.seal_current_copy({'resume_kind': 'user_content', 'result': {'item_count': 0}})
    async def settle(work_id):
        raise AssertionError('copy workflow owns the parent settlement now')
    runner = ProviderWorkRunner(store.factory, resume, settle)
    assert (await runner.run_once())['copy_waiting']
    async with store.factory.begin() as db:
        provider = await db.scalar(select(models.ProviderWork))
        assert provider.state == 'COMPLETED' and not provider.parent_settle_pending
        await db.execute(update(models.CopyWorkTask).values(state='COMPLETED'))
    resumed = await copy_workflow.begin_resume(store.factory, delegated[0].workflow_id, uuid4())
    assert resumed is not None

async def test_migration17_is_additive_idempotent_and_retains_durable_receipts(store):
    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    spec = importlib.util.spec_from_file_location('provider_migration', Path(__file__).parents[2] / 'alembic/versions/0017_provider_work.py')
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    def apply(conn):
        with Operations.context(MigrationContext.configure(conn)):
            migration.upgrade()
            migration.upgrade()
            with pytest.raises(RuntimeError, match='forward migration'):
                migration.downgrade()
    async with store.engine.begin() as conn:
        for model in (models.ProviderWorkUnit, models.ProviderWork, models.ProviderWorkControl, models.ProviderWorkQueue):
            await conn.run_sync(model.__table__.drop)
        await conn.run_sync(apply)
    assert await enqueue(store)

async def test_provider_summary_is_content_free_and_scoped_to_requested_jobs(store, monkeypatch):
    from shared.provider_observability import provider_work_summaries
    pending = await enqueue(store)
    unrelated = await enqueue(store)
    active = await claim(store)
    async with store.factory.begin() as db:
        await work.record_unit_checkpoint(db, active, unit_key='opaque-secret-reference', kind='chat')
        summaries = await provider_work_summaries(db, job_ids=[pending.job_id])
    assert set(summaries) == {str(pending.job_id)}
    assert 'opaque-secret-reference' not in str(summaries)
    assert str(unrelated.job_id) not in str(summaries)
    assert summaries[str(pending.job_id)]['pending_leaves'] == 1

async def test_continuous_high_weight_arrivals_cannot_starve_backlogged_custom_work(store):
    low = await enqueue(store, weight=1)
    served = []
    for index in range(40):
        await enqueue(store, weight=8)
        owner = await claim(store)
        served.append(owner.work_id)
        async with store.factory.begin() as db:
            if owner.work_id == low.work_id:
                await work.suspend_current_work(db, owner, next_eligible_at=datetime.now(timezone.utc), reason='checkpoint')
            else:
                await work.finish_work(db, owner)
    assert served.count(low.work_id) >= 4
    assert all(low.work_id in served[start:start+16] for start in range(25))

async def test_chat_fair_checkpoint_keeps_published_receipt_and_finishes_active_observations(store, source_adapter, monkeypatch):
    from shared.provider_runtime import ProviderWorkRunner
    from unittest.mock import AsyncMock
    source = source_adapter.source
    source.chats = [{'id': 'a'}, {'id': 'b'}, {'id': 'c'}]
    scope = await store.new_scope()
    async with store.factory.begin() as db:
        await work.ensure_work(db, snapshot_id=scope.snapshot_id, snapshot_token=0, job_token=0)
    # The DB fairness decision is separately exercised by the sustained-arrivals test.
    monkeypatch.setattr('shared.provider_runtime.other_provider_work_waiting', AsyncMock(return_value=True))
    async def resume(owner):
        return await source_adapter.run(scope, concurrency=2,
            snapshot_lease_token=owner.snapshot_token, job_lease_token=owner.job_token)
    runner = ProviderWorkRunner(store.factory, resume, AsyncMock())
    assert (await runner.run_once())['provider_waiting']
    async with store.factory() as db:
        published = (await db.scalars(select(models.SnapshotChatInventory))).all()
        assert len(published) == 2 and all(row.coverage_status == 'COMPLETE' for row in published)
        assert all(attempt.state == 'PUBLISHED' for attempt in (await db.scalars(select(models.ChatCaptureAttempt))).all())
    assert (await runner.run_once())['outcome'] == 'complete'
    # Each source observation ran exactly twice; fairness did not restart a proof.
    assert sorted(source.message_calls) == ['a', 'a', 'b', 'b', 'c', 'c']

async def test_swallowed_defer_cannot_complete_and_remaining_provider_tasks_are_joined(store):
    import asyncio
    from unittest.mock import AsyncMock
    from shared.provider_runtime import ProviderWorkRunner, provider_request_scope, ProviderTurnDeferred
    from shared.provider_admission import ProviderDeferred, ProviderRequestContext
    await enqueue(store)
    started, closed = asyncio.Event(), asyncio.Event()
    child_tasks = []
    async def child():
        try:
            async with provider_request_scope():
                started.set()
                await asyncio.Event().wait()
        finally:
            closed.set()
    async def resume(owner):
        child_tasks.append(asyncio.create_task(child()))
        await started.wait()
        try:
            async with provider_request_scope():
                raise ProviderDeferred(ProviderRequestContext('tenant','app','graph'), 600)
        except ProviderTurnDeferred:
            return {'legacy_collector_swallowed': True}
    runner = ProviderWorkRunner(store.factory, resume, AsyncMock())
    try:
        result = await runner.run_once()
        assert result.get('provider_waiting') and closed.is_set()
        assert child_tasks[0].done()
    finally:
        for task in child_tasks:
            task.cancel()
        await asyncio.gather(*child_tasks, return_exceptions=True)

async def test_renew_can_finish_owned_receipt_after_snapshot_becomes_terminal(store):
    await enqueue(store)
    owner = await claim(store)
    async with store.factory.begin() as db:
        snapshot = await db.get(models.Snapshot, owner.snapshot_id)
        snapshot.status = models.SnapshotStatus.COMPLETED
    async with store.factory.begin() as db:
        await work.renew_work(db, owner)
        await work.finish_work(db, owner)
        assert not await work.finish_parent_settlement(db, owner.work_id)
        (await db.get(models.Job, owner.job_id)).status = models.JobStatus.COMPLETED
        await db.flush()
        assert await work.finish_parent_settlement(db, owner.work_id)

async def test_lost_lease_interrupts_turn_without_removing_worker_capacity(store):
    import asyncio
    from unittest.mock import AsyncMock
    from shared.provider_runtime import ProviderWorkRunner
    await enqueue(store)
    started = asyncio.Event()
    async def resume(owner):
        started.set()
        await asyncio.Event().wait()
    runner = ProviderWorkRunner(store.factory, resume, AsyncMock(), renew_s=.01)
    task = asyncio.create_task(runner.run_once())
    await started.wait()
    async with store.factory.begin() as db:
        await work.cancel_provider_work(db, snapshot_id=(await db.scalar(select(models.ProviderWork.snapshot_id))))
    assert (await asyncio.wait_for(task, 2))['provider_lease_lost']
    await enqueue(store)
    runner.resume_callback = AsyncMock(return_value={'completed': True})
    assert (await runner.run_once())['completed']
