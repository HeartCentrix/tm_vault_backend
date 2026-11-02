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
        assert (unit.attempts, unit.waited_s, unit.reconciliations) == (2, 7, 1)
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
        await work.finish_parent_settlement(db, replacement.work_id)
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
    source.chats = [{'id': 'done'}, {'id': 'defer'}, {'id': 'sibling'}]
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
