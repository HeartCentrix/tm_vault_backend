"""Required-copy registration and seal survive complete transaction failures."""
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text

from test_chat_catalogue_postgres import store
from shared import copy_workflow as workflow, models

pytestmark = pytest.mark.integration


class ConnectionDoesNotExistError(RuntimeError):
    pass


class FaultFactory:
    """Inject failures at transaction exit while retaining actual DB effects."""
    def __init__(self, factory, *, before_commit=None, after_commit=None, after_rollback=None):
        self.factory = factory
        self.before_commit = before_commit
        self.after_commit = after_commit
        self.after_rollback = after_rollback
        self.attempts = 0

    def __call__(self):
        return self.factory()

    @asynccontextmanager
    async def begin(self):
        self.attempts += 1
        number = self.attempts
        try:
            async with self.factory.begin() as session:
                yield session
                if self.before_commit is not None:
                    await self.before_commit(session, number)
        except Exception:
            if self.after_rollback is not None:
                await self.after_rollback(number)
            raise
        if self.after_commit is not None:
            await self.after_commit(number)


@pytest.fixture
def enabled(monkeypatch, store):
    monkeypatch.setenv('COPY_WORK_ENABLED', 'true')
    monkeypatch.setattr(workflow, '_COPY_GUARDS_ACTIVE', False)
    monkeypatch.setattr(workflow, 'async_session_factory', store.factory)
    return workflow


async def abort(session, code):
    await session.execute(text("DO $$ BEGIN RAISE EXCEPTION 'injected transaction abort' USING ERRCODE = '" + code + "'; END $$"))


@pytest.mark.parametrize('failure', ['40P01', '40001', 'connection'])
async def test_registration_retries_whole_transaction_without_poisoning_seal(store, monkeypatch, enabled, failure):
    target = await store.new_scope()
    original = enabled.enqueue
    txids = []

    async def enqueue(session, request):
        txids.append(await session.scalar(select(func.txid_current())))
        task = await original(session, request)
        if len(txids) == 1:
            if failure == 'connection':
                await session.execute(text('SELECT pg_terminate_backend(pg_backend_pid())'))
            else:
                await abort(session, failure)
        return task

    monkeypatch.setattr(enabled, 'enqueue', enqueue)
    with enabled.copy_collection(target.snapshot_id) as capture:
        deferred = await enabled.defer_current_copy('CHAT_CARRY', {})
        assert capture.error is None
        result = await enabled.seal_current_copy({'resume_kind': 'user_content'})
    assert len(txids) == len(set(txids)) == 2
    assert result['copy_workflow_id'] == str(deferred.workflow_id)
    async with store.factory() as session:
        assert await session.scalar(select(func.count()).select_from(models.CopyWorkTask)) == 1
        assert (await session.get(models.Snapshot, target.snapshot_id)).lease_token == 1


async def test_registration_lost_commit_ack_reuses_same_intent(store, monkeypatch, enabled):
    target = await store.new_scope()

    async def lost_ack(number):
        if number == 1:
            raise ConnectionDoesNotExistError('connection is closed')

    faulty = FaultFactory(store.factory, after_commit=lost_ack)
    monkeypatch.setattr(enabled, 'async_session_factory', faulty)
    with enabled.copy_collection(target.snapshot_id) as capture:
        deferred = await enabled.defer_current_copy('CHAT_CARRY', {})
        assert capture.error is None
        await enabled.seal_current_copy({'resume_kind': 'user_content'})
    async with store.factory() as session:
        tasks = list((await session.scalars(select(models.CopyWorkTask))).all())
        assert len(tasks) == 1
        assert tasks[0].id == deferred.task_id


async def test_registration_exhaustion_latches_first_failure_across_later_success(store, monkeypatch, enabled):
    target = await store.new_scope()
    original = enabled.enqueue
    attempts = []

    async def unavailable(session, request):
        attempts.append(await session.scalar(select(func.txid_current())))
        await original(session, request)
        await abort(session, '40001')

    monkeypatch.setattr(enabled, 'enqueue', unavailable)
    with enabled.copy_collection(target.snapshot_id) as capture:
        with pytest.raises(Exception):
            await enabled.defer_current_copy('CHAT_CARRY', {})
        first_error = capture.error
        assert len(set(attempts)) == 3
        monkeypatch.setattr(enabled, 'enqueue', original)
        await enabled.defer_current_copy('CHAT_CARRY', {})
        assert capture.error is first_error
        with pytest.raises(enabled.CopyWorkConflict, match='persistence failed'):
            await enabled.seal_current_copy({'resume_kind': 'user_content'})


async def test_permanent_registration_failure_is_not_overwritten(store, monkeypatch, enabled):
    target = await store.new_scope()
    with enabled.copy_collection(target.snapshot_id) as capture:
        with pytest.raises(ValueError):
            await enabled.defer_current_copy('NOT_A_COPY', {})
        first_error = capture.error
        with pytest.raises(enabled.CopyWorkConflict):
            await enabled.defer_current_copy('CHAT_CARRY', {'target_snapshot_id': str(uuid4())})
        assert capture.error is first_error
        await enabled.defer_current_copy('CHAT_CARRY', {})
        with pytest.raises(enabled.CopyWorkConflict, match='persistence failed'):
            await enabled.seal_current_copy({'resume_kind': 'user_content'})


@pytest.mark.parametrize('code', ['40P01', '40001'])
async def test_seal_failed_commit_retries_without_publishing_uncommitted_result(store, monkeypatch, enabled, code):
    target = await store.new_scope()
    with enabled.copy_collection(target.snapshot_id) as capture:
        deferred = await enabled.defer_current_copy('CHAT_CARRY', {})

        async def fail_first(session, number):
            assert capture.sealed_result is None
            if number == 1:
                await abort(session, code)

        faulty = FaultFactory(store.factory, before_commit=fail_first)
        monkeypatch.setattr(enabled, 'async_session_factory', faulty)
        result = await enabled.seal_current_copy({'resume_kind': 'user_content'})
        assert capture.sealed_result == result
    assert faulty.attempts == 2
    async with store.factory() as session:
        assert (await session.get(models.Snapshot, target.snapshot_id)).lease_token == 1
        assert (await session.get(models.CopyWorkflow, deferred.workflow_id)).generation == 1


async def test_exhausted_seal_commit_never_leaves_success_marker(store, monkeypatch, enabled):
    target = await store.new_scope()
    with enabled.copy_collection(target.snapshot_id) as capture:
        deferred = await enabled.defer_current_copy('CHAT_CARRY', {})

        async def unavailable(session, number):
            await abort(session, '40001')

        faulty = FaultFactory(store.factory, before_commit=unavailable)
        monkeypatch.setattr(enabled, 'async_session_factory', faulty)
        with pytest.raises(Exception):
            await enabled.seal_current_copy({'resume_kind': 'user_content'})
        assert capture.sealed_result is None
    assert faulty.attempts == 3
    async with store.factory() as session:
        assert (await session.get(models.Snapshot, target.snapshot_id)).lease_token == 0
        assert (await session.get(models.CopyWorkflow, deferred.workflow_id)).state == 'DRAINING'


async def test_seal_lost_commit_ack_reads_original_receipt_without_second_increment(store, monkeypatch, enabled):
    target = await store.new_scope()
    with enabled.copy_collection(target.snapshot_id) as capture:
        deferred = await enabled.defer_current_copy('CHAT_CARRY', {})

        async def lost_ack(number):
            assert capture.sealed_result is None
            if number == 1:
                raise ConnectionDoesNotExistError('connection is closed')

        faulty = FaultFactory(store.factory, after_commit=lost_ack)
        monkeypatch.setattr(enabled, 'async_session_factory', faulty)
        result = await enabled.seal_current_copy({'resume_kind': 'user_content', 'result': {'item_count': 7}})
        assert capture.sealed_result == result
    async with store.factory() as session:
        assert (await session.get(models.Snapshot, target.snapshot_id)).lease_token == 1
        row = await session.get(models.CopyWorkflow, deferred.workflow_id)
        assert row.generation == 1 and row.continuation['result'] == {'item_count': 7}


@pytest.mark.parametrize('change', ['owner', 'continuation', 'capture'])
async def test_lost_ack_cannot_adopt_replaced_or_conflicting_seal(store, monkeypatch, enabled, change):
    target = await store.new_scope()
    with enabled.copy_collection(target.snapshot_id) as capture:
        deferred = await enabled.defer_current_copy('CHAT_CARRY', {})

        async def lost_ack(number):
            if number != 1:
                return
            async with store.factory.begin() as session:
                row = await session.get(models.CopyWorkflow, deferred.workflow_id)
                snapshot = await session.get(models.Snapshot, target.snapshot_id)
                if change == 'owner':
                    row.state = 'RESUMING'
                    row.generation += 1
                    row.lease_owner_id = uuid4()
                    snapshot.lease_token += 1
                    snapshot.lease_owner_id = row.lease_owner_id
                    row.parent_lease_token = snapshot.lease_token
                elif change == 'capture':
                    row.capture_attempt += 1
                else:
                    row.continuation = {'resume_kind': 'conflicting'}
            raise ConnectionDoesNotExistError('connection is closed')

        faulty = FaultFactory(store.factory, after_commit=lost_ack)
        monkeypatch.setattr(enabled, 'async_session_factory', faulty)
        with pytest.raises(enabled.CopyWorkConflict):
            await enabled.seal_current_copy({'resume_kind': 'user_content'})
        assert capture.sealed_result is None


@pytest.mark.parametrize('code', ['25P02', '55P03', '57014', '23503', None])
async def test_registration_does_not_retry_non_connection_database_failures(store, monkeypatch, enabled, code):
    target = await store.new_scope()
    original = enabled.enqueue
    executions = []

    async def fail(session, request):
        executions.append(await session.scalar(select(func.txid_current())))
        await original(session, request)
        if code is None:
            raise RuntimeError('canceling statement due to statement timeout')
        await abort(session, code)

    monkeypatch.setattr(enabled, 'enqueue', fail)
    with enabled.copy_collection(target.snapshot_id) as capture:
        with pytest.raises(Exception):
            await enabled.defer_current_copy('CHAT_CARRY', {})
        assert capture.error is not None
    assert len(executions) == 1
    async with store.factory() as session:
        assert await session.scalar(select(func.count()).select_from(models.CopyWorkTask)) == 0


async def test_final_lost_ack_is_resolved_read_only_after_retry_budget(store, monkeypatch, enabled):
    target = await store.new_scope()
    with enabled.copy_collection(target.snapshot_id) as capture:
        deferred = await enabled.defer_current_copy('CHAT_CARRY', {})

        async def lose_first_three(number):
            assert capture.sealed_result is None
            if number <= 3:
                raise ConnectionDoesNotExistError('connection is closed')

        faulty = FaultFactory(store.factory, after_commit=lose_first_three)
        monkeypatch.setattr(enabled, 'async_session_factory', faulty)
        result = await enabled.seal_current_copy({'resume_kind': 'user_content'})
        assert capture.sealed_result == result
    assert faulty.attempts == 4
    async with store.factory() as session:
        assert (await session.get(models.Snapshot, target.snapshot_id)).lease_token == 1
        assert (await session.get(models.CopyWorkflow, deferred.workflow_id)).generation == 1


async def test_seal_redelivery_reuses_receipt_but_rejects_different_continuation(store, enabled):
    target = await store.new_scope()
    with enabled.copy_collection(target.snapshot_id):
        await enabled.defer_current_copy('CHAT_CARRY', {})
        first = await enabled.seal_current_copy({'resume_kind': 'user_content'})
        assert await enabled.seal_current_copy({'resume_kind': 'user_content'}) == first
        with pytest.raises(enabled.CopyWorkConflict, match='continuation'):
            await enabled.seal_current_copy({'resume_kind': 'conflicting'})
    async with store.factory() as session:
        assert (await session.get(models.Snapshot, target.snapshot_id)).lease_token == 1


async def test_final_receipt_read_does_not_retry_stale_owner_from_connection_context(store, monkeypatch, enabled):
    target = await store.new_scope()
    with enabled.copy_collection(target.snapshot_id) as capture:
        deferred = await enabled.defer_current_copy('CHAT_CARRY', {})

        async def lose_ack(number):
            if number > 3:
                return
            if number == 3:
                async with store.factory.begin() as session:
                    row = await session.get(models.CopyWorkflow, deferred.workflow_id)
                    row.generation += 1
            raise ConnectionDoesNotExistError('connection is closed')

        faulty = FaultFactory(store.factory, after_commit=lose_ack)
        monkeypatch.setattr(enabled, 'async_session_factory', faulty)
        with pytest.raises(enabled.CopyWorkConflict):
            await enabled.seal_current_copy({'resume_kind': 'user_content'})
        assert capture.sealed_result is None
    assert faulty.attempts == 4


async def add_partition(store, target):
    partition_id = uuid4()
    async with store.factory.begin() as session:
        snapshot = await session.get(models.Snapshot, target.snapshot_id)
        session.add(models.SnapshotPartition(id=partition_id, snapshot_id=snapshot.id,
            tenant_id=store.tenant, resource_id=store.resource, job_id=snapshot.job_id,
            drive_id='', file_ids=[], partition_index=0, total_files=0, total_bytes_est=0,
            partition_type='CHATS', status='IN_PROGRESS', lease_token=0))
    return partition_id


@pytest.mark.parametrize('parent', ['job', 'snapshot', 'partition'])
async def test_registration_retry_keeps_original_parent_generation(store, monkeypatch, enabled, parent):
    target = await store.new_scope()
    partition_id = await add_partition(store, target) if parent == 'partition' else None

    async def fail_first(session, number):
        if number == 1:
            raise ConnectionDoesNotExistError('connection is closed')

    async def replace_owner(number):
        if number != 1:
            return
        async with store.factory.begin() as session:
            row = await session.get(models.Snapshot, target.snapshot_id)
            if parent == 'job':
                row = await session.get(models.Job, row.job_id)
            elif parent == 'partition':
                row = await session.get(models.SnapshotPartition, partition_id)
            row.lease_token += 1
            row.lease_owner_id = uuid4()

    faulty = FaultFactory(store.factory, before_commit=fail_first, after_rollback=replace_owner)
    monkeypatch.setattr(enabled, 'async_session_factory', faulty)
    with enabled.copy_collection(target.snapshot_id, partition_id) as capture:
        with pytest.raises(enabled.CopyWorkConflict, match='generation'):
            await enabled.defer_current_copy('CHAT_CARRY', {})
        assert isinstance(capture.error, enabled.CopyWorkConflict)
    assert faulty.attempts == 2
    async with store.factory() as session:
        assert await session.scalar(select(func.count()).select_from(models.CopyWorkTask)) == 0


async def test_partition_lost_ack_does_not_increment_snapshot_or_second_leaf_lease(store, monkeypatch, enabled):
    target = await store.new_scope()
    partition_id = await add_partition(store, target)
    with enabled.copy_collection(target.snapshot_id, partition_id) as capture:
        deferred = await enabled.defer_current_copy('CHAT_CARRY', {})

        async def lost_ack(number):
            if number == 1:
                raise ConnectionDoesNotExistError('connection is closed')

        monkeypatch.setattr(enabled, 'async_session_factory', FaultFactory(store.factory, after_commit=lost_ack))
        result = await enabled.seal_current_copy({'resume_kind': 'partition', 'partition_id': str(partition_id)})
        assert capture.sealed_result == result
    async with store.factory() as session:
        assert (await session.get(models.Snapshot, target.snapshot_id)).lease_token == 0
        assert (await session.get(models.SnapshotPartition, partition_id)).lease_token == 1
        assert (await session.get(models.CopyWorkflow, deferred.workflow_id)).generation == 1


async def test_registration_lost_ack_preserves_durable_source_choice(store, monkeypatch, enabled):
    prior, newer = await store.new_scope(), await store.new_scope()
    async with store.factory.begin() as session:
        (await session.get(models.Snapshot, prior.snapshot_id)).status = models.SnapshotStatus.COMPLETED
        for source in (prior, newer):
            session.add(models.SnapshotItem(snapshot_id=source.snapshot_id, tenant_id=store.tenant,
                external_id='source-message', item_type='TEAMS_CHAT_MESSAGE', name='captured',
                content_size=7, backend_id=store.backend))
    target = await store.new_scope()

    async def lose_ack(number):
        if number == 1:
            async with store.factory.begin() as session:
                (await session.get(models.Snapshot, newer.snapshot_id)).status = models.SnapshotStatus.COMPLETED
            raise ConnectionDoesNotExistError('connection is closed')

    monkeypatch.setattr(enabled, 'async_session_factory', FaultFactory(store.factory, after_commit=lose_ack))
    with enabled.copy_collection(target.snapshot_id):
        deferred = await enabled.defer_current_copy('CHAT_CARRY', {})
    async with store.factory() as session:
        task = await session.get(models.CopyWorkTask, deferred.task_id)
        assert task.params['prior_snapshot_id'] == str(prior.snapshot_id)
        pins = list((await session.scalars(select(models.CopyWorkItem.source_snapshot_id).where(
            models.CopyWorkItem.task_id == task.id))).all())
        assert pins == [prior.snapshot_id]
