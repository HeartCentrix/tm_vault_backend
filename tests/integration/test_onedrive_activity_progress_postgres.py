"""OneDrive Activity counts only durable shard receipts, including resumed work."""
import asyncio

import pytest
from sqlalchemy import event, select

from test_chat_catalogue_postgres import store
from test_copy_worker_handoff_postgres import bw
from test_onedrive_resume_postgres import setup_worker, file_item, add_partition
from shared import models

pytestmark = pytest.mark.integration


async def test_shard_capture_progress_sums_durable_receipts_without_replay_double_count(store, bw, monkeypatch):
    items = [file_item("file-1"), file_item("file-2")]
    env = await setup_worker(store, bw, monkeypatch, items)
    pids = [await add_partition(store, env.scope, [item], i) for i, item in enumerate(items)]
    in_flight, both_started = [], asyncio.Event()

    async def stream(**kwargs):
        in_flight.append(kwargs["blob_path"])
        if len(in_flight) >= 2:
            both_started.set()
        await asyncio.wait_for(both_started.wait(), timeout=2)
        return {"success": True, "content_sha256": "content-digest"}

    env.worker._stream_graph_to_backend = stream

    async def shard(index):
        bw._bind_partition_lease(pids[index], {"snapshot_id": env.scope.snapshot_id, "lease_token": 1})
        return await env.run([items[index]])

    assert await asyncio.gather(shard(0), shard(1)) == [(1, 1024), (1, 1024)]
    assert await asyncio.gather(shard(0), shard(1)) == [(0, 0), (0, 0)]
    assert sorted(env.calls) == ["file-1", "file-1", "file-2", "file-2"]
    async with store.factory() as session:
        partitions = (await session.scalars(select(models.SnapshotPartition).where(
            models.SnapshotPartition.id.in_(pids)))).all()
        assert [p.status for p in partitions] == ['IN_PROGRESS', 'IN_PROGRESS']
        assert sum(p.payload['onedrive_capture_progress']['bytes'] for p in partitions) == 2048
        assert sum(p.payload['onedrive_capture_progress']['files'] for p in partitions) == 2


async def test_capture_progress_rolls_back_with_blob_links(store, bw, monkeypatch):
    env = await setup_worker(store, bw, monkeypatch, [file_item()])
    pid = await add_partition(store, env.scope, [file_item()])
    async with store.factory.begin() as session:
        partition = await session.get(models.SnapshotPartition, pid)
        partition.payload = {'preserved': True}
    bw._bind_partition_lease(pid, {'snapshot_id': env.scope.snapshot_id, 'lease_token': 1})

    def reject_progress(conn, cursor, statement, parameters, context, executemany):
        if ('UPDATE snapshot_partitions' in statement
                and any(p.get('bytes', 0) > 0 for p in context.compiled_parameters or [])):
            raise RuntimeError('synthetic progress write failure')

    event.listen(store.engine.sync_engine, 'before_cursor_execute', reject_progress)
    try:
        with pytest.raises(RuntimeError, match='progress write failure'):
            await env.run()
    finally:
        event.remove(store.engine.sync_engine, 'before_cursor_execute', reject_progress)
    async with store.factory() as session:
        partition = await session.get(models.SnapshotPartition, pid)
        item = await session.scalar(select(models.SnapshotItem).where(
            models.SnapshotItem.snapshot_id == env.scope.snapshot_id))
        assert partition.payload == {'preserved': True, 'onedrive_capture_progress': {
            'version': 1, 'files': 0, 'bytes': 0}}
        assert item.blob_path is None


async def test_live_progress_is_visible_while_another_file_is_still_downloading(store, bw, monkeypatch):
    items = [file_item('file-1'), file_item('file-2')]
    env = await setup_worker(store, bw, monkeypatch, items)
    pid = await add_partition(store, env.scope, items)
    bw._bind_partition_lease(pid, {'snapshot_id': env.scope.snapshot_id, 'lease_token': 1})
    monkeypatch.setenv('ONEDRIVE_FLUSH_BACKLOG_CAP', '1')
    release = asyncio.Event()

    async def stream(**kwargs):
        if kwargs['blob_path'].endswith('/file-2'):
            await release.wait()
        return {'success': True, 'content_sha256': 'content-digest'}

    env.worker._stream_graph_to_backend = stream
    task = asyncio.create_task(env.run())

    async def wait_for_first_receipt():
        while True:
            async with store.factory() as session:
                partition = await session.get(models.SnapshotPartition, pid)
                if (partition.payload or {}).get('onedrive_capture_progress', {}).get('bytes') == 1024:
                    snapshot = await session.get(models.Snapshot, env.scope.snapshot_id)
                    assert snapshot.status == models.SnapshotStatus.IN_PROGRESS
                    assert partition.status == 'IN_PROGRESS'
                    assert partition.payload['onedrive_capture_progress']['files'] == 1
                    return
            await asyncio.sleep(0.01)

    try:
        await asyncio.wait_for(wait_for_first_receipt(), timeout=5)
        assert not task.done()
    finally:
        release.set()
        result = await asyncio.wait_for(task, timeout=5)
    assert result == (2, 2048)


async def test_failed_partition_keeps_committed_capture_progress(store, bw, monkeypatch):
    env = await setup_worker(store, bw, monkeypatch, [file_item()])
    pid = await add_partition(store, env.scope, [file_item()])
    async with store.factory.begin() as session:
        partition = await session.get(models.SnapshotPartition, pid)
        partition.payload = {'preserved': True}
    bw._bind_partition_lease(pid, {'snapshot_id': env.scope.snapshot_id, 'lease_token': 1})
    assert await env.run() == (1, 1024)
    assert await env.worker._mark_partition_terminal(pid, status='FAILED')
    async with store.factory() as session:
        partition = await session.get(models.SnapshotPartition, pid)
        assert partition.status == 'FAILED'
        assert partition.bytes_uploaded == 0
        assert partition.payload == {'preserved': True, 'onedrive_capture_progress': {
            'version': 1, 'files': 1, 'bytes': 1024}}


async def test_partition_capture_progress_under_provider_claim(store, bw, monkeypatch):
    from shared import provider_runtime, provider_work
    env = await setup_worker(store, bw, monkeypatch, [file_item()])
    pid = await add_partition(store, env.scope, [file_item()])
    async with store.factory.begin() as session:
        await provider_work.ensure_work(session, snapshot_id=env.scope.snapshot_id,
            snapshot_token=0, job_token=0, partition_id=pid, partition_token=1,
            continuation={'kind': 'partition'})

    async def resume(owner):
        bw._bind_partition_lease(pid, {'snapshot_id': env.scope.snapshot_id,
            'lease_token': owner.parent_token, 'lease_owner_id': owner.owner_id})
        return await env.run()

    async def settle(work_id):
        pass

    runner = provider_runtime.ProviderWorkRunner(store.factory, resume, settle)
    assert await asyncio.wait_for(runner.run_once(), timeout=5) == (1, 1024)
    async with store.factory() as session:
        partition = await session.get(models.SnapshotPartition, pid)
        assert partition.payload['onedrive_capture_progress'] == {
            'version': 1, 'files': 1, 'bytes': 1024}


async def test_lost_partition_lease_cannot_advance_capture_progress(store, bw, monkeypatch):
    env = await setup_worker(store, bw, monkeypatch, [file_item()])
    pid = await add_partition(store, env.scope, [file_item()])
    bw._bind_partition_lease(pid, {'snapshot_id': env.scope.snapshot_id, 'lease_token': 1})

    async def stream(**kwargs):
        async with store.factory.begin() as session:
            partition = await session.get(models.SnapshotPartition, pid)
            partition.lease_token += 1
        return {'success': True, 'content_sha256': 'content-digest'}

    env.worker._stream_graph_to_backend = stream
    with pytest.raises(RuntimeError, match='ownership|lease'):
        await env.run()
    async with store.factory() as session:
        partition = await session.get(models.SnapshotPartition, pid)
        item = await session.scalar(select(models.SnapshotItem).where(
            models.SnapshotItem.snapshot_id == env.scope.snapshot_id))
        assert partition.payload['onedrive_capture_progress']['bytes'] == 0
        assert item.blob_path is None
