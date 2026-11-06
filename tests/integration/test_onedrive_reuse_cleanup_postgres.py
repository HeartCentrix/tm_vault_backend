"""A reused blob must have a durable target reference before cleanup can claim its source."""
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from test_chat_catalogue_postgres import store
from test_copy_worker_handoff_postgres import bw
from test_onedrive_resume_postgres import setup_worker, file_item
from shared import models
from shared.backup_cleanup import BlobCandidate, select_deletable_blob_paths

pytestmark = pytest.mark.integration


async def prior_and_target(store, bw, monkeypatch):
    prior = await setup_worker(store, bw, monkeypatch, [file_item()])
    assert await prior.run() == (1, 1024)
    async with store.factory.begin() as session:
        (await session.get(models.Snapshot, prior.scope.snapshot_id)).status = models.SnapshotStatus.COMPLETED
    target = await setup_worker(store, bw, monkeypatch, [file_item()])
    return prior, target


@pytest.mark.parametrize('hash_missing', [False, True])
async def test_cleanup_cannot_delete_reused_bytes_during_graph_verification(store, bw, monkeypatch, hash_missing):
    prior, target = await prior_and_target(store, bw, monkeypatch)
    if hash_missing:
        async with store.factory.begin() as session:
            item = await session.scalar(select(models.SnapshotItem).where(
                models.SnapshotItem.snapshot_id == prior.scope.snapshot_id))
            item.content_hash = None  # The cleanup join uses content_checksum.
    original = target.graph.get_download_urls_batch
    checked = []

    async def urls(*args, **kwargs):
        async with store.factory.begin() as session:
            (await session.get(models.Snapshot, prior.scope.snapshot_id)).status = models.SnapshotStatus.PENDING_DELETION
        async with store.factory() as session:
            item = await session.scalar(select(models.SnapshotItem).where(
                models.SnapshotItem.snapshot_id == prior.scope.snapshot_id))
            plan = await select_deletable_blob_paths(session,
                doomed_snapshot_ids=[prior.scope.snapshot_id], candidates=[BlobCandidate(
                    blob_path=item.blob_path, snapshot_id=prior.scope.snapshot_id,
                    tenant_id=store.tenant, content_checksum=item.content_checksum, backend_id=store.backend)])
            checked.append(plan)
            assert item.blob_path not in plan.deletable
        return await original(*args, **kwargs)

    target.graph.get_download_urls_batch = urls
    assert await target.run() == (0, 0)
    assert checked


async def test_cleanup_claim_before_reuse_pin_falls_back_to_download(store, bw, monkeypatch):
    prior, target = await prior_and_target(store, bw, monkeypatch)
    original = AsyncSession.execute
    claimed, downloads = [], []

    async def execute(session, statement, *args, **kwargs):
        result = await original(session, statement, *args, **kwargs)
        sql = str(statement)
        if not claimed and 'ORDER BY snapshot_items.created_at DESC' in sql:
            claimed.append(True)
            async with store.factory.begin() as other:
                (await other.get(models.Snapshot, prior.scope.snapshot_id)).status = models.SnapshotStatus.PENDING_DELETION
        return result

    async def stream(**kwargs):
        downloads.append(kwargs['blob_path'])
        return {'success': True, 'content_sha256': 'new-digest'}

    target.worker._stream_graph_to_backend = stream
    monkeypatch.setattr(AsyncSession, 'execute', execute)
    assert await target.run() == (1, 1024)
    assert claimed and downloads == [f'{target.scope.snapshot_id}/file-1']
