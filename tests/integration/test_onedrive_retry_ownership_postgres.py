"""OneDrive rescue must not repeat completed work or race the foreground owner."""
import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select, text, update

from test_chat_catalogue_postgres import store
from test_copy_worker_handoff_postgres import bw
from test_onedrive_resume_postgres import file_item, setup_worker
from shared import models

pytestmark = pytest.mark.integration


async def prepare(store, bw, monkeypatch, *, status="PENDING", attempts=0,
                  snapshot_status=models.SnapshotStatus.FAILED, receipt=False):
    item = file_item()
    env = await setup_worker(store, bw, monkeypatch, [item])
    monkeypatch.setenv("ONEDRIVE_RETRY_QUEUE_ENABLED", "true")
    monkeypatch.setenv("ONEDRIVE_RETRY_MAX_ATTEMPTS", "3")
    monkeypatch.setenv("ONEDRIVE_RETRY_HEARTBEAT_S", "1")
    monkeypatch.setenv("ONEDRIVE_RETRY_STALL_MINUTES", "5")
    async with store.factory.begin() as session:
        await session.execute(text("CREATE UNIQUE INDEX retry_snapshot_file ON onedrive_file_retries(snapshot_id, file_external_id)"))
        snapshot = await session.get(models.Snapshot, env.scope.snapshot_id)
        snapshot.status = snapshot_status
        snapshot.extra_data = {"failure_reason": "OneDrive required content persistence failed: EnumerationFailed"}
        env.job_id = snapshot.job_id
        row = models.OneDriveFileRetry(tenant_id=store.tenant, resource_id=store.resource,
            snapshot_id=snapshot.id, file_external_id=item["id"], file_name=item["name"],
            drive_id="drive-A", file_payload=deepcopy(item), status=status,
            attempt_count=attempts, next_retry_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        session.add(row)
        await session.flush()
        env.retry_id = row.id
        if receipt:
            captured = await session.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == snapshot.id))
            captured.blob_path = "existing/verified-file"
            captured.extra_data = {"raw": deepcopy(item), "onedrive_download_source": deepcopy(item)}
    env.source = deepcopy(item)
    env.graph.get_download_url = AsyncMock(side_effect=lambda *a, **kw:
        ("https://download.invalid/file", 1024, "", deepcopy(env.source)))
    env.graph.aclose = AsyncMock()
    env.worker._build_graph_client_for = AsyncMock(return_value=env.graph)
    env.transfers = []

    async def stream(**kwargs):
        env.transfers.append(kwargs["blob_path"])
        return {"success": True, "content_sha256": "digest", "blob_path": kwargs["blob_path"]}

    env.worker._stream_graph_to_backend = stream
    bw.azure_storage_manager.build_blob_path = lambda tenant, resource, snapshot, item, timestamp=None: f"{snapshot}/{timestamp or 'time'}/{item}"

    async def enqueue():
        async with store.factory() as session:
            tenant = await session.get(models.Tenant, store.tenant)
            resource = await session.get(models.Resource, store.resource)
            snapshot = await session.get(models.Snapshot, env.scope.snapshot_id)
        await env.worker._enqueue_onedrive_retry(tenant=tenant, resource=resource, snapshot=snapshot,
            file_payload=deepcopy(item), drive_id="drive-A", err_text="ReadError")

    async def drain():
        return await env.worker._drain_onedrive_retry_batch(batch_size=20, max_attempts=3)

    env.enqueue, env.drain = enqueue, drain
    return env


async def state(store, env):
    async with store.factory() as session:
        retry = await session.get(models.OneDriveFileRetry, env.retry_id)
        item = await session.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == env.scope.snapshot_id))
        snapshot = await session.get(models.Snapshot, env.scope.snapshot_id)
        return retry, item, snapshot


@pytest.mark.parametrize("status", ["IN_PROGRESS", "RESCUED", "FAILED_PERMANENT"])
async def test_enqueue_never_reopens_owned_or_terminal_retry(store, bw, monkeypatch, status):
    env = await prepare(store, bw, monkeypatch, status=status, attempts=2)
    before, _, _ = await state(store, env)
    await env.enqueue()
    after, _, _ = await state(store, env)
    assert after.status == status and after.attempt_count == 2
    assert after.updated_at == before.updated_at


@pytest.mark.parametrize("blocked", ["foreground", "cancelled", "partition", "provider"])
async def test_claim_waits_until_foreground_is_terminal_and_quiet(store, bw, monkeypatch, blocked):
    env = await prepare(store, bw, monkeypatch)
    async with store.factory.begin() as session:
        if blocked == "foreground":
            (await session.get(models.Snapshot, env.scope.snapshot_id)).status = models.SnapshotStatus.IN_PROGRESS
        elif blocked == "cancelled":
            (await session.get(models.Job, env.job_id)).status = models.JobStatus.CANCELLING
        elif blocked == "partition":
            session.add(models.SnapshotPartition(snapshot_id=env.scope.snapshot_id, job_id=env.job_id,
                resource_id=store.resource, tenant_id=store.tenant, partition_index=0,
                file_ids=["file-1"], drive_id="drive-A", status="IN_PROGRESS"))
        else:
            from shared.provider_work import ensure_work
            # Use the real durable work constructor and leave it pending.
            snapshot = await session.get(models.Snapshot, env.scope.snapshot_id)
            snapshot.status = models.SnapshotStatus.IN_PROGRESS
            await session.flush()
            await ensure_work(session, snapshot_id=env.scope.snapshot_id, snapshot_token=0,
                              job_token=0)
            snapshot.status = models.SnapshotStatus.FAILED
    await env.drain()
    assert not env.transfers
    env.worker._build_graph_client_for.assert_not_awaited()
    assert (await state(store, env))[0].attempt_count == 0


async def test_claim_consumes_budget_once_and_exhaustion_cannot_be_reopened(store, bw, monkeypatch):
    env = await prepare(store, bw, monkeypatch, attempts=2)
    seen = []

    async def fail(**kwargs):
        seen.append((await state(store, env))[0].attempt_count)
        raise OSError("synthetic transport failure")

    env.worker._retry_one_onedrive_file = fail
    await env.drain()
    row, _, _ = await state(store, env)
    assert seen == [3] and row.attempt_count == 3 and row.status == "FAILED_PERMANENT"
    await env.enqueue()
    await env.drain()
    assert seen == [3]


async def test_stale_reaper_does_not_spend_or_restore_an_exhausted_attempt(store, bw, monkeypatch):
    env = await prepare(store, bw, monkeypatch, status="IN_PROGRESS", attempts=3)
    async with store.factory.begin() as session:
        (await session.get(models.OneDriveFileRetry, env.retry_id)).updated_at = datetime.now(timezone.utc) - timedelta(minutes=10)
    await env.drain()
    row, _, _ = await state(store, env)
    assert row.status == "FAILED_PERMANENT" and row.attempt_count == 3
    assert not env.transfers


async def test_verified_receipt_skips_graph_and_does_not_promote_failed_snapshot(store, bw, monkeypatch):
    env = await prepare(store, bw, monkeypatch, receipt=True)
    await env.drain()
    row, item, snapshot = await state(store, env)
    assert row.status == "RESCUED" and row.attempt_count == 1
    assert item.blob_path == "existing/verified-file" and not env.transfers
    env.graph.get_download_url.assert_not_awaited()
    assert snapshot.status == models.SnapshotStatus.FAILED and snapshot.delta_token is None
    await env.enqueue()
    await env.drain()
    assert (await state(store, env))[0].attempt_count == 1


async def test_success_writes_proven_receipt_once_and_preserves_failed_status(store, bw, monkeypatch):
    env = await prepare(store, bw, monkeypatch)
    await env.drain()
    row, item, snapshot = await state(store, env)
    assert row.status == "RESCUED" and row.attempt_count == 1
    assert item.extra_data["onedrive_download_source"]["eTag"] == "version-1"
    assert len(env.transfers) == 1 and item.blob_path == env.transfers[0]
    assert str(env.retry_id) in item.blob_path
    assert snapshot.status == models.SnapshotStatus.FAILED and snapshot.delta_token is None
    await env.enqueue()
    await env.drain()
    assert len(env.transfers) == 1


@pytest.mark.parametrize("when", ["before", "during", "missing"])
async def test_retry_source_proof_failure_never_publishes_success(store, bw, monkeypatch, when):
    env = await prepare(store, bw, monkeypatch)
    if when == "before":
        env.source["eTag"] = "version-2"
    elif when == "missing":
        env.source.pop("eTag")
    else:
        original = env.worker._stream_graph_to_backend

        async def stream(**kwargs):
            result = await original(**kwargs)
            env.source["eTag"] = "version-2"
            return result

        env.worker._stream_graph_to_backend = stream
    await env.drain()
    row, item, _ = await state(store, env)
    assert row.status != "RESCUED" and item.blob_path is None
    assert bool(env.transfers) == (when == "during")


async def test_missing_snapshot_item_is_rejected_before_source_io(store, bw, monkeypatch):
    from sqlalchemy import delete
    env = await prepare(store, bw, monkeypatch)
    async with store.factory.begin() as session:
        await session.execute(delete(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == env.scope.snapshot_id))
    await env.drain()
    assert (await state(store, env))[0].status != "RESCUED"
    assert not env.transfers
    env.graph.get_download_url.assert_not_awaited()


async def test_enqueue_during_rescue_cannot_create_second_owner(store, bw, monkeypatch):
    env = await prepare(store, bw, monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()
    original = env.worker._stream_graph_to_backend

    async def stream(**kwargs):
        entered.set()
        await release.wait()
        return await original(**kwargs)

    env.worker._stream_graph_to_backend = stream
    task = asyncio.create_task(env.drain())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        await env.enqueue()
        await env.drain()
    finally:
        release.set()
        await asyncio.wait_for(task, 2)
    assert len(env.transfers) == 1
    assert (await state(store, env))[0].attempt_count == 1


async def test_cancellation_joins_upload_and_releases_only_its_attempt(store, bw, monkeypatch):
    env = await prepare(store, bw, monkeypatch)
    entered, closed = asyncio.Event(), asyncio.Event()

    async def stream(**kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            closed.set()

    env.worker._stream_graph_to_backend = stream
    task = asyncio.create_task(env.drain())
    await asyncio.wait_for(entered.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed.is_set()
    row, item, _ = await state(store, env)
    assert row.status == "PENDING" and row.attempt_count == 1 and item.blob_path is None


@pytest.mark.parametrize("during", ["publication", "heartbeat"])
async def test_replacement_generation_cannot_be_renewed_or_published_by_old_owner(store, bw, monkeypatch, during):
    env = await prepare(store, bw, monkeypatch)
    entered, release, closed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original = env.worker._stream_graph_to_backend

    async def stream(**kwargs):
        entered.set()
        try:
            await release.wait()
            return await original(**kwargs)
        finally:
            closed.set()

    env.worker._stream_graph_to_backend = stream
    task = asyncio.create_task(env.drain())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        async with store.factory.begin() as session:
            row = await session.get(models.OneDriveFileRetry, env.retry_id)
            row.attempt_count = 2
            row.updated_at = datetime.now(timezone.utc)
        if during == "publication":
            release.set()
        await asyncio.wait_for(task, 3)
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    row, item, _ = await state(store, env)
    assert closed.is_set() and row.status == "IN_PROGRESS" and row.attempt_count == 2
    assert item.blob_path is None
    env.graph.aclose.assert_awaited_once()


async def test_job_cancellation_stops_active_upload_without_publishing(store, bw, monkeypatch):
    env = await prepare(store, bw, monkeypatch)
    entered, closed = asyncio.Event(), asyncio.Event()

    async def stream(**kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            closed.set()

    env.worker._stream_graph_to_backend = stream
    task = asyncio.create_task(env.drain())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        async with store.factory.begin() as session:
            (await session.get(models.Job, env.job_id)).status = models.JobStatus.CANCELLED
        await asyncio.wait_for(task, 3)
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    row, item, _ = await state(store, env)
    assert closed.is_set() and row.status == "FAILED_PERMANENT" and item.blob_path is None


async def test_expired_owner_cannot_revive_its_lease(store, bw, monkeypatch):
    from shared.onedrive_retry import renew_retry, claim_retries
    env = await prepare(store, bw, monkeypatch, status="IN_PROGRESS", attempts=1)
    async with store.factory.begin() as session:
        row = await session.get(models.OneDriveFileRetry, env.retry_id)
        row.updated_at = datetime.now(timezone.utc) - timedelta(minutes=10)
    async with store.factory.begin() as session:
        assert await renew_retry(session, retry_id=env.retry_id, attempt=1, lease_seconds=300) is None
        claims, reaped = await claim_retries(session, limit=1, max_attempts=3, lease_seconds=300)
        assert reaped == 1 and [(r.id, r.attempt_count) for r in claims] == [(env.retry_id, 2)]


async def test_concurrent_claimers_spend_exactly_one_attempt(store, bw, monkeypatch):
    env = await prepare(store, bw, monkeypatch)
    await asyncio.gather(env.drain(), env.drain())
    row, item, _ = await state(store, env)
    assert len(env.transfers) == 1 and row.attempt_count == 1 and row.status == "RESCUED"


async def test_missing_row_after_upload_cannot_be_marked_rescued(store, bw, monkeypatch):
    from sqlalchemy import delete
    env = await prepare(store, bw, monkeypatch)
    original = env.worker._stream_graph_to_backend

    async def stream(**kwargs):
        result = await original(**kwargs)
        async with store.factory.begin() as session:
            await session.execute(delete(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == env.scope.snapshot_id))
        return result

    env.worker._stream_graph_to_backend = stream
    await env.drain()
    row, item, _ = await state(store, env)
    assert row.status == "FAILED_PERMANENT" and item is None


async def test_source_drift_never_overwrites_preexisting_blob_bytes(store, bw, monkeypatch):
    env = await prepare(store, bw, monkeypatch)
    objects = {"old/version-1": b"original"}
    async with store.factory.begin() as session:
        row = await session.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == env.scope.snapshot_id))
        row.blob_path = "old/version-1"  # Legacy row has no downloadable version proof.

    async def stream(**kwargs):
        objects[kwargs["blob_path"]] = b"changed during stream"
        env.source["eTag"] = "version-2"
        return {"success": True, "content_sha256": "changed"}

    env.worker._stream_graph_to_backend = stream
    await env.drain()
    row, item, _ = await state(store, env)
    assert row.status == "FAILED_PERMANENT" and item.blob_path == "old/version-1"
    assert objects["old/version-1"] == b"original" and len(objects) == 2


@pytest.mark.parametrize("stage", ["descriptor", "stream", "verification"])
@pytest.mark.parametrize("kind", ["deferred", "budget", "turn", "cancelled"])
async def test_provider_control_flow_releases_only_consumed_attempt_and_propagates(store, bw, monkeypatch, stage, kind):
    from shared.provider_admission import ProviderDeferred
    from shared.provider_runtime import ProviderBudgetExhausted, ProviderTurnDeferred
    env = await prepare(store, bw, monkeypatch)
    error = {"deferred": ProviderDeferred(None, 60), "budget": ProviderBudgetExhausted("original deadline"),
             "turn": ProviderTurnDeferred(ProviderDeferred(None, 60)), "cancelled": asyncio.CancelledError()}[kind]
    if stage == "stream":
        env.worker._stream_graph_to_backend = AsyncMock(side_effect=error)
    else:
        values = [error] if stage == "descriptor" else [
            ("https://download.invalid/file", 1024, "", deepcopy(env.source)), error]
        env.graph.get_download_url = AsyncMock(side_effect=values)
    with pytest.raises(type(error)):
        await env.drain()
    row, item, _ = await state(store, env)
    assert row.status == "PENDING" and row.attempt_count == 1 and item.blob_path is None
    env.graph.aclose.assert_awaited_once()


async def test_cancelled_batch_releases_task_that_never_started(store, bw, monkeypatch):
    env = await prepare(store, bw, monkeypatch)
    original = asyncio.create_task

    def cancel_run(coro, *args, **kwargs):
        task = original(coro, *args, **kwargs)
        if coro.__name__ == "run":
            task.cancel()
        return task

    monkeypatch.setattr(asyncio, "create_task", cancel_run)
    with pytest.raises(asyncio.CancelledError):
        await env.drain()
    row, item, _ = await state(store, env)
    assert row.status == "PENDING" and row.attempt_count == 1 and item.blob_path is None
    env.worker._build_graph_client_for.assert_not_awaited()


async def test_cancellation_releases_claim_waiting_for_local_slot(store, bw, monkeypatch):
    env = await prepare(store, bw, monkeypatch)
    monkeypatch.setenv("ONEDRIVE_RETRY_CONCURRENCY", "1")
    second = file_item("file-2")
    async with store.factory.begin() as session:
        session.add(models.OneDriveFileRetry(tenant_id=store.tenant, resource_id=store.resource,
            snapshot_id=env.scope.snapshot_id, file_external_id=second["id"], drive_id="drive-A",
            file_payload=second, status="PENDING", attempt_count=0,
            next_retry_at=datetime.now(timezone.utc) - timedelta(seconds=1)))
        session.add(models.SnapshotItem(snapshot_id=env.scope.snapshot_id, tenant_id=store.tenant,
            backend_id=store.backend, external_id=second["id"], item_type="ONEDRIVE_FILE",
            name=second["name"], content_size=1024, extra_data={"raw": second}))
    entered, closed = asyncio.Event(), asyncio.Event()

    async def metadata(drive, file_id, **kwargs):
        return "https://download.invalid/file", 1024, "", file_item(file_id)

    async def stream(**kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            closed.set()

    env.graph.get_download_url = metadata
    env.worker._stream_graph_to_backend = stream
    task = asyncio.create_task(env.drain())
    await asyncio.wait_for(entered.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    async with store.factory() as session:
        rows = (await session.scalars(select(models.OneDriveFileRetry))).all()
        assert len(rows) == 2 and all(r.status == "PENDING" and r.attempt_count == 1 for r in rows)
    assert closed.is_set()
    assert not any(t.get_coro().__name__ in ("capture", "maintain_owner") for t in asyncio.all_tasks())

