"""OneDrive starts ready downloads without resolving the entire partition first."""
import asyncio
from collections import Counter
from datetime import datetime, timezone

import pytest
from sqlalchemy import select

from test_chat_catalogue_postgres import store
from test_copy_worker_handoff_postgres import bw
from test_graph_admission_redis import redis_client
from test_onedrive_resume_postgres import setup_worker, file_item
from shared import models, provider_runtime as runtime, provider_work as work
from shared.provider_admission import ProviderDeferred, ProviderRequestContext

pytestmark = pytest.mark.integration


async def test_ready_files_download_while_later_url_batch_is_blocked(store, bw, monkeypatch):
    files = [file_item(f"file-{index:03}") for index in range(45)]
    env = await setup_worker(store, bw, monkeypatch, files)
    original = env.graph.get_download_urls_batch
    first_download, release_later = asyncio.Event(), asyncio.Event()
    requested, downloaded = [], []

    async def urls(drive_id, ids, **kwargs):
        requested.extend(ids)
        if any(key >= "file-020" for key in ids):
            await release_later.wait()
        return await original(drive_id, ids, **kwargs)

    async def stream(**kwargs):
        downloaded.append(kwargs["metadata"]["source_item_id"])
        first_download.set()
        return {"success": True, "content_sha256": "content-digest"}

    env.graph.get_download_urls_batch = urls
    env.worker._stream_graph_to_backend = stream
    task = asyncio.create_task(env.run())
    try:
        await asyncio.wait_for(first_download.wait(), 1)
        assert not release_later.is_set()
    finally:
        release_later.set()
        await asyncio.wait_for(task, 5)
    assert task.result() == (45, 45 * 1024)
    assert Counter(downloaded) == Counter(item["id"] for item in files)
    assert Counter(requested) == Counter({item["id"]: 2 for item in files})
    before = list(requested)
    assert await env.run() == (0, 0)
    assert requested == before


async def test_later_prefetch_defer_commits_ready_files_and_resume_skips_them(store, bw, monkeypatch):
    files = [file_item(f"file-{index:03}") for index in range(45)]
    env = await setup_worker(store, bw, monkeypatch, files)
    async with store.factory.begin() as session:
        await work.ensure_work(session, snapshot_id=env.scope.snapshot_id,
            snapshot_token=0, job_token=0, continuation={"kind": "single"})
    original = env.graph.get_download_urls_batch
    defer = True
    downloaded, requested = [], []

    async def urls(drive_id, ids, **kwargs):
        runtime.check_provider_turn()
        requested.extend(ids)
        if defer and any(key >= "file-020" for key in ids):
            runtime.defer_provider(ProviderDeferred(
                ProviderRequestContext("tenant", "app", "sharepoint"), .01,
                reason="cooldown", scope="tenant"))
        return await original(drive_id, ids, **kwargs)

    async def stream(**kwargs):
        downloaded.append(kwargs["metadata"]["source_item_id"])
        return {"success": True, "content_sha256": "content-digest"}

    env.graph.get_download_urls_batch = urls
    env.worker._stream_graph_to_backend = stream
    async def resume(owner):
        return await env.run()

    async def settle(work_id):
        pass

    runner = runtime.ProviderWorkRunner(store.factory, resume, settle)
    assert (await runner.run_once())["provider_waiting"]
    async with store.factory() as session:
        rows = (await session.scalars(select(models.SnapshotItem).where(
            models.SnapshotItem.snapshot_id == env.scope.snapshot_id))).all()
        committed = {row.external_id for row in rows if row.blob_path}
        assert committed == {item["id"] for item in files[:20]}
        assert all(row.extra_data["onedrive_download_source"]["eTag"] == "version-1"
                   for row in rows if row.blob_path)
    async with store.factory.begin() as session:
        provider = await session.scalar(select(models.ProviderWork).where(
            models.ProviderWork.snapshot_id == env.scope.snapshot_id))
        provider.next_eligible_at = datetime.now(timezone.utc)
    defer = False
    before = len(requested)
    assert await runner.run_once() == (25, 25 * 1024)
    assert not (set(requested[before:]) & committed)
    assert Counter(downloaded) == Counter(item["id"] for item in files)


async def test_download_capacity_bounds_url_lookahead(store, bw, monkeypatch):
    files = [file_item(f"file-{index:03}") for index in range(1000)]
    env = await setup_worker(store, bw, monkeypatch, files)
    monkeypatch.setattr(bw.settings, "ONEDRIVE_BACKUP_FILE_CONCURRENCY", 3)
    monkeypatch.setenv("ONEDRIVE_PREFETCH_CONCURRENCY", "48")
    all_started, release = asyncio.Event(), asyncio.Event()
    active, peak = 0, 0

    async def stream(**kwargs):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        if active == 3:
            all_started.set()
        try:
            await release.wait()
            return {"success": True, "content_sha256": "content-digest"}
        finally:
            active -= 1

    env.worker._stream_graph_to_backend = stream
    task = asyncio.create_task(env.run())
    try:
        await asyncio.wait_for(all_started.wait(), 2)
        assert env.calls == [item["id"] for item in files[:20]]
        assert peak == 3
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert active == 0


async def test_cancelled_pipeline_collects_shared_metadata_task(store, bw, monkeypatch):
    env = await setup_worker(store, bw, monkeypatch, [file_item("a"), file_item("b")])
    started, cancelled = asyncio.Event(), asyncio.Event()
    requests = 0

    async def urls(*args, **kwargs):
        nonlocal requests
        requests += 1
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    env.graph.get_download_urls_batch = urls
    task = asyncio.create_task(env.run())
    await asyncio.wait_for(started.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert requests == 1
    assert cancelled.is_set()
    async with store.factory() as session:
        rows = (await session.scalars(select(models.SnapshotItem).where(
            models.SnapshotItem.snapshot_id == env.scope.snapshot_id))).all()
        assert all(row.blob_path is None for row in rows)


async def test_partial_batch_is_shared_and_missing_urls_fall_back_per_file(store, bw, monkeypatch):
    files = [file_item(key) for key in ("a", "b", "c", "d")]
    env = await setup_worker(store, bw, monkeypatch, files)
    original = env.graph.get_download_urls_batch
    batches, single = [], []

    async def urls(drive_id, ids, **kwargs):
        batches.append(list(ids))
        result = await original(drive_id, ids, **kwargs)
        return {key: value for key, value in result.items() if key in ("a", "c")} if len(batches) == 1 else result

    async def url(drive_id, key, **kwargs):
        single.append(key)
        return (await original(drive_id, [key], **kwargs))[key]

    env.graph.get_download_urls_batch, env.graph.get_download_url = urls, url
    assert await env.run() == (4, 4096)
    assert sorted(single) == ["b", "d"]
    assert len(batches) == 2
    assert all(sorted(batch) == ["a", "b", "c", "d"] for batch in batches)


async def test_local_metadata_slot_wait_keeps_same_turn(store, redis_client, bw, monkeypatch):
    from contextlib import asynccontextmanager
    from uuid import uuid4
    from shared import provider_runtime as runtime, provider_work as work
    from shared.provider_admission import AdmissionLimits, ScopedProviderAdmission

    env = await setup_worker(store, bw, monkeypatch, [file_item("a"), file_item("b")])
    async with store.factory.begin() as session:
        await work.ensure_work(session, snapshot_id=env.scope.snapshot_id,
            snapshot_token=0, job_token=0, continuation={"kind": "single"})

    class ImmediateAdmission(ScopedProviderAdmission):
        @asynccontextmanager
        async def admit(self, context, **kwargs):
            async with super().admit(context, wait_budget=0):
                yield

    limiter = ImmediateAdmission(redis_client, namespace=uuid4().hex,
        limits=AdmissionLimits(rate=10000, burst=10000, global_concurrency=1,
            tenant_concurrency=1, resource_concurrency=1))
    context = ProviderRequestContext("tenant", "app", "sharepoint", "drive:drive-A")
    waiting = asyncio.Event()
    original_wait = runtime.wait_provider_concurrency
    original_urls = env.graph.get_download_urls_batch
    executions, lookups = [], []

    async def observe_wait(error, **kwargs):
        waiting.set()
        return await original_wait(error, **kwargs)

    async def urls(drive_id, ids, **kwargs):
        lookups.append(list(ids))
        async with limiter.admit(context):
            return await original_urls(drive_id, ids, **kwargs)

    async def resume(owner):
        executions.append(runtime.current_execution())
        return await env.run()

    async def settle(work_id):
        pass

    monkeypatch.setattr(runtime, "wait_provider_concurrency", observe_wait)
    env.graph.get_download_urls_batch = urls
    runner = runtime.ProviderWorkRunner(store.factory, resume, settle)
    task = None
    try:
        async with limiter.admit(context):
            task = asyncio.create_task(runner.run_once())
            await asyncio.wait_for(waiting.wait(), 3)
            assert not task.done()
            assert executions[0].congestion is None
        assert await asyncio.wait_for(task, 5) == (2, 2048)
    finally:
        if task is not None:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert len(executions) == 1
    assert lookups == [["a", "b"], ["a", "b"]]  # URL lookup, then version proof.


async def test_terminal_snapshot_aborts_before_url_fetch_without_fallback(store, bw, monkeypatch):
    env = await setup_worker(store, bw, monkeypatch, [file_item("a"), file_item("b")])
    original_live = env.worker._snapshot_still_live

    async def make_terminal(snapshot_id):
        async with store.factory.begin() as session:
            snapshot = await session.get(models.Snapshot, snapshot_id)
            snapshot.status = models.SnapshotStatus.FAILED
        return await original_live(snapshot_id)

    env.worker._snapshot_still_live = make_terminal
    with pytest.raises(bw.RunAbandoned):
        await env.run()
    assert env.calls == []
    async with store.factory() as session:
        rows = (await session.scalars(select(models.SnapshotItem).where(
            models.SnapshotItem.snapshot_id == env.scope.snapshot_id))).all()
        assert all(row.blob_path is None for row in rows)
