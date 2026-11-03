"""Actual OneDrive capture drains admitted bytes and version receipts before defer."""
import asyncio
import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select

from test_chat_catalogue_postgres import store
from test_copy_worker_handoff_postgres import bw
from test_graph_admission_redis import redis_client
from test_onedrive_resume_postgres import setup_worker, file_item
from shared import models, provider_runtime as runtime, provider_work as work
from shared.provider_admission import AdmissionLimits, ScopedProviderAdmission
from shared.provider_http import AdmittedHttpClient

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("backlog", [1000, 1], ids=["drain_callback", "periodic_flusher"])
async def test_onedrive_finishes_admitted_files_and_batched_proof_before_defer(
        store, redis_client, bw, monkeypatch, backlog):
    files = [file_item(key) for key in ("a", "b", "c", "d")]
    env = await setup_worker(store, bw, monkeypatch, files)
    monkeypatch.setattr(bw.settings, "ONEDRIVE_BACKUP_FILE_CONCURRENCY", 3)
    monkeypatch.setenv("ONEDRIVE_FLUSH_BACKLOG_CAP", str(backlog))
    async with store.factory.begin() as session:
        await work.ensure_work(session, snapshot_id=env.scope.snapshot_id,
            snapshot_token=0, job_token=0, continuation={"kind": "single"})

    class ImmediateAdmission(ScopedProviderAdmission):
        @asynccontextmanager
        async def admit(self, context, **kwargs):
            async with super().admit(context, wait_budget=0):
                yield

    limiter = ImmediateAdmission(redis_client, namespace=uuid4().hex,
        limits=AdmissionLimits(rate=10000, burst=10000, global_concurrency=2,
            tenant_concurrency=2, resource_concurrency=2, lease_seconds=.3))
    body_release, storage_release, storage_started = (asyncio.Event() for _ in range(3))
    sent, executions, metadata_batches, staged = [], [], [], []
    base = "https://synthetic.sharepoint.com/"

    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"a" * 512
            await body_release.wait()
            yield b"b" * 512

    async def transport(request):
        sent.append(request.url.path)
        if request.url.path.startswith("/file/"):
            return httpx.Response(200, stream=Body())
        if request.url.path.endswith("/$batch"):
            requests = json.loads(request.content)["requests"]
            return httpx.Response(200, json={"responses": [
                {"id": entry["id"], "status": 200, "body": next(item for item in files if item["id"] == entry["id"])}
                for entry in requests]})
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as raw:
        client = AdmittedHttpClient(raw, SimpleNamespace(tenant_id="tenant", client_id="app"), limiter=limiter)

        async def urls(drive_id, ids, *, include_source_metadata=False):
            metadata_batches.append(list(ids))
            response = await client.post("https://graph.microsoft.com/v1.0/$batch", json={"requests": [
                {"id": key, "method": "GET", "url": f"/drives/{drive_id}/items/{key}"} for key in ids]})
            observed = [entry["body"] for entry in response.json()["responses"]]
            return {item["id"]: (base + "file/" + item["id"], item["size"], "", dict(item))
                    for item in observed}

        async def stream(**kwargs):
            response = await client.get(kwargs["download_url"])
            assert len(response.content) == 1024
            staged.append(kwargs["metadata"]["source_item_id"])
            if len(staged) == 2:
                storage_started.set()
            await storage_release.wait()
            return {"success": True, "content_sha256": "content-digest"}

        env.graph.get_download_urls_batch = urls
        env.worker._stream_graph_to_backend = stream

        async def resume(owner):
            executions.append(runtime.current_execution())
            return await env.run()

        async def settle(work_id):
            pass  # This regression exercises capture; parent finalization is separate.

        runner = runtime.ProviderWorkRunner(store.factory, resume, settle)
        task = asyncio.create_task(runner.run_once())
        try:
            async with asyncio.timeout(5):
                while not task.done() and not (executions and getattr(executions[0], "congestion", None)):
                    await asyncio.sleep(.001)
            assert not task.done(), "congestion cancelled admitted files before their receipt flush"
            assert executions[0].deferred is None
            body_release.set()
            await asyncio.wait_for(storage_started.wait(), 3)
            assert not task.done(), "logical transfer ended before storage finished"
            storage_release.set()
            assert (await asyncio.wait_for(task, 5))["provider_waiting"]
        finally:
            body_release.set()
            storage_release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        admitted = [path.rsplit('/', 1)[-1] for path in sent if path.startswith('/file/')]
        assert len(admitted) == len(set(admitted)) == 2
        assert set(admitted) <= {'a', 'b', 'c'}
        remaining = {'a', 'b', 'c', 'd'} - set(admitted)
        assert not executions[0].drain_callbacks
        assert sorted(key for chunk in metadata_batches[1:] for key in chunk) == sorted(admitted)
        assert all(len(chunk) <= 20 for chunk in metadata_batches)
        async with store.factory.begin() as session:
            rows = (await session.scalars(select(models.SnapshotItem).where(
                models.SnapshotItem.snapshot_id == env.scope.snapshot_id))).all()
            assert {row.external_id for row in rows if row.blob_path} == set(admitted)
            assert all(row.extra_data["onedrive_download_source"]["eTag"] == "version-1"
                       for row in rows if row.blob_path)
            provider = await session.scalar(select(models.ProviderWork).where(
                models.ProviderWork.snapshot_id == env.scope.snapshot_id))
            assert provider.state == "WAITING"
            provider.next_eligible_at = datetime.now(timezone.utc)
        before_resume = len(metadata_batches)
        assert await asyncio.wait_for(runner.run_once(), 5) == (2, 2048)
        assert all(set(chunk) <= remaining for chunk in metadata_batches[before_resume:])
        assert sorted(path for path in sent if path.startswith("/file/")) == ["/file/a", "/file/b", "/file/c", "/file/d"]
