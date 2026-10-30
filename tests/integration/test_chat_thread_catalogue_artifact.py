"""Thread-export consumer stores a routed, tenant-encrypted ZIP artifact."""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime
import importlib
import io
import json
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4
import zipfile

import pytest
from sqlalchemy import select, update

from test_chat_catalogue_postgres import store, message
from shared import models

pytestmark = pytest.mark.integration
_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def thread_consumer():
    package_name = "workers.chat_export_worker"
    if package_name not in sys.modules:
        package = ModuleType(package_name)
        package.__path__ = [str(_ROOT / "workers/chat-export-worker")]
        sys.modules[package_name] = package
    return importlib.import_module(package_name + ".consumers.thread")


@pytest.mark.parametrize("runtime_ready", [True, False])
async def test_thread_consumer_uploads_encrypted_zip_with_exact_download_route_or_refuses_new_write(
        store, thread_consumer, monkeypatch, runtime_ready):
    from shared.azure_storage import _StoreFacade
    from shared.crypto import envelope, runtime
    from shared.storage.router import router
    from shared.storage.seaweedfs import SeaweedStore

    scope, _, _ = await store.capture([message("captured thread content")])
    export_id, target_backend = uuid4(), uuid4()
    async with store.factory.begin() as db:
        await db.execute(update(models.Snapshot).where(models.Snapshot.id == scope.snapshot_id)
            .values(status=models.SnapshotStatus.COMPLETED))
        parent = await db.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == scope.snapshot_id))
        spec = {"resourceId": str(scope.resource_id), "snapshotIds": [str(scope.snapshot_id)],
            "itemIds": [str(parent.id)], "exportFormat": "JSON"}
        db.add(models.Job(id=export_id, type=models.JobType.EXPORT, tenant_id=store.tenant,
            status=models.JobStatus.RUNNING, spec=spec))
        db.add(models.StorageBackend(id=target_backend, kind="seaweedfs", name=str(target_backend),
            endpoint="https://storage.invalid", config={}, secret_ref="synthetic-only"))
        db.add(models.EncryptionConfig(id="singleton", provider="env", enabled=True))
    key, key_id = b"v" * 32, b"w" * 16
    tenants, saved, parts = [], {}, {}
    class Keys:
        async def active_dek(self, tenant_id):
            tenants.append(tenant_id)
            assert tenant_id == str(store.tenant)
            return key_id, key
        async def dek_for_key_id(self, selected):
            assert selected == key_id
            return key
    monkeypatch.setattr(runtime, "_dek_manager", Keys() if runtime_ready else None)
    monkeypatch.setattr(runtime, "_write_enabled", True)
    class Body:
        def __init__(self, data):
            self.data = data
        async def read(self, size=-1):
            return self.data if size < 0 else self.data[:size]
        async def iter_chunks(self, size):
            for start in range(0, len(self.data), 29):
                yield self.data[start:start + 29]
        def close(self):
            pass
    class Client:
        async def create_multipart_upload(self, **kwargs):
            async with store.factory() as db:
                assert list((await db.scalars(select(models.ChatCataloguePin))).all())
            saved["route"] = (kwargs["Bucket"], kwargs["Key"])
            saved["metadata"] = kwargs["Metadata"]
            return {"UploadId": "thread-target"}
        async def upload_part(self, **kwargs):
            parts[kwargs["PartNumber"]] = kwargs["Body"]
            return {"ETag": str(kwargs["PartNumber"])}
        async def complete_multipart_upload(self, **kwargs):
            saved["bytes"] = b"".join(parts[index] for index in sorted(parts))
        async def head_object(self, **kwargs):
            return {"ContentLength": len(saved["bytes"]), "ETag": '"etag"', "LastModified": datetime.utcnow()}
        async def get_object(self, **kwargs):
            assert (kwargs["Bucket"], kwargs["Key"]) == saved["route"]
            return {"Body": Body(saved["bytes"])}
        async def abort_multipart_upload(self, **kwargs):
            pytest.fail("unexpected multipart abort")
    target = object.__new__(SeaweedStore)
    target.backend_id = str(target_backend)
    target._endpoint, target._upload_concurrency, target._tenant_id = "https://storage.invalid", 1, None
    target._bucket = lambda container: container
    target._key = lambda _container, path: path
    target.ensure_container = AsyncMock()
    @asynccontextmanager
    async def client():
        yield Client()
    target._client_ctx = client
    async def wrong_clone_upload(*args, **kwargs):
        raise RuntimeError("default shard clone must not receive catalogue artifact uploads")
    clone = SimpleNamespace(backend_id=str(target_backend), kind="seaweedfs", _tenant_id="default",
        upload_from_file=wrong_clone_upload)
    monkeypatch.setattr(thread_consumer.azure_storage_manager, "get_default_shard", lambda: _StoreFacade(clone))
    def exact_store(backend_id):
        assert backend_id == str(target_backend)
        return target
    monkeypatch.setattr(router, "get_store_by_id", exact_store)
    monkeypatch.setattr(thread_consumer, "async_session_factory", store.factory)
    monkeypatch.setattr(thread_consumer, "publish", AsyncMock())
    monkeypatch.setattr(thread_consumer.settings, "RETRY_DELAY_MS", 1)
    async with store.factory() as db:
        job = await db.get(models.Job, export_id)
        await thread_consumer._execute_export_job(db, str(export_id), spec, job)
    async with store.factory() as db:
        job = await db.get(models.Job, export_id)
        result = job.result
        if not runtime_ready:
            assert job.status == models.JobStatus.FAILED
            assert "configured encryption runtime is unavailable" in result["error"]["message"]
            assert saved == {} and tenants == []
            target.ensure_container.assert_not_awaited()
        else:
            assert job.status == models.JobStatus.COMPLETED
            assert envelope.is_encrypted(saved["bytes"])
            assert result["chat_catalogue_format"] == 1 and result["backend_id"] == str(target_backend)
            assert result["container"] == "exports" and result["blob_path"] == f"exports/{export_id}/export.zip"
            assert result["encryption_key_id"] == str(UUID(bytes=key_id))
            assert tenants == [str(store.tenant)] and target._tenant_id is None and clone._tenant_id == "default"
            decoded = b"".join([chunk async for chunk in target.download_stream(result["container"], result["blob_path"])])
            assert result["size_bytes"] == len(decoded) and result["size_bytes"] != len(saved["bytes"])
            assert saved["metadata"]["tenant_id"] == str(store.tenant)
            with zipfile.ZipFile(io.BytesIO(decoded)) as archive:
                manifest = json.loads(archive.read("_meta/manifest.json"))
                assert result["sha256"] == manifest["sha256"]
                assert result["total_bytes"] == sum(len(archive.read(name)) for name in archive.namelist())
                messages = [archive.read(name) for name in archive.namelist() if not name.startswith("_meta/")]
                assert any(b"captured thread content" in value for value in messages)
        assert not list((await db.scalars(select(models.ChatCataloguePin))).all())


async def run_upload_terminal_race(store, thread_consumer, monkeypatch, status, *, replacement=None, fail_upload=False):
    import shared.chat_capture_storage as storage_module
    scope, _, _ = await store.capture([message("upload race body")])
    export_id = uuid4()
    async with store.factory.begin() as db:
        await db.execute(update(models.Snapshot).where(models.Snapshot.id == scope.snapshot_id)
            .values(status=models.SnapshotStatus.COMPLETED))
        parent = await db.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == scope.snapshot_id))
        spec = {"resourceId": str(scope.resource_id), "snapshotIds": [str(scope.snapshot_id)],
            "itemIds": [str(parent.id)], "exportFormat": "JSON"}
        db.add(models.Job(id=export_id, type=models.JobType.EXPORT, tenant_id=store.tenant,
            status=models.JobStatus.PENDING, spec=spec))
    sentinel = {"existing_terminal_result": "preserved"}
    async def upload(**kwargs):
        assert Path(kwargs["filename"]).stat().st_size > 0
        async with store.factory.begin() as db:
            assert list((await db.scalars(select(models.ChatCataloguePin))).all())
            await db.execute(update(models.Job).where(models.Job.id == export_id).values(status=status,
                result=sentinel if status in {models.JobStatus.COMPLETED, models.JobStatus.FAILED} else None))
        if fail_upload:
            raise RuntimeError("injected upload failure after cancellation")
        return {"success": True, "backend_id": str(store.backend), "encryption_key_id": None}
    if replacement is not None:
        original = thread_consumer._update_job
        async def moved_owner(sess, job_id, **values):
            async with store.factory.begin() as db:
                await db.execute(update(models.Job).where(models.Job.id == export_id).values(status=replacement, result=sentinel))
            return await original(sess, job_id, **values)
        monkeypatch.setattr(thread_consumer, "_update_job", moved_owner)
    monkeypatch.setattr(storage_module, "upload_catalogue_export_artifact", upload)
    monkeypatch.setattr(thread_consumer.azure_storage_manager, "get_default_shard", lambda: object())
    monkeypatch.setattr(thread_consumer, "async_session_factory", store.factory)
    events = AsyncMock()
    monkeypatch.setattr(thread_consumer, "publish", events)
    async with store.factory() as db:
        job = await db.get(models.Job, export_id)
        await thread_consumer._execute_export_job(db, str(export_id), spec, job)
    async with store.factory() as db:
        job = await db.get(models.Job, export_id)
        assert not list((await db.scalars(select(models.ChatCataloguePin))).all())
        return job.status, job.result, events


@pytest.mark.parametrize("status", [models.JobStatus.CANCELLED, models.JobStatus.CANCELLING,
    models.JobStatus.COMPLETED, models.JobStatus.FAILED])
async def test_upload_terminal_race_does_not_publish_or_overwrite_terminal_result(store, thread_consumer, monkeypatch, status):
    actual, result, events = await run_upload_terminal_race(store, thread_consumer, monkeypatch, status)
    expected = models.JobStatus.CANCELLED if status == models.JobStatus.CANCELLING else status
    assert actual == expected
    assert result == ({"existing_terminal_result": "preserved"} if status in {models.JobStatus.COMPLETED, models.JobStatus.FAILED} else None)
    assert all(call.args[1] != "complete" for call in events.await_args_list)
    if status in {models.JobStatus.COMPLETED, models.JobStatus.FAILED}:
        assert all(call.args[1] != "cancelled" for call in events.await_args_list)


@pytest.mark.parametrize("replacement", [models.JobStatus.COMPLETED, models.JobStatus.FAILED])
async def test_zero_row_cancellation_settlement_cannot_change_replacement_terminal(store, thread_consumer, monkeypatch, replacement):
    actual, result, events = await run_upload_terminal_race(store, thread_consumer, monkeypatch,
        models.JobStatus.CANCELLING, replacement=replacement)
    assert actual == replacement and result == {"existing_terminal_result": "preserved"}
    assert all(call.args[1] not in {"complete", "cancelled"} for call in events.await_args_list)


async def test_upload_failure_after_cancelling_settles_cancelled_instead_of_failed(store, thread_consumer, monkeypatch):
    actual, result, events = await run_upload_terminal_race(store, thread_consumer, monkeypatch,
        models.JobStatus.CANCELLING, fail_upload=True)
    assert actual == models.JobStatus.CANCELLED and result is None
    assert all(call.args[1] not in {"complete", "error"} for call in events.await_args_list)
