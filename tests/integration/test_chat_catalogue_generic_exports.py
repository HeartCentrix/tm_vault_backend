"""Generic restore-worker export consumers preserve exact captured chat bytes."""
from __future__ import annotations

import base64
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4, UUID
import zipfile

import pytest
from sqlalchemy import delete, select, update

from test_chat_catalogue_postgres import store, message, parent_for
from shared.chat_catalogue_identity import CapturedChatItem, ChatContentDescriptor
from shared import models

pytestmark = pytest.mark.integration
_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def restore_api():
    path = _ROOT / "workers/restore-worker/main.py"
    sys.path.insert(0, str(path.parent))
    spec = importlib.util.spec_from_file_location("catalogue_generic_restore", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def inline(data, *, parent="message-alpha", graph_id="1"):
    descriptor = ChatContentDescriptor(None, None, None, None, hashlib.sha256(data).hexdigest(), len(data),
        None, 1, base64.b64encode(data).decode(), None, None, None, None)
    return CapturedChatItem("CHAT_HOSTED_CONTENT", graph_id, parent,
        {"name": "image.png", "contentType": "image/png"}, descriptor)


async def terminal(store, scope):
    async with store.factory.begin() as db:
        await db.execute(update(models.Snapshot).where(models.Snapshot.id == scope.snapshot_id)
            .values(status=models.SnapshotStatus.COMPLETED))


@pytest.fixture
def worker(store, restore_api, monkeypatch):
    monkeypatch.setattr(restore_api, "async_session_factory", store.factory)
    instance = restore_api.RestoreWorker.__new__(restore_api.RestoreWorker)
    instance.worker_id = "generic-catalogue-test"
    return instance


async def test_generic_selection_is_exact_and_hydrates_versions_without_resurrecting_deleted_messages(store, worker):
    old, _, _ = await store.capture([message("captured old"), message("deleted later", graph_id="deleted")])
    await terminal(store, old)
    new, _, _ = await store.capture([message("captured current")])
    await terminal(store, new)
    async with store.factory() as db:
        items = await worker.fetch_snapshot_items(db, [str(new.snapshot_id)], [])
        assert len(items) == 1 and items[0].external_id == "message-alpha"
        assert await worker._load_snapshot_item_payload(items[0], "files") == {"id": "message-alpha",
            "body": {"content": "captured current"}}
        physical = (await db.scalars(select(models.SnapshotItem).where(
            models.SnapshotItem.snapshot_id == new.snapshot_id))).one()
        assert physical.extra_data == {"chat_id": "chat-alpha"}
        assert (await worker._load_snapshot_item_payload(physical, "files"))["body"]["content"] == "captured current"
        assert physical.extra_data == {"chat_id": "chat-alpha"}


async def test_explicit_catalogue_item_selection_keeps_parent_children_and_snapshot_boundary(store, worker):
    child = inline(b"image")
    scope, _, _ = await store.capture([parent_for(child, parent=message()), child])
    await terminal(store, scope)
    other, _, _ = await store.capture([])
    await terminal(store, other)
    async with store.factory() as db:
        parent = await db.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == scope.snapshot_id,
            models.SnapshotItem.item_type == "TEAMS_CHAT_MESSAGE"))
        items = await worker.fetch_snapshot_items(db, [str(scope.snapshot_id)], [str(parent.id)])
        assert {i.item_type for i in items} == {"TEAMS_CHAT_MESSAGE", "CHAT_HOSTED_CONTENT"}
        with pytest.raises(Exception, match="snapshot|scope|selected"):
            await worker.fetch_snapshot_items(db, [str(other.snapshot_id)], [str(parent.id)])


@pytest.mark.parametrize("method", ["export_as_zip", "export_download"])
async def test_generic_export_actual_zip_contains_captured_json_and_distinct_binary_children_with_pins(
        store, worker, monkeypatch, method):
    children = [inline(b"first image", parent="message-alpha"), inline(b"second image", parent="message-beta")]
    data = []
    for child in children:
        data.extend([parent_for(child, parent=message("body " + child.parent_graph_id, graph_id=child.parent_graph_id)), child])
    scope, _, _ = await store.capture(data)
    await terminal(store, scope)
    export_id = uuid4()
    async with store.factory.begin() as db:
        db.add(models.Job(id=export_id, tenant_id=store.tenant, type=models.JobType.EXPORT, status=models.JobStatus.RUNNING))
    uploaded = []
    async def upload_file(_container, _path, filename, **kwargs):
        async with store.factory() as db:
            assert list((await db.scalars(select(models.ChatCataloguePin))).all())
        uploaded.append(Path(filename).read_bytes())
        return {"success": True}
    async def forbidden(*args, **kwargs):
        raise AssertionError("whole ZIP must not be uploaded as in-memory bytes")
    shard = SimpleNamespace(backend_id=str(store.backend), upload_blob=forbidden, upload_blob_from_file=upload_file)
    monkeypatch.setattr(worker_module(worker).azure_storage_manager, "get_default_shard", lambda: shard)
    async with store.factory() as db:
        items = await worker.fetch_snapshot_items(db, [str(scope.snapshot_id)], [])
        result = await getattr(worker, method)(db, items, {"jobId": str(export_id)}, {})
    assert result["exported_count"] == 4 and result["failed_count"] == 0
    assert result["export_type"] == "ZIP" and result["blob_path"] and result["container"] == "exports"
    assert result["blob_path"].startswith("exports/")
    assert result["chat_catalogue_format"] == 1 and result["size_bytes"] == len(uploaded[0])
    assert len(uploaded) == 1
    with zipfile.ZipFile(io.BytesIO(uploaded[0])) as archive:
        assert len(archive.namelist()) == len(set(archive.namelist())) == 4
        binaries = [archive.read(name) for name in archive.namelist() if not name.endswith(".json")]
        assert sorted(binaries) == [b"first image", b"second image"]
        bodies = [json.loads(archive.read(name))["body"]["content"] for name in archive.namelist() if name.endswith(".json")]
        assert sorted(bodies) == ["body message-alpha", "body message-beta"]
    async with store.factory() as db:
        assert not list((await db.scalars(select(models.ChatCataloguePin))).all())


def worker_module(worker):
    return sys.modules[type(worker).__module__]


async def test_catalogue_terminal_update_cannot_overwrite_a_cancel_even_with_cached_job(store, worker):
    export_id = uuid4()
    async with store.factory.begin() as db:
        db.add(models.Job(id=export_id, tenant_id=store.tenant, type=models.JobType.EXPORT, status=models.JobStatus.RUNNING))
    async with store.factory() as db:
        cached = await db.get(models.Job, export_id)
        assert cached.status == models.JobStatus.RUNNING
        async with store.factory.begin() as other:
            await other.execute(update(models.Job).where(models.Job.id == export_id).values(status=models.JobStatus.CANCELLED))
        with pytest.raises(Exception, match="active|cancel|owner"):
            await worker.update_job_status(db, export_id, models.JobStatus.COMPLETED,
                {"chat_inventory_format": 1, "exported_count": 1})
        await db.rollback()
    async with store.factory() as db:
        assert (await db.get(models.Job, export_id)).status == models.JobStatus.CANCELLED


async def test_catalogue_process_cancel_during_upload_preserves_cancelled_and_releases_pins(store, worker, monkeypatch):
    scope, _, _ = await store.capture([message("cancel race")])
    await terminal(store, scope)
    export_id = uuid4()
    async with store.factory.begin() as db:
        db.add(models.Job(id=export_id, tenant_id=store.tenant, type=models.JobType.EXPORT, status=models.JobStatus.RUNNING))
    async def upload_file(*args, **kwargs):
        async with store.factory.begin() as db:
            await db.execute(update(models.Job).where(models.Job.id == export_id).values(status=models.JobStatus.CANCELLED))
        return {"success": True}
    monkeypatch.setattr(worker_module(worker).azure_storage_manager, "get_default_shard",
        lambda: SimpleNamespace(backend_id=str(store.backend), upload_blob_from_file=upload_file))
    import asyncio
    worker.semaphore = asyncio.Semaphore(1)
    worker.log_audit_event = AsyncMock()
    await worker.process_restore_message({"jobId": str(export_id), "restoreType": "DOWNLOAD",
        "snapshotIds": [str(scope.snapshot_id)], "spec": {"workloads": ["Chats"]}})
    async with store.factory() as db:
        assert (await db.get(models.Job, export_id)).status == models.JobStatus.CANCELLED
        assert not list((await db.scalars(select(models.ChatCataloguePin))).all())


@pytest.mark.parametrize("status,extra", [(models.SnapshotStatus.FAILED, {}),
    (models.SnapshotStatus.PENDING_DELETION, {}), (models.SnapshotStatus.COMPLETED, {"cancelled_at": "captured cancellation"})])
async def test_generic_catalogue_selection_refuses_failed_cancelled_and_deleting_restore_points(store, worker, status, extra):
    scope, _, _ = await store.capture([message()])
    async with store.factory.begin() as db:
        await db.execute(update(models.Snapshot).where(models.Snapshot.id == scope.snapshot_id)
            .values(status=status, extra_data={"chat_inventory_format": 1, **extra}))
    async with store.factory() as db:
        with pytest.raises(Exception, match="failed|cancelled|unavailable"):
            await worker.fetch_snapshot_items(db, [str(scope.snapshot_id)], [])


@pytest.mark.parametrize("corrupt", [False, True])
async def test_generic_zip_streams_exact_encrypted_backend_child_and_rejects_bad_content(store, worker, monkeypatch, corrupt):
    from shared.crypto import blob_cipher, runtime
    from shared.storage.router import router
    from shared.storage.seaweedfs import SeaweedStore
    scope = await store.new_scope()
    lease = await store.begin(scope)
    child = await store.binary(lease)
    captured = [parent_for(child), child]
    await store.stage(lease, captured)
    await store.observe(scope, lease, captured)
    await store.publish(lease)
    await terminal(store, scope)
    # The first writer's snapshot can disappear; this selected inventory
    # still owns the exact shared captured versions and object descriptor.
    second, _, _ = await store.capture(captured)
    await terminal(store, second)
    async with store.factory.begin() as db:
        await db.execute(delete(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == scope.snapshot_id))
        await db.execute(delete(models.Snapshot).where(models.Snapshot.id == scope.snapshot_id))
    export_id = uuid4()
    async with store.factory.begin() as db:
        db.add(models.Job(id=export_id, tenant_id=store.tenant, type=models.JobType.EXPORT, status=models.JobStatus.RUNNING))
    key, key_id = b"k" * 32, b"i" * 16
    class Keys:
        async def dek_for_key_id(self, actual):
            assert actual == key_id
            return key
    monkeypatch.setattr(runtime, "_dek_manager", Keys())
    encrypted = blob_cipher.seal(b"bad" if corrupt else b"abc", key, key_id)
    calls = []
    class Body:
        async def iter_chunks(self, size):
            for start in range(0, len(encrypted), 7):
                yield encrypted[start:start + 7]
    class Client:
        async def get_object(self, **kwargs):
            calls.append((kwargs["Bucket"], kwargs["Key"]))
            async with store.factory() as db:
                assert list((await db.scalars(select(models.ChatCataloguePin))).all())
            return {"Body": Body()}
    source_store = object.__new__(SeaweedStore)
    source_store._bucket = lambda value: value
    source_store._key = lambda _container, value: value
    @asynccontextmanager
    async def client():
        yield Client()
    source_store._client_ctx = client
    def explicit_backend(backend_id):
        assert backend_id == str(store.backend)
        return source_store
    monkeypatch.setattr(router, "get_store_by_id", explicit_backend)
    monkeypatch.setattr(worker_module(worker).azure_storage_manager, "get_shard_for_item",
        lambda *_args: pytest.fail("catalogue source used legacy resource/shard routing"))
    artifacts = []
    async def upload_file(_container, _path, filename, **kwargs):
        artifacts.append(Path(filename).read_bytes())
        return {"success": True}
    monkeypatch.setattr(worker_module(worker).azure_storage_manager, "get_default_shard",
        lambda: SimpleNamespace(backend_id=str(store.backend), upload_blob_from_file=upload_file))
    async with store.factory() as db:
        selected = await worker.fetch_snapshot_items(db, [str(second.snapshot_id)], [])
        if corrupt:
            with pytest.raises(Exception, match="checksum|size"):
                await worker.export_as_zip(db, selected, {"jobId": str(export_id)}, {})
            assert artifacts == []
        else:
            await worker.export_as_zip(db, selected, {"jobId": str(export_id)}, {})
            with zipfile.ZipFile(io.BytesIO(artifacts[0])) as archive:
                binary = [name for name in archive.namelist() if not name.endswith(".json")]
                assert len(binary) == 1 and archive.read(binary[0]) == b"abc"
    assert calls == [(child.content.container, child.content.path)]
    async with store.factory() as db:
        assert not list((await db.scalars(select(models.ChatCataloguePin))).all())


@pytest.mark.parametrize("runtime_ready", [True, False])
async def test_generic_export_target_uses_tenant_encryption_or_fails_closed_before_upload(store, worker, monkeypatch, runtime_ready):
    from shared.crypto import envelope, runtime
    from shared.storage.seaweedfs import SeaweedStore
    from shared.storage.router import router
    from shared.azure_storage import _StoreFacade
    scope, _, _ = await store.capture([message("encrypted export body")])
    await terminal(store, scope)
    export_id = uuid4()
    async with store.factory.begin() as db:
        db.add(models.Job(id=export_id, tenant_id=store.tenant, type=models.JobType.EXPORT, status=models.JobStatus.RUNNING))
        db.add(models.EncryptionConfig(id="singleton", provider="env", enabled=True))
    key, key_id = b"t" * 32, b"u" * 16
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
            for start in range(0, len(self.data), 31):
                yield self.data[start:start + 31]
        def close(self):
            pass
    class Client:
        async def create_multipart_upload(self, **kwargs):
            saved["route"] = (kwargs["Bucket"], kwargs["Key"])
            saved["metadata"] = kwargs["Metadata"]
            return {"UploadId": "target-upload"}
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
    target.backend_id = str(uuid4())
    target._endpoint, target._upload_concurrency, target._tenant_id = "https://storage.invalid", 1, None
    target._bucket = lambda container: container
    target._key = lambda _container, path: path
    target.ensure_container = AsyncMock()
    @asynccontextmanager
    async def client():
        yield Client()
    target._client_ctx = client
    clone = SimpleNamespace(backend_id=target.backend_id, kind="seaweedfs")
    monkeypatch.setattr(worker_module(worker).azure_storage_manager, "get_default_shard", lambda: _StoreFacade(clone))
    def exact_target(backend_id):
        assert backend_id == target.backend_id
        return target
    monkeypatch.setattr(router, "get_store_by_id", exact_target)
    async with store.factory() as db:
        items = await worker.fetch_snapshot_items(db, [str(scope.snapshot_id)], [])
        if not runtime_ready:
            with pytest.raises(Exception, match="encryption runtime is unavailable"):
                await worker.export_as_zip(db, items, {"jobId": str(export_id)}, {})
            assert saved == {} and tenants == []
        else:
            result = await worker.export_as_zip(db, items, {"jobId": str(export_id)}, {})
            assert envelope.is_encrypted(saved["bytes"])
            assert result["chat_catalogue_format"] == 1 and result["backend_id"] == target.backend_id
            assert result["encryption_key_id"] == str(UUID(bytes=key_id))
            assert tenants == [str(store.tenant)] and target._tenant_id is None
            decoded = b"".join([chunk async for chunk in target.download_stream(result["container"], result["blob_path"])])
            with zipfile.ZipFile(io.BytesIO(decoded)) as archive:
                content = json.loads(archive.read(archive.namelist()[0]))
                assert content["body"]["content"] == "encrypted export body"
            assert saved["metadata"]["tenant_id"] == str(store.tenant)
    async with store.factory() as db:
        assert not list((await db.scalars(select(models.ChatCataloguePin))).all())


async def test_legacy_union_and_captured_exact_snapshot_can_be_exported_together(store, worker, monkeypatch):
    legacy, later = await store.new_scope(), await store.new_scope()
    async with store.factory.begin() as db:
        await db.execute(update(models.Snapshot).where(models.Snapshot.id.in_([legacy.snapshot_id, later.snapshot_id]))
            .values(status=models.SnapshotStatus.COMPLETED, extra_data={}))
        db.add(models.SnapshotItem(snapshot_id=legacy.snapshot_id, tenant_id=store.tenant,
            backend_id=store.backend, external_id="legacy-message", item_type="TEAMS_CHAT_MESSAGE", name="legacy",
            extra_data={"raw": {"id": "legacy-message", "body": {"content": "legacy content"}}}))
    captured, _, _ = await store.capture([message("catalogue content")])
    await terminal(store, captured)
    export_id = uuid4()
    async with store.factory.begin() as db:
        db.add(models.Job(id=export_id, tenant_id=store.tenant, type=models.JobType.EXPORT, status=models.JobStatus.RUNNING))
    saved = []
    async def upload_file(_container, _path, filename, **kwargs):
        saved.append(Path(filename).read_bytes())
        return {"success": True}
    monkeypatch.setattr(worker_module(worker).azure_storage_manager, "get_default_shard",
        lambda: SimpleNamespace(backend_id=str(store.backend), upload_blob_from_file=upload_file))
    async with store.factory() as db:
        selected = await worker.fetch_snapshot_items(db, [str(later.snapshot_id), str(captured.snapshot_id)], [])
        assert {item.external_id for item in selected} == {"legacy-message", "message-alpha"}
        result = await worker.export_as_zip(db, selected, {"jobId": str(export_id)}, {})
    assert result["exported_count"] == 2
    with zipfile.ZipFile(io.BytesIO(saved[0])) as archive:
        assert json.loads(archive.read("teams_messages/legacy-message.json"))["body"]["content"] == "legacy content"
        catalogue_name = next(name for name in archive.namelist() if name.startswith("catalogue/"))
        assert json.loads(archive.read(catalogue_name))["body"]["content"] == "catalogue content"
