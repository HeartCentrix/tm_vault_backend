"""OneDrive resumes only durable, version-matching rows owned by its snapshot."""
import asyncio
from copy import deepcopy
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import event, select, update

from test_chat_catalogue_postgres import store
from test_copy_worker_handoff_postgres import bw
from shared import models

pytestmark = pytest.mark.integration


def file_item(key="file-1", **changes):
    return {"id": key, "name": key, "size": 1024, "eTag": "version-1",
            "parentReference": {"driveId": "drive-A"}, **changes}


async def setup_worker(store, bw, monkeypatch, items, *, blob=False, legacy=False):
    scope = await store.new_scope()
    legacy_result = {"unrelated": "preserve"}
    if legacy:
        legacy_result["backup_checkpoint"] = {
            "resource_id": str(store.resource), "drive_id": "drive-A",
            "snapshot_id": str(scope.snapshot_id), "files_done_ext_ids": [i["id"] for i in items],
            "files_done_count": len(items), "bytes_done": sum(i["size"] for i in items)}
    async with store.factory.begin() as session:
        resource = await session.get(models.Resource, store.resource)
        resource.type = models.ResourceType.USER_ONEDRIVE
        resource.extra_data = {"drive_id": "drive-A"}
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        snapshot.extra_data = {}
        job = await session.get(models.Job, snapshot.job_id)
        job.result = legacy_result
        for item in items:
            session.add(models.SnapshotItem(snapshot_id=snapshot.id, tenant_id=store.tenant,
                backend_id=store.backend, external_id=item["id"], item_type="ONEDRIVE_FILE",
                name=item["name"], content_size=item["size"], extra_data={"raw": deepcopy(item)},
                blob_path=("saved/" + item["id"]) if blob else None))
    monkeypatch.setattr(bw, "async_session_factory", store.factory)
    monkeypatch.setattr(bw, "azure_storage_manager", SimpleNamespace(
        get_shard_for_resource=lambda *args: SimpleNamespace(name="test"),
        get_container_name=lambda *args: "files",
        build_blob_path=lambda tenant, resource, snapshot, item: f"{snapshot}/{item}"))
    monkeypatch.setattr(bw.settings, "ONEDRIVE_BACKUP_V2_ENABLED", True)
    monkeypatch.setattr(bw.settings, "ONEDRIVE_BACKUP_FILE_CONCURRENCY", 2)
    monkeypatch.setattr(bw.settings, "ONEDRIVE_BACKUP_FILE_TIMEOUT_SECONDS", 0)
    monkeypatch.setenv("ONEDRIVE_FLUSH_INTERVAL_S", "60")
    monkeypatch.setenv("ONEDRIVE_FLUSH_BACKLOG_CAP", "1000")
    monkeypatch.setenv("ONEDRIVE_RETRY_QUEUE_ENABLED", "false")
    calls = []

    async def urls(drive_id, ids):
        calls.extend(ids)
        return {key: (f"https://download.invalid/{key}", 1024, "") for key in ids}

    async def stream(**kwargs):
        return {"success": True, "content_sha256": "content-digest"}

    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker.worker_id = "onedrive-resume-test"
    worker._huge_sem = asyncio.Semaphore(2)
    worker._huge_sem_capacity = 2
    worker._stream_graph_to_backend = stream
    graph = SimpleNamespace(get_download_urls_batch=urls)
    bw._PARTITION_LEASE_CTX.set(None)

    async def run(current=None, **kwargs):
        async with store.factory() as session:
            snapshot = await session.get(models.Snapshot, scope.snapshot_id)
            resource = await session.get(models.Resource, store.resource)
            tenant = await session.get(models.Tenant, store.tenant)
        return await worker._useronedrive_backup_files(graph, resource, snapshot, tenant,
            "drive-A", deepcopy(items if current is None else current), **kwargs)

    return SimpleNamespace(scope=scope, worker=worker, run=run, calls=calls)


async def test_repeated_delta_id_downloads_only_latest_observed_version(store, bw, monkeypatch):
    original, latest = file_item(), file_item(eTag="version-2")
    env = await setup_worker(store, bw, monkeypatch, [original])
    assert await env.run([original, latest]) == (1, 1024)
    assert env.calls == ["file-1"]
    async with store.factory() as session:
        row = await session.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == env.scope.snapshot_id))
        assert row.extra_data["raw"]["eTag"] == "version-2"


async def test_supplied_job_generation_is_not_recaptured_after_enumeration(store, bw, monkeypatch):
    env = await setup_worker(store, bw, monkeypatch, [file_item()])
    with pytest.raises(RuntimeError, match="ownership"):
        await env.run(expected_job_token=42)
    assert not env.calls


async def test_sibling_job_checkpoint_eviction_does_not_repeat_durable_downloads(store, bw, monkeypatch):
    env = await setup_worker(store, bw, monkeypatch, [file_item()])
    assert await env.run() == (1, 1024)
    sibling_result = {"unrelated": "preserve", "backup_checkpoint": {
        "resource_id": str(uuid4()), "drive_id": "sibling-drive", "snapshot_id": str(uuid4()),
        "files_done_ext_ids": ["file-1"]}}
    async with store.factory.begin() as session:
        job = await session.get(models.Job, (await session.get(models.Snapshot, env.scope.snapshot_id)).job_id)
        job.result = sibling_result
    assert await env.run() == (0, 0)
    assert env.calls == ["file-1"]
    async with store.factory() as session:
        job = await session.get(models.Job, (await session.get(models.Snapshot, env.scope.snapshot_id)).job_id)
        assert job.result == sibling_result


async def add_partition(store, scope, items, index=0):
    pid = uuid4()
    async with store.factory.begin() as session:
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        session.add(models.SnapshotPartition(id=pid, snapshot_id=snapshot.id, job_id=snapshot.job_id,
            resource_id=store.resource, tenant_id=store.tenant, drive_id="drive-A",
            file_ids=[item["id"] for item in items], partition_index=index,
            total_files=len(items), total_bytes_est=1024 * len(items), status="IN_PROGRESS", lease_token=1))
    return pid


async def test_concurrent_shards_keep_both_sets_of_durable_receipts(store, bw, monkeypatch):
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
    assert sorted(env.calls) == ["file-1", "file-2"]


@pytest.mark.parametrize("kind", ["metadata_only", "changed_version", "changed_drive", "unknown_version", "changed_size", "legacy_unstamped"])
async def test_legacy_done_set_cannot_skip_unproven_current_content(store, bw, monkeypatch, kind):
    saved = file_item()
    if kind == "changed_drive":
        saved["parentReference"] = {"driveId": "other-drive"}
    if kind == "unknown_version":
        saved.pop("eTag")
    current = file_item(**({"eTag": "version-2"} if kind == "changed_version" else {}))
    if kind == "changed_size":
        saved["size"] = 512
    env = await setup_worker(store, bw, monkeypatch, [saved], blob=kind != "metadata_only", legacy=True)
    assert await env.run([current]) == (1, 1024)
    async with store.factory() as session:
        row = await session.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == env.scope.snapshot_id))
        assert row.extra_data["raw"] == current
        assert row.blob_path == f"{env.scope.snapshot_id}/file-1"


async def test_metadata_rewrite_cannot_relabel_previously_downloaded_bytes(store, bw, monkeypatch):
    original, latest = file_item(), file_item(eTag="version-2")
    env = await setup_worker(store, bw, monkeypatch, [original])
    assert await env.run() == (1, 1024)
    async with store.factory() as session:
        # Exercise the actual replay enumeration path: its conflict is a no-op.
        await bw._bulk_upsert_snapshot_items(session, [{"id": uuid4(),
            "snapshot_id": env.scope.snapshot_id, "tenant_id": store.tenant, "backend_id": store.backend,
            "external_id": "file-1", "item_type": "ONEDRIVE_FILE", "name": "file-1",
            "content_size": 1024, "extra_data": {"raw": latest}}])
    async with store.factory.begin() as session:
        row = await session.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == env.scope.snapshot_id))
        assert row.extra_data["raw"] == original
        # A metadata refresh may describe a newer observation without replacing
        # its bytes. The downloaded-source receipt must remain independent.
        row.extra_data = {**row.extra_data, "raw": latest}
    assert await env.run([latest]) == (1, 1024)
    assert env.calls == ["file-1", "file-1"]


async def test_unmatched_required_row_cannot_report_success(store, bw, monkeypatch):
    env = await setup_worker(store, bw, monkeypatch, [])
    with pytest.raises(RuntimeError, match="row|persist|durab"):
        await env.run([file_item()])


@pytest.mark.parametrize("permanent", [False, True], ids=["retry", "exhausted"])
async def test_failed_flush_keeps_pending_rows_and_never_reports_false_success(store, bw, monkeypatch, permanent):
    env = await setup_worker(store, bw, monkeypatch, [file_item()])
    failures = []

    def reject_update(conn, cursor, statement, parameters, context, executemany):
        if "UPDATE snapshot_items AS si" in statement and (permanent or not failures):
            failures.append(True)
            raise RuntimeError("synthetic required blob row flush failure")

    event.listen(store.engine.sync_engine, "before_cursor_execute", reject_update)
    try:
        if permanent:
            with pytest.raises(RuntimeError, match="flush failure"):
                await env.run()
        else:
            assert await env.run() == (1, 1024)
    finally:
        event.remove(store.engine.sync_engine, "before_cursor_execute", reject_update)
    async with store.factory() as session:
        row = await session.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == env.scope.snapshot_id))
        assert bool(row.blob_path) is not permanent


@pytest.mark.parametrize("moved", ["snapshot", "job", "partition", "cancelled"])
async def test_original_ownership_is_required_when_uploaded_blob_is_linked(store, bw, monkeypatch, moved):
    env = await setup_worker(store, bw, monkeypatch, [file_item()])
    pid = await add_partition(store, env.scope, [file_item()]) if moved == "partition" else None
    if pid:
        bw._bind_partition_lease(pid, {"snapshot_id": env.scope.snapshot_id, "lease_token": 1})

    async def stream(**kwargs):
        async with store.factory.begin() as session:
            snapshot = await session.get(models.Snapshot, env.scope.snapshot_id)
            if moved == "snapshot":
                snapshot.lease_token += 1
            elif moved == "partition":
                partition = await session.get(models.SnapshotPartition, pid)
                partition.lease_token += 1
            else:
                job = await session.get(models.Job, snapshot.job_id)
                if moved == "cancelled":
                    job.status = models.JobStatus.CANCELLED
                else:
                    job.lease_token += 1
        return {"success": True, "content_sha256": "content-digest"}

    env.worker._stream_graph_to_backend = stream
    with pytest.raises(RuntimeError, match="ownership|lease|cancel|active"):
        await env.run()
    async with store.factory() as session:
        row = await session.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == env.scope.snapshot_id))
        assert row.blob_path is None
