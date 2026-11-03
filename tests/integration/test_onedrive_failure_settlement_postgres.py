"""Required blob-link failure cannot become success through the single-resource wrapper."""
import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import event, select

from test_chat_catalogue_postgres import store
from test_copy_worker_handoff_postgres import bw
from test_onedrive_resume_postgres import setup_worker, file_item
from test_onedrive_download_version_postgres import install_metadata
from shared import models
from shared import provider_work as work

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("landed,record_failure", [(False, True), (True, True), (False, False)])
@pytest.mark.parametrize("failure", ["flush", "before_url", "during_stream", "stream_failure"])
@pytest.mark.parametrize("v2", [False, True], ids=["legacy", "v2"])
async def test_nonpartition_required_content_failure_survives_aggregation_and_single_wrapper(
        store, bw, monkeypatch, landed, record_failure, failure, v2):
    good, bad = file_item("good", file={}), file_item("bad", file={})
    env = await setup_worker(store, bw, monkeypatch, [good, bad])
    monkeypatch.setattr(bw.settings, "ONEDRIVE_BACKUP_V2_ENABLED", v2)
    if landed:
        assert await env.run([good]) == (1, 1024)
    async with store.factory.begin() as session:
        resource = await session.get(models.Resource, store.resource)
        resource.extra_data = {"user_id": "principal-alpha", "drive_id": "drive-A"}
    async with store.factory() as session:
        snapshot = await session.get(models.Snapshot, env.scope.snapshot_id)
        resource = await session.get(models.Resource, store.resource)
        tenant = await session.get(models.Tenant, store.tenant)
    worker = env.worker
    worker.create_snapshot = AsyncMock(return_value=snapshot)
    worker.get_sla_policy = AsyncMock(return_value=None)
    worker._onedrive_backup_semaphore = asyncio.Semaphore(1)
    worker.update_resource_backup_info = AsyncMock()
    worker._seal_copy_handoff = AsyncMock(return_value=None)
    monkeypatch.setattr(bw.settings, "ONEDRIVE_PARTITION_ENABLED", False)
    monkeypatch.setattr(bw, "_raise_if_job_cancelled", AsyncMock())
    env.graph.GRAPH_URL = "https://graph.invalid"
    env.graph._get = AsyncMock(return_value={"value": [good, bad], "@odata.deltaLink": "https://graph.invalid/delta-final"})
    if not record_failure:
        worker._record_enumeration_failure = AsyncMock(return_value="not_recorded")
    if failure in ("before_url", "during_stream"):
        source = {"good": dict(good), "bad": dict(bad)}
        install_metadata(env, source)
        if failure == "before_url":
            source["bad"]["eTag"] = "version-2"
        else:
            async def stream(**kwargs):
                if kwargs["metadata"]["source_item_id"] == "bad":
                    source["bad"]["eTag"] = "version-2"
                return {"success": True, "content_sha256": "content-digest"}
            worker._stream_graph_to_backend = stream
    elif failure == "stream_failure":
        worker._stream_graph_to_backend = AsyncMock(return_value={"success": False, "error": "synthetic upload failure"})

    def reject_link(conn, cursor, statement, parameters, context, executemany):
        if failure == "flush" and "UPDATE snapshot_items AS si" in statement:
            raise RuntimeError("synthetic link transaction failure")

    event.listen(store.engine.sync_engine, "before_cursor_execute", reject_link)
    try:
        with pytest.raises(bw.EnumerationFailed):
            await worker._backup_user_content_single(env.graph, resource, snapshot, tenant,
                {"jobId": str(snapshot.job_id), "_job_lease_token": 0})
    finally:
        event.remove(store.engine.sync_engine, "before_cursor_execute", reject_link)
    async with store.factory() as session:
        live = await session.get(models.Snapshot, snapshot.id)
        assert live.status != models.SnapshotStatus.COMPLETED
        if record_failure:
            assert live.status in (models.SnapshotStatus.PARTIAL, models.SnapshotStatus.FAILED)
        row = await session.scalar(select(models.SnapshotItem).where(
            models.SnapshotItem.snapshot_id == snapshot.id, models.SnapshotItem.external_id == "bad"))
        assert row.blob_path is None
        current_resource = await session.get(models.Resource, store.resource)
        assert not current_resource.extra_data.get("onedrive_delta_link")


@pytest.mark.parametrize("replaced_at", ["before_capture", "during_stream", "stream_conflict", "stringified_conflict", "owned_failure"])
async def test_provider_ownership_fences_nonpartition_failure(
        store, bw, monkeypatch, replaced_at):
    item = file_item(file={})
    env = await setup_worker(store, bw, monkeypatch, [item])
    async with store.factory.begin() as session:
        resource = await session.get(models.Resource, store.resource)
        resource.extra_data = {"user_id": "principal-alpha", "drive_id": "drive-A"}
        await work.ensure_work(session, snapshot_id=env.scope.snapshot_id,
            snapshot_token=0, job_token=0, continuation={"kind": "single"})
    async with store.factory.begin() as session:
        original = await work.claim_due_work(session, owner_id=uuid4())
    assert original is not None
    async with store.factory() as session:
        snapshot = await session.get(models.Snapshot, env.scope.snapshot_id)
        resource = await session.get(models.Resource, store.resource)
        tenant = await session.get(models.Tenant, store.tenant)
    worker = env.worker
    worker.create_snapshot = AsyncMock(return_value=snapshot)
    worker.get_sla_policy = AsyncMock(return_value=None)
    worker._onedrive_backup_semaphore = asyncio.Semaphore(1)
    worker.update_resource_backup_info = AsyncMock()
    worker._seal_copy_handoff = AsyncMock(return_value=None)
    failure_writer = AsyncMock(wraps=worker._record_enumeration_failure)
    worker._record_enumeration_failure = failure_writer
    monkeypatch.setattr(bw.settings, "ONEDRIVE_PARTITION_ENABLED", False)
    monkeypatch.setattr(bw, "_raise_if_job_cancelled", AsyncMock())
    env.graph.GRAPH_URL = "https://graph.invalid"
    env.graph._get = AsyncMock(return_value={"value": [item], "@odata.deltaLink": "https://graph.invalid/delta-final"})
    replacement = None

    async def replace_owner():
        nonlocal replacement
        async with store.factory.begin() as session:
            row = await session.get(models.ProviderWork, original.work_id)
            row.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        async with store.factory.begin() as session:
            replacement = await work.claim_due_work(session, owner_id=uuid4())
            assert replacement is not None and replacement.generation > original.generation
            live = await session.get(models.Snapshot, snapshot.id)
            live.item_count, live.bytes_total = 7, 8192
            live.extra_data = {"replacement_generation": replacement.generation}

    if replaced_at == "before_capture":
        await replace_owner()
    else:
        async def stream(**kwargs):
            if replaced_at == "owned_failure":
                return {"success": False, "error": "storage unavailable"}
            await replace_owner()
            if replaced_at == "stream_conflict":
                raise work.ProviderWorkConflict("provider work ownership changed")
            if replaced_at == "stringified_conflict":
                return {"success": False, "error": "provider work ownership changed"}
            return {"success": True, "content_sha256": "content-digest"}
        worker._stream_graph_to_backend = stream

    with work.work_scope(original):
        expected = bw.EnumerationFailed if replaced_at == "owned_failure" else bw.ChatCatalogueWorkLeaseMoved
        with pytest.raises(expected):
            await worker._backup_user_content_single(env.graph, resource, snapshot, tenant,
                {"jobId": str(snapshot.job_id), "_job_lease_token": original.job_token})
    if replaced_at not in ("stringified_conflict", "owned_failure"):
        failure_writer.assert_not_awaited()
    else:
        failure_writer.assert_awaited_once()
    async with store.factory() as session:
        live = await session.get(models.Snapshot, snapshot.id)
        if replaced_at == "owned_failure":
            assert live.status == models.SnapshotStatus.FAILED
            assert live.lease_owner_id == original.owner_id
            return
        assert live.status == models.SnapshotStatus.IN_PROGRESS
        assert (live.item_count, live.bytes_total) == (7, 8192)
        assert live.extra_data == {"replacement_generation": replacement.generation}
        assert live.lease_owner_id == replacement.owner_id
        assert live.lease_token == replacement.parent_token
        provider = await session.get(models.ProviderWork, original.work_id)
        assert provider.state == "RESUMING"
        assert provider.lease_owner_id == replacement.owner_id
        row = await session.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == snapshot.id))
        assert row.blob_path is None
