"""Manual API jobs must become active before the catalogue's ownership gate."""
import asyncio
import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import delete, select

from test_chat_catalogue_postgres import store
from shared import models
from shared.chat_catalogue_entry import adopt_chat_catalogue
from shared.storage.router import router

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def worker_module():
    path = Path(__file__).resolve().parents[2] / "workers/backup-worker/main.py"
    spec = importlib.util.spec_from_file_location("catalogue_single_intake_worker", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("status", [models.JobStatus.QUEUED, models.JobStatus.PENDING, models.JobStatus.RUNNING])
async def test_real_single_intake_activates_manual_job_before_catalogue_adoption(store, worker_module, monkeypatch, status):
    scope = await store.new_scope()
    async with store.factory.begin() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        job_id = snapshot.job_id
        await db.execute(delete(models.Snapshot).where(models.Snapshot.id == snapshot.id))
        (await db.get(models.Job, job_id)).status = status
        (await db.get(models.Resource, scope.resource_id)).extra_data = {"user_id": scope.principal_id}
    monkeypatch.setattr(worker_module, "async_session_factory", store.factory)
    monkeypatch.setattr(worker_module.settings, "CHAT_CATALOGUE_WRITER_ENABLED", True)
    monkeypatch.setattr(router, "_active_backend_id", str(store.backend))
    worker = worker_module.BackupWorker.__new__(worker_module.BackupWorker)
    worker.worker_id = "manual-intake-test"
    worker.get_graph_client = AsyncMock(return_value=object())
    worker.audit_logger = SimpleNamespace(log=AsyncMock())
    seen = []

    async def handler(graph, resource, snapshot, tenant, envelope):
        assert snapshot.backend_id == store.backend
        async with store.factory.begin() as db:
            assert (await db.get(models.Job, job_id)).status == models.JobStatus.RUNNING
            assert await adopt_chat_catalogue(db, resource_id=resource.id, snapshot_id=snapshot.id,
                writer_enabled=True, backend_id=store.backend, snapshot_lease_token=snapshot.lease_token)
            seen.append((await db.get(models.Snapshot, snapshot.id)).type)
        raise asyncio.CancelledError  # Stop after the real DB intake/gate, before source I/O.

    worker._backup_user_content_single = handler
    with pytest.raises(asyncio.CancelledError):
        await worker._process_single_backup(job_id, {"jobId": str(job_id)}, str(scope.resource_id))
    assert seen == [models.SnapshotType.FULL]


@pytest.mark.parametrize("status", [models.JobStatus.FAILED, models.JobStatus.COMPLETED, models.JobStatus.CANCELLING])
async def test_catalogue_single_intake_cannot_reactivate_terminal_or_cancelling_job(store, worker_module, monkeypatch, status):
    scope = await store.new_scope()
    async with store.factory.begin() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        job_id = snapshot.job_id
        await db.execute(delete(models.Snapshot).where(models.Snapshot.id == snapshot.id))
        (await db.get(models.Job, job_id)).status = status
    monkeypatch.setattr(worker_module, "async_session_factory", store.factory)
    monkeypatch.setattr(worker_module.settings, "CHAT_CATALOGUE_WRITER_ENABLED", True)
    worker = worker_module.BackupWorker.__new__(worker_module.BackupWorker)
    worker.worker_id = "manual-intake-test"
    worker.get_graph_client = AsyncMock(side_effect=AssertionError("inactive jobs must not contact the source"))
    await worker._process_single_backup(job_id, {"jobId": str(job_id)}, str(scope.resource_id))
    async with store.factory() as db:
        assert (await db.get(models.Job, job_id)).status == status
        assert await db.scalar(select(models.Snapshot.id).where(models.Snapshot.job_id == job_id)) is None
