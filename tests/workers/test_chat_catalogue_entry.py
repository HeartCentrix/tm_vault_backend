"""Exercise worker entry/finalization seams rather than mirrored capture code."""
from __future__ import annotations

import ast
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
import importlib.util
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def bw():
    path = _ROOT / "workers/backup-worker/main.py"
    spec = importlib.util.spec_from_file_location("catalogue_entry_worker", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


async def test_catalogue_entry_preempts_legacy_fetch_copy_and_detached_paths(bw):
    tree = ast.parse((_ROOT / "workers/backup-worker/main.py").read_text())
    outer = next(n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef) and n.name == "_backup_user_content_parallel")
    function = next(n for n in ast.walk(outer) if isinstance(n, ast.AsyncFunctionDef) and n.name == "_backup_one")
    namespace = dict(bw.__dict__)
    resource = SimpleNamespace(id=uuid4(), type=bw.ResourceType.USER_CHATS, display_name="Synthetic")
    result = {"chat_inventory_format": 1, "item_count": 2, "bytes_total": 100, "bytes_added": 0, "new_item_count": 0}
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker.worker_id = "test"
    worker.create_snapshot = AsyncMock(return_value=SimpleNamespace(id=uuid4()))
    worker._maybe_backup_chat_catalogue = AsyncMock(return_value=result)
    legacy = AsyncMock(side_effect=AssertionError("legacy fetch must not run"))
    namespace.update(self=worker, semaphore=asyncio.Semaphore(1), _snap_by_resource={},
        _raise_if_job_cancelled=AsyncMock(), job_id=uuid4(), message={}, tenant=object(), graph_client=object(),
        _fetch_for=legacy)
    exec(compile(ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[])), "worker-entry", "exec"), namespace)
    assert await namespace["_backup_one"](resource) == result
    worker._maybe_backup_chat_catalogue.assert_awaited_once()
    legacy.assert_not_awaited()


async def test_single_wrapper_preserves_catalogue_logical_and_delta_counters(bw, monkeypatch):
    result = {"chat_inventory_format": 1, "item_count": 3, "new_item_count": 0, "bytes_total": 100, "bytes_added": 0}
    stored = SimpleNamespace(id=uuid4(), status=bw.SnapshotStatus.PARTIAL,
        item_count=3, new_item_count=0, bytes_total=100, bytes_added=0, extra_data={"chat_inventory_format": 1})
    session = SimpleNamespace(get=AsyncMock(return_value=stored), commit=AsyncMock())
    @asynccontextmanager
    async def factory():
        yield session
    monkeypatch.setattr(bw, "async_session_factory", factory)
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker.worker_id = "test"
    worker._backup_user_content_parallel = AsyncMock(return_value=result)
    returned = await worker._backup_user_content_single(object(), SimpleNamespace(id=uuid4()),
        SimpleNamespace(id=stored.id), object(), {"jobId": str(uuid4())})
    assert returned == result
    assert stored.bytes_total == 100 and stored.bytes_added == 0 and stored.new_item_count == 0
    session.commit.assert_not_awaited()


async def test_outer_finalizer_keeps_persisted_catalogue_partial_outcome(bw, monkeypatch):
    snapshot_id = uuid4()
    stored = SimpleNamespace(id=snapshot_id, status=bw.SnapshotStatus.PARTIAL, type=bw.SnapshotType.FULL,
        item_count=3, new_item_count=1, bytes_total=100, bytes_added=10, completed_at=datetime.utcnow(),
        duration_secs=4, extra_data={"chat_inventory_format": 1, "degraded_reason": "known missing content"})
    stale = SimpleNamespace(id=snapshot_id, resource_id=uuid4(), job_id=uuid4(), status=bw.SnapshotStatus.IN_PROGRESS,
        type=bw.SnapshotType.INCREMENTAL, item_count=0, new_item_count=0, bytes_total=0, bytes_added=0,
        extra_data={}, delta_tokens_json={}, started_at=None, duration_secs=0)
    session = SimpleNamespace(get=AsyncMock(side_effect=lambda model, _id, **kw: stored if model is bw.Snapshot else None))
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker.worker_id = "test"
    worker.flush_version_rows = AsyncMock(return_value=0)
    worker._write_snapshot_terminal = AsyncMock(return_value=True)
    worker._check_snapshot_anomaly = AsyncMock()
    monkeypatch.setattr(bw, "_coerce_snapshot_terminal_on_cancel", AsyncMock(return_value=bw.SnapshotStatus.COMPLETED))
    await worker.complete_snapshot(session, stale, {"chat_inventory_format": 1, "item_count": 3,
        "new_item_count": 0, "bytes_total": 100, "bytes_added": 0})
    assert stale.status == bw.SnapshotStatus.PARTIAL
    assert stale.bytes_total == 100 and stale.bytes_added == 10
    worker._write_snapshot_terminal.assert_not_awaited()


def test_catalogue_writer_defaults_off(monkeypatch):
    from shared.config import Settings
    monkeypatch.delenv("CHAT_CATALOGUE_WRITER_ENABLED", raising=False)
    assert Settings().CHAT_CATALOGUE_WRITER_ENABLED is False


async def test_parallel_wrapper_returns_catalogue_counters_and_propagates_moved_owner(bw, monkeypatch):
    result = {"chat_inventory_format": 1, "item_count": 2, "bytes_total": 100, "bytes_added": 0,
        "new_item_count": 0, "status": "PARTIAL"}
    resource = SimpleNamespace(id=uuid4(), type=bw.ResourceType.USER_CHATS, display_name="Synthetic")
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker.worker_id = "test"
    worker.create_snapshot = AsyncMock(return_value=SimpleNamespace(id=uuid4()))
    worker._maybe_backup_chat_catalogue = AsyncMock(return_value=result)
    monkeypatch.setattr(bw, "_raise_if_job_cancelled", AsyncMock())
    args = ([resource], object(), object(), {}, uuid4())
    assert await worker._backup_user_content_parallel(*args) == result
    worker._maybe_backup_chat_catalogue.side_effect = bw.ChatCatalogueWorkLeaseMoved("moved")
    with pytest.raises(bw.ChatCatalogueWorkLeaseMoved):
        await worker._backup_user_content_parallel(*args)


async def test_catalogue_partial_job_is_terminal_and_cannot_reverse_cancellation(bw):
    result = {"chat_inventory_format": 1, "status": "PARTIAL", "snapshots_partial": 1}
    job = SimpleNamespace(status=bw.JobStatus.RUNNING, completed_at=None, progress_pct=12)
    session = SimpleNamespace(get=AsyncMock(return_value=job), commit=AsyncMock())
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker.worker_id = "test"
    await worker.update_job_status(session, uuid4(), bw.JobStatus.COMPLETED, result)
    assert job.status == bw.JobStatus.COMPLETED and job.result["snapshots_partial"] == 1
    assert job.completed_at is not None and job.progress_pct == 100
    job.status = bw.JobStatus.CANCELLED
    await worker.update_job_status(session, uuid4(), bw.JobStatus.COMPLETED, result)
    assert job.status == bw.JobStatus.CANCELLED


async def test_single_wrapper_returns_failed_terminal_catalogue_without_rewriting(bw, monkeypatch):
    result = {"chat_inventory_format": 1, "status": "FAILED", "failed": True, "item_count": 0}
    snapshot = SimpleNamespace(id=uuid4(), status=bw.SnapshotStatus.FAILED)
    session = SimpleNamespace(get=AsyncMock(return_value=snapshot), commit=AsyncMock())
    @asynccontextmanager
    async def factory():
        yield session
    monkeypatch.setattr(bw, "async_session_factory", factory)
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker._backup_user_content_parallel = AsyncMock(return_value=result)
    assert await worker._backup_user_content_single(object(), object(), snapshot, object(), {}) == result
    session.commit.assert_not_awaited()
