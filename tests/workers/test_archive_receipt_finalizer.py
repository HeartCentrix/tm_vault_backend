"""Marked archive snapshots cannot finalize from optimistic frozen counters."""
import os
os.environ.setdefault('ALLOW_DEV_JWT_SECRETS', 'true')
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4
from datetime import datetime
import pytest
import importlib.util
from pathlib import Path
_spec = importlib.util.spec_from_file_location("archive_finalizer_fixture", Path(__file__).with_name("test_durable_copy_worker_handoff.py"))
_fixture = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_fixture)
worker_module = _fixture.worker_module


@pytest.mark.parametrize('missing', [0, 1])
async def test_complete_snapshot_uses_persisted_receipt_gate(monkeypatch, worker_module, missing):
    from shared import copy_workflow, chat_presentation, archive_capture
    monkeypatch.setattr(copy_workflow, 'has_pending_copy', AsyncMock(return_value=False))
    monkeypatch.setattr(chat_presentation, 'presentation_ready', AsyncMock(return_value=True))
    worker = worker_module.BackupWorker.__new__(worker_module.BackupWorker)
    worker.worker_id = 'archive-finalizer-test'
    worker.flush_version_rows = AsyncMock(return_value=0)
    worker._write_snapshot_terminal = AsyncMock(return_value=False)
    marker = {'version': 1, 'generation': str(uuid4())}
    snapshot = SimpleNamespace(id=uuid4(), job_id=uuid4(), resource_id=uuid4(),
        lease_token=4, status=worker_module.SnapshotStatus.IN_PROGRESS,
        extra_data={}, delta_tokens_json={}, started_at=datetime.utcnow(),
        type=worker_module.SnapshotType.INCREMENTAL)
    live = SimpleNamespace(**vars(snapshot))
    live.extra_data = {'archive_capture': marker}
    job = SimpleNamespace(lease_token=2, status=worker_module.JobStatus.RUNNING)
    async def get(model, id, **kwargs):
        return job if model is worker_module.Job else live
    session = SimpleNamespace(get=get, scalar=AsyncMock(return_value=live.extra_data))
    async def gate(db, current, **kwargs):
        assert current.extra_data['archive_capture'] == marker
        current.delta_tokens_json = {'files_failed': missing}
        return {'files_failed': missing, 'files_succeeded': 1 - missing}
    monkeypatch.setattr(archive_capture, 'archive_final_gate', gate)
    async def terminal(current, proposed): return proposed
    monkeypatch.setattr(worker_module, '_coerce_snapshot_terminal_on_cancel', terminal)
    result = {'item_count': 999, 'new_item_count': 0, 'bytes_added': 0, 'files_failed': 0,
        'archive_snapshot_token': 4, 'archive_job_token': 2}
    assert await worker.complete_snapshot(session, snapshot, result) is False
    assert snapshot.item_count == 1 - missing
    assert snapshot.status == (worker_module.SnapshotStatus.PARTIAL if missing else worker_module.SnapshotStatus.COMPLETED)


async def test_stale_archive_result_cannot_promote_terminal_snapshot(monkeypatch, worker_module):
    from shared import copy_workflow, chat_presentation, archive_capture
    monkeypatch.setattr(copy_workflow, 'has_pending_copy', AsyncMock(return_value=False))
    monkeypatch.setattr(chat_presentation, 'presentation_ready', AsyncMock(return_value=True))
    worker = worker_module.BackupWorker.__new__(worker_module.BackupWorker)
    snap = SimpleNamespace(id=uuid4(), job_id=uuid4(), lease_token=4,
        status=worker_module.SnapshotStatus.PARTIAL, extra_data={'archive_capture': {'version': 1}})
    job = SimpleNamespace(lease_token=2, status=worker_module.JobStatus.RUNNING)
    async def get(model, id, **kwargs): return job if model is worker_module.Job else snap
    with pytest.raises(archive_capture.ArchiveConflict, match='generation'):
        await worker.complete_snapshot(SimpleNamespace(get=get, scalar=AsyncMock(return_value=snap.extra_data)), snap,
            {'archive_snapshot_token': 4, 'archive_job_token': 2})
