"""A failed Tier-2 drain must not reach the outer success finalizer.

The October export contains six empty COMPLETED USER_MAIL snapshots with
an enumeration failure reason. The nested handler persisted FAILED; the
single-resource wrapper returned clean counters and the outer handler
promoted the failed row using its stale snapshot instance.
"""
from contextlib import asynccontextmanager
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
import sys
import uuid

import pytest


@pytest.fixture(scope="module")
def bw():
    path = Path(__file__).resolve().parents[2] / "workers/backup-worker/main.py"
    spec = importlib.util.spec_from_file_location("bw_single_failure", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("status", ["FAILED", "PARTIAL", "COMPLETED", "IN_PROGRESS"])
async def test_single_content_wrapper_preserves_inner_outcome(bw, monkeypatch, status):
    snapshot_id, resource_id, job_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    reason = "enumeration failed: USER_MAIL: every folder failed; zero items persisted"
    stored = SimpleNamespace(
        id=snapshot_id, status=bw.SnapshotStatus[status],
        extra_data={"failure_reason": reason, "degraded_reason": reason},
        item_count=0, new_item_count=0, bytes_added=0, bytes_total=0,
    )
    session = SimpleNamespace(get=AsyncMock(return_value=stored), commit=AsyncMock())

    @asynccontextmanager
    async def factory():
        yield session

    monkeypatch.setattr(bw, "async_session_factory", factory)
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker.worker_id = "regression"
    worker._backup_user_content_parallel = AsyncMock(
        return_value={"item_count": 0, "bytes_added": 0}
    )
    call = worker._backup_user_content_single(
        object(), SimpleNamespace(id=resource_id),
        SimpleNamespace(id=snapshot_id, status=bw.SnapshotStatus.IN_PROGRESS),
        object(), {"jobId": str(job_id)},
    )
    if status == "FAILED":
        with pytest.raises(bw.EnumerationFailed, match="every folder failed"):
            await call
        assert stored.status == bw.SnapshotStatus.FAILED
        assert stored.extra_data["failure_reason"] == reason
    else:
        assert await call == {"item_count": 0, "bytes_added": 0}
        assert stored.status == bw.SnapshotStatus[status]
