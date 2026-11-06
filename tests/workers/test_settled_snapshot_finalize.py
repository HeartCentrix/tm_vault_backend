"""A snapshot the handler already settled is not finalized a second time.

Railway 2026-11-06 02:56 (USER_CALENDAR, worker-f2fa3569): the handler's inline
settle landed COMPLETED and stamped the resource, then the generic success path
recomputed the delta, re-ran the terminal write (which can never change a
COMPLETED/PARTIAL row: "already COMPLETED (prior attempt landed)") and re-stamped
storage_bytes. ~10 s of a 30 s slot at 62 ms per round trip.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import uuid
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
_spec = importlib.util.spec_from_file_location(
    "bw_settled_finalize", _REPO_ROOT / "workers" / "backup-worker" / "main.py")
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_settled_finalize"] = _bw
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(f"backup-worker module failed to import: {exc}", allow_module_level=True)

S = _bw.SnapshotStatus


def _snapshot():
    return SimpleNamespace(
        id=uuid.uuid4(), resource_id=uuid.uuid4(), job_id=None, status=S.IN_PROGRESS,
        type=_bw.SnapshotType.INCREMENTAL, started_at=datetime.utcnow() - timedelta(seconds=30),
        completed_at=None, duration_secs=None, item_count=0, new_item_count=0, bytes_added=0,
        bytes_total=0, delta_token=None, delta_tokens_json={}, extra_data={})


def _row(status):
    return SimpleNamespace(
        status=status, type=_bw.SnapshotType.INCREMENTAL, item_count=40, new_item_count=3,
        bytes_total=4096, bytes_added=300, completed_at=datetime.utcnow(), duration_secs=9,
        extra_data={"settled_by": "handler"})


class _Session:
    def __init__(self, resource=None, snapshot=None):
        self.objects = {_bw.Resource: resource, _bw.Snapshot: snapshot}

    async def scalar(self, statement):
        return None

    async def get(self, model, _pk, **_kwargs):
        return self.objects.get(model)

    async def commit(self):
        return None


def _worker(calls, monkeypatch):
    w = _bw.BackupWorker.__new__(_bw.BackupWorker)
    w.worker_id = "test-worker"

    async def flush(force=False):
        return 0

    async def terminal(session, snap, allowed_from):
        calls.append("terminal_write")
        return True

    async def delta(*args, **kwargs):
        calls.append("delta_recompute")
        return 0, 0

    async def anomaly(snap):
        calls.append(("anomaly", snap.status, snap.item_count))

    async def cancelled(job_id):
        calls.append("cancel_query")
        return False

    w.flush_version_rows = flush
    w._write_snapshot_terminal = terminal
    w._compute_snapshot_delta_from_prior = delta
    w._check_snapshot_anomaly = anomaly
    monkeypatch.setattr(_bw, "_is_job_cancelled", cancelled)
    monkeypatch.setattr(_bw, "lease_unregister", lambda table, key: calls.append(("unregister", table, key)))
    return w


@pytest.mark.parametrize("status", [S.COMPLETED, S.PARTIAL])
def test_handler_settled_snapshot_skips_the_second_terminal_write(monkeypatch, status):
    calls = []
    w = _worker(calls, monkeypatch)
    snap = _snapshot()
    snap.job_id = uuid.uuid4()
    asyncio.run(w.complete_snapshot(_Session(), snap, {"item_count": 1, "bytes_added": 1},
                                    settled_row=_row(status)))
    assert "terminal_write" not in calls and "delta_recompute" not in calls and "cancel_query" not in calls
    # Metrics and the anomaly tripwire report what actually landed.
    assert (snap.status, snap.item_count, snap.bytes_added, snap.extra_data) == (
        status, 40, 300, {"settled_by": "handler"})
    assert (("anomaly", S.COMPLETED, 40) in calls) == (status == S.COMPLETED)
    # The terminal write's lease release still happens exactly once.
    assert calls.count(("unregister", "snapshots", snap.id)) == 1


@pytest.mark.parametrize("status,result", [
    (S.FAILED, {"item_count": 1, "bytes_added": 1}),          # sweep FAILED: recovery write must run
    (S.IN_PROGRESS, {"item_count": 1, "bytes_added": 1}),     # handler did not settle
    (S.COMPLETED, {"item_count": 1, "bytes_added": 1, "new_delta_token": "t"}),  # token still persists
])
def test_unsettled_or_token_carrying_runs_keep_the_terminal_write(monkeypatch, status, result):
    calls = []
    w = _worker(calls, monkeypatch)
    asyncio.run(w.complete_snapshot(_Session(), _snapshot(), dict(result), settled_row=_row(status)))
    assert "terminal_write" in calls


def _stamp(monkeypatch, *, stamped_job, stamped_status, row_status=S.COMPLETED):
    job_id = uuid.uuid4()
    resource = SimpleNamespace(id=uuid.uuid4(), status=None, storage_bytes=0,
        last_backup_job_id=job_id if stamped_job else uuid.uuid4(), last_backup_status=stamped_status)
    session = _Session(resource=resource, snapshot=SimpleNamespace(status=row_status, extra_data={}))
    calls = []
    w = _worker(calls, monkeypatch)

    async def update(session, resource, job_id, snapshot_id, result=None, **kwargs):
        calls.append(("stamp", kwargs.get("status")))

    w.update_resource_backup_info = update
    asyncio.run(w._stamp_resource_from_snapshot_row(session, resource, job_id, uuid.uuid4(), {},
                                                     skip_if_stamped=True))
    return calls


def test_resource_already_stamped_for_this_job_and_outcome_is_not_restamped(monkeypatch):
    assert _stamp(monkeypatch, stamped_job=True, stamped_status="COMPLETED") == []


@pytest.mark.parametrize("stamped_job,stamped_status", [(False, "COMPLETED"), (True, "PARTIAL"), (True, None)])
def test_resource_stamp_runs_when_this_outcome_is_not_recorded(monkeypatch, stamped_job, stamped_status):
    assert _stamp(monkeypatch, stamped_job=stamped_job, stamped_status=stamped_status) == [("stamp", "COMPLETED")]
