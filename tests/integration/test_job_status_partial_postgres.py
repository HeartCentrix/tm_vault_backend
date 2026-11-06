"""A backup job that finished with PARTIAL/FAILED snapshots is PARTIAL, not COMPLETED.

Taylor Morrison log issue #35: 115 jobs read COMPLETED while their result
showed snapshots_partial > 0 (USER_ARCHIVE 81, USER_CHATS 24, ...), because
the jobs table had no PARTIAL status. The rule is the activity feed's "Done
with warnings" (derive_batch_status, which counts every snapshot by job_id):
a job that would finish COMPLETED finishes PARTIAL when any of its snapshots
is PARTIAL or FAILED. PARTIAL is terminal everywhere COMPLETED is.
"""
import importlib.util
import os
import pathlib
import sys
from datetime import datetime
from unittest.mock import AsyncMock
from uuid import uuid4

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

import pytest

from test_chat_catalogue_postgres import store
from test_copy_worker_handoff_postgres import bw, prepare
from shared import models

pytestmark = pytest.mark.integration

_SVC_DIR = pathlib.Path(__file__).resolve().parents[2] / "services" / "audit-service"
if str(_SVC_DIR) not in sys.path:
    sys.path.insert(0, str(_SVC_DIR))
_spec = importlib.util.spec_from_file_location("audit_main_partial_under_test", _SVC_DIR / "main.py")
_as = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _as
_spec.loader.exec_module(_as)


def _worker(bw):
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker.worker_id = "partial-job-status"
    return worker


async def _add_snapshot(db, store, job_id, status, resource_id=None):
    db.add(models.Snapshot(id=uuid4(), resource_id=resource_id or store.resource, job_id=job_id,
                           backend_id=store.backend, status=status, completed_at=datetime.utcnow(),
                           extra_data={}))


@pytest.mark.parametrize("statuses,expected", [
    (("COMPLETED",), "COMPLETED"),
    (("PARTIAL",), "PARTIAL"),
    (("COMPLETED", "FAILED"), "PARTIAL"),
])
async def test_single_job_completion_follows_the_activity_feed_warning_rule(
        store, monkeypatch, bw, statuses, expected):
    scope, _ = await prepare(store, monkeypatch, bw)
    async with store.factory.begin() as db:
        head = await db.get(models.Snapshot, scope.snapshot_id)
        head.status = models.SnapshotStatus[statuses[0]]
        for status in statuses[1:]:
            await _add_snapshot(db, store, head.job_id, models.SnapshotStatus[status])
        job_id = head.job_id
    async with store.factory() as db:
        await _worker(bw).update_job_status(db, job_id, models.JobStatus.COMPLETED, {})
    async with store.factory() as db:
        job = await db.get(models.Job, job_id)
    assert job.status.value == expected
    assert job.completed_at is not None and job.progress_pct == 100


async def test_bulk_parent_with_a_partial_child_flips_partial_once(store, monkeypatch, bw):
    scope, _ = await prepare(store, monkeypatch, bw)
    second = uuid4()
    async with store.factory.begin() as db:
        head = await db.get(models.Snapshot, scope.snapshot_id)
        head.status, head.completed_at = models.SnapshotStatus.COMPLETED, datetime.utcnow()
        job = await db.get(models.Job, head.job_id)
        job.batch_resource_ids = [str(store.resource), str(second)]
        db.add(models.Resource(id=second, tenant_id=store.tenant, type=models.ResourceType.USER_CHATS,
                               external_id="second-partial", display_name="Second", extra_data={}))
        await db.flush()
        await _add_snapshot(db, store, job.id, models.SnapshotStatus.PARTIAL, second)
        job_id = job.id
    worker = _worker(bw)
    worker._stamp_unsettled_bulk_job_resources_failed = AsyncMock(return_value=0)
    async with store.factory() as db:
        assert await worker._finalize_bulk_parent_if_complete(db, job_id) == "PARTIAL"
        # PARTIAL is terminal: a late sibling finalizer must not re-flip it.
        assert await worker._finalize_bulk_parent_if_complete(db, job_id) is None
    worker._stamp_unsettled_bulk_job_resources_failed.assert_awaited_once()
    async with store.factory() as db:
        job = await db.get(models.Job, job_id)
    assert job.status == models.JobStatus.PARTIAL and job.result["snapshots_partial"] == 1


async def test_reconciler_orphan_job_with_partial_snapshot_finishes_partial(store):
    from sqlalchemy import text
    from shared.reconciler import sweep_orphans
    scope = await store.new_scope()
    async with store.factory.begin() as db:
        # Raw-SQL table outside the ORM metadata (same DDL as test_copy_guard_wiring_postgres).
        await db.execute(text("CREATE TABLE worker_heartbeats (worker_id uuid, last_seen_at timestamp)"))
        head = await db.get(models.Snapshot, scope.snapshot_id)
        head.status, head.completed_at = models.SnapshotStatus.PARTIAL, datetime.utcnow()
        job_id = head.job_id
    async with store.factory() as db:
        assert (await sweep_orphans(db)).jobs_finalized == 1
    async with store.factory() as db:
        assert (await db.get(models.Job, job_id)).status == models.JobStatus.PARTIAL


async def _partial_batch(store, *, unrun_mail_child=False):
    """One PARTIAL job: ENTRA_USER parent + a chats child that ran PARTIAL."""
    scope = await store.new_scope()
    batch_id, parent = str(uuid4()), uuid4()
    async with store.factory.begin() as db:
        db.add(models.Resource(id=parent, tenant_id=store.tenant, type=models.ResourceType.ENTRA_USER,
                               external_id="aad-partial", display_name="Partial user"))
        await db.flush()
        (await db.get(models.Resource, store.resource)).parent_resource_id = parent
        if unrun_mail_child:
            db.add(models.Resource(id=uuid4(), tenant_id=store.tenant, type=models.ResourceType.USER_MAIL,
                                   external_id="aad-partial", display_name="Mail (not in this run)",
                                   parent_resource_id=parent))
        head = await db.get(models.Snapshot, scope.snapshot_id)
        head.status, head.completed_at = models.SnapshotStatus.PARTIAL, datetime.utcnow()
        job = await db.get(models.Job, head.job_id)
        job.status, job.completed_at = models.JobStatus.PARTIAL, datetime.utcnow()
        job.spec = {"batch_id": batch_id}
        job.batch_resource_ids = [str(parent), str(store.resource)]
    return batch_id


async def test_activity_row_for_a_partial_job_is_done_with_warnings_and_filterable(store, monkeypatch):
    await _partial_batch(store)
    monkeypatch.setattr(_as, "async_session_factory", store.factory)

    async def rows(status):
        async with store.factory() as db:
            return await _as._batch_backup_activity_rows(
                db, tenant_id=str(store.tenant), start_date=None, end_date=None, limit=50, status=status)

    [row] = await rows(None)
    assert row["status"] == "Done"
    assert row["warnings"]["partial"] == 1
    assert [r["id"] for r in await rows("Partial")] == [row["id"]]
    assert await rows("Failed") == []


async def test_batch_drilldown_treats_a_partial_job_as_terminal(store, monkeypatch):
    batch_id = await _partial_batch(store, unrun_mail_child=True)
    monkeypatch.setattr(_as, "async_session_factory", store.factory)
    body = await _as.get_batch_children(batch_id)
    children = [c["type"] for r in body["resources"] for c in (r.get("children") or [])]
    # Terminal batch: the never-run Mail child is hidden, the chats child stays.
    assert children == ["USER_CHATS"]
