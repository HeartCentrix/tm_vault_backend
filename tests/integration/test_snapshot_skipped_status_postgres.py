"""TM log issue #1: the cross-job concurrency placeholder (a job that found its
resource already being backed up by another job) is SKIPPED, not a 0-item
COMPLETED. SKIPPED is terminal and never a restore point, and a job whose only
snapshots are SKIPPED still ends COMPLETED, as it did when they were COMPLETED."""
import importlib.util
import os
import pathlib
from datetime import datetime
from unittest.mock import AsyncMock
from uuid import uuid4

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

import pytest
from sqlalchemy import text

from test_chat_catalogue_postgres import store
from test_copy_worker_handoff_postgres import bw, prepare
from shared import models

pytestmark = pytest.mark.integration

ROOT = pathlib.Path(__file__).resolve().parents[2]
SKIPPED = models.SnapshotStatus.SKIPPED
_FLAG = {"skipped_concurrent": True}


def _migration():
    [path] = (ROOT / "alembic" / "versions").glob("*_snapshot_status_skipped.py")
    spec = importlib.util.spec_from_file_location("migration_skipped_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _worker(bw):
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker.worker_id = "skipped-snapshot-status"
    return worker


def test_skipped_is_terminal():
    assert SKIPPED in models.SnapshotStatus.terminal()


def test_worker_writes_the_concurrency_placeholder_as_skipped():
    src = (ROOT / "workers/backup-worker/main.py").read_text()
    branch = src[src.index("if other_active is not None:"):src.index("# ── Step 2: network work without a pinned session")]
    assert "_live.status = SnapshotStatus.SKIPPED" in branch
    assert "SnapshotStatus.COMPLETED" not in branch


async def test_single_job_whose_only_snapshot_is_skipped_completes(store, monkeypatch, bw):
    scope, _ = await prepare(store, monkeypatch, bw)
    async with store.factory.begin() as db:
        head = await db.get(models.Snapshot, scope.snapshot_id)
        head.status, head.extra_data, head.completed_at = SKIPPED, _FLAG, datetime.utcnow()
        job_id = head.job_id
    async with store.factory() as db:
        await _worker(bw).update_job_status(db, job_id, models.JobStatus.COMPLETED, {})
    async with store.factory() as db:
        assert (await db.get(models.Job, job_id)).status == models.JobStatus.COMPLETED


@pytest.mark.parametrize("head_status", ["COMPLETED", "SKIPPED"])
async def test_bulk_parent_with_skipped_children_completes(store, monkeypatch, bw, head_status):
    scope, _ = await prepare(store, monkeypatch, bw)
    second = uuid4()
    async with store.factory.begin() as db:
        head = await db.get(models.Snapshot, scope.snapshot_id)
        head.status, head.completed_at = models.SnapshotStatus[head_status], datetime.utcnow()
        job = await db.get(models.Job, head.job_id)
        job.batch_resource_ids = [str(store.resource), str(second)]
        db.add(models.Resource(id=second, tenant_id=store.tenant, type=models.ResourceType.USER_CHATS,
                               external_id="second-skipped", display_name="Second", extra_data={}))
        await db.flush()
        db.add(models.Snapshot(id=uuid4(), resource_id=second, job_id=job.id, backend_id=store.backend,
                               status=SKIPPED, completed_at=datetime.utcnow(), extra_data=_FLAG))
        job_id = job.id
    worker = _worker(bw)
    worker._stamp_unsettled_bulk_job_resources_failed = AsyncMock(return_value=0)
    async with store.factory() as db:
        assert await worker._finalize_bulk_parent_if_complete(db, job_id) == "COMPLETED"


async def test_reconciler_orphan_job_with_only_skipped_snapshot_completes(store):
    from shared.reconciler import sweep_orphans
    scope = await store.new_scope()
    async with store.factory.begin() as db:
        await db.execute(text("CREATE TABLE worker_heartbeats (worker_id uuid, last_seen_at timestamp)"))
        head = await db.get(models.Snapshot, scope.snapshot_id)
        head.status, head.extra_data, head.completed_at = SKIPPED, _FLAG, datetime.utcnow()
        job_id = head.job_id
    async with store.factory() as db:
        assert (await sweep_orphans(db)).jobs_finalized == 1
    async with store.factory() as db:
        assert (await db.get(models.Job, job_id)).status == models.JobStatus.COMPLETED


async def test_migration_backfills_flagged_placeholders_and_downgrade_reverts(store):
    mig = _migration()
    assert mig.down_revision in ("0020", "0021")
    scope = await store.new_scope()
    ids = {name: uuid4() for name in ("placeholder", "real", "flag_false")}
    async with store.factory.begin() as db:
        job_id = (await db.get(models.Snapshot, scope.snapshot_id)).job_id
        for name, sid in ids.items():
            extra = {"placeholder": _FLAG, "flag_false": {"skipped_concurrent": False}}.get(name, {})
            db.add(models.Snapshot(id=sid, resource_id=scope.resource_id, job_id=job_id,
                                   backend_id=scope.backend_id, status=models.SnapshotStatus.COMPLETED,
                                   extra_data=extra))

    async def statuses():
        async with store.factory() as db:
            rows = (await db.execute(text("SELECT id, status::text FROM snapshots WHERE id = ANY(:ids)"),
                                     {"ids": list(ids.values())})).all()
        return {name: dict(rows)[sid] for name, sid in ids.items()}

    async with store.factory.begin() as db:
        await db.execute(text(mig.BACKFILL_SQL))
    assert await statuses() == {"placeholder": "SKIPPED", "real": "COMPLETED", "flag_false": "COMPLETED"}
    async with store.factory.begin() as db:
        await db.execute(text(mig.DOWNGRADE_SQL))
    assert await statuses() == {"placeholder": "COMPLETED", "real": "COMPLETED", "flag_false": "COMPLETED"}


def test_railway_sql_adds_the_value_before_the_backfill():
    [sql_file] = (ROOT / "scripts").glob("*snapshot_status_skipped.sql")
    sql = sql_file.read_text()
    mig = _migration()
    assert sql.index("ADD VALUE IF NOT EXISTS 'SKIPPED'") < sql.index(mig.BACKFILL_SQL.strip())


def test_retention_may_prune_a_skipped_placeholder():
    from types import SimpleNamespace
    from shared.retention_cleanup import _is_protected_in_flight
    placeholder = SimpleNamespace(status=SKIPPED, extra_data=_FLAG, item_count=0,
                                  hc_drain_status="NOT_APPLICABLE", lease_expires_at=None)
    assert not _is_protected_in_flight(placeholder)


def test_init_db_self_heals_the_enum_value():
    # Services that boot through init_db (no alembic) must still be able to write SKIPPED.
    assert "ALTER TYPE snapshotstatus ADD VALUE IF NOT EXISTS 'SKIPPED';" in (ROOT / "shared/database.py").read_text()


def _scheduler_finalizer_sql(main):
    spans = [("WITH bulk_jobs AS", "RETURNING j.id"),
             ("_reap_stuck_running_jobs():", "'reaped_by', 'stuck_running_reaper'"),
             ("_reap_missing_snapshot_jobs():", "'reaped_by', 'missing_snapshot_reaper'"),
             ("_reap_settled_single_resource_jobs():", "'reaped_by', 'single_resource_job_reaper'")]
    for start, end in spans:
        i = main.index(start)
        yield start, main[i:main.index(end, i)]


def test_scheduler_finalizers_treat_skipped_as_terminal_and_done():
    # Each counts SKIPPED once as settled and once as done; otherwise a SKIPPED-only
    # job sits RUNNING (not terminal) or lands FAILED (no COMPLETED child).
    main = (ROOT / "services/backup-scheduler/main.py").read_text()
    for start, sql in _scheduler_finalizer_sql(main):
        assert sql.count("'SKIPPED'") == 2, start
