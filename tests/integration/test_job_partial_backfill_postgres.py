"""Migration 0019 relabels finished backups that left PARTIAL/FAILED snapshots as PARTIAL
(TM #35: 115 jobs read COMPLETED with snapshots_partial > 0), and downgrade puts them back."""
import importlib.util
import os
import pathlib
from uuid import uuid4

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

import pytest
from sqlalchemy import text

from test_chat_catalogue_postgres import store
from shared import models

pytestmark = pytest.mark.integration

_spec = importlib.util.spec_from_file_location(
    "migration_0019_under_test",
    pathlib.Path(__file__).resolve().parents[2] / "alembic" / "versions" / "0019_job_status_partial.py")
_mig = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mig)


async def test_backfill_relabels_only_completed_jobs_with_partial_or_failed_snapshots(store):
    scope = await store.new_scope()
    jobs = {name: uuid4() for name in ("partial_snap", "failed_snap", "clean", "failed_job")}
    async with store.factory.begin() as session:
        tenant_id = await session.scalar(text("SELECT tenant_id FROM resources WHERE id=:id"), {"id": scope.resource_id})
        for name, jid in jobs.items():
            status = models.JobStatus.FAILED if name == "failed_job" else models.JobStatus.COMPLETED
            session.add(models.Job(id=jid, type=models.JobType.BACKUP, tenant_id=tenant_id,
                                   resource_id=scope.resource_id, status=status))
            await session.flush()
            snap = {"partial_snap": models.SnapshotStatus.PARTIAL, "failed_snap": models.SnapshotStatus.FAILED,
                    "failed_job": models.SnapshotStatus.PARTIAL}.get(name, models.SnapshotStatus.COMPLETED)
            session.add(models.Snapshot(resource_id=scope.resource_id, job_id=jid, backend_id=scope.backend_id,
                                        status=snap, type=models.SnapshotType.INCREMENTAL))

    async def statuses():
        async with store.factory() as session:
            rows = (await session.execute(text("SELECT id, status::text FROM jobs WHERE id = ANY(:ids)"),
                                          {"ids": list(jobs.values())})).all()
        by_id = dict(rows)
        return {name: by_id[jid] for name, jid in jobs.items()}

    async with store.factory.begin() as session:
        await session.execute(text(_mig.BACKFILL_SQL))
    assert await statuses() == {"partial_snap": "PARTIAL", "failed_snap": "PARTIAL", "clean": "COMPLETED",
                                "failed_job": "FAILED"}
    async with store.factory.begin() as session:
        await session.execute(text(_mig.DOWNGRADE_SQL))
    assert await statuses() == {"partial_snap": "COMPLETED", "failed_snap": "COMPLETED", "clean": "COMPLETED",
                                "failed_job": "FAILED"}
