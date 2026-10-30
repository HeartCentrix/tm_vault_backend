"""Regression: cancelling a RESTORE must not corrupt backup bookkeeping.

Bug (found 2026-08-04 review): cancel_job ran
``UPDATE resources SET last_backup_status='CANCELLED',
last_backup_job_id=:jid`` for every resource on the job with no job.type
check. For a RESTORE job, job.resource_id is the SOURCE resource whose
latest backup is intact — cancelling the restore overwrote its backup
status with CANCELLED and pointed last_backup_job_id at a restore job.
report-service counts protection by ``last_backup_status == 'COMPLETED'``,
so the resource silently dropped out of the compliance report.

Contract pinned here:
  - Cancelling a RESTORE job never touches resources.last_backup_status.
  - Cancelling a BACKUP job still stamps it (the column is honest there).
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import uuid
from types import SimpleNamespace

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

import pytest

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_SVC_DIR = _REPO_ROOT / "services" / "job-service"
if str(_SVC_DIR) not in sys.path:
    sys.path.insert(0, str(_SVC_DIR))

_spec = importlib.util.spec_from_file_location(
    "js_main_cancel_under_test", _SVC_DIR / "main.py"
)
_js = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["js_main_cancel_under_test"] = _js
assert _spec and _spec.loader
_spec.loader.exec_module(_js)  # type: ignore[union-attr]

from shared.models import Job, JobStatus, JobType  # noqa: E402


class _Result:
    def __init__(self, rows=()):
        self._rows = list(rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return list(self._rows)

    def scalars(self):
        return self

    def scalar_one_or_none(self):
        return self._rows[0] if self._rows else None


class _FakeSession:
    def __init__(self, job):
        self.job = job
        self.calls = []

    async def execute(self, stmt, params=None):
        if hasattr(stmt, "column_descriptions"):
            return _Result([self.job])
        sql = str(stmt)
        self.calls.append((sql, params or {}))
        if "FROM snapshots s" in sql and "count(s.id)" in sql:
            return _Result([SimpleNamespace(n_snaps=0, n_items=0, n_bytes=0)])
        return _Result([])

    async def commit(self):
        pass

    async def flush(self):
        pass

    async def rollback(self):
        pass

    def sqls(self, fragment):
        return [(s, p) for s, p in self.calls if fragment in s]


def _job(jtype):
    return Job(
        id=uuid.uuid4(),
        type=jtype,
        tenant_id=uuid.uuid4(),
        resource_id=uuid.uuid4(),
        status=JobStatus.RUNNING,
        spec={},
    )


def _cancel(job):
    sess = _FakeSession(job)
    asyncio.run(_js.cancel_job(str(job.id), sess))
    return sess


def test_cancel_restore_leaves_backup_bookkeeping_alone():
    job = _job(JobType.RESTORE)
    sess = _cancel(job)
    assert sess.sqls("SET last_backup_status") == [], (
        "cancelling a restore must not stamp CANCELLED on the intact "
        "source resource's backup status"
    )
    assert job.status == JobStatus.CANCELLED


def test_cancel_backup_still_stamps_cancelled():
    job = _job(JobType.BACKUP)
    sess = _cancel(job)
    stamped = sess.sqls("SET last_backup_status = 'CANCELLED'")
    assert len(stamped) == 1
    assert stamped[0][1]["jid"] == job.id
    assert job.status == JobStatus.CANCELLED
