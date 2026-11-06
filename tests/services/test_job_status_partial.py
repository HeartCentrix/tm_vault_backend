"""PARTIAL is a terminal job status everywhere COMPLETED is (TM log issue #35).

Behavioural checks for the pure helpers; source pins for the scheduler's SQL
reapers, which live in closures over live sessions (same convention as
test_scheduler_activity_finalize). The Postgres-backed roll-up is covered by
tests/integration/test_job_status_partial_postgres.py.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import re
import sys
from contextlib import asynccontextmanager
from types import SimpleNamespace

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.models import JobStatus  # noqa: E402


def _load(service: str, name: str):
    path = ROOT / "services" / service
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
    spec = importlib.util.spec_from_file_location(name, path / "main.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _src(rel: str) -> str:
    return (ROOT / rel).read_text()


def _sql_after(src: str, anchor: str, end: str = "RETURNING") -> str:
    i = src.index(anchor)
    return src[i:src.index(end, i)]


# ── behaviour ────────────────────────────────────────────────────────────


def test_dashboard_counts_a_partial_batch_as_warnings_not_success():
    dash = _load("dashboard-service", "dashboard_main_partial_status")
    assert dash._roll_up_group_outcome([(JobStatus.PARTIAL, {})]) == "warnings"
    assert dash._roll_up_group_outcome([(JobStatus.COMPLETED, {}), (JobStatus.PARTIAL, {})]) == "warnings"
    assert dash._roll_up_group_outcome([(JobStatus.COMPLETED, {})]) == "success"


def test_job_progress_pegs_a_partial_job_to_100():
    jobs = _load("job-service", "job_service_partial_status")
    job = SimpleNamespace(status=JobStatus.PARTIAL, progress_pct=40)
    assert jobs._project_job_progress(job, {"progress": 40, "snapshots_total": 2}) == 100


async def test_late_running_progress_update_cannot_resurrect_a_partial_job(monkeypatch):
    tracker = _load("progress-tracker", "progress_tracker_partial_status")
    job = SimpleNamespace(status=JobStatus.PARTIAL, progress_pct=100, items_processed=0, bytes_processed=0)

    async def commit():
        return None

    @asynccontextmanager
    async def factory():
        async def get(_model, _id):
            return job
        yield SimpleNamespace(get=get, commit=commit)

    monkeypatch.setattr(tracker, "async_session_factory", factory)
    await tracker.update_progress({"resource_id": "r", "job_id": "00000000-0000-0000-0000-000000000001",
                                   "status": "RUNNING", "progress_pct": 50})
    assert job.status == JobStatus.PARTIAL


# ── source pins: every finalizer and terminal set ─────────────────────────


def test_scheduler_bulk_backstop_flips_partial_and_treats_it_terminal():
    sql = _sql_after(_src("services/backup-scheduler/main.py"), "WITH bulk_jobs AS")
    assert sql.count("'PARTIAL'::jobstatus") >= 3, "both NOT IN guards + the CASE branch"
    assert re.search(r"WHEN c\.partial > 0 OR c\.failed > 0\s+THEN 'PARTIAL'::jobstatus", sql)


def test_scheduler_late_bytes_reconcile_includes_partial_jobs():
    sql = _sql_after(_src("services/backup-scheduler/main.py"), "WITH recent_bulks AS")
    assert "'PARTIAL'::jobstatus" in sql


def test_scheduler_reapers_finalize_partial():
    main = _src("services/backup-scheduler/main.py")

    def between(start, end):
        i = main.index(start)
        return main[i:main.index(end, i)]

    stuck = between("_reap_stuck_running_jobs():", "'reaped_by', 'stuck_running_reaper'")
    assert "job_partial_sql('j.id')" in stuck and "THEN 'PARTIAL'" in stuck
    missing = between("_reap_missing_snapshot_jobs():", "'reaped_by', 'missing_snapshot_reaper'")
    assert re.search(r"WHEN t\.n_completed > 0\s+THEN 'PARTIAL'", missing)
    single = between("_reap_settled_single_resource_jobs():", "'reaped_by', 'single_resource_job_reaper'")
    assert "n_warn" in single and "THEN 'PARTIAL'" in single


def test_report_counts_partial_backups_as_finished():
    src = _src("services/report-service/main.py")
    assert src.count("Job.status.in_([JobStatus.COMPLETED, JobStatus.PARTIAL])") == 2


def test_migration_adds_partial_outside_a_transaction():
    # revision ids are pinned by tests/shared/test_alembic_revision_chain.py
    src = _src("alembic/versions/0019_job_status_partial.py")
    assert "autocommit_block()" in src
    assert "ALTER TYPE jobstatus ADD VALUE IF NOT EXISTS 'PARTIAL'" in src
    assert "ALTER TYPE jobstatus ADD VALUE IF NOT EXISTS 'PARTIAL';" in _src("shared/database.py")
