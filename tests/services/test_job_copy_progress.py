"""Copy progress extends existing job serialization and authentication."""
import importlib.util
import os
import pathlib
import sys
import uuid
from datetime import datetime

from fastapi.testclient import TestClient
import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

ROOT = pathlib.Path(__file__).resolve().parents[2]
SERVICE = ROOT / "services" / "job-service"
sys.path.insert(0, str(SERVICE))
spec = importlib.util.spec_from_file_location("job_copy_progress_under_test", SERVICE / "main.py")
service = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = service
spec.loader.exec_module(service)

from shared.database import get_db
from shared.models import Job, JobStatus, JobType


class Rows:
    def __init__(self, rows=()):
        self.rows = list(rows)

    def scalar_one_or_none(self):
        return self.rows[0] if self.rows else None

    def scalars(self):
        return self

    def all(self):
        return self.rows

    fetchall = all

    def scalar(self):
        return self.rows[0] if self.rows else 0


class Session:
    def __init__(self, jobs):
        self.jobs = jobs
        self.sql = []

    async def execute(self, statement, params=None):
        sql = str(statement)
        self.sql.append(sql)
        assert "copy_work" not in sql, "flag-off job read queried durable copy tables"
        if "count(jobs.id)" in sql:
            return Rows([len(self.jobs)])
        if "FROM jobs" in sql:
            return Rows(self.jobs)
        return Rows()


@pytest.fixture
def job():
    return Job(id=uuid.uuid4(), type=JobType.BACKUP, status=JobStatus.RUNNING,
               resource_id=uuid.uuid4(), tenant_id=uuid.uuid4(), progress_pct=5,
               created_at=datetime.utcnow(), updated_at=datetime.utcnow(),
               result={"download_url": "https://private/blob?sig=secret"})


@pytest.fixture(autouse=True)
def clear_dependencies():
    yield
    service.app.dependency_overrides.clear()


def client(session, *, authenticated=True):
    service.app.dependency_overrides[get_db] = lambda: session
    if authenticated:
        service.app.dependency_overrides[service.require_principal] = lambda: {"id": "authenticated-user"}
    return TestClient(service.app)


def test_job_response_exposes_only_safe_copy_summary_without_mutating_result(monkeypatch, job):
    summary = {"state": "QUEUED", "pending_tasks": 2, "committed_rows": 10}

    async def copy_summary(session, *, job_id):
        assert job_id == job.id
        return summary

    monkeypatch.setattr(service, "copy_work_summary", copy_summary, raising=False)
    session = Session([job])
    result_before = dict(job.result)
    response = client(session).get(f"/api/v1/jobs/{job.id}")
    assert response.status_code == 200
    assert response.json()["copy_work"] == summary
    assert "private" not in response.text
    assert "secret" not in response.text
    assert job.result == result_before


def test_job_copy_summary_remains_authenticated(monkeypatch, job):
    async def copy_summary(*args, **kwargs):
        pytest.fail("anonymous callers must not reach copy progress")

    monkeypatch.setattr(service, "copy_work_summary", copy_summary, raising=False)
    response = client(Session([job]), authenticated=False).get(f"/api/v1/jobs/{job.id}")
    assert response.status_code == 401


def test_flag_off_preserves_job_fields_without_copy_sql(monkeypatch, job):
    from shared import copy_workflow
    monkeypatch.setenv("COPY_WORK_ENABLED", "false")
    monkeypatch.setattr(copy_workflow, "_COPY_GUARDS_ACTIVE", False)
    session = Session([job])
    response = client(session).get(f"/api/v1/jobs/{job.id}")
    assert response.status_code == 200
    assert response.json()["id"] == str(job.id)
    assert response.json()["progress"] == 5
    assert response.json().get("copy_work") is None
    assert len(session.sql) == 2


def test_job_page_collects_copy_progress_in_one_bulk_query(monkeypatch, job):
    calls = []

    async def copy_summaries(session, *, job_ids):
        calls.append(job_ids)
        return {str(job.id): {"state": "WAITING_RESUME", "pending_tasks": 0}}

    monkeypatch.setattr(service, "copy_work_summaries", copy_summaries, raising=False)
    response = client(Session([job])).get("/api/v1/jobs?size=2")
    assert response.status_code == 200
    assert response.json()["content"][0]["copy_work"]["state"] == "WAITING_RESUME"
    assert calls == [[job.id]]
