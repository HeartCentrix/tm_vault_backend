"""Bounded progress aggregates, without loading copy content or manifests."""
import importlib
import json
import os
from types import SimpleNamespace
import uuid

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

pytestmark = pytest.mark.integration


def observability():
    try:
        return importlib.import_module("shared.copy_observability")
    except ModuleNotFoundError:
        pytest.fail("bounded durable copy-work progress is not implemented")


@pytest_asyncio.fixture
async def progress_db(monkeypatch):
    url = os.getenv("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires explicitly configured disposable PostgreSQL")
    schema = "copy_progress_test_" + uuid.uuid4().hex
    engine = create_async_engine(url, connect_args={
        "server_settings": {"search_path": schema}, "statement_cache_size": 0,
    })
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await conn.execute(text("CREATE TABLE copy_workflows (id uuid PRIMARY KEY, job_id uuid, "
                                "state text, capture_attempt bigint DEFAULT 0)"))
        # No params, source content, owner identifiers or manifest table exist.
        await conn.execute(text("CREATE TABLE copy_work_tasks (id uuid PRIMARY KEY, workflow_id uuid, "
                                "capture_attempt bigint DEFAULT 0, sla_policy_id uuid, state text, "
                                "row_count bigint DEFAULT 0, bytes_count bigint DEFAULT 0, "
                                "cumulative_wait_s float DEFAULT 0, queue_entered_at timestamptz, "
                                "created_at timestamptz DEFAULT NOW(), error json)"))
    factory = async_sessionmaker(engine)
    job, workflow = uuid.uuid4(), uuid.uuid4()
    async with factory.begin() as session:
        await session.execute(text("INSERT INTO copy_workflows(id,job_id,state) VALUES (:w,:j,'WAITING')"),
                              {"w": workflow, "j": job})
    from shared import copy_workflow
    monkeypatch.setenv("COPY_WORK_ENABLED", "false")
    monkeypatch.setattr(copy_workflow, "_COPY_GUARDS_ACTIVE", True)

    async def task(state="READY", *, policy=None, rows=0, size=0, wait=0, age=0, code=None, attempt=0):
        async with factory.begin() as session:
            await session.execute(text("""
                INSERT INTO copy_work_tasks(id,workflow_id,state,sla_policy_id,row_count,bytes_count,
                    cumulative_wait_s,queue_entered_at,error,capture_attempt)
                VALUES (:id,:w,:state,:policy,:rows,:bytes,:wait,
                    NOW()-(:age*INTERVAL '1 second'),CAST(:error AS json),:attempt)
            """), {"id": uuid.uuid4(), "w": workflow, "state": state, "policy": policy,
                   "rows": rows, "bytes": size, "wait": wait, "age": age, "attempt": attempt,
                   "error": json.dumps({"code": code, "message": "https://private/blob?sig=secret"}) if code else None})

    async def state(value):
        async with factory.begin() as session:
            await session.execute(text("UPDATE copy_workflows SET state=:state WHERE id=:w"),
                                  {"state": value, "w": workflow})

    async def summary():
        async with factory() as session:
            return await observability().copy_work_summary(session, job_id=job)

    try:
        yield SimpleNamespace(factory=factory, job=job, task=task, state=state, summary=summary)
    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await engine.dispose()


@pytest.mark.parametrize("task_state,workflow_state,expected", [
    ("READY", "WAITING", "QUEUED"), ("RUNNING", "WAITING", "RUNNING"),
    ("COMPLETED", "WAITING", "WAITING_RESUME"), ("COMPLETED", "RESUMING", "WAITING_RESUME"),
    ("FAILED", "PARTIAL", "FAILED"), ("COMPLETED", "COMPLETED", "COMPLETE"),
])
async def test_progress_exposes_copy_phase(progress_db, task_state, workflow_state, expected):
    await progress_db.task(task_state)
    await progress_db.state(workflow_state)
    assert (await progress_db.summary())["state"] == expected


async def test_admission_wait_and_committed_counts_exclude_sql_runtime_and_old_attempts(progress_db):
    await progress_db.task("READY", rows=11, size=123, wait=7, age=20)
    await progress_db.task("RUNNING", rows=3, size=41, wait=5, age=600)
    await progress_db.task("FAILED", rows=999, size=999, wait=999, age=999, attempt=1)
    summary = await progress_db.summary()
    assert summary["pending_tasks"] == 2
    assert summary["committed_rows"] == 14
    assert summary["committed_bytes"] == 164
    assert 32 <= summary["admission_wait_s"] < 40
    assert 20 <= summary["oldest_queue_age_s"] < 30
    assert summary["sql_runtime_s"] is None


async def test_typed_wait_and_gap_codes_never_echo_arbitrary_error_content(progress_db):
    await progress_db.task("READY", code="copy_tenant_busy")
    await progress_db.task("FAILED", code="copy_admission_timeout")
    await progress_db.task("FAILED", code="https://private/blob?sig=secret")
    summary = await progress_db.summary()
    assert summary["wait_reasons"] == {"copy_tenant_busy": 1}
    assert summary["gap_counts"] == {"copy_admission_timeout": 1, "copy_execution_failed": 1}
    assert "private" not in json.dumps(summary)
    assert "secret" not in json.dumps(summary)


async def test_custom_policy_ids_are_aggregated_and_overflow_is_explicit(progress_db):
    policies = [uuid.uuid4() for _ in range(40)]
    for policy in policies:
        await progress_db.task(policy=policy, rows=1, size=2)
    summary = await progress_db.summary()
    assert summary["policy_count"] == 40
    assert len(summary["policies"]) == 32
    assert {entry["sla_policy_id"] for entry in summary["policies"]}.issubset({str(p) for p in policies})
    assert summary["other_policies"]["policy_count"] == 8
    assert summary["other_policies"]["pending_tasks"] == 8
    assert sum(entry["committed_rows"] for entry in summary["policies"]) + summary["other_policies"]["committed_rows"] == 40


async def test_unknown_job_has_no_copy_summary(progress_db):
    async with progress_db.factory() as session:
        assert await observability().copy_work_summary(session, job_id=uuid.uuid4()) is None


async def test_flag_off_without_observed_work_performs_no_sql(monkeypatch):
    from shared import copy_workflow
    monkeypatch.setenv("COPY_WORK_ENABLED", "false")
    monkeypatch.setattr(copy_workflow, "_COPY_GUARDS_ACTIVE", False)

    class Session:
        async def execute(self, *args, **kwargs):
            pytest.fail("flag-off job reads must not query copy tables")

    assert await observability().copy_work_summary(Session(), job_id=uuid.uuid4()) is None
