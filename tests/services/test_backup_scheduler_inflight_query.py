"""Regression: the SLA concurrency-cap inflight query must compile against
Job.spec, which is a plain JSON column (not JSONB).

Bug (commit 0ba1566): main.py used Job.spec["sla_policy_id"].astext, but
.astext is a JSONB-only accessor. On the JSON column it raises AttributeError
at dispatch time, crashing every Gold (THREE_DAILY, max_concurrent_backups>0)
dispatch before any backup is queued. Fix: use .as_string() (generic-JSON ->>).
"""
import uuid

import pytest
from sqlalchemy import JSON, func, select
from sqlalchemy.dialects import postgresql

from shared.models import Job, JobStatus


def test_job_spec_is_plain_json():
    # Guards the premise: if spec ever becomes JSONB, .astext would be valid.
    assert isinstance(Job.__table__.c.spec.type, JSON)


def test_astext_is_unavailable_on_json_spec():
    # Reproduces the original crash.
    with pytest.raises(AttributeError):
        _ = Job.spec["sla_policy_id"].astext


def test_inflight_query_compiles_with_as_string():
    pid = str(uuid.uuid4())
    stmt = select(func.count(Job.id)).where(
        Job.spec["sla_policy_id"].as_string() == pid,
        Job.status.in_([JobStatus.QUEUED, JobStatus.RUNNING]),
    )
    sql = str(stmt.compile(dialect=postgresql.dialect(),
                           compile_kwargs={"literal_binds": False}))
    assert "->>" in sql  # text extraction, comparable to the UUID string
