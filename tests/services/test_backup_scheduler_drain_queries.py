"""SQL contracts for the deferred-resource drain.

Compiled under the postgresql dialect (no DB). A single NULL inside a
NOT IN subquery makes PostgreSQL return zero rows, which would silently
switch the drain off — so the carried-ids exclusion MUST be NOT EXISTS.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys
import types
import uuid
from datetime import datetime

from sqlalchemy.dialects import postgresql

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret-value")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

_HELPERS = (
    pathlib.Path(__file__).resolve().parents[2]
    / "services" / "backup-scheduler" / "dispatch_policy.py"
)


def _load():
    spec = importlib.util.spec_from_file_location(
        "backup_scheduler_dispatch_policy_drainq", _HELPERS
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


dp = _load()
PID = uuid.uuid4()
SINCE = datetime(2026, 9, 3, 2, 38)


def _sql(stmt) -> str:
    # literal_binds=True: SQLAlchemy's JSON path-key accessor (Job.spec["x"])
    # always compiles its key to a bindparam, never inline text — with
    # literal_binds=False the "sla_policy_id" / "batch_id" / "wave" fragment
    # checks below can never see the key at all. test_backup_scheduler_
    # overlap_guard.py hit the same thing and settled on literal_binds=True
    # "to assert on the fully-rendered statement"; matching that here.
    return str(stmt.compile(dialect=postgresql.dialect(),
                            compile_kwargs={"literal_binds": True}))


def test_cycle_dispatched_ids_unnests_scheduled_jobs_of_this_policy_any_status():
    sql = _sql(dp.build_cycle_dispatched_ids_query(PID, SINCE))
    assert "unnest(jobs.batch_resource_ids)" in sql
    assert "jobs.spec ->> " in sql and "sla_policy_id" in sql
    assert "triggered_by" in sql
    assert "jobs.created_at >= " in sql
    assert "DISTINCT" in sql
    assert "jobs.status" not in sql, "FAILED must count as carried (AUTO_RETRY owns it)"


def test_cycle_batch_id_is_the_earliest_scheduled_job():
    sql = _sql(dp.build_cycle_batch_id_query(PID, SINCE))
    assert "batch_id" in sql
    assert "ORDER BY jobs.created_at ASC" in sql
    assert "LIMIT" in sql


def test_cycle_max_wave_coalesces_to_zero():
    sql = _sql(dp.build_cycle_max_wave_query(PID, SINCE))
    assert "max(" in sql and "wave" in sql
    assert "coalesce(" in sql


def test_drainable_policies_are_enabled_capped_and_scheduled():
    sql = _sql(dp.build_drainable_policies_query())
    assert "sla_policies.enabled IS true" in sql
    assert "sla_policies.frequency != " in sql
    assert "sla_policies.max_concurrent_backups > " in sql


def test_drain_candidates_use_not_exists_and_never_backed_up_first():
    sql = _sql(dp.build_drain_candidates_query(PID, SINCE, ["USER_MAIL", "ROOM_MAILBOX"]))
    assert "NOT (EXISTS" in sql, "NOT IN would go blind on a single NULL"
    assert "NOT IN" not in sql
    assert "resources.status IN" in sql
    assert "resources.type IN" in sql
    assert "resources.type != " in sql  # POWER_BI is cron-only (modified-workspace prefilter)
    assert "ORDER BY resources.last_backup_at ASC NULLS FIRST, resources.id ASC" in sql
    # light projection, not SELECT resources.*
    assert "resources.metadata" not in sql.split("FROM resources", 1)[0]
    assert "json_extract_path_text(resources.metadata, 'license_missing')" in sql
    assert "!= 'true'" in sql


def test_wave_audit_details_carry_counts_only():
    policy = types.SimpleNamespace(id=PID, name="Silver")
    d = dp.wave_audit_details(
        policy, batch_id="b1", wave=3, dispatched=50, remaining=206, held=4,
        inflight_resources=0, max_concurrent=50, started_cycle=False,
    )
    assert d["wave"] == 3 and d["dispatched_count"] == 50
    assert d["remaining_count"] == 206 and d["held_count"] == 4
    assert d["batch_id"] == "b1" and d["started_cycle"] is False
    assert "resource_ids" not in d and "resources" not in d
    assert "wave 3" in d["message"] and d["source"] == "backup_scheduler"


def test_cycle_incomplete_details_list_a_bounded_set_of_ids():
    policy = types.SimpleNamespace(id=PID, name="Silver")
    rows = [types.SimpleNamespace(id=uuid.uuid4(), type=types.SimpleNamespace(value="USER_MAIL"),
                                  display_name=f"r{i}") for i in range(5)]
    d = dp.cycle_incomplete_audit_details(
        policy, rows, batch_id="b1", window_end=datetime(2026, 9, 4, 2, 39), max_ids=3)
    assert d["remaining_count"] == 5
    assert len(d["resource_ids"]) == 3 and len(d["resources"]) == 3
    assert d["skip_reason"] == "cycle_incomplete"
    assert "never dispatched" in d["message"]
    assert d["held_count"] == 0

    held_rows = [types.SimpleNamespace(id=uuid.uuid4(), type=types.SimpleNamespace(value="USER_MAIL"),
                                       display_name=f"h{i}") for i in range(2)]
    d2 = dp.cycle_incomplete_audit_details(
        policy, rows, batch_id="b1", window_end=datetime(2026, 9, 4, 2, 39), max_ids=3,
        held=held_rows)
    assert d2["held_count"] == 2
    assert d2["remaining_count"] == 5   # unchanged — held is counted, not merged in
    assert len(d2["resource_ids"]) == 3 and len(d2["resources"]) == 3  # bounded over remaining only
    assert "more held in flight" in d2["message"]


def test_deferred_message_no_longer_promises_the_next_cycle():
    policy = types.SimpleNamespace(id=PID, name="Silver")
    rows = [types.SimpleNamespace(id=uuid.uuid4(), type=types.SimpleNamespace(value="USER_MAIL"),
                                  display_name="r")]
    d = dp.deferred_audit_details(policy, rows, max_concurrent=50,
                                  inflight_resources=0, dispatched=50)
    assert "later wave" in d["message"] and "next cycle" not in d["message"]
