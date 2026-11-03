"""Automatic retry of failed backup jobs (Configuration tab, 2026-08-18).

Operator ask: retry a failed backup job after a configurable delay
(default 5 min), up to a configurable count (default 3), both editable on
the Configuration tab (app_settings singleton).

Source pins (repo convention — no DB fixture): the sweep's safety rails
each exist because of a specific incident:
  * fresh Job row per retry, never re-queueing a FAILED job (2026-08-07
    review: re-queued bulk jobs are dropped by bulk_fanout_seen and sit
    non-terminal forever);
  * per-pass dispatch cap + newest-failure-only + nothing-in-flight
    (2026-08-18: the BufferMapping convoy was a retry-amplified pile-up);
  * bounded lookback so enabling the feature can't resurrect stale
    failures; user cancels are final; concurrent-skip no-ops don't
    suppress a retry.
"""
from __future__ import annotations

import pathlib
import re

import pytest

_SCHED = (
    pathlib.Path(__file__).resolve().parents[2]
    / "services" / "backup-scheduler" / "main.py"
).read_text()


def _tick() -> str:
    i = _SCHED.find("async def _job_retry_tick")
    assert i != -1
    return _SCHED[i: _SCHED.find("\nasync def start_job_retry_loop", i)]


def test_retry_dispatches_fresh_job_rows_only():
    blk = _tick()
    assert "job = Job(" in blk and "session.add(job)" in blk
    assert "SET status = 'QUEUED'" not in blk and "status='QUEUED'" not in blk, (
        "retries must mint NEW job rows — re-queueing a FAILED job is the "
        "shape the 2026-08-07 review rejected (the only allowed jobs UPDATE "
        "here is the FAILED/publish_failed marker)"
    )
    assert '"triggered_by": "AUTO_RETRY"' in blk
    assert '"retry_root": root' in blk


def test_retry_candidate_sql_safety_rails():
    blk = _tick()
    assert "make_interval(mins => :lookback)" in blk, "bounded lookback missing"
    assert "make_interval(mins => :delay)" in blk, "configurable delay missing"
    assert "coalesce(s.extra_data->>'cancelled_at', '') = ''" in blk, (
        "a user cancel is final — it must never be auto-retried"
    )
    assert "skipped_concurrent" in blk, (
        "a concurrent-skip no-op is not a real capture and must not "
        "suppress the retry"
    )
    assert "DISTINCT ON (s.resource_id)" in blk and "s.completed_at DESC" in blk, (
        "only the NEWEST failure per resource may be retried"
    )
    assert "n.status = 'IN_PROGRESS'" in blk, (
        "an in-flight run must suppress the retry"
    )


def test_retry_chain_is_bounded_and_capped():
    blk = _tick()
    assert "spec->>'retry_root' = :root" in blk, "chain accounting missing"
    assert "used >= retry_max" in blk
    assert "JOB_RETRY_DISPATCH_CAP" in blk, "per-pass dispatch cap missing"
    assert "retry_max <= 0" in blk, "0 must disable the sweep"


def test_retry_settings_come_from_app_settings_each_pass():
    blk = _tick()
    assert "get_app_settings" in blk
    assert "job_retry_max" in blk and "job_retry_delay_minutes" in blk


def test_loop_is_registered_at_startup():
    assert "await start_job_retry_loop()" in _SCHED
    loop = _SCHED[_SCHED.find("async def start_job_retry_loop"):]
    assert "except Exception as exc" in loop, (
        "a tick failure must not kill the loop"
    )


def test_app_settings_validation_bounds():
    import importlib.util, os, sys
    os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://u:p@localhost/db")
    root = pathlib.Path(__file__).resolve().parents[2]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    from shared import app_settings as aps
    assert aps._validate_retry_max(0) == 0
    assert aps._validate_retry_max(10) == 10
    assert aps._validate_retry_max("3") == 3
    with pytest.raises(ValueError):
        aps._validate_retry_max(11)
    with pytest.raises(ValueError):
        aps._validate_retry_max(-1)
    with pytest.raises(ValueError):
        aps._validate_retry_max("lots")
    assert aps._validate_retry_delay(1) == 1
    assert aps._validate_retry_delay(1440) == 1440
    with pytest.raises(ValueError):
        aps._validate_retry_delay(0)
    with pytest.raises(ValueError):
        aps._validate_retry_delay(1441)


# ── Review-round rails (2026-08-18, 3-lens adversarial review) ──

def test_budget_is_per_resource_and_per_failure():
    blk = _tick()
    assert "AND resource_id = CAST(:rid AS uuid)" in blk, (
        "bulk cycles put 1000 resources on ONE job id — the budget count "
        "must scope to the resource or retry_max is a shared batch budget"
    )
    # chain roots at the failed SNAPSHOT (per-resource per-run), never the
    # (possibly bulk) job id
    assert "root = str(cand.snapshot_id)" in blk
    assert "root = str(failed_job.id)" not in blk


def test_queued_jobs_suppress_candidacy():
    blk = _tick()
    assert "jf.status IN ('QUEUED','PENDING','RUNNING','RETRYING')" in blk, (
        "a dispatched-but-unconsumed retry has no snapshot row — without a "
        "jobs-table in-flight guard the budget burns one attempt per 60s pass"
    )


def test_lookback_always_covers_the_configured_delay():
    blk = _tick()
    assert "max(JOB_RETRY_LOOKBACK_HOURS * 60, delay_min + 120)" in blk
    assert "make_interval(mins => :lookback)" in blk


def test_lost_publish_neither_strands_nor_burns_budget():
    blk = _tick()
    assert "publish_failed" in blk and "except Exception as _pe" in blk
    assert "coalesce(result->>'publish_failed', '') <> 'true'" in blk
    sched = _SCHED
    assert "republished single-resource job" in sched, (
        "the outbox reconciler must republish zero-snapshot single-resource "
        "QUEUED jobs (the crash-between-commit-and-publish path)"
    )


def test_deterministic_failures_are_not_retried():
    blk = _tick()
    assert "coalesce((s.extra_data->>'retryable')::bool, true)" in blk
    worker = pathlib.Path(
        str(pathlib.Path(__file__).resolve().parents[2]
            / "workers" / "backup-worker" / "main.py")).read_text()
    assert 'existing["retryable"] = False' in worker
    archive = pathlib.Path(
        str(pathlib.Path(__file__).resolve().parents[2]
            / "workers" / "backup-worker" / "archive_backup.py")).read_text()
    assert "_gate_exc.retryable = False" in archive


def test_resource_status_filter_matches_dispatch_paths():
    blk = _tick()
    assert "r.status IN ('DISCOVERED', 'ACTIVE')" in blk
