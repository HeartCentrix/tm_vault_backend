from __future__ import annotations

import ast
import asyncio
import datetime as dt
import importlib.util
import pathlib
import sys
from types import SimpleNamespace

import pytest


_GOLD_DAYS = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]


class _FakeScheduler:
    """Minimal APScheduler stand-in that records add/remove/get calls so we
    can assert reschedule idempotency without a running event loop."""

    def __init__(self):
        self._jobs = {}
        self.add_calls = []
        self.remove_calls = []

    def get_job(self, job_id):
        return self._jobs.get(job_id)

    def add_job(self, func, **kwargs):
        jid = kwargs.get("id")
        self.add_calls.append(jid)
        self._jobs[jid] = SimpleNamespace(id=jid, func=func, kwargs=kwargs)
        return self._jobs[jid]

    def remove_job(self, job_id):
        self.remove_calls.append(job_id)
        self._jobs.pop(job_id, None)


def _gold_policy(frequency="THREE_DAILY"):
    return SimpleNamespace(
        id="09378695-3437-4242-a041-22ff5b9a1d7b",
        name="Gold",
        frequency=frequency,
        backup_days=list(_GOLD_DAYS),
        backup_window_start=None,
    )


_SCHEDULER_MAIN = (
    pathlib.Path(__file__).resolve().parents[2]
    / "services" / "backup-scheduler" / "main.py"
)


def _load_scheduler(monkeypatch):
    monkeypatch.setenv("ALLOW_DEV_JWT_SECRETS", "true")
    spec = importlib.util.spec_from_file_location("backup_scheduler_under_test", _SCHEDULER_MAIN)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules["backup_scheduler_under_test"] = module
    spec.loader.exec_module(module)
    return module


def test_three_daily_fire_times_are_reconstructed_after_restart(monkeypatch):
    module = _load_scheduler(monkeypatch)
    policy = SimpleNamespace(
        id="09378695-3437-4242-a041-22ff5b9a1d7b",
        frequency="THREE_DAILY",
        backup_days=["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"],
        backup_window_start=None,
    )

    fires = module._policy_fire_times_between(
        policy,
        dt.datetime(2026, 6, 20, 0, 0),
        dt.datetime(2026, 6, 20, 23, 59),
    )

    assert [f.strftime("%H:%M") for f in fires] == ["02:49", "10:49", "18:49"]


def test_backup_scheduler_registers_self_healing_sweeps():
    source = _SCHEDULER_MAIN.read_text()

    assert "schedule_all_policies" in source
    assert "catch_up_missed_policy_runs" in source
    assert "sla_policy_reschedule_sweep" in source
    assert "sla_policy_catchup_sweep" in source


def test_policy_jobs_have_misfire_and_single_instance_guards():
    tree = ast.parse(_SCHEDULER_MAIN.read_text())
    fn = next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "schedule_policy_job"
    )
    source = ast.get_source_segment(_SCHEDULER_MAIN.read_text(), fn) or ""

    assert "misfire_grace_time" in source
    assert "coalesce=True" in source
    assert "max_instances=1" in source


def test_schedule_policy_job_is_idempotent_for_unchanged_policy(monkeypatch):
    """The 5-min reschedule sweep calls schedule_policy_job for every policy.
    If the cron params are unchanged it must NOT remove+re-add the job:
    re-adding a cron job resets next_run_time and races the fire window,
    which is what drops scheduled Gold fires (observed 1-2x/day vs 3x)."""
    module = _load_scheduler(monkeypatch)
    sched = _FakeScheduler()
    monkeypatch.setattr(module, "scheduler", sched)

    policy = _gold_policy()
    asyncio.run(module.schedule_policy_job(policy))
    asyncio.run(module.schedule_policy_job(policy))  # identical -> no-op

    job_id = "policy_backup_09378695-3437-4242-a041-22ff5b9a1d7b"
    assert sched.add_calls == [job_id], (
        "an unchanged policy must be registered exactly once; re-adding the "
        "cron job on every reschedule sweep resets next_run_time and drops fires"
    )
    assert sched.remove_calls == []


def test_schedule_policy_job_reregisters_when_frequency_changes(monkeypatch):
    """A genuine policy change (frequency/window/days) MUST re-register so the
    durability backstop still propagates edits without a redeploy."""
    module = _load_scheduler(monkeypatch)
    sched = _FakeScheduler()
    monkeypatch.setattr(module, "scheduler", sched)

    job_id = "policy_backup_09378695-3437-4242-a041-22ff5b9a1d7b"
    asyncio.run(module.schedule_policy_job(_gold_policy("THREE_DAILY")))
    asyncio.run(module.schedule_policy_job(_gold_policy("DAILY")))

    assert sched.add_calls.count(job_id) == 2, (
        "changing frequency must re-register the cron job"
    )


def test_resolve_finished_job_status_completed(monkeypatch):
    """A non-terminal job whose snapshots are ALL terminal (none in-flight)
    must be flipped — COMPLETED when any snapshot completed. This covers the
    'job stuck QUEUED/RUNNING while its work is done' case (single-resource
    triggers + workers that died before the job-level update) that made the
    Activity feed show 'In Progress' forever."""
    module = _load_scheduler(monkeypatch)
    assert module.resolve_finished_job_status(n_snaps=5, n_open=0, n_completed=5, n_failed=0) == "COMPLETED"
    assert module.resolve_finished_job_status(n_snaps=5, n_open=0, n_completed=4, n_failed=1) == "COMPLETED"


def test_resolve_finished_job_status_failed_when_no_completions(monkeypatch):
    module = _load_scheduler(monkeypatch)
    assert module.resolve_finished_job_status(n_snaps=3, n_open=0, n_completed=0, n_failed=3) == "FAILED"


def test_resolve_finished_job_status_leaves_inflight_and_empty(monkeypatch):
    module = _load_scheduler(monkeypatch)
    # Still has in-flight snapshots → leave it (genuinely running).
    assert module.resolve_finished_job_status(n_snaps=5, n_open=2, n_completed=3, n_failed=0) is None
    # No snapshots yet → not this reaper's job (outbox reconciler republishes).
    assert module.resolve_finished_job_status(n_snaps=0, n_open=0, n_completed=0, n_failed=0) is None


def test_scheduler_json_filters_use_json_extract_path_text():
    source = _SCHEDULER_MAIN.read_text()

    assert ".astext" not in source
    assert 'func.json_extract_path_text(Job.spec, "sla_policy_id")' in source
    assert 'func.json_extract_path_text(Job.spec, "triggered_by")' in source
