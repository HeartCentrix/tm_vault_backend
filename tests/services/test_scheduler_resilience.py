from __future__ import annotations

import ast
import datetime as dt
import importlib.util
import pathlib
import sys
from types import SimpleNamespace

import pytest


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
