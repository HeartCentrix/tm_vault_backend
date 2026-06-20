from __future__ import annotations

import ast
from pathlib import Path


_RESOURCE_MAIN = (
    Path(__file__).resolve().parents[2]
    / "services" / "resource-service" / "main.py"
)


def _parsed() -> ast.Module:
    return ast.parse(_RESOURCE_MAIN.read_text())


def _find_async_fn(tree: ast.Module, name: str) -> ast.AsyncFunctionDef:
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == name:
            return node
    raise AssertionError(f"async function {name!r} not found")


def _calls(fn: ast.AsyncFunctionDef, name: str) -> bool:
    for node in ast.walk(fn):
        call = node.value if isinstance(node, ast.Await) else node
        if not isinstance(call, ast.Call):
            continue
        func = call.func
        if isinstance(func, ast.Name) and func.id == name:
            return True
    return False


def test_scheduler_notifications_use_configured_railway_url():
    source = ast.get_source_segment(
        _RESOURCE_MAIN.read_text(),
        _find_async_fn(_parsed(), "notify_scheduler_reschedule"),
    ) or ""

    assert "settings.BACKUP_SCHEDULER_URL" in source
    assert "backup-scheduler:8008" not in source


def test_policy_assignment_endpoints_reschedule_scheduler():
    tree = _parsed()
    for fn_name in (
        "assign_policy",
        "unassign_policy",
        "bulk_assign_policy",
        "bulk_unassign_policy",
    ):
        assert _calls(_find_async_fn(tree, fn_name), "notify_scheduler_reschedule"), (
            f"{fn_name} must notify the scheduler so newly assigned SLA "
            "policies are registered without waiting for a redeploy"
        )
