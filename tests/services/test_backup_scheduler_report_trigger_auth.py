"""backup-scheduler must present the internal key when triggering reports.

report-service's POST /api/v1/reports/generate now requires a principal (a
user JWT or X-Internal-Api-Key — tests/services/test_report_service_auth.py).
The scheduler's send_daily_backup_report / send_weekly_summary_report are its
only machine callers and used to POST with no headers; without the header
every scheduled report would be rejected with 401 and only logged.

backup-scheduler has a hyphen in its dir name, so main.py is loaded via
importlib (same pattern as test_backup_scheduler_dispatch_helper.py).
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret-value")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _load_scheduler():
    path = _ROOT / "services" / "backup-scheduler" / "main.py"
    spec = importlib.util.spec_from_file_location("backup_scheduler_main_report_auth", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


sched = _load_scheduler()


class _Resp:
    status_code = 200
    text = "ok"


class _FakeClient:
    captured: list = []

    def __init__(self, *_a, **_kw):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False

    async def post(self, url, json=None, headers=None, **_kw):
        type(self).captured.append({"url": url, "json": json, "headers": headers or {}})
        return _Resp()


@pytest.fixture
def fake_httpx(monkeypatch):
    _FakeClient.captured = []
    monkeypatch.setattr(sched.httpx, "AsyncClient", _FakeClient)
    monkeypatch.setenv("INTERNAL_API_KEY", "unit-test-key")
    return _FakeClient


@pytest.mark.parametrize("fn,report_type", [
    ("send_daily_backup_report", "DAILY"),
    ("send_weekly_summary_report", "WEEKLY"),
])
def test_report_trigger_sends_internal_api_key(fake_httpx, fn, report_type):
    asyncio.run(getattr(sched, fn)())
    assert len(fake_httpx.captured) == 1
    sent = fake_httpx.captured[0]
    assert sent["url"].endswith("/api/v1/reports/generate")
    assert sent["json"] == {"report_type": report_type}
    assert sent["headers"].get("X-Internal-Api-Key") == "unit-test-key"
