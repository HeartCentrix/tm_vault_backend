"""Restore terminal status must reflect the handler's counts for EVERY type.

Defect (restore-worker main.py, process_restore_message, until 2026-10-14):
`failed = int(result.get("failed_count"))` was computed and never read. Only
the export types (EXPORT_ZIP / EXPORT_PST / DOWNLOAD) could end FAILED, and
only when exported == 0. Every live restore — IN_PLACE / CROSS_USER /
CROSS_RESOURCE, whose handlers return restored_count / failed_count — was
stamped COMPLETED with audit outcome SUCCESS even when restored_count == 0
and every single item had failed: a green job for a restore that wrote
nothing.

Pinned here via the extracted pure decision `decide_restore_terminal_status`,
the persisted side effects (`update_job_status` stamping error_message /
completed_at on FAILED, `log_audit_event` carrying done_with_errors) and the
hardened export-completed notification (`_post_export_notification` sends
X-Internal-Api-Key and logs a non-2xx reply — the bare POST was 401'd).

restore-worker has a hyphen in its dir name, so main.py is loaded via
importlib (same pattern as test_restore_heavy_consume.py).
"""
from __future__ import annotations

import asyncio
import datetime as dt
import importlib.util
import inspect
import os
import pathlib
import sys
import uuid
from types import SimpleNamespace

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "restore-worker" / "main.py"
sys.path.insert(0, str(_MAIN_PATH.parent))
_spec = importlib.util.spec_from_file_location("rw_main_under_test_terminal", _MAIN_PATH)
_rw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["rw_main_under_test_terminal"] = _rw
try:
    _spec.loader.exec_module(_rw)  # type: ignore[union-attr]
except Exception as exc:
    pytest.skip(
        f"restore-worker module failed to import: {exc}",
        allow_module_level=True,
    )

from shared.models import JobStatus  # noqa: E402

decide = _rw.decide_restore_terminal_status

LIVE_TYPES = ["IN_PLACE", "CROSS_USER", "CROSS_RESOURCE"]
EXPORT_TYPES = ["EXPORT_ZIP", "EXPORT_PST", "DOWNLOAD"]


# ───────────────────────────── live restores ─────────────────────────────


@pytest.mark.parametrize("restore_type", LIVE_TYPES)
def test_live_restore_with_nothing_restored_and_failures_is_FAILED(restore_type):
    d = decide(restore_type, {"restored_count": 0, "failed_count": 3})
    assert d.status is JobStatus.FAILED
    assert d.audit_outcome == "FAILURE"
    assert d.audit_action == "RESTORE_FAILED"
    assert d.done_with_errors is False
    assert (d.succeeded, d.failed) == (0, 3)
    assert d.error_message and "0 restored" in d.error_message and "3 failed" in d.error_message


@pytest.mark.parametrize("restore_type", LIVE_TYPES)
def test_live_restore_with_some_failures_is_COMPLETED_done_with_errors(restore_type):
    d = decide(restore_type, {"restored_count": 5, "failed_count": 1})
    assert d.status is JobStatus.COMPLETED
    assert d.audit_outcome == "PARTIAL"
    assert d.audit_action == "RESTORE_COMPLETED"
    assert d.done_with_errors is True
    assert d.error_message is None
    assert (d.succeeded, d.failed) == (5, 1)


def test_clean_live_restore_is_COMPLETED_SUCCESS():
    d = decide("IN_PLACE", {"restored_count": 7, "failed_count": 0})
    assert d.status is JobStatus.COMPLETED
    assert d.audit_outcome == "SUCCESS"
    assert d.done_with_errors is False
    assert d.error_message is None


def test_live_restore_with_nothing_to_do_stays_COMPLETED():
    """Only skipped items (e.g. Teams messages, reported via manual_actions):
    nothing failed, so it is not a failure."""
    d = decide("IN_PLACE", {"restored_count": 0, "failed_count": 0, "manual_actions": ["x"]})
    assert d.status is JobStatus.COMPLETED
    assert d.audit_outcome == "SUCCESS"


def test_missing_or_null_counts_are_tolerated():
    d = decide("CROSS_USER", {"restored_count": None, "failed_count": None})
    assert d.status is JobStatus.COMPLETED
    assert (d.succeeded, d.failed) == (0, 0)


# ─────────────────────────────── exports ─────────────────────────────────


@pytest.mark.parametrize("restore_type", EXPORT_TYPES)
def test_export_with_nothing_exported_is_FAILED_even_without_failures(restore_type):
    """Pre-existing rule kept: no file was produced, so there is nothing to
    download — the UI must not offer a Download button."""
    d = decide(restore_type, {"exported_count": 0, "failed_count": 0})
    assert d.status is JobStatus.FAILED
    assert d.audit_outcome == "FAILURE"
    assert d.audit_action == "RESTORE_FAILED"
    assert "0 exported" in d.error_message


@pytest.mark.parametrize("restore_type", EXPORT_TYPES)
def test_partial_export_is_COMPLETED_done_with_errors(restore_type):
    d = decide(restore_type, {"exported_count": 3, "failed_count": 2})
    assert d.status is JobStatus.COMPLETED
    assert d.audit_outcome == "PARTIAL"
    assert d.done_with_errors is True


def test_clean_export_is_COMPLETED_SUCCESS():
    d = decide("EXPORT_ZIP", {"exported_count": 3, "failed_count": 0})
    assert d.status is JobStatus.COMPLETED
    assert d.audit_outcome == "SUCCESS"


def test_export_decision_ignores_a_stray_restored_count():
    """An export result never carries restored_count; a stray one must not
    rescue an empty export."""
    d = decide("EXPORT_PST", {"restored_count": 9, "exported_count": 0, "failed_count": 0})
    assert d.status is JobStatus.FAILED


def test_handler_error_text_is_kept_in_error_message():
    d = decide("IN_PLACE", {"restored_count": 0, "failed_count": 2, "error": "Graph 403 on every item"})
    assert d.error_message.startswith("Graph 403 on every item")
    assert "2 failed" in d.error_message


# ───────────────────────── persisted side effects ────────────────────────


class _FakeSession:
    def __init__(self, job):
        self.job = job
        self.flushed = 0

    async def get(self, model, job_id):
        return self.job

    async def flush(self):
        self.flushed += 1


def _job():
    return SimpleNamespace(
        status=JobStatus.RUNNING, completed_at=None, progress_pct=5,
        result=None, error_message=None, attempts=0,
    )


def _worker():
    return _rw.RestoreWorker()


def test_update_job_status_FAILED_stamps_error_message_and_completed_at():
    job = _job()
    sess = _FakeSession(job)
    asyncio.run(_worker().update_job_status(
        sess, uuid.uuid4(), JobStatus.FAILED,
        {"restored_count": 0, "failed_count": 3},
        error_message="Restore finished with no items restored (0 restored, 3 failed)",
    ))
    assert job.status is JobStatus.FAILED
    assert job.error_message.startswith("Restore finished with no items restored")
    assert isinstance(job.completed_at, dt.datetime)
    assert job.result == {"restored_count": 0, "failed_count": 3}
    assert job.progress_pct == 5  # untouched on FAILED
    assert sess.flushed == 1


def test_update_job_status_COMPLETED_unchanged():
    job = _job()
    asyncio.run(_worker().update_job_status(
        _FakeSession(job), uuid.uuid4(), JobStatus.COMPLETED, {"x": 1},
    ))
    assert job.status is JobStatus.COMPLETED
    assert job.progress_pct == 100
    assert job.error_message is None
    assert isinstance(job.completed_at, dt.datetime)


def test_update_job_status_RUNNING_does_not_stamp_completed_at():
    job = _job()
    asyncio.run(_worker().update_job_status(_FakeSession(job), uuid.uuid4(), JobStatus.RUNNING))
    assert job.completed_at is None
    assert job.error_message is None


# ───────────────────────────── httpx capture ─────────────────────────────


class _Resp:
    def __init__(self, status_code: int, text: str = ""):
        self.status_code = status_code
        self.text = text


class _FakeClient:
    captured: list = []
    status = 200
    text = "ok"

    def __init__(self, *_a, **_kw):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False

    async def post(self, url, json=None, headers=None, **_kw):
        type(self).captured.append({"url": url, "json": json, "headers": headers or {}})
        return _Resp(type(self).status, type(self).text)


@pytest.fixture
def fake_httpx(monkeypatch):
    _FakeClient.captured = []
    _FakeClient.status = 200
    _FakeClient.text = "ok"
    monkeypatch.setattr(_rw.httpx, "AsyncClient", _FakeClient)
    return _FakeClient


def test_audit_event_carries_partial_outcome_and_flag(fake_httpx):
    result = {"restored_count": 5, "failed_count": 1, "done_with_errors": True}
    asyncio.run(_worker().log_audit_event(
        uuid.uuid4(), {"restoreType": "IN_PLACE", "tenantId": "t"}, result,
        action="RESTORE_COMPLETED", outcome="PARTIAL",
    ))
    assert len(fake_httpx.captured) == 1
    body = fake_httpx.captured[0]["json"]
    assert body["outcome"] == "PARTIAL"
    assert body["action"] == "RESTORE_COMPLETED"
    assert body["details"]["done_with_errors"] is True
    assert body["details"]["restored_count"] == 5
    assert body["details"]["failed_count"] == 1


def test_audit_event_omits_flag_on_clean_run(fake_httpx):
    asyncio.run(_worker().log_audit_event(
        uuid.uuid4(), {"restoreType": "IN_PLACE"}, {"restored_count": 5, "failed_count": 0},
    ))
    assert "done_with_errors" not in fake_httpx.captured[0]["json"]["details"]


# ─────────────────── export-completed notification (fix 2) ───────────────


def test_export_notification_sends_internal_api_key(fake_httpx, monkeypatch):
    monkeypatch.setenv("INTERNAL_API_KEY", "unit-test-key")
    asyncio.run(_rw._post_export_notification(
        "http://alert/api/v1/alerts/notify/export-completed", {"job_id": "j"},
    ))
    assert len(fake_httpx.captured) == 1
    sent = fake_httpx.captured[0]
    assert sent["headers"].get("X-Internal-Api-Key") == "unit-test-key"
    assert sent["json"] == {"job_id": "j"}


def test_export_notification_logs_non_2xx(fake_httpx, monkeypatch, capsys):
    monkeypatch.setenv("INTERNAL_API_KEY", "unit-test-key")
    fake_httpx.status = 401
    fake_httpx.text = "Authentication required"
    asyncio.run(_rw._post_export_notification("http://alert/x", {}))
    out = capsys.readouterr().out
    assert "export-completed notify rejected" in out
    assert "HTTP 401" in out


def test_export_notification_silent_on_2xx(fake_httpx, monkeypatch, capsys):
    monkeypatch.setenv("INTERNAL_API_KEY", "unit-test-key")
    fake_httpx.status = 202
    asyncio.run(_rw._post_export_notification("http://alert/x", {}))
    assert "rejected" not in capsys.readouterr().out


# ───────────────────────────── wiring pins ───────────────────────────────


def test_process_restore_message_uses_the_decision_and_passes_error_message():
    src = inspect.getsource(_rw.RestoreWorker.process_restore_message)
    assert "decide_restore_terminal_status(restore_type, result)" in src
    assert "error_message=decision.error_message" in src
    assert 'result["done_with_errors"] = True' in src
    assert 'result["failed_count"] = decision.failed' in src
    # The old export-only rule must be gone.
    assert "if is_export_type and exported == 0" not in src


def test_export_path_posts_through_the_hardened_helper():
    src = inspect.getsource(_rw.RestoreWorker.export_as_zip)
    assert "_post_export_notification(" in src
    assert "_httpx.AsyncClient" not in src
