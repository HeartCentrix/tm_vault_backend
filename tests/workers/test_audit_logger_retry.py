"""#32: AuditLogger must name the failure, retry once on timeout/transport
errors, and never block or fail the backup that emits the event."""
from __future__ import annotations

import asyncio
import importlib.util
import pathlib
import sys

import httpx
import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def bw():
    name = "bw_audit_logger_retry"
    spec = importlib.util.spec_from_file_location(name, ROOT / "workers/backup-worker/main.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _fake_client(outcomes, calls, gate=None):
    class FakeClient:
        def __init__(self, *a, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def post(self, url, json=None, headers=None):
            calls.append(json)
            if gate is not None:
                await gate.wait()
            outcome = outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome

    return FakeClient


async def _drain(bw):
    await asyncio.gather(*list(bw.AuditLogger._pending))


async def test_timeout_is_retried_once_then_succeeds(bw, monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(httpx, "AsyncClient", _fake_client(
        [httpx.ReadTimeout(""), httpx.Response(200)], calls))
    await bw.AuditLogger().log(action="BACKUP_STARTED")
    await _drain(bw)
    assert len(calls) == 2
    assert "ReadTimeout" in capsys.readouterr().out


async def test_two_transport_failures_log_type_and_never_raise(bw, monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(httpx, "AsyncClient", _fake_client(
        [httpx.ConnectError("refused"), httpx.ConnectError("refused")], calls))
    await bw.AuditLogger().log(action="BACKUP_COMPLETED", details={"secret": "s3cr3t"})
    await _drain(bw)
    out = capsys.readouterr().out
    assert len(calls) == 2
    assert "ConnectError('refused')" in out
    assert "s3cr3t" not in out


async def test_non_transport_error_is_not_retried(bw, monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(httpx, "AsyncClient", _fake_client([ValueError("bad")], calls))
    await bw.AuditLogger().log(action="BACKUP_FAILED")
    await _drain(bw)
    assert len(calls) == 1
    assert "ValueError" in capsys.readouterr().out


async def test_log_returns_without_waiting_for_audit_service(bw, monkeypatch):
    calls = []
    gate = asyncio.Event()
    monkeypatch.setattr(httpx, "AsyncClient", _fake_client([httpx.Response(200)], calls, gate))
    await asyncio.wait_for(bw.AuditLogger().log(action="BACKUP_STARTED"), timeout=1)
    gate.set()
    await _drain(bw)
    assert len(calls) == 1
