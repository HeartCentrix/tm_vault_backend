"""Caller side of the audit-log auth fix.

audit-service's POST /api/v1/audit/log now requires the internal-services
shared secret (X-Internal-Api-Key). Every in-repo emitter must present it or
the audit trail goes quiet — and because emission is fire-and-forget, a
rejected POST must be logged loudly instead of vanishing.
"""
from __future__ import annotations

import asyncio
import logging
import os

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")

from shared import audit as audit_mod  # noqa: E402


class _Resp:
    def __init__(self, status_code: int):
        self.status_code = status_code
        self.text = "denied" if status_code >= 400 else "ok"


class _FakeClient:
    """Captures the single POST shared/audit.py makes."""

    captured: dict = {}
    status = 200

    def __init__(self, *_a, **_kw):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False

    async def post(self, url, json=None, headers=None, **_kw):
        type(self).captured = {"url": url, "json": json, "headers": headers or {}}
        return _Resp(type(self).status)


@pytest.fixture(autouse=True)
def _fake_httpx(monkeypatch):
    _FakeClient.captured = {}
    _FakeClient.status = 200
    monkeypatch.setattr(audit_mod.httpx, "AsyncClient", _FakeClient)


def test_emit_audit_event_sends_internal_api_key(monkeypatch):
    monkeypatch.setenv("INTERNAL_API_KEY", "unit-test-key")

    asyncio.run(audit_mod.emit_audit_event(action="BACKUP_TRIGGERED"))

    assert _FakeClient.captured, "no POST issued"
    assert (
        _FakeClient.captured["headers"].get("X-Internal-Api-Key") == "unit-test-key"
    ), _FakeClient.captured["headers"]


def test_internal_audit_headers_empty_when_unset(monkeypatch):
    """No key configured -> no bogus header value; the server fails closed."""
    monkeypatch.delenv("INTERNAL_API_KEY", raising=False)

    assert audit_mod.internal_audit_headers() == {}


def test_unset_key_is_reported_loudly(monkeypatch, caplog):
    """A half-configured deploy must be visible in logs, not silent.

    Services enumerate individual secretKeyRef entries in
    deploy/taylor-morrison/k8s/40-services.yaml, so it is entirely possible to
    turn the gate ON at audit-service while an emitter pod still has no key.
    That emitter then sends no header, gets a 401, and -- at the raw
    ``client.post`` call sites in job-service / snapshot-service, which sit
    inside bare ``except Exception: pass`` and never inspect the status --
    loses the audit row with no trace at all. Returning ``{}`` quietly is what
    makes that invisible, so the header builder itself has to complain.
    """
    monkeypatch.delenv("INTERNAL_API_KEY", raising=False)
    audit_mod.reset_internal_key_warning()

    with caplog.at_level(logging.WARNING, logger="tmvault.audit"):
        assert audit_mod.internal_audit_headers() == {}

    msgs = [r.getMessage() for r in caplog.records]
    assert any("INTERNAL_API_KEY" in m for m in msgs), msgs


def test_unset_key_warning_is_rate_limited(monkeypatch, caplog):
    """Emission is per-event and high volume -- the complaint must not become
    the log flood that hides everything else."""
    monkeypatch.delenv("INTERNAL_API_KEY", raising=False)
    audit_mod.reset_internal_key_warning()

    with caplog.at_level(logging.WARNING, logger="tmvault.audit"):
        for _ in range(50):
            audit_mod.internal_audit_headers()

    hits = [r for r in caplog.records if "INTERNAL_API_KEY" in r.getMessage()]
    assert 1 <= len(hits) <= 5, len(hits)


def test_configured_key_is_silent(monkeypatch, caplog):
    """The normal, fully-configured path logs nothing."""
    monkeypatch.setenv("INTERNAL_API_KEY", "unit-test-key")
    audit_mod.reset_internal_key_warning()

    with caplog.at_level(logging.WARNING, logger="tmvault.audit"):
        for _ in range(5):
            assert audit_mod.internal_audit_headers() == {
                "X-Internal-Api-Key": "unit-test-key"
            }

    assert not [r for r in caplog.records if "INTERNAL_API_KEY" in r.getMessage()]


def test_rejected_emission_is_logged_not_swallowed(monkeypatch, caplog):
    """A 401/503 from audit-service means audit rows are being dropped.
    httpx doesn't raise on 4xx/5xx, so without an explicit status check a
    misconfigured INTERNAL_API_KEY would silently erase the audit trail."""
    monkeypatch.setenv("INTERNAL_API_KEY", "unit-test-key")
    _FakeClient.status = 401

    with caplog.at_level(logging.WARNING, logger="tmvault.audit"):
        asyncio.run(audit_mod.emit_audit_event(action="BACKUP_TRIGGERED"))

    assert any(
        "401" in r.getMessage() for r in caplog.records
    ), [r.getMessage() for r in caplog.records]
