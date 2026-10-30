"""Tests for audit-service audit-trail integrity.

Two halves of the same finding:

  1. POST /api/v1/audit/log builds an AuditEvent entirely from the request
     body (action, outcome, actor_*, resource_*, details) and had NO auth
     dependency, while api-gateway proxied it. Anyone able to reach the
     ingress could forge audit rows — the audit trail is the compliance
     artifact for a backup product, so an anonymous writer destroys its
     integrity. It must require the internal-services shared secret
     (X-Internal-Api-Key), the same gate delta-token uses, and fail
     closed when that secret is not configured.

  2. GET /api/v1/audit/siem/stream?format=cef interpolated event fields
     into CEF without escaping. A field value containing `|`, `=`, `\\`
     or a newline could forge extra CEF fields or a whole extra event
     record in the SIEM.

audit-service has a hyphen in the dir name, so main.py is loaded via
importlib path (same pattern as test_activity_backup_rollup.py).
"""
from __future__ import annotations

import datetime as dt
import importlib.util
import logging
import os
import pathlib
import sys
import uuid

import pytest

# ── Env required before any shared.config import ──────────────────────────
os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "services" / "audit-service" / "main.py"
_spec = importlib.util.spec_from_file_location("audit_main_under_test", _MAIN_PATH)
_audit = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["audit_main_under_test"] = _audit
try:
    _spec.loader.exec_module(_audit)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"audit-service module failed to import: {exc}",
        allow_module_level=True,
    )

from fastapi.testclient import TestClient  # noqa: E402


# ── Fake DB session ───────────────────────────────────────────────────────


class _FakeResult:
    def __init__(self, rows):
        self._rows = list(rows)

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)


class _FakeSession:
    """Minimal async-session stand-in: records add()s, returns canned rows."""

    def __init__(self, rows=None):
        self.rows = list(rows or [])
        self.added: list = []
        self.committed = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, *_a, **_kw):
        return _FakeResult(self.rows)

    async def get(self, *_a, **_kw):
        return None

    def add(self, obj):
        self.added.append(obj)

    async def commit(self):
        self.committed += 1

    async def rollback(self):
        return None


def _install_fake_db(monkeypatch, session: _FakeSession):
    monkeypatch.setattr(_audit, "async_session_factory", lambda: session)


def _client() -> TestClient:
    return TestClient(_audit.app)


_BODY = {
    "action": "BACKUP_COMPLETED",
    "actor_type": "SYSTEM",
    "outcome": "SUCCESS",
    "resource_name": "forged@contoso.com",
}


# ── 1. /api/v1/audit/log must not accept anonymous writes ─────────────────


def test_audit_log_rejects_anonymous_write(monkeypatch):
    """No credential -> no audit row. Pre-fix this returned 200 and
    persisted an attacker-authored AuditEvent."""
    sess = _FakeSession()
    _install_fake_db(monkeypatch, sess)
    monkeypatch.setattr(_audit.settings, "INTERNAL_API_KEY", "unit-test-key")

    r = _client().post("/api/v1/audit/log", json=_BODY)

    assert r.status_code == 401, r.text
    assert sess.added == [], "anonymous caller persisted an audit event"


def test_audit_log_rejects_wrong_internal_key(monkeypatch):
    sess = _FakeSession()
    _install_fake_db(monkeypatch, sess)
    monkeypatch.setattr(_audit.settings, "INTERNAL_API_KEY", "unit-test-key")

    r = _client().post(
        "/api/v1/audit/log",
        json=_BODY,
        headers={"X-Internal-Api-Key": "not-the-key"},
    )

    assert r.status_code == 401, r.text
    assert sess.added == []


def test_audit_log_accepts_valid_internal_key(monkeypatch):
    """Legitimate internal callers (job-service, workers, shared/audit.py)
    keep working when they present the shared secret."""
    sess = _FakeSession()
    _install_fake_db(monkeypatch, sess)
    monkeypatch.setattr(_audit.settings, "INTERNAL_API_KEY", "unit-test-key")

    r = _client().post(
        "/api/v1/audit/log",
        json=_BODY,
        headers={"X-Internal-Api-Key": "unit-test-key"},
    )

    assert r.status_code == 200, r.text
    assert len(sess.added) == 1
    assert sess.added[0].action == "BACKUP_COMPLETED"


def test_unconfigured_key_rejects_proxied_write(monkeypatch):
    """Ops/Data Check 17: the gate now fails CLOSED. With INTERNAL_API_KEY
    unset, every write is refused (503) — including a proxied one — so no
    anonymous caller can persist an audit event."""
    sess = _FakeSession()
    _install_fake_db(monkeypatch, sess)
    monkeypatch.setattr(_audit.settings, "INTERNAL_API_KEY", "")

    r = _client().post(
        "/api/v1/audit/log",
        json=_BODY,
        headers={"X-Forwarded-For": "203.0.113.7"},
    )

    assert r.status_code == 503, r.text
    assert sess.added == [], "proxied anonymous caller persisted an audit event"


def test_unconfigured_key_refuses_in_cluster_write(monkeypatch):
    """Ops/Data Check 17: INTERNAL_API_KEY is provisioned in tmvault-secrets
    now, so an empty value is a misconfiguration — an unproxied pod-to-pod
    write with no key is REFUSED (fail closed), not accepted. (The former
    degraded 'allow + log' mode is gone.)"""
    sess = _FakeSession()
    _install_fake_db(monkeypatch, sess)
    monkeypatch.setattr(_audit.settings, "INTERNAL_API_KEY", "")

    r = _client().post("/api/v1/audit/log", json=_BODY)

    assert r.status_code == 503, r.text
    assert sess.added == [], "unauthenticated in-cluster write was persisted"


def test_in_process_caller_still_works(monkeypatch):
    """audit-service's own chat-integrity sweep awaits create_audit_event()
    directly (main.py ~454). The header dependency must not break that
    in-process call path."""
    import asyncio

    sess = _FakeSession()
    _install_fake_db(monkeypatch, sess)

    asyncio.run(_audit.create_audit_event(dict(_BODY, action="INTEGRITY_GAP")))

    assert len(sess.added) == 1
    assert sess.added[0].action == "INTEGRITY_GAP"


# ── 2. CEF output must not let a field value break out of position ────────


def _event(**kw):
    base = dict(
        id=uuid.uuid4(),
        action="BACKUP_COMPLETED",
        outcome="SUCCESS",
        occurred_at=dt.datetime(2026, 7, 31, 12, 0, 0),
        resource_name="mailbox@contoso.com",
        resource_id=None,
        actor_email="ops@contoso.com",
        job_id=None,
        tenant_id=None,
    )
    base.update(kw)
    return type("E", (), base)


def _cef_lines(monkeypatch, events) -> list[str]:
    # The SIEM stream is now admin-gated (Ops/Data Check 17). Authenticate as
    # the internal (admin-equivalent) principal; the CEF-escaping properties
    # under test are unchanged.
    monkeypatch.setenv("INTERNAL_API_KEY", "unit-test-key")
    _install_fake_db(monkeypatch, _FakeSession(events))
    r = _client().get("/api/v1/audit/siem/stream", params={"format": "cef"},
                      headers={"X-Internal-Api-Key": "unit-test-key"})
    assert r.status_code == 200, r.text
    return [ln for ln in r.text.split("\n") if ln.strip()]


def test_cef_field_cannot_forge_a_second_record(monkeypatch):
    """A newline in a field value must not split one event into two CEF
    records — that is how a forged 'clean' event gets into the SIEM."""
    lines = _cef_lines(
        monkeypatch,
        [_event(resource_name="ok\nCEF:0|TMVault|BackupPlatform|1.0|LOGIN|Login|1|act=LOGIN")],
    )
    assert len(lines) == 1, f"field value injected extra CEF record(s): {lines}"


def test_cef_field_cannot_forge_extra_fields(monkeypatch):
    """`|` and `=` inside a value must arrive escaped, not as new
    delimiters/keys."""
    lines = _cef_lines(
        monkeypatch,
        [_event(actor_email="a@b.c | act=CLEARED | outcome=SUCCESS")],
    )
    assert len(lines) == 1
    line = lines[0]
    # The injected pipes/equals must be escaped, so no unescaped
    # separator or key can originate from the value.
    assert "| act=CLEARED" not in line, line
    assert "\\|" in line and "\\=" in line, line


def test_cef_escapes_backslash_and_preserves_real_fields(monkeypatch):
    lines = _cef_lines(
        monkeypatch,
        [_event(resource_name="DOMAIN\\user", action="BACKUP_FAILED", outcome="FAILURE")],
    )
    assert len(lines) == 1
    line = lines[0]
    assert "DOMAIN\\\\user" in line, line
    # Real (non-injected) structure survives escaping.
    assert line.startswith("CEF:0"), line
    assert "act=BACKUP_FAILED" in line, line
    assert "outcome=FAILURE" in line, line
