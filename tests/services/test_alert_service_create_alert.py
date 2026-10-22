"""POST /api/v1/alerts — the route backup-scheduler has been posting SLA
violations to since the SLA check shipped, and which did not exist.

Until 2026-10-14 alert-service had GET /api/v1/alerts only, so every
SLA_VIOLATION POST was answered 405 and none was ever stored. The route
is gated by shared.auth_enforcement.require_admin_principal — the
internal-services key (X-Internal-Api-Key) OR an admin user token — and
dedupes: an UNRESOLVED alert with the same (tenant_id, type, resource_id)
is refreshed in place and answered 200, anything else is inserted and
answered 201. Both replies use the list_alerts row shape.

Fake session + TestClient style follows
tests/services/test_alert_service_write_guard.py; the auth dependency is
NOT overridden — it is the thing under test.
"""
from __future__ import annotations

import datetime as _dt
import importlib.util
import os
import pathlib
import sys
import uuid

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ["INTERNAL_API_KEY"] = os.environ.get("INTERNAL_API_KEY") or "test-internal-key-alerts"

import pytest
from fastapi.testclient import TestClient

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_SVC_DIR = _REPO_ROOT / "services" / "alert-service"
_spec = importlib.util.spec_from_file_location("alert_main_create_alert", _SVC_DIR / "main.py")
_al = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["alert_main_create_alert"] = _al
assert _spec and _spec.loader
_spec.loader.exec_module(_al)  # type: ignore[union-attr]

import shared.audit  # noqa: E402
from shared.database import get_db  # noqa: E402
from shared.models import Alert  # noqa: E402
from shared.security import create_access_token  # noqa: E402

ORG = uuid.UUID("00000000-0000-0000-0000-0000000000a0")
TENANT = uuid.UUID("00000000-0000-0000-0000-0000000000a1")
RESOURCE = uuid.UUID("00000000-0000-0000-0000-0000000000b1")
POLICY = uuid.UUID("00000000-0000-0000-0000-0000000000d1")
KEY_HEADER = {"X-Internal-Api-Key": os.environ["INTERNAL_API_KEY"]}
LIST_ROW_KEYS = {"id", "severity", "title", "description", "status", "createdAt",
                 "resolved", "tenantId", "type", "message"}


class _Result:
    def __init__(self, rows):
        self._rows = list(rows)

    def scalars(self):
        return self

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return list(self._rows)


class _FakeSession:
    """Serves the dedupe SELECT create_alert issues. Binds are
    tenant_id_1 / type_1 / resource_id_1 (+ param_1 = LIMIT); a None key
    compiles to IS NULL and carries no bind, so a missing bind means None.
    Records add() / flush()."""

    def __init__(self, alerts=()):
        self.alerts = list(alerts)
        self.added = []
        self.flushes = 0
        self.queries = []

    async def execute(self, stmt):
        assert stmt.column_descriptions[0]["entity"] is Alert
        sql = str(stmt)
        assert "alerts.resolved = false" in sql, "dedupe must only consider UNRESOLVED rows"
        p = stmt.compile().params
        self.queries.append(p)
        rows = [
            a for a in self.alerts
            if a.tenant_id == p.get("tenant_id_1")
            and a.type == p.get("type_1")
            and a.resource_id == p.get("resource_id_1")
            and not a.resolved
        ]
        rows.sort(key=lambda a: a.created_at or _dt.datetime.min, reverse=True)
        return _Result(rows[: p.get("param_1") or len(rows)])

    def add(self, obj):
        self.added.append(obj)

    async def flush(self):
        self.flushes += 1


def _violation(**over):
    v = {
        "resource_id": str(RESOURCE), "tenant_id": str(TENANT), "resource_type": "USER_MAIL",
        "sla_policy_id": str(POLICY), "sla_policy_name": "Gold",
        "last_backup_at": None, "never_backed_up": True, "backup_in_progress": False,
        "hours_overdue": None, "severity": "CRITICAL",
    }
    v.update(over)
    return v


def _payload(**over):
    """Exactly the JSON backup-scheduler's send_violations_to_alert_service posts."""
    v = _violation()
    body = {
        "type": "SLA_VIOLATION", "severity": v["severity"],
        "message": "SLA violation: USER_MAIL has NEVER been backed up",
        "tenant_id": v["tenant_id"], "resource_id": v["resource_id"],
        "resource_type": v["resource_type"], "details": v,
    }
    body.update(over)
    return body


def _bearer(roles, email="admin@taylormorrison.com"):
    token = create_access_token({
        "sub": str(uuid.uuid4()), "email": email, "roles": list(roles),
        "orgId": str(ORG), "tenantIds": [str(TENANT)],
    })
    return {"Authorization": f"Bearer {token}"}


def _existing(**over) -> Alert:
    base = dict(
        id=uuid.uuid4(), tenant_id=TENANT, type="SLA_VIOLATION", severity="WARNING",
        message="old message", resource_id=RESOURCE, resource_type="USER_MAIL",
        resolved=False, details={"hours_overdue": 1.0},
        created_at=_dt.datetime(2026, 10, 1, 12, 0, 0),
        updated_at=_dt.datetime(2026, 10, 1, 12, 0, 0),
    )
    base.update(over)
    return Alert(**base)


def _client(sess) -> TestClient:
    _al.app.dependency_overrides[get_db] = lambda: sess
    return TestClient(_al.app)


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    async def _no_audit(**fields):
        return None

    # The mounted AuditMiddleware emits one audit POST per mutating request;
    # keep it off the network here.
    monkeypatch.setattr(shared.audit, "emit_audit_event", _no_audit)
    yield
    _al.app.dependency_overrides.clear()


# ── auth ────────────────────────────────────────────────────────────────

def test_post_without_credentials_is_401():
    sess = _FakeSession()
    r = _client(sess).post("/api/v1/alerts", json=_payload())
    assert r.status_code == 401
    assert sess.added == [] and sess.queries == []


def test_post_with_the_wrong_internal_key_is_401():
    sess = _FakeSession()
    r = _client(sess).post("/api/v1/alerts", json=_payload(),
                           headers={"X-Internal-Api-Key": "not-the-key"})
    assert r.status_code == 401
    assert sess.added == []


def test_post_with_a_plain_user_token_is_403():
    sess = _FakeSession()
    r = _client(sess).post("/api/v1/alerts", json=_payload(), headers=_bearer(["USER"]))
    assert r.status_code == 403
    assert sess.added == []


# ── create ──────────────────────────────────────────────────────────────

def test_internal_key_creates_an_alert_201_in_the_list_row_shape():
    sess = _FakeSession()
    r = _client(sess).post("/api/v1/alerts", json=_payload(), headers=KEY_HEADER)

    assert r.status_code == 201, r.text
    body = r.json()
    assert set(body) == LIST_ROW_KEYS
    assert body["type"] == "SLA_VIOLATION"
    assert body["severity"] == "CRITICAL"
    assert body["status"] == "ACTIVE" and body["resolved"] is False
    assert body["tenantId"] == str(TENANT)
    assert body["message"] == body["description"] == _payload()["message"]
    assert body["createdAt"]

    assert len(sess.added) == 1 and sess.flushes == 1
    row = sess.added[0]
    assert isinstance(row, Alert)
    assert body["id"] == str(row.id)
    assert row.tenant_id == TENANT and row.resource_id == RESOURCE
    assert row.type == "SLA_VIOLATION" and row.severity == "CRITICAL"
    assert row.resource_type == "USER_MAIL"
    assert row.details == _violation()
    assert row.resolved is False
    assert row.triggered_by == "internal-service"
    assert row.created_at is not None and row.updated_at == row.created_at


def test_an_admin_user_token_may_create_an_alert():
    sess = _FakeSession()
    r = _client(sess).post("/api/v1/alerts", json=_payload(),
                           headers=_bearer(["SUPER_ADMIN"], email="ops@taylormorrison.com"))
    assert r.status_code == 201, r.text
    assert sess.added[0].triggered_by == "ops@taylormorrison.com"


def test_unknown_fields_are_rejected_422():
    """extra="forbid": the scheduler payload is the contract (see
    tests/services/test_backup_scheduler_sla_alert_delivery.py)."""
    sess = _FakeSession()
    r = _client(sess).post("/api/v1/alerts", json=_payload(bogus=1), headers=KEY_HEADER)
    assert r.status_code == 422
    assert sess.added == []


# ── dedupe ──────────────────────────────────────────────────────────────

def test_a_duplicate_of_an_unresolved_alert_is_refreshed_and_answered_200():
    existing = _existing()
    sess = _FakeSession(alerts=[existing])

    r = _client(sess).post("/api/v1/alerts", json=_payload(), headers=KEY_HEADER)

    assert r.status_code == 200, r.text
    body = r.json()
    assert set(body) == LIST_ROW_KEYS
    assert body["id"] == str(existing.id)
    assert body["severity"] == "CRITICAL"
    assert body["message"] == _payload()["message"]
    assert body["status"] == "ACTIVE"
    # No second row; the existing one escalated in place.
    assert sess.added == [] and sess.flushes == 1
    assert existing.severity == "CRITICAL"
    assert existing.message == _payload()["message"]
    assert existing.details == _violation()
    assert existing.updated_at > _dt.datetime(2026, 10, 1, 12, 0, 0)
    assert existing.resolved is False
    # The dedupe key is (tenant_id, type, resource_id).
    q = sess.queries[0]
    assert (q["tenant_id_1"], q["type_1"], q["resource_id_1"]) == (TENANT, "SLA_VIOLATION", RESOURCE)


def test_a_resolved_alert_does_not_absorb_a_new_violation():
    """Once an operator resolved the old row, a fresh violation is a new row
    (201) — otherwise it would silently reopen history."""
    sess = _FakeSession(alerts=[_existing(resolved=True)])

    r = _client(sess).post("/api/v1/alerts", json=_payload(), headers=KEY_HEADER)

    assert r.status_code == 201, r.text
    assert len(sess.added) == 1


def test_another_resource_in_the_same_tenant_gets_its_own_row():
    sess = _FakeSession(alerts=[_existing()])
    other = str(uuid.uuid4())

    r = _client(sess).post(
        "/api/v1/alerts",
        json=_payload(resource_id=other, details=_violation(resource_id=other)),
        headers=KEY_HEADER,
    )

    assert r.status_code == 201, r.text
    assert len(sess.added) == 1 and str(sess.added[0].resource_id) == other


def test_tenant_wide_alert_without_a_resource_dedupes_on_null_keys():
    """resource_id None compiles to IS NULL; two such posts are one row."""
    sess = _FakeSession()
    body = _payload(resource_id=None, resource_type=None, details={"scope": "tenant"})
    client = _client(sess)

    first = client.post("/api/v1/alerts", json=body, headers=KEY_HEADER)
    assert first.status_code == 201, first.text
    # Make the inserted row visible to the next dedupe query.
    sess.alerts.extend(sess.added)

    second = client.post("/api/v1/alerts", json=body, headers=KEY_HEADER)
    assert second.status_code == 200, second.text
    assert second.json()["id"] == first.json()["id"]
    assert len(sess.added) == 1
