"""report-service: POST /api/v1/reports/send-test and /generate require auth.

Defect: both routes had NO auth dependency while every sibling
(/reports/config GET/POST/PUT, /reports/history*) uses
Depends(get_current_user_from_token). api-gateway proxies both, so anyone
reaching the ingress could fire test e-mails at an arbitrary ?to= address
(send-test) or trigger a full report send to the configured recipients
(generate).

Fix:
  * send-test -> Depends(get_current_user_from_token), same as the siblings.
  * generate  -> Depends(require_principal): a user JWT OR the
                 X-Internal-Api-Key shared secret, because backup-scheduler
                 triggers the DAILY / WEEKLY runs machine-to-machine (it now
                 sends the key — tests/services/test_backup_scheduler_report_trigger_auth.py).

No admin gate: none of the siblings has one, so parity is kept.

report-service has a hyphen in its dir name, so main.py is loaded via
importlib (same pattern as test_alert_service_write_guard.py).
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys
import uuid

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

import pytest
from fastapi.testclient import TestClient

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_SVC_DIR = _REPO_ROOT / "services" / "report-service"
_spec = importlib.util.spec_from_file_location("report_main_under_test", _SVC_DIR / "main.py")
_rs = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["report_main_under_test"] = _rs
assert _spec and _spec.loader
_spec.loader.exec_module(_rs)  # type: ignore[union-attr]

from shared.security import create_access_token, get_current_user_from_token  # noqa: E402

_SEND_TEST = "/api/v1/reports/send-test"
_GENERATE = "/api/v1/reports/generate"
_KEY = "unit-test-internal-key"


class _NoConfigResult:
    def scalar_one_or_none(self):
        return None


class _FakeSession:
    """generate_report's first query is `select(ReportConfig).limit(1)`; with
    no row it answers "Reports not enabled" and touches nothing else."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False

    async def execute(self, *_a, **_kw):
        return _NoConfigResult()


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("INTERNAL_API_KEY", _KEY)
    # Never try to talk SMTP or Postgres from a unit test.
    monkeypatch.setattr(_rs, "SMTP_ENABLED", False)
    monkeypatch.setattr(_rs, "async_session_factory", lambda: _FakeSession())
    _rs.app.dependency_overrides.clear()
    yield
    _rs.app.dependency_overrides.clear()


def _client() -> TestClient:
    return TestClient(_rs.app)


def _user_token() -> str:
    return create_access_token({
        "sub": str(uuid.uuid4()),
        "email": "member@taylormorrison.com",
        "roles": ["USER"],
        "orgId": str(uuid.uuid4()),
        "tenantIds": [],
    })


# ───────────────────────────── anonymous: 401 ─────────────────────────────


def test_send_test_anonymous_is_401():
    r = _client().post(_SEND_TEST, params={"to": "x@taylormorrison.com"})
    assert r.status_code == 401


def test_generate_anonymous_is_401():
    r = _client().post(_GENERATE, json={"report_type": "DAILY"})
    assert r.status_code == 401


def test_generate_wrong_internal_key_is_401():
    r = _client().post(_GENERATE, json={"report_type": "DAILY"},
                       headers={"X-Internal-Api-Key": "not-the-key"})
    assert r.status_code == 401


def test_generate_garbage_bearer_is_401():
    r = _client().post(_GENERATE, json={"report_type": "DAILY"},
                       headers={"Authorization": "Bearer not-a-jwt"})
    assert r.status_code == 401


# ─────────────────────────── authenticated: not 401 ───────────────────────


def test_send_test_with_user_jwt_passes_auth():
    r = _client().post(_SEND_TEST, params={"to": "x@taylormorrison.com"},
                       headers={"Authorization": f"Bearer {_user_token()}"})
    assert r.status_code == 200, r.text
    assert r.json()["sent"] is False  # SMTP disabled in the unit env


def test_send_test_honours_the_sibling_dependency_override():
    """Same dependency the siblings use, so the existing override style works."""
    _rs.app.dependency_overrides[get_current_user_from_token] = lambda: {"id": "u", "roles": ["USER"]}
    r = _client().post(_SEND_TEST, params={"to": "x@taylormorrison.com"})
    assert r.status_code == 200, r.text


def test_generate_with_internal_key_passes_auth():
    """backup-scheduler's path: machine-to-machine with the shared secret."""
    r = _client().post(_GENERATE, json={"report_type": "DAILY"},
                       headers={"X-Internal-Api-Key": _KEY})
    assert r.status_code == 200, r.text
    assert r.json() == {"success": False, "report_id": None, "message": "Reports not enabled"}


def test_generate_with_user_jwt_passes_auth():
    r = _client().post(_GENERATE, json={"report_type": "WEEKLY"},
                       headers={"Authorization": f"Bearer {_user_token()}"})
    assert r.status_code == 200, r.text


def test_siblings_still_require_the_token_dependency():
    """Guard against the fix accidentally loosening the config routes."""
    r = _client().get("/api/v1/reports/config")
    assert r.status_code == 401
