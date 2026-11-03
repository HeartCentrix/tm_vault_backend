"""alert-service POST /api/v1/alerts/notify/export-completed auth.

Defect: the route accepted only a user JWT (Depends(get_current_user_from_token)),
but its one caller is the restore-worker, which holds no user session and
posted with no headers at all — so every export-completed notification was
answered 401 and silently dropped (the worker swallowed the response).

Fix: the route takes shared.auth_enforcement.require_principal — EITHER a
valid user JWT OR the X-Internal-Api-Key shared secret (the machine path
audit-service / delta-token already use). Anonymous callers are still 401.

alert-service has a hyphen in its dir name, so main.py is loaded via
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

_SVC_DIR = _REPO_ROOT / "services" / "alert-service"
_spec = importlib.util.spec_from_file_location("alert_main_under_test_notify", _SVC_DIR / "main.py")
_al = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["alert_main_under_test_notify"] = _al
assert _spec and _spec.loader
_spec.loader.exec_module(_al)  # type: ignore[union-attr]

from shared.security import create_access_token  # noqa: E402

_PATH = "/api/v1/alerts/notify/export-completed"
_BODY = {
    "user_email": "someone@taylormorrison.com",
    "user_display_name": "Someone",
    "job_id": str(uuid.uuid4()),
    "status": "COMPLETED",
    "download_url": "https://vault.example/recovery?job=x",
    "exported_count": 12,
    "failed_count": 0,
    "duration_seconds": 75,
    "size_bytes": 0,
}
_KEY = "unit-test-internal-key"


@pytest.fixture(autouse=True)
def _internal_key(monkeypatch):
    monkeypatch.setenv("INTERNAL_API_KEY", _KEY)
    _al.app.dependency_overrides.clear()
    yield
    _al.app.dependency_overrides.clear()


def _client() -> TestClient:
    return TestClient(_al.app)


def _user_token(roles=("USER",)) -> str:
    return create_access_token({
        "sub": str(uuid.uuid4()),
        "email": "member@taylormorrison.com",
        "roles": list(roles),
        "orgId": str(uuid.uuid4()),
        "tenantIds": [],
    })


def test_anonymous_post_is_401():
    r = _client().post(_PATH, json=_BODY)
    assert r.status_code == 401


def test_wrong_internal_key_is_401():
    r = _client().post(_PATH, json=_BODY, headers={"X-Internal-Api-Key": "not-the-key"})
    assert r.status_code == 401


def test_garbage_bearer_is_401():
    r = _client().post(_PATH, json=_BODY, headers={"Authorization": "Bearer not-a-jwt"})
    assert r.status_code == 401


def test_internal_key_is_accepted():
    """The restore-worker path: machine-to-machine with the shared secret."""
    r = _client().post(_PATH, json=_BODY, headers={"X-Internal-Api-Key": _KEY})
    assert r.status_code == 202, r.text
    assert r.json() == {"queued": True}


def test_user_jwt_is_still_accepted():
    r = _client().post(_PATH, json=_BODY, headers={"Authorization": f"Bearer {_user_token()}"})
    assert r.status_code == 202, r.text


def test_internal_key_unset_does_not_open_the_route(monkeypatch):
    """A pod without INTERNAL_API_KEY must not become an anonymous endpoint."""
    monkeypatch.setenv("INTERNAL_API_KEY", "")
    r = _client().post(_PATH, json=_BODY, headers={"X-Internal-Api-Key": ""})
    assert r.status_code == 401
