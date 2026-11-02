"""OAuth provider failures must fail closed without unhandled ASGI errors."""
from __future__ import annotations

import importlib.util
import json
import logging
import os
from pathlib import Path
import sys
from uuid import uuid4

import httpx
import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test-only-microsoft-oauth-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location(
    "auth_oauth_errors_under_test", ROOT / "services/auth-service/main.py"
)
auth = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = auth
spec.loader.exec_module(auth)

PROFILE = {
    "id": "058f009c-156b-4a80-b743-0753d398bdba",
    "displayName": "Test User",
    "mail": "test@example.invalid",
    "userPrincipalName": "test@example.invalid",
}
SENSITIVE = "sensitive-provider-content-do-not-log"


@pytest.fixture
def harness(monkeypatch):
    original_client = httpx.AsyncClient
    requests = []
    events = []

    class NoDatabase:
        async def execute(self, *_args):
            pytest.fail("rejected OAuth request accessed the user database")

    db = NoDatabase()

    async def get_db():
        yield db

    auth.app.dependency_overrides[auth.get_db] = get_db
    if hasattr(auth, "emit_audit_event_bg"):
        monkeypatch.setattr(auth, "emit_audit_event_bg", lambda **event: events.append(event))
    monkeypatch.setattr(auth.settings, "GRAPH_APPS", [{
        "index": 1, "client_id": "test-client", "client_secret": SENSITIVE,
        "tenant_id": "test-tenant",
    }])

    def install(*, token=None, profile=None, failure=None):
        def handler(request):
            requests.append(request)
            if failure is not None:
                raise failure("provider transport failed " + SENSITIVE, request=request)
            if request.method == "POST":
                status, body = token or (200, {"access_token": "test-access-token"})
            else:
                status, body = profile or (200, PROFILE)
            if isinstance(body, str):
                return httpx.Response(status, text=body)
            return httpx.Response(status, json=body)

        monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: original_client(
            **kwargs, transport=httpx.MockTransport(handler)
        ))

    yield TestClient(auth.app), install, requests, events
    auth.app.dependency_overrides.clear()


@pytest.mark.parametrize("token,status", [
    ((401, {"error": "invalid_client", "error_codes": [7000222], "error_description": SENSITIVE}), 503),
    ((400, {"error": "invalid_grant", "error_description": SENSITIVE}), 401),
    ((400, {"error": "access_denied"}), 401),
    ((429, {"error": "temporarily_unavailable"}), 503),
    ((500, {"error": "server_error"}), 503),
    ((401, "not-json " + SENSITIVE), 502),
    ((400, {"error": SENSITIVE + "\nforged-log"}), 502),
    ((200, {}), 502),
    ((200, {"access_token": None}), 502),
    ((200, "not-json " + SENSITIVE), 502),
])
def test_token_rejection_is_controlled_and_never_issues_session(harness, token, status, caplog):
    client, install, requests, events = harness
    install(token=token)
    with caplog.at_level(logging.WARNING):
        result = client.post("/api/v1/auth/callback", json={"code": SENSITIVE})
    assert result.status_code == status
    assert isinstance(result.json()["detail"], str)
    assert "set-cookie" not in result.headers
    assert len(requests) == 1
    assert SENSITIVE not in result.text + caplog.text + json.dumps(events)
    if token[1] == {"error": "invalid_client", "error_codes": [7000222], "error_description": SENSITIVE}:
        assert "7000222" in caplog.text
        assert "code_exchange" in caplog.text


@pytest.mark.parametrize("profile", [
    (401, {"error": {"message": SENSITIVE}}),
    (403, {"error": {"message": SENSITIVE}}),
    (200, []),
    (200, {}),
    (200, "not-json " + SENSITIVE),
    (200, {**PROFILE, "mail": None, "userPrincipalName": None}),
])
def test_profile_failure_never_creates_user_or_session(harness, profile, caplog):
    client, install, requests, events = harness
    install(profile=profile)
    with caplog.at_level(logging.WARNING):
        result = client.post("/api/v1/auth/callback", json={"code": SENSITIVE})
    assert result.status_code == 502
    assert "set-cookie" not in result.headers
    assert len(requests) == 2
    assert SENSITIVE not in result.text + caplog.text + json.dumps(events)


@pytest.mark.parametrize("failure,status", [(httpx.ReadTimeout, 504), (httpx.ConnectError, 503)])
def test_transport_failure_returns_retry_message_without_exception(harness, failure, status, caplog):
    client, install, requests, events = harness
    install(failure=failure)
    with caplog.at_level(logging.WARNING):
        result = client.post("/api/v1/auth/callback", json={"code": SENSITIVE})
    assert result.status_code == status
    assert "set-cookie" not in result.headers
    assert len(requests) == 1
    assert SENSITIVE not in result.text + caplog.text + json.dumps(events)


def test_success_preserves_login_claims_and_secure_cookies(harness, monkeypatch):
    from shared.models import PlatformUser
    from shared.security import decode_token

    client, install, requests, events = harness
    install()
    user = PlatformUser(id=uuid4(), org_id=uuid4(), email=PROFILE["mail"], name=PROFILE["displayName"])

    class ExistingUserDatabase:
        async def execute(self, _stmt):
            return self

        def scalar_one_or_none(self):
            return user

        async def flush(self):
            pass

    async def get_db():
        yield ExistingUserDatabase()

    claims = {"sub": str(user.id), "email": user.email, "roles": ["USER"],
              "orgId": str(user.org_id), "tenantIds": [str(uuid4())]}

    async def load_claims(_db, _user_id):
        return claims

    monkeypatch.setattr(auth, "_load_token_claims_from_db", load_claims)
    auth.app.dependency_overrides[auth.get_db] = get_db
    result = client.post("/api/v1/auth/callback", json={"code": "test-code"})
    assert result.status_code == 200, result.text
    body = result.json()
    assert body["user"]["email"] == PROFILE["mail"]
    assert body["user"]["roles"] == ["USER"]
    assert decode_token(body["accessToken"])["tenantIds"] == claims["tenantIds"]
    cookies = result.headers.get_list("set-cookie")
    assert any(c.startswith("access_token=") and "HttpOnly" in c for c in cookies)
    assert any(c.startswith("refresh_token=") and "HttpOnly" in c for c in cookies)
    assert len(requests) == 2
    assert requests[1].headers["Authorization"] == "Bearer test-access-token"
