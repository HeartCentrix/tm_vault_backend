"""graph-proxy: the /graph passthrough routes must require the internal API key.

GET/POST /graph/{path} and POST /graph/batch forwarded arbitrary Microsoft
Graph calls with the backup app's client-credentials token and had NO
authentication at all — any pod (or a kubectl exec) in the namespace could
act as the app against every tenant. They now carry the same
``X-Internal-Api-Key`` gate as services/delta-token: 401 without / with a
wrong key, 503 (fail closed) when INTERNAL_API_KEY is not configured, and
unchanged pass-through behaviour with the key.

graph-proxy has a hyphen in the dir name, so main.py is loaded via importlib
(same pattern as test_delta_token_invalidation.py). The outbound Microsoft
Graph call is intercepted by a fake httpx.AsyncClient; the token mint is
stubbed.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys

import httpx
import pytest

# ── Env required before any shared.config import ──────────────────────────
os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "services" / "graph-proxy" / "main.py"
_spec = importlib.util.spec_from_file_location("graph_proxy_main_under_test", _MAIN_PATH)
_gp = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["graph_proxy_main_under_test"] = _gp
try:
    _spec.loader.exec_module(_gp)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"graph-proxy module failed to import: {exc}",
        allow_module_level=True,
    )

from fastapi.testclient import TestClient  # noqa: E402

KEY = "unit-test-internal-key"
_TENANT = "11111111-1111-1111-1111-111111111111"


class _FakeGraphClient(httpx.AsyncClient):
    """Stands in for httpx.AsyncClient inside graph-proxy: real request
    building, canned send() — records every outbound attempt."""

    sent: list = []

    async def send(self, request, **kwargs):
        _FakeGraphClient.sent.append(request)
        return httpx.Response(
            200, json={"value": [], "via": "fake-graph"}, request=request
        )


@pytest.fixture
def client(monkeypatch):
    _FakeGraphClient.sent = []
    monkeypatch.setattr(_gp.settings, "INTERNAL_API_KEY", KEY)
    monkeypatch.setattr(_gp, "throttle_manager", _gp.AdaptiveThrottleManager())
    monkeypatch.setattr(_gp, "redis_client", None)

    async def _fake_token(tenant_id):
        return "fake-graph-token"

    monkeypatch.setattr(_gp, "get_graph_token", _fake_token)
    monkeypatch.setattr(_gp.httpx, "AsyncClient", _FakeGraphClient)
    # No `with`: the startup hook (Redis) must not run.
    return TestClient(_gp.app)


_BATCH = {"requests": [{"id": "1", "method": "GET", "url": "/users?$top=1"}]}


# ── 1. No key → 401, and Microsoft Graph is never reached ─────────────────


@pytest.mark.parametrize(
    "method,url,body",
    [
        ("GET", f"/graph/users?tenant_id={_TENANT}", None),
        ("POST", f"/graph/users?tenant_id={_TENANT}", {"displayName": "x"}),
        ("POST", f"/graph/batch?tenant_id={_TENANT}", _BATCH),
    ],
)
def test_rejects_request_without_internal_key(client, method, url, body):
    r = client.request(method, url, json=body)

    assert r.status_code == 401, r.text
    assert _FakeGraphClient.sent == [], "request reached Graph without the internal key"


@pytest.mark.parametrize(
    "method,url,body",
    [
        ("GET", f"/graph/users?tenant_id={_TENANT}", None),
        ("POST", f"/graph/users?tenant_id={_TENANT}", {"displayName": "x"}),
        ("POST", f"/graph/batch?tenant_id={_TENANT}", _BATCH),
    ],
)
def test_rejects_wrong_internal_key(client, method, url, body):
    r = client.request(method, url, json=body, headers={"X-Internal-Api-Key": "not-the-key"})

    assert r.status_code == 401, r.text
    assert _FakeGraphClient.sent == []


def test_unconfigured_key_fails_closed(client, monkeypatch):
    """INTERNAL_API_KEY unset is a misconfiguration: refuse (503), never
    fall open — same contract as delta-token."""
    monkeypatch.setattr(_gp.settings, "INTERNAL_API_KEY", "")

    r = client.get(f"/graph/users?tenant_id={_TENANT}", headers={"X-Internal-Api-Key": KEY})

    assert r.status_code == 503, r.text
    assert _FakeGraphClient.sent == []


# ── 2. With the key the pass-through is unchanged ─────────────────────────


def test_get_passthrough_reached_with_key(client):
    r = client.get(f"/graph/users?tenant_id={_TENANT}", headers={"X-Internal-Api-Key": KEY})

    assert r.status_code == 200, r.text
    assert r.json()["via"] == "fake-graph"
    assert len(_FakeGraphClient.sent) == 1
    sent = _FakeGraphClient.sent[0]
    assert sent.method == "GET"
    assert str(sent.url) == "https://graph.microsoft.com/v1.0/users"
    assert sent.headers["Authorization"] == "Bearer fake-graph-token"


def test_post_passthrough_reached_with_key(client):
    r = client.post(
        f"/graph/users?tenant_id={_TENANT}",
        json={"displayName": "x"},
        headers={"X-Internal-Api-Key": KEY},
    )

    assert r.status_code == 200, r.text
    assert len(_FakeGraphClient.sent) == 1
    sent = _FakeGraphClient.sent[0]
    assert sent.method == "POST"
    assert str(sent.url) == "https://graph.microsoft.com/v1.0/users"
    assert b'"displayName"' in sent.content


def test_batch_passthrough_reached_with_key(client):
    r = client.post(
        f"/graph/batch?tenant_id={_TENANT}",
        json=_BATCH,
        headers={"X-Internal-Api-Key": KEY},
    )

    assert r.status_code == 200, r.text
    assert len(_FakeGraphClient.sent) == 1
    sent = _FakeGraphClient.sent[0]
    assert sent.method == "POST"
    assert str(sent.url) == "https://graph.microsoft.com/v1.0/$batch"
    assert sent.headers["Authorization"] == "Bearer fake-graph-token"
    assert b'"requests"' in sent.content


def test_health_stays_public(client):
    r = client.get("/health")
    assert r.status_code == 200, r.text
