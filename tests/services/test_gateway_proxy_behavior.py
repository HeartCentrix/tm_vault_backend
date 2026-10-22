"""api-gateway proxy_request behavior pins.

Four production failure modes, all in the single proxy hop every browser
request takes (nginx -> api-gateway -> service):

1. Repeated Set-Cookie headers must be forwarded as separate headers.
   httpx's Headers.items() comma-joins duplicates, so auth-service's three
   Set-Cookie headers (access_token / refresh_token / expires breadcrumb)
   collapsed into one — browsers stored only access_token and every session
   died unrefreshable at access-token expiry.

2. The upstream request must carry a real timeout. Passing timeout=None to
   httpx does not mean "client default" — it disables all timeouts, so one
   hung downstream pinned a pooled connection forever.

3. Non-idempotent methods must NOT be retried on RemoteProtocolError /
   ReadTimeout: the downstream may have already committed (job row written,
   pod rolled) and a replayed POST duplicates restore/backup/export jobs.

4. Download/export bodies must be streamed through, not buffered — a
   multi-GB export artifact OOMKilled the 512Mi gateway pod.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys

import httpx
import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "api-gateway" / "main.py"
_spec = importlib.util.spec_from_file_location("gateway_proxy_under_test", _MAIN_PATH)
_gw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["gateway_proxy_under_test"] = _gw
try:
    _spec.loader.exec_module(_gw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"api-gateway module failed to import: {exc}",
        allow_module_level=True,
    )


class _UpstreamBody(httpx.AsyncByteStream):
    """Async byte stream so httpx.Response supports aiter_raw()."""

    def __init__(self, chunks):
        self._chunks = chunks

    async def __aiter__(self):
        for c in self._chunks:
            yield c

    async def aclose(self):
        pass


class _FakeClient(httpx.AsyncClient):
    """Real build_request, canned send() — records every attempt."""

    def __init__(self, responder):
        super().__init__(timeout=_gw.GATEWAY_TIMEOUT, limits=_gw.GATEWAY_LIMITS)
        self._responder = responder
        self.sent: list[httpx.Request] = []

    async def send(self, request, **kwargs):
        self.sent.append(request)
        result = self._responder(request)
        if isinstance(result, Exception):
            raise result
        return result


def _client_with(responder) -> tuple[TestClient, _FakeClient]:
    fake = _FakeClient(responder)
    _gw.app.state.http_client = fake
    return TestClient(_gw.app, raise_server_exceptions=False), fake


def test_multiple_set_cookie_headers_forwarded_separately():
    cookies = [
        "access_token=A; HttpOnly; Path=/; SameSite=strict",
        "refresh_token=R; HttpOnly; Path=/api/v1/auth; SameSite=strict",
        "access_token_expires_at=1754300000; Path=/",
    ]
    def responder(req):
        return httpx.Response(
            200,
            headers=[("content-type", "application/json")]
                    + [("set-cookie", c) for c in cookies],
            stream=_UpstreamBody([b"{}"]),
        )
    tc, _ = _client_with(responder)
    r = tc.post("/api/v1/auth/callback", json={"code": "x"})
    assert r.status_code == 200
    assert r.headers.get_list("set-cookie") == cookies, (
        "Set-Cookie headers were collapsed — refresh_token never reaches "
        "the browser and sessions die at access-token expiry"
    )


def test_upstream_request_has_real_timeout():
    def responder(req):
        return httpx.Response(
            200,
            headers=[("content-type", "application/json")],
            stream=_UpstreamBody([b"{}"]),
        )
    tc, fake = _client_with(responder)
    r = tc.get("/api/v1/jobs")
    assert r.status_code == 200
    timeout = fake.sent[0].extensions.get("timeout")
    assert timeout == _gw.GATEWAY_TIMEOUT.as_dict()
    assert timeout["read"] is not None, (
        "proxy passed timeout=None to httpx — all timeouts disabled, hung "
        "downstreams pin pooled connections forever"
    )


def test_post_not_retried_on_remote_protocol_error():
    def responder(req):
        return httpx.RemoteProtocolError("server disconnected")
    tc, fake = _client_with(responder)
    r = tc.post("/api/v1/jobs/restore", json={"snapshot_id": "s1"})
    assert r.status_code == 503
    assert len(fake.sent) == 1, (
        "POST was replayed after RemoteProtocolError — the downstream may "
        "already have committed the job, so this duplicates a restore"
    )


def test_post_not_retried_on_read_timeout():
    def responder(req):
        return httpx.ReadTimeout("read timed out")
    tc, fake = _client_with(responder)
    r = tc.post("/api/v1/backups/trigger", json={})
    assert r.status_code == 504
    assert len(fake.sent) == 1


def test_get_still_retried_on_remote_protocol_error(monkeypatch):
    async def _no_sleep(_):
        return None
    import asyncio as _asyncio
    monkeypatch.setattr(_asyncio, "sleep", _no_sleep)

    def responder(req):
        return httpx.RemoteProtocolError("server disconnected")
    tc, fake = _client_with(responder)
    r = tc.get("/api/v1/jobs")
    assert r.status_code == 503
    assert len(fake.sent) == 3, "GET (idempotent) should keep its retries"


def test_post_still_retried_on_connect_error(monkeypatch):
    """ConnectError means the request never reached the service — replaying
    a POST there is safe and must keep working."""
    async def _no_sleep(_):
        return None
    import asyncio as _asyncio
    monkeypatch.setattr(_asyncio, "sleep", _no_sleep)

    def responder(req):
        return httpx.ConnectError("connection refused")
    tc, fake = _client_with(responder)
    r = tc.post("/api/v1/jobs/restore", json={})
    assert r.status_code == 503
    assert len(fake.sent) == 3


def test_download_body_streamed_through_intact():
    chunks = [b"PK\x03\x04", b"zip-payload-part-1", b"zip-payload-part-2"]
    disposition = 'attachment; filename="export.zip"'
    def responder(req):
        return httpx.Response(
            200,
            headers=[
                ("content-type", "application/zip"),
                ("content-disposition", disposition),
                ("content-length", str(sum(len(c) for c in chunks))),
            ],
            stream=_UpstreamBody(chunks),
        )
    tc, _ = _client_with(responder)
    r = tc.get("/api/v1/exports/job-1/download")
    assert r.status_code == 200
    assert r.content == b"".join(chunks)
    assert r.headers["content-disposition"] == disposition
    # Content-Length passes through so the browser can show download progress.
    assert r.headers.get("content-length") == str(len(r.content))


@pytest.mark.parametrize("method,path", [
    ("get", "/api/v1/encryption/config"),
    ("put", "/api/v1/encryption/config"),
    ("post", "/api/v1/encryption/rewrap"),
])
def test_encryption_routes_reach_tenant_service(method, path):
    def responder(req):
        return httpx.Response(
            200, headers=[("content-type", "application/json")],
            stream=_UpstreamBody([b"{}"]),
        )
    tc, fake = _client_with(responder)
    response = getattr(tc, method)(path)
    assert response.status_code == 200
    assert len(fake.sent) == 1
    assert str(fake.sent[0].url) == _gw.SERVICES["tenant"] + path
