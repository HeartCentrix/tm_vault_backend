"""``GraphClient._iter_pages`` must let callers add request headers that ride
EVERY page of a streaming walk — first request and every nextLink alike.

Why: on ``/mailFolders/{id}/messages/delta`` the page size cannot be set
with ``$top`` (Graph treats ``$top`` there as a cap on the whole delta round
and then emits a deltaLink — see tests/workers/test_mail_delta_no_top.py).
The only page-size control is ``Prefer: odata.maxpagesize=N``, and Graph
applies that preference per request, so a header sent only on the first
call would drop the walk back to 10-item pages from page 2 onward.

Both iterator implementations (legacy kill-switch path and the hardened
path) are covered; the public ``_iter_pages`` dispatcher must forward the
argument to whichever one is active.
"""
from __future__ import annotations

import asyncio
import inspect
import os

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

import pytest

from shared.graph_client import GraphClient

PAGE1 = {
    "value": [{"id": "i1"}],
    "@odata.nextLink": "https://graph.microsoft.com/v1.0/users/u/mailFolders/f/messages/delta?$skiptoken=s1",
}
PAGE2 = {
    "value": [{"id": "i2"}],
    "@odata.deltaLink": "https://graph.microsoft.com/v1.0/users/u/mailFolders/f/messages/delta?$deltatoken=d1",
}
PREFER = {"Prefer": "odata.maxpagesize=999"}


class _FakeResp:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status
        self.headers = {}
        self.elapsed = None

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


class _FakeClient:
    """Serves scripted responses; records the FULL header dict per call."""

    def __init__(self, script):
        self.script = list(script)
        self.calls = []

    async def get(self, url, headers=None, params=None):
        self.calls.append({"url": url, "headers": dict(headers or {}), "params": params})
        return self.script.pop(0)


class _FakeSession:
    def __init__(self, client):
        self.client = client

    async def __aenter__(self):
        return self.client

    async def __aexit__(self, *exc):
        return False


class _FakeBucket:
    async def acquire(self, priority=None):
        return None


class _FakeAction:
    exhausted = False
    sleep_seconds = 0.0


class _FakePolicy:
    stream_bucket = _FakeBucket()

    def reset_on_success(self):
        return None

    def decide_transient_error(self):
        return _FakeAction()

    def decide(self, status_code=None, retry_after=None):
        return _FakeAction()


def _bare_client(script, monkeypatch):
    g = GraphClient.__new__(GraphClient)
    g.client_id = "app-1"
    fake = _FakeClient(script)
    g._http_session = lambda: _FakeSession(fake)
    g._effective_priority = lambda: 5
    monkeypatch.setattr(GraphClient, "_policy", property(lambda _s: _FakePolicy()))

    async def fake_token():
        return "tok"

    g._get_token = fake_token

    async def _no_migration(_app_id):
        return None, None

    g._try_migrate_app = _no_migration

    async def _noop(*a, **k):
        return None

    monkeypatch.setattr("shared.graph_rate_limiter.graph_rate_limiter.acquire", _noop)
    monkeypatch.setattr("shared.multi_app_manager.multi_app_manager.acquire_app_token", _noop)
    monkeypatch.setattr("shared.multi_app_manager.multi_app_manager.mark_success", lambda *a, **k: None)
    monkeypatch.setattr("shared.multi_app_manager.multi_app_manager.mark_throttled", lambda *a, **k: None)
    monkeypatch.setattr("shared.multi_app_manager.multi_app_manager.is_app_throttled", lambda *a, **k: False)
    return g, fake


def _drain(agen):
    async def _go():
        return [p async for p in agen]

    return asyncio.run(_go())


def test_iter_pages_accepts_headers_keyword():
    params = inspect.signature(GraphClient._iter_pages).parameters
    assert "headers" in params, "_iter_pages(url, params=None, headers=None) expected"


def test_legacy_iter_sends_extra_headers_on_every_page(monkeypatch):
    g, fake = _bare_client([_FakeResp(PAGE1), _FakeResp(PAGE2)], monkeypatch)

    pages = _drain(g._iter_pages_legacy(
        "https://graph.microsoft.com/v1.0/users/u/mailFolders/f/messages/delta",
        params={"$select": "id"},
        headers=PREFER,
    ))

    assert [i["id"] for p in pages for i in p["value"]] == ["i1", "i2"]
    assert len(fake.calls) == 2
    for call in fake.calls:
        assert call["headers"].get("Prefer") == "odata.maxpagesize=999", call
        assert call["headers"].get("Authorization") == "Bearer tok", call


def test_hardened_iter_sends_extra_headers_on_every_page(monkeypatch):
    g, fake = _bare_client([_FakeResp(PAGE1), _FakeResp(PAGE2)], monkeypatch)

    pages = _drain(g._iter_pages_hardened(
        "https://graph.microsoft.com/v1.0/users/u/mailFolders/f/messages/delta",
        params={"$select": "id"},
        headers=PREFER,
    ))

    assert [i["id"] for p in pages for i in p["value"]] == ["i1", "i2"]
    assert len(fake.calls) == 2
    for call in fake.calls:
        assert call["headers"].get("Prefer") == "odata.maxpagesize=999", call


def test_extra_headers_cannot_clobber_authorization(monkeypatch):
    g, fake = _bare_client([_FakeResp(PAGE2)], monkeypatch)

    _drain(g._iter_pages_legacy(
        "https://graph.microsoft.com/v1.0/x",
        headers={"Authorization": "Bearer attacker", "Prefer": "odata.maxpagesize=5"},
    ))

    assert fake.calls[0]["headers"]["Authorization"] == "Bearer tok"
    assert fake.calls[0]["headers"]["Prefer"] == "odata.maxpagesize=5"


def test_dispatcher_forwards_headers_to_active_path(monkeypatch):
    g, fake = _bare_client([_FakeResp(PAGE2)], monkeypatch)
    monkeypatch.setattr("shared.config.settings.GRAPH_HARDENING_ENABLED", True, raising=False)

    _drain(g._iter_pages("https://graph.microsoft.com/v1.0/x", headers=PREFER))

    assert fake.calls[0]["headers"].get("Prefer") == "odata.maxpagesize=999"
