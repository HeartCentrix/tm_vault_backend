"""Regression: an expired pre-signed downloadUrl must not poison the
source share URL as permanently unreachable.

Bug: _stream_download_url treated ANY 400/401/403/404/410/423 from the
SharePoint CDN byte-download as a permanent property of the SOURCE share
URL and called _mark_url_unreachable(source_url_for_cache). But
@microsoft.graph.downloadUrl is a ~1-hour pre-signed URL: chat
attachments are bulk-resolved up front (resolve_shares_batch) and
downloaded much later behind a semaphore, so under a heavy user plus the
documented 429/503 storms the tail of the queue downloads with an EXPIRED
signature → CDN 401/403 → a perfectly healthy share URL landed in the
class-level, worker-lifetime, TTL-less _unreachable_urls cache. On the
next run resolve_share_to_drive_item / resolve_shares_batch short-circuit
to None and the backup-worker persists unreachable=True into the
DB-backed tenant URL cache (whose reads bump last_used_at, so the row
never ages out) — those attachment bytes are then silently skipped on
EVERY future backup.

Fix under test: a CDN-side 401/403 on a pre-signed download re-resolves
the share for a fresh downloadUrl (bounded) instead of caching a
negative; the unreachable cache is reserved for Graph-side RESOLVE
failures.
"""
from __future__ import annotations

import asyncio
import os

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

import httpx
import pytest

from shared.graph_client import GraphClient

SOURCE_URL = "https://contoso.sharepoint.com/:b:/g/personal/x/AbCdEf"
STALE_URL = "https://cdn.sharepoint.com/download?tempauth=EXPIRED"
FRESH_URL = "https://cdn.sharepoint.com/download?tempauth=FRESH"
PAYLOAD = b"attachment-bytes"


class _FakeStreamResp:
    def __init__(self, status, body=b"", headers=None):
        self.status_code = status
        self._body = body
        self.headers = headers or {}

    async def aread(self):
        return self._body

    async def aiter_bytes(self, _chunk=None):
        yield self._body


class _FakeStreamCtx:
    def __init__(self, resp):
        self._resp = resp

    async def __aenter__(self):
        return self._resp

    async def __aexit__(self, *exc):
        return False


class _FakeDownloadClient:
    """Answers client.stream() from a per-URL script; records URLs hit."""

    def __init__(self, by_url):
        self.by_url = by_url
        self.urls = []

    def stream(self, _method, url, headers=None):
        self.urls.append(url)
        resp = self.by_url[url]
        if isinstance(resp, list):
            resp = resp.pop(0)
        return _FakeStreamCtx(resp)


class _FakeSession:
    def __init__(self, client):
        self.client = client

    async def __aenter__(self):
        return self.client

    async def __aexit__(self, *exc):
        return False


def _bare_client(by_url):
    g = GraphClient.__new__(GraphClient)
    g.client_id = "app-1"
    fake = _FakeDownloadClient(by_url)
    g._http_session = lambda: _FakeSession(fake)
    # Class-level cache spans instances — isolate each test.
    GraphClient._unreachable_urls = {}
    return g, fake


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def test_cdn_403_reresolves_and_does_not_poison_source_url():
    """403 = expired signature. Re-resolve for a fresh downloadUrl, return
    the bytes, and leave the share URL usable."""
    g, fake = _bare_client({
        STALE_URL: _FakeStreamResp(403, b"Access denied. Invalid tempauth."),
        FRESH_URL: _FakeStreamResp(200, PAYLOAD),
    })
    resolves = {"n": 0}

    async def fake_resolve(src):
        assert src == SOURCE_URL
        resolves["n"] += 1
        return {"id": "di-1", "@microsoft.graph.downloadUrl": FRESH_URL,
                "size": len(PAYLOAD)}

    g.resolve_share_to_drive_item = fake_resolve

    out = _run(g._stream_download_url(
        STALE_URL, len(PAYLOAD), source_url_for_cache=SOURCE_URL,
    ))

    assert out == PAYLOAD, "expired signature must be recovered by re-resolve"
    assert resolves["n"] == 1
    assert fake.urls == [STALE_URL, FRESH_URL]
    assert not GraphClient._is_url_unreachable(SOURCE_URL), (
        "a CDN-side 403 must NOT mark the source share URL unreachable — "
        "that silently skips these bytes on every future backup"
    )


def test_cdn_401_reresolves_and_does_not_poison_source_url():
    g, fake = _bare_client({
        STALE_URL: _FakeStreamResp(401, b"401 Unauthorized"),
        FRESH_URL: _FakeStreamResp(200, PAYLOAD),
    })

    async def fake_resolve(_src):
        return {"id": "di-1", "@microsoft.graph.downloadUrl": FRESH_URL,
                "size": len(PAYLOAD)}

    g.resolve_share_to_drive_item = fake_resolve

    out = _run(g._stream_download_url(
        STALE_URL, len(PAYLOAD), source_url_for_cache=SOURCE_URL,
    ))

    assert out == PAYLOAD
    assert not GraphClient._is_url_unreachable(SOURCE_URL)


def test_cdn_403_refresh_is_bounded_and_still_never_poisons():
    """If even the freshly-resolved URL 403s, give up after a bounded number
    of re-resolves (no infinite loop) — but still never poison the cache, so
    the next backup gets another chance."""
    g, fake = _bare_client({
        STALE_URL: [_FakeStreamResp(403, b"denied") for _ in range(20)],
        FRESH_URL: [_FakeStreamResp(403, b"denied") for _ in range(20)],
    })
    resolves = {"n": 0}

    async def fake_resolve(_src):
        resolves["n"] += 1
        return {"id": "di-1", "@microsoft.graph.downloadUrl": FRESH_URL}

    g.resolve_share_to_drive_item = fake_resolve

    out = _run(g._stream_download_url(
        STALE_URL, 0, source_url_for_cache=SOURCE_URL,
    ))

    assert out is None
    assert 1 <= resolves["n"] <= 4, f"re-resolve must be bounded, got {resolves['n']}"
    assert not GraphClient._is_url_unreachable(SOURCE_URL)


def test_cdn_404_does_not_poison_source_url():
    """Even a CDN 404/410 describes the pre-signed URL, not the share. Graph
    is the only authority on whether the share itself is gone."""
    g, _fake = _bare_client({STALE_URL: _FakeStreamResp(404, b"not found")})

    async def _never(_src):
        raise AssertionError("404 should not trigger a re-resolve")

    g.resolve_share_to_drive_item = _never

    out = _run(g._stream_download_url(
        STALE_URL, 0, source_url_for_cache=SOURCE_URL,
    ))

    assert out is None
    assert not GraphClient._is_url_unreachable(SOURCE_URL)


def test_reresolve_that_finds_no_download_url_gives_up_without_poisoning():
    g, _fake = _bare_client({STALE_URL: _FakeStreamResp(403, b"denied")})

    async def fake_resolve(_src):
        return None  # Graph-side resolve failed / marked it itself

    g.resolve_share_to_drive_item = fake_resolve

    out = _run(g._stream_download_url(
        STALE_URL, 0, source_url_for_cache=SOURCE_URL,
    ))

    assert out is None
    # _stream_download_url itself must not add the entry; whether the share
    # is broken is resolve_share_to_drive_item's call (see test below).
    assert not GraphClient._is_url_unreachable(SOURCE_URL)


def test_cdn_403_without_source_url_still_returns_none():
    """No source URL to re-resolve from (direct driveItem download) — the
    call just fails, no crash, no cache write."""
    g, _fake = _bare_client({STALE_URL: _FakeStreamResp(403, b"denied")})

    out = _run(g._stream_download_url(STALE_URL, 0))

    assert out is None
    assert GraphClient._unreachable_urls == {}


def test_graph_side_resolve_403_still_marks_unreachable(monkeypatch):
    """The other half of the contract: a permanent 4xx from the Graph
    /shares/{id}/driveItem RESOLVE is still cached, so genuinely broken
    references keep short-circuiting."""
    g, _fake = _bare_client({})

    class _Resp:
        status_code = 403

    async def fake_get(_url, params=None):
        raise httpx.HTTPStatusError("403", request=None, response=_Resp())

    g._get = fake_get

    out = _run(g.resolve_share_to_drive_item(SOURCE_URL))

    assert out is None
    assert GraphClient._is_url_unreachable(SOURCE_URL)
