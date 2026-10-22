"""Regression: paginated Graph walks must not outlive the bearer token.

Bug (2026-07-29, ITO-9777 pilot): _get_hardened / _get_legacy minted the
bearer token ONCE before their pagination loop. A single long walk (jrue's
Online Archive: one folder paged past $skip=20700, paced by the rate
limiter) outlived the ~60-min token and died mid-walk with a hard 401 —
snapshot FAILED after 65 minutes with 0 items. _get_token() already
caches with a 5-min-early refresh; the loops just never re-consulted it.

Fix under test:
  * every page re-consults _get_token() (cached => free) unless the loop
    is riding a 429-migrated alternate-app token;
  * a 401 on a page forces one token refresh + same-page retry (covers
    migrated-token expiry), then raises if it repeats.
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

PAGE1 = {
    "value": [{"id": "i1"}, {"id": "i2"}],
    "@odata.nextLink": "https://graph.microsoft.com/beta/x?$skip=2",
}
PAGE2 = {"value": [{"id": "i3"}]}


class _FakeResp:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status
        self.headers = {}
        self.elapsed = None

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                f"Client error '{self.status_code}'",
                request=None, response=None,
            )


class _FakeClient:
    """Serves a scripted list of responses; records (url, auth) per call.

    A script entry that is an Exception instance is raised instead of
    returned (models transport-level failures like httpx.ReadError)."""

    def __init__(self, script):
        self.script = list(script)
        self.calls = []

    async def get(self, url, headers=None, params=None):
        self.calls.append((url, (headers or {}).get("Authorization")))
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    async def post(self, url, headers=None, json=None, timeout=None):
        self.calls.append((url, (headers or {}).get("Authorization")))
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


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
    """GraphClient without __init__/network, wired to a scripted fake HTTP
    client and a counting fake _get_token (tok-1, tok-2, ...)."""
    g = GraphClient.__new__(GraphClient)
    g.client_id = "app-1"
    fake = _FakeClient(script)
    g._http_session = lambda: _FakeSession(fake)
    g._effective_priority = lambda: 5
    setattr(g, "_GraphClient__policy_cached", _FakePolicy())

    counter = {"n": 0}

    async def fake_token():
        counter["n"] += 1
        return f"tok-{counter['n']}"

    g._get_token = fake_token

    async def _no_migration(_app_id):
        return None, None

    g._try_migrate_app = _no_migration

    async def _noop(*a, **k):
        return None

    monkeypatch.setattr(
        "shared.graph_rate_limiter.graph_rate_limiter.acquire", _noop)
    monkeypatch.setattr(
        "shared.multi_app_manager.multi_app_manager.acquire_app_token", _noop)
    monkeypatch.setattr(
        "shared.multi_app_manager.multi_app_manager.mark_success",
        lambda *a, **k: None)
    monkeypatch.setattr(
        "shared.multi_app_manager.multi_app_manager.mark_throttled",
        lambda *a, **k: None)
    return g, fake


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def test_hardened_pager_reconsults_token_per_page(monkeypatch):
    g, fake = _bare_client([_FakeResp(PAGE1), _FakeResp(PAGE2)], monkeypatch)

    result = _run(g._get_hardened("https://graph.microsoft.com/beta/x"))

    assert [i["id"] for i in result["value"]] == ["i1", "i2", "i3"]
    assert fake.calls[0][1] == "Bearer tok-1"
    # page 2 must carry a token minted AT page-2 time, not the page-1 one
    assert fake.calls[1][1] == "Bearer tok-2"


def test_legacy_pager_reconsults_token_per_page(monkeypatch):
    g, fake = _bare_client([_FakeResp(PAGE1), _FakeResp(PAGE2)], monkeypatch)

    result = _run(g._get_legacy("https://graph.microsoft.com/beta/x"))

    assert [i["id"] for i in result["value"]] == ["i1", "i2", "i3"]
    assert fake.calls[0][1] == "Bearer tok-1"
    assert fake.calls[1][1] == "Bearer tok-2"


def test_hardened_pager_retries_page_once_on_401(monkeypatch):
    g, fake = _bare_client(
        [_FakeResp({}, status=401), _FakeResp(PAGE1), _FakeResp(PAGE2)],
        monkeypatch,
    )

    result = _run(g._get_hardened("https://graph.microsoft.com/beta/x"))

    assert [i["id"] for i in result["value"]] == ["i1", "i2", "i3"]
    # same URL retried once with a fresh token after the 401
    assert fake.calls[0][0] == fake.calls[1][0]
    assert fake.calls[0][1] != fake.calls[1][1]


def test_hardened_pager_persistent_401_raises(monkeypatch):
    g, fake = _bare_client(
        [_FakeResp({}, status=401), _FakeResp({}, status=401)],
        monkeypatch,
    )

    with pytest.raises(httpx.HTTPStatusError):
        _run(g._get_hardened("https://graph.microsoft.com/beta/x"))


def test_hardened_pager_retries_transient_readerror(monkeypatch):
    """A dropped/stale keepalive connection (httpx.ReadError) mid-walk must
    be retried like other transients, not kill the whole walk (2026-07-29
    archive pilot run #3 died 19 min in on 'ReadError: ')."""
    g, fake = _bare_client(
        [httpx.ReadError(""), _FakeResp(PAGE1), _FakeResp(PAGE2)],
        monkeypatch,
    )

    result = _run(g._get_hardened("https://graph.microsoft.com/beta/x"))

    assert [i["id"] for i in result["value"]] == ["i1", "i2", "i3"]
    assert fake.calls[0][0] == fake.calls[1][0]  # same page retried


def test_legacy_pager_retries_transient_readerror(monkeypatch):
    async def _instant(_secs):
        return None

    monkeypatch.setattr("shared.graph_client.asyncio.sleep", _instant)
    g, fake = _bare_client(
        [httpx.ReadError(""), _FakeResp(PAGE1), _FakeResp(PAGE2)],
        monkeypatch,
    )

    result = _run(g._get_legacy("https://graph.microsoft.com/beta/x"))

    assert [i["id"] for i in result["value"]] == ["i1", "i2", "i3"]
    assert fake.calls[0][0] == fake.calls[1][0]


def test_hardened_pager_retries_504(monkeypatch):
    """Graph beta admin/exchange intermittently 504s mid-walk (2026-07-29
    archive pilot run #5 died 58 min in). MS throttling guidance says
    429/503/504 are the retryable trio — 504 must back off + retry, not
    kill the walk."""
    g, fake = _bare_client(
        [_FakeResp({}, status=504), _FakeResp(PAGE1), _FakeResp(PAGE2)],
        monkeypatch,
    )

    result = _run(g._get_hardened("https://graph.microsoft.com/beta/x"))

    assert [i["id"] for i in result["value"]] == ["i1", "i2", "i3"]
    assert fake.calls[0][0] == fake.calls[1][0]  # same page retried


def test_hardened_pager_retries_502(monkeypatch):
    g, fake = _bare_client(
        [_FakeResp({}, status=502), _FakeResp(PAGE1), _FakeResp(PAGE2)],
        monkeypatch,
    )

    result = _run(g._get_hardened("https://graph.microsoft.com/beta/x"))

    assert [i["id"] for i in result["value"]] == ["i1", "i2", "i3"]
    assert fake.calls[0][0] == fake.calls[1][0]


def test_legacy_pager_retries_504(monkeypatch):
    async def _instant(_secs):
        return None

    monkeypatch.setattr("shared.graph_client.asyncio.sleep", _instant)
    g, fake = _bare_client(
        [_FakeResp({}, status=504), _FakeResp(PAGE1), _FakeResp(PAGE2)],
        monkeypatch,
    )

    result = _run(g._get_legacy("https://graph.microsoft.com/beta/x"))

    assert [i["id"] for i in result["value"]] == ["i1", "i2", "i3"]
    assert fake.calls[0][0] == fake.calls[1][0]


def test_post_json_retries_504(monkeypatch):
    async def _instant(_secs):
        return None

    monkeypatch.setattr("shared.graph_client.asyncio.sleep", _instant)
    g, fake = _bare_client(
        [_FakeResp({}, status=504), _FakeResp({"ok": True})],
        monkeypatch,
    )

    result = _run(g._post_json("https://graph.microsoft.com/beta/x", {"a": 1}))

    assert result == {"ok": True}
    assert fake.calls[0][0] == fake.calls[1][0]


def test_legacy_pager_retries_page_once_on_401(monkeypatch):
    g, fake = _bare_client(
        [_FakeResp({}, status=401), _FakeResp(PAGE1), _FakeResp(PAGE2)],
        monkeypatch,
    )

    result = _run(g._get_legacy("https://graph.microsoft.com/beta/x"))

    assert [i["id"] for i in result["value"]] == ["i1", "i2", "i3"]
    assert fake.calls[0][0] == fake.calls[1][0]
    assert fake.calls[0][1] != fake.calls[1][1]


# ─── streaming pagers (_iter_pages_*) — same token-lifetime + 401 fixes ────
# The streaming paths originally minted the token ONCE per stream with no
# 401 handling, so any delta drain / history walk longer than the token's
# remaining life deterministically aborted mid-stream (callers kept the
# partial page set and never persisted the deltaLink).

DELTA_PAGE = {
    "value": [{"id": "i3"}],
    "@odata.deltaLink": "https://graph.microsoft.com/v1.0/me/calendarView/delta?$deltatoken=a1",
}


async def _drain(gen):
    return [p async for p in gen]


def test_iter_pages_hardened_reconsults_token_per_page(monkeypatch):
    g, fake = _bare_client([_FakeResp(PAGE1), _FakeResp(PAGE2)], monkeypatch)

    pages = _run(_drain(g._iter_pages_hardened("https://graph.microsoft.com/beta/x")))

    assert [i["id"] for p in pages for i in p.get("value", [])] == ["i1", "i2", "i3"]
    assert fake.calls[0][1] == "Bearer tok-1"
    # page 2 must carry a token minted AT page-2 time, not the page-1 one
    assert fake.calls[1][1] == "Bearer tok-2"


def test_iter_pages_legacy_reconsults_token_per_page(monkeypatch):
    g, fake = _bare_client([_FakeResp(PAGE1), _FakeResp(PAGE2)], monkeypatch)

    pages = _run(_drain(g._iter_pages_legacy("https://graph.microsoft.com/beta/x")))

    assert [i["id"] for p in pages for i in p.get("value", [])] == ["i1", "i2", "i3"]
    assert fake.calls[0][1] == "Bearer tok-1"
    assert fake.calls[1][1] == "Bearer tok-2"


def test_iter_pages_hardened_retries_page_once_on_401(monkeypatch):
    g, fake = _bare_client(
        [_FakeResp({}, status=401), _FakeResp(PAGE1), _FakeResp(PAGE2)],
        monkeypatch,
    )

    pages = _run(_drain(g._iter_pages_hardened("https://graph.microsoft.com/beta/x")))

    assert [i["id"] for p in pages for i in p.get("value", [])] == ["i1", "i2", "i3"]
    # same URL retried once with a fresh token after the 401
    assert fake.calls[0][0] == fake.calls[1][0]
    assert fake.calls[0][1] != fake.calls[1][1]


def test_iter_pages_legacy_retries_page_once_on_401(monkeypatch):
    g, fake = _bare_client(
        [_FakeResp({}, status=401), _FakeResp(PAGE1), _FakeResp(PAGE2)],
        monkeypatch,
    )

    pages = _run(_drain(g._iter_pages_legacy("https://graph.microsoft.com/beta/x")))

    assert [i["id"] for p in pages for i in p.get("value", [])] == ["i1", "i2", "i3"]
    assert fake.calls[0][0] == fake.calls[1][0]
    assert fake.calls[0][1] != fake.calls[1][1]


def test_iter_pages_hardened_persistent_401_raises(monkeypatch):
    g, fake = _bare_client(
        [_FakeResp({}, status=401), _FakeResp({}, status=401)],
        monkeypatch,
    )

    with pytest.raises(httpx.HTTPStatusError):
        _run(_drain(g._iter_pages_hardened("https://graph.microsoft.com/beta/x")))


def test_iter_pages_delta_cursor_flows_per_stream_not_on_client(monkeypatch):
    """Regression: the terminal @odata.deltaLink used to be stashed on the
    SHARED GraphClient instance (clients are cached per tenant+resource_type
    and run concurrent jobs), so two users' streams clobbered each other's
    cursor — user A could persist user B's deltaLink as its own resume
    point (cross-user contamination = silent wrong data + missed backups).
    The cursor must ride the yielded terminal page and leave NO state on
    the client."""
    for pager_name in ("_iter_pages_hardened", "_iter_pages_legacy"):
        g, fake = _bare_client([_FakeResp(PAGE1), _FakeResp(DELTA_PAGE)], monkeypatch)

        pages = _run(_drain(getattr(g, pager_name)("https://graph.microsoft.com/beta/x")))

        assert pages[-1]["@odata.deltaLink"].endswith("$deltatoken=a1"), pager_name
        assert getattr(g, "_last_delta_link", None) is None, pager_name
