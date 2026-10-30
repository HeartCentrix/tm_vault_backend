"""Regression: STREAMING Graph walks must not outlive the bearer token.

Sibling of test_graph_paged_token_refresh.py. That bug was fixed for the
non-streaming _get_legacy / _get_hardened (per-page mint + one-shot 401
retry) but the streaming _iter_pages_legacy / _iter_pages_hardened were
missed: both minted the token ONCE before the page loop and had no 401
branch at all, so an aged-out token hit resp.raise_for_status() and killed
the whole stream.

Real trigger: a USER_CHATS baseline (628k messages at $top=50 ≈ 12.5k
pages, plus 300s Retry-After sleeps that burn token life with zero
re-mint). _get_token() caches with a 5-min-early expiry, so a stream can
begin with as little as ~5 minutes of real validity. Amplifier: the
backup-worker misreads ANY exception raised while a delta_token was
supplied as "delta token rejected" and restarts a FULL export.

Fix under test:
  * every page re-consults _get_token() (cached => free) unless the stream
    is riding a 429-migrated alternate-app token;
  * a 401 on a page forces one token refresh + same-page retry, then
    raises if it repeats (fail loudly, never silently truncate a stream).
Also covered: _iter_pages_hardened's retry classification aligned with
_get_hardened (502/504 retryable, all httpx.TransportError transients).
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

from shared.graph_client import GraphClient, GraphRetryExhaustedError

PAGE1 = {
    "value": [{"id": "i1"}, {"id": "i2"}],
    "@odata.nextLink": "https://graph.microsoft.com/beta/x?$skip=2",
}
PAGE2 = {"value": [{"id": "i3"}], "@odata.deltaLink": "https://graph.microsoft.com/beta/x?$deltatoken=d1"}


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


def _bare_client(script, monkeypatch, policy=None):
    """GraphClient without __init__/network, wired to a scripted fake HTTP
    client and a counting fake _get_token (tok-1, tok-2, ...)."""
    g = GraphClient.__new__(GraphClient)
    g.client_id = "app-1"
    fake = _FakeClient(script)
    g._http_session = lambda: _FakeSession(fake)
    g._effective_priority = lambda: 5
    # _policy is a property; per-instance setattr of the mangled cache attr
    # does NOT inject (the property's cache read uses an unmangled literal),
    # so patch the property itself or these tests silently run against the
    # real settings-driven RateLimitPolicy + its real pacing bucket.
    pol = policy or _FakePolicy()
    monkeypatch.setattr(GraphClient, "_policy", property(lambda _s: pol))

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
    monkeypatch.setattr(
        "shared.multi_app_manager.multi_app_manager.is_app_throttled",
        lambda *a, **k: False)
    return g, fake


def _drain(agen):
    async def _go():
        return [p async for p in agen]

    return asyncio.new_event_loop().run_until_complete(_go())


def _instant_sleep(monkeypatch):
    async def _nosleep(_secs):
        return None

    monkeypatch.setattr("shared.graph_client.asyncio.sleep", _nosleep)


def test_hardened_iter_reconsults_token_per_page(monkeypatch):
    g, fake = _bare_client([_FakeResp(PAGE1), _FakeResp(PAGE2)], monkeypatch)

    pages = _drain(g._iter_pages_hardened("https://graph.microsoft.com/beta/x"))

    assert [i["id"] for p in pages for i in p["value"]] == ["i1", "i2", "i3"]
    assert fake.calls[0][1] == "Bearer tok-1"
    # page 2 must carry a token minted AT page-2 time, not the page-1 one
    assert fake.calls[1][1] == "Bearer tok-2"


def test_legacy_iter_reconsults_token_per_page(monkeypatch):
    g, fake = _bare_client([_FakeResp(PAGE1), _FakeResp(PAGE2)], monkeypatch)

    pages = _drain(g._iter_pages_legacy("https://graph.microsoft.com/beta/x"))

    assert [i["id"] for p in pages for i in p["value"]] == ["i1", "i2", "i3"]
    assert fake.calls[0][1] == "Bearer tok-1"
    assert fake.calls[1][1] == "Bearer tok-2"


def test_hardened_iter_retries_page_once_on_401(monkeypatch):
    g, fake = _bare_client(
        [_FakeResp(PAGE1), _FakeResp({}, status=401), _FakeResp(PAGE2)],
        monkeypatch,
    )

    pages = _drain(g._iter_pages_hardened("https://graph.microsoft.com/beta/x"))

    # the 401 page must be retried, not kill the stream
    assert [i["id"] for p in pages for i in p["value"]] == ["i1", "i2", "i3"]
    assert fake.calls[1][0] == fake.calls[2][0]  # same page URL retried
    assert fake.calls[1][1] != fake.calls[2][1]  # with a fresh token
    # The cursor rides the terminal PAGE, not the shared client instance:
    # GraphClients are cached per (tenant, resource_type) and run concurrent
    # jobs, so a `self._last_delta_link` stash let user A persist user B's
    # deltaLink (cross-user contamination). See
    # test_graph_paged_token_refresh.py::
    # test_iter_pages_delta_cursor_flows_per_stream_not_on_client.
    assert pages[-1]["@odata.deltaLink"] == PAGE2["@odata.deltaLink"]


def test_legacy_iter_retries_page_once_on_401(monkeypatch):
    g, fake = _bare_client(
        [_FakeResp(PAGE1), _FakeResp({}, status=401), _FakeResp(PAGE2)],
        monkeypatch,
    )

    pages = _drain(g._iter_pages_legacy("https://graph.microsoft.com/beta/x"))

    assert [i["id"] for p in pages for i in p["value"]] == ["i1", "i2", "i3"]
    assert fake.calls[1][0] == fake.calls[2][0]
    assert fake.calls[1][1] != fake.calls[2][1]
    # Cursor rides the terminal page — see the hardened twin above.
    assert pages[-1]["@odata.deltaLink"] == PAGE2["@odata.deltaLink"]


def test_hardened_iter_persistent_401_raises(monkeypatch):
    """A genuinely revoked credential must FAIL LOUDLY, not end the stream
    early and let the caller record a truncated backup as complete."""
    g, _fake = _bare_client(
        [_FakeResp({}, status=401), _FakeResp({}, status=401)],
        monkeypatch,
    )

    with pytest.raises(httpx.HTTPStatusError):
        _drain(g._iter_pages_hardened("https://graph.microsoft.com/beta/x"))


def test_legacy_iter_persistent_401_raises(monkeypatch):
    g, _fake = _bare_client(
        [_FakeResp({}, status=401), _FakeResp({}, status=401)],
        monkeypatch,
    )

    with pytest.raises(httpx.HTTPStatusError):
        _drain(g._iter_pages_legacy("https://graph.microsoft.com/beta/x"))


def test_hardened_iter_401_after_migration_remints_own_token(monkeypatch):
    """While riding a 429-migrated alternate-app token the pager must NOT
    re-mint per page (that would defeat the migration) — but a 401 on that
    borrowed token has to fall back to a fresh own-app mint."""
    _instant_sleep(monkeypatch)
    g, fake = _bare_client(
        [_FakeResp({}, status=429), _FakeResp({}, status=401), _FakeResp(PAGE2)],
        monkeypatch,
    )

    async def _migrate(_app_id):
        return "migrated-tok", "app-2"

    g._try_migrate_app = _migrate

    pages = _drain(g._iter_pages_hardened("https://graph.microsoft.com/beta/x"))

    assert [i["id"] for p in pages for i in p["value"]] == ["i3"]
    assert fake.calls[1][1] == "Bearer migrated-tok"  # retried on alt app
    assert fake.calls[2][1].startswith("Bearer tok-")  # 401 -> own fresh mint


def test_hardened_iter_retries_502_and_504(monkeypatch):
    """_get_hardened treats 429/502/503/504 as the retryable set; the
    streaming twin only handled 429/503, so a mid-stream 502/504 (seen on
    beta endpoints during the 2026-07-29 archive pilot) killed the walk."""
    _instant_sleep(monkeypatch)
    g, fake = _bare_client(
        [_FakeResp({}, status=502), _FakeResp({}, status=504), _FakeResp(PAGE2)],
        monkeypatch,
    )

    pages = _drain(g._iter_pages_hardened("https://graph.microsoft.com/beta/x"))

    assert [i["id"] for p in pages for i in p["value"]] == ["i3"]
    assert fake.calls[0][0] == fake.calls[1][0] == fake.calls[2][0]


def test_hardened_iter_persistent_502_raises_instead_of_looping(monkeypatch):
    """502/504 must go through the TRANSIENT walker (backoff + cumulative
    cap), not policy.decide() — decide() has no 502/504 rule and falls
    through to sleep=0/exhausted=False, which would retry the same page
    forever. A never-ending backup job is worse than a failed one."""
    _instant_sleep(monkeypatch)

    class _ExhaustedAction:
        exhausted = True
        sleep_seconds = 0.0

    class _ExhaustingPolicy(_FakePolicy):
        """Transient walker that gives up after 3 tries, like the real one
        does once GRAPH_MAX_CUMULATIVE_WAIT_SECONDS is reached."""

        def __init__(self):
            self.n = 0

        def decide_transient_error(self):
            self.n += 1
            return _ExhaustedAction() if self.n > 3 else _FakeAction()

    g, fake = _bare_client(
        [_FakeResp({}, status=502)] * 40, monkeypatch,
        policy=_ExhaustingPolicy(),
    )

    with pytest.raises(GraphRetryExhaustedError):
        _drain(g._iter_pages_hardened("https://graph.microsoft.com/beta/x"))

    assert len(fake.calls) == 4, (
        f"persistent 502 must stop after the transient budget, got "
        f"{len(fake.calls)} requests"
    )


def test_hardened_iter_retries_transient_readerror(monkeypatch):
    """httpx.ReadError (stale keepalive conn) is a TransportError but not in
    the narrow (ReadTimeout, ConnectTimeout, RemoteProtocolError) tuple the
    streaming pager caught — it escaped and killed the walk."""
    _instant_sleep(monkeypatch)
    g, fake = _bare_client([httpx.ReadError(""), _FakeResp(PAGE2)], monkeypatch)

    pages = _drain(g._iter_pages_hardened("https://graph.microsoft.com/beta/x"))

    assert [i["id"] for p in pages for i in p["value"]] == ["i3"]
    assert fake.calls[0][0] == fake.calls[1][0]
