"""Regression: every 429/503 sleep in GraphClient must be CAPPED and LOGGED.

Bug (2026-08-12, prod): job 197d2b39 / snapshot 0e423f79 ("Archive — Jason
Rue") sat RUNNING for 15.6 h having written 188,380 of 340,258 items. The
owning worker was alive (heartbeat every 10 s, archive liveness probe every
120 s) but idle — 1 millicore of CPU and ZERO HTTP sockets.

``_post_json`` — the exportItems path — did::

    retry_after = int(resp.headers.get("Retry-After", "30"))
    ...
    await asyncio.sleep(retry_after)     # uncapped, and SILENT

Three defects in three lines:

  1. **Uncapped.** Graph's Exchange admin endpoints can hand back a
     Retry-After measured in hours. The sleep happens INSIDE
     ``archive_backup``'s ``asyncio.Semaphore(ARCHIVE_EXPORT_CONCURRENCY)``,
     so with concurrency 4 it takes exactly four such responses to freeze all
     ~17,000 chunk tasks of a 340k-item export.
  2. **Silent.** No print on this branch, which is why 15.6 h of wedge left
     not one log line to explain itself.
  3. ``int()`` raises ValueError on the HTTP-date form of Retry-After, which
     RFC 7231 explicitly permits.

``_parse_retry_after`` (same module, added earlier for the streaming paths)
already solves all three — clamped, HTTP-date aware, total. These three call
sites simply never adopted it. This test pins that they do.

The cap also breaks a second-order cascade: the raw value was passed to
``multi_app_manager.mark_throttled``, which uses Retry-After as the ban
*floor* — so one hostile value banned all six Graph apps for hours and
defeated ``_try_migrate_app``, which is the very thing that would have
avoided the sleep.
"""
from __future__ import annotations

import asyncio
import os
import pathlib
import re

import httpx
import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://u:p@localhost/db")

_SRC_PATH = (
    pathlib.Path(__file__).resolve().parents[2] / "shared" / "graph_client.py"
)
_SRC = _SRC_PATH.read_text()


# ─────────────────────────── static pins ────────────────────────────

def test_no_raw_uncapped_retry_after_parse_remains():
    """No call site may re-introduce the raw int(...) parse."""
    offenders = re.findall(
        r'int\(\s*resp\.headers\.get\(\s*["\']Retry-After["\']', _SRC,
    )
    assert offenders == [], (
        f"{len(offenders)} uncapped Retry-After parse(s) left in "
        f"graph_client.py — use _parse_retry_after(resp) instead"
    )


def test_parse_retry_after_clamps_and_tolerates_http_date():
    import shared.graph_client as gc

    def _resp(value):
        return httpx.Response(
            429, headers={"Retry-After": value},
            request=httpx.Request("GET", "https://graph.microsoft.com/x"),
        )

    assert gc._parse_retry_after(_resp("86400")) <= 120.0
    assert gc._parse_retry_after(_resp("5")) == 5.0
    # HTTP-date form must not raise (bare int() would).
    assert gc._parse_retry_after(_resp("Wed, 21 Oct 2099 07:28:00 GMT")) <= 120.0
    # Junk falls back to the default, still clamped.
    assert gc._parse_retry_after(_resp("not-a-number")) <= 120.0


# ─────────────────────── behavioural: _post_json ────────────────────────

class _StubClient:
    """httpx-shaped client that always 503s with a hostile Retry-After."""

    def __init__(self, retry_after: str = "86400"):
        self.retry_after = retry_after
        self.posts = 0

    async def post(self, url, headers=None, json=None, timeout=None):
        self.posts += 1
        return httpx.Response(
            503,
            headers={"Retry-After": self.retry_after},
            request=httpx.Request("POST", url),
        )


class _StubSessionCM:
    def __init__(self, client):
        self._client = client

    async def __aenter__(self):
        return self._client

    async def __aexit__(self, *exc):
        return False


def test_post_json_sleep_is_capped_and_logged(monkeypatch, capsys):
    """A 24-hour Retry-After must never become a 24-hour sleep."""
    import shared.graph_client as gc

    client = _StubClient(retry_after="86400")
    gclient = gc.GraphClient("cid", "secret", "tid")

    async def _tok():
        return "token"

    async def _no_migrate(_app_id):
        return (None, None)

    monkeypatch.setattr(gclient, "_get_token", _tok, raising=False)
    monkeypatch.setattr(gclient, "_try_migrate_app", _no_migrate, raising=False)
    monkeypatch.setattr(
        gclient, "_http_session", lambda timeout=None: _StubSessionCM(client),
        raising=False,
    )

    slept: list[float] = []

    async def _fake_sleep(secs):
        slept.append(float(secs))

    monkeypatch.setattr(gc.asyncio, "sleep", _fake_sleep)

    loop = asyncio.new_event_loop()
    try:
        with pytest.raises(httpx.HTTPStatusError):
            loop.run_until_complete(
                gclient._post_json(
                    "https://graph.microsoft.com/beta/admin/exchange/"
                    "mailboxes/MBX:x/exportItems",
                    {"itemIds": ["a"]},
                    timeout=600,
                )
            )
    finally:
        loop.close()

    assert slept, "expected the throttle path to sleep at least once"
    assert max(slept) <= 120.0, (
        f"uncapped Retry-After honored verbatim: slept {max(slept)}s "
        f"(this is the 2026-08-12 archive wedge)"
    )
    # The whole retry budget must not exceed a bounded wall-clock.
    assert sum(slept) <= 5 * 120.0

    out = capsys.readouterr().out
    assert "Retry-After" in out or "throttl" in out.lower(), (
        "the throttle-sleep branch must log — 15.6 h of prod wedge produced "
        "no log line because it did not"
    )


def test_post_json_prefers_app_migration_over_sleeping(monkeypatch):
    """When a healthy alternate app exists we must not sleep at all."""
    import shared.graph_client as gc

    class _FlipClient:
        def __init__(self):
            self.posts = 0

        async def post(self, url, headers=None, json=None, timeout=None):
            self.posts += 1
            if self.posts == 1:
                return httpx.Response(
                    503, headers={"Retry-After": "86400"},
                    request=httpx.Request("POST", url),
                )
            return httpx.Response(
                200, json={"value": [{"itemId": "a", "data": "d"}]},
                request=httpx.Request("POST", url),
            )

    client = _FlipClient()
    gclient = gc.GraphClient("cid", "secret", "tid")

    async def _tok():
        return "token"

    async def _migrate(_app_id):
        return ("token2", "other-app-id")

    monkeypatch.setattr(gclient, "_get_token", _tok, raising=False)
    monkeypatch.setattr(gclient, "_try_migrate_app", _migrate, raising=False)
    monkeypatch.setattr(
        gclient, "_http_session", lambda timeout=None: _StubSessionCM(client),
        raising=False,
    )

    slept: list[float] = []

    async def _fake_sleep(secs):
        slept.append(float(secs))

    monkeypatch.setattr(gc.asyncio, "sleep", _fake_sleep)

    loop = asyncio.new_event_loop()
    try:
        out = loop.run_until_complete(
            gclient._post_json("https://graph.microsoft.com/x", {"itemIds": ["a"]})
        )
    finally:
        loop.close()

    assert out["value"][0]["itemId"] == "a"
    assert slept == [], "migrated to a healthy app — must not sleep Retry-After"


def test_hardened_get_throttle_sleep_is_clamped():
    """ITO-10146 review finding: this was the 4th call site the 2026-08-12
    clamp missed — RateLimitPolicy.decide trusts Retry-After verbatim up to
    the 14,400s cumulative cap, so a single hostile header produced one
    unbroken multi-hour sleep inside _get_hardened's 429 branch, longer
    than the archive stall guard's 5,400s window (false stall abort). The
    sleep and mark_throttled's ban floor must use the clamped value; the
    raw value still charges the cumulative budget inside policy.decide."""
    idx = _SRC.find("async def _get_hardened(")
    assert idx != -1
    blk = _SRC[idx: _SRC.find("\n    async def ", idx + 10)]
    assert "_sleep_s = min(float(action.sleep_seconds), 120.0)" in blk
    assert "await asyncio.sleep(_sleep_s)" in blk
    assert "mark_throttled(\n                        self.client_id, int(_sleep_s),\n                    )" in blk
    # the UNCLAMPED sleep must not remain on the throttle branch
    throttle = blk[blk.find("(429, 502, 503, 504)"): blk.find("resp.status_code == 401")]
    assert "await asyncio.sleep(action.sleep_seconds)" not in throttle
