"""Regressions for two OneDrive silent-loss modes in GraphClient.

1. get_download_url used to raise the SAME "Could not obtain downloadUrl"
   RuntimeError for transient retry exhaustion (429 storm / 5xx) as for
   genuinely non-downloadable cloud-native items. The OneDrive backup
   caller string-matches 'downloadurl' to skip cloud-native objects
   silently — so during a throttle storm ordinary files vanished from a
   "successful" snapshot. Exhaustion must raise GraphRetryExhaustedError
   (a distinct type) instead; the cloud-native RuntimeError message
   contract is preserved.

2. uploadSession chunk loops: a chunk PUT that timed out AFTER committing
   server-side got 416 on the blind Content-Range replay and failed the
   whole restore; a persistent throttle looped forever; and
   upload_large_attachment had no retry at all.
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
from shared.graph_ratelimit import GraphRetryExhaustedError


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


# ─── get_download_url: exhaustion vs cloud-native ─────────────────────────

def _dl_client(script, monkeypatch):
    """GraphClient with a scripted _get: entries are dicts (returned) or
    Exceptions (raised). The last entry repeats if the script runs out."""
    g = GraphClient.__new__(GraphClient)
    calls = {"n": 0}

    async def fake_get(url, params=None):
        calls["n"] += 1
        item = script[min(calls["n"] - 1, len(script) - 1)]
        if isinstance(item, Exception):
            raise item
        return item

    g._get = fake_get

    async def _instant(_s):
        return None

    monkeypatch.setattr("shared.graph_client.asyncio.sleep", _instant)
    return g, calls


def test_transient_exhaustion_raises_distinct_error(monkeypatch):
    g, calls = _dl_client([Exception("HTTP 429 Too Many Requests")], monkeypatch)

    with pytest.raises(GraphRetryExhaustedError):
        _run(g.get_download_url("d1", "i1"))

    assert calls["n"] == 3  # all attempts burned before giving up


def test_transient_then_success_recovers(monkeypatch):
    ok = {"id": "i1", "size": 5,
          "file": {"hashes": {"quickXorHash": "q"}},
          "@microsoft.graph.downloadUrl": "https://dl"}
    g, calls = _dl_client([Exception("boom"), ok], monkeypatch)

    url, size, qxh = _run(g.get_download_url("d1", "i1"))

    assert (url, size, qxh) == ("https://dl", 5, "q")


def test_cloud_native_keeps_downloadurl_runtimeerror_contract(monkeypatch):
    # file facet present but Graph never mints a URL (Whiteboard/Loop)
    item = {"id": "i1", "size": 5, "file": {"hashes": {}}}
    g, calls = _dl_client([item], monkeypatch)

    with pytest.raises(RuntimeError) as ei:
        _run(g.get_download_url("d1", "i1"))

    assert not isinstance(ei.value, GraphRetryExhaustedError)
    # callers string-match this to skip cloud-native items silently
    assert "downloadurl" in str(ei.value).lower()


def test_no_file_facet_short_circuits_without_retry(monkeypatch):
    item = {"id": "i1", "size": 5}
    g, calls = _dl_client([item], monkeypatch)

    with pytest.raises(RuntimeError) as ei:
        _run(g.get_download_url("d1", "i1"))

    assert not isinstance(ei.value, GraphRetryExhaustedError)
    assert "downloadurl" in str(ei.value).lower()
    assert calls["n"] == 1  # a facet-less item can't grow one — no retries


# ─── uploadSession chunk loops ─────────────────────────────────────────────

class _UpResp:
    def __init__(self, status, body=None, headers=None, text=""):
        self.status_code = status
        self._body = body or {}
        self.headers = headers or {}
        self.text = text

    def json(self):
        return self._body


class _UpClient:
    """Scripted POST/PUT/GET fake for uploadSession flows."""

    def __init__(self, put_script, get_script=None, post_script=None):
        self.put_script = list(put_script)
        self.get_script = list(get_script or [])
        self.post_script = list(post_script or [])
        self.puts = []

    async def post(self, url, headers=None, json=None):
        return self.post_script.pop(0)

    async def put(self, url, headers=None, content=None):
        self.puts.append((headers or {}).get("Content-Range"))
        item = self.put_script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    async def get(self, url, headers=None):
        return self.get_script.pop(0)


class _Session:
    def __init__(self, c):
        self.c = c

    async def __aenter__(self):
        return self.c

    async def __aexit__(self, *exc):
        return False


def _upload_client(up, monkeypatch):
    g = GraphClient.__new__(GraphClient)
    g._http_session = lambda: _Session(up)

    async def tok():
        return "tok"

    g._get_token = tok

    async def _instant(_s):
        return None

    monkeypatch.setattr("shared.graph_client.asyncio.sleep", _instant)
    return g


def test_upload_chunk_committed_before_timeout_resyncs(monkeypatch):
    """ReadTimeout after the chunk landed server-side: the Content-Range
    replay gets 416. The loop must consult nextExpectedRanges and move on —
    failing here loses a restore whose bytes actually uploaded."""
    up = _UpClient(
        put_script=[
            httpx.ReadTimeout(""),                  # chunk 0 PUT dies post-commit
            _UpResp(416, text="range not satisfiable"),  # blind replay rejected
            _UpResp(201, {"id": "item1"}),          # chunk 1 completes the file
        ],
        get_script=[_UpResp(200, {"nextExpectedRanges": ["10-19"]})],
        post_script=[_UpResp(200, {"uploadUrl": "https://up"})],
    )
    g = _upload_client(up, monkeypatch)

    out = _run(g.upload_large_file_to_drive("d1", "a/b.bin", b"x" * 20, 20,
                                            chunk_size=10))

    assert out == {"id": "item1"}
    assert len(up.puts) == 3


def test_upload_persistent_throttle_raises_instead_of_looping(monkeypatch):
    up = _UpClient(
        put_script=[_UpResp(429, headers={"Retry-After": "1"})] * 30,
        post_script=[_UpResp(200, {"uploadUrl": "https://up"})],
    )
    g = _upload_client(up, monkeypatch)

    with pytest.raises(RuntimeError, match="throttled persistently"):
        _run(g.upload_large_file_to_drive("d1", "a/b.bin", b"x" * 20, 20,
                                          chunk_size=10))


def test_upload_large_attachment_retries_throttle(monkeypatch):
    """One 429 mid-attachment used to permanently fail the message restore."""
    up = _UpClient(
        put_script=[_UpResp(429, headers={"Retry-After": "1"}), _UpResp(200)],
    )
    g = _upload_client(up, monkeypatch)

    async def fake_post(url, payload, headers=None):
        return {"uploadUrl": "https://up"}

    g._post = fake_post

    _run(g.upload_large_attachment("u1", "m1", "a.bin", 10, b"y" * 10))

    assert len(up.puts) == 2
