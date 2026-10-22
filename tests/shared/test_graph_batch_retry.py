"""Regression: BatchClient retry classification.

A transient outer-$batch failure (502/504 from the Graph gateway) used to
be "collected" as the final answer for every sub-request in the chunk —
one gateway blip silently emptied channel-reply / chat-member fetches in
an otherwise "successful" snapshot. Likewise a 401 from a token that
expired mid-chunk (the chunk's token is minted once and reused across its
whole retry lifetime) permanently failed all 20 subs.

Fix under test:
  * 429/502/503/504 are retryable — whether they arrive on a sub-response
    or as an outer-$batch status that _send_once maps onto every sub;
  * 401 drops the chunk-pinned token AND the client's cached token, then
    replays with a fresh mint;
  * retry exhaustion reports the LAST seen status, not a synthetic 429.
"""
from __future__ import annotations

import asyncio
import os

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

from shared.graph_batch import BatchClient, BatchRequest


class _Resp:
    def __init__(self, status, body=None, headers=None):
        self.status_code = status
        self._body = body or {}
        self.headers = headers or {}

    def json(self):
        return self._body


class _FakePoster:
    """Scripted stand-in for the shared httpx client; records
    (auth_header, payload) per POST."""

    def __init__(self, script):
        self.script = list(script)
        self.calls = []

    async def post(self, url, json=None, headers=None, timeout=None):
        self.calls.append(((headers or {}).get("Authorization"), json))
        return self.script.pop(0)


class _FakeBucket:
    async def acquire(self, priority=None):
        return None


class _FakePolicy:
    stream_bucket = _FakeBucket()


class _FakeGC:
    client_id = "app-1"

    def __init__(self, poster):
        self._poster = poster
        self._policy = _FakePolicy()
        self._access_token = "cached"
        self._minted = 0

    async def _get_token(self):
        self._minted += 1
        return f"tok-{self._minted}"

    async def _try_migrate_app(self, app_id):
        return None, None

    async def _get_shared_http(self):
        return self._poster


def _patch(monkeypatch):
    async def _noop(*a, **k):
        return None

    monkeypatch.setattr(
        "shared.multi_app_manager.multi_app_manager.acquire_app_token", _noop)
    monkeypatch.setattr(
        "shared.multi_app_manager.multi_app_manager.mark_throttled",
        lambda *a, **k: None)
    monkeypatch.setattr(
        "shared.multi_app_manager.multi_app_manager.mark_success",
        lambda *a, **k: None)
    monkeypatch.setattr("shared.graph_batch.asyncio.sleep", _noop)


def _reqs(*ids):
    return [BatchRequest(id=i, method="GET", url=f"/users/{i}") for i in ids]


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


OK_BOTH = {"responses": [
    {"id": "a", "status": 200, "body": {"v": 1}},
    {"id": "b", "status": 200, "body": {"v": 2}},
]}


def test_outer_batch_502_is_retried_not_final(monkeypatch):
    _patch(monkeypatch)
    poster = _FakePoster([_Resp(502), _Resp(200, OK_BOTH)])

    out = _run(BatchClient(_FakeGC(poster)).batch(_reqs("a", "b")))

    assert out["a"].status == 200 and out["b"].status == 200
    assert len(poster.calls) == 2


def test_outer_batch_504_is_retried_not_final(monkeypatch):
    _patch(monkeypatch)
    poster = _FakePoster([_Resp(504), _Resp(200, OK_BOTH)])

    out = _run(BatchClient(_FakeGC(poster)).batch(_reqs("a", "b")))

    assert out["a"].status == 200 and out["b"].status == 200


def test_sub_response_502_is_retried(monkeypatch):
    _patch(monkeypatch)
    first = {"responses": [
        {"id": "a", "status": 200, "body": {"v": 1}},
        {"id": "b", "status": 502, "body": {}},
    ]}
    second = {"responses": [{"id": "b", "status": 200, "body": {"v": 2}}]}
    poster = _FakePoster([_Resp(200, first), _Resp(200, second)])

    out = _run(BatchClient(_FakeGC(poster)).batch(_reqs("a", "b")))

    assert out["a"].status == 200 and out["b"].status == 200
    # the follow-up POST re-sent only the failed sub
    assert [r["id"] for r in poster.calls[1][1]["requests"]] == ["b"]


def test_401_re_mints_token_and_replays(monkeypatch):
    _patch(monkeypatch)
    first = {"responses": [{"id": "a", "status": 401, "body": {}}]}
    second = {"responses": [{"id": "a", "status": 200, "body": {"v": 1}}]}
    poster = _FakePoster([_Resp(200, first), _Resp(200, second)])

    out = _run(BatchClient(_FakeGC(poster)).batch(_reqs("a")))

    assert out["a"].status == 200
    # the replay must ride a token minted AFTER the 401, not the first one
    assert poster.calls[0][0] == "Bearer tok-1"
    assert poster.calls[1][0] == "Bearer tok-2"


def test_exhausted_chunk_reports_last_status(monkeypatch):
    _patch(monkeypatch)
    # GRAPH_MAX_RETRIES defaults to 5 → up to 6 sends; script plenty.
    poster = _FakePoster([_Resp(502)] * 12)

    out = _run(BatchClient(_FakeGC(poster)).batch(_reqs("a")))

    # still failed — but as the real last status, not a synthetic 429
    assert out["a"].status == 502
