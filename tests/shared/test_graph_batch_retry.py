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
import json
import os
from contextlib import asynccontextmanager

import httpx
import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

from shared.graph_batch import BatchClient, BatchRequest
from shared.provider_read import ProviderReadError


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
        result = self.script.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result


class _FakeBucket:
    async def acquire(self, priority=None):
        return None


class _FakePolicy:
    stream_bucket = _FakeBucket()


class _FakeGC:
    client_id = "app-1"
    tenant_id = "tenant-1"

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


@pytest.mark.parametrize("error_type", [httpx.WriteError, httpx.ReadError, httpx.ReadTimeout])
@pytest.mark.parametrize("method", ["GET", "HEAD"])
def test_read_transport_failure_retries_pending_batch(monkeypatch, error_type, method):
    _patch(monkeypatch)
    poster = _FakePoster([error_type("connection lost"), _Resp(200, OK_BOTH)])
    requests = _reqs("a", "b")
    requests[0].method = method

    out = _run(BatchClient(_FakeGC(poster)).batch(requests))

    assert out["a"].body == {"v": 1} and out["b"].body == {"v": 2}
    assert len(poster.calls) == 2


def test_transport_retry_keeps_successful_subresponses(monkeypatch):
    _patch(monkeypatch)
    poster = _FakePoster([
        _Resp(200, {"responses": [
            {"id": "a", "status": 200, "body": {"v": 1}},
            {"id": "b", "status": 502, "body": {}},
        ]}),
        httpx.ReadError("connection lost"),
        _Resp(200, {"responses": [{"id": "b", "status": 200, "body": {"v": 2}}]}),
    ])

    out = _run(BatchClient(_FakeGC(poster)).batch(_reqs("a", "b")))

    assert out["a"].body == {"v": 1} and out["b"].body == {"v": 2}
    assert [[r["id"] for r in call[1]["requests"]] for call in poster.calls] == [
        ["a", "b"], ["b"], ["b"],
    ]


def test_transport_retry_does_not_restart_attempt_budget(monkeypatch):
    _patch(monkeypatch)
    monkeypatch.setattr("shared.config.settings.GRAPH_MAX_RETRIES", 2)
    failure = httpx.WriteError("connection lost")
    poster = _FakePoster([_Resp(502), failure, failure, _Resp(200, OK_BOTH)])

    with pytest.raises(ProviderReadError) as raised:
        _run(BatchClient(_FakeGC(poster)).batch(_reqs("a", "b")))

    assert raised.value.stage == "batch_transport"
    assert raised.value.attempts == 3
    assert raised.value.responses == {}
    assert len(poster.calls) == 3


@pytest.mark.parametrize("methods", [("POST",), ("GET", "POST"), ("PATCH", "HEAD")])
def test_unknown_mutation_transport_outcome_is_never_replayed(monkeypatch, methods):
    _patch(monkeypatch)
    failure = httpx.WriteError("connection lost")
    poster = _FakePoster([failure, _Resp(200, OK_BOTH)])
    requests = [BatchRequest(id=str(i), method=m, url="/users/a/messages")
                for i, m in enumerate(methods)]

    with pytest.raises(httpx.WriteError) as raised:
        _run(BatchClient(_FakeGC(poster)).batch(requests))

    assert raised.value is failure
    assert len(poster.calls) == 1


@pytest.mark.parametrize("kind", ["cancel", "defer", "turn_defer", "deadline"])
def test_batch_control_flow_is_not_retried(monkeypatch, kind):
    from shared.provider_admission import ProviderDeferred
    from shared.provider_runtime import ProviderBudgetExhausted, ProviderTurnDeferred
    _patch(monkeypatch)
    failure = {
        "cancel": asyncio.CancelledError(),
        "defer": ProviderDeferred(None, 60),
        "turn_defer": ProviderTurnDeferred(ProviderDeferred(None, 60)),
        "deadline": ProviderBudgetExhausted("original deadline exhausted"),
    }[kind]
    poster = _FakePoster([failure, _Resp(200, OK_BOTH)])

    with pytest.raises(type(failure)) as raised:
        _run(BatchClient(_FakeGC(poster)).batch(_reqs("a", "b")))

    assert raised.value is failure
    assert len(poster.calls) == 1


def test_oversized_read_batch_splits_without_losing_results(monkeypatch):
    _patch(monkeypatch)
    poster = _FakePoster([
        ProviderReadError("response_byte_limit", status=200),
        _Resp(200, {"responses": [{"id": "a", "status": 200, "body": {"v": 1}}]}),
        _Resp(200, {"responses": [{"id": "b", "status": 200, "body": {"v": 2}}]}),
    ])

    out = _run(BatchClient(_FakeGC(poster)).batch(_reqs("a", "b")))

    assert out["a"].body == {"v": 1} and out["b"].body == {"v": 2}
    assert [[r["id"] for r in call[1]["requests"]] for call in poster.calls] == [
        ["a", "b"], ["a"], ["b"],
    ]


def test_split_reads_inherit_attempts_spent_on_original_batch(monkeypatch):
    _patch(monkeypatch)
    monkeypatch.setattr("shared.config.settings.GRAPH_MAX_RETRIES", 2)
    poster = _FakePoster([
        httpx.ReadError("connection lost"),
        ProviderReadError("response_byte_limit", status=200),
        _Resp(200, {"responses": [{"id": "a", "status": 200, "body": {"v": 1}}]}),
        httpx.WriteError("connection lost"),
        _Resp(200, OK_BOTH),
    ])

    with pytest.raises(ProviderReadError) as raised:
        _run(BatchClient(_FakeGC(poster)).batch(_reqs("a", "b")))

    assert raised.value.stage == "batch_transport"
    assert raised.value.attempts == 3
    assert set(raised.value.responses) == {"a"}
    assert raised.value.responses["a"].body == {"v": 1}
    assert len(poster.calls) == 4


def test_unreadable_singleton_fails_explicitly_with_confirmed_other_chunks(monkeypatch):
    _patch(monkeypatch)
    monkeypatch.setattr("shared.config.settings.GRAPH_BATCH_MAX_SIZE", 2)
    poster = _FakePoster([
        ProviderReadError("response_byte_limit", status=200),
        _Resp(200, {"responses": [{"id": "a", "status": 200, "body": {"v": 1}}]}),
        ProviderReadError("response_byte_limit", status=200),
        _Resp(200, {"responses": [{"id": "c", "status": 200, "body": {"v": 3}}]}),
    ])

    with pytest.raises(ProviderReadError) as raised:
        _run(BatchClient(_FakeGC(poster)).batch(_reqs("a", "b", "c")))

    assert raised.value.stage == "response_byte_limit"
    assert set(raised.value.responses) == {"a", "c"}
    assert raised.value.responses["a"].body == {"v": 1}
    assert raised.value.responses["c"].body == {"v": 3}
    assert len(poster.calls) == 4


@pytest.mark.parametrize("methods", [("POST",), ("GET", "POST")])
def test_oversized_mutation_batch_is_not_split(monkeypatch, methods):
    _patch(monkeypatch)
    failure = ProviderReadError("response_byte_limit", status=200)
    poster = _FakePoster([failure, _Resp(200, OK_BOTH)])
    requests = [BatchRequest(id=str(i), method=m, url="/users/a/messages")
                for i, m in enumerate(methods)]

    with pytest.raises(ProviderReadError) as raised:
        _run(BatchClient(_FakeGC(poster)).batch(requests))

    assert raised.value is failure
    assert len(poster.calls) == 1


@pytest.mark.parametrize("kind", ["cancel", "defer", "turn_defer", "deadline", "invalid_payload"])
def test_split_batch_cannot_swallow_control_flow_or_invalid_payload(monkeypatch, kind):
    from shared.provider_admission import ProviderDeferred
    from shared.provider_runtime import ProviderBudgetExhausted, ProviderTurnDeferred
    _patch(monkeypatch)
    failure = {
        "cancel": asyncio.CancelledError(),
        "defer": ProviderDeferred(None, 60),
        "turn_defer": ProviderTurnDeferred(ProviderDeferred(None, 60)),
        "deadline": ProviderBudgetExhausted("original deadline exhausted"),
        "invalid_payload": ProviderReadError("invalid_batch_response"),
    }[kind]
    poster = _FakePoster([
        ProviderReadError("response_byte_limit", status=200), failure, _Resp(200, OK_BOTH),
    ])

    with pytest.raises(type(failure)) as raised:
        _run(BatchClient(_FakeGC(poster)).batch(_reqs("a", "b")))

    assert raised.value is failure
    assert len(poster.calls) == 2


async def test_split_reads_keep_real_response_ceiling_and_charge_every_send(monkeypatch):
    from shared.provider_http import AdmittedHttpClient, response_byte_limit
    _patch(monkeypatch)
    admitted, charged, sent = [], [], []

    class Limiter:
        @asynccontextmanager
        async def admit_many(self, requirements):
            admitted.append(requirements)
            yield

    async def account(context):
        charged.append(context)

    async def handler(request):
        ids = [r["id"] for r in json.loads(request.content)["requests"]]
        sent.append(ids)
        return httpx.Response(200, json={"responses": [
            {"id": id_, "status": 200, "body": {"value": "x" * 700}} for id_ in ids
        ]})

    monkeypatch.setattr("shared.provider_runtime.account_provider_attempt", account)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as raw:
        gc = _FakeGC(None)
        gc._poster = AdmittedHttpClient(raw, gc, limiter=Limiter())
        with response_byte_limit(1024):
            out = await BatchClient(gc).batch(_reqs("a", "b"))

    assert set(out) == {"a", "b"}
    assert out["a"].body["value"] == "x" * 700
    assert sent == [["a", "b"], ["a"], ["b"]]
    assert len(admitted) == len(charged) == 3
    assert [len(requirements) for requirements in admitted] == [3, 2, 2]


def test_read_failure_does_not_start_later_mutation_chunks(monkeypatch):
    _patch(monkeypatch)
    monkeypatch.setattr("shared.config.settings.GRAPH_BATCH_MAX_SIZE", 1)
    poster = _FakePoster([
        ProviderReadError("response_byte_limit", status=200),
        _Resp(200, {"responses": [{"id": "write", "status": 201, "body": {}}]}),
    ])
    requests = [*_reqs("a"), BatchRequest(id="write", method="POST", url="/users/a/messages")]

    with pytest.raises(ProviderReadError):
        _run(BatchClient(_FakeGC(poster)).batch(requests))

    assert len(poster.calls) == 1


def test_exhausted_oversize_batch_cannot_reset_budget_by_splitting(monkeypatch):
    _patch(monkeypatch)
    monkeypatch.setattr("shared.config.settings.GRAPH_MAX_RETRIES", 0)
    poster = _FakePoster([ProviderReadError("response_byte_limit", status=200), _Resp(200, OK_BOTH)])

    with pytest.raises(ProviderReadError) as raised:
        _run(BatchClient(_FakeGC(poster)).batch(_reqs("a", "b")))

    assert raised.value.responses == {}
    assert raised.value.attempts == 1
    assert len(poster.calls) == 1


async def test_download_url_prefetch_keeps_only_confirmed_batch_urls(monkeypatch):
    from shared.graph_client import GraphClient
    _patch(monkeypatch)
    poster = _FakePoster([
        ProviderReadError("response_byte_limit", status=200),
        _Resp(200, {"responses": [{"id": "a", "status": 200, "body": {
            "@microsoft.graph.downloadUrl": "https://example.invalid/a", "size": 7,
            "file": {"hashes": {"quickXorHash": "hash-a"}},
        }}]}),
        ProviderReadError("response_byte_limit", status=200),
    ])
    gc = GraphClient.__new__(GraphClient)
    gc.batch = BatchClient(_FakeGC(poster)).batch

    out = await gc.get_download_urls_batch("drive", ["a", "b"])

    assert out["a"] == ("https://example.invalid/a", 7, "hash-a")
    assert not out.get("b", (None, 0, None))[0]
    assert len(poster.calls) == 3


@pytest.mark.parametrize("kind", ["defer", "deadline"])
async def test_download_url_prefetch_does_not_turn_control_flow_into_fallback(kind):
    from shared.graph_client import GraphClient
    from shared.provider_admission import ProviderDeferred
    from shared.provider_runtime import ProviderBudgetExhausted
    failure = (ProviderDeferred(None, 60) if kind == "defer"
               else ProviderBudgetExhausted("original deadline exhausted"))

    async def batch(requests):
        raise failure

    gc = GraphClient.__new__(GraphClient)
    gc.batch = batch

    with pytest.raises(type(failure)) as raised:
        await gc.get_download_urls_batch("drive", ["a"])

    assert raised.value is failure


@pytest.mark.parametrize("first_result", ["transport", "oversize"])
async def test_retries_and_splits_preserve_original_unit_deadline(monkeypatch, first_result):
    from datetime import datetime, timedelta, timezone
    from shared import provider_runtime as runtime
    from shared.provider_http import AdmittedHttpClient, response_byte_limit
    _patch(monkeypatch)
    sent = []
    expired = datetime.now(timezone.utc) - timedelta(seconds=1)
    unit = runtime.UnitExecution("file", "file", expired + timedelta(minutes=1), expired, 0, remaining=10)

    class Limiter:
        @asynccontextmanager
        async def admit_many(self, requirements):
            yield

    async def handler(request):
        sent.append(request)
        unit.deadline_at = expired
        if first_result == "transport":
            raise httpx.ReadError("connection lost")
        return httpx.Response(200, content=b"x" * 2048)

    execution_marker = runtime._EXECUTION.set(runtime.ProviderExecution(None, None))
    unit_marker = runtime._UNIT.set(unit)
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as raw:
            gc = _FakeGC(None)
            gc._poster = AdmittedHttpClient(raw, gc, limiter=Limiter())
            with response_byte_limit(1024), pytest.raises(runtime.ProviderBudgetExhausted):
                await BatchClient(gc).batch(_reqs("a", "b"))
    finally:
        runtime._UNIT.reset(unit_marker)
        runtime._EXECUTION.reset(execution_marker)

    assert len(sent) == 1
    assert unit.remaining == 9
    assert unit.deadline_at == expired
