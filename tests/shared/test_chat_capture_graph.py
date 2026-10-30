"""Strict chat capture uses complete pages, fixed credentials and bounded streams."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import importlib
import os
import secrets

import httpx
import pytest

os.environ.setdefault("JWT_SECRET", secrets.token_hex(32))

from shared.graph_client import GraphClient


class Bytes(httpx.AsyncByteStream):
    def __init__(self, chunks):
        self.chunks = chunks
        self.closed = False
        self.consumed = 0

    async def __aiter__(self):
        for chunk in self.chunks:
            self.consumed += 1
            yield chunk

    async def aclose(self):
        self.closed = True


@pytest.fixture
def source_factory(monkeypatch):
    """Real adapter and clients; replace only HTTP transport and pacing."""
    requests, clients = [], []
    original_client = httpx.AsyncClient
    tokens = {"value": "opaque-access-token"}
    state = {}

    @asynccontextmanager
    async def make(handler):
        module = importlib.import_module("shared.chat_capture_graph")

        async def transport(request):
            requests.append(request)
            if request.url.host == "login.microsoftonline.com":
                return httpx.Response(200, json={"access_token": tokens["value"], "expires_in": 3600})
            result = handler(request)
            if hasattr(result, "__await__"):
                result = await result
            return result

        def create_client(*args, **kwargs):
            kwargs["transport"] = httpx.MockTransport(transport)
            client = original_client(*args, **kwargs)
            clients.append(client)
            return client

        monkeypatch.setattr(httpx, "AsyncClient", create_client)
        from shared.config import settings
        monkeypatch.setattr(settings, "GRAPH_STREAM_PACE_REQS_PER_SEC", 0)
        monkeypatch.setattr(settings, "GRAPH_APP_PACE_REQS_PER_SEC", 0)
        from shared.graph_rate_limiter import graph_rate_limiter

        async def no_pacing(*args, **kwargs):
            return 0

        monkeypatch.setattr(graph_rate_limiter, "acquire", no_pacing)
        base = GraphClient("app", "secret", "tenant")
        async with module.pinned_chat_source(base, principal_id="user") as source:
            state["source"] = source
            state["base"] = base
            yield source
        assert base._http is None

    return make, requests, clients, tokens, state


async def test_full_paging_uses_user_scope_and_top_50(source_factory):
    make, requests, clients, *_ = source_factory
    url = "https://graph.microsoft.com/v1.0/users/user/chats/chat/messages"

    def handler(request):
        assert request.url.path == "/v1.0/users/user/chats/chat/messages"
        assert "$filter" not in request.url.params and "$expand" not in request.url.params
        if "$skiptoken" not in request.url.params:
            assert request.url.params["$top"] == "50"
            return httpx.Response(200, json={"value": [{"id": "one"}], "@odata.nextLink": url + "?$skiptoken=next"})
        return httpx.Response(200, json={"value": [{"id": "two"}]})

    async with make(handler) as source:
        pages = [page async for page in source.iter_message_pages("chat")]
        assert [[m["id"] for m in page] for page in pages] == [["one"], ["two"]]
        assert len(source.permission_scope_hash) == 64
    assert all(client.is_closed for client in clients)
    assert len([r for r in requests if r.url.host == "graph.microsoft.com"]) == 2


async def test_chat_and_hosted_collections_paginate(source_factory):
    make, *_ = source_factory

    def handler(request):
        if request.url.path.endswith("/hostedContents"):
            return httpx.Response(200, json={"value": [{"id": "hosted"}]})
        return httpx.Response(200, json={"value": [{"id": "chat"}]})

    async with make(handler) as source:
        assert [c["id"] async for c in source.iter_chats()] == ["chat"]
        assert [c["id"] async for c in source.iter_hosted_contents("chat", "message")] == ["hosted"]


async def test_next_link_equivalent_path_encoding_is_accepted(source_factory):
    make, *_ = source_factory
    chat_id = "19:chat@thread.v2"

    def handler(request):
        if "$skiptoken" not in request.url.params:
            return httpx.Response(200, json={"value": [], "@odata.nextLink": "https://graph.microsoft.com/v1.0/users/user/chats/19:chat@thread.v2/messages?$skiptoken=next"})
        return httpx.Response(200, json={"value": [{"id": "message", "chatId": chat_id}]})

    async with make(handler) as source:
        assert [p async for p in source.iter_message_pages(chat_id)] == [[], [{"id": "message", "chatId": chat_id}]]


@pytest.mark.parametrize("payload", [{}, {"value": None}, {"value": {}}, {"value": [None]}, {"value": [{"id": ""}]}, {"value": [], "@odata.nextLink": ""}, {"value": [], "error": {"message": "not complete"}}, {"value": [], "@odata.count": 1}])
async def test_malformed_page_never_looks_complete(source_factory, payload):
    make, *_ = source_factory
    async with make(lambda request: httpx.Response(200, json=payload)) as source:
        with pytest.raises(RuntimeError, match="collection|page|cursor"):
            _ = [page async for page in source.iter_message_pages("chat")]


async def test_cursor_cycle_is_rejected(source_factory):
    make, *_ = source_factory
    url = "https://graph.microsoft.com/v1.0/users/user/chats/chat/messages?$skiptoken=loop"
    async with make(lambda request: httpx.Response(200, json={"value": [], "@odata.nextLink": url})) as source:
        with pytest.raises(RuntimeError, match="cycle"):
            _ = [page async for page in source.iter_message_pages("chat")]


async def test_next_page_failure_propagates_instead_of_terminal_success(source_factory):
    make, *_ = source_factory

    def handler(request):
        if "$skiptoken" in request.url.params:
            return httpx.Response(403)
        return httpx.Response(200, json={"value": [{"id": "one"}], "@odata.nextLink": "https://graph.microsoft.com/v1.0/users/user/chats/chat/messages?$skiptoken=next"})

    async with make(handler) as source:
        with pytest.raises(RuntimeError, match="403"):
            _ = [page async for page in source.iter_message_pages("chat")]


async def test_cursor_cannot_change_principal_or_send_bearer_elsewhere(source_factory):
    make, requests, *_ = source_factory
    async with make(lambda request: httpx.Response(200, json={"value": [], "@odata.nextLink": "https://attacker.example/?token=private"})) as source:
        with pytest.raises(RuntimeError):
            _ = [page async for page in source.iter_message_pages("chat")]
    assert not any(r.url.host == "attacker.example" for r in requests)


async def test_same_host_cursor_cannot_change_principal(source_factory):
    make, requests, *_ = source_factory
    async with make(lambda request: httpx.Response(200, json={"value": [], "@odata.nextLink": "https://graph.microsoft.com/v1.0/users/other/chats/chat/messages?$skiptoken=next"})) as source:
        with pytest.raises(RuntimeError, match="scope"):
            _ = [page async for page in source.iter_message_pages("chat")]
    assert not any("/users/other/" in r.url.path for r in requests)


async def test_fixed_app_under_throttling(source_factory, monkeypatch):
    make, requests, *_ = source_factory
    calls = 0

    async def migration_forbidden(*args):
        raise AssertionError("capture must not rotate applications")

    monkeypatch.setattr(GraphClient, "_try_migrate_app", migration_forbidden)

    def handler(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, headers={"Retry-After": "0"})
        return httpx.Response(200, json={"value": []})

    async with make(handler) as source:
        assert [p async for p in source.iter_message_pages("chat")] == [[]]
    assert calls == 2
    bearers = {r.headers["Authorization"] for r in requests if r.url.host == "graph.microsoft.com"}
    assert len(bearers) == 1


@pytest.mark.parametrize("field", ["client_id", "tenant_id"])
async def test_changed_owned_identity_fails_capture(source_factory, field):
    make, _, _, _, state = source_factory

    def handler(request):
        setattr(state["source"]._client, field, "different")
        return httpx.Response(200, json={"value": []})

    async with make(handler) as source:
        with pytest.raises(RuntimeError, match="context"):
            _ = [p async for p in source.iter_message_pages("chat")]


async def test_opaque_token_and_opaque_refresh_are_supported(source_factory):
    make, _, _, tokens, state = source_factory
    tokens["value"] = "opaque-access-token-first"
    hashes = []

    def handler(request):
        hashes.append(state["source"].permission_scope_hash)
        tokens["value"] = "opaque-access-token-refreshed"
        state["source"]._client._access_token = None
        return httpx.Response(200, json={"value": []})

    async with make(handler) as source:
        initial = source.permission_scope_hash
        assert [p async for p in source.iter_message_pages("chat")] == [[]]
        assert source.permission_scope_hash == initial == hashes[0]


async def test_file_resolver_bypasses_negative_cache_and_reports_errors(source_factory, monkeypatch):
    make, requests, *_ = source_factory
    calls = 0

    async def legacy_resolver_forbidden(*args):
        raise AssertionError("legacy negative cache must not be used")

    monkeypatch.setattr(GraphClient, "resolve_share_to_drive_item", legacy_resolver_forbidden)

    def handler(request):
        nonlocal calls
        calls += 1
        assert request.url.path.startswith("/v1.0/shares/u!")
        assert "prefer" not in request.headers
        if calls == 1:
            return httpx.Response(404)
        return httpx.Response(200, json={"id": "item", "parentReference": {"driveId": "drive"}, "eTag": "v1"})

    async with make(handler) as source:
        with pytest.raises(RuntimeError, match="404"):
            await source.resolve_file("https://tenant.sharepoint.com/file.pdf")
        assert (await source.resolve_file("https://tenant.sharepoint.com/file.pdf"))["id"] == "item"
    assert calls == 2


async def test_documented_onedrive_short_share_url_is_resolved(source_factory):
    make, *_ = source_factory
    async with make(lambda request: httpx.Response(200, json={"id": "item", "parentReference": {"driveId": "drive"}})) as source:
        assert (await source.resolve_file("https://1drv.ms/u/s!example"))["id"] == "item"


async def test_file_refresh_checks_stable_identity(source_factory):
    make, *_ = source_factory
    async with make(lambda request: httpx.Response(200, json={"id": "other", "parentReference": {"driveId": "drive"}})) as source:
        with pytest.raises(RuntimeError, match="identity"):
            await source.refresh_file("drive", "item")


async def test_file_download_streams_without_bearer_on_redirect(source_factory):
    make, requests, *_ = source_factory
    content = Bytes([b"one", b"two"])

    def handler(request):
        if request.url.path == "/start":
            return httpx.Response(302, headers={"Location": "https://cdn.files.1drv.com/end?signature=secret"})
        return httpx.Response(200, stream=content, headers={"Content-Length": "6", "Content-Type": "application/pdf"})

    async with make(handler) as source:
        item = {"id": "item", "size": 6, "parentReference": {"driveId": "drive"}, "@microsoft.graph.downloadUrl": "https://tenant.sharepoint.com/start?signature=secret"}
        async with source.open_file(item) as (stream, size, content_type):
            assert content.consumed == 0
            assert size == 6 and content_type == "application/pdf"
            assert b"".join([chunk async for chunk in stream]) == b"onetwo"
    assert content.closed
    assert all("authorization" not in r.headers for r in requests if r.url.host != "login.microsoftonline.com")


async def test_untrusted_download_redirect_is_rejected_and_redacted(source_factory):
    make, requests, *_ = source_factory
    async with make(lambda request: httpx.Response(302, headers={"Location": "https://attacker.example/private?signature=secret"})) as source:
        with pytest.raises(RuntimeError) as error:
            async with source.open_file({"size": 1, "@microsoft.graph.downloadUrl": "https://tenant.sharepoint.com/start?signature=secret"}):
                pytest.fail("untrusted redirect was accepted")
        assert "secret" not in str(error.value)
    assert not any(r.url.host == "attacker.example" for r in requests)


async def test_hosted_unknown_length_remains_unknown_and_partial_read_closes(source_factory):
    make, requests, *_ = source_factory
    content = Bytes([b"one", b"two"])
    async with make(lambda request: httpx.Response(200, stream=content)) as source:
        async with source.open_hosted("chat", "message", "hosted") as (stream, size, _):
            assert size is None
            assert await anext(stream)
    assert content.closed
    assert next(r for r in requests if r.url.host == "graph.microsoft.com").headers["authorization"].startswith("Bearer ")


async def test_cancelled_stream_closes_response_and_owned_pool(source_factory):
    make, _, clients, *_ = source_factory
    content = Bytes([b"one", b"two"])
    with pytest.raises(asyncio.CancelledError):
        async with make(lambda request: httpx.Response(200, stream=content)) as source:
            async with source.open_hosted("chat", "message", "hosted") as (stream, _, _):
                await anext(stream)
                raise asyncio.CancelledError
    assert content.closed and all(c.is_closed for c in clients)


async def test_truncated_stream_is_not_success(source_factory):
    make, *_ = source_factory
    async with make(lambda request: httpx.Response(200, stream=Bytes([b"one"]), headers={"Content-Length": "6"})) as source:
        with pytest.raises(RuntimeError, match="length"):
            async with source.open_hosted("chat", "message", "hosted") as (stream, _, _):
                _ = [chunk async for chunk in stream]


async def test_exhausted_throttling_is_explicit_failure(source_factory):
    make, requests, *_ = source_factory
    async with make(lambda request: httpx.Response(503, headers={"Retry-After": "0"})) as source:
        with pytest.raises(RuntimeError, match="503"):
            _ = [page async for page in source.iter_message_pages("chat")]
    assert len([r for r in requests if r.url.host == "graph.microsoft.com"]) == 5


@pytest.mark.parametrize("operation", ["messages", "hosted"])
async def test_closed_source_cannot_reopen_owned_pool(source_factory, operation):
    make, _, clients, *_ = source_factory
    async with make(lambda request: httpx.Response(200, json={"value": []})) as source:
        pass
    count = len(clients)
    with pytest.raises(RuntimeError, match="closed"):
        if operation == "messages":
            _ = [page async for page in source.iter_message_pages("chat")]
        else:
            async with source.open_hosted("chat", "message", "hosted"):
                pass
    assert len(clients) == count
    assert all(c.is_closed for c in clients)


async def test_public_configuration_hash_matches_open_source(source_factory):
    make, *_ = source_factory
    module = importlib.import_module("shared.chat_capture_graph")
    async with make(lambda request: httpx.Response(200, json={"value": []})) as source:
        assert source.permission_scope_hash == module.permission_scope_hash_for(
            tenant_id="tenant", application_id="app", principal_id="user")
