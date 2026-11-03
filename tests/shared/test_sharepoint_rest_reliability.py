"""REST pagination and streaming must fail closed without replaying bytes."""
from contextlib import asynccontextmanager
import os
import secrets

import httpx
import pytest

os.environ.setdefault("JWT_SECRET", secrets.token_hex(32))
from shared.graph_client import GraphClient

HOST = "tenant.sharepoint.com"
WEB = f"https://{HOST}/sites/a/child"


class Interrupted(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield b"abc"
        raise httpx.ReadTimeout("interrupted")


@pytest.fixture
def client_factory(monkeypatch):
    @asynccontextmanager
    async def make(handler):
        client = GraphClient("app", "secret", "tenant")
        state = {"tokens": 0, "calls": 0, "sleeps": []}

        async def token(host):
            assert host == HOST
            state["tokens"] += 1
            return "opaque-token"

        async def route(request):
            state["calls"] += 1
            assert state["calls"] < 10, "unbounded retries"
            return handler(request, state["calls"])

        async def sleep(seconds):
            state["sleeps"].append(seconds)

        async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as http:
            @asynccontextmanager
            async def session():
                yield http
            monkeypatch.setattr(client, "_http_session", session)
            monkeypatch.setattr(client, "_get_sharepoint_token", token)
            monkeypatch.setattr("shared.graph_client.asyncio.sleep", sleep)
            yield client, state
    return make


async def test_denial_after_page_is_not_successful_eof(client_factory):
    def route(req, n):
        if n == 1:
            return httpx.Response(200, json={"value": [{"Id": 1}], "odata.nextLink": WEB + "/_api/web/lists(guid'list')/items?cursor=next"})
        return httpx.Response(403, json={"error": {"code": "accessDenied", "message": "private-url?token=secret"}})
    async with client_factory(route) as (client, state):
        rows = []
        with pytest.raises(Exception, match="403") as exc:
            async for row in client.iter_sharepoint_list_items_via_rest(HOST, WEB, "list"):
                rows.append(row)
        assert rows == [{"Id": 1}]
        assert "secret" not in str(exc.value)
        assert state["tokens"] == 2


@pytest.mark.parametrize("status", [403, 404, 500])
async def test_metadata_failure_is_not_non_file(client_factory, status):
    async with client_factory(lambda r, n: httpx.Response(status)) as (client, _):
        with pytest.raises(Exception, match=str(status)):
            await client.get_sharepoint_file_metadata_via_rest(HOST, WEB, "/sites/a/x")


async def test_malformed_page_is_not_empty_list(client_factory):
    async with client_factory(lambda r, n: httpx.Response(200, json={"error": {}})) as (client, _):
        with pytest.raises(Exception, match="page|payload"):
            _ = [x async for x in client.iter_sharepoint_list_items_via_rest(HOST, WEB, "list")]


async def test_retry_budget_finite_even_retry_after_zero(client_factory):
    async with client_factory(lambda r, n: httpx.Response(429, headers={"Retry-After": "0"})) as (client, state):
        with pytest.raises(Exception, match="429|budget"):
            _ = [x async for x in client.iter_sharepoint_list_items_via_rest(HOST, WEB, "list")]
        assert state["calls"] <= 4


async def test_long_retry_after_deferred_without_early_retry(client_factory):
    async with client_factory(lambda r, n: httpx.Response(503, headers={"Retry-After": "600"})) as (client, state):
        with pytest.raises(Exception) as exc:
            _ = [x async for x in client.iter_sharepoint_list_items_via_rest(HOST, WEB, "list")]
        assert getattr(exc.value, "retry_after", None) == 600
        assert state["calls"] == 1
        assert state["sleeps"] == []


async def test_interrupted_stream_never_replays_prefix(client_factory):
    def route(req, n):
        return httpx.Response(200, stream=Interrupted()) if n == 1 else httpx.Response(200, content=b"abcdef")
    async with client_factory(route) as (client, state):
        received = bytearray()
        with pytest.raises(Exception):
            async for chunk in client.stream_sharepoint_file_via_rest(HOST, WEB, "/sites/a/x", chunk_size=3):
                received.extend(chunk)
        assert received == b"abc"
        assert state["calls"] == 1


async def test_stream_truncation_fails(client_factory):
    async with client_factory(lambda r, n: httpx.Response(200, headers={"Content-Length": "8"}, content=b"abc")) as (client, _):
        with pytest.raises(Exception, match="length|truncated"):
            _ = [x async for x in client.stream_sharepoint_file_via_rest(HOST, WEB, "/sites/a/x")]


async def test_stream_rejects_wrong_etag_and_length(client_factory):
    async with client_factory(lambda r, n: httpx.Response(200, headers={"ETag": '"new"'}, content=b"abc")) as (client, _):
        with pytest.raises(Exception, match="version|ETag"):
            _ = [x async for x in client.stream_sharepoint_file_via_rest(HOST, WEB, "/sites/a/x", expected_size=3, expected_etag='"old"')]


async def test_stream_target_checked_before_credentials(client_factory):
    async with client_factory(lambda r, n: httpx.Response(200, content=b"x")) as (client, state):
        with pytest.raises(ValueError):
            _ = [x async for x in client.stream_sharepoint_file_via_rest(HOST, "https://evil.invalid", "/x")]
        assert state["calls"] == state["tokens"] == 0


from shared.provider_read import ProviderReadError


async def test_list_discovery_preserves_exact_subweb_and_reports_rest_failure(monkeypatch):
    gc = GraphClient('app', 'secret', 'tenant')
    called = []
    async def get(url, params=None, **kwargs):
        if url.endswith('/lists'):
            return {'value': [{'id': 'list', 'webUrl': 'https://tenant.sharepoint.com/sites/main/sub/Lists/L'}]}
        return {'webUrl': 'https://tenant.sharepoint.com/sites/main/sub'}
    async def rest(host, url):
        called.append((host, url))
        raise ProviderReadError('list_collection', status=403)
    monkeypatch.setattr(gc, '_get', get)
    monkeypatch.setattr(gc, '_get_sharepoint_lists_via_rest', rest)
    with pytest.raises(ProviderReadError):
        await gc.get_sharepoint_site_lists('tenant.sharepoint.com,collection,web', strict_rest=True)
    assert called == [('tenant.sharepoint.com', 'https://tenant.sharepoint.com/sites/main/sub')]


async def test_rest_collection_follows_all_pages_and_detects_multi_page_cycle(monkeypatch):
    gc = GraphClient('app', 'secret', 'tenant')
    calls = []
    async def unexpected_token(*args):
        pytest.fail('list discovery bypassed bounded REST reader')
    monkeypatch.setattr(gc, '_get_sharepoint_token', unexpected_token)
    first = 'https://tenant.sharepoint.com/sites/sub/_api/web/lists'
    second = first + '?cursor=two'
    async def page(host, url, **kwargs):
        calls.append(url)
        return {'value': [{'Id': '1' if url == first else '2'}], '@odata.nextLink': second if url == first else first}
    monkeypatch.setattr(gc, '_sharepoint_rest_json', page)
    with pytest.raises(ProviderReadError):
        await gc._get_sharepoint_lists_via_rest('tenant.sharepoint.com', 'https://tenant.sharepoint.com/sites/sub')
    assert calls == [first, second]
