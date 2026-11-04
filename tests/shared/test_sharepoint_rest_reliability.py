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
        assert state["calls"] <= 8


async def test_sharepoint_throttling_waits_out_retry_after_instead_of_dropping_items(client_factory):
    """Local 19-site run 2026-11-04: 359 SharePoint 429s; 41 items/lists were dropped because
    the generic budget gives up past 30 s of waiting. Honour Retry-After within a bounded budget."""
    responses = [httpx.Response(429, headers={"Retry-After": "40"}), httpx.Response(429, headers={"Retry-After": "40"}),
                 httpx.Response(200, json={"value": [{"Id": 1}]})]
    async with client_factory(lambda r, n: responses[n - 1]) as (client, state):
        rows = [x async for x in client.iter_sharepoint_list_items_via_rest(HOST, WEB, "list")]
        assert rows == [{"Id": 1}] and state["sleeps"] == [40, 40]


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


def test_server_errors_retry_on_a_short_fixed_wait_while_throttling_backs_off():
    from shared.provider_read import ProviderReadBudget, ProviderReadError
    budget = ProviderReadBudget()
    server = []
    for _ in range(3):
        budget.attempts += 1
        server.append(budget.retry_delay(ProviderReadError("file_stream", status=500)))
    throttled = ProviderReadBudget()
    throttled.attempts = 3
    assert server == [2.0, 2.0, 2.0]
    assert throttled.retry_delay(ProviderReadError("file_stream", status=429)) == 8.0


def test_transport_failures_back_off_like_before():
    """A dropped connection is network trouble, not a Microsoft 500: keep the backoff.
    Local real-data run 2026-11-04: fixed 2 s waits gave up on a ~10 s network blip that the
    previous 1+2+4+8 s backoff rode out, leaving a 4,689-message chat PARTIAL."""
    from shared.graph_ratelimit import retry_wait
    assert [retry_wait(None, None, attempt) for attempt in range(4)] == [1, 2, 4, 8]
    assert [retry_wait(500, None, attempt) for attempt in range(4)] == [2, 2, 2, 2]


async def test_sharepoint_token_uses_the_certificate_app_not_the_pooled_graph_app(monkeypatch):
    """Railway 2026-11-04: SharePoint discovery/backup got 401 — the SharePoint certificate is
    registered only on the main app, but tokens were requested as whichever pooled Graph app the
    worker held (AADSTS700027). SharePoint tokens are minted as the certificate's app."""
    import jwt
    from contextlib import asynccontextmanager
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from shared.graph_client import GraphClient
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048).private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    monkeypatch.setattr(GraphClient, "_load_sharepoint_cert", classmethod(lambda cls: (key, b"dGh1bWI")))
    monkeypatch.setenv("SHAREPOINT_CLIENT_ID", "sharepoint-cert-app")
    posted = []

    class Session:
        async def post(self, url, data=None):
            posted.append(data)
            return httpx.Response(200, json={"access_token": "t", "expires_in": 3600}, request=httpx.Request("POST", url))

    client = GraphClient("pooled-graph-app", "secret", "tenant-id")

    @asynccontextmanager
    async def session():
        yield Session()

    monkeypatch.setattr(client, "_http_session", session)
    assert await client._get_sharepoint_token("contoso.sharepoint.com") == "t"
    claims = jwt.decode(posted[0]["client_assertion"], options={"verify_signature": False})
    assert posted[0]["client_id"] == claims["iss"] == claims["sub"] == "sharepoint-cert-app"
