import os
os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
import asyncio
from contextlib import asynccontextmanager
import httpx
import pytest
from shared.archive_provider import export_items, ArchiveExportOversize

URL = 'https://graph.microsoft.com/beta/admin/exchange/mailboxes/Primary/exportItems'


class Body(httpx.AsyncByteStream):
    def __init__(self):
        self.reads = 0
        self.closed = False

    async def __aiter__(self):
        for _ in range(100):
            self.reads += 1
            yield b'x' * 65536

    async def aclose(self):
        self.closed = True


async def test_stream_stops_at_byte_ceiling_and_releases_admission():
    body = Body()
    class Graph:
        active = False
        async def _get_token(self): return 'synthetic'
        @asynccontextmanager
        async def _admit_data_request(self, *args, **kwargs):
            self.active = True
            try: yield
            finally: self.active = False
        @asynccontextmanager
        async def _http_session(self):
            async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=body))) as client:
                yield client
    graph = Graph()
    with pytest.raises(ArchiveExportOversize):
        await export_items(graph, URL, ['one'], timeout=10, byte_limit=65536)
    assert body.reads == 2 and body.closed and not graph.active


async def test_long_retry_after_is_preserved_and_no_resource_is_held(monkeypatch):
    from shared.provider_read import ProviderReadError
    class Graph:
        active = False
        requests = 0
        async def _get_token(self): return 'synthetic'
        @asynccontextmanager
        async def _admit_data_request(self, *args, **kwargs):
            self.active = True
            try: yield
            finally: self.active = False
        @asynccontextmanager
        async def _http_session(self):
            def respond(request):
                self.requests += 1
                return httpx.Response(429, headers={'Retry-After': '600'})
            async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
                yield client
    graph = Graph()
    async def sleep(delay):
        pytest.fail('long provider cooldown must be returned to durable owner')
    monkeypatch.setattr('shared.archive_provider.asyncio.sleep', sleep)
    with pytest.raises(ProviderReadError) as raised:
        await export_items(graph, URL, ['one'], timeout=10)
    assert raised.value.retry_after == 600
    assert graph.requests == 1 and not graph.active


async def test_export_auth_refresh_and_redirects_are_explicit_and_bounded():
    calls = []
    class Graph:
        _access_token = 'old'
        async def _get_token(self): return self._access_token or 'fresh'
        @asynccontextmanager
        async def _admit_data_request(self, *args, **kwargs): yield
        @asynccontextmanager
        async def _http_session(self):
            def respond(request):
                calls.append((str(request.url), request.headers['Authorization']))
                if len(calls) == 1:
                    return httpx.Response(401)
                if len(calls) == 2:
                    return httpx.Response(308, headers={'Location': URL.replace('Primary', 'CaseSensitive')})
                return httpx.Response(200, json={'value': [{'itemId': 'one', 'data': 'YWJj', 'changeKey': 'a'}]})
            async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
                yield client
    result = await export_items(Graph(), URL, ['one'], timeout=10)
    assert result['value'][0]['itemId'] == 'one'
    assert calls[1][1] == 'Bearer fresh'
    assert calls[-1][0].endswith('/CaseSensitive/exportItems')


async def test_external_redirect_is_rejected_before_credentialed_request():
    calls = []
    class Graph:
        async def _get_token(self): return 'synthetic'
        @asynccontextmanager
        async def _admit_data_request(self, *args, **kwargs): yield
        @asynccontextmanager
        async def _http_session(self):
            def respond(request):
                calls.append(request.url)
                return httpx.Response(308, headers={'Location': 'https://evil.invalid/beta/admin/exchange/mailboxes/X/exportItems'})
            async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
                yield client
    with pytest.raises(ValueError, match='route'):
        await export_items(Graph(), URL, ['one'], timeout=10)
    assert len(calls) == 1
