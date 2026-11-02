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


async def test_large_singleton_spools_through_receipt_lifetime_and_cleans_up():
    import base64
    import hashlib
    from pathlib import Path
    from shared.archive_provider import export_items_spooled
    from shared.archive_spool import SpooledArchiveData
    block = b'abc' * 16384
    encoded = base64.b64encode(block)
    class LargeBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'{"value":[{"itemId":"large","changeKey":"v1","data":"'
            for _ in range(512):
                yield encoded
            yield b'"}]}'
    class Graph:
        async def _get_token(self): return 'synthetic'
        @asynccontextmanager
        async def _admit_data_request(self, *args, **kwargs): yield
        @asynccontextmanager
        async def _http_session(self):
            async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, stream=LargeBody()))) as client:
                yield client
    async with export_items_spooled(Graph(), URL, ['large'], timeout=10) as response:
        data = response['value'][0]['data']
        assert isinstance(data, SpooledArchiveData)
        assert data.size == len(block) * 512 > 16 * 1024 * 1024
        path = Path(data.path)
        assert path.exists()
        expected = hashlib.sha256()
        for _ in range(512):
            expected.update(block)
        assert data.sha256 == expected.hexdigest()
    assert not path.exists()


async def test_owned_export_long_cooldown_yields_after_releasing_http(monkeypatch):
    from types import SimpleNamespace
    from shared import provider_runtime as runtime
    graph = SimpleNamespace(tenant_id='tenant', client_id='app', active=False)
    async def token(): return 'synthetic'
    @asynccontextmanager
    async def admit(*a, **kw):
        graph.active = True
        try: yield
        finally: graph.active = False
    @asynccontextmanager
    async def http():
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r:
                httpx.Response(429, headers={'Retry-After': '600'}))) as client:
            yield client
    graph._get_token, graph._admit_data_request, graph._http_session = token, admit, http
    execution = runtime.ProviderExecution(None, None)
    marker = runtime._EXECUTION.set(execution)
    try:
        with pytest.raises(runtime.ProviderTurnDeferred):
            await export_items(graph, URL, ['one'], timeout=10)
        assert execution.deferred.retry_after == 600
        assert execution.deferred.context.workload == 'exchange'
        assert not graph.active
    finally:
        runtime._EXECUTION.reset(marker)
