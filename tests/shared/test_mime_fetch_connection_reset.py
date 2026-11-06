"""A reset Graph connection must not fail the MIME fetch outright.

Railway 2026-11-06: one dead HTTP/2 connection failed every stream on it with
ReadError/WriteError (158 of 178 in one MIME-BULK). get_message_mime_source
retried timeouts but not these, so each message dropped to the slow single path.
"""
import os
os.environ.setdefault('ALLOW_DEV_JWT_SECRETS', 'true')
import httpx
import pytest
from shared.graph_client import GraphClient


@pytest.mark.parametrize('error', [httpx.ReadError, httpx.WriteError])
@pytest.mark.asyncio
async def test_mime_fetch_retries_on_reset_connection(monkeypatch, error):
    calls = []

    def handler(request):
        calls.append(request.url.path)
        if len(calls) == 1:
            raise error('connection reset by peer', request=request)
        return httpx.Response(200, content=b'MIME')

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    g = GraphClient('app', 'secret', 'tenant')
    async def token(*a, **k):
        return 't'
    async def no_sleep(_):
        return None
    monkeypatch.setattr(g, '_get_token', token)
    monkeypatch.setattr(g, '_get_shared_http', lambda: _ret(http))
    monkeypatch.setattr('shared.graph_client.asyncio.sleep', no_sleep)

    assert await g.get_message_mime_source('u', 'm') == b'MIME'
    assert len(calls) == 2
    await http.aclose()


async def _ret(value):
    return value
