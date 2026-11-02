"""Admission wraps actual data I/O, while OAuth remains independent."""
from contextlib import asynccontextmanager
import os
os.environ.setdefault('ALLOW_DEV_JWT_SECRETS', 'true')
import httpx
import pytest


class Owner:
    tenant_id = 'verified-tenant'
    client_id = 'verified-app'


class Limiter:
    def __init__(self):
        self.events = []
    @asynccontextmanager
    async def admit(self, context, **kwargs):
        self.events.append(('enter', context))
        try:
            yield
        finally:
            self.events.append(('exit', context))
    async def cooldown(self, context, delay):
        self.events.append(('cooldown', context, delay))


async def test_stream_slot_is_held_until_body_is_closed():
    from shared.provider_http import AdmittedHttpClient
    limiter = Limiter()
    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):
            assert len(limiter.events) == 1
            yield b'payload'
        async def aclose(self):
            assert limiter.events[-1][0] == 'enter'
    async def handler(request):
        return httpx.Response(200, stream=Body())
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as raw:
        client = AdmittedHttpClient(raw, Owner(), limiter=limiter)
        async with client.stream('GET', 'https://graph.microsoft.com/v1.0/users/a/messages/b/$value') as response:
            assert await response.aread() == b'payload'
    assert [e[0] for e in limiter.events] == ['enter', 'exit']
    assert limiter.events[0][1].workload == 'exchange'
    assert limiter.events[0][1].resource == 'mailbox:a'


async def test_oauth_exempt_unknown_data_host_rejected_before_transmission():
    from shared.provider_http import AdmittedHttpClient
    limiter, sent = Limiter(), []
    async def handler(request):
        sent.append(request.url.host)
        return httpx.Response(200, json={'value': []})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as raw:
        client = AdmittedHttpClient(raw, Owner(), limiter=limiter)
        await client.post('https://login.microsoftonline.com/verified-tenant/oauth2/v2.0/token')
        with pytest.raises(ValueError):
            await client.get('https://attacker.invalid/file')
    assert limiter.events == []
    assert sent == ['login.microsoftonline.com']


async def test_retry_after_preserved_and_non_read_write_never_replayed():
    from shared.provider_http import AdmittedHttpClient
    from shared.provider_admission import ProviderDeferred
    limiter, calls = Limiter(), []
    async def handler(request):
        calls.append(request.method)
        return httpx.Response(429, headers={'Retry-After': '900'}, json={'error': {'code': 'throttled'}})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as raw:
        client = AdmittedHttpClient(raw, Owner(), limiter=limiter)
        with pytest.raises(ProviderDeferred) as error:
            await client.get('https://graph.microsoft.com/v1.0/users/a/messages')
        assert error.value.retry_after == 900
        response = await client.post('https://graph.microsoft.com/v1.0/users/a/messages', json={})
        assert response.status_code == 429
    assert calls == ['GET', 'POST']
    assert [e[2] for e in limiter.events if e[0] == 'cooldown'] == [900, 900]


def test_route_scopes_share_sharepoint_budget_and_preserve_subweb():
    from shared.provider_http import request_context
    owner = Owner()
    graph = request_context(owner, 'https://graph.microsoft.com/v1.0/sites/site/lists/list/items')
    rest = request_context(owner, 'https://tenant.sharepoint.com/sites/root/sub/_api/web/lists')
    assert graph.workload == rest.workload == 'sharepoint'
    assert rest.resource == 'site:tenant.sharepoint.com/sites/root/sub'


async def test_graphclient_shared_transport_uses_admission_and_nested_seam_once(monkeypatch):
    from shared.graph_client import GraphClient
    from shared.graph_rate_limiter import graph_rate_limiter
    limiter = Limiter()
    monkeypatch.setenv('PROVIDER_SCOPED_ADMISSION_ENABLED', 'true')
    monkeypatch.setattr(graph_rate_limiter, 'admit', limiter.admit)
    monkeypatch.setattr(graph_rate_limiter, 'cooldown', limiter.cooldown)
    gc = GraphClient('app', 'secret', 'tenant')
    async def handler(request):
        return httpx.Response(200, json={'value': []})
    gc._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    url = 'https://graph.microsoft.com/beta/users/a/mailbox/folders'
    try:
        async with gc._admit_data_request(url):
            client = await gc._get_shared_http()
            assert (await client.get(url)).status_code == 200
        assert [e[0] for e in limiter.events] == ['enter', 'exit']
        # Child tasks must obtain their own grant even when they inherited a ContextVar.
        import asyncio
        async with gc._admit_data_request(url):
            await asyncio.create_task(client.get(url))
        assert [e[0] for e in limiter.events[2:]] == ['enter', 'enter', 'exit', 'exit']
    finally:
        await gc.aclose()
