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
        assert [e[0] for e in limiter.events[2:]] == ['enter', 'exit']
    finally:
        await gc.aclose()


async def test_archive_page_sink_has_an_actual_response_byte_bound(monkeypatch):
    from shared.graph_client import GraphClient
    from shared.provider_read import ProviderReadError
    from shared.graph_rate_limiter import graph_rate_limiter
    from shared.config import settings
    monkeypatch.setenv('PROVIDER_SCOPED_ADMISSION_ENABLED', 'false')
    monkeypatch.setenv('ARCHIVE_PAGE_MAX_RESPONSE_BYTES', '65536')
    monkeypatch.setattr(settings, 'GRAPH_HARDENING_ENABLED', False)
    async def no_limit(*a, **k): pass
    monkeypatch.setattr(graph_rate_limiter, 'acquire', no_limit)
    gc = GraphClient('app', 'secret', 'tenant')
    async def token(): return 'synthetic'
    monkeypatch.setattr(gc, '_get_token', token)
    calls = []
    class BigPage(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'{"value":[{"id":"one","oversized":"'
            for _ in range(100):
                calls.append(1)
                yield b'x' * 65536
            yield b'"}]}'
    gc._http = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, stream=BigPage())))
    async def sink(*args):
        pytest.fail('oversized page must not reach durable commit')
    try:
        with pytest.raises(ProviderReadError, match='response_byte_limit'):
            await gc._get('https://graph.microsoft.com/beta/admin/exchange/mailboxes/primary/folders', on_page=sink)
        assert len(calls) <= 2
    finally:
        await gc.aclose()


async def test_bounded_json_response_is_not_decompressed_twice():
    import gzip
    from shared.provider_http import AdmittedHttpClient, response_byte_limit
    compressed = gzip.compress(b'{"value":[{"id":"one"}]}')
    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield compressed
    async def handler(request):
        return httpx.Response(200, headers={'Content-Encoding': 'gzip'}, stream=Body())
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as raw:
        client = AdmittedHttpClient(raw, Owner(), limiter=Limiter())
        with response_byte_limit(1024):
            response = await client.get('https://graph.microsoft.com/v1.0/users')
        assert response.json() == {'value': [{'id': 'one'}]}


async def test_batch_charges_subrequests_and_defers_only_retryable_reads():
    from shared.provider_http import AdmittedHttpClient
    from shared.provider_admission import ProviderDeferred
    class BatchLimiter(Limiter):
        @asynccontextmanager
        async def admit_many(self, requirements, **kwargs):
            self.requirements = requirements
            yield
    limiter = BatchLimiter()
    calls = []
    async def handler(request):
        calls.append(1)
        return httpx.Response(200, json={'responses': [
            {'id': 'mail', 'status': 200, 'body': {}},
            {'id': 'site', 'status': 429, 'headers': {'Retry-After': '600'}, 'body': {}},
        ]})
    payload = {'requests': [
        {'id': 'mail', 'method': 'GET', 'url': '/users/a/messages/x'},
        {'id': 'site', 'method': 'GET', 'url': '/sites/s/lists/x'},
    ]}
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as raw:
        client = AdmittedHttpClient(raw, Owner(), limiter=limiter)
        with pytest.raises(ProviderDeferred):
            await client.post('https://graph.microsoft.com/v1.0/$batch', json=payload)
        assert [r[0].workload for r in limiter.requirements] == ['batch', 'exchange', 'sharepoint']
        assert [r[0].workload for r in limiter.requirements if r[1] == 1] == ['batch', 'exchange', 'sharepoint']
        payload['requests'][0]['method'] = 'POST'
        response = await client.post('https://graph.microsoft.com/v1.0/$batch', json=payload)
        assert response.status_code == 200
    assert len(calls) == 2
    assert all(e[1].workload == 'sharepoint' for e in limiter.events if e[0] == 'cooldown')


@pytest.mark.parametrize('outer_status', [429, 200])
async def test_sent_mutation_batch_never_becomes_an_unsent_deferral(outer_status):
    from shared.provider_http import AdmittedHttpClient
    from shared.provider_admission import ProviderDeferred
    from shared.provider_read import ProviderReadError
    class FailedCoordinator(Limiter):
        @asynccontextmanager
        async def admit_many(self, requirements):
            yield
        async def cooldown(self, context, delay):
            raise ProviderDeferred(context, delay, stage='redis_unavailable')
    sent = []
    async def handler(request):
        sent.append(request)
        return httpx.Response(outer_status, headers={'Retry-After': '600'}, json={'responses': [
            {'id': 'write', 'status': 429, 'headers': {'Retry-After': '600'}}]})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as raw:
        client = AdmittedHttpClient(raw, Owner(), limiter=FailedCoordinator())
        with pytest.raises(ProviderReadError, match='provider_cooldown_unavailable'):
            await client.post('https://graph.microsoft.com/v1.0/$batch', json={
                'requests': [{'id': 'write', 'method': 'POST', 'url': '/users/a/messages'}]})
    assert len(sent) == 1


@pytest.mark.parametrize('target', ['https://tenant.sharepoint.com/file', 'https://attacker.invalid/file'])
async def test_redirects_are_checked_and_charged_per_hop(target):
    from shared.provider_http import AdmittedHttpClient
    limiter, sent = Limiter(), []
    async def handler(request):
        sent.append(request)
        if len(sent) == 1:
            return httpx.Response(302, headers={'Location': target})
        assert 'authorization' not in request.headers
        return httpx.Response(200, content=b'abc')
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=True) as raw:
        client = AdmittedHttpClient(raw, Owner(), limiter=limiter)
        call = client.get('https://graph.microsoft.com/v1.0/drives/d/items/i/content', headers={'Authorization': 'Bearer test'})
        if 'attacker' in target:
            with pytest.raises(ValueError, match='host'):
                await call
            assert len(sent) == 1
        else:
            assert (await call).content == b'abc'
            assert len(sent) == 2
            assert [e[0] for e in limiter.events] == ['enter', 'exit', 'enter', 'exit']
            assert all(e[1].resource == 'drive:d' for e in limiter.events)


async def test_batch_response_obeys_byte_envelope_before_json_parse():
    from shared.provider_http import AdmittedHttpClient, response_byte_limit
    from shared.provider_read import ProviderReadError
    class BatchLimiter(Limiter):
        @asynccontextmanager
        async def admit_many(self, requirements): yield
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r:
            httpx.Response(200, content=b'x' * 2048))) as raw:
        with response_byte_limit(1024), pytest.raises(ProviderReadError, match='response_byte_limit'):
            await AdmittedHttpClient(raw, Owner(), limiter=BatchLimiter()).post(
                'https://graph.microsoft.com/v1.0/$batch', json={'requests': [
                    {'id': 'one', 'method': 'GET', 'url': '/users/a/messages'}]})


def test_nested_readers_cannot_relax_the_callers_byte_ceiling():
    from shared.provider_http import response_byte_limit, _response_limit
    with response_byte_limit(2048):
        with response_byte_limit(None):
            assert _response_limit.get() == 2048
        with response_byte_limit(4096):
            assert _response_limit.get() == 2048
        with response_byte_limit(1024):
            assert _response_limit.get() == 1024
        assert _response_limit.get() == 2048


async def test_preauthenticated_archive_import_is_metered_without_auth_or_replay(monkeypatch):
    from shared.archive_export import post_json_no_auth
    from shared.graph_rate_limiter import graph_rate_limiter
    limiter, sent = Limiter(), []
    monkeypatch.setenv('PROVIDER_SCOPED_ADMISSION_ENABLED', 'true')
    monkeypatch.setattr(graph_rate_limiter, 'admit', limiter.admit)
    monkeypatch.setattr(graph_rate_limiter, 'cooldown', limiter.cooldown)
    async def handler(request):
        sent.append(request)
        assert 'authorization' not in request.headers
        return httpx.Response(429, headers={'Retry-After': '600'})
    with pytest.raises(httpx.HTTPStatusError):
        await post_json_no_auth('https://outlook.office365.com/synthetic/import', {},
            graph_client=Owner(), _client_transport=httpx.MockTransport(handler))
    assert len(sent) == 1
    assert limiter.events[0][1].workload == 'exchange'
    assert limiter.events[0][1].tenant_id == 'verified-tenant'


@pytest.mark.parametrize('path, resource', [
    ('/v1.0/chats/chat-A/messages', 'chat:chat-A'),
    ('/v1.0/chats/chat-A/members', 'chat:chat-A'),
    ('/beta/users/user-A/chats', 'user_chats:user-A'),
])
def test_pinned_chat_routes_opt_into_shared_pacing(path, resource):
    from types import SimpleNamespace
    from shared.provider_http import request_context
    owner = SimpleNamespace(tenant_id='tenant', client_id='app', provider_chat_pacing=True)
    context = request_context(owner, 'https://graph.microsoft.com' + path)
    assert context.workload == 'teams'
    assert context.resource == resource
    assert context.paced_chat


@pytest.mark.parametrize('path', [
    '/v1.0/users/user-A/chats/getAllMessages',
    '/v1.0/users/user-A/chats/getAllRetainedMessages',
    '/v1.0/teams/team-A/channels/channel-A/messages',
    '/v1.0/users/user-A/messages',
])
def test_unrelated_routes_do_not_get_pinned_chat_pacing(path):
    from types import SimpleNamespace
    from shared.provider_http import request_context
    owner = SimpleNamespace(tenant_id='tenant', client_id='app', provider_chat_pacing=True)
    context = request_context(owner, 'https://graph.microsoft.com' + path)
    assert not getattr(context, 'paced_chat', False)


def test_regular_chat_clients_keep_existing_admission_context():
    from shared.provider_http import request_context
    context = request_context(Owner(), 'https://graph.microsoft.com/v1.0/chats/chat-A/messages')
    assert context.resource == 'chat:chat-A'
    assert not getattr(context, 'paced_chat', False)
