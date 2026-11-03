"""Reject invalid pool credentials before binding a new Graph client."""
import os
os.environ.setdefault('ALLOW_DEV_JWT_SECRETS', 'true')
from contextlib import asynccontextmanager
from types import SimpleNamespace
from urllib.parse import parse_qs
import httpx
import pytest
from shared.graph_client import GraphClient
from shared.multi_app_manager import MultiAppManager


def pool(monkeypatch, ids=('bad', 'good')):
    import shared.multi_app_manager as module
    from shared.config import settings
    proxy = SimpleNamespace(**vars(settings))
    proxy.GRAPH_APPS = [dict(index=i, client_id=cid, client_secret='secret', tenant_id='tenant')
                        for i, cid in enumerate(ids, 1)]
    monkeypatch.setattr(module, 'settings', proxy)
    manager = MultiAppManager()
    manager._current_index = 0
    monkeypatch.setattr(module, 'multi_app_manager', manager)
    return manager


def token_transport(monkeypatch, responses):
    calls = []
    async def handler(request):
        cid = parse_qs(request.content.decode())['client_id'][0]
        calls.append(cid)
        status, data = responses[cid]
        return httpx.Response(status, json=data)
    @asynccontextmanager
    async def session(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            yield client
    monkeypatch.setattr(GraphClient, '_http_session', session)
    return calls


async def test_new_client_skips_expired_app_and_caches_valid_token(monkeypatch):
    manager = pool(monkeypatch)
    calls = token_transport(monkeypatch, {'bad': (401, {'error': 'invalid_client', 'error_codes': [7000222]}),
                                         'good': (200, {'access_token': 'token', 'expires_in': 3600})})
    client = await GraphClient.from_pool(manager, 'tenant')
    assert client.client_id == 'good'
    assert await client._get_token() == 'token'
    assert calls == ['bad', 'good']
    assert manager.is_credential_invalid('bad', 'tenant')
    assert not manager.is_credential_invalid('bad', 'other-tenant')
    assert manager.get_next_app(tenant_id='tenant').client_id == 'good'


async def test_existing_client_never_switches_identity_on_expiry(monkeypatch):
    manager = pool(monkeypatch)
    calls = token_transport(monkeypatch, {'bad': (401, {'error': 'invalid_client', 'error_codes': [7000215]})})
    client = GraphClient('bad', 'secret', 'tenant')
    with pytest.raises(httpx.HTTPStatusError):
        await client._get_token()
    assert client.client_id == 'bad'
    assert manager.is_credential_invalid('bad', 'tenant')
    with pytest.raises(httpx.HTTPStatusError):
        await client._get_token()
    assert calls == ['bad']


@pytest.mark.parametrize('status,error', [(403, {'error': 'access_denied'}), (401, {'error': 'invalid_client', 'error_codes': [999]})])
async def test_unclassified_auth_errors_do_not_rotate_or_quarantine(monkeypatch, status, error):
    manager = pool(monkeypatch)
    calls = token_transport(monkeypatch, {'bad': (status, error)})
    client = await GraphClient.from_pool(manager, 'tenant')
    assert client.client_id == 'bad'  # Existing handler owns final failure reporting.
    assert calls == ['bad']
    assert not manager.is_credential_invalid('bad', 'tenant')


async def test_all_invalid_is_bounded_and_returns_failing_client_not_false_success(monkeypatch):
    manager = pool(monkeypatch, ('a', 'b', 'c', 'd'))
    calls = token_transport(monkeypatch, {cid: (401, {'error': 'invalid_client', 'error_codes': [7000222]}) for cid in 'abcd'})
    client = await GraphClient.from_pool(manager, 'tenant')
    assert len(calls) == 3
    with pytest.raises(httpx.HTTPStatusError):
        await client._get_token()
    assert len(calls) == 3


async def test_exhausted_pool_does_not_request_known_bad_credentials_again(monkeypatch):
    manager = pool(monkeypatch, ('bad',))
    calls = token_transport(monkeypatch, {'bad': (401, {'error': 'invalid_client', 'error_codes': [7000222]})})
    await GraphClient.from_pool(manager, 'tenant')
    client = await GraphClient.from_pool(manager, 'tenant')
    with pytest.raises(httpx.HTTPStatusError):
        await client._get_token()
    assert calls == ['bad']


async def test_worker_binding_is_single_flight_and_keeps_existing_identity(monkeypatch):
    import ast
    import asyncio
    from pathlib import Path
    from unittest.mock import AsyncMock
    manager = pool(monkeypatch)
    tree = ast.parse((Path(__file__).resolve().parents[2] / 'workers/backup-worker/main.py').read_text())
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'BackupWorker')
    method = next(n for n in cls.body if isinstance(n, ast.AsyncFunctionDef) and n.name == 'get_graph_client')
    namespace = dict(asyncio=asyncio, GraphClient=GraphClient, multi_app_manager=manager,
                     Tenant=object, Optional=__import__('typing').Optional)
    exec(compile(ast.Module(body=[method], type_ignores=[]), '<worker-binding>', 'exec'), namespace)
    selected = GraphClient('good', 'secret', 'tenant')
    factory = AsyncMock(return_value=selected)
    monkeypatch.setattr(GraphClient, 'from_pool', factory)
    worker = SimpleNamespace(graph_clients={})
    tenant = SimpleNamespace(external_tenant_id='tenant')
    a, b = await asyncio.gather(*(namespace['get_graph_client'](worker, tenant, 'USER_MAIL') for _ in range(2)))
    assert a is b is selected
    factory.assert_awaited_once()
    manager.mark_invalid_credential('good', 'tenant')
    assert await namespace['get_graph_client'](worker, tenant, 'USER_MAIL') is selected
