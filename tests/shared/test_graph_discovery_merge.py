"""Integration regressions for customer paging and local archive/licensing."""
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import httpx
import pytest

from shared.graph_client import GraphClient
from shared.graph_batch import BatchResponse


@pytest.mark.asyncio
@pytest.mark.parametrize('method', ['discover_users_paged', 'discover_mailboxes_paged'])
async def test_discovery_yields_before_fetching_the_next_http_page(monkeypatch, method):
    requests = []
    def respond(request):
        requests.append(request)
        index = len(requests)
        body = {'value': [{
            'id': f'u{index}', 'mail': f'u{index}@example.com', 'userType': 'Member',
            'assignedPlans': [{'service': 'exchange', 'capabilityStatus': 'Enabled'}] if index == 1 else [],
        }]}
        if index == 1:
            body['@odata.nextLink'] = 'https://graph.microsoft.com/v1.0/users?$skiptoken=page2'
        return httpx.Response(200, json=body)

    client = GraphClient('app', 'secret', 'tenant')
    monkeypatch.setattr(client, '_get_token', AsyncMock(return_value='token'))
    monkeypatch.setattr(client, '_iter_pages', client._iter_pages_legacy)
    monkeypatch.setattr(client, '_get', client._get_legacy)
    monkeypatch.setattr('shared.graph_rate_limiter.graph_rate_limiter.acquire', AsyncMock())
    async def batch(reqs):
        return {r.id: BatchResponse(id=r.id, status=200, headers={}, body={'userPurpose': 'shared'}) for r in reqs}
    monkeypatch.setattr(client, 'batch', batch)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        @asynccontextmanager
        async def session(*args, **kwargs):
            yield http
        monkeypatch.setattr(client, '_http_session', session)
        stream = getattr(client, method)()
        first = await anext(stream)
        assert len(requests) == 1, 'first batch must be available before page 2 is requested'
        assert [r['external_id'] for r in first] == ['u1']
        second = await anext(stream)
        assert [r['external_id'] for r in second] == ['u2']
        with pytest.raises(StopAsyncIteration):
            await anext(stream)
    assert all(r.headers.get('ConsistencyLevel') == 'eventual' for r in requests)
    assert requests[0].url.params['$top'] == '999'
    assert '$select' in requests[0].url.params
    assert dict(requests[1].url.params) == {'$skiptoken': 'page2'}
    if method == 'discover_users_paged':
        assert 'assignedPlans' in requests[0].url.params['$select']
        assert first[0]['metadata']['licensed_services']['exchange'] is True
        assert second[0]['metadata']['licensed_services']['exchange'] is False
