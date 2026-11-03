"""A committed page is a backpressure boundary, never an aggregate."""
import asyncio
import httpx
import pytest

from test_graph_context_and_malformed_body import _bare, _Resp

_run = asyncio.run


@pytest.mark.parametrize('path', ['_get_legacy', '_get_hardened'])
def test_sink_is_awaited_before_next_request_and_does_not_collect(path, monkeypatch):
    first = 'https://graph.microsoft.com/beta/x'
    second = first + '?page=2'
    pages = [{'value': [{'id': 'a'}], '@odata.nextLink': second, '@odata.context': 'one'},
             {'value': [{'id': 'b'}], '@odata.context': 'two'}]
    g, client, _ = _bare([_Resp(p) for p in pages], monkeypatch)
    seen = []

    async def sink(page, url):
        assert len(client.calls) == len(seen) + 1
        seen.append((page, url))

    out = _run(getattr(g, path)(first, on_page=sink))
    assert seen == [(pages[0], first), (pages[1], second)]
    assert out['value'] == []
    assert out['@odata.count'] == 2


@pytest.mark.parametrize('path', ['_get_legacy', '_get_hardened'])
@pytest.mark.parametrize('error', [RuntimeError('db failed'), httpx.ReadError('sink failed')])
def test_sink_error_propagates_without_fetching_or_retrying(path, error, monkeypatch):
    g, client, _ = _bare([_Resp({'value': [], '@odata.nextLink': 'https://graph.microsoft.com/beta/y'})], monkeypatch)

    async def sink(page, url):
        raise error

    with pytest.raises(type(error), match=str(error)):
        _run(getattr(g, path)('https://graph.microsoft.com/beta/x', on_page=sink))
    assert len(client.calls) == 1


@pytest.mark.parametrize('path', ['_get_legacy', '_get_hardened'])
def test_sink_receives_malformed_collection_for_validation(path, monkeypatch):
    g, _, _ = _bare([_Resp({'id': 'not-a-page'})], monkeypatch)

    async def sink(page, url):
        if not isinstance(page.get('value'), list):
            raise ValueError('invalid collection')

    with pytest.raises(ValueError, match='invalid collection'):
        _run(getattr(g, path)('https://graph.microsoft.com/beta/x', on_page=sink))
