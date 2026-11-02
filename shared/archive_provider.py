"""Archive export read adapter with an actual streamed response-byte ceiling."""
from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
import tempfile
from urllib.parse import urlsplit

import httpx

from shared.archive_capture import canonical_base, ArchivePageError
from shared.provider_read import ProviderReadBudget, ProviderReadError


class ArchiveExportOversize(ProviderReadError):
    def __init__(self):
        super().__init__('export_response_byte_limit')


def export_base(url, expected):
    if not isinstance(url, str) or not url.endswith('/exportItems'):
        raise ArchivePageError('invalid archive export route')
    return canonical_base(url[:-len('/exportItems')] + '/folders', expected=expected)


async def _request_export(graph, url, ids, *, timeout, read_body):
    """Only exportItems is a repeatable POST; no restore mutation uses this path."""
    if not ids or len(ids) > 20:
        raise ValueError('invalid archive export envelope')
    original_base = export_base(url, None)
    budget = ProviderReadBudget()
    auth_refreshed = False
    redirects = 0
    while True:
        budget.attempts += 1
        token = await graph._get_token()
        retry = None
        try:
            async with graph._admit_data_request(url, method='POST', operation='archive_export'):
                async with graph._http_session() as client:
                    async with client.stream('POST', url, headers={'Authorization': f'Bearer {token}'},
                            json={'itemIds': ids}, timeout=timeout, follow_redirects=False) as response:
                        if response.status_code == 308:
                            target = response.headers.get('Location')
                            export_base(target, original_base)
                            if redirects >= 2 or target == url:
                                raise ProviderReadError('export_redirect_limit', status=308)
                            redirects += 1
                            url = target
                        elif response.status_code == 401 and not auth_refreshed:
                            auth_refreshed = True
                            graph._access_token = None
                        elif response.status_code in (429, 500, 502, 503, 504):
                            retry = ProviderReadError.from_response(response, 'archive_export', attempts=budget.attempts)
                        elif response.status_code >= 400:
                            raise ProviderReadError.from_response(response, 'archive_export', attempts=budget.attempts)
                        elif response.status_code != 200:
                            raise ProviderReadError('unexpected_export_status', status=response.status_code)
                        else:
                            return await read_body(response)
        except httpx.TransportError:
            retry = ProviderReadError('archive_export_transport', attempts=budget.attempts)
        if retry is not None:
            # Close the response, client context and physical admission before delay.
            try:
                delay = budget.retry_delay(retry)
            except ProviderReadError:
                from shared.provider_runtime import current_execution, defer_provider
                if current_execution() is not None:
                    from shared.provider_admission import ProviderDeferred
                    from shared.provider_http import request_context
                    defer_provider(ProviderDeferred(request_context(graph, url, operation='archive_export'),
                        max(1.0, retry.retry_after or 5), stage=retry.stage))
                raise
            await asyncio.sleep(delay)


async def export_items(graph, url, ids, *, timeout, byte_limit=16 * 1024 * 1024):
    """Bounded in-memory compatibility reader for small diagnostic responses."""
    if byte_limit < 1024:
        raise ValueError('invalid archive export envelope')
    async def read(response):
        body = bytearray()
        async for chunk in response.aiter_bytes(chunk_size=65536):
            if len(body) + len(chunk) > byte_limit:
                raise ArchiveExportOversize()
            body.extend(chunk)
        try:
            data = json.loads(body)
        except (ValueError, UnicodeError):
            raise ProviderReadError('malformed_export') from None
        if not isinstance(data, dict) or not isinstance(data.get('value'), list):
            raise ProviderReadError('malformed_export_collection')
        return data
    return await _request_export(graph, url, ids, timeout=timeout, read_body=read)


@asynccontextmanager
async def export_items_spooled(graph, url, ids, *, timeout, metadata_limit=4 * 1024 * 1024):
    """Keep opaque bytes on disk through upload/receipt, then remove all spools."""
    from shared.archive_spool import ExportSpoolDecoder
    with tempfile.TemporaryDirectory(prefix='tmvault-archive-') as directory:
        async def read(response):
            with ExportSpoolDecoder(directory, metadata_limit=metadata_limit) as parser:
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    parser.feed(chunk)
                return parser.finish()
        yield await _request_export(graph, url, ids, timeout=timeout, read_body=read)
