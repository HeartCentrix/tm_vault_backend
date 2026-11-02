"""Archive export read adapter with an actual streamed response-byte ceiling."""
from __future__ import annotations

import asyncio
import json
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


async def export_items(graph, url, ids, *, timeout, byte_limit=16 * 1024 * 1024):
    """Only exportItems is a repeatable POST. No mutation uses this adapter.

    The byte ceiling is checked while streaming, before JSON parse or base64
    decode. Callers split oversized multi-item batches; a singleton remains an
    explicit unresolved receipt until a spooled export adapter is available.
    """
    if not ids or len(ids) > 20 or byte_limit < 1024:
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
                            body = bytearray()
                            async for chunk in response.aiter_bytes(chunk_size=64 * 1024):
                                if len(body) + len(chunk) > byte_limit:
                                    raise ArchiveExportOversize()
                                body.extend(chunk)
                            try:
                                data = json.loads(body)
                            except (ValueError, UnicodeError):
                                retry = ProviderReadError('malformed_export', attempts=budget.attempts)
                            else:
                                if not isinstance(data, dict) or not isinstance(data.get('value'), list):
                                    raise ProviderReadError('malformed_export_collection')
                                return data
        except httpx.TransportError:
            retry = ProviderReadError('archive_export_transport', attempts=budget.attempts)
        if retry is not None:
            # Close the response, client context and physical admission before delay.
            await asyncio.sleep(budget.retry_delay(retry))
