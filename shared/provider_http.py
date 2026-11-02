"""Verified Graph-client admission at the actual HTTP boundary.

The adapter never retries a request. Callers retain their operation-specific
retry and publication rules. Data identity comes from the credential owner;
only bounded workload/resource names are selected from approved routes.
"""
from __future__ import annotations

import asyncio
import httpx
import time
from datetime import timedelta
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from urllib.parse import unquote, urlsplit

from shared.graph_ratelimit import parse_retry_after
from shared.provider_admission import ProviderDeferred, ProviderRequestContext
from shared.provider_read import ProviderReadError

_active = ContextVar('provider_admitted_request', default=None)
_response_limit = ContextVar('provider_response_byte_limit', default=None)


@contextmanager
def response_byte_limit(limit):
    if limit is not None and (type(limit) is not int or limit < 1024):
        raise ValueError('invalid provider response byte budget')
    marker = _response_limit.set(limit)
    try:
        yield
    finally:
        _response_limit.reset(marker)


def has_response_byte_limit():
    return _response_limit.get() is not None


def _target(url):
    parsed = urlsplit(str(url))
    if (parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password
            or parsed.port not in (None, 443) or parsed.fragment):
        raise ValueError('invalid provider HTTPS target')
    return parsed


def identity_request(owner, url):
    parsed = _target(url)
    return (parsed.hostname == 'login.microsoftonline.com'
            and parsed.path in (f'/{owner.tenant_id}/oauth2/v2.0/token', f'/{owner.tenant_id}/oauth2/token'))


def request_context(owner, url, *, operation=None):
    parsed = _target(url)
    host, path = parsed.hostname.lower(), parsed.path
    workload, resource = 'directory', None
    if host == 'graph.microsoft.com':
        parts = [unquote(p) for p in path.split('/') if p]
        if not parts or parts[0] not in ('v1.0', 'beta'):
            raise ValueError('unsupported Graph data route')
        parts = parts[1:]
        lowered = [part.lower() for part in parts]
        if 'exchange' in lowered and 'mailboxes' in lowered:
            workload = 'exchange'
            i = lowered.index('mailboxes')
            resource = 'mailbox:' + parts[i+1] if len(parts) > i+1 else None
        elif 'chats' in parts:
            workload = 'teams'
            i = parts.index('chats')
            resource = 'chat:' + parts[i+1] if len(parts) > i+1 else None
        elif 'channels' in parts or (parts and parts[0] == 'teams'):
            workload = 'teams'
            resource = 'team:' + parts[1] if len(parts) > 1 else None
        elif any(p in parts for p in ('sites', 'drives', 'drive', 'shares')):
            workload = 'sharepoint'
            for kind in ('sites', 'drives'):
                if kind in parts:
                    i = parts.index(kind)
                    resource = kind[:-1] + ':' + parts[i+1] if len(parts) > i+1 else None
                    break
        elif any(p in parts for p in ('messages', 'mailFolders', 'contacts', 'contactFolders', 'events', 'calendars', 'mailboxSettings', 'mailbox')):
            workload = 'exchange'
            if 'users' in parts:
                i = parts.index('users')
                resource = 'mailbox:' + parts[i+1] if len(parts) > i+1 else None
        elif parts == ['$batch']:
            workload = 'batch'
    elif host.endswith(('.sharepoint.com', '.sharepoint.us')):
        workload = 'sharepoint'
        if '/_api/' in path:
            resource = 'site:' + host + path.split('/_api/', 1)[0]
    elif host in ('1drv.ms', '1drv.com', 'onedrive.com', 'onedrive.live.com', 'storage.live.com') or host.endswith((
            '.1drv.com', '.onedrive.com', '.onedrive.live.com', '.storage.live.com', '.livefilestore.com', '.sharepointonline.com')):
        workload = 'sharepoint'
        # A signed download URL does not establish a site identity. The tenant
        # and workload parent still enforce a common cap; never key by its token.
    else:
        raise ValueError('unsupported provider data host')
    if operation == 'archive_export':
        if workload != 'exchange' or not path.endswith('/exportItems'):
            raise ValueError('archive export requires its validated Exchange route')
        operation = None
    if operation is not None:
        # Only adapter-owned immutable contexts may supply opaque route scope.
        if not isinstance(operation, ProviderRequestContext) or (
            operation.tenant_id != owner.tenant_id or operation.app_id != owner.client_id):
            raise ValueError('provider operation identity does not match credential owner')
        return operation
    return ProviderRequestContext(owner.tenant_id, owner.client_id, workload, resource)


@asynccontextmanager
async def admitted(limiter, context, **kwargs):
    from shared.provider_runtime import provider_request_scope
    async with provider_request_scope():
        async with _admitted(limiter, context, **kwargs):
            yield


@asynccontextmanager
async def _admitted(limiter, context, **kwargs):
    active = _active.get()
    task = asyncio.current_task()
    if active == (task, context):
        yield
        return
    async with limiter.admit(context, **kwargs):
        marker = _active.set((task, context))
        try:
            yield
        finally:
            _active.reset(marker)


class AdmittedHttpClient:
    def __init__(self, raw, owner, *, limiter=None):
        if limiter is None:
            from shared.graph_rate_limiter import graph_rate_limiter
            limiter = graph_rate_limiter
        self.raw, self.owner, self.limiter = raw, owner, limiter

    def __getattr__(self, name):
        if name in ('send', 'build_request'):
            raise AttributeError('use admitted request or stream for provider data')
        return getattr(self.raw, name)

    async def _cooldown_after_send(self, context, delay, *, readonly, status):
        try:
            await self.limiter.cooldown(context, delay)
        except ProviderDeferred:
            # A mutation was already transmitted. Do not report it as unsent.
            if not readonly:
                raise ProviderReadError('provider_cooldown_unavailable', status=status, retry_after=delay) from None
            raise

    async def _response(self, response, context, method):
        if response.status_code not in (429, 500, 502, 503, 504):
            return
        delay = parse_retry_after(response.headers.get('Retry-After'))
        delay = max(1.0, delay if delay is not None else 5.0)
        await self._cooldown_after_send(context, delay, readonly=method.upper() in ('GET', 'HEAD'),
            status=response.status_code)
        if method.upper() in ('GET', 'HEAD') and delay > 1:
            await response.aclose()
            raise ProviderDeferred(context, delay, stage='provider_throttle')

    async def _batch(self, method, url, **kwargs):
        from shared.provider_runtime import account_provider_attempt, provider_request_scope
        context = request_context(self.owner, url)
        payload = kwargs.get('json')
        entries = payload.get('requests') if isinstance(payload, dict) else None
        if not isinstance(entries, list) or not 1 <= len(entries) <= 20:
            raise ValueError('Graph batch requires1..20 explicit subrequests')
        requests, costs, readonly = {}, [(context, 1)], True
        for item in entries:
            if not isinstance(item, dict) or not isinstance(item.get('id'), str) or item['id'] in requests:
                raise ValueError('invalid batch identity')
            route = item.get('url')
            if not isinstance(route, str) or not route.startswith('/') or route.startswith('//'):
                raise ValueError('batch subrequest must use a relative Graph route')
            verb = item.get('method', '').upper()
            if verb not in ('GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE'):
                raise ValueError('invalid batch method')
            readonly = readonly and verb in ('GET', 'HEAD')
            sub = request_context(self.owner, 'https://graph.microsoft.com/v1.0' + route)
            requests[item['id']] = sub
            costs.append((sub, 1))
        async with provider_request_scope():
            async with self.limiter.admit_many(costs):
                await account_provider_attempt(context)
                kwargs['follow_redirects'] = False
                async with self.raw.stream(method, url, **kwargs) as streamed:
                    if streamed.status_code in (429, 500, 502, 503, 504):
                        # Headers establish cooldown even when an error body is
                        # oversized. No response payload is required to defer.
                        response = httpx.Response(streamed.status_code, headers=streamed.headers,
                            request=streamed.request)
                    else:
                        response = await self._read_response(streamed, _response_limit.get() or 16 * 1024 * 1024)
                if response.status_code in (429, 500, 502, 503, 504):
                    delay = max(1.0, parse_retry_after(response.headers.get('Retry-After')) or 5.0)
                    for sub in set(requests.values()):
                        await self._cooldown_after_send(sub, delay, readonly=readonly, status=response.status_code)
                    await self._response(response, context, 'GET' if readonly else method)
                    return response
                if response.status_code != 200:
                    return response
                try:
                    results = response.json().get('responses')
                    if not isinstance(results, list):
                        raise ValueError
                    seen, deferred = set(), []
                    for result in results:
                        if (not isinstance(result, dict) or result.get('id') not in requests
                                or result['id'] in seen or type(result.get('status')) is not int):
                            raise ValueError
                        seen.add(result['id'])
                        if result['status'] in (429, 500, 502, 503, 504):
                            headers = httpx.Headers(result.get('headers') or {})
                            delay = max(1.0, parse_retry_after(headers.get('Retry-After')) or 5.0)
                            sub = requests[result['id']]
                            await self._cooldown_after_send(sub, delay, readonly=readonly, status=result['status'])
                            if readonly and delay > 1:
                                deferred.append(ProviderDeferred(sub, delay, stage='provider_batch_throttle'))
                    if seen != set(requests):
                        raise ValueError
                except (ValueError, TypeError, AttributeError):
                    raise ProviderReadError('invalid_batch_response') from None
                if deferred:
                    await response.aclose()
                    raise max(deferred, key=lambda error: error.retry_after)
                return response

    @staticmethod
    async def _read_response(response, limit):
        if limit is None:
            await response.aread()
            return response
        body = bytearray()
        async for chunk in response.aiter_bytes(chunk_size=min(65536, limit + 1)):
            if len(body) + len(chunk) > limit:
                raise ProviderReadError('response_byte_limit', status=response.status_code)
            body.extend(chunk)
        headers = dict(response.headers)
        for header in ('content-encoding', 'content-length', 'transfer-encoding'):
            headers.pop(header, None)
        return httpx.Response(response.status_code, headers=headers,
            content=bytes(body), request=response.request, extensions=response.extensions)

    @asynccontextmanager
    async def _stream_hops(self, method, url, **kwargs):
        from shared.provider_runtime import account_provider_attempt
        original = request_context(self.owner, url)
        follow = kwargs.pop('follow_redirects', self.raw.follow_redirects)
        # Restore mutations retain their explicit operation-specific redirect policy.
        follow = bool(follow and method.upper() in ('GET', 'HEAD'))
        auth = kwargs.pop('auth', httpx.USE_CLIENT_DEFAULT)
        request = self.raw.build_request(method, url, **kwargs)
        for hop in range(6):
            # Validate every target, including preauthenticated download redirects.
            request_context(self.owner, request.url)
            # Opaque CDN routes retain the verified originating workload/resource.
            async with admitted(self.limiter, original):
                await account_provider_attempt(original)
                response = await self.raw.send(request, stream=True, follow_redirects=False, auth=auth)
                try:
                    await self._response(response, original, request.method)
                    next_request = response.next_request if follow else None
                    if next_request is None:
                        yield response
                        return
                    if hop == 5:
                        raise ProviderReadError('provider_redirect_limit', status=response.status_code)
                    # httpx builds redirect headers and strips cross-origin auth.
                    request = next_request
                    auth = None
                finally:
                    await response.aclose()
        raise ProviderReadError('provider_redirect_limit')

    async def request(self, method, url, **kwargs):
        if identity_request(self.owner, url):
            return await self.raw.request(method, url, **kwargs)
        if _target(url).path in ('/v1.0/$batch', '/beta/$batch'):
            return await self._batch(method, url, **kwargs)
        started = time.monotonic()
        async with self._stream_hops(method, url, **kwargs) as streamed:
            response = await self._read_response(streamed, _response_limit.get())
        response.elapsed = timedelta(seconds=time.monotonic() - started)
        return response

    @asynccontextmanager
    async def stream(self, method, url, **kwargs):
        async with self._stream_hops(method, url, **kwargs) as response:
            yield response

    async def get(self, url, **kwargs):
        return await self.request('GET', url, **kwargs)

    async def post(self, url, **kwargs):
        return await self.request('POST', url, **kwargs)

    async def put(self, url, **kwargs):
        return await self.request('PUT', url, **kwargs)

    async def patch(self, url, **kwargs):
        return await self.request('PATCH', url, **kwargs)

    async def delete(self, url, **kwargs):
        return await self.request('DELETE', url, **kwargs)

    async def head(self, url, **kwargs):
        return await self.request('HEAD', url, **kwargs)


def download_client(raw, graph_client):
    """Signed URLs retain the verified originating client's credential scope."""
    from shared.graph_rate_limiter import graph_rate_limiter
    if not graph_rate_limiter.scoped_enabled:
        return raw
    if graph_client is None:
        raise ProviderReadError('missing_download_context', retry_after=5)
    return AdmittedHttpClient(raw, graph_client)


async def bounded_error_body(response, limit=16384):
    body = bytearray()
    async for block in response.aiter_bytes(chunk_size=4096):
        if len(body) + len(block) > limit:
            return b''
        body.extend(block)
    return bytes(body)
