"""Strict, read-only Graph source for immutable chat captures.

The scope hash records owned client configuration; it does not prove granted
permissions or effective visibility. Access tokens remain opaque. Each capture
still enumerates its own source membership.
"""
from __future__ import annotations

import asyncio
import base64
from contextlib import asynccontextmanager
import hashlib
import json
from typing import AsyncIterator
from urllib.parse import parse_qsl, quote, unquote, urlencode, urljoin, urlsplit

import httpx

from shared.graph_client import GraphClient
from shared.graph_ratelimit import parse_retry_after


class ChatGraphError(RuntimeError):
    """Capture cannot establish complete source coverage; URLs are redacted."""

    def __init__(self, message: str, *, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class _PinnedGraphClient(GraphClient):
    async def _try_migrate_app(self, throttled_app_id):
        return None, None

    async def _get_shared_http(self):
        if self._http is None or self._http.is_closed:
            async with self._http_lock:
                if self._http is None or self._http.is_closed:
                    self._http = httpx.AsyncClient(
                        follow_redirects=False,
                        timeout=httpx.Timeout(connect=30, read=120, write=30, pool=30),
                        limits=httpx.Limits(max_connections=16, max_keepalive_connections=16),
                    )
        return self._http


def _target(url: str, *, graph: bool) -> None:
    try:
        parsed = urlsplit(url)
        host = (parsed.hostname or "").lower()
        valid = parsed.scheme == "https" and parsed.port in (None, 443)
        valid = valid and not parsed.username and not parsed.password and not parsed.fragment
        if graph:
            valid = valid and host == "graph.microsoft.com" and parsed.path.startswith("/v1.0/")
        else:
            valid = valid and (host in {"1drv.ms", "1drv.com", "onedrive.com", "onedrive.live.com", "storage.live.com"} or any(host.endswith(suffix) for suffix in (
                ".sharepoint.com", ".sharepoint.us", ".1drv.com",
                ".onedrive.com", ".onedrive.live.com", ".storage.live.com",
                ".livefilestore.com", ".sharepointonline.com",
            )))
    except (TypeError, ValueError):
        valid = False
    if not valid:
        raise ChatGraphError("untrusted HTTPS capture target")


def _component(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ChatGraphError("missing source identity")
    return quote(value, safe="")


def _path_scope(path: str) -> tuple[str, ...]:
    # Decode per segment: an encoded slash inside an ID stays distinct from
    # an actual path separator, while equivalent percent encodings compare equal.
    try:
        return tuple(unquote(part, errors="strict") for part in path.split("/"))
    except UnicodeError:
        raise ChatGraphError("malformed Graph page cursor path") from None


def _content_length(response: httpx.Response) -> int | None:
    raw = response.headers.get("Content-Length")
    if raw is None:
        return None
    try:
        length = int(raw)
    except ValueError:
        raise ChatGraphError("invalid content length") from None
    if length < 0:
        raise ChatGraphError("invalid content length")
    return length


def permission_scope_hash_for(*, tenant_id: str, application_id: str, principal_id: str) -> str:
    """Hash owned credentials' configuration; this does not attest actual grants."""
    for value in (tenant_id, application_id, principal_id):
        _component(value)
    context = {"tenant": tenant_id, "application": application_id,
               "principal": principal_id, "auth_mode": "client_credentials",
               "requested_scope": "https://graph.microsoft.com/.default"}
    return hashlib.sha256(json.dumps(context, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def effective_capture_scope_hash(base_hash: str, scope_filter_hash: str | None = None) -> str:
    """Include the configured source-selection policy in capture identity."""
    if not isinstance(base_hash, str) or not base_hash:
        raise ValueError("base source scope hash is required")
    if scope_filter_hash is None:
        return base_hash
    if not isinstance(scope_filter_hash, str) or not scope_filter_hash:
        raise ValueError("source filter hash must be nonempty")
    return hashlib.sha256(json.dumps(["chat-capture-filter-v1", base_hash, scope_filter_hash],
        separators=(",", ":")).encode()).hexdigest()


class StrictChatGraphSource:
    """Pinned application source. Construct through ``pinned_chat_source``."""

    def __init__(self, client: _PinnedGraphClient, *, principal_id: str):
        _component(principal_id)
        self._client = client
        self.principal_id = principal_id
        self._initial_app = client.client_id
        self._initial_tenant = client.tenant_id
        self._initial_secret = client.client_secret
        self.permission_scope_hash = permission_scope_hash_for(
            tenant_id=self._initial_tenant, application_id=self._initial_app, principal_id=principal_id)
        self._closed = False
        self._pacing_policy = client._policy

    async def _context(self) -> str:
        if self._closed:
            raise ChatGraphError("capture source is closed")
        if (self._client.client_id, self._client.tenant_id, self._client.client_secret) != (self._initial_app, self._initial_tenant, self._initial_secret):
            raise ChatGraphError("capture authorization context changed")
        try:
            token = await self._client._get_token()
            if not isinstance(token, str) or not token:
                raise ValueError
        except Exception:
            raise ChatGraphError("capture authorization token unavailable") from None
        return token

    async def _pace(self):
        from shared.graph_rate_limiter import graph_rate_limiter
        await graph_rate_limiter.acquire(reason="chat_catalogue_capture")
        policy = self._pacing_policy
        await policy.stream_bucket.acquire()
        await policy.app_bucket.acquire()

    async def _pause(self, response: httpx.Response | None, attempt: int):
        delay = parse_retry_after(response.headers.get("Retry-After")) if response is not None else None
        await asyncio.sleep(min(120, max(0, delay if delay is not None else 2 ** attempt)))

    async def _json(self, url: str, *, params=None) -> dict:
        _target(url, graph=True)
        for attempt in range(5):
            token = await self._context()
            client = await self._client._get_shared_http()
            await self._pace()
            try:
                response = await client.get(url, params=params, headers={"Authorization": f"Bearer {token}"}, follow_redirects=False)
            except httpx.TransportError:
                if attempt == 4:
                    raise ChatGraphError("Graph capture transport retries exhausted") from None
                await self._pause(None, attempt)
                continue
            await self._context()
            if response.status_code == 401 and attempt < 4:
                self._client._access_token = None
                continue
            if response.status_code in (429, 500, 502, 503, 504) and attempt < 4:
                await self._pause(response, attempt)
                continue
            if response.status_code != 200:
                raise ChatGraphError(f"Graph capture failed with HTTP {response.status_code}", status_code=response.status_code)
            try:
                data = response.json()
            except ValueError:
                raise ChatGraphError("malformed Graph capture response") from None
            if not isinstance(data, dict):
                raise ChatGraphError("malformed Graph capture response")
            return data
        raise ChatGraphError("Graph capture retries exhausted")

    async def _pages(self, path: str, *, params=None) -> AsyncIterator[list[dict]]:
        url = self._client.GRAPH_URL + path
        collection_path = _path_scope(urlsplit(url).path)
        seen = set()
        while True:
            parsed = urlsplit(url)
            canonical = parsed._replace(
                path="/".join(quote(part, safe="") for part in _path_scope(parsed.path)),
                query=urlencode(sorted(parse_qsl(parsed.query, keep_blank_values=True))),
            ).geturl()
            fingerprint = hashlib.sha256(canonical.encode()).digest()
            if fingerprint in seen:
                raise ChatGraphError("Graph capture cursor cycle")
            seen.add(fingerprint)
            data = await self._json(url, params=params)
            values = data.get("value")
            if "error" in data or not isinstance(values, list) or any(not isinstance(row, dict) or not isinstance(row.get("id"), str) or not row["id"] for row in values):
                raise ChatGraphError("malformed Graph collection page")
            if "@odata.count" in data:
                count = data["@odata.count"]
                if type(count) is not int or count < 0:
                    raise ChatGraphError("malformed Graph page count")
                # Teams may emit per-page counts; completion follows nextLink exhaustion.
            next_url = data.get("@odata.nextLink")
            if "@odata.nextLink" in data and (not isinstance(next_url, str) or not next_url):
                raise ChatGraphError("malformed Graph page cursor")
            if next_url is not None:
                _target(next_url, graph=True)
                if _path_scope(urlsplit(next_url).path) != collection_path:
                    raise ChatGraphError("Graph page cursor changed collection scope")
            yield values
            if next_url is None:
                return
            url, params = next_url, None

    async def iter_chats(self) -> AsyncIterator[dict]:
        async for page in self._pages(f"/users/{_component(self.principal_id)}/chats", params={"$top": "50"}):
            for chat in page:
                yield chat

    async def iter_message_pages(self, chat_id: str) -> AsyncIterator[list[dict]]:
        path = f"/users/{_component(self.principal_id)}/chats/{_component(chat_id)}/messages"
        async for page in self._pages(path, params={"$top": "50"}):
            if len(page) > 50 or any(row.get("chatId", chat_id) != chat_id for row in page):
                raise ChatGraphError("Graph message page changed collection scope")
            yield page

    async def iter_hosted_contents(self, chat_id: str, message_id: str) -> AsyncIterator[dict]:
        path = f"/users/{_component(self.principal_id)}/chats/{_component(chat_id)}/messages/{_component(message_id)}/hostedContents"
        async for page in self._pages(path):
            for item in page:
                yield item

    @staticmethod
    def _file_identity(item: dict) -> tuple[str, str]:
        parent = item.get("parentReference")
        drive = parent.get("driveId") if isinstance(parent, dict) else None
        item_id = item.get("id")
        if not isinstance(drive, str) or not drive or not isinstance(item_id, str) or not item_id:
            raise ChatGraphError("Graph file identity unavailable")
        return drive, item_id

    async def resolve_file(self, source_url: str) -> dict:
        _target(source_url, graph=False)
        share = "u!" + base64.urlsafe_b64encode(source_url.encode()).decode().rstrip("=")
        item = await self._json(f"{self._client.GRAPH_URL}/shares/{share}/driveItem")
        self._file_identity(item)
        return item

    async def refresh_file(self, drive_id: str, item_id: str) -> dict:
        item = await self._json(f"{self._client.GRAPH_URL}/drives/{_component(drive_id)}/items/{_component(item_id)}")
        if self._file_identity(item) != (drive_id, item_id):
            raise ChatGraphError("Graph file identity changed")
        return item

    @asynccontextmanager
    async def _stream(self, url: str, *, graph: bool, expected_size: int | None = None):
        token = await self._context()
        client = await self._client._get_shared_http()
        headers = {"Accept-Encoding": "identity"}
        if graph:
            headers["Authorization"] = f"Bearer {token}"
        redirects = 0
        attempt = 0
        while True:
            _target(url, graph=graph)
            if graph:
                headers["Authorization"] = f"Bearer {await self._context()}"
                await self._pace()
            try:
                async with client.stream("GET", url, headers=headers, follow_redirects=False) as response:
                    await self._context()
                    if response.status_code in (301, 302, 303, 307, 308):
                        location = response.headers.get("Location")
                        if graph or not location or redirects >= 5:
                            raise ChatGraphError("capture content redirect refused")
                        url = urljoin(url, location)
                        _target(url, graph=False)
                        redirects += 1
                        continue
                    if response.status_code in (429, 500, 502, 503, 504) and attempt < 4:
                        # Close this response before waiting and retrying.
                        retry_response = response
                    else:
                        retry_response = None
                        if response.status_code != 200:
                            raise ChatGraphError(f"capture content failed with HTTP {response.status_code}", status_code=response.status_code)
                        if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                            raise ChatGraphError("unexpected capture content encoding")
                        length = _content_length(response)
                        if length is not None and expected_size is not None and length != expected_size:
                            raise ChatGraphError("capture content length changed")
                        expected = expected_size if expected_size is not None else length

                        async def chunks():
                            total = 0
                            try:
                                async for chunk in response.aiter_bytes():
                                    total += len(chunk)
                                    if expected is not None and total > expected:
                                        raise ChatGraphError("capture content length exceeded")
                                    yield chunk
                                if expected is not None and total != expected:
                                    raise ChatGraphError("capture content length incomplete")
                                await self._context()
                            except httpx.HTTPError:
                                raise ChatGraphError("capture content stream interrupted") from None

                        iterator = chunks()
                        try:
                            yield iterator, expected, response.headers.get("Content-Type", "application/octet-stream")
                        finally:
                            await iterator.aclose()
                        return
                await self._pause(retry_response, attempt)
                attempt += 1
            except httpx.HTTPError:
                raise ChatGraphError("capture content request failed") from None

    @asynccontextmanager
    async def open_file(self, drive_item: dict):
        url = drive_item.get("@microsoft.graph.downloadUrl")
        if not isinstance(url, str) or not url:
            raise ChatGraphError("Graph file download target unavailable")
        size = drive_item.get("size")
        if size is not None and (type(size) is not int or size < 0):
            raise ChatGraphError("invalid Graph file content length")
        async with self._stream(url, graph=False, expected_size=size) as opened:
            yield opened

    @asynccontextmanager
    async def open_hosted(self, chat_id: str, message_id: str, hcid: str):
        path = f"/users/{_component(self.principal_id)}/chats/{_component(chat_id)}/messages/{_component(message_id)}/hostedContents/{_component(hcid)}/$value"
        async with self._stream(self._client.GRAPH_URL + path, graph=True) as opened:
            yield opened


@asynccontextmanager
async def pinned_chat_source(base_client: GraphClient, *, principal_id: str):
    """Own a fresh credential/pool context, without changing the cached client."""
    client = _PinnedGraphClient(base_client.client_id, base_client.client_secret, base_client.tenant_id)
    source = StrictChatGraphSource(client, principal_id=principal_id)
    try:
        await source._context()
        yield source
    finally:
        source._closed = True
        await client.aclose()
