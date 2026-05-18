"""Microsoft Graph /v1.0/$batch wrapper.

Bundles up to 20 non-paginated GET sub-requests into one HTTP call.
Retries 429 sub-responses in a follow-up batch honoring each sub's
Retry-After. Rejects endpoints known to paginate (delta, $skiptoken,
$top) at submission time — $batch cannot follow @odata.nextLink inside
a sub-response and using it for paged data silently truncates.

Outlook caveat: inside a single batch, Graph serializes Outlook
sub-requests 4-at-a-time. Batch still saves outer-bucket cost (one
throttle accounting per batch) but does not linearly parallelize mail
reads. See:
https://learn.microsoft.com/en-us/graph/throttling

Spec: docs/superpowers/specs/2026-04-19-graph-api-throttle-hardening-design.md

This module exposes two entry points:

* ``BatchClient`` — production-grade class wrapping a ``GraphClient``.
  Handles auth, rate-limit policy, 429 sub-response retry. Use this
  for live Graph traffic.

* ``batch_requests(post, requests)`` — thin functional helper that
  takes a caller-supplied POST coroutine. Used by engines (e.g.
  EntraRestoreEngine) that stub Graph at the ``_post`` boundary for
  testing and don't need the full rate-limit machinery.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional

import httpx

from shared.graph_ratelimit import (
    GraphRetryExhaustedError, parse_retry_after,
)


GRAPH_BATCH_URL = "https://graph.microsoft.com/v1.0/$batch"
_BATCH_CHUNK_SIZE = 20

# Sub-paths that return paginated responses. $batch sub-requests cannot
# follow their @odata.nextLink — using them silently truncates.
_PAGINATED_MARKERS = ("/delta", "$skiptoken=", "$top=")


@dataclass
class BatchRequest:
    """One logical operation inside a /$batch POST. `id` MUST be unique
    within the surrounding batch so we can re-order the responses back
    to input order."""
    id: str
    method: str
    url: str
    # Original BatchClient callers pass headers as a dict with a default
    # factory. `batch_requests` callers pass body via the keyword.
    headers: Dict[str, str] = field(default_factory=dict)
    body: Optional[dict] = None


@dataclass
class BatchResponse:
    id: str
    status: int
    headers: Dict[str, str]
    body: dict


class BatchClient:
    """Async wrapper around /v1.0/$batch."""

    def __init__(self, graph_client):
        self._gc = graph_client

    @staticmethod
    def validate_requests(requests: List[BatchRequest]) -> None:
        for r in requests:
            if any(marker in r.url for marker in _PAGINATED_MARKERS):
                raise ValueError(
                    f"BatchRequest url {r.url!r} looks paginated; "
                    f"$batch sub-responses cannot follow @odata.nextLink. "
                    f"Use GraphClient._iter_pages instead."
                )

    async def batch(
        self, requests: List[BatchRequest],
    ) -> Dict[str, BatchResponse]:
        from shared.config import settings as s
        self.validate_requests(requests)
        result: Dict[str, BatchResponse] = {}
        chunk_size = s.GRAPH_BATCH_MAX_SIZE
        for i in range(0, len(requests), chunk_size):
            chunk = requests[i:i + chunk_size]
            sub_result = await self._send_chunk_with_retry(chunk)
            result.update(sub_result)
        return result

    async def _send_chunk_with_retry(
        self, chunk: List[BatchRequest],
    ) -> Dict[str, BatchResponse]:
        from shared.config import settings as s
        from shared.multi_app_manager import multi_app_manager
        pending = list(chunk)
        collected: Dict[str, BatchResponse] = {}
        attempts = 0
        # Track which app's token is being used for this chunk's lifetime.
        # On 429/503 we try to swap to a healthier app before sleeping —
        # /$batch hits Graph's per-app throttle the same as single requests
        # do, so 30s of Retry-After is pure waste when another app's
        # bucket has full credit. Migration is tried up to N times per
        # chunk (one per retry); if no healthy alt app exists we fall
        # back to the original sleep behaviour.
        current_token: Optional[str] = None
        current_app_id: str = self._gc.client_id
        while pending and attempts < s.GRAPH_MAX_RETRIES + 1:
            attempts += 1
            responses = await self._send_once(
                pending, override_token=current_token,
                override_app_id=current_app_id,
            )
            failed: List[BatchRequest] = []
            max_retry_after = 0.0
            by_id = {r.id: r for r in pending}
            any_throttle = False
            for resp in responses:
                if resp.status in (429, 503):
                    any_throttle = True
                    ra = parse_retry_after(resp.headers.get("Retry-After"))
                    if ra is not None:
                        max_retry_after = max(max_retry_after, ra)
                    failed.append(by_id[resp.id])
                else:
                    collected[resp.id] = resp
            pending = failed
            if pending:
                # Mark the current app throttled (per-app accounting)
                # then attempt migration. The "any other healthy app"
                # case is the common one with 20 apps registered.
                multi_app_manager.mark_throttled(
                    current_app_id, int(max(max_retry_after, 1.0)),
                )
                new_token, new_app = await self._gc._try_migrate_app(current_app_id)
                if new_token and new_app:
                    current_token = new_token
                    current_app_id = new_app
                    # No sleep — retry immediately on the new app's
                    # token. Graph per-app caps are independent.
                    continue
                # Fallback: all other apps throttled too OR single-app
                # deployment. Honor Retry-After.
                await asyncio.sleep(max(max_retry_after, 1.0))
            else:
                if any_throttle is False and current_app_id != self._gc.client_id:
                    # Migrated mid-chunk and the new app served clean —
                    # credit it so adaptive recovery can lift its rate.
                    try:
                        multi_app_manager.mark_success(current_app_id, 0.0)
                    except Exception:
                        pass
        # Any still-failing requests: return as-is with their last 429 so
        # the caller can decide whether to surface or skip.
        for req in pending:
            collected[req.id] = BatchResponse(
                id=req.id, status=429, headers={}, body={},
            )
        return collected

    async def _send_once(
        self, chunk: List[BatchRequest],
        *,
        override_token: Optional[str] = None,
        override_app_id: Optional[str] = None,
    ) -> List[BatchResponse]:
        """Send one /$batch POST.

        ``override_token`` / ``override_app_id`` let the retry loop send
        through a different app's token after migration. When unset
        (the common first-attempt case), we use the parent GraphClient's
        own credentials.
        """
        policy = self._gc._policy
        await policy.stream_bucket.acquire()
        from shared.multi_app_manager import multi_app_manager
        effective_app = override_app_id or self._gc.client_id
        await multi_app_manager.acquire_app_token(effective_app)

        token = override_token or await self._gc._get_token()
        payload = {
            "requests": [
                {
                    "id": r.id, "method": r.method, "url": r.url,
                    **({"headers": r.headers} if r.headers else {}),
                    **({"body": r.body} if r.body is not None else {}),
                }
                for r in chunk
            ]
        }
        # PERF (Item A): route /$batch through the SHARED httpx client so
        # batch POSTs ride the same HTTP/2 connection as the per-sub-request
        # GETs. The previous per-call httpx.AsyncClient() forced a fresh
        # TCP+TLS handshake on every batch — adding 80-150ms per batch on
        # WAN. With HTTP/2 the batch is just one more multiplexed stream.
        try:
            client = await self._gc._get_shared_http()
            resp = await client.post(
                GRAPH_BATCH_URL, json=payload,
                headers={"Authorization": f"Bearer {token}"},
                timeout=60.0,
            )
        except AttributeError:
            # _gc didn't expose the shared http (unusual — only stubs in
            # tests). Fall back to a fresh client so the batch still goes.
            async with httpx.AsyncClient(timeout=60.0) as _fallback:
                resp = await _fallback.post(
                    GRAPH_BATCH_URL, json=payload,
                    headers={"Authorization": f"Bearer {token}"},
                )
        if resp.status_code != 200:
            # Outer batch itself rejected — e.g., whole app throttled.
            # Treat as 429 on every sub-request so the retry loop handles it.
            return [
                BatchResponse(
                    id=r.id, status=resp.status_code,
                    headers=dict(resp.headers),
                    body={},
                )
                for r in chunk
            ]
        data = resp.json() or {}
        out: List[BatchResponse] = []
        for sub in (data.get("responses") or []):
            out.append(BatchResponse(
                id=str(sub.get("id")),
                status=int(sub.get("status", 200)),
                headers=dict(sub.get("headers") or {}),
                body=sub.get("body") or {},
            ))
        return out


# ---- Functional helper used by engines that stub Graph at _post ---------

PostFn = Callable[[str, Dict[str, Any]], Awaitable[Dict[str, Any]]]


async def batch_requests(post: PostFn, requests: List[BatchRequest]) -> List[Dict[str, Any]]:
    """POST ``requests`` to Graph's /$batch endpoint in chunks of 20.
    Returns a flat list of sub-response dicts in input order. Each
    response dict has at least ``status`` and ``body``.

    Use ``BatchClient`` for production code that needs rate-limit +
    retry semantics. Use ``batch_requests`` for engines that stub
    Graph at the ``_post`` boundary for unit testing."""
    if not requests:
        return []

    out: List[Dict[str, Any]] = [None] * len(requests)  # type: ignore[list-item]
    id_to_index: Dict[str, int] = {r.id: i for i, r in enumerate(requests)}

    for chunk_start in range(0, len(requests), _BATCH_CHUNK_SIZE):
        chunk = requests[chunk_start:chunk_start + _BATCH_CHUNK_SIZE]
        payload = {
            "requests": [
                {
                    "id": r.id,
                    "method": r.method,
                    "url": r.url,
                    **({"body": r.body} if r.body is not None else {}),
                    **({"headers": r.headers} if r.headers else {}),
                }
                for r in chunk
            ],
        }
        resp = await post(GRAPH_BATCH_URL, payload)
        for item in (resp or {}).get("responses", []) or []:
            idx = id_to_index.get(item.get("id"))
            if idx is not None:
                out[idx] = item
    return out
