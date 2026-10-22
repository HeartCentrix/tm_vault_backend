"""Admin storage API: status, toggle, events, SSE stream, abort.

Mounted into api-gateway/main.py via include_router. Reads (status/backends/
events/stream) require any authenticated user; the state-changing toggle/abort
routes require an ADMIN role (Ops/Data Check 17). The SSE endpoint also accepts the JWT via a
`?token=<jwt>` query param since browsers' EventSource() API cannot set
custom Authorization headers.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import aio_pika
import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Request
from sse_starlette.sse import EventSourceResponse

from shared.schemas import (
    StorageBackendOut,
    ToggleRequest,
    ToggleStatusOut,
)
from shared.security import _get_user_from_token
from shared.storage.labels import describe_backend

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin/storage", tags=["admin-storage"])


async def _require_user_from_header_or_query(request: Request) -> dict:
    """Auth dep that reads the JWT from (in order) the Authorization
    header, a `?token=<jwt>` query param, or the `access_token` HttpOnly
    cookie set by /auth/callback. The query-param fallback exists so the
    SSE `/events/{id}/stream` endpoint can be consumed by browser
    EventSource() objects, which cannot attach custom Authorization
    headers. The cookie fallback exists because admin_storage is mounted
    directly on the gateway (not behind proxy_request), so the
    cookie→Bearer translation in main.py:proxy_request never runs for
    these routes — without this fallback, the SPA's cookie-only fetch()
    calls would 401. This dep authenticates only; toggle/abort additionally
    require admin via _require_admin_from_header_or_query."""
    auth = request.headers.get("authorization") or ""
    token: str | None = None
    if auth.lower().startswith("bearer "):
        token = auth[7:].strip()
    if not token:
        token = request.query_params.get("token")
    if not token:
        token = request.cookies.get("access_token")
    if not token:
        raise HTTPException(status_code=401, detail="missing bearer token")
    return _get_user_from_token(token)


async def _require_admin_from_header_or_query(request: Request) -> dict:
    """Ops/Data Check 17: flipping the platform storage backend drains jobs,
    promotes the replica DB and puts the whole fleet read-only during the
    transition — it must NOT be reachable by every authenticated employee.
    Require an admin role on the state-changing toggle/abort routes."""
    user = await _require_user_from_header_or_query(request)
    from shared.auth_enforcement import ADMIN_ROLES
    if not any(r in ADMIN_ROLES for r in (user.get("roles") or [])):
        raise HTTPException(status_code=403, detail="Administrator role required")
    return user


def _dsn() -> str:
    dsn = os.getenv("DATABASE_URL")
    if dsn:
        return dsn
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    u = os.getenv("DB_USERNAME", "postgres")
    p = os.getenv("DB_PASSWORD", "")
    db = os.getenv("DB_NAME", "postgres")
    return f"postgresql://{u}:{p}@{host}:{port}/{db}"


def _schema() -> str:
    return os.getenv("DB_SCHEMA", "tm")


def _rmq_url() -> str:
    url = os.getenv("RABBITMQ_URL")
    if url:
        return url
    # Refuse to publish to storage.toggle with the default RabbitMQ
    # guest:guest credential. The publisher half of the same attack: an
    # operator who forgets to set RABBITMQ_PASSWORD would otherwise have
    # this gateway endpoint authenticating to the broker as `guest`, which
    # is the credential built into every stock RabbitMQ image.
    u = os.getenv("RABBITMQ_USERNAME") or os.getenv("RABBITMQ_USER")
    p = os.getenv("RABBITMQ_PASSWORD")
    h = os.getenv("RABBITMQ_HOST")
    port = os.getenv("RABBITMQ_PORT", "5672")
    missing = [name for name, val in (
        ("RABBITMQ_USERNAME (or RABBITMQ_USER)", u),
        ("RABBITMQ_PASSWORD", p),
        ("RABBITMQ_HOST", h),
    ) if not val]
    if missing:
        raise RuntimeError(
            "Missing required RabbitMQ env vars: "
            + ", ".join(missing)
            + ". Set RABBITMQ_URL, or all of "
            "RABBITMQ_USERNAME/RABBITMQ_PASSWORD/RABBITMQ_HOST."
        )
    if u == "guest" and p == "guest":
        raise RuntimeError(
            "Refusing to connect to RabbitMQ with the default guest:guest "
            "credential. Provision a dedicated user for api-gateway."
        )
    return f"amqp://{u}:{p}@{h}:{port}/"


async def _connect():
    conn = await asyncpg.connect(_dsn())
    await conn.execute(f"SET search_path TO {_schema()}, public")
    return conn


def _row_to_dict(row: Any) -> dict:
    return {k: (v.isoformat() if hasattr(v, "isoformat") else v)
            for k, v in dict(row).items()}


@router.get("/status")
async def status(user: dict = Depends(_require_user_from_header_or_query)):
    db = await _connect()
    try:
        sc = await db.fetchrow(
            "SELECT sc.active_backend_id, sc.transition_state, sc.last_toggle_at, "
            "  sc.cooldown_until, sb.id AS b_id, sb.kind, sb.name, sb.endpoint, "
            "  sb.is_enabled "
            "FROM system_config sc "
            "JOIN storage_backends sb ON sb.id = sc.active_backend_id "
            "WHERE sc.id=1"
        )
        inflight = await db.fetchval(
            "SELECT count(*)::int FROM jobs "
            "WHERE status IN ('RUNNING','QUEUED','PENDING')"
        )
    finally:
        await db.close()
    if not sc:
        raise HTTPException(500, detail="system_config singleton missing")
    # 2026-08-11 QA-11: serve the display label from the server instead of
    # letting each surface derive its own. The Storage page computed
    # "Wasabi (S3)" from the endpoint in the browser while /health/deep
    # printed the raw slot identifiers ("seaweedfs/seaweedfs-local") off the
    # SAME row — two truthful readings that looked like a disagreement about
    # where 3.5M items/day are being written. shared.storage.labels is now
    # the one place that turns (kind, name, endpoint) into words.
    return {
        "active_backend": {
            "id": str(sc["b_id"]), "kind": sc["kind"], "name": sc["name"],
            "endpoint": sc["endpoint"], "is_enabled": sc["is_enabled"],
            **describe_backend(sc["kind"], sc["name"], sc["endpoint"]),
        },
        "transition_state": sc["transition_state"],
        "last_toggle_at": sc["last_toggle_at"].isoformat() if sc["last_toggle_at"] else None,
        "cooldown_until": sc["cooldown_until"].isoformat() if sc["cooldown_until"] else None,
        "inflight_jobs_count": inflight,
    }


@router.get("/backends")
async def list_backends(user: dict = Depends(_require_user_from_header_or_query)):
    db = await _connect()
    try:
        rows = await db.fetch(
            "SELECT id::text, kind, name, endpoint, is_enabled "
            "FROM storage_backends ORDER BY name"
        )
    finally:
        await db.close()
    # Same server-side label as /status (QA-11) — the toggle button's text is
    # built from this list, so it must not be able to disagree with the
    # active-backend card it sits under.
    return [
        {**dict(r), **describe_backend(r["kind"], r["name"], r["endpoint"])}
        for r in rows
    ]


@router.post("/toggle")
async def submit_toggle(
    req: ToggleRequest, request: Request,
    user: dict = Depends(_require_admin_from_header_or_query),
):
    org_id = user.get("org_id")
    # Org name fetched via a separate query — in production, resolve from
    # users.org_id -> organizations.name. For pilot we accept the raw org_id
    # string or "TMVAULT-ORG" (documented fallback for single-org installs).
    expected = _EXPECTED_ORG_CONFIRM or (org_id and str(org_id)) or "TMVAULT-ORG"
    if req.confirmation_text not in (expected, "TMVAULT-ORG"):
        raise HTTPException(400, detail="confirmation_text does not match org name")

    db = await _connect()
    try:
        sc = await db.fetchrow(
            "SELECT active_backend_id, transition_state, cooldown_until "
            "FROM system_config WHERE id=1"
        )
        if sc["transition_state"] != "stable":
            raise HTTPException(
                409, detail=f"transition already in progress: {sc['transition_state']}",
            )
        if sc["cooldown_until"] and sc["cooldown_until"] > datetime.now(timezone.utc):
            raise HTTPException(
                429, detail=f"cooldown active until {sc['cooldown_until'].isoformat()}",
            )

        from_id = sc["active_backend_id"]
        if str(from_id) == str(req.target_backend_id):
            raise HTTPException(400, detail="target backend is already active")

        # 2026-08-11 QA-10: fail closed on a target that the data plane
        # cannot serve. shared/storage/router.py:132 only registers rows
        # WHERE is_enabled = true, so promoting a disabled backend makes
        # every subsequent write raise BackendNotFoundError. Today's prod is
        # exactly that shape: the only alternate row is `azure-primary`,
        # is_enabled=false, endpoint https://PLACEHOLDER.blob.core.windows.net/
        # — and the UI offers it as the toggle target simply because its id
        # differs from the active one. The toggle worker's preflight does
        # catch it (router.get_store_by_id raises -> target_reachable=False),
        # but only after this endpoint has written a storage_toggle_events
        # row and queued a message: an audit trail of an operation that was
        # never possible. Reject it here, with the reason spelled out.
        target = await db.fetchrow(
            "SELECT kind, name, endpoint, is_enabled "
            "FROM storage_backends WHERE id=$1",
            req.target_backend_id,
        )
        if target is None:
            raise HTTPException(
                404, detail=f"unknown target backend {req.target_backend_id}",
            )
        if not target["is_enabled"]:
            raise HTTPException(
                400,
                detail=(
                    f"target backend {target['name']} is disabled and is not "
                    "registered with the storage router; enable and configure "
                    "it before toggling"
                ),
            )

        event_id = uuid.uuid4()
        actor_id = user.get("id")
        await db.execute(
            "INSERT INTO storage_toggle_events (id, actor_id, actor_ip, "
            "  from_backend_id, to_backend_id, reason, status) "
            "VALUES ($1,$2,$3,$4,$5,$6,'started')",
            event_id,
            uuid.UUID(str(actor_id)) if actor_id else uuid.UUID(int=0),
            request.client.host if request.client else None,
            from_id, req.target_backend_id, req.reason,
        )
    finally:
        await db.close()

    # Enqueue toggle message
    conn = await aio_pika.connect_robust(_rmq_url())
    try:
        ch = await conn.channel()
        await ch.declare_queue("storage.toggle", durable=True)
        await ch.default_exchange.publish(
            aio_pika.Message(
                body=json.dumps({
                    "event_id": str(event_id),
                    "from_id": str(from_id),
                    "to_id": str(req.target_backend_id),
                    "actor_id": str(actor_id),
                    "reason": req.reason,
                }).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key="storage.toggle",
        )
    finally:
        await conn.close()

    return {"event_id": str(event_id), "status": "queued"}


@router.get("/events")
async def list_events(
    limit: int = 20,
    user: dict = Depends(_require_user_from_header_or_query),
):
    db = await _connect()
    try:
        rows = await db.fetch(
            "SELECT * FROM storage_toggle_events ORDER BY started_at DESC LIMIT $1",
            limit,
        )
    finally:
        await db.close()
    return [_row_to_dict(r) for r in rows]


@router.get("/events/{event_id}")
async def get_event(
    event_id: uuid.UUID,
    user: dict = Depends(_require_user_from_header_or_query),
):
    db = await _connect()
    try:
        row = await db.fetchrow(
            "SELECT * FROM storage_toggle_events WHERE id=$1", event_id,
        )
    finally:
        await db.close()
    if not row:
        raise HTTPException(404)
    return _row_to_dict(row)


@router.get("/events/{event_id}/stream")
async def stream_event(
    event_id: uuid.UUID, request: Request,
    user: dict = Depends(_require_user_from_header_or_query),
):
    async def generator():
        conn = await _connect()
        try:
            last_status = None
            while True:
                if await request.is_disconnected():
                    break
                row = await conn.fetchrow(
                    "SELECT status, drained_job_count, retried_job_count, error_message "
                    "FROM storage_toggle_events WHERE id=$1", event_id,
                )
                if row and row["status"] != last_status:
                    last_status = row["status"]
                    yield {"event": "status", "data": json.dumps(dict(row))}
                if row and row["status"] in ("completed", "aborted", "failed"):
                    break
                await asyncio.sleep(1)
        finally:
            await conn.close()

    return EventSourceResponse(generator())



async def _release_transition_if_no_live_toggle(db, event_id) -> None:
    """Free system_config.transition_state when no live toggle owns it.

    While transition_state is not 'stable', router.get_active_store()
    raises TransitionInProgressError for EVERY backup write and
    submit_toggle 409s — so a transition left dirty by a dead worker is a
    fleet-wide outage with no UI escape hatch.

    Liveness is expressed as "NOT terminal" rather than a list of
    in-flight statuses: the in-flight set has seven members (started,
    drain_started, drain_completed, db_promoted, dns_flipped,
    smoke_passed, workers_restarted) and enumerating it positively
    already missed four, which would have let this release steal a
    transition from a toggle that was mid-promote.
    """
    tag = await db.execute(
        "UPDATE system_config SET transition_state='stable', updated_at=NOW() "
        " WHERE id = 1 "
        "   AND transition_state <> 'stable' "
        "   AND NOT EXISTS ("
        "         SELECT 1 FROM storage_toggle_events e "
        "          WHERE e.id <> $1 "
        "            AND e.status NOT IN ('completed','failed','aborted')"
        "       )",
        event_id,
    )
    if tag and tag != "UPDATE 0":
        log.info(
            "abort %s released a stuck transition_state -> stable "
            "(backup writes were blocked)", event_id,
        )


@router.post("/toggle/{event_id}/abort")
async def abort_toggle(
    event_id: uuid.UUID,
    user: dict = Depends(_require_admin_from_header_or_query),
):
    db = await _connect()
    try:
        # Atomic claim: only wins while the toggle worker hasn't committed
        # 'drain_completed' (the worker's own status write is guarded on
        # status <> 'aborted'), so whichever UPDATE commits first sticks —
        # an abort can no longer be clobbered by the worker promoting on.
        claimed = await db.fetchval(
            "UPDATE storage_toggle_events SET status='aborted', completed_at=NOW() "
            "WHERE id=$1 AND status IN ('started','drain_started') "
            "RETURNING status",
            event_id,
        )
        if claimed is None:
            row = await db.fetchrow(
                "SELECT status FROM storage_toggle_events WHERE id=$1", event_id,
            )
            if not row:
                raise HTTPException(404)
            # Already-terminal events: abort is idempotent, and — crucially
            # — must still free a stuck transition. The mid-drain pod-death
            # path flips the event to 'aborted' from the worker's own
            # preflight failure while transition_state stays 'draining', so
            # 409ing here was the last door closing on the operator (every
            # backup write failing, submit_toggle 409ing, recovery only via
            # manual SQL).
            if row["status"] in ("aborted", "failed", "completed"):
                await _release_transition_if_no_live_toggle(db, event_id)
                return {"status": row["status"]}
            raise HTTPException(
                409, detail="cannot abort after drain has completed",
            )
        # Release the transition, but ONLY when no other toggle is live.
        #
        # An unconditional reset here was the original bug: it let
        # submit_toggle's `!= 'stable'` guard admit a second concurrent
        # toggle while the first worker was still promoting. Delegating
        # it entirely to the worker was the opposite failure — if the
        # worker pod died mid-drain (the message is un-acked for the full
        # 900s drain window), the redelivery path stands down without
        # clearing the state, and while transition_state is 'draining'
        # router.get_active_store() raises TransitionInProgressError for
        # EVERY backup write and submit_toggle 409s: the fleet is
        # read-only with no operator escape hatch short of manual SQL.
        #
        # The claim above only wins from 'started'/'drain_started' — i.e.
        # strictly before promotion — and the worker's own post-drain
        # write is guarded on `status <> 'aborted'`, so a successful claim
        # means no promotion can follow. The NOT EXISTS keeps this from
        # stealing a transition owned by a DIFFERENT live toggle. The
        # worker's redelivery path does the same thing idempotently.
        await _release_transition_if_no_live_toggle(db, event_id)
    finally:
        await db.close()
    return {"status": "aborted"}


# Expected org-confirm text — if unset, falls back to the user's org_id or
# the well-known "TMVAULT-ORG" (accepted in both cases for single-org
# internal deploys).
_EXPECTED_ORG_CONFIRM = os.getenv("TOGGLE_CONFIRM_TEXT") or None
