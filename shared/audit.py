"""Shared audit-emission helpers.

All services/workers post audit events to audit-service the same way:
fire-and-forget HTTP POST to ``{AUDIT_SERVICE_URL}/api/v1/audit/log``.
Before this module each caller inlined a ~12-line httpx block plus a
bare ``except Exception: pass``, and several code paths simply forgot
to emit — most visibly the scheduler-triggered + trigger-user backup
paths, which was why Activity stayed empty between "user clicked
backup" and "worker picked up the message".

Usage:

    from shared.audit import emit_backup_triggered
    await emit_backup_triggered(
        job=job, resource=resource, tenant=tenant,
        trigger_label="MANUAL", full_backup=True,
    )

Callers get structured events with zero transport code; the helper
swallows its own transport errors so a slow audit-service never masks
the real workflow. Event shape mirrors what audit-service's
``/api/v1/audit/log`` already normalizes.
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Dict, Optional, Set

import httpx

log = logging.getLogger("tmvault.audit")

# Strong refs so fire-and-forget emits are not garbage-collected mid-flight.
_BG_EMITS: Set["asyncio.Task"] = set()


def emit_audit_event_bg(**fields: Any) -> None:
    """Schedule ``emit_audit_event`` fire-and-forget on the running loop.

    Use on latency-sensitive request paths (login / refresh / logout) so a
    slow or down audit-service adds no latency to the request — the whole
    audit path is already best-effort (``emit_audit_event`` swallows transport
    errors), and here we additionally decline to *wait* for it. No running loop
    (called from sync/non-async context) means the emit is skipped rather than
    crashing the caller."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        log.warning("emit_audit_event_bg with no running loop (action=%s) — skipped",
                    fields.get("action"))
        return
    task = loop.create_task(emit_audit_event(**fields))
    _BG_EMITS.add(task)
    task.add_done_callback(_BG_EMITS.discard)

_AUDIT_POST_TIMEOUT_S = float(os.getenv("AUDIT_POST_TIMEOUT_S", "5.0"))


def _audit_url() -> str:
    """Resolve the audit endpoint at call time, not import time — the
    service URL can change across redeploys and some services import
    shared/audit before their Settings object is fully built."""
    base = os.getenv("AUDIT_SERVICE_URL", "http://audit-service:8080").rstrip("/")
    return f"{base}/api/v1/audit/log"


_MISSING_KEY_CALLS = 0
_MISSING_KEY_LOG_EVERY = 500


def reset_internal_key_warning() -> None:
    """Test hook: forget that the missing-key warning has already fired."""
    global _MISSING_KEY_CALLS
    _MISSING_KEY_CALLS = 0


def internal_audit_headers() -> Dict[str, str]:
    """Headers every ``/api/v1/audit/log`` POST must carry.

    audit-service gates the route on the internal-services shared secret
    (``X-Internal-Api-Key``) — without it the endpoint accepted forged audit
    events from anyone who could reach the ingress. Callers that build their
    own httpx request use this helper so the header can't drift. Read from
    the env for the same reason ``_audit_url`` does.

    A missing key is reported LOUDLY (rate-limited) rather than returned as a
    quiet ``{}``. The gate's two halves live in different pods and are wired
    separately — services enumerate individual ``secretKeyRef`` entries in
    ``deploy/taylor-morrison/k8s/40-services.yaml`` while workers inherit the
    whole Secret via ``envFrom`` — so a half-configured rollout is a real
    state. In it this emitter sends no header, audit-service answers 401, and
    the raw ``client.post`` call sites in job-service / snapshot-service
    neither inspect the status (httpx does not raise on 4xx) nor let the
    exception escape their bare ``except Exception: pass``. Without this
    warning the audit row simply disappears — on a compliance product. The
    caller still gets ``{}`` and still attempts the POST: audit emission is
    fire-and-forget and must never block the workflow it is describing.
    """
    key = os.getenv("INTERNAL_API_KEY", "")
    if key:
        return {"X-Internal-Api-Key": key}

    global _MISSING_KEY_CALLS
    _MISSING_KEY_CALLS += 1
    if (
        _MISSING_KEY_CALLS == 1
        or _MISSING_KEY_CALLS % _MISSING_KEY_LOG_EVERY == 0
    ):
        log.error(
            "INTERNAL_API_KEY is not set in this pod (occurrence #%d): audit "
            "POSTs will carry no X-Internal-Api-Key and audit-service will "
            "reject them with 401 once the gate is configured. Add "
            "INTERNAL_API_KEY to Secret tmvault-secrets AND make sure this "
            "pod's Deployment enumerates it (services do not use "
            "envFrom/secretRef).",
            _MISSING_KEY_CALLS,
        )
    return {}


async def emit_audit_event(**fields: Any) -> None:
    """Low-level: POST a pre-shaped audit event.

    Accepts any keys; audit-service's ``_normalize_event`` tolerates
    unknowns and stamps defaults for missing ones. Fire-and-forget:
    transport errors are logged at WARNING and swallowed so the calling
    workflow is never blocked by an audit outage.
    """
    try:
        async with httpx.AsyncClient(timeout=_AUDIT_POST_TIMEOUT_S) as client:
            resp = await client.post(
                _audit_url(), json=fields, headers=internal_audit_headers()
            )
        # httpx doesn't raise on 4xx/5xx. A rejected POST means the event was
        # dropped — most likely INTERNAL_API_KEY missing/mismatched between
        # this caller and audit-service — so say so instead of losing the
        # audit row silently.
        if resp.status_code >= 400:
            log.error(
                "audit emit rejected (action=%s): HTTP %s %s",
                fields.get("action"), resp.status_code, resp.text[:200],
            )
    except Exception as exc:
        log.warning("audit emit failed (action=%s): %s", fields.get("action"), exc)


def _resource_type(resource: Any) -> Optional[str]:
    if resource is None:
        return None
    t = getattr(resource, "type", None)
    if t is None:
        return None
    return t.value if hasattr(t, "value") else str(t)


def _resource_name(resource: Any) -> Optional[str]:
    if resource is None:
        return None
    return (
        getattr(resource, "display_name", None)
        or getattr(resource, "email", None)
        or (str(resource.id) if getattr(resource, "id", None) else None)
    )


async def emit_backup_triggered(
    *,
    job: Any,
    resource: Any = None,
    tenant: Any = None,
    trigger_label: str = "MANUAL",
    actor_type: str = "USER",
    actor_id: Optional[str] = None,
    actor_email: Optional[str] = None,
    batch_resource_count: Optional[int] = None,
    full_backup: bool = False,
    extra_details: Optional[Dict[str, Any]] = None,
) -> None:
    """Emit BACKUP_TRIGGERED the instant a backup Job row is persisted.

    Fires BEFORE the RMQ message is consumed, so Activity / Audit pages
    reflect "backup queued" immediately instead of waiting the ~seconds
    for the worker to produce BACKUP_STARTED. trigger_label distinguishes
    MANUAL vs SCHEDULED vs PREEMPTIVE, which the UI groups on.

    Outcome is IN_PROGRESS — audit-service _normalize_event allows this
    value (added alongside CANCELLED) so the lifecycle row stays distinct
    from the terminal SUCCESS row the worker emits later.
    """
    details: Dict[str, Any] = {
        "triggered_by": trigger_label,
        "fullBackup": full_backup,
    }
    if batch_resource_count is not None:
        details["batch_resource_count"] = batch_resource_count
    if extra_details:
        details.update(extra_details)

    tenant_id = None
    org_id = None
    if tenant is not None:
        tid = getattr(tenant, "id", None)
        if tid is not None:
            tenant_id = str(tid)
        oid = getattr(tenant, "org_id", None)
        if oid is not None:
            org_id = str(oid)
    elif resource is not None:
        tid = getattr(resource, "tenant_id", None)
        if tid is not None:
            tenant_id = str(tid)
    # Batch jobs don't carry a resource or tenant — fall back to the
    # Job row's own tenant_id so audit-service can still enrich the
    # event with SLA + org context.
    if tenant_id is None and job is not None:
        jtid = getattr(job, "tenant_id", None)
        if jtid is not None:
            tenant_id = str(jtid)

    await emit_audit_event(
        action="BACKUP_TRIGGERED",
        tenant_id=tenant_id,
        org_id=org_id,
        actor_type=actor_type,
        actor_id=actor_id,
        actor_email=actor_email,
        resource_id=(str(resource.id) if resource is not None and getattr(resource, "id", None) else None),
        resource_type=_resource_type(resource) if resource is not None else ("BATCH" if batch_resource_count else None),
        resource_name=_resource_name(resource) if resource is not None else (
            f"Batch backup: {batch_resource_count} resources" if batch_resource_count else None
        ),
        outcome="IN_PROGRESS",
        job_id=str(job.id) if job is not None and getattr(job, "id", None) else None,
        details=details,
    )
