"""Dashboard Service - Aggregated metrics and statistics"""
import asyncio
import logging
import os
import random
from contextlib import asynccontextmanager
from contextlib import suppress
from typing import Optional
from uuid import UUID
from datetime import datetime, timedelta, timezone, date as _date
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import FastAPI, Depends, Query, HTTPException, Request


def _resolve_tz(tz_name: Optional[str]) -> ZoneInfo:
    """Resolve an IANA tz string from the `tz` query param.

    Default to UTC. Invalid names fall back to UTC rather than 4xx
    because dashboards are read-only and a typo'd tz should not break
    the operator's view — they'll just see UTC-bucketed data and
    notice the mismatch.

    Bucketing in client tz means an operator in IST (UTC+5:30) sees
    the day boundary at midnight IST instead of midnight UTC. Without
    this, a backup at 22:31 UTC (04:01 IST next day) lands in the
    previous calendar day, confusing the operator.
    """
    if not tz_name:
        return ZoneInfo("UTC")
    try:
        return ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, ValueError):
        log.warning("dashboard: unknown tz %r — falling back to UTC", tz_name)
        return ZoneInfo("UTC")


def _bucket_date(dt: datetime, tz: ZoneInfo) -> _date:
    """Return the calendar date of `dt` as observed in tz.

    `dt` may be naive (treated as UTC since the DB stores TIMESTAMP
    WITHOUT TIME ZONE in UTC) or aware. Aware values are converted;
    naive values are localized to UTC first.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(tz).date()
from sqlalchemy import select, func, text, and_, or_
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from shared.config import settings
from shared.database import get_db, close_db, AsyncSession, engine, async_session_factory
from shared.models import (
    Resource, Job, JobType, JobStatus, Snapshot, SnapshotItem, SnapshotStatus,
    ResourceType, ResourceStatus, Tenant, TenantType, UI_HIDDEN_TYPES,
)
from shared.storage_rollup import exclude_tier2_storage_dupes_clause

log = logging.getLogger("dashboard-service")


async def _wait_for_db(timeout_total_s: int = 120) -> None:
    """Ping the DB with exponential backoff so the lifespan startup can
    survive Railway's internal-DNS / Postgres cold-start race.

    Observed Railway 2026-05-13: dashboard-service crashed in a restart
    loop because asyncpg's default connect timeout (10s) raced PG coming
    online, the lifespan ``SELECT 1`` ping raised TimeoutError, and
    Uvicorn exited. Without a retry the only recovery was a manual
    redeploy. Other services in this repo (tenant_service, job_service,
    audit_service, etc.) all retry their startup checks for the same
    reason; dashboard was the outlier.

    Retries 1s → 2s → 4s → 8s → 8s … up to ``timeout_total_s`` wall.
    Each individual attempt is capped at 10s by asyncpg's default, so
    a "stuck DB" scenario can't hold a single attempt forever. Logs
    every failed attempt for visibility into how flaky the dep is.
    """
    deadline = asyncio.get_event_loop().time() + timeout_total_s
    delay = 1.0
    attempt = 0
    while True:
        attempt += 1
        try:
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            if attempt > 1:
                log.warning(
                    "[startup] DB reachable after %d attempt(s)", attempt,
                )
            return
        except Exception as exc:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                log.error(
                    "[startup] DB unreachable after %ds (%d attempts): %s — "
                    "exiting so Railway can restart the container",
                    timeout_total_s, attempt, exc,
                )
                raise
            log.warning(
                "[startup] DB ping attempt %d failed (%s: %s); "
                "retrying in %.1fs (deadline in %.0fs)",
                attempt, type(exc).__name__, exc, delay, remaining,
            )
            await asyncio.sleep(min(delay, remaining))
            delay = min(delay * 2, 8.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Dashboard is read-only and should not run heavyweight schema migration logic
    # during startup. That path can block on application traffic from other services
    # and leave the container stuck in "Waiting for application startup".
    from shared import core_metrics
    core_metrics.init()
    await _wait_for_db()
    prewarm_task = _start_protected_prewarm()
    yield
    if prewarm_task is not None:
        prewarm_task.cancel()
        with suppress(asyncio.CancelledError):
            await prewarm_task
    await close_db()
    if _ro_engine is not None:
        await _ro_engine.dispose()


from shared.audit_middleware import AuditMiddleware  # Ops/Data Check 3: mutation-audit backstop
app = FastAPI(title="Dashboard Service", version="1.0.0", lifespan=lifespan)
app.add_middleware(AuditMiddleware, service_name="dashboard-service")
from shared.auth_enforcement import AuthMiddleware  # Ops/Data Check 17: verified principal required
app.add_middleware(AuthMiddleware, service_name="dashboard-service", enforce_tenant_scope=True)


# Per-user content types — kept for M365_RESOURCE_TYPES below (service-level
# rollups need to know which row types carry per-user backup bytes). The
# Protection Status "Users" card no longer counts these; it counts ENTRA_USER
# rows directly so the card's total equals the Users tab list exactly.
USER_CONTENT_TYPES = {
    ResourceType.MAILBOX,
    ResourceType.ONEDRIVE,
    ResourceType.USER_MAIL,
    ResourceType.USER_ONEDRIVE,
    ResourceType.USER_CONTACTS,
    ResourceType.USER_CALENDAR,
    ResourceType.USER_CHATS,
}

# Each bucket's type set MUST exactly mirror the corresponding tab in
# tm_vault/src/services/resource.ts:M365_TAB_TYPE_MAP. The denominator of
# every Protection Status card has to equal the count shown when the operator
# clicks into that tab — anything else is a lie. No bucket gets a bespoke
# query: every one of them is summed from the single GROUP BY below, over the
# PROTECTION_SCOPE_* universe /by-type uses. SharePoint used to be re-counted
# with a group-name-collision exclusion; /by-type deleted that rule on
# 2026-07-15 and the leftover copy here reported 752 sites against a tab
# showing 4,239 (QA-05, 2026-08-11).
PROTECTION_BUCKETS = {
    "users": {ResourceType.ENTRA_USER},
    "sharedMailboxes": {ResourceType.SHARED_MAILBOX},
    "rooms": {ResourceType.ROOM_MAILBOX},
    "sharepointSites": {ResourceType.SHAREPOINT_SITE},
    "groupsAndTeams": {ResourceType.ENTRA_GROUP, ResourceType.M365_GROUP, ResourceType.TEAMS_CHANNEL},
    "entraId": {ResourceType.ENTRA_DIRECTORY},
    "powerPlatform": {ResourceType.POWER_BI, ResourceType.POWER_APPS, ResourceType.POWER_AUTOMATE},
}

# ── Canonical Protection-universe scope ───────────────────────────────────
#
# VERBATIM MIRROR of services/resource-service/main.py's constants of the
# same names — read the long comment there for the 2026-08-11 measurements
# and the reasoning. Duplicated (not imported) because the two services ship
# as separate images with no shared service-level module; the right home is
# shared/models.py next to UI_HIDDEN_TYPES.
# tests/services/test_protection_scope_single_definition.py loads both
# modules and fails if these ever diverge again.
PROTECTION_SCOPE_EXCLUDED_STATUSES: frozenset = frozenset()
PROTECTION_SCOPE_EXCLUDED_TYPES: frozenset = frozenset(UI_HIDDEN_TYPES)

AZURE_PROTECTION_BUCKETS = {
    "virtualMachines": {ResourceType.AZURE_VM},
    "sqlDatabases": {ResourceType.AZURE_SQL_DB},
    "postgresqlDatabases": {ResourceType.AZURE_POSTGRESQL, ResourceType.AZURE_POSTGRESQL_SINGLE},
}

M365_RESOURCE_TYPES = {
    ResourceType.MAILBOX,
    ResourceType.SHARED_MAILBOX,
    ResourceType.ROOM_MAILBOX,
    ResourceType.ONEDRIVE,
    ResourceType.SHAREPOINT_SITE,
    ResourceType.TEAMS_CHANNEL,
    ResourceType.TEAMS_CHAT,
    ResourceType.ENTRA_USER,
    ResourceType.ENTRA_GROUP,
    # Unified (modern) M365 group — distinct row type from ENTRA_GROUP and
    # required here so the Protection Status m365 filter doesn't silently
    # drop the 7 M365_GROUP rows. Without this entry, Groups & Teams card
    # reads 4 (ENTRA_GROUP only) when the Tab shows 11 (both types).
    ResourceType.M365_GROUP,
    # Per-tenant "Azure Active Directory" singleton — the Entra ID card on
    # Overview needs to count this one row. Missing here previously caused
    # the card to read 0/0 even though the Tab list shows 1.
    ResourceType.ENTRA_DIRECTORY,
    ResourceType.ENTRA_APP,
    ResourceType.ENTRA_DEVICE,
    ResourceType.ENTRA_SERVICE_PRINCIPAL,
    ResourceType.POWER_BI,
    ResourceType.POWER_APPS,
    ResourceType.POWER_AUTOMATE,
    ResourceType.POWER_DLP,
    ResourceType.COPILOT,
    ResourceType.PLANNER,
    ResourceType.TODO,
    ResourceType.ONENOTE,
    ResourceType.DYNAMIC_GROUP,
    # Tier 2 per-user content types — these hold the actual backup bytes
    # post-refactor, so they MUST be in this set. Leaving them out
    # silently zeroed out the M365 filter on the dashboard (backup-size
    # dropped from 2.5 GB → 18 KB, 24h / 7d job counts dropped to 0).
    ResourceType.USER_MAIL,
    ResourceType.USER_ONEDRIVE,
    ResourceType.USER_CONTACTS,
    ResourceType.USER_CALENDAR,
    ResourceType.USER_CHATS,
    ResourceType.TEAMS_CHAT_EXPORT,
}

AZURE_RESOURCE_TYPES = {
    ResourceType.AZURE_VM,
    ResourceType.AZURE_SQL_DB,
    ResourceType.AZURE_POSTGRESQL,
    ResourceType.AZURE_POSTGRESQL_SINGLE,
    ResourceType.RESOURCE_GROUP,
}


def parse_service_type(service_type: Optional[str]) -> Optional[str]:
    if not service_type:
        return None
    normalized = service_type.lower()
    if normalized not in ("m365", "azure"):
        raise HTTPException(status_code=400, detail="Unsupported serviceType. Expected 'm365' or 'azure'.")
    return normalized


def resource_types_for_service(service_type: Optional[str]):
    if service_type == "m365":
        return M365_RESOURCE_TYPES
    if service_type == "azure":
        return AZURE_RESOURCE_TYPES
    return None


def datasource_batch_trigger_label(service_type: str) -> str:
    return f"MANUAL_DATASOURCE_{service_type.upper()}"


def format_bytes(bytes_val: int) -> str:
    if bytes_val < 1024**3:
        return f"{bytes_val / 1024**2:.1f} MB"
    return f"{bytes_val / 1024**3:.1f} GB"


@app.get("/health")
async def health():
    return {"status": "ok", "service": "dashboard"}


@app.get("/health/deep")
async def health_deep(detailed: bool = Query(True)):
    """Aggregated subsystem health (DB, RabbitMQ, queues, workers, storage,
    leases). Consumed by the gateway's GET /api/v1/health. Never raises —
    a failing subsystem degrades its component, not the whole response."""
    from shared.health import build_health
    report = await build_health(detailed=detailed)
    report["service"] = "dashboard"
    return report


@app.get("/api/v1/dashboard/overview")
async def get_overview(
    tenantId: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    filters = []
    if tenantId:
        filters.append(Resource.tenant_id == UUID(tenantId))
    
    total = (await db.execute(select(func.count(Resource.id)).where(*filters))).scalar() or 0
    protected = (await db.execute(select(func.count(Resource.id)).where(Resource.sla_policy_id.isnot(None), *filters))).scalar() or 0
    
    # Use naive datetime to match Job.created_at declared as
    # TIMESTAMP WITHOUT TIME ZONE. asyncpg raises DataError on a
    # tz-aware >= naive-column compare. Sibling queries in this file
    # (status/24hour, status/7day, …) already follow this pattern.
    yesterday = datetime.utcnow() - timedelta(hours=24)
    failed = (await db.execute(select(func.count(Job.id)).where(Job.status == JobStatus.FAILED, Job.type == JobType.BACKUP, Job.created_at >= yesterday, *filters))).scalar() or 0
    pending = (await db.execute(select(func.count(Job.id)).where(Job.status.in_([JobStatus.QUEUED, JobStatus.RUNNING]), Job.type == JobType.BACKUP, *filters))).scalar() or 0
    
    storage = (await db.execute(
        select(func.sum(Resource.storage_bytes))
        .where(exclude_tier2_storage_dupes_clause(), *filters)
    )).scalar() or 0
    last_backup = (await db.execute(select(func.max(Resource.last_backup_at)).where(*filters))).scalar()
    
    return {
        "totalResources": total,
        "protectedResources": protected,
        "failedBackups": failed,
        "pendingBackups": pending,
        "storageUsed": format_bytes(storage),
        "lastBackupTime": last_backup.isoformat() if last_backup else None,
    }


def _build_service_clause(service_key, service_resource_types):
    """Return the service-bucket filter shared by 24h + 7d endpoints.

    Two shapes of backup job exist:
      1. Per-resource: Job.resource_id points at a single Resource.
         Match by that resource's type.
      2. Batch (MANUAL_BATCH / USER_ORCHESTRATION): Job.resource_id is
         NULL and batch_resource_ids holds the fan-out. We can't join
         array → resources cheaply, so we match by Tenant.type — a
         tenant is either M365 or AZURE, not both.
    """
    if not (service_key and service_resource_types):
        return None
    service_tenant_type = TenantType.M365 if service_key == "m365" else TenantType.AZURE
    return or_(
        and_(Job.resource_id.is_not(None), Resource.type.in_(service_resource_types)),
        and_(Job.resource_id.is_(None), Tenant.type == service_tenant_type),
    )


def _batch_group_key(job_id, status, spec, result, created_at, tenant_id):
    """Compute the Activity-style batch key for a Job row.

    A single operator click ("Backup all 9 users") fans out into N Job
    rows in the DB: one per (tenant, routing_key) partition for the
    parent ENTRA_USER bulk, plus more rows for the Tier-2 child fan-out
    that discovery-worker enqueues. All these rows share a `batch_id`
    in `spec` (job-service propagates it through every stage), so
    grouping by that key collapses them back to ONE logical task.

    For pre-batch_id legacy rows (and SLA-scheduled jobs that don't
    share a batch), the fallback (tenant, triggered_by, second-precision
    created_at) is the same grouping the Audit / Activity tab uses, so
    every dashboard surface counts the same way.
    """
    spec = spec or {}
    batch_id = spec.get("batch_id")
    if batch_id:
        return (tenant_id, "BATCH", str(batch_id))
    trigger = str(spec.get("triggered_by") or "")
    created_key = (
        created_at.replace(microsecond=0).isoformat()
        if created_at else ""
    )
    return (tenant_id, trigger, created_key)


def _roll_up_group_outcome(children):
    """Reduce a batch group's child Jobs to a single outcome bucket.

    Buckets mirror the per-Job semantics of the old query but applied
    to the whole batch:
      * success   — every child COMPLETED with no failed/skipped items
      * warnings  — at least one child COMPLETED with failed/skipped
                    items, OR at least one child RETRYING (mid-retry)
      * failures  — at least one child FAILED
      * inflight  — at least one child still RUNNING/QUEUED and no
                    child has failed yet. Counted nowhere (the card
                    only shows finished outcomes).
      * cancelled — every child CANCELLED. Counted nowhere.
    Returns "success" | "warnings" | "failures" | None.
    """
    statuses = [s for s, _ in children]
    results = [r or {} for _, r in children]
    any_failed = any(s == JobStatus.FAILED for s in statuses)
    any_retrying = any(s == JobStatus.RETRYING for s in statuses)
    any_running = any(s in (JobStatus.RUNNING, JobStatus.QUEUED) for s in statuses)
    any_completed = any(s == JobStatus.COMPLETED for s in statuses)
    any_partial = any(
        int(r.get("failed_count") or 0) > 0
        or int(r.get("skipped_count") or 0) > 0
        for r in results
    )

    if any_failed:
        return "failures"
    if any_running:
        return None  # in-flight — not an outcome yet
    if any_retrying:
        return "warnings"
    if any_completed and any_partial:
        return "warnings"
    if any_completed:
        return "success"
    return None  # all cancelled / pending — not counted


# How long an in-flight batch may run before the 24-hour card calls it
# stalled. TMV-022 (QA sweep 2026-08-11): two BACKUP Jobs of batch
# 2123e84f had been RUNNING since 18:07 UTC while the card read "Success,
# 2 success / 0 failures" — `_roll_up_group_outcome` returns None for any
# group holding a RUNNING/QUEUED child and the endpoint tallied only the
# three terminal buckets, so an in-flight batch was counted NOWHERE.
#
# Why age and not a heartbeat: `jobs.updated_at` is not one. Job
# ee9bac4f ran 13.6 h to a clean COMPLETED with updated_at last touched
# 4 minutes in — an idle-time rule would flag healthy long runs.
#
# Why 24 h: measured over 30 days of terminal BACKUP jobs on prod (n=832)
# p50 = 327 s, p90 = 4,846 s, p99 = 60,651 s (16.8 h); the longest clean
# COMPLETED run was 54.6 h. So 24 h sits above the 99th percentile and
# below nothing legitimate except the rare whale — a coarse net, tunable
# without a rebuild. `inProgress` is the real fix; `stalled` is the
# at-risk flag on top of it.
#
# Deliberately independent of the known phantom "In Progress" batch
# (overlap guard counting held resources as missing Tier-2 children,
# still unfixed): this reads Job.status only and needs no batch-rollup
# semantics, so a phantom shows up as in-flight — which is what an
# operator should see — rather than being swallowed as it is today.
_STALL_AFTER_SECONDS = int(os.getenv("DASHBOARD_STALL_AFTER_SECONDS", "86400"))


def _group_inflight_state(children, oldest_created_at, now):
    """None | "inflight" | "stalled" for a batch group.

    Only groups `_roll_up_group_outcome` declines to bucket are relevant:
    a group with a FAILED child is already reported as a failure, and a
    group where everything terminated is already success/warnings.
    """
    statuses = [s for s, _ in children]
    if any(s == JobStatus.FAILED for s in statuses):
        return None
    if not any(s in (JobStatus.RUNNING, JobStatus.QUEUED) for s in statuses):
        return None
    if oldest_created_at is None:
        return "inflight"
    age = (now - oldest_created_at).total_seconds()
    return "stalled" if age >= _STALL_AFTER_SECONDS else "inflight"


@app.get("/api/v1/dashboard/status/24hour")
async def get_24hour_status(
    tenantId: Optional[str] = Query(None),
    serviceType: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    service_key = parse_service_type(serviceType)
    service_resource_types = resource_types_for_service(service_key)

    # Use naive datetime to match TIMESTAMP WITHOUT TIME ZONE
    yesterday = datetime.utcnow() - timedelta(hours=24)
    filters = [Job.type == JobType.BACKUP, Job.created_at >= yesterday]
    if tenantId:
        filters.append(Job.tenant_id == UUID(tenantId))

    service_clause = _build_service_clause(service_key, service_resource_types)

    # One pass: fetch every backup Job in the window, group by batch
    # key, and tally outcomes per group. This is the same grouping the
    # Activity tab uses (audit-service _group_batch_jobs), so a 9-user
    # bulk click that the audit tab shows as one row also counts as one
    # "task" here — matching the operator's mental model of "one click
    # = one task." Counting raw Job rows (the old behavior) leaked the
    # backend's queue partitioning + Tier-2 fan-out into the dashboard.
    jobs_stmt = (
        select(
            Job.id, Job.status, Job.spec, Job.result,
            Job.created_at, Job.tenant_id,
        )
        .select_from(Job)
        .outerjoin(Resource, Job.resource_id == Resource.id)
        .outerjoin(Tenant, Job.tenant_id == Tenant.id)
        .where(*filters)
    )
    if service_clause is not None:
        jobs_stmt = jobs_stmt.where(service_clause)
    rows = (await db.execute(jobs_stmt)).all()

    from collections import defaultdict
    groups: dict = defaultdict(list)
    group_started: dict = {}
    for r in rows:
        key = _batch_group_key(*r)
        groups[key].append((r.status, r.result))
        if r.created_at is not None:
            prev = group_started.get(key)
            if prev is None or r.created_at < prev:
                group_started[key] = r.created_at

    now = datetime.utcnow()
    success = warnings = failures = in_progress = stalled = 0
    oldest_in_progress = None
    for key, children in groups.items():
        outcome = _roll_up_group_outcome(children)
        if outcome == "success":
            success += 1
            continue
        if outcome == "warnings":
            warnings += 1
            continue
        if outcome == "failures":
            failures += 1
            continue
        # outcome is None: cancelled/pending (invisible by design) OR
        # in-flight, which used to be invisible by accident. See
        # _STALL_AFTER_SECONDS.
        started = group_started.get(key)
        state = _group_inflight_state(children, started, now)
        if state is None:
            continue
        in_progress += 1
        if state == "stalled":
            stalled += 1
        if started is not None and (
            oldest_in_progress is None or started < oldest_in_progress
        ):
            oldest_in_progress = started

    return {
        "success": success,
        "warnings": warnings,
        "failures": failures,
        # New (TMV-022). `inProgress` counts every in-flight batch —
        # `stalled` is the subset past _STALL_AFTER_SECONDS. The headline
        # must not read "Success" while inProgress > 0.
        "inProgress": in_progress,
        "stalled": stalled,
        "oldestInProgressStartedAt": (
            oldest_in_progress.isoformat() if oldest_in_progress else None
        ),
        "stallThresholdSeconds": _STALL_AFTER_SECONDS,
    }


@app.get("/api/v1/dashboard/status/7day")
async def get_7day_status(
    tenantId: Optional[str] = Query(None),
    serviceType: Optional[str] = Query(None),
    tz: Optional[str] = Query(
        None,
        description=(
            "IANA timezone (e.g. Asia/Kolkata) to bucket daily counts. "
            "Frontend should pass "
            "Intl.DateTimeFormat().resolvedOptions().timeZone. "
            "Defaults to UTC."
        ),
    ),
    db: AsyncSession = Depends(get_db),
):
    service_key = parse_service_type(serviceType)
    service_resource_types = resource_types_for_service(service_key)
    client_tz = _resolve_tz(tz)

    # Use naive datetime to match TIMESTAMP WITHOUT TIME ZONE
    seven_days_ago = datetime.utcnow() - timedelta(days=7)
    filters = [Job.type == JobType.BACKUP, Job.created_at >= seven_days_ago]
    if tenantId:
        filters.append(Job.tenant_id == UUID(tenantId))

    service_clause = _build_service_clause(service_key, service_resource_types)

    # Group by batch key per Activity semantics so a single bulk click
    # (which fans out into multiple Job rows internally) registers as
    # ONE task on the dashboard, on its earliest-child date. Counting
    # raw Job rows (the old behavior) double/triple-counted the same
    # operator action.
    jobs_stmt = (
        select(
            Job.id, Job.status, Job.spec, Job.result,
            Job.created_at, Job.tenant_id,
        )
        .select_from(Job)
        .outerjoin(Resource, Job.resource_id == Resource.id)
        .outerjoin(Tenant, Job.tenant_id == Tenant.id)
        .where(*filters)
    )
    if service_clause is not None:
        jobs_stmt = jobs_stmt.where(service_clause)
    rows = (await db.execute(jobs_stmt)).all()

    # Group → per-group earliest date + outcome → daily tally.
    from collections import defaultdict
    groups: dict = defaultdict(list)
    earliest_at: dict = {}
    for r in rows:
        key = _batch_group_key(*r)
        groups[key].append((r.status, r.result))
        if r.created_at is not None:
            existing = earliest_at.get(key)
            if existing is None or r.created_at < existing:
                earliest_at[key] = r.created_at

    daily_tally: dict = defaultdict(lambda: {"success": 0, "warnings": 0, "failures": 0})
    for key, children in groups.items():
        outcome = _roll_up_group_outcome(children)
        if outcome is None:
            continue
        anchor = earliest_at.get(key)
        if anchor is None:
            continue
        # Bucket in client's tz so a 22:31 UTC backup lands in the
        # operator's "today" (04:01 IST = 2026-05-15), not "yesterday"
        # (2026-05-14). Frontend passes its browser tz; default UTC.
        date_str = _bucket_date(anchor, client_tz).isoformat()
        daily_tally[date_str][outcome] += 1

    # Fill all 7 days, padding missing days with zeros. The "today"
    # anchor must also be in client tz — otherwise we'd pad based on
    # UTC's today and the bucketed rows wouldn't line up.
    today_in_tz = _bucket_date(datetime.utcnow(), client_tz)
    daily_status = []
    for i in range(7):
        date = today_in_tz - timedelta(days=6-i)
        date_str = date.isoformat()
        bucket = daily_tally.get(date_str, {"success": 0, "warnings": 0, "failures": 0})
        daily_status.append({
            "date": date_str,
            "success": bucket["success"],
            "warnings": bucket["warnings"],
            "failures": bucket["failures"],
        })

    total_backups = sum(d["success"] + d["warnings"] + d["failures"] for d in daily_status)
    total_success = sum(d["success"] for d in daily_status)

    return {
        "dailyStatus": daily_status,
        "summary": {
            "totalBackups": total_backups,
            "successRate": round(total_success / total_backups * 100, 2) if total_backups > 0 else 0,
            "avgDuration": "N/A",
        },
    }


@app.get("/api/v1/dashboard/protection/status")
async def get_protection_status(
    tenantId: Optional[str] = Query(None),
    serviceType: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    service_key = parse_service_type(serviceType)
    service_resource_types = resource_types_for_service(service_key)

    # Canonical Protection scope — the ONE definition these cards share with
    # the /api/v1/resources/by-type tab lists (PROTECTION_SCOPE_* above).
    #
    # 2026-08-11 (QA-05): this used to add
    # `Resource.status.in_([DISCOVERED, ACTIVE])`, which /by-type does not
    # apply. On prod tenant 163c487b… that hid 8,373 INACCESSIBLE rows from
    # the cards while the tabs listed them — Users 6,680 vs 13,812, Shared
    # 120 vs 1,293, Rooms 132 vs 154, Groups & Teams 10,109 vs 10,142 — and
    # it also dropped 2 of the tenant's 13 SLA-assigned resources from the
    # NUMERATOR (1 ENTRA_USER + 1 SHARED_MAILBOX are INACCESSIBLE but still
    # carry a policy). The tab is the correct universe: a terminated user
    # whose mailbox we still hold has to stay listed and countable, or there
    # is no UI path to recover it (see shared/tier2_discovery.py:168,
    # "readers (UI, audit) want the full list").
    filters = []
    if PROTECTION_SCOPE_EXCLUDED_STATUSES:
        filters.append(
            Resource.status.notin_(sorted(PROTECTION_SCOPE_EXCLUDED_STATUSES))
        )
    if tenantId:
        filters.append(Resource.tenant_id == UUID(tenantId))
    if service_resource_types:
        filters.append(Resource.type.in_(service_resource_types))
    # Hidden types: /by-type returns an empty page for these (TEAMS_CHANNEL is
    # redundant with its M365_GROUP twin), so the cards must not count them
    # either. The Groups & Teams tab shows 11; this query summed 15 before
    # the exclusion. See shared.models.UI_HIDDEN_TYPES for per-type rationale.
    filters.append(Resource.type.notin_(PROTECTION_SCOPE_EXCLUDED_TYPES))

    stmt = (
        select(
            Resource.type,
            func.count(Resource.id).label("total"),
            func.count(Resource.id).filter(Resource.sla_policy_id.isnot(None)).label("protected"),
        )
        .where(*filters)
        .group_by(Resource.type)
    )
    rows = (await db.execute(stmt)).all()

    totals_by_type = {
        row.type: {"total": row.total or 0, "protected": row.protected or 0}
        for row in rows
    }

    def bucket_item(bucket_name: str, buckets: dict):
        total = 0
        protected = 0
        for resource_type in buckets[bucket_name]:
            values = totals_by_type.get(resource_type, {"total": 0, "protected": 0})
            total += values["total"]
            protected += values["protected"]
        return {"protectedCount": protected, "total": total}
    if service_key == "azure":
        bucket_values = {name: bucket_item(name, AZURE_PROTECTION_BUCKETS) for name in AZURE_PROTECTION_BUCKETS}
        total = sum(item["total"] for item in bucket_values.values())
        protected = sum(item["protectedCount"] for item in bucket_values.values())
        percentage = round(protected / total * 100, 2) if total > 0 else 0
        return {
            "virtualMachines": bucket_values["virtualMachines"],
            "sqlDatabases": bucket_values["sqlDatabases"],
            "postgresqlDatabases": bucket_values["postgresqlDatabases"],
            "percentage": percentage,
        }

    bucket_values = {name: bucket_item(name, PROTECTION_BUCKETS) for name in PROTECTION_BUCKETS}

    # SharePoint deliberately gets NO special case any more.
    #
    # 2026-08-11 (QA-05): this used to re-count SHAREPOINT_SITE with an
    # extra "NOT EXISTS a group with the same display_name" exclusion,
    # justified as staying in sync with a `sp_exclude_clause` in
    # resource-service /by-type. That clause no longer exists: on 2026-07-15
    # /by-type stopped hiding group-connected sites (group backups capture
    # profile + members only, NOT the group's site content, so hiding the
    # site left no UI path to protect 3,465 of 4,204 pilot sites) and now
    # merely annotates them with `group_connected` for a badge. The card kept
    # the dead rule and reported 752 sites against a tab showing 4,239 —
    # 3,487 protectable sites missing from the denominator. The generic
    # GROUP BY above already counts SHAREPOINT_SITE exactly the way the tab
    # does; anything re-derived here is a second source of truth by
    # construction.

    total = sum(item["total"] for item in bucket_values.values())
    protected = sum(item["protectedCount"] for item in bucket_values.values())
    percentage = round(protected / total * 100, 2) if total > 0 else 0

    return {
        "users": bucket_values["users"],
        "sharedMailboxes": bucket_values["sharedMailboxes"],
        "rooms": bucket_values["rooms"],
        "sharepointSites": bucket_values["sharepointSites"],
        "groupsAndTeams": bucket_values["groupsAndTeams"],
        "entraId": bucket_values["entraId"],
        "powerPlatform": bucket_values["powerPlatform"],
        "percentage": percentage,
    }


# Newest-version content-size rollup for one resource, fully SQL-side.
# Same statement family as snapshot-service's _rollup_latest_items (the
# 2026-07-14 content-snapshots OOM fix): GROUP BY hash-aggregates the
# per-item newest version inside Postgres and returns one scalar —
# never materialize snapshot_items rows in Python (one user's chats
# child holds 11M pointer rows).
_PROTECTED_ROLLUP_SQL = """
    SELECT COALESCE(SUM(size), 0) AS bytes
    FROM (
      SELECT (array_agg(si.content_size ORDER BY si.created_at DESC))[1] AS size
      FROM snapshot_items si
      WHERE si.snapshot_id = ANY(:snaps)
        AND COALESCE(si.is_deleted, false) = false
      GROUP BY COALESCE(si.external_id, si.id::text)
    ) latest
"""

# (resource_id, latest snapshot id) -> protected bytes. L1 in front of
# the durable resource_protected_bytes table (below); per-process,
# rebuilt on restart. Entries self-invalidate when a newer snapshot
# completes. Wholesale clear is a runaway guard only.
_protected_bytes_cache: dict = {}
_PROTECTED_CACHE_MAX = 10000

_PROTECTED_UPSERT_SQL = """
    INSERT INTO resource_protected_bytes
        (resource_id, latest_snapshot_id, bytes, computed_at)
    VALUES (:rid, :snap, :bytes, NOW())
    ON CONFLICT (resource_id) DO UPDATE
        SET latest_snapshot_id = EXCLUDED.latest_snapshot_id,
            bytes = EXCLUDED.bytes,
            computed_at = EXCLUDED.computed_at
"""


# Request-path guards (2026-10-14). Over 4 h on prod 15 of 30
# /backup/size calls came back 504: the gateway's 60 s read timeout
# (api-gateway GATEWAY_TIMEOUT) fired while this endpoint was still
# walking resources, the gateway retried the GET once, and every
# browser call left TWO abandoned 60-120 s rollup chains running on the
# primary (the planner picks a Parallel Seq Scan over snapshot_items
# for mid-size resources). Three bounds now apply to a request:
#   * a wall-clock budget under the gateway timeout — past it the
#     endpoint answers with what it has and `partial: true`;
#   * at most _PROTECTED_ROLLUP_CONCURRENCY heavy rollups in flight per
#     process, request path and prewarm loop together;
#   * each heavy statement runs under SET LOCAL statement_timeout for
#     the budget that is left, so the primary stops it the moment the
#     budget is spent instead of finishing a query nobody waits for.
_PROTECTED_REQUEST_BUDGET_S = float(os.getenv("PROTECTED_REQUEST_BUDGET_S", "50"))
_PROTECTED_ROLLUP_CONCURRENCY = 2
_heavy_rollup_sem = asyncio.Semaphore(_PROTECTED_ROLLUP_CONCURRENCY)
_STATEMENT_CANCELLED_SQLSTATE = "57014"  # query_canceled: statement_timeout hit

# ---------------------------------------------------------------------------
# Read replica for the heavy rollups (2026-10-15 RCA). The newest-version
# hash aggregate over a resource's snapshot_items is a Parallel Seq Scan for
# every mid/large resource; recomputed three times a day per resource it was
# the single largest sequential reader of the primary (37.8 TB through
# parallel workers in 32 h) and the read IO behind every multi-minute
# carry-forward slot hold in the workers. DB_RO_HOST (CNPG's replica
# service, e.g. postgres-ha-ro.databases.svc.cluster.local) moves that
# SELECT off the primary; the resource_protected_bytes upsert still goes to
# the primary. Opt-in: unset => everything stays on the primary as before.
_RO_HOST = os.getenv("DB_RO_HOST", "").strip()
_RO_PORT = int(os.getenv("DB_RO_PORT", "5432"))
_ro_engine = None
_ro_session_factory = None
if _RO_HOST:
    _ro_engine = create_async_engine(
        make_url(settings.DATABASE_URL)
        .set(host=_RO_HOST, port=_RO_PORT)
        .render_as_string(hide_password=False),
        pool_size=int(os.getenv("DB_RO_POOL_SIZE", "2")),
        max_overflow=2,
        pool_pre_ping=True,
        pool_recycle=1800,
    )
    _ro_session_factory = async_sessionmaker(
        _ro_engine, class_=AsyncSession, expire_on_commit=False,
    )

# The replica may lag. Only trust it for a rollup when it already shows the
# snapshot that keys the rollup row as eligible: WAL is replayed in order,
# so the snapshot's items are visible whenever its terminal status is.
_REPLICA_SEES_SNAPSHOT_SQL = (
    "SELECT 1 FROM snapshots WHERE id = :sid "
    "AND status IN ('COMPLETED', 'PARTIAL', 'PENDING_DELETION')"
)


async def _run_rollup(session: AsyncSession, sib_ids, remaining: Optional[float]):
    """The heavy statement on one session, bounded by the request budget
    when there is one."""
    if remaining is None:
        return (await session.execute(
            text(_PROTECTED_ROLLUP_SQL), {"snaps": sib_ids},
        )).first()
    # SAVEPOINT: a statement_timeout abort poisons the whole transaction
    # ("current transaction is aborted"); rolling back to the savepoint
    # keeps the session usable for the next resource. Same pattern as
    # shared/backup_cleanup.py::_probe_unkeyed_paths_bounded. SET cannot
    # take bind parameters; the value is an int.
    async with session.begin_nested():
        await session.execute(text(
            f"SET LOCAL statement_timeout = {max(1000, int(remaining * 1000))}"
        ))
        rollup = (await session.execute(
            text(_PROTECTED_ROLLUP_SQL), {"snaps": sib_ids},
        )).first()
    # GUC changes survive RELEASE SAVEPOINT: do not leave the upsert +
    # commit running under the rollup's timeout.
    await session.execute(text("SET LOCAL statement_timeout = DEFAULT"))
    return rollup


async def _rollup_on_best_session(db: AsyncSession, sib_ids, latest_id, remaining):
    """Run the rollup on the replica when one is configured and it has
    replayed ``latest_id``; otherwise on the primary session ``db``."""
    factory = _ro_session_factory
    if factory is not None:
        async with factory() as ro:
            seen = (await ro.execute(
                text(_REPLICA_SEES_SNAPSHOT_SQL), {"sid": str(latest_id)},
            )).first()
            if seen is not None:
                return await _run_rollup(ro, sib_ids, remaining)
        log.info("[backup-size] replica has not replayed snapshot %s yet — "
                 "rollup on primary", latest_id)
    return await _run_rollup(db, sib_ids, remaining)


async def _protected_bytes_for_resource(
    db: AsyncSession,
    resource_id,
    *,
    deadline: Optional[float] = None,
    recompute: bool = True,
) -> Optional[int]:
    """Protected bytes for one resource, or None when the answer needs
    a heavy rollup this call may not run (``recompute=False``) or that
    did not fit before ``deadline`` (event-loop clock, seconds).

    The cache decision is the resource's newest eligible snapshot id:
    L1 dict, then the durable resource_protected_bytes row, then the
    newest-version rollup over every eligible snapshot plus an upsert.
    A resource is recomputed only when that latest id changed.
    """
    latest = (await db.execute(
        _eligible_snapshots_stmt(resource_id).limit(1)
    )).first()
    if latest is None:
        return 0
    latest_id = latest[0]
    key = (str(resource_id), str(latest_id))
    hit = _protected_bytes_cache.get(key)
    if hit is not None:
        return hit
    # L2: durable rollup row — shared across uvicorn workers and pods,
    # survives restarts. The per-process dict above is only an L1 that
    # saves this round trip on a warm worker. Both are keyed on the
    # latest snapshot id, so a newer snapshot is an automatic miss.
    row = (await db.execute(
        text(
            "SELECT bytes FROM resource_protected_bytes "
            "WHERE resource_id = :rid AND latest_snapshot_id = :snap"
        ),
        {"rid": str(resource_id), "snap": str(latest_id)},
    )).first()
    if row is not None:
        out = int(row[0] or 0)
    else:
        if not recompute:
            return None
        out = await _recompute_protected_bytes(db, resource_id, latest_id, deadline)
        if out is None:
            return None
    if len(_protected_bytes_cache) >= _PROTECTED_CACHE_MAX:
        _protected_bytes_cache.clear()
    _protected_bytes_cache[key] = out
    return out


def _eligible_snapshots_stmt(resource_id):
    # resource_id equality + ORDER BY created_at DESC is exactly
    # ix_snapshots_resource_created (resource_id, created_at DESC): the
    # newest eligible snapshot is one index probe + LIMIT 1, and the
    # full sibling list (rollup input) walks the same index.
    return (
        select(Snapshot.id)
        .where(
            Snapshot.resource_id == resource_id,
            Snapshot.status.in_([
                SnapshotStatus.COMPLETED,
                SnapshotStatus.PARTIAL,
                SnapshotStatus.PENDING_DELETION,
            ]),
        )
        .order_by(Snapshot.created_at.desc())
    )


def _is_statement_cancelled(exc: BaseException) -> bool:
    orig = getattr(exc, "orig", None)
    for err in (orig, getattr(orig, "__cause__", None)):
        if getattr(err, "sqlstate", None) == _STATEMENT_CANCELLED_SQLSTATE:
            return True
    return False


async def _recompute_protected_bytes(
    db: AsyncSession, resource_id, latest_id, deadline: Optional[float],
) -> Optional[int]:
    """The heavy path: hash-aggregate every eligible snapshot's items,
    then upsert the durable row. None means skipped because the budget
    ran out — waiting for a rollup slot, or the statement itself was
    cancelled by statement_timeout — and the row stays stale for the
    prewarm loop to fill.
    """
    loop = asyncio.get_running_loop()

    def _remaining() -> Optional[float]:
        return None if deadline is None else deadline - loop.time()

    remaining = _remaining()
    if remaining is not None and remaining <= 0:
        return None
    try:
        if remaining is None:
            await _heavy_rollup_sem.acquire()
        else:
            await asyncio.wait_for(_heavy_rollup_sem.acquire(), timeout=remaining)
    except asyncio.TimeoutError:
        log.warning("[backup-size] no rollup slot within budget for resource %s — skipped",
                    resource_id)
        return None
    try:
        sib_ids = [r[0] for r in (await db.execute(
            _eligible_snapshots_stmt(resource_id)
        )).all()]
        if not sib_ids:
            return 0
        remaining = _remaining()
        if remaining is not None and remaining <= 0:
            return None
        try:
            # Heavy SELECT on the replica when configured and caught up,
            # else on the primary; the upsert below is always primary.
            rollup = await _rollup_on_best_session(db, sib_ids, latest_id, remaining)
        except DBAPIError as exc:
            if deadline is None or not _is_statement_cancelled(exc):
                raise
            log.warning("[backup-size] rollup for resource %s cancelled at the request "
                        "budget — skipped, prewarm will fill it", resource_id)
            return None
        out = int(rollup[0] or 0) if rollup else 0
        await db.execute(text(_PROTECTED_UPSERT_SQL), {
            "rid": str(resource_id),
            "snap": str(latest_id),
            "bytes": out,
        })
        await db.commit()
        return out
    finally:
        _heavy_rollup_sem.release()


_PREWARM_INTERVAL_S = int(os.getenv("PROTECTED_PREWARM_INTERVAL_S", "120"))

# Resources whose newest eligible snapshot no longer matches their
# rollup row (or that have no row yet). Statuses mirror
# _protected_bytes_for_resource exactly.
_STALE_ROLLUPS_SQL = """
    WITH latest AS (
        SELECT s.resource_id,
               (array_agg(s.id ORDER BY s.created_at DESC))[1] AS latest_snapshot_id
        FROM snapshots s
        WHERE s.status IN ('COMPLETED', 'PARTIAL', 'PENDING_DELETION')
        GROUP BY s.resource_id
    )
    SELECT l.resource_id
    FROM latest l
    LEFT JOIN resource_protected_bytes p ON p.resource_id = l.resource_id
    WHERE p.resource_id IS NULL
       OR p.latest_snapshot_id <> l.latest_snapshot_id
"""

_PRUNE_ROLLUPS_SQL = """
    DELETE FROM resource_protected_bytes p
    WHERE NOT EXISTS (
        SELECT 1 FROM snapshots s
        WHERE s.resource_id = p.resource_id
          AND s.status IN ('COMPLETED', 'PARTIAL', 'PENDING_DELETION')
    )
"""


_PREWARM_LOCK_KEY = 420019  # pg advisory key: one prewarm pass fleet-wide at a time


async def _protected_prewarm_pass() -> tuple[str, int]:
    """One prewarm pass under a transaction-scoped advisory lock on the
    primary, so only ONE uvicorn worker / pod recomputes stale rollups at
    a time. 2026-10-15: both uvicorn workers ran the loop and the
    jitter did not de-sync them — 878 upserts for 566 new eligible
    snapshots, every duplicate a full Parallel Seq Scan on the primary.
    Transaction-scoped (not session-scoped) because the pooler runs in
    transaction mode. Returns (status, refreshed_count)."""
    async with async_session_factory() as lock_db:
        async with lock_db.begin():
            got = (await lock_db.execute(
                text("SELECT pg_try_advisory_xact_lock(:k)"),
                {"k": _PREWARM_LOCK_KEY},
            )).scalar()
            if not got:
                return ("skipped", 0)
            async with async_session_factory() as db:
                stale = [r[0] for r in (await db.execute(text(_STALE_ROLLUPS_SQL))).all()]
                pruned = (await db.execute(text(_PRUNE_ROLLUPS_SQL))).rowcount
                await db.commit()
            if stale or pruned:
                log.info("[prewarm] %d stale rollups, %d pruned", len(stale), pruned or 0)
            pass_started = asyncio.get_running_loop().time()
            for rid in stale:
                async with async_session_factory() as rdb:
                    await _protected_bytes_for_resource(rdb, rid)
            if stale:
                log.info("[prewarm] refreshed %d rollups in %.1fs", len(stale),
                         asyncio.get_running_loop().time() - pass_started)
            return ("refreshed", len(stale))


async def _protected_prewarm_loop():
    """Keep resource_protected_bytes fresh so no user request ever pays
    the multi-second snapshot_items rollup. 2026-07-29: the first
    Overview load after each Gold batch recomputed everything on the
    request path — 16.1s for a real user. Recompute is serial on
    purpose (one rollup at a time keeps DB pressure flat); with the
    read-through in _protected_bytes_for_resource a concurrent user
    request at worst duplicates one resource's rollup, never waits on
    the whole pass. Each pass runs under an advisory lock
    (_protected_prewarm_pass) so a second uvicorn worker or pod skips
    it instead of repeating it, and the heavy SELECT goes to the read
    replica when DB_RO_HOST is set.
    """
    await asyncio.sleep(random.uniform(5, 35))
    while True:
        try:
            await _protected_prewarm_pass()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("[prewarm] protected-bytes pass failed")
        await asyncio.sleep(_PREWARM_INTERVAL_S)


_PREWARM_ENABLED = os.getenv("PROTECTED_PREWARM_ENABLED", "true").lower() in ("true", "1", "yes")
_prewarm_task: Optional[asyncio.Task] = None


def _start_protected_prewarm() -> Optional[asyncio.Task]:
    """Start the prewarm loop at most once per process (lifespan
    re-entry, tests); PROTECTED_PREWARM_ENABLED=false is the kill
    switch for the background load on the primary."""
    global _prewarm_task
    if not _PREWARM_ENABLED:
        log.info("[prewarm] disabled via PROTECTED_PREWARM_ENABLED")
        return None
    if _prewarm_task is not None and not _prewarm_task.done():
        return _prewarm_task
    _prewarm_task = asyncio.create_task(_protected_prewarm_loop())
    return _prewarm_task


@app.get("/api/v1/dashboard/backup/size")
async def get_backup_size(
    request: Request,
    tenantId: Optional[str] = Query(None),
    serviceType: Optional[str] = Query(None),
    tz: Optional[str] = Query(
        None,
        description=(
            "IANA timezone to bucket daily growth in. "
            "Defaults to UTC."
        ),
    ),
    db: AsyncSession = Depends(get_db),
):
    service_key = parse_service_type(serviceType)
    service_resource_types = resource_types_for_service(service_key)
    client_tz = _resolve_tz(tz)

    filters = []
    if tenantId:
        filters.append(Resource.tenant_id == UUID(tenantId))
    if service_resource_types:
        filters.append(Resource.type.in_(service_resource_types))

    # Headline = PROTECTED-DATA size: newest-version content_size per
    # live item, per resource, summed. The previous source
    # (SUM(Resource.storage_bytes)) is a bytes-WRITTEN metric — the
    # worker stores "sum of bytes_added across retained snapshots",
    # which undercounts twice: content-hash dedup means re-baselines
    # upload almost nothing, and retention pruning drops the original
    # baseline's bytes while its blobs stay referenced. Observed
    # 2026-07-14: Overview said 50 GB while one user's OneDrive alone
    # held 124 GB. Mirrors the snapshot-service /storage-summary fix.
    # Parent rows carry only identity items post-Tier-2-refactor, so
    # summing per-resource rollups does not double-count children —
    # no tier2-dupes clause needed here.
    proto_res_rows = (await db.execute(
        select(Snapshot.resource_id)
        .join(Resource, Resource.id == Snapshot.resource_id)
        .where(
            Snapshot.status.in_([
                SnapshotStatus.COMPLETED,
                SnapshotStatus.PARTIAL,
                SnapshotStatus.PENDING_DELETION,
            ]),
            *filters,
        )
        .group_by(Snapshot.resource_id)
    )).all()
    # Two passes: first everything the caches already answer (cheap,
    # so a budget-cut answer still carries every warm resource), then
    # the cold ones under the request budget. Between resources the
    # client is checked — the gateway drops the connection at its read
    # timeout, and nothing should keep the primary busy for a response
    # no one will read.
    loop_time = asyncio.get_running_loop().time
    started = loop_time()
    deadline = started + _PROTECTED_REQUEST_BUDGET_S
    disconnected = False

    async def _out_of_time() -> bool:
        nonlocal disconnected
        if await request.is_disconnected():
            disconnected = True
            return True
        return loop_time() >= deadline

    total = 0
    partial = False
    cold: list = []
    for (res_id,) in proto_res_rows:
        if await _out_of_time():
            partial = True
            break
        val = await _protected_bytes_for_resource(db, res_id, recompute=False)
        if val is None:
            cold.append(res_id)
        else:
            total += val
    rolled = 0
    for res_id in cold:
        if partial or await _out_of_time():
            partial = True
            break
        val = await _protected_bytes_for_resource(db, res_id, deadline=deadline)
        if val is None:
            partial = True
            break
        total += val
        rolled += 1
    if partial:
        log.warning(
            "[backup-size] partial total after %.1fs: %d of %d cold resources "
            "rolled up (disconnected=%s, budget=%.0fs)",
            loop_time() - started, rolled, len(cold), disconnected,
            _PROTECTED_REQUEST_BUDGET_S,
        )

    # Per-day chart = bytes ADDED per day (write growth), bucketed by
    # snapshot completed_at in the client tz. The previous version
    # bucketed each resource's ENTIRE cumulative size into its
    # last_backup_at day — with daily incrementals every resource's
    # last backup is today, so the whole chart collapsed onto one bar
    # and every other day read zero (2026-07-14: "only shows data for
    # 7/14 but we had backup data yesterday").
    today = _bucket_date(datetime.utcnow(), client_tz)
    window_start = today - timedelta(days=29)
    window_start_ts = datetime.combine(window_start, datetime.min.time())

    hist_rows = (await db.execute(
        select(Snapshot.completed_at, Snapshot.bytes_added)
        .join(Resource, Resource.id == Snapshot.resource_id)
        .where(
            exclude_tier2_storage_dupes_clause(),
            Snapshot.status.in_([
                SnapshotStatus.COMPLETED,
                SnapshotStatus.PARTIAL,
                SnapshotStatus.PENDING_DELETION,
            ]),
            Snapshot.completed_at.isnot(None),
            Snapshot.completed_at >= window_start_ts,
            *filters,
        )
    )).all()
    per_day_map: dict = {}
    for ts, added in hist_rows:
        if ts is None:
            continue
        day_key = _bucket_date(ts, client_tz)
        per_day_map[day_key] = per_day_map.get(day_key, 0) + int(added or 0)

    daily_data = []
    for i in range(30):
        date = window_start + timedelta(days=i)
        daily_data.append({
            "date": date.isoformat(),
            "bytes": per_day_map.get(date, 0),
        })

    seven_day_change = sum(d["bytes"] for d in daily_data[-7:])
    one_day_change = daily_data[-1]["bytes"]

    # Storage-page tiles: raw byte fields (client formats) + linear
    # projections rooted at the protected total, using the mean daily
    # write growth across the whole 30-day window. Mean, not median —
    # re-baseline spikes are real growth the object store has to hold
    # (blobs are never deleted on Wasabi, retention_cleanup.py:55).
    daily_growth = int(round(sum(d["bytes"] for d in daily_data) / 30.0))

    return {
        "total": format_bytes(total),
        "oneDayChange": format_bytes(abs(one_day_change)) + (" ↑" if one_day_change > 0 else " ↓"),
        "oneMonthChange": format_bytes(abs(seven_day_change)) + (" ↑" if seven_day_change > 0 else " ↓"),
        "allTimeTotal": format_bytes(total),
        "dailyData": daily_data,
        "totalBytes": total,
        # True when the walk stopped early (client gone or budget spent):
        # total/projections then cover only the resources reached.
        "partial": partial,
        "dailyGrowthBytes": daily_growth,
        "projectedBytes1y": total + daily_growth * 365,
        "projectedBytes3y": total + daily_growth * 1095,
    }
