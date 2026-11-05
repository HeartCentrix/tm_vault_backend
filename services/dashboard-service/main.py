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
from sqlalchemy import select, func, text, and_, or_, event
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
    await _refresh_engine.dispose()
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
    ResourceType.USER_ARCHIVE,
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
        return f"{bytes_val / 1024**2:.1f} MiB"
    return f"{bytes_val / 1024**3:.1f} GiB"


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


# Protected size is a durable read model; HTTP requests never rebuild item history.
from shared.protected_size import read_summary as _read_size_summary
from shared.protected_size import refresh_once as _refresh_size_once
from shared.database import _engine_kwargs
from shared.auth_enforcement import verified_user

_SIZE_REQUEST_BUDGET_S = float(os.getenv('PROTECTED_REQUEST_BUDGET_S', '5'))
_PREWARM_INTERVAL_S = int(os.getenv('PROTECTED_PREWARM_INTERVAL_S', '300'))
_REFRESH_IDLE_S = float(os.getenv('PROTECTED_REFRESH_IDLE_S', '2'))
_PREWARM_ENABLED = os.getenv('PROTECTED_PREWARM_ENABLED', 'true').lower() in ('true', '1', 'yes')
_prewarm_task: Optional[asyncio.Task] = None
_RO_HOST = os.getenv('DB_RO_HOST', '').strip()
_RO_PORT = int(os.getenv('DB_RO_PORT', '5432'))
_ro_engine = None
_ro_session_factory = None


def _size_engine(url):
    kwargs = _engine_kwargs()
    kwargs.update(pool_size=1, max_overflow=0, pool_timeout=5)
    size_engine = create_async_engine(url, **kwargs)
    schema = '"' + settings.DB_SCHEMA.replace('"', '""') + '",public'

    @event.listens_for(size_engine.sync_engine, 'begin')
    def set_transaction_schema(connection):
        # Transaction pooling can ignore startup search_path; set it on the actual backend.
        connection.exec_driver_sql('SET LOCAL search_path TO ' + schema)

    return size_engine


_refresh_engine = _size_engine(settings.DATABASE_URL)
_refresh_session_factory = async_sessionmaker(_refresh_engine, class_=AsyncSession, expire_on_commit=False)
if _RO_HOST:
    _ro_engine = _size_engine(make_url(settings.DATABASE_URL).set(host=_RO_HOST, port=_RO_PORT))
    _ro_session_factory = async_sessionmaker(_ro_engine, class_=AsyncSession, expire_on_commit=False)


async def _protected_prewarm_pass() -> dict:
    return await _refresh_size_once(
        _refresh_session_factory, source_factory=_ro_session_factory,
        page_size=1000, statement_timeout_ms=5000,
    )


async def _protected_prewarm_loop():
    await asyncio.sleep(random.uniform(.1, 1))
    failures = 0
    while True:
        try:
            result = await _protected_prewarm_pass()
            failures = 0
            idle = result.get('status') in ('idle', 'busy', 'deferred', 'replica_lag',
                                          'replica_deferred', 'backoff', 'retry', 'lost_lease')
            await asyncio.sleep(_REFRESH_IDLE_S if idle else .05)
        except asyncio.CancelledError:
            raise
        except Exception:
            failures += 1
            log.exception('[protected-size] background refresh failed; published totals remain available')
            await asyncio.sleep(min(30, 2 ** min(failures, 5)))


def _start_protected_prewarm() -> Optional[asyncio.Task]:
    global _prewarm_task
    if not _PREWARM_ENABLED:
        return None
    if _prewarm_task is None or _prewarm_task.done():
        _prewarm_task = asyncio.create_task(_protected_prewarm_loop())
    return _prewarm_task


async def _size_scope(db: AsyncSession, principal: dict, requested_tenant) -> Optional[tuple[UUID, ...]]:
    roles = {str(role).upper() for role in principal.get('roles', [])}
    requested = None
    if requested_tenant is not None:
        try:
            requested = UUID(str(requested_tenant))
        except (ValueError, TypeError):
            raise HTTPException(status_code=422, detail='Invalid tenantId')
    if principal.get('_principal') == 'internal' or 'SUPER_ADMIN' in roles:
        return (requested,) if requested else None
    entitled = set()
    for tenant in principal.get('tenant_ids', []):
        try:
            entitled.add(UUID(str(tenant)))
        except (ValueError, TypeError):
            continue
    if 'ORG_ADMIN' in roles:
        try:
            organization = UUID(str(principal.get('org_id')))
        except (ValueError, TypeError):
            entitled = set()
        else:
            entitled = set((await db.execute(select(Tenant.id).where(Tenant.org_id == organization))).scalars().all())
    if requested:
        if requested not in entitled:
            raise HTTPException(status_code=403, detail='Not entitled to the requested tenant')
        return (requested,)
    return tuple(sorted(entitled, key=lambda value: value.int))


_SIZE_HISTORY_SQL = '''
    SELECT timezone(:tz, s.completed_at AT TIME ZONE 'UTC')::date AS day,
           COALESCE(SUM(s.bytes_added), 0)::bigint AS bytes
    FROM snapshots s JOIN resources r ON r.id = s.resource_id
    WHERE s.status::text IN ('COMPLETED', 'PARTIAL', 'PENDING_DELETION')
      AND s.completed_at >= :window_start AND s.completed_at < :window_end
      AND (:all_tenants OR r.tenant_id = ANY(CAST(:tenant_ids AS uuid[])))
      AND (:all_types OR r.type::text = ANY(CAST(:resource_types AS text[])))
    GROUP BY timezone(:tz, s.completed_at AT TIME ZONE 'UTC')::date
'''


def _size_response(summary: dict, daily_data: list, *, history_complete: bool) -> dict:
    total = summary['totalBytes']
    day = daily_data[-1]['bytes'] if daily_data else 0
    week = sum(row['bytes'] for row in daily_data[-7:])
    growth = int(round(sum(row['bytes'] for row in daily_data) / 30.0))
    baseline_complete = (
        total is not None and not summary.get('partial') and not summary.get('stale')
        and not summary.get('initializingScope') and summary.get('freshness') == 'fresh'
    )
    can_project = baseline_complete and history_complete
    def change(value):
        return format_bytes(abs(value)) + (' ↑' if value > 0 else ' ↓')
    return {
        **summary,
        'total': format_bytes(total) if total is not None else 'Calculating',
        'allTimeTotal': format_bytes(total) if total is not None else None,
        'oneDayChange': change(day) if history_complete else None,
        'sevenDayChange': change(week) if history_complete else None,
        'oneMonthChange': change(week) if history_complete else None,  # Existing seven-day client alias.
        'dailyData': daily_data,
        'dailyGrowthBytes': growth if history_complete else None,
        'historyStatus': 'complete' if history_complete else 'unavailable',
        'historyPartial': not history_complete,
        'growthMetric': 'captured-change-volume',
        'projectionKind': 'linear-capture-volume-estimate',
        'projectedBytes1y': total + growth * 365 if can_project else None,
        'projectedBytes3y': total + growth * 1095 if can_project else None,
    }


@app.get('/api/v1/dashboard/backup/size')
async def get_backup_size(
    request: Request,
    tenantId: Optional[UUID] = Query(None),
    serviceType: Optional[str] = Query(None),
    tz: Optional[str] = Query(None, description='IANA timezone for daily captured-volume buckets.'),
    db: AsyncSession = Depends(get_db),
):
    summary = None
    history_complete = False
    daily_data = []
    tasks = []

    async def stop_reads():
        for task in tasks:
            task.cancel()
        if tasks:
            try:
                await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=.5)
            except TimeoutError:
                # An unresponsive canceled connection must never return to the pool.
                with suppress(Exception):
                    await asyncio.wait_for(db.invalidate(), timeout=.5)
            tasks.clear()

    async def read():
        nonlocal summary, history_complete, daily_data
        principal = verified_user(request)
        service_key = parse_service_type(serviceType)
        resource_types = resource_types_for_service(service_key)
        type_names = tuple(sorted(t.value for t in resource_types)) if resource_types else None
        schema = '"' + settings.DB_SCHEMA.replace('"', '""') + '",public'
        await db.execute(text(
            "SELECT set_config('statement_timeout', :timeout, true), "
            "set_config('search_path', :schema, true)"
        ), {'timeout': str(max(1, int(_SIZE_REQUEST_BUDGET_S * 1000))), 'schema': schema})
        tenant_ids = await _size_scope(db, principal, tenantId)
        summary = await _read_size_summary(db, tenant_ids=tenant_ids, service_types=type_names)
        if await request.is_disconnected():
            raise HTTPException(status_code=499, detail='Client disconnected')
        client_tz = _resolve_tz(tz)
        today = datetime.now(timezone.utc).astimezone(client_tz).date()
        start = today - timedelta(days=29)
        start_utc = datetime.combine(start, datetime.min.time(), client_tz).astimezone(timezone.utc).replace(tzinfo=None)
        end_utc = datetime.combine(today + timedelta(days=1), datetime.min.time(), client_tz).astimezone(timezone.utc).replace(tzinfo=None)
        hist_rows = (await db.execute(text(_SIZE_HISTORY_SQL), {
            'tz': str(client_tz), 'window_start': start_utc, 'window_end': end_utc,
            'all_tenants': tenant_ids is None, 'tenant_ids': list(tenant_ids or ()),
            'all_types': type_names is None, 'resource_types': list(type_names or ()),
        })).all()
        buckets = {str(day): int(value or 0) for day, value in hist_rows}
        daily_data = [{'date': (start + timedelta(days=i)).isoformat(),
                       'bytes': buckets.get((start + timedelta(days=i)).isoformat(), 0)} for i in range(30)]
        history_complete = True
        # Finish the read transaction inside the handler budget; get_db has no SQL to commit.
        await db.rollback()

    async def disconnected():
        while not await request.is_disconnected():
            await asyncio.sleep(.025)

    try:
        async with asyncio.timeout(_SIZE_REQUEST_BUDGET_S):
            if await request.is_disconnected():
                raise HTTPException(status_code=499, detail='Client disconnected')
            work = asyncio.create_task(read())
            monitor = asyncio.create_task(disconnected())
            tasks = [work, monitor]
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            if monitor in done:
                await monitor
                raise HTTPException(status_code=499, detail='Client disconnected')
            await work
    except (TimeoutError, DBAPIError):
        # AsyncSession cannot roll back while its child is still using the connection.
        await stop_reads()
        log.warning('[protected-size] bounded dashboard read unavailable', exc_info=True)
        with suppress(Exception):
            await asyncio.wait_for(db.rollback(), timeout=.5)
        if summary is None:
            raise HTTPException(status_code=503, detail='Backup size is temporarily unavailable')
    finally:
        await stop_reads()
    return _size_response(summary, daily_data, history_complete=history_complete)
