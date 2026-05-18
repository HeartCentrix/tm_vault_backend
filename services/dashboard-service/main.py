"""Dashboard Service - Aggregated metrics and statistics"""
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional
from uuid import UUID
from datetime import datetime, timedelta, timezone, date as _date
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import FastAPI, Depends, Query, HTTPException


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

from shared.database import get_db, close_db, AsyncSession, engine
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
    yield
    await close_db()


app = FastAPI(title="Dashboard Service", version="1.0.0", lifespan=lifespan)


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
# clicks into that tab — anything else is a lie. SharePoint additionally
# applies the same group-name-collision exclusion that
# /api/v1/resources/by-type?type=SHAREPOINT_SITE applies (handled below).
PROTECTION_BUCKETS = {
    "users": {ResourceType.ENTRA_USER},
    "sharedMailboxes": {ResourceType.SHARED_MAILBOX},
    "rooms": {ResourceType.ROOM_MAILBOX},
    "sharepointSites": {ResourceType.SHAREPOINT_SITE},
    "groupsAndTeams": {ResourceType.ENTRA_GROUP, ResourceType.M365_GROUP, ResourceType.TEAMS_CHANNEL},
    "entraId": {ResourceType.ENTRA_DIRECTORY},
    "powerPlatform": {ResourceType.POWER_BI, ResourceType.POWER_APPS, ResourceType.POWER_AUTOMATE},
}

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
    for r in rows:
        key = _batch_group_key(*r)
        groups[key].append((r.status, r.result))

    success = warnings = failures = 0
    for children in groups.values():
        outcome = _roll_up_group_outcome(children)
        if outcome == "success":
            success += 1
        elif outcome == "warnings":
            warnings += 1
        elif outcome == "failures":
            failures += 1

    return {"success": success, "warnings": warnings, "failures": failures}


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

    filters = [Resource.status.in_([ResourceStatus.DISCOVERED, ResourceStatus.ACTIVE])]
    if tenantId:
        filters.append(Resource.tenant_id == UUID(tenantId))
    if service_resource_types:
        filters.append(Resource.type.in_(service_resource_types))
    # Mirror resource-service's UI_HIDDEN_TYPES filter so the Overview card
    # universe matches the tab list universe exactly. Without this, a hidden
    # type (e.g. TEAMS_CHANNEL — redundant with its M365_GROUP twin) inflates
    # the Groups & Teams total here even though /by-type hides those rows.
    # The Tab shows 11; previously this query summed 15 because TEAMS_CHANNEL
    # rows still got counted. See shared.models.UI_HIDDEN_TYPES for per-type
    # rationale.
    filters.append(Resource.type.notin_(UI_HIDDEN_TYPES))

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

    # SharePoint sites: the Sites tab hides sites whose name collides with an
    # M365 group / Entra group / Teams channel (the admin API surfaces the
    # group's display name as the site title, so a single Team appears as
    # both a SP site row AND a Groups & Teams row otherwise). The Overview
    # card must hide the same rows or the denominator inflates above what
    # the operator can actually click into. The exclusion below MUST stay
    # in sync with resource-service /api/v1/resources/by-type's
    # sp_exclude_clause — a future change to the rule has to touch both.
    sp_params = {}
    sp_tenant_clause = ""
    if tenantId:
        sp_tenant_clause = "AND r.tenant_id = :tenant_id"
        sp_params["tenant_id"] = str(UUID(tenantId))
    sp_total_row = (await db.execute(text(f"""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE r.sla_policy_id IS NOT NULL) AS protected_count
        FROM resources r
        WHERE r.type = 'SHAREPOINT_SITE'
          AND r.status IN ('DISCOVERED', 'ACTIVE')
          {sp_tenant_clause}
          AND NOT EXISTS (
            SELECT 1 FROM resources g
            WHERE g.tenant_id = r.tenant_id
              AND g.type IN ('M365_GROUP', 'ENTRA_GROUP', 'TEAMS_CHANNEL')
              AND LOWER(g.display_name) = LOWER(r.display_name)
              AND (
                    COALESCE(r.email, '') = ''
                 OR LOWER(COALESCE(g.email, '')) = LOWER(COALESCE(r.email, ''))
              )
          )
    """), sp_params)).first()
    bucket_values["sharepointSites"] = {
        "total": int(sp_total_row.total or 0),
        "protectedCount": int(sp_total_row.protected_count or 0),
    }

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


@app.get("/api/v1/dashboard/backup/size")
async def get_backup_size(
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

    # Single source of truth: Resource.storage_bytes. The snapshots table holds
    # many legacy FAILED rows and bytes_added is unreliable per snapshot, so using
    # it produced a dashboard where the headline said 7.9 MB but the chart/pills
    # read 0. All four fields below are derived from resources only.
    # Dedup: a Tier-1 ONEDRIVE/MAILBOX walk + a Tier-2 USER_ONEDRIVE/USER_MAIL
    # walk both write storage_bytes for the same user. Exclude the Tier-2 dupe.
    total = int((await db.execute(
        select(func.sum(Resource.storage_bytes))
        .where(exclude_tier2_storage_dupes_clause(), *filters)
    )).scalar() or 0)

    # Per-day growth based on when each resource was last backed up.
    # Window anchored to client tz so the 30-day frame and per-day
    # bucket boundaries align with the operator's calendar. Without
    # this, a backup at 22:31 UTC (04:01 IST) lands in the previous
    # IST day on the chart.
    today = _bucket_date(datetime.utcnow(), client_tz)
    window_start = today - timedelta(days=29)
    window_start_ts = datetime.combine(window_start, datetime.min.time())

    # Pull raw last_backup_at timestamps + bytes; bucket in Python by
    # client_tz instead of Postgres date_trunc (which buckets in the
    # DB session's tz, not the operator's).
    per_day_rows = (await db.execute(
        select(Resource.last_backup_at, Resource.storage_bytes)
        .where(
            exclude_tier2_storage_dupes_clause(),
            Resource.last_backup_at.isnot(None),
            Resource.last_backup_at >= window_start_ts,
            *filters,
        )
    )).all()
    per_day_map: dict = {}
    for row in per_day_rows:
        ts = row[0]
        if ts is None:
            continue
        day_key = _bucket_date(ts, client_tz)
        per_day_map[day_key] = per_day_map.get(day_key, 0) + int(row[1] or 0)

    baseline = int((await db.execute(
        select(func.sum(Resource.storage_bytes)).where(
            exclude_tier2_storage_dupes_clause(),
            Resource.last_backup_at.isnot(None),
            Resource.last_backup_at < window_start_ts,
            *filters,
        )
    )).scalar() or 0)

    daily_data = []
    running_total = baseline
    for i in range(30):
        date = window_start + timedelta(days=i)
        running_total += per_day_map.get(date, 0)
        daily_data.append({"date": date.isoformat(), "bytes": running_total})

    seven_day_change = daily_data[-1]["bytes"] - (daily_data[-8]["bytes"] if len(daily_data) > 7 else 0)
    one_day_change = daily_data[-1]["bytes"] - (daily_data[-2]["bytes"] if len(daily_data) > 1 else 0)

    return {
        "total": format_bytes(total),
        "oneDayChange": format_bytes(abs(one_day_change)) + (" ↑" if one_day_change > 0 else " ↓"),
        "oneMonthChange": format_bytes(abs(seven_day_change)) + (" ↑" if seven_day_change > 0 else " ↓"),
        "allTimeTotal": format_bytes(total),
        "dailyData": daily_data,
    }
