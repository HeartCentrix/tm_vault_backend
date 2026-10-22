"""
Backup Scheduler Service - SLA Policy Frequency-Based Scheduling
Port: 8008

Responsibilities:
- Scan active resources grouped by their assigned SLA policy
- Schedule backup jobs dynamically based on each policy's frequency field
- Filter resources by SLA policy backup flags (backup_exchange, backup_onedrive, etc.)
- Group resources by type and tenant for batch processing
- Dispatch mass backup jobs to RabbitMQ queues
- Track SLA compliance and violations
"""
import asyncio
import json
import os
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple, Sequence
from fastapi import FastAPI, BackgroundTasks
from sqlalchemy import select, and_, func, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from shared.database import async_session_factory
from shared.models import (
    Resource, SlaPolicy, Tenant, Job, Organization, Snapshot,
    ResourceType, ResourceStatus, JobType, JobStatus, SnapshotType, TenantStatus, TenantType
)
from shared.message_bus import (
    message_bus,
    create_mass_backup_message,
    create_backup_message,
    create_audit_event_message,
)
from shared.config import settings
from shared.power_bi_client import PowerBIClient
from shared.azure_storage import azure_storage_manager
from shared.audit import emit_backup_triggered, internal_audit_headers

# Sibling-file load: backup-scheduler has a hyphen so a plain
# `from .dispatch_policy import` won't resolve. Same importlib pattern
# audit-service uses for activity_backup.py.
import importlib.util as _ilu_dp
import pathlib as _pl_dp
import sys as _sys_dp
_dp_path = _pl_dp.Path(__file__).resolve().parent / "dispatch_policy.py"
_dp_spec = _ilu_dp.spec_from_file_location(
    "backup_scheduler_dispatch_policy", _dp_path,
)
dispatch_policy = _ilu_dp.module_from_spec(_dp_spec)
_sys_dp.modules["backup_scheduler_dispatch_policy"] = dispatch_policy
_dp_spec.loader.exec_module(dispatch_policy)
from shared.sla_workloads import RESOURCE_TYPE_TO_SLA_FLAG, resource_type_enabled

from shared.audit_middleware import AuditMiddleware  # Ops/Data Check 3: mutation-audit backstop
app = FastAPI(title="Backup Scheduler Service", version="3.0.0")
app.add_middleware(AuditMiddleware, service_name="backup-scheduler")

# APScheduler for dynamic SLA policy scheduling
scheduler = AsyncIOScheduler()

RESOURCE_TYPE_DISPLAY_NAMES: Dict[str, str] = {
    "MAILBOX": "Exchange mailboxes",
    "SHARED_MAILBOX": "shared mailboxes",
    "ROOM_MAILBOX": "room mailboxes",
    "ONEDRIVE": "OneDrive",
    "SHAREPOINT_SITE": "SharePoint",
    "TEAMS_CHANNEL": "Teams channel data",
    "TEAMS_CHAT": "Teams chats",
    "TEAMS_CHAT_EXPORT": "Teams chat exports",
    "USER_MAIL": "User mailboxes",
    "USER_ONEDRIVE": "User OneDrive",
    "USER_CONTACTS": "User contacts",
    "USER_CALENDAR": "User calendars",
    "USER_CHATS": "User chats",
    "USER_ARCHIVE": "Online archives",
    "ENTRA_DIRECTORY": "Azure Active Directory",
    "ENTRA_USER": "Entra user data",
    "ENTRA_GROUP": "Entra group data",
    "ENTRA_APP": "Entra app data",
    "ENTRA_SERVICE_PRINCIPAL": "Entra service principal data",
    "ENTRA_DEVICE": "Entra device data",
    "ENTRA_ROLE": "Entra role data",
    "ENTRA_ADMIN_UNIT": "Entra administrative unit data",
    "ENTRA_AUDIT_LOG": "Entra audit data",
    "INTUNE_MANAGED_DEVICE": "Intune managed devices",
    "POWER_BI": "Power BI",
    "POWER_APPS": "Power Apps",
    "POWER_AUTOMATE": "Power Automate",
    "POWER_DLP": "Power DLP",
    "COPILOT": "Copilot",
    "PLANNER": "Planner",
    "TODO": "Microsoft To Do",
    "ONENOTE": "OneNote",
    "AZURE_VM": "Azure virtual machines",
    "AZURE_SQL_DB": "Azure SQL databases",
    "AZURE_POSTGRESQL": "Azure PostgreSQL",
    "AZURE_POSTGRESQL_SINGLE": "Azure PostgreSQL",
}

SLA_FLAG_DISPLAY_NAMES: Dict[str, str] = {
    "backup_exchange": "Exchange",
    "backup_exchange_archive": "Online archives",
    "backup_onedrive": "OneDrive and OneNote",
    "backup_sharepoint": "SharePoint",
    "backup_teams": "Teams channels",
    "backup_teams_chats": "Teams chats",
    "backup_entra_id": "Entra ID",
    "backup_power_platform": "Power Platform",
    "backup_copilot": "Copilot",
    "planner": "Planner",
    "tasks": "Tasks",
    "group_mailbox": "group mailbox",
    "backup_azure_vm": "Virtual machines",
    "backup_azure_sql": "Azure SQL databases",
    "backup_azure_postgresql": "Azure PostgreSQL servers",
}

# AZ-4: Azure workload queue routing
AZURE_WORKLOAD_QUEUES = {
    ResourceType.AZURE_VM: "azure.vm",
    ResourceType.AZURE_SQL_DB: "azure.sql",
    ResourceType.AZURE_POSTGRESQL: "azure.postgres",
    ResourceType.AZURE_POSTGRESQL_SINGLE: "azure.postgres",
}


# Valid APScheduler day-of-week abbreviations
DAY_MAP = {
    "MON": "mon", "TUE": "tue", "WED": "wed", "THU": "thu",
    "FRI": "fri", "SAT": "sat", "SUN": "sun",
}


def _parse_window_start(window_start: str | None) -> tuple[int, int]:
    """Parse 'HH:MM' (or 'HHMM') from policy.backup_window_start. Returns
    (hour, minute) UTC. Falls back to (2, 0) — afi default 02:00 — on any
    parse failure so a malformed value never breaks the scheduler."""
    if not window_start:
        return (2, 0)
    try:
        s = window_start.strip()
        if ":" in s:
            h, m = s.split(":", 1)
            return (int(h) % 24, int(m) % 60)
        if len(s) >= 3 and s.isdigit():  # "0830"
            return (int(s[:-2]) % 24, int(s[-2:]) % 60)
    except Exception:
        pass
    return (2, 0)


def _policy_minute_jitter(policy_id: str | None) -> int:
    """Deterministic minute offset (0–54) for a given policy ID.

    R3.1 — without jitter, every DAILY policy fires at HH:00:00 and a
    100-tenant deployment thundering-herds the worker pool + Graph API at
    the same moment. A stable per-policy offset spreads load across the
    hour while keeping the schedule predictable for the same policy across
    deploys (deterministic from the ID, not random)."""
    if not policy_id:
        return 0
    import hashlib
    h = hashlib.md5(str(policy_id).encode()).digest()
    return h[0] % 55  # 0..54 — leaves the last 5 minutes of the hour clear


def frequency_to_cron_params(
    frequency: str,
    backup_days: list[str] | None = None,
    window_start: str | None = None,
    policy_id: str | None = None,
):
    """Convert SLA policy frequency + window into APScheduler cron parameters.

    afi-parity behaviour:
      THREE_DAILY — 3x/day, fires at window_start + 0h/+8h/+16h
      DAILY       — 1x/day at window_start (default 02:00 UTC)
      MANUAL      — returns None; caller must skip schedule registration

    Per-policy minute jitter (R3.1) is added to base_minute so concurrent
    policies stagger naturally without operator intervention.

    backup_days restricts to a day-of-week subset (only applied when fewer
    than 7 days are selected — selecting all 7 is equivalent to no restriction).
    """
    if frequency == "MANUAL":
        return None

    base_hour, base_minute = _parse_window_start(window_start)
    jitter = _policy_minute_jitter(policy_id)
    final_minute = (base_minute + jitter) % 60
    # Only roll forward an hour if jitter pushes us past 60min
    hour_carry = (base_minute + jitter) // 60

    # Day-of-week restriction
    dow_clause: dict = {}
    if backup_days:
        aps_days = [DAY_MAP.get(d.upper(), d.lower()) for d in backup_days if DAY_MAP.get(d.upper())]
        if aps_days and len(aps_days) < 7:
            dow_clause["day_of_week"] = ",".join(aps_days)

    if frequency == "THREE_DAILY":
        # Three windows starting at base_hour, base_hour+8, base_hour+16 (mod 24)
        hours = ",".join(str((base_hour + hour_carry + offset) % 24) for offset in (0, 8, 16))
        return {"trigger": "cron", "hour": hours, "minute": final_minute, **dow_clause}

    # DAILY (and any legacy / unrecognized value)
    return {"trigger": "cron", "hour": (base_hour + hour_carry) % 24, "minute": final_minute, **dow_clause}


def resolve_finished_job_status(
    n_snaps: int, n_open: int, n_completed: int, n_failed: int
) -> Optional[str]:
    """Decide the terminal status for a non-terminal (QUEUED/RUNNING) BACKUP
    job whose work may already be done.

    Returns None to leave the job alone — either it still has in-flight
    snapshots (genuinely running) or it has none yet (the outbox reconciler's
    job to republish). Otherwise returns the terminal status to flip it to.

    This backs the orphan-job reaper that closes the "snapshots all terminal
    but the job row never flipped" gap — single-resource triggers (which use
    resource_id, not batch_resource_ids) and workers that died before the
    job-level update — which left the Activity feed showing 'In Progress'
    forever.
    """
    if n_snaps <= 0 or n_open > 0:
        return None
    return "COMPLETED" if n_completed > 0 else "FAILED"


def _utc_naive(value: datetime) -> datetime:
    """Normalize datetimes for comparisons with the DB's UTC-naive columns."""
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def _policy_fire_times_between(policy: SlaPolicy, start: datetime, end: datetime) -> List[datetime]:
    """Reconstruct scheduled SLA fire times without relying on APScheduler memory."""
    start = _utc_naive(start)
    end = _utc_naive(end)
    if end < start:
        return []

    cron_params = frequency_to_cron_params(
        getattr(policy, "frequency", "DAILY"),
        getattr(policy, "backup_days", None),
        getattr(policy, "backup_window_start", None),
        policy_id=str(getattr(policy, "id", "")) if getattr(policy, "id", None) else None,
    )
    if cron_params is None:
        return []

    raw_hours = str(cron_params["hour"]).split(",")
    hours = [int(hour) for hour in raw_hours if str(hour).strip() != ""]
    minute = int(cron_params["minute"])
    allowed_days = {
        day.strip().lower()
        for day in str(cron_params.get("day_of_week", "")).split(",")
        if day.strip()
    }

    fires: List[datetime] = []
    current_day = datetime(start.year, start.month, start.day)
    last_day = datetime(end.year, end.month, end.day)
    while current_day <= last_day:
        day_name = current_day.strftime("%a").lower()[:3]
        if not allowed_days or day_name in allowed_days:
            for hour in hours:
                fire = current_day.replace(hour=hour, minute=minute, second=0, microsecond=0)
                if start <= fire <= end:
                    fires.append(fire)
        current_day += timedelta(days=1)

    return sorted(fires)


def build_sla_skip_message(resource_type: str, policy: SlaPolicy) -> tuple[str, str | None, str | None]:
    workload_name = RESOURCE_TYPE_DISPLAY_NAMES.get(resource_type, resource_type.replace("_", " ").title())
    flag_name = RESOURCE_TYPE_TO_SLA_FLAG.get(resource_type)
    flag_label = SLA_FLAG_DISPLAY_NAMES.get(flag_name or "", flag_name)

    if flag_label:
        message = (
            f"Scheduled backup skipped because SLA '{policy.name}' does not cover {workload_name}. "
            f"Enable '{flag_label}' in the policy to include this resource."
        )
    else:
        message = (
            f"Scheduled backup skipped because SLA '{policy.name}' has no workload mapping for {workload_name}."
        )
    return message, flag_name, flag_label


try:
    from shared import sla_metrics as _sla_metrics
    _sla_metrics.init()
except Exception:
    _sla_metrics = None  # type: ignore


async def _emit_policy_audit(
    action: str,
    tenant_id: str,
    policy: "SlaPolicy",
    details: Optional[Dict[str, Any]] = None,
    outcome: str = "SUCCESS",
) -> None:
    """Publish an audit.events message for a policy-driven Azure mutation
    (CMK status change, WORM lock, legal hold toggle, lifecycle reconcile
    failure). Never raises — observability must not block reconciliation.

    Subscribers: audit-service writes to `audit_events` for SOC 2 trail;
    downstream alerting consumers (Slack, PagerDuty) can subscribe to the
    same queue with their own routing key filters.
    """
    try:
        msg = create_audit_event_message(
            action=action,
            tenant_id=tenant_id,
            actor_type="SYSTEM",
            resource_id=str(policy.id),
            resource_type="SLA_POLICY",
            resource_name=policy.name,
            outcome=outcome,
            details={
                "policy_id": str(policy.id),
                "policy_name": policy.name,
                "source": "backup_scheduler",
                **(details or {}),
            },
        )
        await message_bus.publish("audit.events", msg, priority=4)
    except Exception as exc:
        # Audit must not crash reconcile. Log and move on.
        print(f"[AUDIT] policy-audit publish failed action={action} policy={policy.id}: {exc}")
        try:
            if _sla_metrics is not None:
                _sla_metrics.inc_audit_publish_failed()
        except Exception:
            pass


async def publish_cycle_audit_event(
    policy: SlaPolicy, action: str, details: Dict[str, Any],
    tenant_id: Optional[str] = None, *,
    outcome: str = "PARTIAL", priority: int = 3,
):
    """Emit ONE cycle-level warning event for a policy-wide dispatch
    decision (overlap skip / override / concurrency deferral).

    Cycle-level on purpose: a 33-resource deferral emitted per resource
    would flood the Activity feed with 33 warning rows. Per-resource
    detail rides in ``details.resource_ids``.

    ``resource_type``/``resource_name`` are set because the Activity
    warning row renders ``resource_name or resource_type or "Unknown
    resource"`` — without them every one of these reads "Unknown
    resource".

    Waves pass ``outcome='SUCCESS'`` — a wave is normal progress, not a
    PARTIAL cycle.
    """
    if tenant_id is None:
        tenant_id = str(getattr(policy, "tenant_id", "") or "")
    if not tenant_id:
        # tenant_id is nullable on the column but carries an FK to
        # tenants.id, and the Activity feed filters by tenantId — a
        # tenant-less event would be invisible anyway. Log and move on
        # rather than risk an FK error aborting the dispatch.
        print(f"[SCHEDULER] {action}: no tenant on policy "
              f"'{policy.name}' — logging only, no audit event")
        return
    try:
        audit_message = create_audit_event_message(
            action=action,
            tenant_id=tenant_id,
            actor_type="SYSTEM",
            resource_id=None,
            resource_type="SLA_POLICY",
            resource_name=policy.name,
            outcome=outcome,
            details=details,
        )
        await message_bus.publish("audit.events", audit_message, priority=priority)
    except Exception as exc:
        # Audit is observability, not the control path — never let a bus
        # hiccup abort or double-run a dispatch decision.
        print(f"[SCHEDULER] {action}: audit publish failed ({exc}) — "
              f"decision already applied")


async def publish_sla_skip_audit_events(policy: SlaPolicy, skipped_resources: List[Resource]):
    """Emit warning audit events for resources skipped by SLA coverage filters."""
    if not skipped_resources:
        return

    for resource in skipped_resources:
        resource_type = resource.type.value if hasattr(resource.type, "value") else str(resource.type)
        message, flag_name, flag_label = build_sla_skip_message(resource_type, policy)
        audit_message = create_audit_event_message(
            action="BACKUP_SKIPPED_SLA_SCOPE",
            tenant_id=str(resource.tenant_id),
            actor_type="SYSTEM",
            resource_id=str(resource.id),
            resource_type=resource_type,
            resource_name=resource.display_name,
            outcome="PARTIAL",
            details={
                "message": message,
                "skip_reason": "sla_scope_mismatch",
                "policy_id": str(policy.id),
                "policy_name": policy.name,
                "required_flag": flag_name,
                "required_flag_label": flag_label,
                "resource_status": resource.status.value if hasattr(resource.status, "value") else str(resource.status),
                "source": "backup_scheduler",
            },
        )
        await message_bus.publish("audit.events", audit_message, priority=3)


async def publish_license_skip_audit_events(policy: SlaPolicy, skipped_resources: List[Resource]):
    """Emit audit events for workloads skipped because the user has no M365
    license for them. Distinct from ``publish_sla_skip_audit_events`` (SLA
    scope mismatch) — the reason here is entitlement, not policy config, so
    the message and ``skip_reason`` differ. Auto-resolves when a license is
    added (discovery clears ``license_missing``)."""
    if not skipped_resources:
        return
    for resource in skipped_resources:
        resource_type = resource.type.value if hasattr(resource.type, "value") else str(resource.type)
        meta = getattr(resource, "extra_data", None) or {}
        hint = meta.get("license_hint") or "the required M365 service"
        audit_message = create_audit_event_message(
            action="BACKUP_SKIPPED_NO_LICENSE",
            tenant_id=str(resource.tenant_id),
            actor_type="SYSTEM",
            resource_id=str(resource.id),
            resource_type=resource_type,
            resource_name=resource.display_name,
            outcome="PARTIAL",
            details={
                "message": (
                    f"Skipped — this user has no {hint} license, so this "
                    f"workload cannot be backed up. It resumes automatically "
                    f"once a license is assigned."
                ),
                "skip_reason": "no_license",
                "license_hint": hint,
                "policy_id": str(policy.id),
                "policy_name": policy.name,
                "resource_status": resource.status.value if hasattr(resource.status, "value") else str(resource.status),
                "source": "backup_scheduler",
            },
        )
        await message_bus.publish("audit.events", audit_message, priority=3)


@app.on_event("startup")
async def startup():
    """Initialize services on startup and schedule jobs per SLA policy"""
    # Auto-create schema and tables if they don't exist
    from shared.database import init_db as db_init_db
    from shared.storage.startup import startup_router
    from shared import core_metrics
    core_metrics.init()
    await db_init_db()
    await startup_router()
    # At-rest encryption hook so scheduler blob reads decrypt (no-op unless
    # BACKUP_ENCRYPTION_* env is configured).
    try:
        from shared.crypto import bootstrap as _enc_bootstrap
        if await _enc_bootstrap.install(async_session_factory):
            print("[SCHEDULER] at-rest encryption hook installed")
    except Exception as e:
        print(f"[SCHEDULER] encryption hook install failed (non-fatal): {e}")

    await message_bus.connect()

    # Dynamically schedule backup jobs for each active SLA policy
    await schedule_all_policies()

    # Durable backstops for in-memory APScheduler state. Startup rebuilds
    # policies from the DB, this sweep catches later policy/assignment changes
    # if the on-save HTTP nudge is dropped during a deploy or network blip.
    scheduler.add_job(
        schedule_all_policies,
        "interval",
        seconds=max(60, settings.SCHEDULER_RESCHEDULE_INTERVAL_SECONDS),
        id="sla_policy_reschedule_sweep",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        catch_up_missed_policy_runs,
        "interval",
        seconds=max(60, settings.SCHEDULER_CATCHUP_INTERVAL_SECONDS),
        id="sla_policy_catchup_sweep",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    # Deferred-resource drain: re-dispatch what the concurrency cap held
    # back, in waves, as in-flight work finishes (see drain_policy_cycle).
    # max_instances=1 + coalesce so a slow tick never stacks; a 30 s
    # misfire grace because APScheduler's default (1 s) silently drops a
    # tick that lands while the loop is busy.
    scheduler.add_job(
        drain_policy_cycles, "interval",
        seconds=dispatch_policy.drain_tick_seconds(),
        id="policy_cycle_drain",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=30,
    )

    # Schedule SLA violation check every 30 minutes
    scheduler.add_job(check_sla_violations, "interval", minutes=30)

    # Schedule pre-emptive backup check every 15 minutes (ransomware/anomaly detection)
    scheduler.add_job(check_preemptive_backup_triggers, "interval", minutes=15)

    # Schedule M365 audit log ingestion every hour
    scheduler.add_job(ingest_m365_audit_logs, "interval", hours=1)

    # AZ-0: Schedule lifecycle policy reconciler (daily) + durable 5-min
    # sweeper that picks up policies marked lifecycle_dirty=True since the
    # last pass. The 24h pass guards against drift / Azure manual edits;
    # the 5-min pass is the durability backstop for the on-save HTTP nudge.
    scheduler.add_job(reconcile_lifecycle_policies, "interval", hours=24)
    scheduler.add_job(sweep_dirty_lifecycle_policies, "interval", minutes=5)

    # AZ-4: Schedule DR setup reconciler (every 6 hours)
    scheduler.add_job(reconcile_dr_setup, "interval", hours=6)

    # Phase 2: Retention cleanup — FLAT/GFS snapshot pruning per SLA policy (daily)
    scheduler.add_job(run_retention_cleanup, "interval", hours=24)

    # Stale-snapshot sweep — close out snapshots that have been
    # IN_PROGRESS for >30min with no live handler. This happens when a
    # worker is killed mid-run (redeploy, OOM, docker restart) and the
    # snapshot row gets stranded; without a sweeper, the Recovery UI
    # shows a permanently-spinning row and downstream consumers (SLA,
    # dashboard, dr-replication) skip these rows forever.
    #
    # Runs every 5 min. Snapshots with items already persisted get
    # promoted to COMPLETED with the true item_count; snapshots with
    # zero items get FAILED. Mirrors the same COMPLETED-vs-FAILED
    # logic the job-service cancel path uses.
    async def _sweep_stale_snapshots():
        try:
            async with async_session_factory() as session:
                result = await session.execute(text("""
                    WITH counts AS (
                        SELECT s.id,
                               s.started_at,
                               s.resource_id,
                               -- snapshots has no tenant_id column — derive
                               -- it from the joined Resource at retry time
                               -- (the loop below already does session.get).
                               (SELECT count(*) FROM snapshot_items si
                                WHERE si.snapshot_id = s.id) AS n_items,
                               (SELECT MAX(si.created_at) FROM snapshot_items si
                                WHERE si.snapshot_id = s.id) AS last_item_at
                        FROM snapshots s
                        WHERE s.status = 'IN_PROGRESS'
                          -- Liveness check: a snapshot is alive if its
                          -- most-recent snapshot_items row was inserted
                          -- recently. OneDrive / SharePoint / Power BI
                          -- handlers insert one row per file/asset, so a
                          -- live drain keeps this watermark moving even
                          -- when no other Snapshot field changes. Falls
                          -- back to started_at for the not-yet-written-
                          -- anything-case (genuinely orphaned at start).
                          -- 30 min is generous: even an 80 GB file streams
                          -- at ≥45 MB/s on Azure Front Door, so a single
                          -- file in flight without ANY siblings persisting
                          -- in 30 min is genuinely stuck.
                          AND COALESCE(
                                (SELECT MAX(si.created_at) FROM snapshot_items si
                                 WHERE si.snapshot_id = s.id),
                                s.started_at
                              ) < NOW() - INTERVAL '30 minutes'
                          -- Lease guard (2026-07-14): a live worker renews
                          -- lease_expires_at every 120s (LeaseExtender) —
                          -- including during OneDrive *enumeration*, which
                          -- writes no snapshot_items rows and so never
                          -- advances the watermark above. Only sweep rows
                          -- whose lease lapsed (dead worker) or that never
                          -- had one (heartbeat init failed / legacy rows),
                          -- which keep the 30-min horizon.
                          -- 2026-08-25 grace: a lease merely PAST its TTL
                          -- proves only that a few renew WRITES were
                          -- severed — during the 8/24-8/25 pooler storms
                          -- that was true of live walks (the reconciler's
                          -- STEP B gained the same grace; without it here
                          -- this 5-min sweep reaps the live walk first and
                          -- can even derive COMPLETED for a half-done run).
                          AND (s.lease_expires_at IS NULL
                               OR s.lease_expires_at < NOW() - INTERVAL '900 seconds')
                          -- Bug #169 defense: never reap a partitioned
                          -- snapshot whose shards are still in flight.
                          -- The partition stale-sweep below owns the
                          -- per-shard liveness check; this CTE only
                          -- targets snapshots that have no live shards
                          -- left (either non-partitioned runs or
                          -- partitioned runs whose every shard already
                          -- settled). Avoids racing the partition
                          -- finalizer with a stale-bytes flip.
                          AND NOT EXISTS (
                              SELECT 1 FROM snapshot_partitions sp
                               WHERE sp.snapshot_id = s.id
                                 AND sp.status IN (
                                     'QUEUED','CLAIMED','IN_PROGRESS'
                                 )
                          )
                    )
                    UPDATE snapshots s SET
                        -- A partitioned run whose shards finished but LOST
                        -- files records the count in
                        -- snapshot_partitions.failure_state (see
                        -- _mark_partition_terminal — partition status stays
                        -- a two-value terminal domain). Deriving purely from
                        -- item count would flip such a run to COMPLETED when
                        -- this 5-min sweep reaches the row before the 60s
                        -- reconciler does, so the snapshot would pose as a
                        -- complete restore point that retention may promote
                        -- over a genuinely-complete predecessor. Same
                        -- predicate as the other four derivation sites, and
                        -- type-checked so a malformed value can never abort
                        -- the sweep for every other snapshot.
                        status = (CASE
                            WHEN c.n_items = 0 THEN 'FAILED'
                            WHEN EXISTS (
                                SELECT 1 FROM snapshot_partitions sp
                                 WHERE sp.snapshot_id = s.id
                                   AND jsonb_typeof(
                                         sp.failure_state::jsonb -> 'files_failed'
                                       ) = 'number'
                                   AND (sp.failure_state::jsonb ->> 'files_failed')::bigint > 0
                            ) THEN 'PARTIAL'
                            ELSE 'COMPLETED' END)::snapshotstatus,
                        item_count = c.n_items,
                        new_item_count = COALESCE(s.new_item_count, c.n_items),
                        completed_at = NOW(),
                        duration_secs = EXTRACT(EPOCH FROM
                            (NOW() - c.started_at))::int,
                        extra_data = COALESCE(s.extra_data::jsonb, '{}'::jsonb)
                                     || jsonb_build_object(
                                        'stale_sweep', true,
                                        'reason',
                                        'handler orphaned by worker restart')
                    FROM counts c
                    WHERE s.id = c.id
                    RETURNING s.id, s.status::text, s.resource_id
                """))
                rows = result.fetchall()
                await session.commit()
                failed_rows = [r for r in rows if r[1] == "FAILED"]
                if rows:
                    print(
                        f"[backup-scheduler] stale_snapshot_sweep: "
                        f"closed {len(rows)} orphaned IN_PROGRESS rows "
                        f"({len(failed_rows)} FAILED → enqueue retry)",
                    )

            # Resumable: a sweep-FAILED snapshot has zero persisted items —
            # the handler died before writing anything useful. Enqueue a
            # retry message so the operator never has to re-trigger by hand.
            # Sweep-COMPLETED rows already have data, no retry needed.
            if failed_rows and settings.RABBITMQ_ENABLED:
                from shared.message_bus import (
                    message_bus as _mb,
                    create_backup_message as _mk,
                )
                try:
                    from shared.export_routing import pick_backup_queue
                except Exception:
                    pick_backup_queue = None
                async with async_session_factory() as session:
                    for _snap_id, _status, _rid in failed_rows:
                        try:
                            res = await session.get(Resource, _rid)
                            if not res:
                                continue
                            rtype = (
                                res.type.value
                                if hasattr(res.type, "value") else str(res.type)
                            )
                            q = "backup.normal"
                            if pick_backup_queue:
                                try:
                                    q = pick_backup_queue(
                                        resource_type=rtype,
                                        default_queue="backup.normal",
                                    )
                                except Exception:
                                    pass
                            msg = _mk(
                                job_id=str(uuid.uuid4()),
                                resource_id=str(_rid),
                                tenant_id=str(res.tenant_id),
                                full_backup=False,
                            )
                            msg["retry_of_snapshot"] = str(_snap_id)
                            await _mb.publish(q, msg, priority=3)
                            print(
                                f"[backup-scheduler] stale_sweep retry: "
                                f"snapshot={_snap_id} type={rtype} → {q}"
                            )
                        except Exception as inner:
                            print(
                                f"[backup-scheduler] stale_sweep retry failed for "
                                f"snapshot={_snap_id}: {inner}"
                            )

            # Bulk-parent finalizer backstop. Each per-resource handler
            # under the fan-out path opportunistically tries to flip
            # its bulk parent Job terminal via
            # ``_finalize_bulk_parent_if_complete``. If every sibling
            # finalizer happened to be CANCELLING or hit a transient
            # race, the parent can sit RUNNING forever even though all
            # its children settled. This pass catches that.
            #
            # Same CTE as the worker's finalizer, but applied to every
            # RUNNING bulk Job in one shot. Idempotent: the WHERE
            # ``j.status NOT IN (terminal)`` guard makes repeated
            # invocations harmless.
            try:
                async with async_session_factory() as session:
                    flip_rows = (await session.execute(text("""
                        WITH bulk_jobs AS (
                            SELECT id,
                                   COALESCE(array_length(batch_resource_ids, 1), 0) AS n
                            FROM jobs
                            WHERE status NOT IN (
                                'COMPLETED'::jobstatus, 'FAILED'::jobstatus,
                                'CANCELLED'::jobstatus, 'CANCELLING'::jobstatus
                            )
                              AND batch_resource_ids IS NOT NULL
                              AND array_length(batch_resource_ids, 1) > 0
                        ),
                        counts AS (
                            SELECT
                                s.job_id,
                                COUNT(*) FILTER (
                                    WHERE s.status IN (
                                        'COMPLETED','FAILED','PARTIAL'
                                    )
                                ) AS terminal,
                                COUNT(*) FILTER (WHERE s.status='FAILED') AS failed,
                                COUNT(*) FILTER (WHERE s.status='COMPLETED') AS completed,
                                COUNT(*) FILTER (WHERE s.status='PARTIAL') AS partial,
                                COALESCE(SUM(s.item_count) FILTER (
                                    WHERE s.status IN ('COMPLETED','PARTIAL')
                                ), 0) AS items,
                                COALESCE(SUM(s.bytes_added) FILTER (
                                    WHERE s.status IN ('COMPLETED','PARTIAL')
                                ), 0) AS bytes
                            FROM snapshots s
                            JOIN bulk_jobs bj ON s.job_id = bj.id
                            GROUP BY s.job_id
                        )
                        UPDATE jobs j SET
                            -- Terminal-status mapping matches jobstatus
                            -- enum (COMPLETED / FAILED only — no PARTIAL
                            -- at the Job level). Mixed-outcome bulks
                            -- flip to COMPLETED with c.failed in result;
                            -- only an all-failed bulk lands at FAILED.
                            status = (CASE
                                WHEN c.completed = 0 AND c.partial = 0
                                    THEN 'FAILED'::jobstatus
                                ELSE 'COMPLETED'::jobstatus
                            END),
                            completed_at = NOW(),
                            progress_pct = 100,
                            items_processed = c.items,
                            bytes_processed = c.bytes,
                            result = COALESCE(j.result::jsonb, '{}'::jsonb)
                                     || jsonb_build_object(
                                        'finalized_by', 'stale_sweep',
                                        'snapshots_total', bj.n,
                                        'snapshots_completed', c.completed,
                                        'snapshots_failed', c.failed,
                                        'snapshots_partial', c.partial,
                                        'items_processed', c.items,
                                        'bytes_processed', c.bytes
                                     )
                        FROM counts c, bulk_jobs bj
                        WHERE j.id = bj.id
                          AND j.id = c.job_id
                          AND c.terminal >= bj.n
                          AND j.status NOT IN (
                              'COMPLETED'::jobstatus, 'FAILED'::jobstatus,
                              'CANCELLED'::jobstatus, 'CANCELLING'::jobstatus
                          )
                        RETURNING j.id, j.status::text
                    """))).fetchall()
                    await session.commit()
                    if flip_rows:
                        print(
                            f"[backup-scheduler] bulk_parent_finalize: "
                            f"flipped {len(flip_rows)} stuck bulk Jobs "
                            f"({[r[1] for r in flip_rows[:5]]}{'...' if len(flip_rows) > 5 else ''})"
                        )
            except Exception as bulk_exc:
                print(
                    f"[backup-scheduler] bulk_parent_finalize failed: {bulk_exc}",
                )

            # Bug B fix — late-bytes reconcile for recently-terminal
            # bulks. When a partition consumer commits its snapshot's
            # bytes_added AFTER the parent Job has already flipped
            # terminal (the worker's _finalize_bulk_parent_if_complete
            # already refreshes bytes unconditionally on its own call,
            # but if no sibling finalizer fires after the late commit,
            # the parent's bytes_processed can still lag). This sweep
            # re-rolls bytes_processed for bulks that flipped terminal
            # in the last 2h, idempotently — same SUM produces the
            # same value, so repeated passes are harmless. Bounded to
            # 2h to keep the sweep cheap on large history.
            try:
                async with async_session_factory() as session:
                    reconciled = (await session.execute(text("""
                        WITH recent_bulks AS (
                            SELECT id,
                                   COALESCE(array_length(batch_resource_ids, 1), 0) AS n,
                                   bytes_processed AS prev_bytes
                            FROM jobs
                            WHERE status IN (
                                'COMPLETED'::jobstatus, 'FAILED'::jobstatus
                            )
                              AND batch_resource_ids IS NOT NULL
                              AND array_length(batch_resource_ids, 1) > 0
                              AND completed_at >= NOW() - interval '2 hours'
                        ),
                        counts AS (
                            SELECT
                                s.job_id,
                                COUNT(*) FILTER (WHERE s.status='COMPLETED') AS completed,
                                COUNT(*) FILTER (WHERE s.status='FAILED')    AS failed,
                                COUNT(*) FILTER (WHERE s.status='PARTIAL')   AS partial,
                                COALESCE(SUM(s.item_count) FILTER (
                                    WHERE s.status IN ('COMPLETED','PARTIAL')
                                ), 0) AS items,
                                COALESCE(SUM(s.bytes_added) FILTER (
                                    WHERE s.status IN ('COMPLETED','PARTIAL')
                                ), 0) AS bytes
                            FROM snapshots s
                            JOIN recent_bulks rb ON s.job_id = rb.id
                            GROUP BY s.job_id
                        )
                        UPDATE jobs j SET
                            items_processed = c.items,
                            bytes_processed = c.bytes,
                            result = COALESCE(j.result::jsonb, '{}'::jsonb)
                                     || jsonb_build_object(
                                         'reconciled_by', 'late_bytes_sweep',
                                         'reconciled_at', NOW()::text,
                                         'snapshots_total', rb.n,
                                         'snapshots_completed', c.completed,
                                         'snapshots_failed', c.failed,
                                         'snapshots_partial', c.partial,
                                         'items_processed', c.items,
                                         'bytes_processed', c.bytes
                                     )
                        FROM counts c, recent_bulks rb
                        WHERE j.id = rb.id
                          AND j.id = c.job_id
                          AND c.bytes <> rb.prev_bytes
                        RETURNING j.id,
                                  rb.prev_bytes AS old_bytes,
                                  c.bytes      AS new_bytes
                    """))).fetchall()
                    await session.commit()
                    if reconciled:
                        diffs = [
                            f"{r[0]}: {int(r[1] or 0)} → {int(r[2] or 0)}"
                            for r in reconciled[:5]
                        ]
                        print(
                            f"[backup-scheduler] late_bytes_reconcile: "
                            f"corrected bytes_processed on "
                            f"{len(reconciled)} terminal bulks "
                            f"({diffs}{'...' if len(reconciled) > 5 else ''})"
                        )
            except Exception as late_exc:
                print(
                    f"[backup-scheduler] late_bytes_reconcile failed: "
                    f"{late_exc}",
                )

            # Partition stale-sweep — mirrors the snapshot sweep above
            # but for `snapshot_partitions`. A partition consumer that
            # died mid-shard leaves its row IN_PROGRESS; without this
            # sweep the parent Snapshot can never flip terminal (the
            # partition finalizer waits for ALL shards to be terminal).
            # Same 30-min idle horizon as the snapshot sweep so ops
            # semantics are consistent.
            #
            # Resilience hardening (Phase 2.4):
            #  - Increment `retry_count` on every reset.
            #  - When retry_count crosses PARTITION_MAX_RETRIES, mark
            #    the row FAILED instead of re-queuing — caps redelivery
            #    storms on a partition that's fundamentally broken.
            #  - Before re-publishing, check the parent Job status. If
            #    CANCELLING/CANCELLED, mark the row FAILED quietly so
            #    we don't resurrect work the user explicitly stopped.
            try:
                stale_min = int(
                    os.getenv("ONEDRIVE_PARTITION_STALE_SWEEP_MIN", "30")
                )
                max_retries = int(
                    os.getenv("PARTITION_MAX_RETRIES", "5")
                )

                # Step 1: fail any row that's already at retry_count >=
                # PARTITION_MAX_RETRIES and stuck IN_PROGRESS past the
                # horizon. Done first so we don't bump retry_count for
                # rows we're about to terminate.
                async with async_session_factory() as session:
                    failed_rows = (await session.execute(
                        text(
                            """
                            UPDATE snapshot_partitions
                               SET status        = 'FAILED',
                                   completed_at  = NOW(),
                                   failure_state = COALESCE(
                                       failure_state, '{}'::json
                                   )::jsonb || jsonb_build_object(
                                       'reason', 'max_retries_exceeded',
                                       'retry_count', retry_count,
                                       'stale_sweep_at', NOW()
                                   )
                             WHERE status = 'IN_PROGRESS'
                               AND started_at IS NOT NULL
                               AND started_at < NOW()
                                                - (:stale * INTERVAL '1 minute')
                               -- Lease guard (2026-07-14): a shard being
                               -- actively drained has its lease renewed
                               -- every 120s by the owning worker's
                               -- LeaseExtender; started_at age alone says
                               -- nothing about liveness on multi-hour
                               -- downloads. Only touch lapsed/lease-less
                               -- rows.
                               AND (lease_expires_at IS NULL
                                    OR lease_expires_at < NOW())
                               AND retry_count >= :max_r
                         RETURNING id, snapshot_id
                            """
                        ),
                        {"stale": stale_min, "max_r": max_retries},
                    )).fetchall()
                    await session.commit()
                if failed_rows:
                    print(
                        f"[backup-scheduler] partition_sweep: "
                        f"{len(failed_rows)} partitions exceeded "
                        f"max_retries={max_retries} — marked FAILED"
                    )

                # Step 2: reset still-eligible stuck rows back to QUEUED
                # and bump retry_count. Stamp the eligible job_id so we
                # can cancel-guard before re-publish.
                async with async_session_factory() as session:
                    stuck_rows = (await session.execute(
                        text(
                            """
                            UPDATE snapshot_partitions
                               SET status        = 'QUEUED',
                                   worker_id     = NULL,
                                   started_at    = NULL,
                                   retry_count   = retry_count + 1,
                                   failure_state = COALESCE(
                                       failure_state, '{}'::json
                                   )::jsonb || jsonb_build_object(
                                       'stale_sweep', true,
                                       'reset_at', NOW()
                                   )
                             WHERE status = 'IN_PROGRESS'
                               AND started_at IS NOT NULL
                               AND started_at < NOW()
                                                - (:stale * INTERVAL '1 minute')
                               -- Lease guard (2026-07-14): see step 1. This
                               -- reset ran every sweep pass against LIVE
                               -- multi-hour shards, so the sweep itself
                               -- manufactured the retry_count that step 1
                               -- then failed on (jlucas snapshot 370e1af6:
                               -- 4 shards x 5 sweep-resets = FAILED with 0
                               -- real worker deaths).
                               AND (lease_expires_at IS NULL
                                    OR lease_expires_at < NOW())
                               AND retry_count < :max_r
                         RETURNING id, snapshot_id, job_id, tenant_id,
                                   resource_id, drive_id, partition_type,
                                   retry_count, payload
                            """
                        ),
                        {"stale": stale_min, "max_r": max_retries},
                    )).fetchall()
                    await session.commit()
                if stuck_rows:
                    print(
                        f"[backup-scheduler] partition_sweep: reset "
                        f"{len(stuck_rows)} IN_PROGRESS partitions to "
                        f"QUEUED (idle > {stale_min} min)"
                    )

                    # Cancel-guard: jobs in CANCELLING/CANCELLED state
                    # should NOT resurrect partitions. Fetch the set of
                    # cancelled job_ids in one query, then split.
                    job_ids = {str(r[2]) for r in stuck_rows if r[2]}
                    cancelled_jobs: set = set()
                    if job_ids:
                        async with async_session_factory() as session:
                            cancelled_jobs = {
                                str(r[0]) for r in (await session.execute(
                                    text(
                                        """
                                        SELECT id FROM jobs
                                         WHERE id::text = ANY(:ids)
                                           AND status IN (
                                               'CANCELLING'::jobstatus,
                                               'CANCELLED'::jobstatus
                                           )
                                        """
                                    ),
                                    {"ids": list(job_ids)},
                                )).fetchall()
                            }

                    # Mark any reset partition whose parent job was
                    # cancelled as FAILED without re-publishing.
                    cancelled_part_ids = [
                        str(r[0]) for r in stuck_rows
                        if str(r[2]) in cancelled_jobs
                    ]
                    if cancelled_part_ids:
                        async with async_session_factory() as session:
                            await session.execute(
                                text(
                                    """
                                    UPDATE snapshot_partitions
                                       SET status       = 'FAILED',
                                           completed_at = NOW(),
                                           failure_state = COALESCE(
                                               failure_state, '{}'::json
                                           )::jsonb || jsonb_build_object(
                                               'reason', 'job_cancelled_during_sweep'
                                           )
                                     WHERE id::text = ANY(:ids)
                                    """
                                ),
                                {"ids": cancelled_part_ids},
                            )
                            await session.commit()
                        print(
                            f"[backup-scheduler] partition_sweep: "
                            f"{len(cancelled_part_ids)} partitions had "
                            f"cancelled parent job — marked FAILED, "
                            f"skipping re-publish"
                        )

                    # Re-publish only the partitions whose parent job
                    # is still alive. Partition_type drives the queue:
                    # ONEDRIVE_FILES → backup.onedrive_partition,
                    # CHATS → backup.chats_partition (Phase 2.3),
                    # MAIL_FOLDERS → backup.mail_partition (Phase 3.2),
                    # SHAREPOINT_DRIVES → backup.sharepoint_partition
                    # (Phase 3.3 — Tier-1).
                    if settings.RABBITMQ_ENABLED:
                        from shared.message_bus import (
                            message_bus as _mb,
                            create_onedrive_partition_message as _mk_od,
                            create_chats_partition_message as _mk_chats,
                            create_mail_partition_message as _mk_mail,
                            create_sharepoint_partition_message as _mk_sp,
                        )
                        import json as _json_re
                        for row_re in stuck_rows:
                            (_pid, _sid, _jid, _tid, _rid, _drv,
                             _ptype, _rcnt, _payload) = row_re
                            if str(_jid) in cancelled_jobs:
                                continue
                            ptype = str(_ptype or "ONEDRIVE_FILES")
                            # Payload arrives as dict (JSON column) on
                            # asyncpg; defensively handle JSON-string
                            # fallback for older driver paths.
                            payload_obj = _payload or {}
                            if isinstance(payload_obj, str):
                                try:
                                    payload_obj = _json_re.loads(payload_obj)
                                except Exception:
                                    payload_obj = {}
                            try:
                                if ptype == "ONEDRIVE_FILES":
                                    msg = _mk_od(
                                        partition_id=str(_pid),
                                        snapshot_id=str(_sid),
                                        job_id=str(_jid),
                                        tenant_id=str(_tid),
                                        resource_id=str(_rid),
                                        drive_id=str(_drv) if _drv else "",
                                    )
                                    await _mb.publish(
                                        "backup.onedrive_partition",
                                        msg, priority=3,
                                    )
                                elif ptype == "CHATS":
                                    msg = _mk_chats(
                                        partition_id=str(_pid),
                                        snapshot_id=str(_sid),
                                        job_id=str(_jid),
                                        tenant_id=str(_tid),
                                        resource_id=str(_rid),
                                        chat_ids=list(
                                            payload_obj.get("chat_ids") or []
                                        ),
                                    )
                                    await _mb.publish(
                                        "backup.chats_partition",
                                        msg, priority=3,
                                    )
                                elif ptype == "MAIL_FOLDERS":
                                    msg = _mk_mail(
                                        partition_id=str(_pid),
                                        snapshot_id=str(_sid),
                                        job_id=str(_jid),
                                        tenant_id=str(_tid),
                                        resource_id=str(_rid),
                                        folder_ids=list(
                                            payload_obj.get("folder_ids") or []
                                        ),
                                        resource_type=str(
                                            payload_obj.get("resource_type")
                                            or "USER_MAIL"
                                        ),
                                    )
                                    await _mb.publish(
                                        "backup.mail_partition",
                                        msg, priority=3,
                                    )
                                elif ptype == "SHAREPOINT_DRIVES":
                                    msg = _mk_sp(
                                        partition_id=str(_pid),
                                        snapshot_id=str(_sid),
                                        job_id=str(_jid),
                                        tenant_id=str(_tid),
                                        resource_id=str(_rid),
                                        drive_ids=list(
                                            payload_obj.get("drive_ids") or []
                                        ),
                                        site_id=str(
                                            payload_obj.get("site_id") or ""
                                        ),
                                    )
                                    await _mb.publish(
                                        "backup.sharepoint_partition",
                                        msg, priority=3,
                                    )
                                else:
                                    # Unknown partition_type — leave
                                    # QUEUED with the bumped retry_count;
                                    # PARTITION_MAX_RETRIES eventually
                                    # DLQs it.
                                    continue
                            except Exception as pub_exc:
                                print(
                                    f"[backup-scheduler] partition_sweep "
                                    f"re-publish failed (partition={_pid}, "
                                    f"type={ptype}): {pub_exc}"
                                )
            except Exception as part_exc:
                print(
                    f"[backup-scheduler] partition stale sweep failed: "
                    f"{part_exc}"
                )
        except Exception as exc:
            print(f"[backup-scheduler] stale_snapshot_sweep failed: {exc}")

    scheduler.add_job(
        _sweep_stale_snapshots, "interval", minutes=5,
    )

    # Cancelled-snapshot sweep — paired with the job-service cancel
    # endpoint's atomic-flip refactor.
    #
    # Cancel used to do destructive deletes inline (DELETE FROM
    # snapshot_items + DELETE FROM snapshots) which raced with concurrent
    # backup_worker INSERTs and threw two real prod errors:
    #   * ForeignKeyViolationError on snapshot_items_snapshot_id_fkey
    #     (worker inserted a row between cancel's two deletes)
    #   * DeadlockDetectedError (two concurrent cancels colliding)
    # The cancel endpoint now atomically flips status to FAILED and
    # writes ``extra_data.cancelled_at`` as a marker. This sweep owns
    # the destructive teardown — runs at 30s cadence so the UI stops
    # showing "In Progress 16%" within one polling cycle and the
    # backing storage actually gets freed.
    #
    # Catches three populations:
    #   A. Cancel-flipped snapshots — status=FAILED with cancelled_at.
    #      Cleanest population; cancel endpoint already wrote the marker.
    #   B. Late-arrival orphans — IN_PROGRESS snapshots whose owning
    #      job.status IN ('CANCELLED','FAILED'). Worker created the
    #      snapshot AFTER cancel ran but before _is_job_cancelled fired.
    #      No cancelled_at marker (worker doesn't write one), so we key
    #      on the join.
    #   C. Worker-self-flipped — IN_PROGRESS snapshots with cancelled_at.
    #      Belt-and-braces with backup-worker's JobCancelledMidFlight
    #      handler (see workers/backup-worker/main.py).
    async def _sweep_cancelled_snapshots() -> None:
        from shared.storage.router import router as _storage_router
        from shared.blob_gc import blobs_safe_to_delete, container_candidates

        try:
            async with async_session_factory() as session:
                # Find candidates first — keep the destructive work
                # OUT of a long transaction so one slow blob delete
                # doesn't block the next batch.
                # NOTE: snapshots.extra_data is plain JSON (not JSONB) per the
                # model definition (shared/models.py:216). The `?` key-exists
                # operator is JSONB-only — using it on a JSON column raises
                # ``operator does not exist: json ? unknown``. We use
                # ``(extra_data::jsonb ->> 'cancelled_at') IS NOT NULL``
                # instead which works regardless of the underlying type
                # (the cast is a no-op when the column is already jsonb).
                candidates = (await session.execute(text("""
                    SELECT s.id AS sid, s.resource_id, s.started_at,
                           s.extra_data,
                           r.type::text   AS resource_type,
                           r.tenant_id::text AS tenant_id,
                           j.status::text   AS job_status,
                           (s.extra_data::jsonb ->> 'cancelled_at') AS cancelled_at
                      FROM snapshots s
                      LEFT JOIN resources r ON r.id = s.resource_id
                      LEFT JOIN jobs j      ON j.id = s.job_id
                     WHERE (
                          (
                            -- Population A + C: explicit cancellation marker.
                            (s.extra_data::jsonb ->> 'cancelled_at') IS NOT NULL
                          ) OR (
                            -- Population B: late-arrival orphans.
                            s.status = 'IN_PROGRESS'
                            AND j.status IN ('CANCELLED', 'FAILED')
                          ) OR (
                            -- Population D: teardown resumption — a prior
                            -- sweep pass claimed this row (see the Step-0
                            -- claim below) and crashed before finishing.
                            s.status = 'PENDING_DELETION'
                          )
                       )
                       -- NEVER hard-delete under a live lease. A renewing
                       -- lease means a worker is actively walking this
                       -- snapshot; deleting the row out from under it made
                       -- the worker walk blind for hours and die on the
                       -- first item INSERT (FK violation), then resurrect
                       -- the row via fail_snapshot's merge — the archive
                       -- run-#8 chain (job flipped FAILED by an earlier
                       -- attempt's error path, RMQ redelivery created new
                       -- IN_PROGRESS snapshots under the FAILED job, and
                       -- this sweep deleted each within 30s of creation,
                       -- 2026-07-31). Lease-dead rows still clean up
                       -- within one lease TTL (~10 min), including
                       -- operator cancels — a deferred teardown beats a
                       -- corrupted live run.
                       AND (s.lease_expires_at IS NULL OR s.lease_expires_at < NOW())
                       -- Defense against partitioned-shard double-cleanup.
                       AND NOT EXISTS (
                           SELECT 1 FROM snapshot_partitions sp
                            WHERE sp.snapshot_id = s.id
                              AND sp.status IN ('QUEUED','CLAIMED','IN_PROGRESS')
                       )
                     LIMIT 200
                """))).all()

            if not candidates:
                return

            cleaned = 0
            blob_errors = 0
            blobs_deleted = 0
            for row in candidates:
                sid = row.sid
                # Target the SAME container the bytes were written to. Blobs are
                # written under the workload suffix (ONEDRIVE -> "files" ->
                # backup-files-<t>), NOT the raw resource_type — deriving the
                # container from resource_type ("backup-onedrive-<t>") targets an
                # empty bucket and silently orphans the blob. Primary candidate
                # is the correct workload container; legacy split-container rows
                # fall through to the remaining candidates.
                containers = container_candidates(row.resource_type, row.tenant_id)

                # Step 0 — ATOMIC CLAIM before any destruction. The
                # candidate SELECT's lease check is stale by the time we
                # get here (blob deletion alone can take minutes, and a
                # redelivered run re-claims a dead lease / the archive
                # liveness guard re-arms it every ~2 min). Destroying
                # blobs+items first and only lease-checking the final
                # row DELETE would GUT a surviving run: row stays, its
                # items and blobs are gone, and the run finalizes
                # COMPLETED with silent data loss. Re-verify candidacy
                # + dead lease and flip to PENDING_DELETION (the
                # established "this row is being torn down" status —
                # retention treats it as delete-me, the archive
                # liveness guard sees non-IN_PROGRESS and aborts, and
                # Population D resumes the teardown if we crash here).
                # rowcount 0 → someone re-leased or changed state:
                # skip, next pass reconsiders.
                try:
                    async with async_session_factory() as ds:
                        claim = await ds.execute(
                            text(
                                "UPDATE snapshots s "
                                "   SET status = 'PENDING_DELETION', "
                                "       lease_owner_id = NULL, "
                                "       lease_expires_at = NULL, "
                                "       lease_token = s.lease_token + 1 "
                                "  FROM jobs j "
                                " WHERE s.id = :sid "
                                "   AND s.job_id = j.id "
                                "   AND (s.lease_expires_at IS NULL OR s.lease_expires_at < NOW()) "
                                "   AND ( (s.extra_data::jsonb ->> 'cancelled_at') IS NOT NULL "
                                "         OR (s.status IN ('IN_PROGRESS','FAILED') "
                                "             AND j.status IN ('CANCELLED','FAILED')) "
                                "         OR s.status = 'PENDING_DELETION' )"
                            ),
                            {"sid": sid},
                        )
                        await ds.commit()
                    if not (claim.rowcount and claim.rowcount > 0):
                        continue
                except Exception as claim_exc:
                    print(
                        f"[backup-scheduler] cancelled_sweep claim failed "
                        f"sid={sid}: {claim_exc}"
                    )
                    continue

                # Step 1 — delete blobs. Done in its own short
                # transaction-per-snapshot so any one snapshot stuck on
                # a slow backend doesn't pin the whole sweep.
                try:
                    async with async_session_factory() as ds:
                        items = (await ds.execute(
                            text(
                                "SELECT blob_path, backend_id "
                                "  FROM snapshot_items "
                                " WHERE snapshot_id = :sid "
                                "   AND blob_path IS NOT NULL "
                                "   AND backend_id IS NOT NULL"
                            ),
                            {"sid": sid},
                        )).all()

                    # Ref-counted GC: a blob_path may be shared (content-addressed
                    # / carried-forward) with a SURVIVING snapshot. Physically
                    # deleting a shared blob would corrupt that live snapshot, so
                    # only delete blob_paths no other snapshot still references.
                    doomed_paths = [bp for bp, _ in items]
                    backend_by_path = {bp: bid for bp, bid in items}
                    safe_paths: set = set()
                    if doomed_paths:
                        async with async_session_factory() as ds:
                            ref_rows = (await ds.execute(
                                text(
                                    "SELECT DISTINCT blob_path "
                                    "  FROM snapshot_items "
                                    " WHERE blob_path = ANY(CAST(:paths AS TEXT[])) "
                                    "   AND snapshot_id != CAST(:sid AS UUID)"
                                ),
                                {"paths": doomed_paths, "sid": str(sid)},
                            )).all()
                        still_referenced = {r[0] for r in ref_rows}
                        safe_paths = blobs_safe_to_delete(doomed_paths, still_referenced)
                        shared_kept = len(set(doomed_paths)) - len(safe_paths)
                        if shared_kept > 0:
                            print(
                                f"[backup-scheduler] cancelled_sweep "
                                f"ref-count protected {shared_kept} shared blob(s) "
                                f"sid={sid}"
                            )

                    for blob_path in safe_paths:
                        backend_id = backend_by_path.get(blob_path)
                        deleted_ok = False
                        last_err = None
                        # Delete is idempotent (S3/SeaweedFS 404 == success), so
                        # attempting each candidate container is safe: the wrong
                        # ones no-op, the right one frees the blob.
                        for container in containers:
                            try:
                                store = _storage_router.get_store_by_id(str(backend_id))
                                await store.delete(container, blob_path)
                                deleted_ok = True
                                break
                            except Exception as bx:
                                last_err = bx
                        if deleted_ok:
                            blobs_deleted += 1
                        else:
                            blob_errors += 1
                            print(
                                f"[backup-scheduler] cancelled_sweep "
                                f"blob delete failed sid={sid} "
                                f"path={blob_path}: {last_err}"
                            )

                    # Step 2 — DB cleanup (items first, then snapshot).
                    # The Step-0 claim already re-verified candidacy +
                    # dead lease and took ownership via
                    # PENDING_DELETION, so both deletes key on that
                    # claimed status — nothing here can gut a live run.
                    # Belt on top of the Step-0 claim: also require the
                    # lease to still be dead at delete time. Every lease
                    # writer is fenced on status='IN_PROGRESS' so nothing
                    # should re-arm a PENDING_DELETION row — this guards
                    # the invariant against future unfenced writers.
                    async with async_session_factory() as ds:
                        await ds.execute(
                            text(
                                "DELETE FROM snapshot_items si "
                                " USING snapshots s "
                                " WHERE si.snapshot_id = :sid "
                                "   AND s.id = si.snapshot_id "
                                "   AND s.status = 'PENDING_DELETION' "
                                "   AND (s.lease_expires_at IS NULL OR s.lease_expires_at < NOW())"
                            ),
                            {"sid": sid},
                        )
                        res = await ds.execute(
                            text(
                                "DELETE FROM snapshots s "
                                " WHERE s.id = :sid "
                                "   AND s.status = 'PENDING_DELETION' "
                                "   AND (s.lease_expires_at IS NULL OR s.lease_expires_at < NOW())"
                            ),
                            {"sid": sid},
                        )
                        await ds.commit()
                        if res.rowcount and res.rowcount > 0:
                            cleaned += 1
                except Exception as cleanup_exc:
                    print(
                        f"[backup-scheduler] cancelled_sweep "
                        f"snapshot cleanup failed sid={sid}: {cleanup_exc}"
                    )

            if cleaned or blobs_deleted or blob_errors:
                print(
                    f"[backup-scheduler] cancelled_sweep: "
                    f"reaped {cleaned} snapshot(s), "
                    f"deleted {blobs_deleted} blob(s), "
                    f"{blob_errors} blob error(s)"
                )
        except Exception as exc:
            print(f"[backup-scheduler] cancelled_sweep failed: {exc}")

    scheduler.add_job(
        _sweep_cancelled_snapshots, "interval", seconds=30,
    )

    # Job outbox reconciler — fixes the classic "row committed but
    # publish failed" dual-write bug. job-service writes the Job row and
    # then publishes a RabbitMQ message in separate steps; if the
    # service crashes, restarts, or the broker hiccups between those
    # two operations, the row sits in QUEUED forever with no message
    # ever delivered to a worker.
    #
    # This sweeper finds any BACKUP job that's been QUEUED for >3min
    # and has no in-flight snapshots (i.e. no worker has ever started
    # on it), and re-publishes the message using the same helper the
    # original submit path uses. Idempotent: if the original message
    # is still in flight, the handler will just no-op on the first
    # one that wins the race (or create one batch of duplicates in
    # the worst case, which the idempotent-snapshot resume handles).
    #
    # Also catches jobs that were created while RabbitMQ was down
    # (brief outage) — they come back automatically once RMQ is up.
    async def _reconcile_stuck_queued_jobs():
        try:
            from shared.message_bus import (
                message_bus, create_mass_backup_message,
            )
            from shared.models import Job, Snapshot, JobStatus
            await message_bus.connect()
            async with async_session_factory() as session:
                stuck = await session.execute(select(Job).where(
                    Job.status == JobStatus.QUEUED,
                    Job.type == JobType.BACKUP,
                    Job.created_at < datetime.utcnow() - timedelta(minutes=3),
                ))
                stuck_jobs = stuck.scalars().all()
                published = 0
                for job in stuck_jobs:
                    # Skip if a worker did start on it (handler may be
                    # mid-initialization before first ack).
                    snap_exists = await session.execute(
                        select(func.count(Snapshot.id))
                        .where(Snapshot.job_id == job.id),
                    )
                    if (snap_exists.scalar() or 0) > 0:
                        continue
                    # Bug #159 fix: also skip if the bulk-fanout coordinator
                    # has already INSERTed bulk_fanout_seen rows for this
                    # job — that means the per-resource messages have been
                    # published and snapshots will appear shortly. Without
                    # this guard, the 3-min sweep races the fanout's snap-
                    # shot-creation window and produces redundant publishes
                    # (eaten by bulk_fanout_seen dedup downstream, but still
                    # wasteful and confusing in logs).
                    fanout_exists = await session.execute(
                        text(
                            "SELECT COUNT(*) FROM bulk_fanout_seen "
                            "WHERE job_id = CAST(:jid AS uuid)"
                        ),
                        {"jid": str(job.id)},
                    )
                    if (fanout_exists.scalar() or 0) > 0:
                        continue
                    resource_ids = [
                        str(r) for r in (job.batch_resource_ids or [])
                    ]
                    if not resource_ids:
                        # Single-resource shape (manual trigger, AUTO_RETRY):
                        # a publish lost between the Job commit and the AMQP
                        # send left this row QUEUED forever — no other
                        # sweeper covers zero-snapshot single-resource jobs
                        # (2026-08-18 review finding; the :1751 comment
                        # documents the same gap for manual triggers).
                        # Re-send from the row, bounded by jobs.attempts.
                        if job.resource_id is None:
                            continue
                        attempts = int(job.attempts or 0)
                        if attempts >= 3:
                            job.status = JobStatus.FAILED
                            job.completed_at = datetime.utcnow()
                            job.error_message = (
                                "outbox reconcile: message never consumed "
                                "after 3 republish attempts"
                            )
                            await session.commit()
                            print(f"[backup-scheduler] outbox_reconcile: "
                                  f"single-resource job {job.id} FAILED — "
                                  f"republish budget exhausted")
                            continue
                        job.attempts = attempts + 1
                        await session.commit()
                        spec = job.spec or {}
                        try:
                            await message_bus.publish(
                                "backup.urgent",
                                {
                                    "jobId": str(job.id),
                                    "resourceId": str(job.resource_id),
                                    "tenantId": str(job.tenant_id),
                                    "type": "FULL" if spec.get("full_backup") else "INCREMENTAL",
                                    "priority": job.priority or 1,
                                    "triggeredBy": spec.get("triggered_by", "MANUAL"),
                                    "snapshotLabel": spec.get("snapshot_label", "manual"),
                                    "forceFullBackup": bool(spec.get("full_backup", False)),
                                },
                                priority=job.priority or 1,
                            )
                            published += 1
                            print(f"[backup-scheduler] outbox_reconcile: "
                                  f"republished single-resource job {job.id} "
                                  f"(attempt {attempts + 1}/3)")
                        except Exception as _spe:
                            print(f"[backup-scheduler] outbox_reconcile: "
                                  f"single-resource republish failed for "
                                  f"{job.id}: {_spe}")
                        continue
                    queue = (job.spec or {}).get("queue", "backup.urgent")
                    msg = create_mass_backup_message(
                        job_id=str(job.id),
                        tenant_id=str(job.tenant_id),
                        resource_type="BATCH",
                        resource_ids=resource_ids,
                        sla_policy_id=None,
                        full_backup=bool(
                            (job.spec or {}).get("fullBackup", False),
                        ),
                    )
                    try:
                        await message_bus.publish(
                            queue, msg, priority=job.priority or 1,
                        )
                        published += 1
                        print(
                            f"[backup-scheduler] outbox_reconcile: "
                            f"republished QUEUED job {str(job.id)[:8]} "
                            f"→ {queue} ({len(resource_ids)} resources)",
                        )
                    except Exception as pub_err:
                        print(
                            f"[backup-scheduler] outbox_reconcile: "
                            f"republish failed for {str(job.id)[:8]}: "
                            f"{pub_err}",
                        )
                if published:
                    print(
                        f"[backup-scheduler] outbox_reconcile: "
                        f"republished {published} stuck jobs",
                    )
        except Exception as exc:
            print(f"[backup-scheduler] outbox_reconcile failed: {exc}")

    scheduler.add_job(
        _reconcile_stuck_queued_jobs, "interval", minutes=3,
    )

    # Tier-2 backstop sweep — catches users whose SLA was assigned before
    # the SLA-hook shipped, or whose Tier-2 children got soft-deleted, or
    # whose discovery message lost a race. Idempotent — the helper skips
    # users that already have all 5 USER_* rows.
    #
    # Interval matches the production backup cadence (3×/day at 8h
    # intervals) so the sweep runs once between backup rounds, ensuring
    # every scheduled backup finds discovery already done. Env-tunable
    # for tenants on different cadences.
    _TIER2_BACKSTOP_S = int(os.getenv("TIER2_DISCOVERY_BACKSTOP_S", str(7 * 3600)))

    async def _sweep_tier2_gaps():
        try:
            from shared.tier2_discovery import chunk_user_ids, find_users_missing_tier2
            from shared.message_bus import message_bus as _mb
            async with async_session_factory() as session:
                missing = await find_users_missing_tier2(session, require_sla=True)
            if not missing:
                return
            await _mb.connect()
            # Group by tenant — discovery-worker builds one GraphClient per
            # tenant per message, so tenant batching is materially cheaper.
            by_tenant: dict[str, list[str]] = {}
            for u in missing:
                by_tenant.setdefault(str(u.tenant_id), []).append(str(u.id))
            for tid, user_ids in by_tenant.items():
                for chunk in chunk_user_ids(user_ids, settings.TIER2_DISCOVERY_MESSAGE_CHUNK_SIZE):
                    await _mb.publish(
                        "discovery.tier2",
                        {
                            "tenantId": tid,
                            "userResourceIds": chunk,
                            "source": "SCHEDULER_BACKSTOP",
                            "thenBackup": False,
                        },
                        priority=4,
                    )
            print(
                f"[backup-scheduler] tier2_backstop: enqueued discovery for "
                f"{len(missing)} user(s) across {len(by_tenant)} tenant(s)",
            )
        except Exception as exc:
            print(f"[backup-scheduler] tier2_backstop failed: {exc}")

    scheduler.add_job(
        _sweep_tier2_gaps, "interval", seconds=_TIER2_BACKSTOP_S,
    )

    # batch_pending_users watchdog — flip stuck WAITING_DISCOVERY rows
    # to DISCOVERY_FAILED once their deadline_at expires. Expected
    # path is the discovery-worker writing BACKUP_ENQUEUED / NO_CONTENT /
    # DISCOVERY_FAILED inline as discovery completes; this sweeper
    # covers the case where the worker never processed the message
    # (down, queue stuck, publish failed). Without it, a batch could
    # pin IN_PROGRESS forever on a WAITING_DISCOVERY row that no one
    # is going to touch. WHERE state='WAITING_DISCOVERY' makes the
    # UPDATE race-commute with the discovery-worker write — whichever
    # runs first wins, the other is a no-op.
    # See docs/superpowers/specs/2026-05-15-backup-batch-race-fix-design.md.
    async def _sweep_pending_user_deadlines():
        try:
            async with async_session_factory() as session:
                result = await session.execute(text("""
                    UPDATE batch_pending_users
                       SET state = 'DISCOVERY_FAILED', updated_at = NOW()
                     WHERE state = 'WAITING_DISCOVERY'
                       AND deadline_at < NOW()
                """))
                await session.commit()
                if result.rowcount and result.rowcount > 0:
                    print(
                        f"[batch-watchdog] flipped {result.rowcount} rows "
                        f"from WAITING_DISCOVERY to DISCOVERY_FAILED "
                        f"(deadline expired)"
                    )
        except Exception as exc:
            print(f"[batch-watchdog] sweeper failed (non-fatal): {exc}")

    scheduler.add_job(
        _sweep_pending_user_deadlines, "interval", seconds=60,
        id="batch_pending_user_watchdog",
        replace_existing=True,
        max_instances=1,
    )

    # backup_batches finalizer sweep — gated by
    # BATCH_ROW_REDESIGN_ENABLED. Re-runs the strict 4-condition gate
    # for every IN_PROGRESS batch (catches lost finalize events), and
    # flips long-stalled rows to PARTIAL / FAILED so operators see a
    # terminal status rather than a row stuck "In Progress" forever.
    async def _sweep_inflight_batches():
        try:
            from shared.config import settings as _s
            if not _s.BATCH_ROW_REDESIGN_ENABLED:
                return
            from shared.batch_rollup import _finalize_batch_if_complete
            async with async_session_factory() as session:
                rows = (await session.execute(text("""
                    SELECT id FROM backup_batches
                     WHERE status = 'IN_PROGRESS'
                       AND created_at > NOW() - make_interval(hours => :hh)
                     ORDER BY created_at ASC
                     LIMIT 500
                """), {"hh": _s.BATCH_STALL_TIMEOUT_HOURS})).all()
                for r in rows:
                    try:
                        await _finalize_batch_if_complete(r.id, session)
                    except Exception as fex:
                        print(
                            f"[backup-scheduler] batch re-finalize {r.id} "
                            f"failed: {fex}"
                        )

                stalled = (await session.execute(text("""
                    SELECT b.id,
                           EXISTS(
                               SELECT 1 FROM snapshots s
                                 JOIN jobs j ON j.id = s.job_id
                                WHERE COALESCE(j.spec::jsonb->>'batch_id','') = b.id::text
                                  AND s.created_at > b.created_at
                           ) AS has_any_snapshot
                      FROM backup_batches b
                     WHERE b.status = 'IN_PROGRESS'
                       AND b.created_at < NOW() - make_interval(hours => :hh)
                     LIMIT 500
                """), {"hh": _s.BATCH_STALL_TIMEOUT_HOURS})).all()
                for r in stalled:
                    new_status = "PARTIAL" if r.has_any_snapshot else "FAILED"
                    await session.execute(text("""
                        UPDATE backup_batches
                           SET status = :ns, completed_at = NOW()
                         WHERE id = cast(:bid AS uuid)
                           AND status = 'IN_PROGRESS'
                    """), {"ns": new_status, "bid": str(r.id)})
                    await session.commit()
                    print(
                        f"[backup-scheduler] stalled batch {r.id} → "
                        f"{new_status} (no progress in "
                        f"{_s.BATCH_STALL_TIMEOUT_HOURS}h)"
                    )
        except Exception as exc:
            print(f"[backup-scheduler] _sweep_inflight_batches failed: {exc}")

    scheduler.add_job(_sweep_inflight_batches, "interval", minutes=5)

    # Stuck-RUNNING job reaper — fixes the "all snapshots COMPLETED
    # but job stays RUNNING forever" failure mode.
    #
    # The worker's `_process_mass_backup` flow is: kick off per-group
    # tasks → asyncio.gather → update_job_status(COMPLETED). If the
    # worker dies between "all snapshots got written to DB" and "job
    # row flips to COMPLETED", the job stays at RUNNING / partial-pct
    # forever. RabbitMQ may redeliver the message, but the existing
    # handler unconditionally sets status=RUNNING + progress_pct=5
    # and starts the whole job over — wasting all the work the
    # snapshots already capture.
    #
    # Triggers when (1) job has been RUNNING with no updates for
    # >5 min AND (2) every resource in batch_resource_ids has a
    # terminal-status snapshot for THIS job. Idempotent: the WHERE
    # clause filters out already-COMPLETED jobs, so re-runs are
    # safe.
    async def _reap_stuck_running_jobs():
        try:
            async with async_session_factory() as session:
                result = await session.execute(text("""
                    WITH terminal_per_job AS (
                        SELECT j.id AS job_id,
                               cardinality(j.batch_resource_ids) AS n_resources,
                               (SELECT count(*) FROM snapshots s
                                WHERE s.job_id = j.id
                                  AND s.status IN ('COMPLETED','FAILED','PARTIAL')) AS n_terminal,
                               (SELECT count(*) FROM snapshots s
                                WHERE s.job_id = j.id
                                  AND s.status = 'COMPLETED') AS n_completed,
                               (SELECT COALESCE(SUM(s.item_count), 0) FROM snapshots s
                                WHERE s.job_id = j.id) AS items_total,
                               (SELECT COALESCE(SUM(s.bytes_total), 0) FROM snapshots s
                                WHERE s.job_id = j.id) AS bytes_total
                        FROM jobs j
                        WHERE j.status = 'RUNNING'
                          AND j.type = 'BACKUP'
                          AND j.updated_at < NOW() - INTERVAL '5 minutes'
                          AND cardinality(j.batch_resource_ids) > 0
                    )
                    UPDATE jobs j SET
                        status = 'COMPLETED'::jobstatus,
                        progress_pct = 100,
                        completed_at = NOW(),
                        updated_at = NOW(),
                        result = COALESCE(j.result::jsonb, '{}'::jsonb)
                                 || jsonb_build_object(
                                    'reaped_by', 'stuck_running_reaper',
                                    'total', t.n_resources,
                                    'completed', t.n_completed,
                                    'failed', t.n_resources - t.n_completed,
                                    'item_count', t.items_total,
                                    'bytes_added', t.bytes_total
                                 )
                    FROM terminal_per_job t
                    WHERE j.id = t.job_id
                      AND t.n_terminal >= t.n_resources
                    RETURNING j.id
                """))
                rows = result.fetchall()
                await session.commit()
                if rows:
                    print(
                        f"[backup-scheduler] stuck_running_reaper: "
                        f"finalized {len(rows)} jobs whose snapshots "
                        f"all completed but worker died before the "
                        f"job-level update"
                    )
        except Exception as exc:
            print(f"[backup-scheduler] stuck_running_reaper failed: {exc}")

    scheduler.add_job(
        _reap_stuck_running_jobs, "interval", minutes=2,
    )

    # Missing-snapshot zombie reaper — third phantom-"In Progress"
    # class (observed 2026-07-22, job 572bb0b6). The failed-snapshot
    # retry sweep can hard-DELETE a job's snapshot rows (reap-vs-live-
    # consumer race, 7/20 ITOPS SP incident); the reaper above counts
    # EXISTING terminal rows so a job with deleted snapshots never
    # satisfies n_terminal >= n_resources and stays RUNNING forever.
    # Finalize only when the job is provably dead: lease expired,
    # updated_at stale >= 1 hour (early enumeration legitimately has
    # zero snapshot rows; live walks renew the job lease via
    # LeaseExtender), nothing non-terminal under it, no in-flight
    # shard, and rows actually MISSING (the all-rows-settled case
    # belongs to _reap_stuck_running_jobs).
    async def _reap_missing_snapshot_jobs():
        try:
            async with async_session_factory() as session:
                result = await session.execute(text("""
                    WITH t AS (
                        SELECT j.id AS job_id,
                               cardinality(j.batch_resource_ids) AS n_resources,
                               (SELECT count(*) FROM snapshots s
                                WHERE s.job_id = j.id) AS n_existing,
                               (SELECT count(*) FROM snapshots s
                                WHERE s.job_id = j.id
                                  AND s.status NOT IN
                                      ('COMPLETED','FAILED','PARTIAL',
                                       'PENDING_DELETION')) AS n_nonterminal,
                               (SELECT count(*) FROM snapshots s
                                WHERE s.job_id = j.id
                                  AND s.status = 'COMPLETED') AS n_completed,
                               (SELECT COALESCE(SUM(s.item_count), 0)
                                FROM snapshots s
                                WHERE s.job_id = j.id) AS items_total,
                               (SELECT COALESCE(SUM(s.bytes_total), 0)
                                FROM snapshots s
                                WHERE s.job_id = j.id) AS bytes_total
                        FROM jobs j
                        WHERE j.status = 'RUNNING'
                          AND j.type = 'BACKUP'
                          AND cardinality(j.batch_resource_ids) > 0
                          AND j.updated_at < NOW() - INTERVAL '1 hour'
                          AND (j.lease_expires_at IS NULL OR j.lease_expires_at < NOW())
                    )
                    UPDATE jobs j SET
                        status = (CASE WHEN t.n_completed > 0
                                       THEN 'COMPLETED' ELSE 'FAILED'
                                  END)::jobstatus,
                        progress_pct = 100,
                        completed_at = NOW(),
                        updated_at = NOW(),
                        -- 2026-08-25: the old text asserted "deleted
                        -- after dispatch" but absence-of-row cannot
                        -- distinguish deletion from a dispatch message
                        -- that dead-lettered before any worker touched
                        -- it (batch b552bb16: per-resource message
                        -- poisoned to backup.urgent.dlq during a
                        -- conn-kill storm — nothing was ever deleted).
                        -- Say what is actually known, and expose WHICH
                        -- resources are uncovered so they can be
                        -- re-driven instead of silently skipped.
                        error_message = COALESCE(
                            j.error_message,
                            (t.n_resources - t.n_existing)::text ||
                            ' of ' || t.n_resources::text ||
                            ' resource(s) have no snapshot row — dispatch '
                            'message lost/dead-lettered before any worker '
                            'ran, or rows deleted after dispatch; '
                            'finalized by missing_snapshot_reaper'),
                        result = COALESCE(j.result::jsonb, '{}'::jsonb)
                                 || jsonb_build_object(
                                    'reaped_by', 'missing_snapshot_reaper',
                                    'total', t.n_resources,
                                    'completed', t.n_completed,
                                    'failed', t.n_resources - t.n_completed,
                                    'missing', t.n_resources - t.n_existing,
                                    -- Cap at 50 (a wholesale dead-lettered
                                    -- 5k-resource batch must not bloat the
                                    -- result row); exclude resources with a
                                    -- snapshot under a SIBLING job since
                                    -- dispatch — G2 dedup skips resources
                                    -- already in flight elsewhere, and those
                                    -- are covered, not missing.
                                    'missing_resource_ids', (
                                        SELECT COALESCE(jsonb_agg(rid::text)
                                                    FILTER (WHERE ord <= 50),
                                                '[]'::jsonb)
                                          FROM (
                                              SELECT rid,
                                                     row_number() OVER (ORDER BY rid) AS ord
                                                FROM unnest(j.batch_resource_ids) AS rid
                                               WHERE NOT EXISTS (
                                                   SELECT 1 FROM snapshots s
                                                    WHERE s.resource_id = rid
                                                      AND (s.job_id = j.id
                                                           OR s.created_at >= j.created_at))
                                          ) v
                                    ),
                                    'item_count', t.items_total,
                                    'bytes_added', t.bytes_total
                                 )
                    FROM t
                    WHERE j.id = t.job_id
                      AND t.n_nonterminal = 0
                      AND t.n_existing < t.n_resources
                      AND NOT EXISTS (
                          SELECT 1 FROM snapshot_partitions sp
                           WHERE sp.job_id = j.id
                             AND sp.status NOT IN ('COMPLETED','FAILED')
                      )
                    RETURNING j.id, j.status::text
                """))
                rows = result.fetchall()
                await session.commit()
                if rows:
                    print(
                        f"[backup-scheduler] missing_snapshot_reaper: "
                        f"finalized {len(rows)} zombie jobs whose "
                        f"snapshot rows were deleted: "
                        f"{[(str(r[0]), r[1]) for r in rows]}"
                    )
        except Exception as exc:
            print(f"[backup-scheduler] missing_snapshot_reaper failed: {exc}")

    scheduler.add_job(
        _reap_missing_snapshot_jobs, "interval", minutes=5,
    )

    # Single-resource trigger-job reaper — sibling of the reaper above
    # for the OTHER uncovered shape. `POST /scheduler/resource/{id}`
    # (manual / force-full trigger) creates its Job QUEUED with
    # `resource_id` set and EMPTY batch_resource_ids, publishes to
    # backup.urgent, and no code path ever flips the Job row — the
    # worker only manages job status for mass-backup messages. Result:
    # the Activity feed showed force-full runs from 7/13 as "In
    # Progress" forever even though their snapshots were COMPLETED /
    # FAILED (all_jobs_terminal gate, observed 2026-07-14). Mirror the
    # snapshot outcome onto the Job once every snapshot under it has
    # settled (5-min grace) and no shard is still in flight.
    async def _reap_settled_single_resource_jobs():
        try:
            async with async_session_factory() as session:
                result = await session.execute(text("""
                    WITH agg AS (
                        SELECT s.job_id,
                               COUNT(*) FILTER (WHERE s.status IN ('COMPLETED','PARTIAL')) AS n_ok,
                               COUNT(*) FILTER (WHERE s.status NOT IN
                                   ('COMPLETED','PARTIAL','FAILED','PENDING_DELETION')) AS n_pending,
                               MAX(s.completed_at) AS last_settled,
                               COALESCE(SUM(s.item_count), 0)  AS items_total,
                               COALESCE(SUM(s.bytes_total), 0) AS bytes_total
                        FROM snapshots s
                        GROUP BY s.job_id
                    )
                    UPDATE jobs j SET
                        status = (CASE WHEN a.n_ok > 0
                                       THEN 'COMPLETED' ELSE 'FAILED'
                                  END)::jobstatus,
                        progress_pct = 100,
                        completed_at = COALESCE(a.last_settled, NOW()),
                        updated_at = NOW(),
                        -- 2026-08-25: this reaper used to finalize FAILED
                        -- with error_message NULL (jobs fef2894a/2d0da6b8),
                        -- making reaped failures indistinguishable from
                        -- "no error recorded". Copy the newest snapshot's
                        -- failure_reason up so the job row answers WHY.
                        error_message = CASE WHEN a.n_ok > 0
                            THEN j.error_message
                            ELSE COALESCE(
                                j.error_message,
                                LEFT('finalized FAILED by single_resource_job_reaper — '
                                     || COALESCE(
                                        (SELECT NULLIF(s2.extra_data->>'failure_reason', '')
                                           FROM snapshots s2
                                          WHERE s2.job_id = j.id
                                          ORDER BY s2.created_at DESC
                                          LIMIT 1),
                                        'snapshot recorded no failure_reason'),
                                     500))
                            END,
                        result = COALESCE(j.result::jsonb, '{}'::jsonb)
                                 || jsonb_build_object(
                                    'reaped_by', 'single_resource_job_reaper',
                                    'item_count', a.items_total,
                                    'bytes_added', a.bytes_total
                                 )
                    FROM agg a
                    WHERE j.id = a.job_id
                      AND j.type = 'BACKUP'
                      AND j.status IN ('QUEUED','RUNNING')
                      AND j.resource_id IS NOT NULL
                      AND COALESCE(cardinality(j.batch_resource_ids), 0) = 0
                      AND a.n_pending = 0
                      AND a.last_settled < NOW() - INTERVAL '5 minutes'
                      AND NOT EXISTS (
                          SELECT 1 FROM snapshot_partitions sp
                           WHERE sp.job_id = j.id
                             AND sp.status NOT IN ('COMPLETED','FAILED')
                      )
                    RETURNING j.id, j.status::text
                """))
                rows = result.fetchall()
                await session.commit()
                if rows:
                    print(
                        f"[backup-scheduler] single_resource_job_reaper: "
                        f"finalized {len(rows)} trigger jobs from their "
                        f"snapshot outcomes: "
                        f"{[(str(r[0]), r[1]) for r in rows[:5]]}"
                    )
        except Exception as exc:
            print(f"[backup-scheduler] single_resource_job_reaper failed: {exc}")

    scheduler.add_job(
        _reap_settled_single_resource_jobs, "interval", minutes=2,
    )

    # Task 26: Delete orphaned export ZIPs older than 1 day (3am UTC daily).
    # Primary mechanism is Azure lifecycle rule (ops/azure-lifecycle-exports.json);
    # this is a fallback for envs without lifecycle API access (local Azurite, restricted tenants).
    async def _exports_cleanup_daily():
        try:
            from services.exports_cleanup import cleanup_exports
            from shared.azure_storage import azure_storage_manager
            shard = azure_storage_manager.get_default_shard()
            deleted = await cleanup_exports(shard=shard, container="exports")
            print(f"[backup-scheduler] exports_cleanup: deleted {len(deleted)} blobs")
        except Exception as exc:
            print(f"[backup-scheduler] exports_cleanup failed: {exc}")
        # Task 17: enforce chat-export TTL + Hot->Cool tier shift across all
        # configured blob-account shards.
        try:
            from services.exports_cleanup import apply_chat_export_lifecycle
            await apply_chat_export_lifecycle()
            print("[backup-scheduler] chat_export_lifecycle: ok")
        except Exception as exc:
            print(f"[backup-scheduler] chat_export_lifecycle failed: {exc}")

    scheduler.add_job(_exports_cleanup_daily, "cron", hour=3, minute=0, timezone="UTC", id="exports_cleanup_daily")

    # Round 1.5 — daily retry of FAILED snapshots (one shot, throttled).
    scheduler.add_job(retry_failed_snapshots, "interval", hours=24)

    # R3.2 — daily backup integrity sample (random snapshots, hash check).
    scheduler.add_job(run_backup_verification, "interval", hours=24)

    # Round 1.5 — DLQ consumer (poison-message alerting). Runs as a background
    # task, NOT an APScheduler job; consumes continuously from backup.*.dlq.
    asyncio.create_task(consume_backup_dlq())

    # Schedule daily backup report (email/Slack/Teams)
    scheduler.add_job(send_daily_backup_report, "cron", hour=8, minute=0, timezone="UTC")

    # Schedule weekly summary report (Monday 9am UTC)
    scheduler.add_job(send_weekly_summary_report, "cron", hour=9, minute=0, day_of_week="mon", timezone="UTC")

    scheduler.start()

    # Start discovery reconciler (runs every 5 min, independent of APScheduler)
    await start_reconciler_loop()

    # Distributed-reconciliation orphan sweeper (2026-05-16). Closes
    # backup work rows whose owning worker died (redeploy, OOM,
    # network-split) so the Activity feed doesn't sit at "99 % In
    # Progress" forever. See
    # docs/superpowers/specs/2026-05-16-distributed-reconciliation-design.md.
    await start_orphan_sweeper_loop()

    # Automatic retry of failed backup jobs (Configuration tab). Reads
    # job_retry_max / job_retry_delay_minutes from app_settings each pass.
    await start_job_retry_loop()


@app.on_event("shutdown")
async def shutdown():
    """Cleanup on shutdown"""
    scheduler.shutdown()
    await message_bus.disconnect()


async def schedule_all_policies():
    """Scan all active SLA policies and schedule backup jobs for each one"""
    async with async_session_factory() as session:
        policies_result = await session.execute(
            select(SlaPolicy).where(SlaPolicy.enabled == True)
        )
        policies = policies_result.scalars().all()

    scheduled_count = 0
    for policy in policies:
        try:
            await schedule_policy_job(policy)
            scheduled_count += 1
        except Exception as e:
            print(f"[SCHEDULER] Failed to schedule job for policy {policy.name} ({policy.id}): {e}")

    print(f"[SCHEDULER] Scheduled backup jobs for {scheduled_count} SLA policies")


async def catch_up_missed_policy_runs():
    """Dispatch the latest missed scheduled run after scheduler downtime.

    APScheduler jobs are in-memory. Startup and the reschedule sweep rebuild
    future timers from the DB, but they cannot fire a cron event that happened
    while the process/cloud instance was down. This DB-backed pass reconstructs
    policy fire times over a bounded lookback and creates one scheduled run when
    no matching scheduled Job exists.
    """
    now = datetime.utcnow()
    lookback_minutes = max(60, settings.SCHEDULER_CATCHUP_LOOKBACK_MINUTES)
    window_start = now - timedelta(minutes=lookback_minutes)
    to_dispatch: List[Tuple[str, str, datetime, int]] = []

    async with async_session_factory() as session:
        policies_result = await session.execute(
            select(SlaPolicy).where(
                and_(
                    SlaPolicy.enabled == True,
                    SlaPolicy.frequency != "MANUAL",
                )
            )
        )
        policies = policies_result.scalars().all()

        for policy in policies:
            try:
                # Capped policies have a durable within-cycle drain that opens
                # missed cron cycles itself. Catch-up owns only uncapped ones.
                if (getattr(policy, "max_concurrent_backups", None) or 0) > 0:
                    continue
                active_since = _utc_naive(policy.created_at) if policy.created_at else window_start
                fire_start = max(window_start, active_since)
                fires = _policy_fire_times_between(policy, fire_start, now)
                if not fires:
                    continue
                last_fire = fires[-1]

                resources_result = await session.execute(
                    select(Resource).where(
                        and_(
                            Resource.sla_policy_id == policy.id,
                            Resource.status.in_([ResourceStatus.DISCOVERED, ResourceStatus.ACTIVE]),
                        )
                    )
                )
                active_resources = resources_result.scalars().all()
                enabled_count = 0
                for resource in active_resources:
                    resource_type = resource.type.value if hasattr(resource.type, "value") else str(resource.type)
                    if resource_type_enabled(resource_type, policy) and resource_backup_eligible(resource):
                        enabled_count += 1

                if enabled_count <= 0:
                    continue

                in_flight = (await session.execute(
                    select(func.count(Job.id)).where(
                        and_(
                            Job.type == JobType.BACKUP,
                            func.json_extract_path_text(Job.spec, "sla_policy_id") == str(policy.id),
                            func.json_extract_path_text(Job.spec, "triggered_by") == "SCHEDULED",
                            Job.status.in_([JobStatus.QUEUED, JobStatus.RUNNING]),
                        )
                    )
                )).scalar() or 0
                if in_flight:
                    continue

                recent_scheduled = (await session.execute(
                    select(func.count(Job.id)).where(
                        and_(
                            Job.type == JobType.BACKUP,
                            func.json_extract_path_text(Job.spec, "sla_policy_id") == str(policy.id),
                            func.json_extract_path_text(Job.spec, "triggered_by") == "SCHEDULED",
                            Job.created_at >= last_fire - timedelta(minutes=5),
                        )
                    )
                )).scalar() or 0
                if recent_scheduled:
                    continue

                to_dispatch.append((str(policy.id), policy.name, last_fire, enabled_count))
            except Exception as exc:
                print(f"[SCHEDULER] Catch-up scan failed for policy {policy.name} ({policy.id}): {exc}")

    for policy_id, policy_name, last_fire, enabled_count in to_dispatch:
        print(
            f"[SCHEDULER] Catching up missed scheduled fire for policy "
            f"'{policy_name}' at {last_fire.isoformat()}Z "
            f"({enabled_count} eligible resources)"
        )
        try:
            # Cron may have fired after the scan. Recheck under the same lock
            # that serializes cron/drain dispatch before opening another run.
            async with _policy_lock(policy_id):
                async with async_session_factory() as session:
                    recent = (await session.execute(
                        select(func.count(Job.id)).where(
                            Job.type == JobType.BACKUP,
                            func.json_extract_path_text(Job.spec, "sla_policy_id") == policy_id,
                            func.json_extract_path_text(Job.spec, "triggered_by") == "SCHEDULED",
                            Job.created_at >= last_fire - timedelta(minutes=5),
                        )
                    )).scalar() or 0
                if not recent:
                    _cycle_exhausted_until.pop(policy_id, None)
                    await _dispatch_policy_backups_impl(policy_id)
        except Exception as exc:
            print(f"[SCHEDULER] Catch-up dispatch failed for policy {policy_name} ({policy_id}): {exc}")


# Records the effective cron fingerprint last registered for each policy job
# so the 5-min reschedule sweep can skip unchanged policies instead of
# churning their APScheduler timers (see schedule_policy_job).
_POLICY_JOB_FINGERPRINTS: dict[str, tuple] = {}


async def schedule_policy_job(policy: SlaPolicy):
    """Schedule or reschedule a backup job for a specific SLA policy.

    MANUAL policies are intentionally never scheduled — they only run when
    explicitly triggered via the /trigger endpoint. afi behaves identically.

    Idempotent: the durability sweep (sla_policy_reschedule_sweep) re-invokes
    this for every policy every SCHEDULER_RESCHEDULE_INTERVAL_SECONDS. Removing
    and re-adding an *unchanged* cron job resets its next_run_time and races the
    fire window — repeatedly doing so dropped scheduled Gold fires (observed
    1-2x/day instead of 3x, with 12.5h-vs-8h SLA violations). We therefore only
    (re)register when the effective schedule actually changed."""
    job_id = f"policy_backup_{policy.id}"

    cron_params = frequency_to_cron_params(
        policy.frequency, policy.backup_days, policy.backup_window_start,
        policy_id=str(policy.id),
    )

    # MANUAL / unschedulable: tear down any live job and forget its fingerprint.
    if cron_params is None:
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)
        _POLICY_JOB_FINGERPRINTS.pop(job_id, None)
        print(f"[SCHEDULER] Skipping '{policy.name}' (MANUAL policy — admin-triggered only)")
        return

    # Skip the no-op reschedule: same effective cron AND the live timer still
    # exists. add_job(replace_existing=True) below handles genuine changes
    # atomically, so the explicit remove_job is no longer needed.
    fingerprint = tuple(sorted(cron_params.items()))
    if (
        scheduler.get_job(job_id) is not None
        and _POLICY_JOB_FINGERPRINTS.get(job_id) == fingerprint
    ):
        return

    scheduler.add_job(
        dispatch_policy_backups,
        args=[str(policy.id)],
        id=job_id,
        name=f"Backup: {policy.name}",
        replace_existing=True,
        timezone="UTC",
        misfire_grace_time=max(60, settings.SCHEDULER_MISFIRE_GRACE_SECONDS),
        coalesce=True,
        max_instances=1,
        **cron_params,
    )
    _POLICY_JOB_FINGERPRINTS[job_id] = fingerprint

    days_info = f" days={policy.backup_days}" if policy.backup_days and len(policy.backup_days) < 7 else ""
    print(f"[SCHEDULER] Scheduled '{policy.name}' ({policy.frequency}{days_info}) -> job_id={job_id}")


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "backup-scheduler"}


@app.post("/scheduler/policy/{policy_id}/trigger")
async def trigger_policy_backup(policy_id: str, background_tasks: BackgroundTasks):
    """Manually trigger backup dispatch for a specific SLA policy.

    Bypasses the run-overlap guard on purpose — an explicit operator
    trigger must not be silently swallowed because a cycle is in flight.
    Per-resource safety still holds: the worker's cross-job
    ``skipped_concurrent`` guard prevents any resource from being backed
    up twice concurrently.
    """
    background_tasks.add_task(
        dispatch_policy_backups, policy_id, False,
    )
    return {"status": "scheduled", "policy_id": policy_id}


@app.post("/scheduler/reschedule-all")
async def reschedule_all_policies(background_tasks: BackgroundTasks):
    """Re-scan all SLA policies and rebuild the scheduler"""
    background_tasks.add_task(schedule_all_policies)
    return {"status": "rescheduling"}


@app.post("/scheduler/reconcile-lifecycle")
async def trigger_lifecycle_reconcile(background_tasks: BackgroundTasks):
    """Trigger an immediate run of the lifecycle / immutability / legal-hold
    reconciler. Called by resource-service after a policy save so the
    operator's changes take effect right away — no waiting for the daily
    cron tick. Skipped silently when the active backend is SeaweedFS."""
    background_tasks.add_task(reconcile_lifecycle_policies)
    return {"status": "reconciling"}


@app.post("/scheduler/resource/{resource_id}")
async def trigger_single_backup(resource_id: str, full_backup: bool = False):
    """Trigger immediate backup for a single resource"""
    async with async_session_factory() as session:
        resource = await session.get(Resource, uuid.UUID(resource_id))
        if not resource:
            return {"error": "Resource not found"}, 404

        # Prevent manual backup on inaccessible resources
        status_val = resource.status.value if hasattr(resource.status, 'value') else str(resource.status)
        if status_val in ("INACCESSIBLE", "SUSPENDED", "PENDING_DELETION"):
            return {
                "error": f"Resource is {status_val} and cannot be backed up. "
                         f"Run discovery first to restore access or remove the resource.",
                "resource_status": status_val,
            }, 422

        job_id = uuid.uuid4()

        # Create job record
        job = Job(
            id=job_id,
            type=JobType.BACKUP,
            tenant_id=resource.tenant_id,
            resource_id=resource.id,
            status=JobStatus.QUEUED,
            priority=1,  # Urgent
            spec={
                "full_backup": full_backup,
                "triggered_by": "MANUAL",
                "snapshot_label": "manual",
            }
        )
        session.add(job)
        await session.commit()

        # Send to urgent queue
        await message_bus.publish(
            "backup.urgent",
            {
                "jobId": str(job_id),
                "resourceId": resource_id,
                "tenantId": str(resource.tenant_id),
                "type": "FULL" if full_backup else "INCREMENTAL",
                "priority": 1,
                "triggeredBy": "MANUAL",
                "snapshotLabel": "manual",
                "forceFullBackup": full_backup,
            },
            priority=1
        )

        await emit_backup_triggered(
            job=job, resource=resource,
            trigger_label="MANUAL", full_backup=full_backup,
        )
        return {"status": "queued", "job_id": str(job_id)}


def resource_backup_eligible(resource) -> bool:
    """False when the user has NO M365 license for this workload.

    Discovery probes each Tier-2 workload and, on a permanent 403/404/423
    (no Exchange Online / no OneDrive / locked mailbox), stamps
    ``metadata.license_missing=True`` on the resource (surfaced on the ORM as
    ``resource.extra_data``). An unlicensed workload can NEVER produce a
    snapshot, so dispatching it just queues a doomed job AND leaves the
    Activity batch pinned at "In Progress" forever with an inflated progress
    denominator (the Amit-Kumar / Lyly "1.1 GiB so far, 14% — never finishes"
    report). We therefore skip it at dispatch.

    This is re-evaluated EVERY fire: ``tier2_discovery`` clears
    ``license_missing`` as soon as a probe succeeds again (license added), so
    a newly-licensed workload rejoins the dispatch set automatically with no
    manual re-enable. Absence of the flag = eligible.
    """
    meta = getattr(resource, "extra_data", None) or {}
    return not bool(meta.get("license_missing"))


def select_resources_for_dispatch(enabled_resources, inflight_resource_ids):
    """Filter in-flight and unlicensed resources before applying wave capacity.

    The caller applies the concurrency budget; the drain carries the remaining
    eligible resources during the same policy cycle.
    """
    skip = set(inflight_resource_ids or ())
    return [
        r for r in enabled_resources
        if r.id not in skip and resource_backup_eligible(r)
    ]


# One asyncio.Lock per policy: the cron tick and the drain tick (Task 4)
# both read the in-flight count and then INSERT jobs against the same
# cap, so they must never interleave for one policy. Process-local — the
# scheduler is single-replica; scaling it out needs a DB advisory lock.
_policy_locks: Dict[str, asyncio.Lock] = {}
# Drain memo: once a cycle has nothing left to dispatch, remember the
# window end so idle ticks cost nothing. Cleared by the cron tick.
_cycle_exhausted_until: Dict[str, datetime] = {}


def _policy_lock(policy_id: str) -> asyncio.Lock:
    lock = _policy_locks.get(policy_id)
    if lock is None:
        lock = _policy_locks[policy_id] = asyncio.Lock()
    return lock


async def dispatch_policy_backups(
    policy_id: str, respect_overlap_guard: bool = True,
):
    """Cron / manual entry point. Holds the per-policy lock so a drain wave
    can never interleave with the tick; see ``_dispatch_policy_backups_impl``
    for the strategy."""
    # Canonicalize BEFORE touching the lock map: the manual
    # /scheduler/policy/{id}/trigger path can hand in a differently-cased
    # or brace-wrapped id, and that must serialize against the SAME lock
    # the cron/drain path uses — not a second, distinct lock entry. This
    # also rejects a garbage id before it can insert a bogus lock.
    pid = str(uuid.UUID(str(policy_id)))
    async with _policy_lock(pid):
        _cycle_exhausted_until.pop(pid, None)
        return await _dispatch_policy_backups_impl(policy_id, respect_overlap_guard)


async def _dispatch_policy_backups_impl(
    policy_id: str, respect_overlap_guard: bool = True,
):
    """
    Dispatch backups for a specific SLA policy, filtering resources by the policy's backup flags.

    ``respect_overlap_guard`` — True for the cron path (skip the tick if
    the previous scheduled cycle is still draining). The manual
    ``/scheduler/policy/{id}/trigger`` endpoint passes False: an operator
    asking for a run explicitly has already decided, and silently
    swallowing their click would look like a broken button.

    The policy_id comes from the resource's sla_policy_id assignment.

    Strategy:
    1. Fetch the SLA policy
    2. Fetch all active resources assigned to this policy
    3. Filter resources to only include types enabled in the policy's backup flags
    4. Group by resource_type + tenant_id
    5. Split into batches of 1000 resources
    6. Dispatch batches to RabbitMQ
    """
    print(f"[SCHEDULER] Starting backup dispatch for policy {policy_id}")

    async with async_session_factory() as session:
        # Fetch the SLA policy
        policy = await session.get(SlaPolicy, uuid.UUID(policy_id))
        if not policy:
            print(f"[SCHEDULER] Policy {policy_id} not found")
            return

        if not policy.enabled:
            print(f"[SCHEDULER] Policy {policy.name} is disabled, skipping")
            return

        print(f"[SCHEDULER] Processing policy '{policy.name}' (frequency={policy.frequency})")

        # R2.3 — run-overlap guard. Do NOT start a new cycle while the
        # previous SCHEDULED cycle for this policy is still draining.
        #
        # There was no cycle-level control before this: dispatch only
        # publishes to RabbitMQ and returns in seconds, so APScheduler's
        # max_instances=1 never engaged, and Gold (THREE_DAILY, 8h) fired
        # cycle N+1 with cycle N 12h into a 62GB SharePoint partition.
        # max_concurrent_backups could not cover this — it counted Jobs
        # (~10/cycle) against a resource budget (50), so its defer path
        # needed five stalled cycles.
        #
        # Checked BEFORE the resource fetch / Power BI prefilter so a
        # skipped tick costs one cheap COUNT instead of a Graph walk.
        stale_after_hours = dispatch_policy.overlap_stale_after_hours(
            policy.frequency,
        )
        inflight_jobs, inflight_oldest = (
            await session.execute(
                dispatch_policy.build_scheduled_inflight_query(policy.id)
            )
        ).one()
        now_utc = datetime.utcnow()
        overlap_action = dispatch_policy.decide_overlap_action(
            inflight_jobs=int(inflight_jobs or 0),
            oldest_created_at=inflight_oldest,
            now=now_utc,
            stale_after_hours=stale_after_hours,
            guard_enabled=(
                respect_overlap_guard and dispatch_policy.overlap_guard_enabled()
            ),
        )
        # SKIP is now a per-RESOURCE hold rather than a whole-policy stop —
        # see dispatch_policy.partition_overlap_holds. One over-cadence
        # resource (IT OPS NOTES, 12.84h vs Gold's 8h) suppressed the other
        # 49 and lost the entire 2026-08-06 02:07Z tick. We therefore can no
        # longer return here: the resource list is needed to decide what is
        # actually busy, so the hold is applied after the flag filter below
        # (still before the Power BI Graph prefilter, so a fully-held tick
        # costs no Graph calls).
        overlap_hold = overlap_action == dispatch_policy.SKIP
        if overlap_action != dispatch_policy.DISPATCH:
            details = dispatch_policy.overlap_audit_details(
                policy, action=overlap_action,
                inflight_jobs=int(inflight_jobs or 0),
                oldest_created_at=inflight_oldest, now=now_utc,
                stale_after_hours=stale_after_hours,
            )
            if not overlap_hold:
                # OVERRIDE — a cycle wedged past the horizon must not halt
                # every future backup, so we dispatch anyway and say so.
                print(f"[SCHEDULER] WARNING: {details['message']}")
                await publish_cycle_audit_event(
                    policy, "BACKUP_CYCLE_OVERLAP_OVERRIDE", details,
                )

        # Fetch all active resources assigned to this policy
        resources_result = await session.execute(
            select(Resource).where(
                and_(
                    Resource.sla_policy_id == policy.id,
                    Resource.status.in_([ResourceStatus.DISCOVERED, ResourceStatus.ACTIVE]),
                )
            ).options(selectinload(Resource.tenant))
        )
        all_resources = resources_result.scalars().all()

        if not all_resources:
            print(f"[SCHEDULER] No active resources for policy '{policy.name}'")
            return

        # Filter resources by the policy's backup flags
        enabled_resources = []
        skipped_resources = []
        for resource in all_resources:
            resource_type = resource.type.value if hasattr(resource.type, "value") else str(resource.type)
            if resource_type_enabled(resource_type, policy):
                enabled_resources.append(resource)
            else:
                skipped_resources.append(resource)

        skipped = len(skipped_resources)
        if skipped > 0:
            print(f"[SCHEDULER] Filtered out {skipped} resources (disabled by policy flags)")
            await publish_sla_skip_audit_events(policy, skipped_resources)

        if not enabled_resources:
            print(f"[SCHEDULER] No enabled resources for policy '{policy.name}' after flag filtering")
            return

        # Skip workloads the user has NO M365 license for BEFORE the
        # in-flight step, so the log/audit attributes each skip to the
        # right reason. resource_backup_eligible re-reads the flag every
        # fire; discovery clears it when a license appears, so this is
        # self-correcting (no manual re-enable).
        _license_skipped = [
            r for r in enabled_resources if not resource_backup_eligible(r)
        ]
        if _license_skipped:
            enabled_resources = [
                r for r in enabled_resources if resource_backup_eligible(r)
            ]
            print(
                f"[SCHEDULER] Policy '{policy.name}': {len(_license_skipped)} "
                f"resource(s) skipped — user has no M365 license for that "
                f"workload (auto-resumes when a license is added)"
            )
            await publish_license_skip_audit_events(policy, _license_skipped)
        if not enabled_resources:
            print(f"[SCHEDULER] No license-eligible resources for policy '{policy.name}'")
            return

        # Per-resource overlap hold. The previous cycle is still draining, so
        # hold back only the resources that genuinely have a backup in flight
        # and let the idle ones run. Skipping the whole policy (the old
        # behaviour) turned any resource slower than the cadence into a total
        # coverage loss for every other resource on that policy.
        #
        # The in-flight id query is deliberately NOT policy-scoped: a resource
        # busy via a manual "Backup now" or a PREEMPTIVE run must be held too.
        #
        # Bound BEFORE the guard so the job spec always carries the key, even
        # on a tick that never enters the hold branch. An absent key means
        # "batch predates this writer" to the Activity rollup, which is a
        # different (and deliberately conservative) statement from "nothing was
        # held" — see shared/batch_rollup._withheld_children_cte.
        overlap_held_resource_ids: List[str] = []
        if overlap_hold:
            inflight_ids = {
                row[0] for row in (await session.execute(
                    dispatch_policy.build_inflight_resource_ids_query()
                )).all() if row[0] is not None
            }
            enabled_resources, held = dispatch_policy.partition_overlap_holds(
                enabled_resources, inflight_ids,
            )
            if not enabled_resources:
                # Every resource is busy — this is the only case that still
                # skips the cycle outright, and it is the honest one.
                details = dispatch_policy.overlap_audit_details(
                    policy, action=dispatch_policy.SKIP,
                    inflight_jobs=int(inflight_jobs or 0),
                    oldest_created_at=inflight_oldest, now=now_utc,
                    stale_after_hours=stale_after_hours,
                )
                print(f"[SCHEDULER] {details['message']}")
                await publish_cycle_audit_event(
                    policy, "BACKUP_CYCLE_SKIPPED_OVERLAP", details,
                )
                return
            if held:
                # Record WHICH resources the guard withheld, so the Activity
                # rollup can tell a deliberate hold from a lost fan-out
                # instead of inferring it. Measured 2026-08-11: batch
                # 667734c3 (Gold, 10:07 UTC) held 10 busy resources and
                # dispatched 40; its 7 withheld USER_CHATS children left
                # missing_t2 = 7 with every job COMPLETED, so the row read
                # "In Progress · 15 %" and could never clear. The withheld
                # ids are the only durable trace — the audit event is not
                # queryable from the rollup's single CTE query.
                overlap_held_resource_ids = [str(r.id) for r in held]
                details = dispatch_policy.overlap_partial_audit_details(
                    policy, held,
                    inflight_jobs=int(inflight_jobs or 0),
                    oldest_created_at=inflight_oldest, now=now_utc,
                    dispatched=len(enabled_resources),
                )
                print(f"[SCHEDULER] {details['message']}")
                for r in held:
                    rtype = r.type.value if hasattr(r.type, "value") else str(r.type)
                    print(f"[SCHEDULER]   overlap-held: {rtype} {r.id} "
                          f"({getattr(r, 'display_name', None)})")
                await publish_cycle_audit_event(
                    policy, "BACKUP_CYCLE_PARTIAL_OVERLAP", details,
                )

        power_bi_resources = [resource for resource in enabled_resources if resource.type == ResourceType.POWER_BI]
        if power_bi_resources:
            tenant_result = await session.execute(
                select(Tenant).where(Tenant.id.in_({resource.tenant_id for resource in power_bi_resources}))
            )
            tenants_map = {tenant.id: tenant for tenant in tenant_result.scalars().all()}

            filtered_power_bi_ids = set()
            resources_by_tenant: Dict[str, List[Resource]] = {}
            for resource in power_bi_resources:
                resources_by_tenant.setdefault(str(resource.tenant_id), []).append(resource)

            for tenant_id, tenant_resources in resources_by_tenant.items():
                tenant = tenants_map.get(uuid.UUID(tenant_id))
                if not tenant:
                    filtered_power_bi_ids.update(str(resource.id) for resource in tenant_resources)
                    continue

                resources_without_backup = [resource for resource in tenant_resources if resource.last_backup_at is None]
                filtered_power_bi_ids.update(str(resource.id) for resource in resources_without_backup)

                resources_with_backup = [resource for resource in tenant_resources if resource.last_backup_at is not None]
                if not resources_with_backup:
                    continue

                min_last_backup = min(resource.last_backup_at for resource in resources_with_backup if resource.last_backup_at)
                modified_since = max(min_last_backup, datetime.utcnow() - timedelta(days=30))
                if modified_since > datetime.utcnow() - timedelta(minutes=31):
                    modified_since = datetime.utcnow() - timedelta(minutes=31)

                try:
                    client = PowerBIClient(
                        tenant.external_tenant_id or settings.EFFECTIVE_POWER_BI_TENANT_ID,
                        refresh_token=PowerBIClient.get_refresh_token_from_tenant(tenant),
                    )
                    modified_workspace_ids = set(await client.list_modified_workspace_ids(modified_since))
                    for resource in resources_with_backup:
                        workspace_id = (resource.extra_data or {}).get("workspace_id")
                        if not workspace_id and resource.external_id and resource.external_id.startswith("pbi_ws_"):
                            workspace_id = resource.external_id.replace("pbi_ws_", "", 1)
                        if workspace_id in modified_workspace_ids:
                            filtered_power_bi_ids.add(str(resource.id))
                except Exception as exc:
                    print(f"[SCHEDULER] Power BI modified-workspace prefilter unavailable for tenant {tenant.display_name}: {exc}")
                    filtered_power_bi_ids.update(str(resource.id) for resource in tenant_resources)

            enabled_resources = [
                resource for resource in enabled_resources
                if resource.type != ResourceType.POWER_BI or str(resource.id) in filtered_power_bi_ids
            ]

        # R2.2 — honor max_concurrent_backups. Cap how many RESOURCES we
        # publish this tick. The drain picks up the remainder in later
        # waves of this cycle as capacity becomes available.
        #
        # The in-flight side counts RESOURCES, not job rows. It used to
        # count Jobs while spending the budget on resources: one job can
        # carry up to BATCH_SIZE (1000) resources, so a row count
        # understated load by orders of magnitude and made `budget == 0`
        # unreachable. Its only real effect on Gold (50 resources, cap
        # 50) was an off-by-N trim that silently dropped one resource per
        # tick and wedged the Activity row at "In Progress" for 48h.
        #
        # Every deferral is now logged AND audited — a resource dropping
        # out of a cycle in silence is how the 8h SLA got missed with no
        # trace.
        deferred_resource_ids: List[str] = []
        max_concurrent = getattr(policy, "max_concurrent_backups", None) or 0
        if max_concurrent > 0:
            inflight_resources = (await session.execute(
                dispatch_policy.build_inflight_resource_count_query(policy.id)
            )).scalar() or 0
            requested = len(enabled_resources)
            enabled_resources, deferred = dispatch_policy.plan_concurrency_budget(
                enabled_resources, max_concurrent, int(inflight_resources),
            )
            if deferred:
                deferred_resource_ids = [str(r.id) for r in deferred]
                details = dispatch_policy.deferred_audit_details(
                    policy, deferred,
                    max_concurrent=max_concurrent,
                    inflight_resources=int(inflight_resources),
                    dispatched=len(enabled_resources),
                )
                print(f"[SCHEDULER] {details['message']}")
                for r in deferred:
                    rtype = r.type.value if hasattr(r.type, "value") else str(r.type)
                    print(f"[SCHEDULER]   deferred: {rtype} {r.id} "
                          f"({getattr(r, 'display_name', None)})")
                await publish_cycle_audit_event(
                    policy, "BACKUP_DEFERRED_CONCURRENCY_CAP", details,
                )
            if not enabled_resources:
                print(f"[SCHEDULER] Policy '{policy.name}' at concurrency cap "
                      f"({inflight_resources}/{max_concurrent} resources "
                      f"in-flight) — deferring all {requested} this tick")
                return

        print(f"[SCHEDULER] Found {len(enabled_resources)} resources to backup for policy '{policy.name}'")

        # One batch_id per policy dispatch, shared by every Job it
        # creates. The Activity rollup groups jobs on spec.batch_id and
        # expects the Tier-1 users job and its content-type siblings in
        # ONE group — without this stamp each job rolled up alone, the
        # users job's expected Tier-2 children were never "observed"
        # (missing_t2 > 0), and every scheduled dispatch left a
        # permanently-"In Progress" Activity row (2026-07-14). The UI
        # click path (job-service) has stamped batch_id since the
        # 2026-05-15 incident; the scheduled path was missed.
        # Drain waves (Task 4) REUSE this id so the row stays one per cycle.
        dispatch_batch_id = str(uuid.uuid4())

        total_dispatched, dispatched_queues = await _dispatch_resources(
            session, policy, enabled_resources,
            dispatch_batch_id=dispatch_batch_id,
            deferred_resource_ids=deferred_resource_ids,
            overlap_held_resource_ids=overlap_held_resource_ids,
            wave=1,
        )

        # Report the queues actually used — resources fan out across
        # backup.high/normal, backup.heavy and the azure.* lanes, so
        # naming a single queue misreported where the work went.
        queues_used = ", ".join(sorted(dispatched_queues)) or "none"
        print(f"[SCHEDULER] Dispatched {total_dispatched} resources "
              f"across [{queues_used}] for policy '{policy.name}'"
              + (f"; {len(deferred_resource_ids)} deferred to later waves of this cycle"
                 if deferred_resource_ids else "")
              + (f"; {len(overlap_held_resource_ids)} overlap-held to the "
                 f"next cycle" if overlap_held_resource_ids else ""))


async def _dispatch_resources(
    session, policy: SlaPolicy, resources: Sequence[Any], *,
    dispatch_batch_id: str,
    deferred_resource_ids: List[str],
    overlap_held_resource_ids: List[str],
    wave: int = 1,
) -> Tuple[int, set]:
    """Build one QUEUED Job per ``type:tenant`` group (1000-resource
    batches), COMMIT, then publish each job's message.

    Commit-before-publish is load-bearing. Until 2026-09-03 the rows were
    published first: the worker looked up the jobId, found nothing
    (uncommitted), acked and dropped it, and the 3-minute outbox reconciler
    republished every scheduled batch ~5 min late on backup.urgent. A
    publish failure after the commit is logged, not raised — the row is
    QUEUED, counts toward the cap, and the reconciler republishes it.

    Shared by the cron tick (``wave=1``) and the drain (``wave>=2``).
    ``resources`` may be ORM rows or light Row projections: only ``id``,
    ``type``, ``tenant_id`` and ``last_backup_at`` are read.
    """
    groups: Dict[str, List[Any]] = {}
    for resource in resources:
        rtype = resource.type.value if hasattr(resource.type, "value") else str(resource.type)
        groups.setdefault(f"{rtype}:{resource.tenant_id}", []).append(resource)

    print(f"[SCHEDULER] Grouped into {len(groups)} resource type + tenant combinations")

    BATCH_SIZE = 1000
    pending: List[Tuple[Job, str, Dict[str, Any], bool, str, int]] = []

    for group_key, group_resources in groups.items():
        resource_type, tenant_id = group_key.split(":")

        # AZ-4: Route Azure workload resources to their dedicated queues
        azure_queue = None
        try:
            rt = ResourceType(resource_type)
            azure_queue = AZURE_WORKLOAD_QUEUES.get(rt)
        except ValueError:
            pass

        base_queue = "backup.high" if policy.frequency == "THREE_DAILY" else "backup.normal"
        if azure_queue:
            group_queue = azure_queue
            print(f"[SCHEDULER] Azure workload {resource_type} → queue {azure_queue} (not backup.*)")
        else:
            # Heavy-pool routing for file-content workloads — keeps shared
            # backup.high / backup.normal lanes free for MAILBOX / ENTRA /
            # USER_* work so a single 500 GB drive can't starve them.
            from shared.export_routing import pick_backup_queue
            group_queue = pick_backup_queue(
                resource_type=resource_type,
                default_queue=base_queue,
            )
            if group_queue != base_queue:
                print(f"[SCHEDULER] Heavy workload {resource_type} → queue {group_queue}")

        for i in range(0, len(group_resources), BATCH_SIZE):
            batch = group_resources[i:i + BATCH_SIZE]
            resource_ids = [str(r.id) for r in batch]

            # fullBackup: True only if NONE of the resources have been backed up before
            has_previous_backup = any(r.last_backup_at is not None for r in batch)
            effective_full_backup = not has_previous_backup

            job_id = uuid.uuid4()
            job = Job(
                id=job_id,
                type=JobType.BACKUP,
                tenant_id=uuid.UUID(tenant_id),
                batch_resource_ids=[uuid.UUID(rid) for rid in resource_ids],
                status=JobStatus.QUEUED,
                priority=5,
                spec={
                    "sla_policy_id": str(policy.id),
                    "sla_policy_name": policy.name,
                    "resource_type": resource_type,
                    "batch_size": len(resource_ids),
                    "triggered_by": "SCHEDULED",
                    "snapshot_label": "scheduled",
                    "fullBackup": effective_full_backup,
                    "batch_id": dispatch_batch_id,
                    # Resources the concurrency cap held back this
                    # cycle. The Activity rollup subtracts these from
                    # expected_t2 — otherwise a DELIBERATE deferral
                    # is indistinguishable from a LOST fan-out and
                    # wedges the row at "In Progress" for the full
                    # 48h batch_recent horizon (2026-08-05 RCA).
                    # Stamped on every job in the batch; the rollup
                    # unions them per batch_id. On a drain wave this is
                    # the remainder still waiting after the wave.
                    "deferred_resource_ids": deferred_resource_ids,
                    # Resources the run-overlap guard (or the drain's
                    # unconditional in-flight partition) held back
                    # because they still had a backup in flight.
                    # Separate key on purpose: same rollup treatment,
                    # different reason (cap = capacity, overlap = duration).
                    "overlap_held_resource_ids": overlap_held_resource_ids,
                    # Lane the message was published on. The outbox
                    # reconciler republishes on spec.queue (defaulting to
                    # backup.urgent when absent), so recording it keeps a
                    # rescued job on its intended lane.
                    "queue": group_queue,
                    # 1 = the cron tick; 2.. = drain waves within the cycle.
                    "wave": wave,
                }
            )
            session.add(job)

            message = create_mass_backup_message(
                job_id=str(job_id),
                tenant_id=tenant_id,
                resource_type=resource_type,
                resource_ids=resource_ids,
                sla_policy_id=str(policy.id),
                full_backup=effective_full_backup,
            )
            # Propagate the dispatch batch_id so any Jobs the worker
            # spawns downstream (Tier-2 handoff, heavy-lane OneDrive)
            # land in the same Activity group.
            message["batchId"] = dispatch_batch_id
            pending.append((job, group_queue, message, effective_full_backup,
                            resource_type, len(resource_ids)))

    # Rows first. A worker that receives a message for a job it cannot
    # SELECT acks and drops it (workers/backup-worker/main.py, "if not job").
    await session.commit()

    total_dispatched = 0
    dispatched_queues: set = set()
    for job, group_queue, message, effective_full_backup, resource_type, count in pending:
        try:
            await message_bus.publish(
                group_queue,
                message,
                priority=message["priority"],
            )
        except Exception as exc:
            print(f"[SCHEDULER] publish failed for job {job.id} on {group_queue}: "
                  f"{exc} — row is committed; outbox reconcile will republish")

        # The BACKUP_TRIGGERED audit depends only on the committed row,
        # not on the publish succeeding — the outbox reconciler republishes
        # a failed-publish job later WITHOUT ever emitting this, so a job
        # whose first publish attempt failed must still get its trigger
        # audit row here.
        try:
            await emit_backup_triggered(
                job=job, resource=None, tenant=None,
                trigger_label="SCHEDULED",
                actor_type="SYSTEM",
                full_backup=effective_full_backup,
                batch_resource_count=count,
                extra_details={
                    "sla_policy_id": str(policy.id),
                    "sla_policy_name": policy.name,
                    "resource_type": resource_type,
                    "wave": wave,
                },
            )
        except Exception as exc:
            print(f"[SCHEDULER] BACKUP_TRIGGERED audit failed for job {job.id}: {exc}")

        total_dispatched += count
        dispatched_queues.add(group_queue)

        # Stagger dispatch to prevent overwhelming Graph API
        await asyncio.sleep(0.1)

    return total_dispatched, dispatched_queues


# ─── deferred-resource drain ────────────────────────────────────────────
#
# The cron tick trims to max_concurrent_backups and records the remainder
# in spec.deferred_resource_ids. Until 2026-09-03 nothing ever picked that
# remainder up before the NEXT tick, so a DAILY policy with 356 eligible
# resources and cap 50 protected 50 a night (Silver: 219 never backed up).
# This tick recomputes, per capped policy, what the current cycle has NOT
# carried yet and dispatches the next wave whenever slots are free — the
# cap becomes a concurrency limit instead of a daily quota.
#
# Stateless on purpose: the cycle window comes from the policy's own
# CronTrigger, "already carried" comes from the jobs table, in-flight from
# the same queries the tick uses. Nothing to persist, nothing to migrate,
# nothing to get out of sync. BACKUP_CYCLE_DRAIN=off turns it into a no-op.

_cycle_incomplete_emitted: Dict[str, datetime] = {}


def _policy_cycle_trigger(policy: SlaPolicy):
    """The trigger the cron job is registered with, so the drain's idea of
    'this cycle' is exactly the cron's. Falls back to building one from the
    same params if the job is not registered (e.g. unit tests, startup race)."""
    job = scheduler.get_job(f"policy_backup_{policy.id}")
    if job is not None and getattr(job, "trigger", None) is not None:
        return job.trigger
    cron_params = frequency_to_cron_params(
        policy.frequency, policy.backup_days, policy.backup_window_start,
        policy_id=str(policy.id),
    )
    if cron_params is None:
        return None
    from apscheduler.triggers.cron import CronTrigger
    # frequency_to_cron_params returns add_job kwargs, which include
    # trigger="cron" — CronTrigger.__init__ has no such parameter.
    params = {k: v for k, v in cron_params.items() if k != "trigger"}
    return CronTrigger(timezone="UTC", **params)


def _enabled_type_values(policy: SlaPolicy) -> List[str]:
    return [rt.value for rt in ResourceType if resource_type_enabled(rt.value, policy)]


async def drain_policy_cycle(
    policy_id: str, *, now: Optional[datetime] = None,
) -> int:
    """Dispatch the next wave for one policy's open cycle. Returns the
    number of resources dispatched (0 on every no-op path)."""
    if not dispatch_policy.drain_enabled():
        return 0
    # Canonical form, same reasoning as dispatch_policy_backups: this must
    # be the SAME lock key the cron/manual path uses for this policy, and
    # a garbage id must never insert a lock entry.
    pid = str(uuid.UUID(str(policy_id)))
    async with _policy_lock(pid):
        async with async_session_factory() as session:
            policy = await session.get(SlaPolicy, uuid.UUID(pid))
            if not policy or not policy.enabled:
                return 0
            max_concurrent = int(getattr(policy, "max_concurrent_backups", None) or 0)
            if max_concurrent <= 0:
                return 0

            now_utc = now or datetime.utcnow()
            trigger = _policy_cycle_trigger(policy)
            if trigger is None:
                return 0
            window = dispatch_policy.cycle_window_from_trigger(trigger, now_utc)
            if window is None or not dispatch_policy.cycle_is_drainable(window, now=now_utc):
                return 0
            if not dispatch_policy.within_backup_window(
                now=now_utc, window_start=policy.backup_window_start,
                window_end=policy.backup_window_end,
            ):
                return 0
            if _cycle_exhausted_until.get(pid) == window.end:
                return 0

            tick = dispatch_policy.drain_tick_seconds()
            # Only the LAST tick before the window closes — 2×tick let two
            # consecutive ticks both see "closing", so the first one could
            # emit INCOMPLETE with whatever was left BEFORE its own wave
            # even ran, and the once-per-cycle memo then muzzled the
            # accurate report the next (truly last) tick would have sent.
            closing = now_utc >= window.end - timedelta(seconds=tick)

            inflight_resources = int((await session.execute(
                dispatch_policy.build_inflight_resource_count_query(policy.id)
            )).scalar() or 0)
            budget = max_concurrent - inflight_resources
            if budget <= 0 and not closing:
                return 0

            # Anchor at the window start itself. Cron Job rows are
            # created by this same process seconds AFTER the fire time,
            # so `created_at >= window.start` already carries them — a
            # -60s anchor overlapped the PREVIOUS cycle's closing band,
            # mis-carrying its last wave into this new cycle and reusing
            # the old cycle's batch_id/wave numbering.
            since = window.start
            enabled_types = _enabled_type_values(policy)
            if not enabled_types:
                return 0
            rows = (await session.execute(
                dispatch_policy.build_drain_candidates_query(policy.id, since, enabled_types)
            )).all()
            if not rows:
                _cycle_exhausted_until[pid] = window.end
                return 0

            # Never dispatch a resource that is in flight under ANY job
            # (manual, preemptive, or an earlier wave still running).
            inflight_ids = {
                row[0] for row in (await session.execute(
                    dispatch_policy.build_inflight_resource_ids_query()
                )).all() if row[0] is not None
            }
            candidates, held = dispatch_policy.partition_overlap_holds(rows, inflight_ids)
            ordered = sorted(candidates, key=dispatch_policy.backup_priority_key)
            # On the closing tick, bypass the wave floor: with 3 free slots
            # and 100 remaining the floor would wait forever for a fraction
            # of the cap that this cycle no longer has time left to reach.
            # Take whatever capacity is free instead of dispatching nothing.
            wave_items = dispatch_policy.plan_wave(
                ordered, max(0, budget), max_concurrent,
                min_fraction=0.0 if closing else dispatch_policy.DRAIN_MIN_WAVE_FRACTION,
            )
            remaining = ordered[len(wave_items):]

            batch_id = (await session.execute(
                dispatch_policy.build_cycle_batch_id_query(policy.id, since)
            )).scalar()

            async def _report_incomplete_if_closing(final_batch_id):
                # Emitted AFTER the wave attempt (if any) so it reports the
                # TRUE remainder — not whatever was left before this tick's
                # own dispatch — and, when the drain itself opened the
                # cycle, references the same fresh batch_id the wave was
                # dispatched under rather than the nonexistent looked-up one.
                if (closing and (remaining or held)
                        and _cycle_incomplete_emitted.get(pid) != window.end):
                    _cycle_incomplete_emitted[pid] = window.end
                    details = dispatch_policy.cycle_incomplete_audit_details(
                        policy, remaining, held=held,
                        batch_id=final_batch_id, window_end=window.end,
                    )
                    print(f"[SCHEDULER][drain] WARNING: {details['message']}")
                    await publish_cycle_audit_event(
                        policy, "BACKUP_CYCLE_INCOMPLETE", details,
                    )

            if not wave_items:
                if candidates and budget > 0:
                    print(f"[SCHEDULER][drain] Policy '{policy.name}': "
                          f"{len(candidates)} remaining, budget {budget} "
                          f"below wave floor — waiting")
                # A fresh batch_id is not needed here — nothing was
                # dispatched, so the looked-up one (possibly None) is what
                # the cycle actually carries so far.
                await _report_incomplete_if_closing(batch_id)
                return 0

            started_cycle = batch_id is None
            if started_cycle:
                # The cron created no rows this cycle (all held, at cap, or
                # a missed tick). Open the cycle here so coverage does not
                # wait a day; in-flight ids were partitioned above and the
                # cap applies, so this is as safe as the tick itself.
                batch_id = str(uuid.uuid4())
                wave = 1
            else:
                max_wave = int((await session.execute(
                    dispatch_policy.build_cycle_max_wave_query(policy.id, since)
                )).scalar() or 0)
                wave = max(max_wave, 1) + 1

            total, queues = await _dispatch_resources(
                session, policy, wave_items,
                dispatch_batch_id=batch_id,
                deferred_resource_ids=[str(r.id) for r in remaining],
                overlap_held_resource_ids=[str(r.id) for r in held],
                wave=wave,
            )
            details = dispatch_policy.wave_audit_details(
                policy, batch_id=batch_id, wave=wave, dispatched=total,
                remaining=len(remaining), held=len(held),
                inflight_resources=inflight_resources,
                max_concurrent=max_concurrent, started_cycle=started_cycle,
            )
            print(f"[SCHEDULER][drain] {details['message']} "
                  f"queues=[{', '.join(sorted(queues)) or 'none'}]")
            await publish_cycle_audit_event(
                policy, "BACKUP_CYCLE_WAVE_DISPATCHED", details,
                outcome="SUCCESS",
            )
            # batch_id here is final (fresh uuid on the started_cycle path).
            await _report_incomplete_if_closing(batch_id)
            return total


async def drain_policy_cycles():
    """Interval tick: try every capped, scheduled, enabled policy."""
    if not dispatch_policy.drain_enabled():
        return
    try:
        async with async_session_factory() as session:
            policy_ids = [
                str(row[0]) for row in (await session.execute(
                    dispatch_policy.build_drainable_policies_query()
                )).all()
            ]
    except Exception as exc:
        print(f"[SCHEDULER][drain] could not list policies (non-fatal): {exc}")
        print(traceback.format_exc())
        return
    for pid in policy_ids:
        try:
            await drain_policy_cycle(pid)
        except Exception as exc:
            print(f"[SCHEDULER][drain] policy {pid} failed (non-fatal): {exc}")
            print(traceback.format_exc())


async def check_sla_violations():
    """
    Check for SLA violations - resources that missed their backup window
    """
    print("[SLA] Checking for SLA violations...")
    
    async with async_session_factory() as session:
        # Find resources with last_backup_at older than their SLA window
        now = datetime.utcnow()
        
        # Get all active resources with SLA policies
        resources_result = await session.execute(
            select(Resource, SlaPolicy).join(
                SlaPolicy, Resource.sla_policy_id == SlaPolicy.id, isouter=True
            ).where(
                and_(
                    Resource.status.in_([ResourceStatus.DISCOVERED, ResourceStatus.ACTIVE]),
                    Resource.sla_policy_id.isnot(None),
                    SlaPolicy.enabled == True,
                    SlaPolicy.sla_violation_alert == True
                )
            )
        )
        
        # Resources with a backup actively in flight. Used to stop
        # escalating a slow-but-progressing run to CRITICAL every 30min —
        # IT OPS NOTES (62GB SharePoint partition, 12h into a run that
        # was still inserting snapshot_items) paged continuously while
        # data protection was in fact happening. The run-overlap guard
        # makes this more common, since a skipped cycle ages
        # last_backup_at further, so the two fixes ship together.
        inflight_ids = {
            row[0] for row in (await session.execute(
                dispatch_policy.build_inflight_resource_ids_query()
            )).all() if row[0] is not None
        }

        violations = []
        for resource, policy in resources_result.all():
            if not policy:
                continue

            # A policy with no cadence has no SLA window. MANUAL policies
            # are never put on the cron (frequency_to_cron_params returns
            # None), so measuring them against a fabricated 24h default
            # makes every resource on them a permanent, unfixable
            # violation — prod ships `Manual` with sla_violation_alert on.
            if not dispatch_policy.policy_has_sla_cadence(policy.frequency):
                continue

            # Shared with the dispatch path so the two can't drift, and
            # so MONTHLY stops falling back to 24h (which reported a
            # violation every single day).
            frequency_hours = dispatch_policy.frequency_interval_hours(
                policy.frequency
            )

            in_progress = resource.id in inflight_ids
            never_backed_up = resource.last_backup_at is None
            hours_since_backup = (
                None if never_backed_up
                else (now - resource.last_backup_at).total_seconds() / 3600
            )
            severity = dispatch_policy.classify_sla_violation(
                hours_since_backup=hours_since_backup,
                frequency_hours=frequency_hours,
                never_backed_up=never_backed_up,
                backup_in_progress=in_progress,
            )
            if severity is None:
                continue

            violations.append({
                "resource_id": str(resource.id),
                "tenant_id": str(resource.tenant_id),
                "resource_type": resource.type.value,
                "sla_policy_id": str(policy.id),
                "sla_policy_name": policy.name,
                "last_backup_at": (
                    None if never_backed_up
                    else resource.last_backup_at.isoformat()
                ),
                "never_backed_up": never_backed_up,
                "backup_in_progress": in_progress,
                "hours_overdue": (
                    None if hours_since_backup is None
                    else hours_since_backup - frequency_hours
                ),
                "severity": severity,
            })

            if never_backed_up:
                age = "NEVER backed up"
            else:
                age = f"last backed up {hours_since_backup:.1f}h ago"
            suffix = " [backup in flight]" if in_progress else ""
            print(f"[SLA VIOLATION] {severity}: Resource {resource.id} "
                  f"({resource.type.value}) {age} "
                  f"(SLA: {frequency_hours}h){suffix}")


        if violations:
            print(f"[SLA] Found {len(violations)} violations")
            # Send violations to alert-service via HTTP
            await send_violations_to_alert_service(violations)
        else:
            print("[SLA] No violations detected")


# ==================== Pre-emptive Backup Triggers ====================

async def check_preemptive_backup_triggers():
    """Consume unresolved BACKUP_ANOMALY alerts emitted by backup-worker's
    per-resource anomaly detector and fire a per-resource preemptive backup
    for each one. R2.3 — single signal source, no duplicate scoring.

    Flow:
      1. backup-worker._check_snapshot_anomaly raises BACKUP_ANOMALY Alert
         after each completed snapshot (compares item_count vs rolling avg).
      2. THIS job (every 15min) picks up unresolved alerts from the last
         hour and triggers an urgent backup of just that resource.
      3. Marks each alert resolved with a note pointing to the trigger job.

    Window of 1h prevents repeatedly re-acting on the same alert if the
    preemptive backup itself takes longer than one tick to start."""
    from shared.models import Alert
    print("[PREEMPTIVE] Checking unresolved BACKUP_ANOMALY alerts...")

    async with async_session_factory() as session:
        cutoff = datetime.utcnow() - timedelta(hours=1)
        stmt = (
            select(Alert).where(and_(
                Alert.type == "BACKUP_ANOMALY",
                Alert.resolved.is_(False),
                Alert.created_at >= cutoff,
            )).order_by(Alert.created_at.asc())
        )
        alerts = (await session.execute(stmt)).scalars().all()
        if not alerts:
            print("[PREEMPTIVE] No unresolved anomaly alerts in last hour")
            return

        triggered = 0
        skipped = 0
        for alert in alerts:
            if not alert.resource_id:
                # Tenant-level alerts don't have a single resource to back up.
                # We could expand to all tenant resources but that's wasteful;
                # log + resolve so it doesn't keep getting picked up.
                alert.resolved = True
                alert.resolution_note = "no resource_id attached — skipped"
                alert.resolved_at = datetime.utcnow()
                skipped += 1
                continue

            resource = await session.get(Resource, alert.resource_id)
            if not resource:
                alert.resolved = True
                alert.resolution_note = "resource gone"
                alert.resolved_at = datetime.utcnow()
                skipped += 1
                continue

            try:
                preemptive_job_id = await trigger_preemptive_backup_for_resource(
                    session, resource, reason=f"anomaly_alert={alert.id}",
                )
                alert.resolved = True
                alert.resolution_note = f"triggered preemptive backup job {preemptive_job_id}"
                alert.resolved_at = datetime.utcnow()
                triggered += 1
            except Exception as e:
                print(f"[PREEMPTIVE] failed to trigger backup for resource {resource.id}: {e}")

        await session.commit()
        print(f"[PREEMPTIVE] triggered={triggered}, skipped={skipped}, total_alerts={len(alerts)}")


async def trigger_preemptive_backup_for_resource(
    session: AsyncSession, resource: Resource, reason: str,
) -> str:
    """Per-resource preemptive backup. Publishes a single-resource job to
    backup.urgent at priority 1 — jumps the queue ahead of scheduled work.
    Returns the job ID for audit linkage."""
    job_id = uuid.uuid4()
    job = Job(
        id=job_id,
        type=JobType.BACKUP,
        tenant_id=resource.tenant_id,
        batch_resource_ids=[resource.id],
        status=JobStatus.QUEUED,
        priority=1,
        spec={
            "triggered_by": "PREEMPTIVE",
            "reason": reason,
            "resource_type": resource.type.value if hasattr(resource.type, "value") else str(resource.type),
            "preemptive": True,
        },
    )
    session.add(job)
    payload = {
        "jobId": str(job_id),
        "resourceId": str(resource.id),
        "workload": resource.type.value if hasattr(resource.type, "value") else str(resource.type),
        "triggeredBy": "PREEMPTIVE",
        "reason": reason,
    }
    await message_bus.publish("backup.urgent", payload, priority=1)
    print(f"[PREEMPTIVE] queued backup for resource={resource.display_name} ({resource.id}), job={job_id}, reason={reason}")
    await emit_backup_triggered(
        job=job, resource=resource,
        trigger_label="PREEMPTIVE",
        actor_type="SYSTEM",
        full_backup=False,
        extra_details={"reason": reason, "preemptive": True},
    )
    return str(job_id)


async def trigger_preemptive_backup(session: AsyncSession, tenant: Tenant, reason: str):
    """Trigger immediate backup for all active resources in a tenant"""
    # Get all active resources for this tenant
    resources_result = await session.execute(
        select(Resource).where(
            and_(
                Resource.tenant_id == tenant.id,
                Resource.status.in_([ResourceStatus.DISCOVERED, ResourceStatus.ACTIVE]),
                Resource.sla_policy_id.isnot(None)
            )
        )
    )
    resources = resources_result.scalars().all()

    if not resources:
        return

    print(f"[PREEMPTIVE] Triggering backup for {len(resources)} resources in tenant {tenant.display_name}")

    # Group by resource type
    groups: Dict[str, List[Resource]] = {}
    for resource in resources:
        rtype = resource.type.value
        if rtype not in groups:
            groups[rtype] = []
        groups[rtype].append(resource)

    # Dispatch backup jobs
    for rtype, rlist in groups.items():
        resource_ids = [str(r.id) for r in rlist]

        # Create job record
        job_id = uuid.uuid4()
        job = Job(
            id=job_id,
            type=JobType.BACKUP,
            tenant_id=tenant.id,
            batch_resource_ids=[uuid.UUID(rid) for rid in resource_ids],
            status=JobStatus.QUEUED,
            priority=1,  # Urgent
            spec={
                "triggered_by": "PREEMPTIVE",
                "reason": reason,
                "resource_type": rtype,
            }
        )
        session.add(job)

        # Publish to urgent queue
        message = create_mass_backup_message(
            job_id=str(job_id),
            tenant_id=str(tenant.external_tenant_id),
            resource_type=rtype,
            resource_ids=resource_ids,
            sla_policy_id=None,
            full_backup=True,  # Force full backup for preemptive
        )
        message["triggeredBy"] = "PREEMPTIVE"
        message["reason"] = reason

        await message_bus.publish("backup.urgent", message, priority=1)

        await emit_backup_triggered(
            job=job, resource=None, tenant=tenant,
            trigger_label="PREEMPTIVE",
            actor_type="SYSTEM",
            full_backup=True,
            batch_resource_count=len(resource_ids),
            extra_details={
                "reason": reason,
                "preemptive": True,
                "resource_type": rtype,
            },
        )

    await session.commit()


# ==================== M365 Audit Log Ingestion ====================

async def ingest_m365_audit_logs():
    """
    Periodically pull Microsoft 365 audit logs from Graph API
    and store them in the audit_events table.
    Pulls both directory audit logs and sign-in logs for all active tenants.
    """
    print("[AUDIT_INGEST] Starting M365 audit log ingestion...")

    async with async_session_factory() as session:
        # Get all active M365 tenants
        tenants_result = await session.execute(
            select(Tenant).where(
                and_(
                    Tenant.status == TenantStatus.ACTIVE,
                    Tenant.type == TenantType.M365,
                    Tenant.client_id.isnot(None),
                    Tenant.external_tenant_id.isnot(None),
                )
            )
        )
        tenants = tenants_result.scalars().all()

    ingested_total = 0
    audit_service_url = settings.AUDIT_SERVICE_URL

    async with httpx.AsyncClient(timeout=60.0) as client:
        for tenant in tenants:
            for log_type in ["directory", "signin"]:
                try:
                    # Pull last 1 day of logs (running hourly, so this catches any gaps)
                    resp = await client.post(
                        f"{audit_service_url}/api/v1/audit/ingest/graph/{tenant.id}",
                        params={"days": 1, "log_type": log_type},
                    )
                    if resp.status_code == 200:
                        result = resp.json()
                        count = result.get("ingested", 0)
                        ingested_total += count
                        if count > 0:
                            print(f"[AUDIT_INGEST] Ingested {count} {log_type} logs for tenant {tenant.display_name}")
                    else:
                        print(f"[AUDIT_INGEST] Failed to ingest {log_type} for tenant {tenant.display_name}: {resp.status_code}")
                except Exception as e:
                    print(f"[AUDIT_INGEST] Error ingesting {log_type} for tenant {tenant.display_name}: {e}")

    print(f"[AUDIT_INGEST] Completed. Total ingested: {ingested_total} events")


# ==================== Scheduled Reporting ====================

async def send_daily_backup_report():
    """Trigger daily backup report generation via report-service"""
    print("[REPORT] Triggering daily backup report...")
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{settings.REPORT_SERVICE_URL}/api/v1/reports/generate",
                # report-service gates this route on a user JWT or the
                # internal key; the scheduler has no user session.
                headers=internal_audit_headers(),
                json={"report_type": "DAILY"}
                )
            if response.status_code == 200:
                print(f"[REPORT] Daily report triggered successfully")
            else:
                print(f"[REPORT] Failed to trigger daily report: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"[REPORT] Error triggering daily report: {e}")


async def send_weekly_summary_report():
    """Trigger weekly backup report generation via report-service"""
    print("[REPORT] Triggering weekly summary report...")
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{settings.REPORT_SERVICE_URL}/api/v1/reports/generate",
                headers=internal_audit_headers(),
                json={"report_type": "WEEKLY"}
            )
            if response.status_code == 200:
                print(f"[REPORT] Weekly report triggered successfully")
            else:
                print(f"[REPORT] Failed to trigger weekly report: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"[REPORT] Error triggering weekly report: {e}")


async def send_violations_to_alert_service(violations: List[Dict]):
    """Send SLA violations to alert-service.

    The try/except is per-violation on purpose: it used to wrap the whole
    loop, so one bad payload or one 500 from alert-service silently
    dropped every remaining violation in the batch.
    """
    if not violations:
        return
    sent = failed = 0
    # alert-service gates POST /api/v1/alerts on the internal-services key
    # (or an admin token) — same header every other service-to-service
    # call carries.
    headers = (
        {"X-Internal-Api-Key": settings.INTERNAL_API_KEY}
        if settings.INTERNAL_API_KEY else {}
    )
    async with httpx.AsyncClient(timeout=10.0) as client:
        for violation in violations:
            try:
                resp = await client.post(f"{settings.ALERT_SERVICE_URL}/api/v1/alerts", json={
                    "type": "SLA_VIOLATION",
                    "severity": violation.get("severity", "WARNING"),
                    "message": dispatch_policy.sla_alert_message(violation),
                    "tenant_id": violation.get("tenant_id"),
                    "resource_id": violation.get("resource_id"),
                    "resource_type": violation.get("resource_type"),
                    "details": violation,
                }, headers=headers)
                # httpx does not raise on 4xx/5xx. Until this check every
                # response counted as "sent" — including the 405 that
                # alert-service answered to every one of these POSTs (it
                # had no POST route), so 0 SLA_VIOLATION alerts were ever
                # stored while this function reported nothing wrong.
                if resp.status_code >= 400:
                    failed += 1
                    print(f"[SLA] alert POST failed status={resp.status_code} "
                          f"body={resp.text[:200]}")
                    continue
                sent += 1
            except Exception as e:
                failed += 1
                print(f"[SLA] Failed to send violation for resource "
                      f"{violation.get('resource_id')}: {e}")
    if failed:
        print(f"[SLA] Alert delivery: {sent} sent, {failed} failed")


async def reconcile_pending_discovery():
    """
    Reconciler job: runs every 5 minutes.
    Finds tenants stuck in PENDING_DISCOVERY for >10 minutes and re-enqueues discovery.
    """
    from shared.message_bus import message_bus
    from shared.models import TenantStatus

    try:
        cutoff = datetime.utcnow() - timedelta(minutes=10)
        async with async_session_factory() as session:
            stmt = select(Tenant).where(
                Tenant.status == TenantStatus.PENDING_DISCOVERY,
                Tenant.updated_at < cutoff,
            )
            result = await session.execute(stmt)
            pending_tenants = result.scalars().all()

            if not pending_tenants:
                return

            for tenant in pending_tenants:
                try:
                    print(f"[RECONCILER] Re-enqueueing discovery for tenant {tenant.id} ({tenant.display_name})")
                    if not message_bus.connection:
                        await message_bus.connect()
                    await message_bus.publish("discovery.m365", {
                        "jobId": str(uuid.uuid4()),
                        "tenantId": str(tenant.id),
                        "externalTenantId": tenant.external_tenant_id,
                        # Per-user OneDrive is Tier 2 (USER_ONEDRIVE under each
                        # ENTRA_USER) — listing "onedrive" here would create a
                        # duplicate Tier 1 ONEDRIVE row per user and double the
                        # backup walk. The Tier 1 reconciler only needs the
                        # tenant-level container resources.
                        "discoveryScope": ["users", "groups", "mailboxes", "shared_mailboxes",
                                           "sharepoint", "teams"],
                        "triggeredBy": "RECONCILER",
                        "triggeredAt": datetime.utcnow().isoformat(),
                    }, priority=5)
                    tenant.status = TenantStatus.DISCOVERING
                    await session.commit()
                    print(f"[RECONCILER] Discovery re-enqueued for tenant {tenant.id}")
                except Exception as e:
                    print(f"[RECONCILER] Failed to re-enqueue discovery for tenant {tenant.id}: {e}")
                    await session.rollback()
    except Exception as e:
        print(f"[RECONCILER] Reconciler error: {e}")


async def start_reconciler_loop():
    """Start reconciler background task (runs every 5 minutes)."""
    async def _reconciler():
        while True:
            await asyncio.sleep(300)  # 5 minutes
            try:
                await reconcile_pending_discovery()
            except Exception as e:
                print(f"[RECONCILER] Reconciler loop error: {e}")

    asyncio.create_task(_reconciler())


# Sweep interval for the distributed-reconciliation orphan sweeper.
# 60 s matches the lease TTL so a worker that misses one heartbeat
# tick already has its lease seen as expiring at roughly the same
# moment the sweeper runs — minimises detection latency.
ORPHAN_SWEEP_INTERVAL_S = int(os.environ.get("ORPHAN_SWEEP_INTERVAL_S", "60"))


async def start_orphan_sweeper_loop():
    """Distributed-reconciliation orphan sweeper.

    Each tick:
      1. Calls ``shared.reconciler.sweep_orphans`` to bottom-up
         finalize stuck partitions / snapshots / jobs whose owning
         worker is dead.
      2. Re-publishes AMQP messages for any partitions the sweep
         re-queued (snapshot_partitions.status flipped back to
         QUEUED).
      3. After every sweep cycle, calls the existing batch finalizer
         (``_finalize_batch_if_complete``) for any backup_batches row
         that the rollup now sees as fully terminal — so the Activity
         row closes on the same tick instead of waiting for the next
         operator click.

    Idempotent under multiple scheduler replicas: every state write
    in sweep_orphans uses WHERE clauses on the expected status so
    races resolve cleanly.
    """
    async def _sweep_tick():
        from shared.reconciler import sweep_orphans, republish_partition_messages
        from shared.batch_rollup import _finalize_batch_if_complete

        try:
            async with async_session_factory() as session:
                stats = await sweep_orphans(session)
            if stats.requeue_payloads:
                try:
                    await republish_partition_messages(stats, message_bus=message_bus)
                except Exception as exc:
                    print(f"[ORPHAN_SWEEPER] partition republish failed: {exc}")

            # If we just finalised any job/snapshot, give the batch
            # finalizer a chance to close any backup_batches row that
            # was waiting on them. Cheap: only runs when we touched
            # something.
            if stats.snapshots_finalized or stats.jobs_finalized:
                try:
                    async with async_session_factory() as session:
                        rows = (await session.execute(text("""
                            SELECT id FROM backup_batches
                             WHERE status = 'IN_PROGRESS'
                             ORDER BY created_at DESC
                             LIMIT 50
                        """))).all()
                        for row in rows:
                            await _finalize_batch_if_complete(row.id, session)
                except Exception as exc:
                    print(f"[ORPHAN_SWEEPER] batch finalize cascade failed: {exc}")
        except Exception as exc:
            print(f"[ORPHAN_SWEEPER] tick failed: {exc}")

    async def _loop():
        # Stagger 5 s after boot so we don't race the schema migration
        # the first time this code lands on a fresh DB.
        await asyncio.sleep(5)
        while True:
            await _sweep_tick()
            await asyncio.sleep(ORPHAN_SWEEP_INTERVAL_S)

    asyncio.create_task(_loop())
    print(f"[ORPHAN_SWEEPER] loop started — interval={ORPHAN_SWEEP_INTERVAL_S}s")


# ── Automatic retry of failed backup jobs (Configuration tab, 2026-08-18) ──
#
# Operator ask: when a backup job fails, wait N minutes and retry it, up to
# M times. M (job_retry_max, default 3, 0 = disabled) and N
# (job_retry_delay_minutes, default 5) live in the app_settings singleton and
# are edited on the Configuration tab; the loop re-reads them every pass so a
# change takes effect without a restart.
#
# Design constraints (each earned the hard way):
#   * Each retry is a FRESH single-resource Job row + a fresh AMQP publish —
#     the exact shape of the proven POST /scheduler/resource/{id} path. We
#     never flip an existing FAILED job back to QUEUED: the 2026-08-07 review
#     of that shape found re-queued bulk jobs are silently dropped by the
#     bulk_fanout_seen dedup and then sit non-terminal forever.
#   * Retries are counted per FAILURE CHAIN via spec->>'retry_root' (the
#     original failed job's id), so "3 retries" means 3 per original failure
#     — a later scheduled cycle's new failure starts a new chain.
#   * Per-pass dispatch cap + only the NEWEST failure per resource + skip
#     when anything is already in flight: retries multiply load, and the
#     2026-08-18 BufferMapping convoy was a retry-amplified write pile-up.
#   * Only failures newer than JOB_RETRY_LOOKBACK_HOURS are eligible, so
#     enabling the feature can never resurrect a week of stale failures.
JOB_RETRY_SWEEP_INTERVAL_S = int(os.environ.get("JOB_RETRY_SWEEP_INTERVAL_S", "60"))
JOB_RETRY_LOOKBACK_HOURS = int(os.environ.get("JOB_RETRY_LOOKBACK_HOURS", "12"))
JOB_RETRY_DISPATCH_CAP = int(os.environ.get("JOB_RETRY_DISPATCH_CAP", "5"))


async def _job_retry_tick() -> int:
    """One sweep: dispatch retry jobs for recently-failed resources.
    Returns the number of retries dispatched."""
    from shared.app_settings import get_app_settings

    async with async_session_factory() as session:
        settings_row = await get_app_settings(session)
        await session.commit()
    retry_max = int(settings_row.get("job_retry_max") or 0)
    delay_min = int(settings_row.get("job_retry_delay_minutes") or 5)
    if retry_max <= 0:
        return 0
    # The eligibility window must always cover the configured delay —
    # a 24h delay against a fixed 12h lookback would make every candidate
    # age out before it becomes eligible (review finding, 2026-08-18).
    lookback_min = max(JOB_RETRY_LOOKBACK_HOURS * 60, delay_min + 120)

    async with async_session_factory() as session:
        candidates = (await session.execute(text("""
            SELECT DISTINCT ON (s.resource_id)
                   s.id AS snapshot_id, s.resource_id, s.job_id,
                   s.completed_at, r.tenant_id
              FROM snapshots s
              JOIN resources r ON r.id = s.resource_id
             WHERE s.status = 'FAILED'
               AND s.completed_at > NOW() - make_interval(mins => :lookback)
               AND s.completed_at <= NOW() - make_interval(mins => :delay)
               -- a user cancel is final, not a failure to retry
               AND coalesce(s.extra_data->>'cancelled_at', '') = ''
               -- a failure the worker marked deterministic (e.g. the
               -- archive coverage gate) re-fails identically — retrying
               -- it is pure load. Absent flag = retryable.
               AND coalesce((s.extra_data->>'retryable')::bool, true)
               -- match the dispatch paths' status filter (DISCOVERED
               -- resources are backed up too — 'ACTIVE'-only silently
               -- excluded most of the fleet)
               AND r.status IN ('DISCOVERED', 'ACTIVE')
               -- a QUEUED/RUNNING job for this resource IS in-flight even
               -- though it has produced no snapshot row yet — without this
               -- a queued-behind-backlog retry re-dispatched every 60s
               -- until the whole budget was burned (review finding)
               AND NOT EXISTS (
                     SELECT 1 FROM jobs jf
                      WHERE jf.resource_id = s.resource_id
                        AND jf.status IN ('QUEUED','PENDING','RUNNING','RETRYING')
                   )
               -- nothing in flight and nothing newer for this resource:
               -- a newer run (even another failure) supersedes this one —
               -- the newer failure gets its own chain. A concurrent-skip
               -- no-op (skipped_concurrent) is NOT a real capture and
               -- must not suppress the retry.
               AND NOT EXISTS (
                     SELECT 1 FROM snapshots n
                      WHERE n.resource_id = s.resource_id
                        AND n.id <> s.id
                        AND n.created_at > s.created_at
                        AND (n.status = 'IN_PROGRESS'
                             OR (n.status IN ('COMPLETED','PARTIAL','FAILED')
                                 AND NOT coalesce(
                                       (n.extra_data->>'skipped_concurrent')::bool,
                                       false)))
                   )
             ORDER BY s.resource_id, s.completed_at DESC
             LIMIT 50
        """), {"lookback": lookback_min, "delay": delay_min})).all()

        dispatched = 0
        for cand in candidates:
            if dispatched >= JOB_RETRY_DISPATCH_CAP:
                print(f"[JOB_RETRY] pass cap {JOB_RETRY_DISPATCH_CAP} reached — "
                      f"{len(candidates) - dispatched} candidate(s) wait for the next pass")
                break

            # Resolve the failure chain root. A failed retry's snapshot
            # chains back via its AUTO_RETRY job's spec; any OTHER failure
            # roots a fresh chain at its own SNAPSHOT id. The root must be
            # per-resource: scheduled cycles put up to 1000 resources on
            # ONE bulk job id, so rooting at the job made retry_max a
            # SHARED budget across the whole batch (critical review
            # finding) — snapshot ids are per-resource per-run by
            # construction.
            root = str(cand.snapshot_id)
            failed_job = None
            if cand.job_id is not None:
                failed_job = await session.get(Job, cand.job_id)
            if failed_job is not None:
                spec = failed_job.spec or {}
                if spec.get("triggered_by") == "AUTO_RETRY" and spec.get("retry_root"):
                    root = str(spec["retry_root"])

            used = (await session.execute(text("""
                SELECT count(*) FROM jobs
                 WHERE spec->>'retry_root' = :root
                   AND spec->>'triggered_by' = 'AUTO_RETRY'
                   AND resource_id = CAST(:rid AS uuid)
                   -- a retry whose AMQP publish failed never ran; it is
                   -- marked FAILED/publish_failed and must not burn budget
                   AND coalesce(result->>'publish_failed', '') <> 'true'
            """), {"root": root, "rid": str(cand.resource_id)})).scalar() or 0
            if used >= retry_max:
                continue
            # An already-queued retry for this chain that hasn't produced a
            # snapshot yet also shows up in `used`, so double-dispatch on
            # consecutive passes is bounded by the same counter.

            resource = await session.get(Resource, cand.resource_id)
            if resource is None:
                continue

            job_id = uuid.uuid4()
            job = Job(
                id=job_id,
                type=JobType.BACKUP,
                tenant_id=cand.tenant_id,
                resource_id=cand.resource_id,
                status=JobStatus.QUEUED,
                priority=2,
                spec={
                    "full_backup": False,
                    "triggered_by": "AUTO_RETRY",
                    "snapshot_label": "auto-retry",
                    "retry_root": root,
                    "retry_attempt": used + 1,
                    "retry_of_snapshot": str(cand.snapshot_id),
                },
            )
            session.add(job)
            await session.commit()

            try:
                await message_bus.publish(
                    "backup.urgent",
                    {
                        "jobId": str(job_id),
                        "resourceId": str(cand.resource_id),
                        "tenantId": str(cand.tenant_id),
                        "type": "INCREMENTAL",
                        "priority": 2,
                        "triggeredBy": "AUTO_RETRY",
                        "snapshotLabel": "auto-retry",
                        "forceFullBackup": False,
                    },
                    priority=2,
                )
            except Exception as _pe:
                # Publish-after-commit dual-write: without this the row
                # sits QUEUED forever (no reaper covers zero-snapshot
                # single-resource jobs) while still counting against the
                # chain budget. Mark it failed-at-publish; the budget
                # count excludes that marker, and the outbox reconciler
                # (single-resource branch) re-sends any that slipped
                # through a crash between commit and here.
                print(f"[JOB_RETRY] publish failed for job {job_id} "
                      f"({type(_pe).__name__}: {_pe}) — marking publish_failed")
                async with async_session_factory() as _fs:
                    await _fs.execute(text(
                        "UPDATE jobs SET status = 'FAILED', "
                        "       completed_at = NOW(), "
                        # jobs.result is json (not jsonb): concat in jsonb,
                        # cast back on assignment
                        "       result = (coalesce(result::jsonb, '{}'::jsonb) "
                        "                || '{\"publish_failed\": \"true\"}'::jsonb)::json "
                        " WHERE id = CAST(:jid AS uuid) AND status = 'QUEUED'"
                    ), {"jid": str(job_id)})
                    await _fs.commit()
                continue
            try:
                await emit_backup_triggered(
                    job=job, resource=resource,
                    trigger_label="AUTO_RETRY", actor_type="SYSTEM",
                    extra_details={"retry_attempt": used + 1,
                                   "retry_root": root,
                                   "retry_of_snapshot": str(cand.snapshot_id)},
                )
            except Exception as _ae:
                print(f"[JOB_RETRY] audit emit failed (non-fatal): {_ae}")
            dispatched += 1
            print(f"[JOB_RETRY] retry {used + 1}/{retry_max} dispatched for "
                  f"{resource.display_name or cand.resource_id} "
                  f"(failed snapshot {cand.snapshot_id}, delay {delay_min}m, "
                  f"job {job_id})")
        return dispatched


async def start_job_retry_loop():
    async def _loop():
        # Stagger past boot-time schema sync and the orphan sweeper's
        # first pass.
        await asyncio.sleep(20)
        while True:
            try:
                await _job_retry_tick()
            except Exception as exc:
                print(f"[JOB_RETRY] tick failed: {exc}")
            await asyncio.sleep(JOB_RETRY_SWEEP_INTERVAL_S)

    asyncio.create_task(_loop())
    print(f"[JOB_RETRY] loop started — interval={JOB_RETRY_SWEEP_INTERVAL_S}s, "
          f"lookback={JOB_RETRY_LOOKBACK_HOURS}h, "
          f"cap={JOB_RETRY_DISPATCH_CAP}/pass")


# ── R3.2: Backup verification cron ──

# Default sample size — small enough that a daily run is cheap on storage
# (each verify = one blob download), large enough that a corrupt backup gets
# noticed within ~weeks even if it lives in a low-priority tenant.
BACKUP_VERIFY_SAMPLE_SIZE = int(os.environ.get("BACKUP_VERIFY_SAMPLE_SIZE", "50"))
BACKUP_VERIFY_LOOKBACK_HOURS = int(os.environ.get("BACKUP_VERIFY_LOOKBACK_HOURS", "24"))


async def run_backup_verification():
    """Sample-and-verify recent snapshots: download a random SnapshotItem's
    blob and compare its SHA256 against the captured `content_checksum`.
    Mismatches raise a `BACKUP_VERIFICATION_FAILED` Alert.

    afi claims data is "fingerprinted for integrity verification" but doesn't
    document when it's actually checked. We do it daily on a random sample —
    cheap enough that we can run it on every deploy, large enough that
    silent corruption gets flagged within weeks of occurring."""
    import hashlib as _hashlib
    import random
    from shared.models import Snapshot, SnapshotStatus, SnapshotItem, Resource, Alert
    from shared.azure_storage import azure_storage_manager, workload_candidates_for_resource_type

    print("[VERIFY] === START: backup integrity sample ===")
    cutoff = datetime.utcnow() - timedelta(hours=BACKUP_VERIFY_LOOKBACK_HOURS)
    checked = 0
    passed = 0
    failed = 0
    no_blob = 0
    no_checksum = 0

    try:
        async with async_session_factory() as session:
            snap_stmt = (
                select(Snapshot.id, Snapshot.resource_id)
                .where(and_(
                    Snapshot.status == SnapshotStatus.COMPLETED,
                    Snapshot.completed_at >= cutoff,
                    Snapshot.item_count > 0,
                ))
                .order_by(func.random())
                .limit(BACKUP_VERIFY_SAMPLE_SIZE)
            )
            snap_rows = (await session.execute(snap_stmt)).all()
            if not snap_rows:
                print("[VERIFY] No completed snapshots in lookback window — nothing to sample")
                return

            for snap_id, resource_id in snap_rows:
                # Pick one random item from this snapshot to verify
                item_stmt = (
                    select(SnapshotItem)
                    .where(and_(
                        SnapshotItem.snapshot_id == snap_id,
                        SnapshotItem.blob_path.isnot(None),
                    ))
                    .order_by(func.random())
                    .limit(1)
                )
                item = (await session.execute(item_stmt)).scalars().first()
                if not item:
                    no_blob += 1
                    continue
                if not item.content_checksum:
                    no_checksum += 1
                    continue

                resource = await session.get(Resource, resource_id)
                if not resource:
                    continue

                # Find the right container for this item — same workload mapping
                # the backup worker uses on write.
                rtype = resource.type.value if hasattr(resource.type, "value") else str(resource.type)
                workloads = workload_candidates_for_resource_type(rtype) or ("entra",)
                shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(resource.tenant_id))

                content: bytes | None = None
                used_workload: str | None = None
                for wl in workloads:
                    container = azure_storage_manager.get_container_name(str(resource.tenant_id), wl)
                    try:
                        # shard.download_blob returns None on 404 (ResourceNotFound)
                        # and only raises on auth/network errors — try the next
                        # candidate workload either way.
                        content = await shard.download_blob(container, item.blob_path)
                        if content is not None:
                            used_workload = wl
                            break
                    except Exception:
                        continue

                checked += 1
                if content is None:
                    print(f"[VERIFY] FAIL blob unreachable: snapshot={snap_id} item={item.id} blob={item.blob_path}")
                    failed += 1
                    await _raise_verification_alert(
                        session, resource, snap_id, item, reason="blob_unreachable",
                        expected=item.content_checksum, actual=None,
                    )
                    continue

                actual = _hashlib.sha256(content).hexdigest()
                if actual == item.content_checksum:
                    passed += 1
                    continue

                print(f"[VERIFY] FAIL checksum mismatch: snapshot={snap_id} item={item.id} expected={item.content_checksum[:12]}.. actual={actual[:12]}.. workload={used_workload}")
                failed += 1
                await _raise_verification_alert(
                    session, resource, snap_id, item, reason="checksum_mismatch",
                    expected=item.content_checksum, actual=actual,
                )

            await session.commit()
        print(
            f"[VERIFY] === COMPLETE: sampled={len(snap_rows)} checked={checked} "
            f"passed={passed} failed={failed} no_blob={no_blob} no_checksum={no_checksum} ==="
        )
    except Exception as e:
        import traceback as _tb
        print(f"[VERIFY] FATAL: {e}\n{_tb.format_exc()}")


async def _raise_verification_alert(
    session, resource, snapshot_id, item, reason: str,
    expected: str | None, actual: str | None,
) -> None:
    """Persist a BACKUP_VERIFICATION_FAILED alert. Severity HIGH because a
    silent corruption in storage means the next restore will produce wrong data."""
    from shared.models import Alert
    session.add(Alert(
        tenant_id=resource.tenant_id if resource else None,
        type="BACKUP_VERIFICATION_FAILED",
        severity="HIGH",
        message=(
            f"Snapshot integrity check failed ({reason}) for resource "
            f"{resource.display_name if resource else snapshot_id}. "
            f"Stored blob no longer matches the SHA256 captured at backup time — "
            f"restore from this snapshot may produce corrupted data."
        ),
        resource_id=resource.id if resource else None,
        resource_type=resource.type.value if resource and hasattr(resource.type, "value") else None,
        resource_name=resource.display_name if resource else None,
        triggered_by="verification-cron",
        details={
            "snapshot_id": str(snapshot_id),
            "snapshot_item_id": str(item.id),
            "blob_path": item.blob_path,
            "reason": reason,
            "expected_sha256": expected,
            "actual_sha256": actual,
        },
    ))


# ── Round 1.5: DLQ consumer + failed-snapshot retry ──

def backup_dlq_queue_names() -> List[str]:
    """Every backup DLQ RabbitMQ actually declares.

    Fixed 2026-08-11: this consumer watched FOUR queues — the priority lanes
    — while MessageBus._declare_queue creates a `<queue>.dlq` for every
    declared queue, including the heavy lane and all six partition lanes.
    Anything dead-lettered off a partition queue was therefore invisible:
    poison USER_CHATS shard messages had been parked in
    backup.chats_partition.dlq with ZERO consumers and no alert, which is a
    lost slice of a user's chat backup that nothing would ever have reported.

    Derived, not hand-listed, wherever a list already exists: the six
    partition queues come from shared.reconciler._PARTITION_QUEUE_BY_TYPE
    (the single mirror of the publish-side routing, with a CI test that fails
    if a partition type the workers persist is missing from it), and the
    heavy / default lanes come from settings. Only the four priority lanes are
    spelled out here, because MessageBus.connect() declares them as inline
    literals with no constant to import — if that ever changes, read them
    from there instead.
    """
    from shared.reconciler import _PARTITION_QUEUE_BY_TYPE

    queues = [
        "backup.urgent", "backup.high", "backup.normal", "backup.low",
        settings.BACKUP_WORKER_QUEUE,
        settings.BACKUP_HEAVY_QUEUE,
    ]
    queues += sorted(_PARTITION_QUEUE_BY_TYPE.values())
    # dict.fromkeys de-dupes while keeping order — BACKUP_WORKER_QUEUE
    # defaults to backup.normal, which is already in the list.
    return [f"{q}.dlq" for q in dict.fromkeys(queues)]


async def consume_backup_dlq():
    """Long-running consumer for backup.*.dlq queues — every poison message
    becomes an Alert so ops can investigate. Without this, dead-lettered
    messages sit in RabbitMQ forever with no signal to operators."""
    from shared.models import Alert
    dlq_queues = backup_dlq_queue_names()

    # Wait for message_bus to be ready (startup ordering)
    for _ in range(30):
        if message_bus.channel:
            break
        await asyncio.sleep(1)
    if not message_bus.channel:
        print("[DLQ] message bus not connected — DLQ consumer giving up")
        return

    async def consume_one(queue_name: str):
        try:
            queue = await message_bus.channel.get_queue(queue_name)
        except Exception as exc:
            print(f"[DLQ] cannot bind to {queue_name}: {exc}")
            return
        print(f"[DLQ] consuming from {queue_name}")
        async with queue.iterator() as it:
            async for msg in it:
                # Log the RAW body BEFORE anything that can fail. Every path
                # below ends in ack(), so this print is the only guaranteed
                # record of the message — without it a poison message is
                # destroyed silently. Root cause of that: `json` was never
                # imported at module scope, so json.loads raised NameError
                # on EVERY message, the blanket except acked it, and the
                # alert was never written. The `alerts` table has 0 rows in
                # its entire history for that reason.
                raw = msg.body.decode("utf-8", errors="replace")
                print(
                    f"[DLQ] raw message from {queue_name} "
                    f"message_id={getattr(msg, 'message_id', None)} "
                    f"body={raw[:4000]}"
                )
                try:
                    # Parse in its own guard. A malformed body must still
                    # raise an Alert (carrying the raw text) rather than
                    # skipping straight to the swallow-and-ack path.
                    parse_error: Optional[str] = None
                    try:
                        body = json.loads(raw)
                        if not isinstance(body, dict):
                            raise ValueError(f"expected object, got {type(body).__name__}")
                    except Exception as pe:
                        body = {}
                        parse_error = f"{type(pe).__name__}: {pe}"

                    job_id = body.get("jobId")
                    resource_id = body.get("resourceId") or (body.get("resourceIds") or [None])[0]
                    # create_backup_message (shared/message_bus.py) emits
                    # "type" (FULL/INCREMENTAL) — there has never been a
                    # "workload" key, so the old read was always None and
                    # every alert said "(None)".
                    workload = body.get("type")
                    # x-delivery-count is a QUORUM-queue header; every queue
                    # here is classic, so it is always absent. Kept only so
                    # the alert text stays stable if queues are migrated.
                    delivery_count = (msg.headers or {}).get("x-delivery-count", "?")
                    print(
                        f"[DLQ] poison message in {queue_name}: job={job_id} "
                        f"resource={resource_id} type={workload} "
                        f"delivery_count={delivery_count} parse_error={parse_error}"
                    )
                    async with async_session_factory() as session:
                        # tenantId is on the message itself; only fall back to
                        # a Resource lookup when it is absent or unparseable.
                        tenant_id = None
                        _raw_tid = body.get("tenantId")
                        if _raw_tid:
                            try:
                                tenant_id = uuid.UUID(str(_raw_tid))
                            except Exception:
                                tenant_id = None
                        if tenant_id is None and resource_id:
                            try:
                                from shared.models import Resource
                                r = await session.get(Resource, uuid.UUID(resource_id))
                                tenant_id = r.tenant_id if r else None
                            except Exception:
                                pass
                        # Never let a malformed id abort the alert — that
                        # would drop us into the ack-anyway path and destroy
                        # the message. An unparseable resource id is worth an
                        # alert WITHOUT the FK, not no alert at all.
                        _res_fk = None
                        if resource_id:
                            try:
                                _res_fk = uuid.UUID(str(resource_id))
                            except Exception:
                                _res_fk = None
                        session.add(Alert(
                            tenant_id=tenant_id,
                            type="BACKUP_DLQ",
                            severity="HIGH",
                            message=(
                                f"Backup message dead-lettered on {queue_name} "
                                f"(delivery_count={delivery_count}). "
                                f"Job {job_id} for resource {resource_id} ({workload}) "
                                f"is stuck — investigate."
                                + (f" UNPARSEABLE BODY: {parse_error}" if parse_error else "")
                            ),
                            resource_id=_res_fk,
                            triggered_by="dlq-consumer",
                            details={
                                "queue": queue_name,
                                "delivery_count": delivery_count,
                                "job_id": job_id,
                                "resource_id": resource_id,
                                "workload": workload,
                                "parse_error": parse_error,
                                # Truncated raw body, so the message survives
                                # in the DB and not only in pod logs.
                                "raw_body": raw[:8000],
                            },
                        ))
                        await session.commit()
                    await msg.ack()
                except Exception as e:
                    # Ack anyway — this is a DLQ, so rejecting would either
                    # re-queue in a tight loop or (with no onward route) drop
                    # the message regardless. The raw body was already
                    # printed above, so acking here is recoverable from logs
                    # rather than silent. Print the body again with the
                    # failure so both land in one place.
                    print(
                        f"[DLQ] failed to alert on poison message from "
                        f"{queue_name}: {type(e).__name__}: {e} — "
                        f"UNRECORDED body={raw[:4000]}"
                    )
                    try:
                        await msg.ack()
                    except Exception:
                        pass

    await asyncio.gather(*[consume_one(q) for q in dlq_queues], return_exceptions=True)


def _not_skipped_concurrent():
    """SQL predicate: this snapshot row is not a concurrency placeholder.

    backup-worker's cross-job concurrency guard creates a snapshot row and
    immediately marks it COMPLETED with item_count=0 and
    extra_data.skipped_concurrent=true, purely so the owning job's
    FANOUT/FINALIZE counters balance (see SKIP_CONCURRENT in
    workers/backup-worker/main.py). It captured NOTHING, so it must never be
    read as evidence that a backup succeeded. Mirrors
    services/snapshot-service/main.py::_real_restore_point().

    json_extract_path_text is NULL-safe for absent keys and works on both
    json and jsonb (extra_data is json); COALESCE keeps rows that predate
    the flag — without it, NOT IN over NULL yields NULL and would filter
    out every real snapshot. Compared as text so a non-boolean value can't
    raise the way an ->>::boolean cast would.
    """
    return func.coalesce(
        func.json_extract_path_text(Snapshot.extra_data, "skipped_concurrent"),
        "false",
    ).notin_(("true", "t", "1"))


async def retry_failed_snapshots():
    """Daily: find FAILED snapshots between 1h and 25h old and re-queue ONE
    backup attempt. Marker on snapshot.extra_data prevents an infinite retry
    loop on the same failed snapshot. Skips resources whose next scheduled
    backup will likely cover them anyway."""
    from shared.models import Snapshot, SnapshotStatus, Resource
    from sqlalchemy import select, and_

    print("[RETRY] === START: failed-snapshot retry sweep ===")
    requeued = 0
    skipped_marked = 0
    skipped_recent_success = 0

    cutoff_old = datetime.utcnow() - timedelta(hours=25)
    cutoff_recent = datetime.utcnow() - timedelta(hours=1)

    try:
        async with async_session_factory() as session:
            stmt = (
                select(Snapshot)
                .where(and_(
                    Snapshot.status == SnapshotStatus.FAILED,
                    Snapshot.completed_at >= cutoff_old,
                    Snapshot.completed_at < cutoff_recent,
                ))
            )
            failed = (await session.execute(stmt)).scalars().all()

            for snap in failed:
                if (snap.extra_data or {}).get("retry_attempted"):
                    skipped_marked += 1
                    continue

                # If a newer COMPLETED snapshot for the same resource exists,
                # the failure has already been "healed" by the next scheduled
                # run — skip the retry to avoid wasted work.
                #
                # A skipped_concurrent placeholder is NOT a healing success.
                # backup-worker's cross-job guard writes one ~19ms after a
                # genuine failure (COMPLETED, item_count=0, nothing captured),
                # which used to satisfy this test and permanently cancel the
                # FAILED snapshot's ONE automatic retry — the marker written
                # below makes the skip stick on every later sweep, so the
                # resource silently stopped being retried. Exclude
                # placeholders so only a real capture cancels the retry.
                #
                # COMPLETED-only is deliberate: a PARTIAL run captured some
                # but not all items, so it does not prove the failure healed.
                # Erring toward one extra retry is cheap; erring toward a
                # cancelled retry loses the data.
                newer_stmt = select(Snapshot.id).where(and_(
                    Snapshot.resource_id == snap.resource_id,
                    Snapshot.status == SnapshotStatus.COMPLETED,
                    Snapshot.completed_at > snap.completed_at,
                    _not_skipped_concurrent(),
                )).limit(1)
                if (await session.execute(newer_stmt)).first():
                    snap.extra_data = (snap.extra_data or {}) | {"retry_attempted": True, "retry_skipped_reason": "newer_success_exists"}
                    await session.merge(snap)
                    skipped_recent_success += 1
                    continue

                resource = await session.get(Resource, snap.resource_id)
                if not resource:
                    continue

                # Pick the queue based on workload — Azure to its own queue,
                # heavy file-content workloads to backup.heavy, rest to normal.
                resource_type = resource.type.value if hasattr(resource.type, "value") else str(resource.type)
                azure_q = AZURE_WORKLOAD_QUEUES.get(resource.type)
                if azure_q:
                    queue = azure_q
                else:
                    from shared.export_routing import pick_backup_queue
                    queue = pick_backup_queue(
                        resource_type=resource_type,
                        default_queue="backup.normal",
                    )
                payload = {
                    "jobId": str(uuid.uuid4()),
                    "resourceId": str(resource.id),
                    "workload": resource_type,
                    "retry_of_snapshot": str(snap.id),
                }
                try:
                    await message_bus.publish(queue, payload, priority=4)
                    snap.extra_data = (snap.extra_data or {}) | {"retry_attempted": True, "retry_at": datetime.utcnow().isoformat()}
                    await session.merge(snap)
                    requeued += 1
                except Exception as e:
                    print(f"[RETRY] failed to re-publish for snapshot {snap.id}: {e}")

            await session.commit()
        print(f"[RETRY] === COMPLETE: requeued={requeued}, skipped_marked={skipped_marked}, skipped_already_healed={skipped_recent_success} ===")
    except Exception as e:
        import traceback as _tb
        print(f"[RETRY] FATAL: {e}\n{_tb.format_exc()}")


# ── Phase 2: Retention Cleanup ──

async def run_retention_cleanup():
    """Daily: apply FLAT/GFS retention rules, delete snapshots outside the window.
    Azure lifecycle policies still handle blob tier transitions + TTL; this function
    reclaims DB rows + guarantees GFS behavior that storage lifecycle alone can't express."""
    import traceback as _tb
    print("[RETENTION] === START: Snapshot retention cleanup ===")
    try:
        from shared.retention_cleanup import enforce_retention_all_tenants
        results = await enforce_retention_all_tenants(async_session_factory)
        total_deleted = sum(r.get("deleted_snapshots", 0) for r in results.values() if isinstance(r, dict))
        total_kept = sum(r.get("kept_snapshots", 0) for r in results.values() if isinstance(r, dict))
        total_held = sum(r.get("held", 0) for r in results.values() if isinstance(r, dict))
        errors = [tid for tid, r in results.items() if isinstance(r, dict) and "error" in r]
        print(f"[RETENTION] === COMPLETE: deleted={total_deleted}, kept={total_kept}, on_hold={total_held}, errors={len(errors)} ===")
        for tid in errors:
            print(f"[RETENTION]   tenant {tid}: {results[tid].get('error')}")
    except Exception as e:
        print(f"[RETENTION] FATAL ERROR: {e}\n{_tb.format_exc()}")


# ── AZ-0: Lifecycle Policy Reconciler ──

async def reconcile_lifecycle_policies():
    """
    Daily: ensure every tenant's blob containers have current lifecycle policy
    based on their SLA retention settings.
    """
    import traceback as _tb
    print("[LIFECYCLE] === START: Daily lifecycle policy reconciliation ===")

    try:
        from shared.azure_storage import (
            apply_lifecycle_policy,
            apply_container_immutability,
            apply_container_legal_hold,
            apply_encryption_scope,
        )
    except ImportError:
        print("[LIFECYCLE] ERROR: azure_storage.apply_lifecycle_policy not available — skipping")
        return

    # Lifecycle policies, container immutability, and container legal-hold
    # are Azure-Blob-only ARM operations. On SeaweedFS deployments they
    # have no analogue, and the SLA fields that drive them (immutability,
    # legal hold) are honored at the Postgres prune layer instead — see
    # shared/retention_cleanup.py:35-40. Skip the entire reconciler when
    # SeaweedFS is the active backend rather than firing API calls that
    # will all fail.
    try:
        from shared.storage.router import router as storage_router
        active_kind = None
        for be in storage_router.list_backends():
            if str(be.backend_id) == str(storage_router.active_backend_id()):
                active_kind = be.kind
                break
        if active_kind and active_kind != "azure_blob":
            print(f"[LIFECYCLE] Active backend is '{active_kind}' — Azure-only reconciler is a no-op here.")
            return
    except Exception as router_exc:
        # Router not loaded (e.g. very early startup) — fall through and
        # let the per-call ARM credentials check gate any actual work.
        print(f"[LIFECYCLE] Router check skipped: {router_exc}")

    # Tunables. Concurrency is bounded by ARM's per-subscription rate-limit
    # (~50 req/min) — gather=20 with the per-call latency we see (~150ms
    # nominal) lands well inside the budget while still cutting wall-clock
    # ~20× vs serial. Per-tenant timeout protects against a single hung
    # ARM call (auth glitch, network partition) freezing the whole pass.
    LIFECYCLE_PARALLELISM = int(os.getenv("LIFECYCLE_PARALLELISM", "20"))
    # 600s default: at single-tenant 5k-user / 250 TiB scale a tenant has
    # O(10) policies × 4 workloads × 4 ARM calls = ~160 ARM hops per pass.
    # ARM 99p latency is ~600ms cold-cache; 600s gives ~3.5× headroom over
    # the worst case while still bounding a hung call. The legacy 120s
    # default tripped on cold-tenant first runs.
    LIFECYCLE_TENANT_TIMEOUT_S = float(os.getenv("LIFECYCLE_TENANT_TIMEOUT_S", "600"))
    # Parallelize the per-workload inner loop. Workloads (files / azure-vm
    # / azure-sql / azure-postgres) target distinct containers — no shared
    # state, no ordering needed. Cap at the workload count to avoid
    # accidentally exceeding ARM's per-subscription rate-limit ceiling.
    LIFECYCLE_WORKLOAD_PARALLELISM = int(os.getenv("LIFECYCLE_WORKLOAD_PARALLELISM", "4"))

    async def _reconcile_one_tenant(
        tenant: Tenant,
        only_dirty_policies: bool,
    ) -> Tuple[int, int, int]:
        """Reconcile every enabled SLA policy for one tenant. Returns
        (success_count, fail_count, skip_count). Self-contained session so
        concurrent tenants never share a DB connection or pending state."""
        success = fail = skip = 0
        async with async_session_factory() as t_session:
            stmt = select(SlaPolicy).where(
                SlaPolicy.tenant_id == tenant.id,
                SlaPolicy.enabled == True,  # noqa: E712
            )
            if only_dirty_policies:
                stmt = stmt.where(SlaPolicy.lifecycle_dirty == True)  # noqa: E712
            policies = (await t_session.execute(stmt)).scalars().all()
            if not policies:
                return (0, 0, 1)

            for sla in policies:
                try:
                    hot = sla.retention_hot_days or 7
                    cool = sla.retention_cool_days or 30
                    archive = sla.retention_archive_days  # None = unlimited

                    immut_mode = (sla.immutability_mode or "None").strip()
                    if immut_mode not in ("None", "Unlocked", "Locked"):
                        immut_mode = "None"
                    immut_days = (hot or 0) + (cool or 0) + (archive or 0)
                    if immut_days <= 0:
                        immut_days = max(hot, 1)
                    legal_hold_on = bool(sla.legal_hold_enabled)

                    print(
                        f"[LIFECYCLE] {tenant.display_name} / {sla.name}: "
                        f"hot={hot}d cool={cool}d "
                        f"archive={'unlimited' if archive is None else f'{archive}d'} "
                        f"immutability={immut_mode} legal_hold={legal_hold_on} "
                        f"encryption={sla.encryption_mode}"
                    )

                    workload_sem = asyncio.Semaphore(LIFECYCLE_WORKLOAD_PARALLELISM)

                    async def _reconcile_workload(
                        workload: str,
                    ) -> Tuple[int, int, Optional[str], List[Dict[str, Any]]]:
                        """Reconcile one (policy, workload) pair. Returns
                        (success_inc, fail_inc, cmk_status_or_None, audit_intents).
                        Audits are returned rather than emitted directly so the
                        caller can serialize them after the gather (avoids
                        interleaved audit-event publishes from concurrent tasks).
                        """
                        async with workload_sem:
                            s = f = 0
                            cmk_status: Optional[str] = None
                            audits: List[Dict[str, Any]] = []
                            shard = azure_storage_manager.get_default_shard()
                            container = azure_storage_manager.get_container_name(
                                str(tenant.id), workload,
                            )

                            try:
                                result = await apply_lifecycle_policy(container, hot, cool, archive, shard)
                                if result.get("success"):
                                    s += 1
                                else:
                                    f += 1
                                    print(f"[LIFECYCLE]   ✗ {container} lifecycle: {result.get('error')}")
                            except Exception as e:
                                f += 1
                                print(f"[LIFECYCLE]   ✗ {container} lifecycle: {e}")

                            try:
                                im_res = await apply_container_immutability(
                                    container, immut_days, immut_mode, shard,
                                )
                                if not im_res.get("success"):
                                    print(f"[LIFECYCLE]   ✗ {container} immutability: {im_res.get('error')}")
                                elif immut_mode == "Locked" and im_res.get("note") != "already_locked":
                                    audits.append({
                                        "action": "SLA_IMMUTABILITY_LOCKED",
                                        "details": {"container": container, "days": immut_days},
                                    })
                            except Exception as im_exc:
                                print(f"[LIFECYCLE]   ✗ {container} immutability: {im_exc}")

                            try:
                                lh_res = await apply_container_legal_hold(
                                    container, legal_hold_on, shard=shard,
                                )
                                if not lh_res.get("success"):
                                    print(f"[LIFECYCLE]   ✗ {container} legal_hold: {lh_res.get('error')}")
                            except Exception as lh_exc:
                                print(f"[LIFECYCLE]   ✗ {container} legal_hold: {lh_exc}")

                            if (sla.encryption_mode or "").upper() == "CUSTOMER_KEY":
                                try:
                                    enc_res = await apply_encryption_scope(
                                        container,
                                        sla.key_vault_uri or "",
                                        sla.key_name or "",
                                        sla.key_version,
                                        shard,
                                    )
                                    cmk_status = enc_res.get("status", "UNKNOWN")
                                    if not enc_res.get("success"):
                                        print(f"[LIFECYCLE]   ✗ {container} CMK: {cmk_status} — {enc_res.get('error')}")
                                except Exception as enc_exc:
                                    cmk_status = "ERROR"
                                    print(f"[LIFECYCLE]   ✗ {container} CMK: {enc_exc}")
                            return (s, f, cmk_status, audits)

                    workload_results = await asyncio.gather(
                        *(
                            _reconcile_workload(wl)
                            for wl in ("files", "azure-vm", "azure-sql", "azure-postgres")
                        ),
                        return_exceptions=False,
                    )
                    cmk_statuses: List[str] = []
                    pending_audits: List[Dict[str, Any]] = []
                    for s, f, cmk, audits in workload_results:
                        success += s
                        fail += f
                        if cmk is not None:
                            cmk_statuses.append(cmk)
                        pending_audits.extend(audits)
                    # Emit collected audits sequentially so concurrent tasks
                    # don't race on the audit message bus.
                    for evt in pending_audits:
                        await _emit_policy_audit(
                            action=evt["action"],
                            tenant_id=str(tenant.id),
                            policy=sla,
                            details=evt["details"],
                        )

                    # Aggregate CMK status onto the policy.
                    if cmk_statuses:
                        if all(s == "OK" for s in cmk_statuses):
                            new_status = "OK"
                        elif "KEY_VAULT_ACCESS_DENIED" in cmk_statuses:
                            new_status = "KEY_VAULT_ACCESS_DENIED"
                        else:
                            new_status = "ERROR"
                    else:
                        new_status = ""

                    prior_status = sla.encryption_status or ""
                    status_changed = prior_status != new_status

                    if status_changed:
                        if _sla_metrics is not None:
                            _sla_metrics.inc_encryption_transition(prior_status, new_status)
                        sla.encryption_status = new_status
                        # Notify on transition into a non-OK state. OK→non-OK
                        # is a real alert; non-OK→OK is a recovery.
                        if new_status and new_status != "OK":
                            await _emit_policy_audit(
                                action="SLA_ENCRYPTION_STATUS_DEGRADED",
                                tenant_id=str(tenant.id),
                                policy=sla,
                                details={"from": prior_status, "to": new_status},
                                outcome="FAILED",
                            )
                        elif prior_status and prior_status != "OK" and new_status == "OK":
                            await _emit_policy_audit(
                                action="SLA_ENCRYPTION_STATUS_RECOVERED",
                                tenant_id=str(tenant.id),
                                policy=sla,
                                details={"from": prior_status, "to": new_status},
                            )

                    # Clear the dirty flag on success — partial failures keep
                    # it dirty so the next 5-min sweep retries. Also clear
                    # last_cap_alert_at so the next failure cycle starts a
                    # fresh 24h cooldown rather than inheriting an old one.
                    if fail == 0:
                        sla.lifecycle_dirty = False
                        sla.last_reconciled_at = datetime.now(timezone.utc).replace(tzinfo=None)
                        sla.reconcile_attempts = 0
                        sla.last_cap_alert_at = None
                    else:
                        sla.reconcile_attempts = (sla.reconcile_attempts or 0) + 1

                    await t_session.commit()

                except Exception as policy_exc:
                    fail += 1
                    print(f"[LIFECYCLE] policy {sla.id} error: {policy_exc}")
                    try:
                        sla.reconcile_attempts = (sla.reconcile_attempts or 0) + 1
                        await t_session.commit()
                    except Exception:
                        await t_session.rollback()
        return (success, fail, skip)

    try:
        async with async_session_factory() as session:
            tenants_result = await session.execute(select(Tenant))
            tenants = tenants_result.scalars().all()

        if not tenants:
            print("[LIFECYCLE] No tenants found — nothing to reconcile")
            return

        print(
            f"[LIFECYCLE] Found {len(tenants)} tenant(s) to reconcile "
            f"(parallelism={LIFECYCLE_PARALLELISM}, timeout={LIFECYCLE_TENANT_TIMEOUT_S}s)"
        )

        sem = asyncio.Semaphore(LIFECYCLE_PARALLELISM)

        async def _bounded(t: Tenant) -> Tuple[int, int, int]:
            async with sem:
                try:
                    return await asyncio.wait_for(
                        _reconcile_one_tenant(t, only_dirty_policies=False),
                        timeout=LIFECYCLE_TENANT_TIMEOUT_S,
                    )
                except asyncio.TimeoutError:
                    print(f"[LIFECYCLE] tenant {t.id} timed out after {LIFECYCLE_TENANT_TIMEOUT_S}s")
                    return (0, 1, 0)
                except Exception as exc:
                    print(f"[LIFECYCLE] tenant {t.id} fatal: {exc}")
                    return (0, 1, 0)

        results = await asyncio.gather(*(_bounded(t) for t in tenants))
        success_count = sum(r[0] for r in results)
        fail_count = sum(r[1] for r in results)
        skip_count = sum(r[2] for r in results)

        print(
            f"[LIFECYCLE] === COMPLETE: success={success_count}, "
            f"failed={fail_count}, skipped={skip_count} ==="
        )

    except Exception as e:
        print(f"[LIFECYCLE] FATAL ERROR: {e}\n{_tb.format_exc()}")


# Stable 64-bit advisory-lock keys. Postgres pg_try_advisory_lock takes
# a single bigint or two ints; we pick a fixed namespace (high bits) so
# these don't collide with other application advisory locks.
_LOCK_KEY_LIFECYCLE_SWEEP = 0x534C415F4C434753  # ASCII "SLA_LCGS"

# Errors we treat as transient — don't increment reconcile_attempts.
# Operator-recoverable / permanent errors (4xx auth, malformed config)
# still count toward the cap. At 5k-user / 260 TiB scale this matters
# because a regional ARM blip shouldn't burn the cap during normal flake.
_TRANSIENT_ARM_ERROR_TOKENS = (
    "429",                # rate limit
    "throttl", "Throttl",
    "503", "ServiceUnavailable",
    "504", "GatewayTimeout",
    "500", "InternalServerError",
    "TimeoutError", "asyncio.TimeoutError",
    "Connection",
    "TemporaryFailure", "RetryableError",
)


def _is_transient_arm_error(message: str) -> bool:
    if not message:
        return False
    return any(tok in message for tok in _TRANSIENT_ARM_ERROR_TOKENS)


async def sweep_dirty_lifecycle_policies():
    """5-minute durable sweeper for `lifecycle_dirty=True` policies.

    Why this exists: when an operator saves a policy, resource-service
    marks `lifecycle_dirty=True` in the same DB transaction AND fires a
    best-effort HTTP nudge (`/scheduler/reconcile-lifecycle`). The HTTP
    is the fast path (sub-second). This sweeper is the durable backstop
    — if the HTTP dropped (network blip, scheduler restart mid-request,
    transient 502), the dirty flag is still in Postgres and we'll pick
    it up on the next 5-min tick. The flag is cleared only after a
    successful reconcile, so retries are automatic.

    Capped at 25 attempts per policy to prevent a permanently-broken
    policy (e.g. wrong Key Vault URI) from burning ARM quota forever —
    after that we surface an audit event and stop retrying. Operator
    must edit the policy to reset attempts.
    """
    import traceback as _tb
    SWEEP_ATTEMPT_CAP = int(os.getenv("LIFECYCLE_SWEEP_ATTEMPT_CAP", "25"))
    SWEEP_PARALLELISM = int(os.getenv("LIFECYCLE_SWEEP_PARALLELISM", "10"))
    SWEEP_TIMEOUT_S = float(os.getenv("LIFECYCLE_SWEEP_TIMEOUT_S", "60"))

    # Leader election. With multiple scheduler pods (HA), every 5min tick
    # would otherwise have all pods racing on the same dirty rows —
    # double the ARM rate, double the audit volume, lost
    # reconcile_attempts increments. pg_try_advisory_lock returns
    # immediately; non-leaders log and exit. Lock is session-scoped, so
    # we keep a dedicated connection open for the duration of this run
    # and release explicitly at the end.
    #
    # ITO-9652 (pgbouncer pooler) WARNING: this leader-election scheme
    # depends on `lock_session`'s underlying backend connection staying
    # open and unchanged for the *entire* sweep — pg_advisory_lock (session
    # form) is tied to the backend connection/session, not a transaction.
    # Under pgbouncer transaction-mode pooling, any commit() (or any
    # statement that ends the current transaction) on `lock_session` mid-
    # sweep hands the client-side connection back to the pooler, which is
    # free to reassign a *different* backend connection to the next
    # statement — silently dropping the advisory lock without an error and
    # breaking leader election (multiple pods can end up believing they're
    # leader). Do not call lock_session.commit() anywhere in this sweep
    # except via the deliberate acquire/unlock pair below.
    lock_acquired = False
    lock_session = None

    async def _release_lifecycle_lock():
        """Release the pg-advisory lock + close the session.
        Idempotent: safe to call from any early-return path or the
        outer finally. Without this, the early-exit branches below
        (SeaweedFS short-circuit, ImportError fallback, empty-dirty
        result, empty-tenant guard) leaked one asyncpg connection per
        sweep tick, surfacing as `garbage collector is trying to clean
        up non-checked-in connection` SAWarnings in the scheduler log.
        """
        nonlocal lock_acquired, lock_session
        if lock_acquired and lock_session is not None:
            try:
                await lock_session.execute(
                    text("SELECT pg_advisory_unlock(:k)"),
                    {"k": _LOCK_KEY_LIFECYCLE_SWEEP},
                )
                await lock_session.commit()
            except Exception as e:
                print(f"[LIFECYCLE-SWEEP] advisory_unlock failed: {e}")
        if lock_session is not None:
            try:
                await lock_session.__aexit__(None, None, None)
            except Exception:
                pass
        lock_session = None
        lock_acquired = False

    try:
        lock_session = async_session_factory()
        await lock_session.__aenter__()
        got = (await lock_session.execute(
            text("SELECT pg_try_advisory_lock(:k)"),
            {"k": _LOCK_KEY_LIFECYCLE_SWEEP},
        )).scalar()
        lock_acquired = bool(got)
        if not lock_acquired:
            print("[LIFECYCLE-SWEEP] another pod holds the sweeper lock — skipping this tick")
            await _release_lifecycle_lock()
            return
    except Exception as e:
        print(f"[LIFECYCLE-SWEEP] advisory-lock acquire failed, skipping tick: {e}")
        await _release_lifecycle_lock()
        return

    # SeaweedFS short-circuit (same logic as the daily reconciler).
    # IMPORTANT: do NOT clear `lifecycle_dirty` on SeaweedFS. If we did,
    # any policy edits made while SeaweedFS was active would be lost
    # forever — when the operator flips back to Azure, those changes
    # would never replay (no other path sets dirty=true retroactively).
    # Instead, leave the flag set and exit. The flag is bounded by the
    # number of policies (small) so the queue stays trivial; the next
    # sweep tick after the Azure flip will pick them all up.
    try:
        from shared.storage.router import router as storage_router
        active_kind = None
        for be in storage_router.list_backends():
            if str(be.backend_id) == str(storage_router.active_backend_id()):
                active_kind = be.kind
                break
        if active_kind and active_kind != "azure_blob":
            print(f"[LIFECYCLE-SWEEP] active backend is '{active_kind}' — "
                  f"skipping (dirty flags preserved for Azure-flip replay)")
            await _release_lifecycle_lock()
            return
    except Exception:
        pass  # router not loaded yet — let the per-call ARM check gate it

    try:
        from shared.azure_storage import (
            apply_lifecycle_policy,
            apply_container_immutability,
            apply_container_legal_hold,
            apply_encryption_scope,
        )
    except ImportError:
        await _release_lifecycle_lock()
        return

    async with async_session_factory() as session:
        result = await session.execute(
            select(SlaPolicy)
            .where(SlaPolicy.lifecycle_dirty == True)  # noqa: E712
            .where(SlaPolicy.enabled == True)  # noqa: E712
            .where(SlaPolicy.reconcile_attempts < SWEEP_ATTEMPT_CAP)
        )
        dirty_policies = result.scalars().all()

    if _sla_metrics is not None:
        _sla_metrics.set_dirty_count(len(dirty_policies))

    if not dirty_policies:
        await _release_lifecycle_lock()
        return

    print(f"[LIFECYCLE-SWEEP] {len(dirty_policies)} dirty policy(ies) to reconcile")

    # Group by tenant so we can reuse the same per-tenant reconcile shape
    # used by the daily pass — but we filter to lifecycle_dirty=True so a
    # one-policy edit doesn't scan every other policy in the tenant.
    by_tenant: Dict[Any, List[SlaPolicy]] = {}
    for p in dirty_policies:
        by_tenant.setdefault(p.tenant_id, []).append(p)

    # Load tenant rows (single query) so we have display names for logs.
    async with async_session_factory() as session:
        tenant_ids = list(by_tenant.keys())
        if not tenant_ids:
            await _release_lifecycle_lock()
            return
        t_rows = (await session.execute(
            select(Tenant).where(Tenant.id.in_(tenant_ids))
        )).scalars().all()
    tenants_by_id = {t.id: t for t in t_rows}

    sem = asyncio.Semaphore(SWEEP_PARALLELISM)

    async def _sweep_one_tenant_dirty(tenant_id, policies):
        tenant = tenants_by_id.get(tenant_id)
        if tenant is None:
            return
        async with sem:
            try:
                await asyncio.wait_for(
                    _reconcile_policies_for_tenant(tenant, policies),
                    timeout=SWEEP_TIMEOUT_S,
                )
            except asyncio.TimeoutError:
                print(f"[LIFECYCLE-SWEEP] tenant {tenant_id} sweep timed out")
            except Exception as exc:
                print(f"[LIFECYCLE-SWEEP] tenant {tenant_id} error: {exc}\n{_tb.format_exc()}")

    await asyncio.gather(*(
        _sweep_one_tenant_dirty(tid, ps) for tid, ps in by_tenant.items()
    ))

    # Surface policies that hit the attempt cap so the operator can fix
    # the underlying config (wrong vault URI, missing role assignment).
    # Cooldown: dedupe to one alert per policy per 24h. The sweeper runs
    # every 5 minutes; without the cooldown a single stuck policy fires
    # 288 alerts/day into the audit channel and any downstream PagerDuty
    # rule. The cooldown is cleared (column set NULL) on the next
    # successful reconcile, so the next failure restarts the alert cycle.
    CAP_ALERT_COOLDOWN_HOURS = 24
    try:
        async with async_session_factory() as session:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=CAP_ALERT_COOLDOWN_HOURS)
            capped = (await session.execute(
                select(SlaPolicy).where(
                    SlaPolicy.lifecycle_dirty == True,  # noqa: E712
                    SlaPolicy.reconcile_attempts >= SWEEP_ATTEMPT_CAP,
                )
            )).scalars().all()
            now = datetime.now(timezone.utc)
            for p in capped:
                # Always increment the metric — it's a counter on the
                # current rate, not a notification. The cooldown only
                # gates the audit/Slack-style emission.
                if _sla_metrics is not None:
                    _sla_metrics.inc_attempt_cap()
                last = p.last_cap_alert_at
                if last is not None and last.tzinfo is None:
                    # legacy rows may have naive timestamps
                    last = last.replace(tzinfo=timezone.utc)
                if last is not None and last > cutoff:
                    continue  # within cooldown — skip the audit emit
                await _emit_policy_audit(
                    action="SLA_RECONCILE_ATTEMPT_CAP_REACHED",
                    tenant_id=str(p.tenant_id),
                    policy=p,
                    details={"attempts": p.reconcile_attempts, "cap": SWEEP_ATTEMPT_CAP},
                    outcome="FAILED",
                )
                p.last_cap_alert_at = now
            await session.commit()
    finally:
        # Release the advisory lock no matter what — non-leaders never
        # acquired so this is a no-op for them; the leader frees the lock
        # so the next 5-min tick can elect again. Idempotent with the
        # early-return release calls above.
        await _release_lifecycle_lock()


async def _reconcile_policies_for_tenant(tenant, policies):
    """Shared per-tenant body used by the daily full reconcile and the
    5-minute dirty sweep. Caller is responsible for the timeout wrapper
    and the semaphore guard."""
    from shared.azure_storage import (
        apply_lifecycle_policy,
        apply_container_immutability,
        apply_container_legal_hold,
        apply_encryption_scope,
        read_encryption_scope_state,
        resolve_key_vault_latest_version,
    )

    async with async_session_factory() as t_session:
        # Re-fetch policies inside this session so we have managed instances
        # we can mutate + commit.
        ids = [p.id for p in policies]
        rows = (await t_session.execute(
            select(SlaPolicy).where(SlaPolicy.id.in_(ids))
        )).scalars().all()

        for sla in rows:
            had_failure = False
            had_transient_only = True  # flips false on any non-transient failure
            try:
                hot = sla.retention_hot_days or 7
                cool = sla.retention_cool_days or 30
                archive = sla.retention_archive_days

                immut_mode = (sla.immutability_mode or "None").strip()
                if immut_mode not in ("None", "Unlocked", "Locked"):
                    immut_mode = "None"
                immut_days = (hot or 0) + (cool or 0) + (archive or 0)
                if immut_days <= 0:
                    immut_days = max(hot, 1)
                legal_hold_on = bool(sla.legal_hold_enabled)

                cmk_statuses: List[str] = []
                for workload in ["files", "azure-vm", "azure-sql", "azure-postgres"]:
                    shard = azure_storage_manager.get_default_shard()
                    container = azure_storage_manager.get_container_name(str(tenant.id), workload)

                    try:
                        lc_res = await apply_lifecycle_policy(container, hot, cool, archive, shard)
                        if not lc_res.get("success"):
                            had_failure = True
                            if not _is_transient_arm_error(str(lc_res.get("error", ""))):
                                had_transient_only = False
                    except Exception as e:
                        had_failure = True
                        if not _is_transient_arm_error(str(e)):
                            had_transient_only = False
                        print(f"[LIFECYCLE-SWEEP] {container} lifecycle: {e}")

                    try:
                        im_res = await apply_container_immutability(
                            container, immut_days, immut_mode, shard,
                        )
                        if not im_res.get("success"):
                            had_failure = True
                            err = str(im_res.get("error", ""))
                            status = im_res.get("status", "")
                            # REFUSED_LOOSEN_LOCKED is a permanent operator
                            # error (not transient) — emit audit so it
                            # surfaces immediately, not after cap.
                            if status == "REFUSED_LOOSEN_LOCKED":
                                had_transient_only = False
                                if _sla_metrics is not None:
                                    _sla_metrics.inc_worm_loosen_refused()
                                await _emit_policy_audit(
                                    action="SLA_IMMUTABILITY_LOOSEN_REFUSED",
                                    tenant_id=str(tenant.id),
                                    policy=sla,
                                    details={"container": container,
                                             "policy_mode": immut_mode,
                                             "live_mode": "Locked"},
                                    outcome="FAILED",
                                )
                            elif not _is_transient_arm_error(err):
                                had_transient_only = False
                        elif immut_mode == "Locked" and im_res.get("note") != "already_locked":
                            if _sla_metrics is not None:
                                _sla_metrics.inc_worm("Locked")
                            await _emit_policy_audit(
                                action="SLA_IMMUTABILITY_LOCKED",
                                tenant_id=str(tenant.id),
                                policy=sla,
                                details={"container": container, "days": immut_days},
                            )
                    except Exception as e:
                        had_failure = True
                        if not _is_transient_arm_error(str(e)):
                            had_transient_only = False
                        print(f"[LIFECYCLE-SWEEP] {container} immutability: {e}")

                    try:
                        lh_res = await apply_container_legal_hold(
                            container, legal_hold_on, shard=shard,
                        )
                        if not lh_res.get("success"):
                            had_failure = True
                            if not _is_transient_arm_error(str(lh_res.get("error", ""))):
                                had_transient_only = False
                    except Exception as e:
                        had_failure = True
                        if not _is_transient_arm_error(str(e)):
                            had_transient_only = False
                        print(f"[LIFECYCLE-SWEEP] {container} legal_hold: {e}")

                    if (sla.encryption_mode or "").upper() == "CUSTOMER_KEY":
                        # Resolve the target key version. When the operator
                        # left it blank ("latest"), query Key Vault directly
                        # so rotation in the vault gets reflected here on
                        # the next reconcile pass.
                        target_version = sla.key_version
                        if not target_version:
                            target_version = await resolve_key_vault_latest_version(
                                sla.key_vault_uri or "",
                                sla.key_name or "",
                            )
                        # Build the expected keyUri so we can compare against
                        # the live ARM state and skip unchanged scopes.
                        expected_uri = None
                        if sla.key_vault_uri and sla.key_name:
                            base = (sla.key_vault_uri or "").rstrip("/")
                            expected_uri = (
                                f"{base}/keys/{sla.key_name}/{target_version}"
                                if target_version else
                                f"{base}/keys/{sla.key_name}"
                            )
                        try:
                            live = await read_encryption_scope_state(container, shard)
                            live_uri = (live or {}).get("key_uri")
                            in_sync = (
                                expected_uri is not None
                                and live_uri is not None
                                and (live_uri.rstrip("/") == expected_uri.rstrip("/")
                                     or (target_version is None and live_uri.startswith(expected_uri.rstrip("/"))))
                            )
                        except Exception:
                            in_sync = False
                            live_uri = None

                        if in_sync:
                            cmk_statuses.append("OK")
                            # Stamp the resolved version so the UI / audit
                            # trail can show what's actually applied.
                            if target_version and sla.key_version_resolved != target_version:
                                sla.key_version_resolved = target_version
                        else:
                            try:
                                enc_res = await apply_encryption_scope(
                                    container,
                                    sla.key_vault_uri or "",
                                    sla.key_name or "",
                                    target_version,
                                    shard,
                                )
                                cmk_statuses.append(enc_res.get("status", "UNKNOWN"))
                                if enc_res.get("success"):
                                    if target_version:
                                        sla.key_version_resolved = target_version
                                    # Drift / rotation event audit (one per
                                    # container so the operator can see
                                    # exactly what flipped).
                                    if _sla_metrics is not None:
                                        if live_uri:
                                            _sla_metrics.inc_cmk_rotated()
                                        else:
                                            _sla_metrics.inc_cmk_drift()
                                    await _emit_policy_audit(
                                        action=("SLA_CMK_KEY_ROTATED"
                                                if live_uri else "SLA_CMK_SCOPE_APPLIED"),
                                        tenant_id=str(tenant.id),
                                        policy=sla,
                                        details={
                                            "container": container,
                                            "from": live_uri,
                                            "to": expected_uri,
                                        },
                                    )
                                else:
                                    had_failure = True
                                    err = str(enc_res.get("error", ""))
                                    if enc_res.get("status") == "KEY_VAULT_ACCESS_DENIED":
                                        # Permanent until operator grants
                                        # the role assignment — count toward
                                        # the cap so we stop hammering KV.
                                        had_transient_only = False
                                    elif not _is_transient_arm_error(err):
                                        had_transient_only = False
                            except Exception as e:
                                had_failure = True
                                if not _is_transient_arm_error(str(e)):
                                    had_transient_only = False
                                cmk_statuses.append("ERROR")
                                print(f"[LIFECYCLE-SWEEP] {container} CMK: {e}")

                # Aggregate CMK status
                if cmk_statuses:
                    if all(s == "OK" for s in cmk_statuses):
                        new_status = "OK"
                    elif "KEY_VAULT_ACCESS_DENIED" in cmk_statuses:
                        new_status = "KEY_VAULT_ACCESS_DENIED"
                    else:
                        new_status = "ERROR"
                else:
                    new_status = ""

                prior_status = sla.encryption_status or ""
                if prior_status != new_status:
                    sla.encryption_status = new_status
                    if new_status and new_status != "OK":
                        await _emit_policy_audit(
                            action="SLA_ENCRYPTION_STATUS_DEGRADED",
                            tenant_id=str(tenant.id),
                            policy=sla,
                            details={"from": prior_status, "to": new_status},
                            outcome="FAILED",
                        )
                    elif prior_status and prior_status != "OK" and new_status == "OK":
                        await _emit_policy_audit(
                            action="SLA_ENCRYPTION_STATUS_RECOVERED",
                            tenant_id=str(tenant.id),
                            policy=sla,
                            details={"from": prior_status, "to": new_status},
                        )

                if had_failure:
                    # Transient-only failures (429/503/timeout) leave the
                    # dirty flag set for the next 5-min sweep but do NOT
                    # bump the attempt counter — a regional ARM blip must
                    # not consume the cap during normal flake.
                    if not had_transient_only:
                        sla.reconcile_attempts = (sla.reconcile_attempts or 0) + 1
                        if _sla_metrics is not None:
                            _sla_metrics.inc_reconcile("failure")
                    else:
                        if _sla_metrics is not None:
                            _sla_metrics.inc_reconcile("transient")
                else:
                    sla.lifecycle_dirty = False
                    sla.reconcile_attempts = 0
                    sla.last_reconciled_at = datetime.now(timezone.utc).replace(tzinfo=None)
                    if _sla_metrics is not None:
                        _sla_metrics.inc_reconcile("success")
                    print(f"[LIFECYCLE-SWEEP] ✓ {tenant.display_name} / {sla.name}: clean")

                await t_session.commit()

            except Exception as policy_exc:
                print(f"[LIFECYCLE-SWEEP] policy {sla.id} unexpected error: {policy_exc}")
                # Unknown exception path — count toward cap (don't loop
                # forever on a deterministic crash).
                try:
                    sla.reconcile_attempts = (sla.reconcile_attempts or 0) + 1
                    await t_session.commit()
                except Exception:
                    await t_session.rollback()


# ── AZ-4: DR Setup Reconciler ──

async def reconcile_dr_setup():
    """
    Every 6 hours: ensure DR containers exist and have matching lifecycle policies.
    Only runs for tenants with dr_region_enabled=True.
    """
    import traceback as _tb
    print("[DR-RECONCILER] === START: DR setup reconciliation (every 6h) ===")

    try:
        from shared.azure_storage import apply_lifecycle_policy, AzureStorageShard
        from shared.security import decrypt_secret
    except ImportError as ie:
        print(f"[DR-RECONCILER] ERROR: Required imports not available — skipping: {ie}")
        return

    try:
        async with async_session_factory() as session:
            tenants_result = await session.execute(
                select(Tenant).where(Tenant.dr_region_enabled == True)
            )
            tenants = tenants_result.scalars().all()

            if not tenants:
                print("[DR-RECONCILER] No tenants with DR enabled — skipping")
                return

            print(f"[DR-RECONCILER] Found {len(tenants)} tenant(s) with DR enabled")

            success_count = 0
            fail_count = 0

            for tenant in tenants:
                try:
                    if not tenant.dr_storage_account_name:
                        print(f"[DR-RECONCILER] Tenant {tenant.id}: DR storage account name not configured — skipping")
                        fail_count += 1
                        continue

                    print(
                        f"[DR-RECONCILER] Tenant {tenant.id} ({tenant.display_name}): "
                        f"DR region={tenant.dr_region}, DR account={tenant.dr_storage_account_name}"
                    )

                    try:
                        dr_key = decrypt_secret(tenant.dr_storage_account_key_encrypted)
                    except Exception as dec_exc:
                        print(f"[DR-RECONCILER] Tenant {tenant.id}: Cannot decrypt DR storage key: {dec_exc}")
                        fail_count += 1
                        continue

                    dr_shard = AzureStorageShard(
                        account_name=tenant.dr_storage_account_name,
                        account_key=dr_key,
                    )

                    # Get tenant's SLA
                    sla_result = await session.execute(
                        select(SlaPolicy).where(
                            SlaPolicy.tenant_id == tenant.id,
                            SlaPolicy.enabled == True
                        ).limit(1)
                    )
                    sla = sla_result.scalar_one_or_none()
                    hot = sla.retention_hot_days if sla else 7
                    cool = sla.retention_cool_days if sla else 30
                    archive = sla.retention_archive_days if sla else None

                    print(
                        f"[DR-RECONCILER]   SLA: hot={hot}d, cool={cool}d, "
                        f"archive={'unlimited' if archive is None else f'{archive}d'}"
                    )

                    for workload in ["files", "azure-vm", "azure-sql", "azure-postgres"]:
                        container = f"{azure_storage_manager.get_container_name(str(tenant.id), workload)}-dr"
                        try:
                            result = await apply_lifecycle_policy(container, hot, cool, archive, dr_shard)
                            if result.get("success"):
                                print(f"[DR-RECONCILER]   ✓ DR {container}: {result.get('rules_count', 0)} rules applied")
                                success_count += 1
                            else:
                                print(f"[DR-RECONCILER]   ✗ DR {container}: {result.get('error', 'unknown')}")
                                fail_count += 1
                        except Exception as container_exc:
                            print(f"[DR-RECONCILER]   ✗ DR {container}: {container_exc}")
                            fail_count += 1

                except Exception as tenant_exc:
                    print(f"[DR-RECONCILER] Tenant {tenant.id} DR reconciliation error: {tenant_exc}\n{_tb.format_exc()}")
                    fail_count += 1

            print(
                f"[DR-RECONCILER] === COMPLETE: success={success_count}, failed={fail_count} ==="
            )

    except Exception as e:
        print(f"[DR-RECONCILER] FATAL ERROR: {e}\n{_tb.format_exc()}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8008)
