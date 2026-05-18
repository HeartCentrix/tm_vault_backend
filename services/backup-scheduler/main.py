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
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple
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
from shared.audit import emit_backup_triggered

app = FastAPI(title="Backup Scheduler Service", version="3.0.0")

# APScheduler for dynamic SLA policy scheduling
scheduler = AsyncIOScheduler()

# Resource type to SLA flag mapping — determines which workloads a policy backs up
RESOURCE_TYPE_TO_SLA_FLAG: Dict[str, str] = {
    "MAILBOX": "backup_exchange",
    "SHARED_MAILBOX": "backup_exchange",
    "ROOM_MAILBOX": "backup_exchange",
    "ONEDRIVE": "backup_onedrive",
    "SHAREPOINT_SITE": "backup_sharepoint",
    "TEAMS_CHANNEL": "backup_teams",
    # TEAMS_CHAT rows stay in the catalog as the user-facing entity (UI,
    # restore-by-chat), but are excluded from job dispatch — see
    # SCHEDULER_IGNORED_TYPES below. Actual chat backup runs against the
    # per-user TEAMS_CHAT_EXPORT shard emitted by discovery.
    "TEAMS_CHAT": "backup_teams_chats",
    "TEAMS_CHAT_EXPORT": "backup_teams_chats",
    # Tier 2 per-user shards emitted by discovery — one row per user per
    # workload (see graph_client.discover_user_resources). Without these
    # mappings the scheduler can't resolve them to an SLA flag and skips
    # every shard, even when the matching workload toggle is on.
    "USER_MAIL": "backup_exchange",
    "USER_ONEDRIVE": "backup_onedrive",
    "USER_CONTACTS": "contacts",
    "USER_CALENDAR": "calendars",
    "USER_CHATS": "backup_teams_chats",
    "ENTRA_USER": "backup_entra_id",
    "ENTRA_GROUP": "backup_entra_id",
    "ENTRA_APP": "backup_entra_id",
    "ENTRA_SERVICE_PRINCIPAL": "backup_entra_id",
    "ENTRA_DEVICE": "backup_entra_id",
    "ENTRA_ROLE": "backup_entra_id",
    "ENTRA_ADMIN_UNIT": "backup_entra_id",
    "ENTRA_AUDIT_LOG": "backup_entra_id",
    "INTUNE_MANAGED_DEVICE": "backup_entra_id",
    "POWER_BI": "backup_power_platform",
    "POWER_APPS": "backup_power_platform",
    "POWER_AUTOMATE": "backup_power_platform",
    "POWER_DLP": "backup_power_platform",
    "COPILOT": "backup_copilot",
    "PLANNER": "planner",
    "TODO": "tasks",
    "ONENOTE": "backup_onedrive",
    "AZURE_VM": "backup_azure_vm",
    "AZURE_SQL_DB": "backup_azure_sql",
    "AZURE_POSTGRESQL": "backup_azure_postgresql",
    "AZURE_POSTGRESQL_SINGLE": "backup_azure_postgresql",
}

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


# Resource types intentionally excluded from dispatch even when an SLA policy
# would otherwise cover them. TEAMS_CHAT is here because actual chat-message
# backup runs through TEAMS_CHAT_EXPORT (one delta pull per user, not per chat);
# the TEAMS_CHAT rows remain as the user-facing catalog entity for restore.
SCHEDULER_IGNORED_TYPES: set[str] = {"TEAMS_CHAT"}


def resource_type_enabled(resource_type: str, policy: SlaPolicy) -> bool:
    """Check if a resource type is enabled in the SLA policy's backup flags."""
    if resource_type in SCHEDULER_IGNORED_TYPES:
        return False

    if resource_type == "ENTRA_USER":
        return bool(
            getattr(policy, "backup_entra_id", False)
            or getattr(policy, "contacts", False)
            or getattr(policy, "calendars", False)
        )

    if resource_type in {"ENTRA_GROUP", "DYNAMIC_GROUP"}:
        return bool(
            getattr(policy, "backup_entra_id", False)
            or getattr(policy, "group_mailbox", False)
        )

    flag_name = RESOURCE_TYPE_TO_SLA_FLAG.get(resource_type)
    if not flag_name:
        # Unknown or unsupported resource types should not be scheduled automatically.
        return False
    return getattr(policy, flag_name, True)


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

    await message_bus.connect()

    # Dynamically schedule backup jobs for each active SLA policy
    await schedule_all_policies()

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
                        status = (CASE WHEN c.n_items > 0 THEN 'COMPLETED'
                                       ELSE 'FAILED' END)::snapshotstatus,
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
                            -- Population A + C: explicit cancellation marker.
                            (s.extra_data::jsonb ->> 'cancelled_at') IS NOT NULL
                          ) OR (
                            -- Population B: late-arrival orphans.
                            s.status = 'IN_PROGRESS'
                            AND j.status IN ('CANCELLED', 'FAILED')
                          )
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
                resource_type = (row.resource_type or "generic").lower().replace("_", "-")
                tenant_short = (row.tenant_id or "").replace("-", "")[:8]
                container = f"backup-{resource_type}-{tenant_short}"

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
                    for blob_path, backend_id in items:
                        try:
                            store = _storage_router.get_store_by_id(str(backend_id))
                            await store.delete(container, blob_path)
                            blobs_deleted += 1
                        except Exception as bx:
                            blob_errors += 1
                            print(
                                f"[backup-scheduler] cancelled_sweep "
                                f"blob delete failed sid={sid} "
                                f"path={blob_path}: {bx}"
                            )

                    # Step 2 — DB cleanup (items first, then snapshot).
                    # Re-confirms candidacy inside the txn so a snapshot
                    # that COMPLETED between SELECT and DELETE doesn't
                    # get nuked.
                    async with async_session_factory() as ds:
                        await ds.execute(
                            text(
                                "DELETE FROM snapshot_items WHERE snapshot_id = :sid"
                            ),
                            {"sid": sid},
                        )
                        # Only delete the snapshot row if it still
                        # qualifies (cancellation marker present OR
                        # owning job is CANCELLED/FAILED). Protects
                        # against the rare race where a partition
                        # finalizer flipped status to COMPLETED while
                        # we were deleting items.
                        res = await ds.execute(
                            text(
                                "DELETE FROM snapshots s "
                                " USING jobs j "
                                " WHERE s.id = :sid "
                                "   AND s.job_id = j.id "
                                "   AND ( (s.extra_data::jsonb ->> 'cancelled_at') IS NOT NULL "
                                "         OR (s.status = 'FAILED' "
                                "             AND j.status IN ('CANCELLED','FAILED')) "
                                "         OR (s.status = 'IN_PROGRESS' "
                                "             AND j.status IN ('CANCELLED','FAILED')) )"
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
            from shared.tier2_discovery import find_users_missing_tier2
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
                await _mb.publish(
                    "discovery.tier2",
                    {
                        "tenantId": tid,
                        "userResourceIds": user_ids,
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


async def schedule_policy_job(policy: SlaPolicy):
    """Schedule or reschedule a backup job for a specific SLA policy.

    MANUAL policies are intentionally never scheduled — they only run when
    explicitly triggered via the /trigger endpoint. afi behaves identically."""
    job_id = f"policy_backup_{policy.id}"

    # Remove existing job if it exists (covers reschedule + frequency change to MANUAL)
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)

    cron_params = frequency_to_cron_params(
        policy.frequency, policy.backup_days, policy.backup_window_start,
        policy_id=str(policy.id),
    )
    if cron_params is None:
        print(f"[SCHEDULER] Skipping '{policy.name}' (MANUAL policy — admin-triggered only)")
        return

    scheduler.add_job(
        dispatch_policy_backups,
        args=[str(policy.id)],
        id=job_id,
        name=f"Backup: {policy.name}",
        replace_existing=True,
        timezone="UTC",
        **cron_params,
    )

    days_info = f" days={policy.backup_days}" if policy.backup_days and len(policy.backup_days) < 7 else ""
    print(f"[SCHEDULER] Scheduled '{policy.name}' ({policy.frequency}{days_info}) -> job_id={job_id}")


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "backup-scheduler"}


@app.post("/scheduler/policy/{policy_id}/trigger")
async def trigger_policy_backup(policy_id: str, background_tasks: BackgroundTasks):
    """Manually trigger backup dispatch for a specific SLA policy"""
    background_tasks.add_task(dispatch_policy_backups, policy_id)
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


async def dispatch_policy_backups(policy_id: str):
    """
    Dispatch backups for a specific SLA policy, filtering resources by the policy's backup flags.

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

        # R2.2 — honor max_concurrent_backups. Count Jobs already in flight for
        # this policy (QUEUED + RUNNING) and cap how many new resources we
        # publish this tick. The remainder will be picked up by the next
        # scheduled trigger — we'd rather spread load across cycles than
        # overload Graph and induce 429 throttling.
        max_concurrent = getattr(policy, "max_concurrent_backups", None) or 0
        if max_concurrent > 0:
            inflight_stmt = (
                select(func.count(Job.id)).where(and_(
                    Job.spec["sla_policy_id"].astext == str(policy.id),
                    Job.status.in_([JobStatus.QUEUED, JobStatus.RUNNING]),
                ))
            )
            inflight = (await session.execute(inflight_stmt)).scalar() or 0
            budget = max(0, max_concurrent - inflight)
            if budget == 0:
                print(f"[SCHEDULER] Policy '{policy.name}' at concurrency cap "
                      f"({inflight}/{max_concurrent} in-flight) — deferring this tick")
                return
            if budget < len(enabled_resources):
                print(f"[SCHEDULER] Policy '{policy.name}' concurrency cap "
                      f"({inflight}/{max_concurrent} in-flight) — dispatching {budget} of {len(enabled_resources)} resources")
                # Prefer the resources with the OLDEST last_backup — they're the
                # most behind on SLA. None means never backed up → highest priority.
                enabled_resources.sort(
                    key=lambda r: (r.last_backup_at or datetime.min)
                )
                enabled_resources = enabled_resources[:budget]

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

        print(f"[SCHEDULER] Found {len(enabled_resources)} resources to backup for policy '{policy.name}'")

        # Group by resource type + tenant
        groups: Dict[str, List[Resource]] = {}
        for resource in enabled_resources:
            group_key = f"{resource.type.value}:{resource.tenant_id}"
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(resource)

        print(f"[SCHEDULER] Grouped into {len(groups)} resource type + tenant combinations")

        # Determine queue based on frequency (frequent = higher priority queue)
        queue = "backup.high" if policy.frequency == "THREE_DAILY" else "backup.normal"

        # Dispatch batches
        total_dispatched = 0
        BATCH_SIZE = 1000

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

            # Split into batches
            for i in range(0, len(group_resources), BATCH_SIZE):
                batch = group_resources[i:i + BATCH_SIZE]
                resource_ids = [str(r.id) for r in batch]

                # Determine fullBackup: True only if NONE of the resources have been backed up before
                has_previous_backup = any(r.last_backup_at is not None for r in batch)
                effective_full_backup = not has_previous_backup

                # Create job record for tracking
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
                    }
                )
                session.add(job)

                # Create mass backup message
                message = create_mass_backup_message(
                    job_id=str(job_id),
                    tenant_id=tenant_id,
                    resource_type=resource_type,
                    resource_ids=resource_ids,
                    sla_policy_id=str(policy.id),
                    full_backup=effective_full_backup,
                )

                # Send to queue
                await message_bus.publish(
                    group_queue,
                    message,
                    priority=message["priority"],
                )

                await emit_backup_triggered(
                    job=job, resource=None, tenant=None,
                    trigger_label="SCHEDULED",
                    actor_type="SYSTEM",
                    full_backup=effective_full_backup,
                    batch_resource_count=len(resource_ids),
                    extra_details={
                        "sla_policy_id": str(policy.id),
                        "sla_policy_name": policy.name,
                        "resource_type": resource_type,
                    },
                )

                total_dispatched += len(resource_ids)

                # Stagger dispatch to prevent overwhelming Graph API
                await asyncio.sleep(0.1)

        await session.commit()

        print(f"[SCHEDULER] Dispatched {total_dispatched} resources to {queue} for policy '{policy.name}'")


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
        
        violations = []
        for resource, policy in resources_result.all():
            if not policy:
                continue
            
            # Calculate expected backup frequency
            frequency_hours = {
                "THREE_DAILY": 8,  # Every 8 hours
                "DAILY": 24,
                "WEEKLY": 168,
            }.get(policy.frequency, 24)
            
            # Check if backup is overdue
            if resource.last_backup_at:
                hours_since_backup = (now - resource.last_backup_at).total_seconds() / 3600
                
                if hours_since_backup > frequency_hours:
                    severity = "CRITICAL" if hours_since_backup > frequency_hours * 2 else "WARNING"
                    
                    violations.append({
                        "resource_id": str(resource.id),
                        "tenant_id": str(resource.tenant_id),
                        "resource_type": resource.type.value,
                        "sla_policy_id": str(policy.id),
                        "sla_policy_name": policy.name,
                        "last_backup_at": resource.last_backup_at.isoformat(),
                        "hours_overdue": hours_since_backup - frequency_hours,
                        "severity": severity,
                    })
                    
                    # Log violation (in production: send to alert service)
                    print(f"[SLA VIOLATION] {severity}: Resource {resource.id} ({resource.type.value}) "
                          f"last backed up {hours_since_backup:.1f}h ago (SLA: {frequency_hours}h)")
        
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
                json={"report_type": "WEEKLY"}
            )
            if response.status_code == 200:
                print(f"[REPORT] Weekly report triggered successfully")
            else:
                print(f"[REPORT] Failed to trigger weekly report: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"[REPORT] Error triggering weekly report: {e}")


async def send_violations_to_alert_service(violations: List[Dict]):
    """Send SLA violations to alert-service"""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            for violation in violations:
                await client.post(f"{settings.ALERT_SERVICE_URL}/api/v1/alerts", json={
                    "type": "SLA_VIOLATION",
                    "severity": violation.get("severity", "WARNING"),
                    "message": f"SLA violation: {violation.get('resource_type')} resource overdue by "
                               f"{violation.get('hours_overdue', 0):.1f} hours",
                    "tenant_id": violation.get("tenant_id"),
                    "resource_id": violation.get("resource_id"),
                    "resource_type": violation.get("resource_type"),
                    "details": violation,
                })
    except Exception as e:
        print(f"[SLA] Failed to send violations to alert-service: {e}")


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

async def consume_backup_dlq():
    """Long-running consumer for backup.*.dlq queues — every poison message
    becomes an Alert so ops can investigate. Without this, dead-lettered
    messages sit in RabbitMQ forever with no signal to operators."""
    from shared.models import Alert
    dlq_queues = ["backup.urgent.dlq", "backup.high.dlq", "backup.normal.dlq", "backup.low.dlq"]

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
                try:
                    body = json.loads(msg.body.decode())
                    job_id = body.get("jobId")
                    resource_id = body.get("resourceId") or (body.get("resourceIds") or [None])[0]
                    workload = body.get("workload")
                    delivery_count = (msg.headers or {}).get("x-delivery-count", "?")
                    print(f"[DLQ] poison message in {queue_name}: job={job_id} resource={resource_id} workload={workload} delivery_count={delivery_count}")
                    async with async_session_factory() as session:
                        # Look up tenant via resource if available
                        tenant_id = None
                        if resource_id:
                            try:
                                from shared.models import Resource
                                r = await session.get(Resource, uuid.UUID(resource_id))
                                tenant_id = r.tenant_id if r else None
                            except Exception:
                                pass
                        session.add(Alert(
                            tenant_id=tenant_id,
                            type="BACKUP_DLQ",
                            severity="HIGH",
                            message=(
                                f"Backup message dead-lettered after {delivery_count} delivery attempts on {queue_name}. "
                                f"Job {job_id} for resource {resource_id} ({workload}) is stuck — investigate."
                            ),
                            resource_id=uuid.UUID(resource_id) if resource_id else None,
                            triggered_by="dlq-consumer",
                            details={
                                "queue": queue_name,
                                "delivery_count": delivery_count,
                                "job_id": job_id,
                                "resource_id": resource_id,
                                "workload": workload,
                            },
                        ))
                        await session.commit()
                    await msg.ack()
                except Exception as e:
                    # Ack anyway — re-rejecting would just re-queue to DLQ infinitely
                    print(f"[DLQ] failed to alert on poison message: {type(e).__name__}: {e}")
                    try:
                        await msg.ack()
                    except Exception:
                        pass

    await asyncio.gather(*[consume_one(q) for q in dlq_queues], return_exceptions=True)


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
                newer_stmt = select(Snapshot.id).where(and_(
                    Snapshot.resource_id == snap.resource_id,
                    Snapshot.status == SnapshotStatus.COMPLETED,
                    Snapshot.completed_at > snap.completed_at,
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
