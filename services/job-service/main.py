"""Job Service - Manages jobs, backup triggers, restore, and exports"""
from contextlib import asynccontextmanager
from typing import Optional, Dict, List
import uuid
from uuid import UUID, uuid4
from datetime import datetime, timezone
import json
import asyncio

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import select, func, text

from shared.config import settings
from shared.database import get_db, close_db, AsyncSession, engine
from shared.models import Job, JobLog, JobType, JobStatus, Resource, Snapshot, SnapshotItem, SnapshotStatus, SlaPolicy, ResourceType, ResourceStatus, BackupBatch
from shared.schemas import (
    JobResponse, JobListResponse, TriggerBackupRequest, TriggerBulkBackupRequest, TriggerDatasourceBackupRequest
)
from shared.message_bus import message_bus, create_backup_message, create_restore_message
from shared.batch_pending import classify_scope, BatchPendingState
from shared.audit import emit_backup_triggered


def _parse_uuid(value: Optional[str], field_name: str) -> Optional[UUID]:
    """Parse a UUID from external input, raising 400 on malformed strings.

    Bare `UUID(value)` raises ValueError which FastAPI surfaces as 500 — a
    request that's only malformed user input. Every handler that takes a
    UUID-string query/body param should funnel through here so an invalid
    cache key in localStorage (or a curl probe with a typo) reads as a
    clean 400 instead of waking oncall.
    """
    if not value:
        return None
    try:
        return UUID(value)
    except (ValueError, TypeError):
        raise HTTPException(
            status_code=400, detail=f"Invalid {field_name}: not a UUID",
        )

# AZ-4: Azure workload resources go to dedicated queues (not backup.*)
AZURE_WORKLOAD_QUEUES = {
    "AZURE_VM": "azure.vm",
    "AZURE_SQL_DB": "azure.sql",
    "AZURE_SQL": "azure.sql",
    "AZURE_POSTGRESQL": "azure.postgres",
    "AZURE_POSTGRESQL_SINGLE": "azure.postgres",
    "AZURE_PG": "azure.postgres",
}

M365_RESOURCE_TYPES = [
    # Tier-1 mailbox containers that don't have a Tier-2 sibling: shared
    # mailboxes + room mailboxes live at the tenant level (not under any
    # ENTRA_USER), so they stay here and continue to back up via the
    # backup_mailbox handler.
    # User MAILBOX + user ONEDRIVE are intentionally EXCLUDED — the
    # canonical home for user mail / drive content is the Tier-2 child
    # rows (USER_MAIL / USER_ONEDRIVE) listed below. Including the Tier-1
    # peers here would re-walk the same content for every user and double
    # the per-backup Graph + storage cost (see shared/storage_rollup.py
    # for the read-side dedup that kept totals correct while legacy rows
    # still existed).
    ResourceType.SHARED_MAILBOX,
    ResourceType.ROOM_MAILBOX,
    # Tier-2 per-workload children. Each ENTRA_USER discovered today
    # gets one of each — USER_MAIL/USER_CALENDAR/USER_CONTACTS/
    # USER_ONEDRIVE/USER_CHATS — and the actual backup data lives in
    # these rows, not on the parent ENTRA_USER. Earlier versions of this
    # list omitted them, which made "Backup all M365 now" silently skip
    # ~60% of eligible content.
    ResourceType.USER_MAIL,
    ResourceType.USER_CALENDAR,
    ResourceType.USER_CONTACTS,
    ResourceType.USER_ONEDRIVE,
    ResourceType.USER_CHATS,
    # Group + SharePoint resources.
    ResourceType.M365_GROUP,
    ResourceType.SHAREPOINT_SITE,
    ResourceType.TEAMS_CHANNEL,
    ResourceType.TEAMS_CHAT,
    # Identity surface — Entra metadata is its own backup pipeline
    # (entra-export worker), still triggered by the same datasource hit.
    ResourceType.ENTRA_USER,
    ResourceType.ENTRA_GROUP,
    ResourceType.ENTRA_APP,
    ResourceType.ENTRA_DEVICE,
    ResourceType.ENTRA_SERVICE_PRINCIPAL,
    # Power Platform.
    ResourceType.POWER_BI,
    ResourceType.POWER_APPS,
    ResourceType.POWER_AUTOMATE,
    ResourceType.POWER_DLP,
    # Misc M365 surfaces backed by their own workers.
    ResourceType.COPILOT,
    ResourceType.PLANNER,
    ResourceType.TODO,
    ResourceType.ONENOTE,
]

AZURE_RESOURCE_TYPES = [
    ResourceType.AZURE_VM,
    ResourceType.AZURE_SQL_DB,
    ResourceType.AZURE_POSTGRESQL,
    ResourceType.AZURE_POSTGRESQL_SINGLE,
    ResourceType.RESOURCE_GROUP,
]


async def _redirect_teams_chat_to_export(db: AsyncSession, resource: Resource) -> Resource:
    """If `resource` is a per-chat TEAMS_CHAT row, return the matching
    per-user TEAMS_CHAT_EXPORT resource (one delta pull per user covers
    all their chats). If no matching export exists yet, return the
    original so it drains through the legacy handler.

    For group chats multiple users carry the same chatId in their export's
    chatIds. Prefer the user whose Graph id appears as a key in the source
    TEAMS_CHAT's metadata.chat_delta_tokens — that's the user the legacy
    drain path used, so the fast path stays routed to the same shard. If no
    such hint exists (chats discovered post-refactor), fall back to the
    first chatIds match.

    TEAMS_CHAT_EXPORT is UI-hidden, so its SLA is never set by the user.
    Inherit SLA from the source TEAMS_CHAT so the trigger check passes."""
    if resource.type != ResourceType.TEAMS_CHAT:
        return resource
    chat_external_id = resource.external_id
    stmt = select(Resource).where(
        Resource.tenant_id == resource.tenant_id,
        Resource.type == ResourceType.TEAMS_CHAT_EXPORT,
    )
    rows = (await db.execute(stmt)).scalars().all()

    candidates = [
        r for r in rows
        if chat_external_id in (r.extra_data or {}).get("chatIds", [])
    ]
    if not candidates:
        return resource

    preferred_user_ids = set(
        (resource.extra_data or {}).get("chat_delta_tokens", {}).keys()
    )
    matched = None
    if preferred_user_ids:
        matched = next(
            (r for r in candidates if r.external_id in preferred_user_ids),
            None,
        )
    if matched is None:
        matched = candidates[0]
    if matched.sla_policy_id is None and resource.sla_policy_id is not None:
        matched.sla_policy_id = resource.sla_policy_id
        await db.commit()
        await db.refresh(matched)
        print(
            f"[JOB_SERVICE] Inherited SLA {resource.sla_policy_id} "
            f"from TEAMS_CHAT {resource.id} to TEAMS_CHAT_EXPORT {matched.id}"
        )
    print(
        f"[JOB_SERVICE] TEAMS_CHAT {resource.id} ({chat_external_id}) "
        f"→ TEAMS_CHAT_EXPORT {matched.id} (user {matched.external_id})"
    )
    return matched


async def _create_batch_backup_jobs(
    resources_map: Dict[str, Resource],
    db: AsyncSession,
    full_backup: bool = True,
    priority: int = 1,
    note: Optional[str] = None,
    trigger_label: str = "MANUAL_BATCH",
    batch_id: Optional[str] = None,
    tier2: bool = False,
):
    if not resources_map:
        raise HTTPException(status_code=404, detail="No valid resources found")

    # ── Batch-level debounce — protects against rapid duplicate triggers.
    #
    # The existing G1 dedup (further below) checks for IN_PROGRESS
    # snapshots on the requested resource_ids. That misses the
    # MANUAL_DATASOURCE_M365 case where the bulk request carries
    # ENTRA_USER ids but the in-flight snapshots live on Tier-2 children
    # (USER_CHATS, USER_MAIL, ...) — different resource_ids, so the snapshot
    # check doesn't trip. Result observed 2026-05-17: two manual_bulk
    # batches 43 seconds apart (88fd531b + 21d856e7) each dispatched a
    # USER_CHATS job for Gajraj — one finished with 0 items (ghost
    # snapshot), the other anchored against the ghost and stamped a bogus
    # 64MB bytes_added.
    #
    # This guard runs BEFORE the batch INSERT. If a manual_bulk batch for
    # the same tenant is IN_PROGRESS and was created in the last 30s,
    # treat the new trigger as a duplicate and return a pointer to the
    # existing batch instead of creating a parallel run.
    if not tier2:
        try:
            sample_res = next(iter(resources_map.values()))
            tenant_for_debounce = str(sample_res.tenant_id)
            async with db.begin_nested():
                existing = (await db.execute(
                    text(
                        """
                        SELECT id::text AS bid, created_at
                          FROM backup_batches
                         WHERE tenant_id = cast(:tid AS uuid)
                           AND source = 'manual_bulk'
                           AND status = 'IN_PROGRESS'
                           AND created_at > NOW() - INTERVAL '30 seconds'
                           AND (cast(:bid AS uuid) IS NULL OR id <> cast(:bid AS uuid))
                         ORDER BY created_at DESC
                         LIMIT 1
                        """
                    ),
                    {"tid": tenant_for_debounce, "bid": batch_id},
                )).first()
            if existing is not None:
                existing_bid = str(existing[0])
                print(
                    f"[JOB_SERVICE] BATCH_DEBOUNCE: duplicate manual_bulk "
                    f"trigger within 30s for tenant={tenant_for_debounce} — "
                    f"returning existing batch {existing_bid} instead of "
                    f"creating new batch {batch_id}"
                )
                return [{
                    "jobId": None,
                    "status": "RUNNING",
                    "resourceId": "BATCH",
                    "resourceCount": 0,
                    "queue": "deduped_batch",
                    "deduped": True,
                    "duplicateBatchId": existing_bid,
                    "message": (
                        "Another bulk backup just started for this tenant; "
                        "tracking the existing batch."
                    ),
                }]
        except Exception as _debounce_exc:
            # Never block a real trigger on a debounce-check failure.
            print(f"[JOB_SERVICE] BATCH_DEBOUNCE check failed (non-fatal): {_debounce_exc}")

    # Ensure a backup_batches row exists for batch_id. Idempotent:
    # when tenant-service already inserted the row, ON CONFLICT keeps
    # its source/actor_email/bytes_expected. When the caller is the UI
    # "Backup all" (no batchId supplied → fresh uuid4 generated above),
    # this is the first INSERT for that row.
    #
    # Source = manual_bulk for trigger-bulk callers (frontend & datasource);
    # tenant-service's pre-insert already used 'manual_user' so its row
    # is untouched. Tier-2 fanout (tier2=True) does NOT insert — those
    # waves inherit an existing batch_id from their parent click.
    if batch_id and not tier2:
        try:
            sample_res = next(iter(resources_map.values()))
            tenant_for_batch = sample_res.tenant_id
            # scope_user_ids represents the OPERATOR'S intent — what the
            # user/operator clicked. Tier-2 child rows (USER_MAIL /
            # USER_CHATS / USER_ONEDRIVE / USER_CALENDAR / USER_CONTACTS)
            # are discovered automatically as children of ENTRA_USER and
            # MUST NOT appear in the scope: putting them in inflates
            # array_length(scope_user_ids,1) so the UI Activity row says
            # "54 users" when in reality 9 ENTRA_USER + 45 Tier-2 children
            # are present. The batch finalizer gate-1 already expands
            # ENTRA_USER scope entries into Tier-2 children at check
            # time (shared/batch_rollup.py:_finalize_batch_if_complete),
            # so excluding them here doesn't lose coverage.
            _T2_EXCLUDE = {
                ResourceType.USER_MAIL,
                ResourceType.USER_CALENDAR,
                ResourceType.USER_CONTACTS,
                ResourceType.USER_ONEDRIVE,
                ResourceType.USER_CHATS,
            }
            scope_ids = [
                r.id for r in resources_map.values()
                if r.type not in _T2_EXCLUDE
            ]
            await db.execute(text("""
                INSERT INTO backup_batches
                    (id, tenant_id, source, scope_user_ids, status, created_at)
                VALUES
                    (cast(:bid AS uuid), cast(:tid AS uuid), :src,
                     cast(:scope AS uuid[]), 'IN_PROGRESS', NOW())
                ON CONFLICT (id) DO NOTHING
            """), {
                "bid": batch_id,
                "tid": str(tenant_for_batch),
                "src": "manual_bulk",
                "scope": [str(x) for x in scope_ids],
            })
            await db.commit()
        except Exception as _e:
            # Non-fatal — flag-off behaviour: legacy CTE still works
            # if the row never lands; flag-on path will surface this via
            # the validation check below.
            print(f"[JOB_SERVICE] backup_batches INSERT failed (non-fatal): {_e}")
            try:
                await db.rollback()
            except Exception:
                pass

        # ── Classify scope: ready vs deferred ──────────────────────
        # `tier2_owners`: scoped ENTRA_USERs that already have at
        # least one non-archived Tier-2 child resource. Discovery has
        # completed for these (or wasn't needed). Anything not in this
        # set is `deferred` — discovery hasn't produced children yet,
        # so per-user backup needs to wait. The new
        # batch_pending_users row tracks each deferred user's state
        # machine; the discovery-worker's `thenBackup=True` chain (see
        # spec §3) enqueues the backup once discovery completes.
        # Closes the 2026-05-15 incident where 45 of 54 SLA'd users
        # had no Tier-2 children at click time so their backup never
        # ran and the batch hung at IN_PROGRESS forever.
        try:
            tier2_rows = (await db.execute(text("""
                SELECT DISTINCT parent_resource_id
                  FROM resources
                 WHERE parent_resource_id = ANY(cast(:ids AS uuid[]))
                   AND archived_at IS NULL
            """), {"ids": [str(x) for x in scope_ids]})).all()
            tier2_owners = {row[0] for row in tier2_rows if row[0] is not None}

            ready, deferred = classify_scope(scope_ids, tier2_owners)

            if deferred:
                from datetime import datetime as _dt, timedelta as _td
                deadline = _dt.utcnow() + _td(
                    minutes=settings.DISCOVERY_DEADLINE_MIN,
                )
                # One INSERT per deferred user. ON CONFLICT keeps this
                # idempotent under bulk-trigger redelivery (RabbitMQ
                # visibility-timeout / NACK can replay the message).
                # Sequential INSERTs over a small list (<10k) — no
                # need for an executemany dance.
                for uid in deferred:
                    await db.execute(text("""
                        INSERT INTO batch_pending_users
                            (batch_id, user_id, state, deadline_at)
                        VALUES
                            (cast(:bid AS uuid), cast(:uid AS uuid),
                             :st, :dl)
                        ON CONFLICT (batch_id, user_id) DO NOTHING
                    """), {
                        "bid": batch_id,
                        "uid": str(uid),
                        "st": BatchPendingState.WAITING_DISCOVERY,
                        "dl": deadline,
                    })
                await db.commit()

                # Publish chained discovery per tenant. `thenBackup=True`
                # tells discovery-worker to POST /backups/trigger-bulk
                # for the children it creates, propagating batch_id so
                # those backups land in the same Activity row.
                # resources_map keys are strings (Dict[str, Resource]),
                # but `deferred` holds UUID objects (from Resource.id).
                # Stringify the lookup key — direct `.get(uid)` would
                # miss every entry and silently drop all chained
                # discovery publishes.
                by_tenant: dict = {}
                for uid in deferred:
                    res = resources_map.get(str(uid))
                    if res is None:
                        continue
                    by_tenant.setdefault(str(res.tenant_id), []).append(str(uid))
                for tid, uids in by_tenant.items():
                    try:
                        await message_bus.publish("discovery.tier2", {
                            "tenantId": tid,
                            "userResourceIds": uids,
                            "source": "BULK_TRIGGER",
                            "thenBackup": True,
                            "batchId": batch_id,
                        }, priority=5)
                    except Exception as _pub_err:
                        # Non-fatal: scheduler watchdog flips the
                        # pending rows to DISCOVERY_FAILED after
                        # deadline_at so the batch can still
                        # terminalize as PARTIAL.
                        print(
                            f"[JOB_SERVICE] chained discovery.tier2 "
                            f"publish failed for tenant={tid}: "
                            f"{_pub_err} — watchdog will reclaim "
                            f"(deadline_at={deadline.isoformat()})"
                        )
        except Exception as _cls_err:
            # Non-fatal — ready path still runs; watchdog picks up
            # the missing pending rows after deadline.
            print(
                f"[JOB_SERVICE] batch classify+publish failed "
                f"(non-fatal): {_cls_err}"
            )
            try:
                await db.rollback()
            except Exception:
                pass

    # When the feature flag is on, the row MUST exist by now (either via
    # tenant-service pre-insert or the ON CONFLICT block above). Reject
    # if missing so a forgotten propagation site doesn't silently break
    # Activity grouping.
    if settings.BATCH_ROW_REDESIGN_ENABLED and batch_id and not tier2:
        exists_row = (await db.execute(text("""
            SELECT 1 FROM backup_batches WHERE id = cast(:bid AS uuid) LIMIT 1
        """), {"bid": batch_id})).first()
        if not exists_row:
            raise HTTPException(
                status_code=400,
                detail=f"backup_batches row {batch_id} not found "
                       f"(BATCH_ROW_REDESIGN_ENABLED). Caller forgot to "
                       f"INSERT before trigger-bulk.",
            )

    resources_without_sla = [rid for rid, res in resources_map.items() if not res.sla_policy_id]
    if resources_without_sla:
        raise HTTPException(
            status_code=400,
            detail=f"Resources must have SLA policies assigned. {len(resources_without_sla)} resource(s) missing policy: {', '.join(resources_without_sla[:5])}"
        )

    # G1 — cross-bulk per-resource dedup. If the operator clicks "Backup
    # all M365 now" three times in quick succession, we DO NOT want
    # three backups for the same users: it triples Graph load + storage
    # writes for zero new data. Filter requested resources against any
    # IN_PROGRESS snapshot in the last hour. The 1-hour window matches
    # the scheduler stale-sweep horizon (30 min idle threshold + 30 min
    # safety) so a worker that genuinely died mid-flight gets re-
    # scheduled on the next click after sweep reaps it.
    #
    # Three outcomes:
    #   * All overlap → return deduped pointer to the existing in-flight
    #     bulk Job ID(s), no new Job created, no new Activity row.
    #   * Partial overlap → trim resources_map to the residual set so
    #     just-arrived users still get backed up.
    #   * No overlap → normal flow.
    #
    # Defense in depth: the worker fan-out path also runs the same
    # dedup query in _fanout_bulk_to_per_resource to catch the race
    # window between trigger and worker pickup.
    dedup_skipped: List[str] = []
    # Run dedup inside a SAVEPOINT so any failure (schema drift, transient
    # error) can be rolled back without poisoning the outer transaction —
    # Postgres marks the entire tx aborted on the first statement error,
    # and the downstream INSERT into jobs would then fail with
    # InFailedSQLTransactionError. The schema qualifier is intentionally
    # omitted; the connection's search_path already points at DB_SCHEMA
    # (default "tm") so unqualified names resolve correctly in both local
    # and Railway environments.
    try:
        requested_ids = list(resources_map.keys())
        async with db.begin_nested():
            inflight_rows = (await db.execute(
                text(
                    """
                    SELECT DISTINCT s.resource_id, s.job_id
                    FROM snapshots s
                    WHERE s.resource_id = ANY(:rids)
                      AND s.status = 'IN_PROGRESS'
                      AND s.started_at > NOW() - INTERVAL '1 hour'
                    UNION
                    SELECT DISTINCT bid AS resource_id, j.id AS job_id
                      FROM jobs j
                      CROSS JOIN LATERAL unnest(
                          COALESCE(j.batch_resource_ids, ARRAY[]::uuid[])
                      ) AS bid
                     WHERE j.status::text IN ('QUEUED','RUNNING')
                       AND bid = ANY(cast(:rids AS uuid[]))
                       AND COALESCE(j.spec->>'batch_id', '') = COALESCE(:bid, '')
                    """
                ),
                {"rids": requested_ids, "bid": batch_id or ''},
            )).all()
        if inflight_rows:
            inflight_resource_ids = {str(r[0]) for r in inflight_rows}
            inflight_job_ids = {str(r[1]) for r in inflight_rows if r[1]}
            dedup_skipped = [rid for rid in requested_ids if rid in inflight_resource_ids]
            for rid in dedup_skipped:
                resources_map.pop(rid, None)
            print(
                f"[JOB_SERVICE] G1 dedup: {len(dedup_skipped)}/{len(requested_ids)} "
                f"resources already in-flight, trimmed from this bulk "
                f"(parents: {sorted(inflight_job_ids)[:3]}{'...' if len(inflight_job_ids) > 3 else ''})"
            )
            if not resources_map:
                # All overlap — every requested resource is already
                # being backed up by an earlier click. Don't create a
                # new Job; point the caller at the existing in-flight
                # parent(s) so the UI keeps polling the same row.
                parent_job_id = sorted(inflight_job_ids)[0] if inflight_job_ids else None
                return [{
                    "jobId": parent_job_id,
                    "status": "RUNNING",
                    "resourceId": "BATCH",
                    "resourceCount": 0,
                    "queue": "deduped",
                    "deduped": True,
                    "alreadyInFlight": len(dedup_skipped),
                    "message": (
                        "All requested resources already being backed up — "
                        "tracking the existing in-flight job."
                    ),
                }]
    except Exception as dedup_exc:
        # Best-effort dedup — on DB hiccup, fall through and let the
        # worker-side G2 catch the duplicates. Never silently swallow
        # a real schema problem: surface it so we notice.
        print(f"[JOB_SERVICE] G1 dedup query failed (non-fatal): {dedup_exc}")
        dedup_skipped = []

    # Partition by (tenant, queue). Oversized OneDrive children land in a
    # separate (tenant, backup.heavy) bucket so they don't get stuck
    # behind light resources on the shared urgent queue, and vice-versa
    # so small resources don't wait behind a 500 GB drive scan.
    from shared.export_routing import pick_backup_queue
    tenant_queue_groups: Dict[tuple, List[str]] = {}
    for rid, res in resources_map.items():
        res_type = res.type.value if hasattr(res.type, "value") else str(res.type)
        drive_bytes = int((res.extra_data or {}).get("drive_quota_used", 0))
        routing_key = pick_backup_queue(
            drive_bytes_estimate=drive_bytes,
            resource_type=res_type,
            default_queue="backup.urgent",
        )
        tenant_queue_groups.setdefault((res.tenant_id, routing_key), []).append(rid)

    jobs_created = []
    pending_publishes: List[tuple] = []   # (routing_key, msg)
    # Keep the ORM Job objects alive alongside jobs_created so we can
    # emit one BACKUP_TRIGGERED per queued job below — Activity groups
    # these per-job so a single batch request becomes one row per
    # (tenant, routing_key) split.
    job_objects: List[Job] = []

    for (tenant_id, routing_key), resource_ids in tenant_queue_groups.items():
        has_previous_backup = any(resources_map[rid].last_backup_at is not None for rid in resource_ids)
        effective_full_backup = (full_backup or False) and not has_previous_backup

        job = Job(
            id=uuid4(), type=JobType.BACKUP,
            tenant_id=tenant_id,
            resource_id=None,
            batch_resource_ids=[UUID(rid) for rid in resource_ids],
            status=JobStatus.QUEUED, priority=priority,
            progress_pct=0, items_processed=0, bytes_processed=0,
            spec={
                "triggered_by": trigger_label,
                "resource_count": len(resource_ids),
                "fullBackup": effective_full_backup,
                "note": note,
                "queue": routing_key,
                # batch_id stitches every Job that came from one operator
                # click together — even when those Jobs are produced in
                # separate stages (parent-bulk + Tier-2 child fan-out).
                # Audit grouping keys on this first; falls back to
                # (tenant, triggered_by, created_at) for legacy rows.
                "batch_id": batch_id,
                # True for the Tier-2 fan-out wave: the user-clicked
                # resources are already counted on the parent siblings,
                # so audit-service excludes these from the Activity-row
                # "X resources" total to avoid double-counting (an
                # 18-user click stays "18 resources" even after 36
                # OneDrives + 9 mailboxes are discovered and queued).
                # The work is still shown — these siblings still drive
                # status / progress — only the displayed count omits
                # them.
                "tier2": bool(tier2),
            },
        )
        db.add(job)
        job_objects.append(job)
        jobs_created.append({
            "jobId": str(job.id),
            "status": "QUEUED",
            "resourceId": "BATCH",
            "resourceCount": len(resource_ids),
            "queue": routing_key,
        })

        if settings.RABBITMQ_ENABLED:
            from shared.message_bus import create_mass_backup_message
            first_res = resources_map[resource_ids[0]]
            resource_type = first_res.type.value if hasattr(first_res.type, 'value') else str(first_res.type)
            pending_publishes.append((routing_key, create_mass_backup_message(
                job_id=str(job.id),
                tenant_id=str(tenant_id),
                resource_type=resource_type,
                resource_ids=resource_ids,
                sla_policy_id=None,
                full_backup=effective_full_backup,
            )))

    await db.commit()

    for routing_key, msg in pending_publishes:
        print(f"[JOB_SERVICE] batch backup → {routing_key} ({msg.get('batchSize', len(msg.get('resourceIds', [])))} resources)")
        await message_bus.publish(routing_key, msg, priority=priority)

    for queued_job in job_objects:
        await emit_backup_triggered(
            job=queued_job,
            tenant=None,  # BATCH rows carry tenant_id via the Job itself
            trigger_label=trigger_label,
            full_backup=full_backup or False,
            batch_resource_count=len(queued_job.batch_resource_ids or []),
            extra_details={"note": note, "batch": True} if note else {"batch": True},
        )

    if dedup_skipped:
        # Partial-overlap callers: annotate the response so the UI can
        # surface "Scheduled M resources; N already in-flight". Without
        # this the operator sees only the smaller M and wonders where
        # the rest of their click went.
        for j in jobs_created:
            j["dedupedCount"] = len(dedup_skipped)
    return jobs_created


@asynccontextmanager
async def lifespan(app: FastAPI):
    from shared.storage.startup import startup_router, shutdown_router
    from shared import core_metrics
    core_metrics.init()
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
    await message_bus.connect()
    await startup_router()
    try:
        yield
    finally:
        await shutdown_router()
        await message_bus.disconnect()
        await close_db()


app = FastAPI(title="Job Service", version="1.0.0", lifespan=lifespan)

# Chat-export router — /api/v1/exports/chat/* (trigger, estimate, SSE, cancel, delete).
# Imported after app creation so the module's router instance is registered.
from chat_export import router as chat_export_router  # noqa: E402
app.include_router(chat_export_router)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "job"}


async def _job_rollup_bulk(db: AsyncSession, job_ids: List[UUID]) -> Dict[str, Dict]:
    """Derive live progress / item / byte rollups from the snapshots table.

    Source of truth: ``snapshots``. The Job row's own progress_pct and
    items_processed columns are NOT maintained per-snapshot — they get
    written once at start (5%) and once at terminal (100%), with stale
    group-level pings in between. Reading from them gives the operator
    a stuck-at-35% experience even when every child snapshot is done
    (observed Railway 2026-05-13 on job ad2868bd-…).

    This helper aggregates COMPLETED/FAILED/IN_PROGRESS snapshots for
    each job_id in one SQL round-trip (covered by the existing
    snapshots(job_id) index). Empty rollup is returned for job_ids
    with no child snapshots (QUEUED, or single-snapshot already
    deleted).
    """
    if not job_ids:
        return {}
    rows = (await db.execute(text("""
        SELECT
            job_id,
            COUNT(*)                                       AS total,
            COUNT(*) FILTER (WHERE status='COMPLETED')     AS done,
            COUNT(*) FILTER (WHERE status='FAILED')        AS failed,
            COUNT(*) FILTER (WHERE status='IN_PROGRESS')   AS inflight,
            COALESCE(SUM(item_count)  FILTER (WHERE status IN ('COMPLETED','PARTIAL')), 0) AS items,
            COALESCE(SUM(bytes_added) FILTER (WHERE status IN ('COMPLETED','PARTIAL')), 0) AS bytes
        FROM snapshots
        WHERE job_id = ANY(:jids)
        GROUP BY job_id
    """), {"jids": [str(j) for j in job_ids]})).fetchall()
    out: Dict[str, Dict] = {}
    for r in rows:
        total = int(r.total or 0)
        terminal = int(r.done or 0) + int(r.failed or 0)
        progress = int(round(100.0 * terminal / total)) if total else 0
        out[str(r.job_id)] = {
            "progress": progress,
            "items": int(r.items or 0),
            "bytes": int(r.bytes or 0),
            "snapshots_total": total,
            "snapshots_completed": int(r.done or 0),
            "snapshots_failed": int(r.failed or 0),
            "snapshots_in_progress": int(r.inflight or 0),
        }
    return out


def _project_job_progress(job: "Job", roll: Optional[Dict]) -> int:
    """Apply the same terminal-state policy used by the UI: COMPLETED
    pegs to 100, CANCELLED preserves prior, otherwise we use the live
    rollup. Without this a COMPLETED job whose snapshots were retention-
    pruned would show 0%.
    """
    status = job.status.value if hasattr(job.status, "value") else str(job.status)
    if status == "COMPLETED":
        return 100
    if status == "FAILED":
        # Best of: rollup (if there are some completed snapshots, surface
        # the partial-success %), else fall back to stored column.
        if roll and roll.get("snapshots_total"):
            return roll["progress"]
        return job.progress_pct or 0
    if status == "CANCELLED":
        return job.progress_pct or 0
    if roll:
        return roll["progress"]
    return job.progress_pct or 0


def _build_job_response(job: "Job", roll: Optional[Dict]) -> JobResponse:
    return JobResponse(
        id=str(job.id),
        type=job.type.value if hasattr(job.type, "value") else str(job.type),
        status=job.status.value if hasattr(job.status, "value") else str(job.status),
        progress=_project_job_progress(job, roll),
        resourceId=str(job.resource_id) if job.resource_id else None,
        tenantId=str(job.tenant_id) if job.tenant_id else None,
        createdAt=job.created_at.isoformat() if job.created_at else "",
        updatedAt=job.updated_at.isoformat() if job.updated_at else "",
        completedAt=job.completed_at.isoformat() if job.completed_at else None,
        errorMessage=job.error_message,
        itemsProcessed=(roll or {}).get("items"),
        bytesProcessed=(roll or {}).get("bytes"),
        snapshotsTotal=(roll or {}).get("snapshots_total"),
        snapshotsCompleted=(roll or {}).get("snapshots_completed"),
        snapshotsFailed=(roll or {}).get("snapshots_failed"),
        snapshotsInProgress=(roll or {}).get("snapshots_in_progress"),
    )


@app.get("/api/v1/jobs")
async def list_jobs(
    # ge=0 (not 1) so a frontend caller passing page=0 — natural
    # offset-style — gets the first page instead of a 422. Both 0 and 1
    # map to offset 0; subsequent pages behave identically to before.
    page: int = Query(1, ge=0),
    size: int = Query(50, ge=1),
    tenantId: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    type: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    filters = []
    tenant_uuid = _parse_uuid(tenantId, "tenantId")
    if tenant_uuid:
        filters.append(Job.tenant_id == tenant_uuid)
    if status:
        filters.append(Job.status == status)
    if type:
        filters.append(Job.type == type)

    total = (await db.execute(select(func.count(Job.id)).where(*filters))).scalar() or 0
    offset = max(page - 1, 0) * size
    stmt = select(Job).where(*filters).order_by(Job.created_at.desc()).offset(offset).limit(size)
    result = await db.execute(stmt)
    jobs = result.scalars().all()

    # Single bulk rollup query covers every job on this page.
    rollups = await _job_rollup_bulk(db, [j.id for j in jobs])

    return JobListResponse(
        content=[_build_job_response(j, rollups.get(str(j.id))) for j in jobs],
        totalPages=max(1, (total + size - 1) // size),
        totalElements=total,
        size=size, number=page,
        first=page <= 1,
        last=page >= (total + size - 1) // size,
    )


@app.get("/api/v1/jobs/{job_id}", response_model=JobResponse)
async def get_job(job_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Job).where(Job.id == _parse_uuid(job_id, "job_id"))
    result = await db.execute(stmt)
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    rollups = await _job_rollup_bulk(db, [job.id])
    return _build_job_response(job, rollups.get(str(job.id)))


@app.get("/api/v1/jobs/{job_id}/progress")
async def get_job_progress(job_id: str, token: Optional[str] = Query(None)):
    async def event_stream():
        for i in range(300):
            yield f"data: {json.dumps({'jobId': job_id, 'status': 'RUNNING', 'progress': min(i, 100), 'message': 'Processing'})}\n\n"
            await asyncio.sleep(1)
    return StreamingResponse(event_stream(), media_type="text/event-stream")


async def _revert_snapshot_storage(db: AsyncSession, snapshot) -> dict:
    """Delete every blob written for `snapshot`, then delete its DB rows.

    Called on cancel and on backup failure. Idempotent: unknown blobs are
    swallowed (NoSuchKey / 404) so a partial second call finishes cleanly.
    Returns a summary {items_reverted, bytes_reverted, blobs_deleted,
    blob_errors} the caller folds into its audit event.

    The container for Azure shards is derived from the owning resource's
    type (matches AzureStorageManager.get_container_name). SeaweedFS
    ignores the container arg (forced_bucket), so passing an Azure-shaped
    name there is harmless."""
    from shared.storage.router import router as _storage_router
    import logging as _logging
    _log = _logging.getLogger(__name__)

    # Snapshot has no tenant_id column — pull both type + tenant_id off
    # the owning resource in one round-trip. Falls back to the snapshot's
    # own resource_id only if the resource row is gone (shouldn't happen
    # in a normal cancel, but keeps the helper crash-free either way).
    res_row = (await db.execute(
        text("SELECT type::text, tenant_id::text FROM resources WHERE id = :rid"),
        {"rid": snapshot.resource_id},
    )).first()
    resource_type = (res_row[0] if res_row else "generic").lower().replace("_", "-")
    tenant_id_str = (res_row[1] if res_row and res_row[1]
                     else str(snapshot.resource_id or ""))
    tenant_short = tenant_id_str.replace("-", "")[:8]
    container = f"backup-{resource_type}-{tenant_short}"

    # Reuse-chain awareness (2026-05-15 design). If THIS snapshot is a
    # reuse row (owns zero snapshot_items, points at an ancestor), the
    # cancel is a row-only delete — no blobs were written, no items to
    # revert. We must still re-point any *descendant* whose chain root
    # was this row at the same ancestor we used, so the chain stays
    # connected. Pre-deploy snapshots have NULL reuse_* columns and
    # take the existing full-revert path.
    reuse_row = (await db.execute(
        text(
            "SELECT reuse_of_snapshot_id, reuse_chain_root_id "
            "FROM snapshots WHERE id = :sid"
        ),
        {"sid": snapshot.id},
    )).first()
    is_reuse = bool(reuse_row and reuse_row.reuse_of_snapshot_id is not None)
    if is_reuse:
        # Descendants get re-anchored to OUR chain root (which is
        # itself a non-doomed full snapshot of the same resource — the
        # validation trigger guarantees that). reuse_of_snapshot_id
        # for descendants that pointed straight at us moves up by
        # one step to our parent.
        await db.execute(text("""
            UPDATE snapshots
               SET reuse_of_snapshot_id = CASE
                       WHEN reuse_of_snapshot_id = CAST(:sid AS UUID)
                       THEN CAST(:parent AS UUID)
                       ELSE reuse_of_snapshot_id
                   END,
                   reuse_chain_root_id = CAST(:root AS UUID)
             WHERE reuse_chain_root_id = CAST(:root AS UUID)
               AND reuse_of_snapshot_id IS NOT NULL
               AND id != CAST(:sid AS UUID)
        """), {
            "sid": str(snapshot.id),
            "parent": str(reuse_row.reuse_of_snapshot_id),
            "root":   str(reuse_row.reuse_chain_root_id),
        })
        await db.execute(
            text("DELETE FROM snapshots WHERE id = :sid"),
            {"sid": snapshot.id},
        )
        _log.info(
            "cancel-revert: reuse-snapshot row-only delete sid=%s "
            "(no blob revert, no items_reverted)",
            snapshot.id,
        )
        return {
            "items_reverted": 0,
            "bytes_reverted": 0,
            "blobs_deleted": 0,
            "blob_errors":   0,
            "reuse_snapshot": True,
        }

    items = (await db.execute(
        text(
            "SELECT id, blob_path, backend_id, COALESCE(content_size,0) "
            "FROM snapshot_items WHERE snapshot_id = :sid"
        ),
        {"sid": snapshot.id},
    )).all()

    bytes_total = 0
    blobs_deleted = 0
    blob_errors = 0
    for _item_id, blob_path, backend_id, size in items:
        bytes_total += int(size or 0)
        if not blob_path or not backend_id:
            continue
        try:
            store = _storage_router.get_store_by_id(str(backend_id))
            await store.delete(container, blob_path)
            blobs_deleted += 1
        except Exception as exc:
            blob_errors += 1
            _log.warning(
                "cancel-revert: delete failed backend=%s path=%s: %s",
                backend_id, blob_path, exc,
            )

    # Reuse-chain safety: if this full snapshot is the chain root for
    # live descendants, rehydrate the heir BEFORE wiping items so no
    # descendant resolves to a dead pointer. Same helper retention
    # uses — single source of truth for the rehydration sequence.
    from shared.retention_cleanup import _rehydrate_reuse_heir
    await _rehydrate_reuse_heir(
        db, snapshot.id, doomed_ids={snapshot.id},
    )

    # Wipe snapshot_items first (FK: snapshot_items.snapshot_id → snapshots)
    await db.execute(
        text("DELETE FROM snapshot_items WHERE snapshot_id = :sid"),
        {"sid": snapshot.id},
    )
    # Then the snapshot row itself. vm_file_index CASCADEs automatically.
    await db.execute(
        text("DELETE FROM snapshots WHERE id = :sid"),
        {"sid": snapshot.id},
    )

    return {
        "items_reverted": len(items),
        "bytes_reverted": bytes_total,
        "blobs_deleted": blobs_deleted,
        "blob_errors": blob_errors,
    }


@app.post("/api/v1/jobs/{job_id}/cancel", status_code=204)
async def cancel_job(job_id: str, db: AsyncSession = Depends(get_db)):
    """Cancel a job and fully revert any partial work.

    Concurrency model: the UI fires Cancel POSTs for every sibling job
    in a batch via Promise.all. Without serialization, three concurrent
    cancels race for locks on the same backup_batches / jobs / snapshots
    rows and deadlock (observed 2026-05-18). Take a Postgres advisory
    lock keyed on batch_id at function entry — only one cancel per
    batch can run at a time; the rest wait at most a few hundred ms
    and proceed sequentially. Locks are transaction-scoped
    (pg_advisory_xact_lock) so they release automatically on commit
    /rollback / connection drop. Cheap (~100 ns) and Postgres-native.

    Cancel is a *revert*, not a soft-close — to keep backing storage
    consistent with the DB and to avoid ghost bytes piling up on
    SeaweedFS / Azure. See `_revert_snapshot_storage` for the per-
    snapshot cleanup. After this call:
      * `jobs.status` = CANCELLED, `completed_at` stamped.
      * Every resource touched has `last_backup_status='CANCELLED'`.
      * Every IN_PROGRESS snapshot this job owned is **deleted** —
        along with its `snapshot_items` rows and underlying blobs.
      * Audit event BACKUP_CANCELLED / RESTORE_CANCELLED emitted with
        a summary of what was reverted.

    In-flight workers poll `_is_job_cancelled()` between items and
    raise early, so the window between "cancel issued" and "no more new
    blobs written" is bounded by one item, not one resource."""
    job = (await db.execute(select(Job).where(Job.id == _parse_uuid(job_id, "job_id")))).scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Take a transaction-scoped advisory lock keyed on batch_id so
    # concurrent cancels for sibling jobs in the same batch serialize.
    # hashtext() collapses the UUID string to a 32-bit int for
    # pg_advisory_xact_lock; collisions across different batches are
    # acceptable (worst case: cancels for unrelated batches wait a few
    # ms for each other, instead of deadlocking the DB). Lock auto-
    # releases on commit/rollback. If there is no batch_id (e.g. an
    # ancient pre-batch-redesign job), we lock on the job id itself
    # which still avoids self-races but doesn't serialize siblings —
    # those legacy jobs predate the deadlock pattern anyway.
    _lock_key = None
    if isinstance(job.spec, dict):
        _lock_key = job.spec.get("batch_id")
    if not _lock_key:
        _lock_key = str(job.id)
    try:
        await db.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:k))"),
            {"k": str(_lock_key)},
        )
    except Exception as _le:
        # Lock acquisition is best-effort — falling through without it
        # is safer than 500-ing the cancel button.
        print(f"[JOB_SERVICE] cancel advisory_xact_lock failed (continuing): {_le}")

    if job.status not in (JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.RETRYING):
        # The targeted job is already terminal, but the batch it belonged
        # to may still be IN_PROGRESS because sibling jobs / partition
        # consumers are still draining. The user clicking Cancel a second
        # time (or clicking on an already-finished resource) is signalling
        # "stop the whole backup," so we still need to attempt the batch-
        # level cascade. Without this, repeat clicks on the same Activity
        # row never flip backup_batches.status → the row stays "In Progress"
        # on reload even though the user clearly asked for it to stop.
        try:
            batch_id_t = None
            if isinstance(job.spec, dict):
                batch_id_t = job.spec.get("batch_id")
            if batch_id_t:
                # Cascade-cancel every still-running sibling job in the batch
                # and force-flip the batch row. Idempotent (`status =
                # 'IN_PROGRESS'` guards).
                sib_rows = (await db.execute(
                    text(
                        "SELECT id FROM jobs "
                        " WHERE COALESCE(spec::jsonb->>'batch_id','') = :bid "
                        "   AND status IN ('QUEUED','RUNNING','RETRYING') "
                    ),
                    {"bid": str(batch_id_t)},
                )).all()
                sib_ids = [r.id for r in sib_rows]
                if sib_ids:
                    await db.execute(
                        text(
                            "UPDATE jobs SET status = 'CANCELLED', "
                            "                completed_at = NOW() "
                            " WHERE id = ANY(:ids)"
                        ),
                        {"ids": sib_ids},
                    )
                    await db.execute(
                        text(
                            "UPDATE snapshots SET "
                            "  status = 'FAILED'::snapshotstatus, "
                            "  completed_at = NOW(), "
                            "  duration_secs = EXTRACT(EPOCH FROM "
                            "                  (NOW() - started_at))::int, "
                            "  extra_data = (COALESCE(extra_data::jsonb, '{}'::jsonb) "
                            "               || jsonb_build_object( "
                            "                   'cancelled_at', NOW(), "
                            "                   'cancelled_by_batch_cascade', true, "
                            "                   'cancel_phase', "
                            "                   'flip_pending_sweep'))::json "
                            " WHERE job_id = ANY(:ids) "
                            "   AND status = 'IN_PROGRESS'"
                        ),
                        {"ids": sib_ids},
                    )
                    # Full-revert semantics: also mark sibling
                    # COMPLETED/PARTIAL snapshots so the sweep at
                    # backup-scheduler:1019 deletes blobs+items+rows for
                    # work that finished in the race window between
                    # "user clicked cancel" and "this UPDATE ran." Status
                    # stays as-is (the data was genuinely walked); the
                    # cancelled_at marker is enough for the sweep to
                    # pick it up. Previous successful snapshots for the
                    # same resource have a different job_id and are
                    # untouched.
                    await db.execute(
                        text(
                            "UPDATE snapshots SET "
                            "  extra_data = (COALESCE(extra_data::jsonb, '{}'::jsonb) "
                            "               || jsonb_build_object( "
                            "                   'cancelled_at', NOW(), "
                            "                   'cancelled_by_batch_cascade', true, "
                            "                   'cancel_phase', "
                            "                   'completed_before_cancel'))::json "
                            " WHERE job_id = ANY(:ids) "
                            "   AND status IN ('COMPLETED', 'PARTIAL') "
                            "   AND (extra_data::jsonb ->> 'cancelled_at') IS NULL"
                        ),
                        {"ids": sib_ids},
                    )
                    print(
                        f"[JOB_SERVICE] cancel-on-terminal cascaded to "
                        f"{len(sib_ids)} sibling job(s) in batch "
                        f"{batch_id_t}"
                    )
                await db.execute(
                    text(
                        "UPDATE backup_batches "
                        "   SET status = 'CANCELLED', "
                        "       completed_at = NOW() "
                        " WHERE id = cast(:bid AS uuid) "
                        "   AND status = 'IN_PROGRESS'"
                    ),
                    {"bid": str(batch_id_t)},
                )
                await db.commit()
                print(
                    f"[JOB_SERVICE] cancel-on-terminal force-flipped batch "
                    f"{batch_id_t} -> CANCELLED (job {job.id} was already "
                    f"{job.status.value if hasattr(job.status, 'value') else job.status})"
                )
        except Exception as _be:
            print(
                f"[JOB_SERVICE] cancel-on-terminal batch flip for "
                f"job={job.id} failed (non-fatal): "
                f"{type(_be).__name__}: {_be}"
            )
        return

    job.status = JobStatus.CANCELLED
    job.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)

    # Collect every resource this job touched.
    resource_ids: List[UUID] = []
    if job.resource_id:
        resource_ids.append(job.resource_id)
    for rid in (job.batch_resource_ids or []):
        try:
            resource_ids.append(UUID(rid) if isinstance(rid, str) else rid)
        except (ValueError, TypeError):
            continue

    revert_totals = {"items_reverted": 0, "bytes_reverted": 0,
                     "blobs_deleted": 0, "blob_errors": 0,
                     "snapshots_reverted": 0}

    if resource_ids:
        await db.execute(
            text("UPDATE resources SET last_backup_status = 'CANCELLED', last_backup_job_id = :jid WHERE id = ANY(:ids)"),
            {"jid": job.id, "ids": resource_ids},
        )
        # Atomic flip — was destructive delete, caused two real prod
        # incidents:
        #
        #   1. ForeignKeyViolationError on `snapshot_items_snapshot_id_fkey`
        #      when a backup_worker inserted a NEW snapshot_items row
        #      BETWEEN this endpoint's "DELETE FROM snapshot_items" and
        #      "DELETE FROM snapshots" — the second delete failed with
        #      FK-violation → 500 to the UI cancel button.
        #   2. DeadlockDetectedError when two concurrent cancels (or
        #      cancel + worker insert) collide on the same snapshot row.
        #
        # New strategy: atomically flip every in-flight snapshot to
        # FAILED with ``extra_data.cancelled_at`` set. No deletes here —
        # FK violations are impossible because no row vanishes. Async
        # blob + row teardown is owned by ``_sweep_cancelled_snapshots``
        # in backup-scheduler (runs every 30s, idempotent).
        #
        # We still need the *summary* (item/byte counts) for the audit
        # event below, so we snapshot the aggregate before the flip.
        # Reads only — concurrent worker inserts after this point land
        # in the post-cancel reaper's hands, not ours.
        agg = (await db.execute(
            text(
                "SELECT count(s.id) AS n_snaps, "
                "       COALESCE(SUM(si.n),  0)::bigint AS n_items, "
                "       COALESCE(SUM(si.b),  0)::bigint AS n_bytes "
                "  FROM snapshots s "
                "  LEFT JOIN LATERAL ( "
                "    SELECT count(*) AS n, "
                "           COALESCE(SUM(content_size),0)::bigint AS b "
                "      FROM snapshot_items "
                "     WHERE snapshot_id = s.id "
                "  ) si ON TRUE "
                " WHERE s.job_id = :jid AND s.status = 'IN_PROGRESS'"
            ),
            {"jid": job.id},
        )).first()
        revert_totals["snapshots_reverted"] = int(agg.n_snaps or 0) if agg else 0
        revert_totals["items_reverted"] = int(agg.n_items or 0) if agg else 0
        revert_totals["bytes_reverted"] = int(agg.n_bytes or 0) if agg else 0
        # The atomic flip itself — single UPDATE, no per-row Python.
        # status=FAILED so the UI's IN_PROGRESS filter stops showing
        # the row immediately. extra_data.cancelled_at is the durable
        # marker the reaper keys on; extra_data.cancelled_by_job_id
        # gives operators a clean trail back to the cancel event.
        #
        # Cast through ::jsonb so the `||` concat operator works even
        # though the column is plain JSON in the model
        # (shared/models.py:216). The trailing ::json coerces the
        # jsonb result back to the column type so UPDATE doesn't trip
        # an implicit-cast warning. Same idea is used by the reaper
        # SQL in backup-scheduler.
        # IN_PROGRESS branch — race-safe flip to FAILED + cancelled_at
        # marker so the sweep at backup-scheduler:1019 reaps blobs + items
        # + snapshot row. Worker self-flips on its next _is_job_cancelled
        # check would also reach this state, but doing it here closes the
        # window where the worker is still inside a Graph page.
        await db.execute(
            text(
                "UPDATE snapshots SET "
                "  status = 'FAILED'::snapshotstatus, "
                "  completed_at = NOW(), "
                "  duration_secs = EXTRACT(EPOCH FROM (NOW() - started_at))::int, "
                "  extra_data = (COALESCE(extra_data::jsonb, '{}'::jsonb) "
                "               || jsonb_build_object( "
                "                   'cancelled_at', NOW(), "
                "                   'cancelled_by_job_id', cast(:jid AS text), "
                "                   'cancel_phase', 'flip_pending_sweep'))::json "
                " WHERE job_id = cast(:jid AS uuid) AND status = 'IN_PROGRESS'"
            ),
            {"jid": str(job.id)},
        )
        # COMPLETED / PARTIAL branch — the snapshot already stamped a
        # terminal state in the milliseconds before cancel landed (race
        # observed 2026-05-17: Gajraj USER_CHATS 18:59:44 COMPLETED, then
        # batch cancel at 19:04:47). Per operator requirement, a cancelled
        # backup must leave NO durable artifacts — its blobs and rows
        # must be purged so the next incremental anchors against the
        # PREVIOUS successful snapshot, not the cancelled one. We keep
        # snapshot.status here (don't downgrade COMPLETED to FAILED — it
        # genuinely walked the data) but stamp the cancelled_at marker so
        # the sweep picks it up. The sweep's WHERE-clause already accepts
        # "cancelled_at IS NOT NULL" as sufficient for revert regardless
        # of snapshot.status, so blob teardown + row delete will follow.
        # Previous successful snapshots for the same resource have a
        # different job_id and are untouched.
        await db.execute(
            text(
                "UPDATE snapshots SET "
                "  extra_data = (COALESCE(extra_data::jsonb, '{}'::jsonb) "
                "               || jsonb_build_object( "
                "                   'cancelled_at', NOW(), "
                "                   'cancelled_by_job_id', cast(:jid AS text), "
                "                   'cancel_phase', "
                "                   'completed_before_cancel'))::json "
                " WHERE job_id = cast(:jid AS uuid) "
                "   AND status IN ('COMPLETED', 'PARTIAL') "
                "   AND (extra_data::jsonb ->> 'cancelled_at') IS NULL"
            ),
            {"jid": str(job.id)},
        )

    await db.flush()

    # Bug fix (2026-05-17): cancel used to leave backup_batches.status
    # stuck at IN_PROGRESS — the Activity feed reads that column directly
    # (audit-service /api/v1/activity), so a user-cancelled batch kept
    # showing "In Progress" on page reload. The previous code path only
    # touched jobs + snapshots; nothing called the batch finalizer.
    #
    # Now: after the snapshot flip above, derive the batch_id from
    # job.spec and call _finalize_batch_if_complete(). With every snapshot
    # for this job already FAILED (gate-2 terminal), the finalizer will
    # mark the batch terminal if and only if every other sibling job in
    # the same batch is also terminal — which is the correct semantics
    # for partial cancels in multi-job batches. Best-effort: an exception
    # here must not 500 the cancel endpoint, the rest of the work (job
    # flip, audit log, blob cleanup) already succeeded.
    try:
        batch_id = None
        if isinstance(job.spec, dict):
            batch_id = job.spec.get("batch_id")
        if batch_id:
            from shared.batch_rollup import _finalize_batch_if_complete
            new_status = await _finalize_batch_if_complete(batch_id, db)
            if new_status:
                print(
                    f"[JOB_SERVICE] cancel finalized batch {batch_id} "
                    f"-> {new_status} (after job {job.id})"
                )
            else:
                # Strict finalizer wouldn't flip because sibling jobs
                # in the same batch are still IN_PROGRESS. But the user
                # who clicked Cancel meant "stop this whole backup,"
                # not "cancel one resource and let the rest run." A
                # fanout-mass batch can have 30+ child jobs — the UI
                # only exposes a single Cancel button, so a per-job
                # cancel that leaves siblings running is observed by
                # the user as "I clicked cancel, why is it still
                # running?". Cascade-cancel every sibling job in the
                # batch, then force-flip backup_batches.status to
                # CANCELLED so the Activity feed reflects the user's
                # intent on the next reload.
                sib_rows = (await db.execute(
                    text(
                        "SELECT id FROM jobs "
                        " WHERE COALESCE(spec::jsonb->>'batch_id','') = :bid "
                        "   AND status IN ('QUEUED','RUNNING','RETRYING') "
                        "   AND id <> :self_jid "
                    ),
                    {"bid": str(batch_id), "self_jid": job.id},
                )).all()
                sib_ids = [r.id for r in sib_rows]
                if sib_ids:
                    await db.execute(
                        text(
                            "UPDATE jobs "
                            "   SET status = 'CANCELLED', "
                            "       completed_at = NOW() "
                            " WHERE id = ANY(:ids)"
                        ),
                        {"ids": sib_ids},
                    )
                    await db.execute(
                        text(
                            "UPDATE snapshots SET "
                            "  status = 'FAILED'::snapshotstatus, "
                            "  completed_at = NOW(), "
                            "  duration_secs = EXTRACT(EPOCH FROM "
                            "                  (NOW() - started_at))::int, "
                            "  extra_data = (COALESCE(extra_data::jsonb, '{}'::jsonb) "
                            "               || jsonb_build_object( "
                            "                   'cancelled_at', NOW(), "
                            "                   'cancelled_by_batch_cascade', true, "
                            "                   'cancel_phase', "
                            "                   'flip_pending_sweep'))::json "
                            " WHERE job_id = ANY(:ids) "
                            "   AND status = 'IN_PROGRESS'"
                        ),
                        {"ids": sib_ids},
                    )
                    # Full-revert semantics: also mark sibling
                    # COMPLETED/PARTIAL snapshots so the sweep at
                    # backup-scheduler:1019 deletes blobs+items+rows for
                    # work that finished in the race window between
                    # "user clicked cancel" and "this UPDATE ran." Status
                    # stays as-is (the data was genuinely walked); the
                    # cancelled_at marker is enough for the sweep to
                    # pick it up. Previous successful snapshots for the
                    # same resource have a different job_id and are
                    # untouched.
                    await db.execute(
                        text(
                            "UPDATE snapshots SET "
                            "  extra_data = (COALESCE(extra_data::jsonb, '{}'::jsonb) "
                            "               || jsonb_build_object( "
                            "                   'cancelled_at', NOW(), "
                            "                   'cancelled_by_batch_cascade', true, "
                            "                   'cancel_phase', "
                            "                   'completed_before_cancel'))::json "
                            " WHERE job_id = ANY(:ids) "
                            "   AND status IN ('COMPLETED', 'PARTIAL') "
                            "   AND (extra_data::jsonb ->> 'cancelled_at') IS NULL"
                        ),
                        {"ids": sib_ids},
                    )
                    print(
                        f"[JOB_SERVICE] cancel cascaded to "
                        f"{len(sib_ids)} sibling job(s) in batch "
                        f"{batch_id}"
                    )
                # Force-flip the batch row so the Activity feed
                # reflects "Canceled" on the very next read. Idempotent
                # (only flips if currently IN_PROGRESS, matching the
                # _finalize_batch_if_complete safety semantics).
                await db.execute(
                    text(
                        "UPDATE backup_batches "
                        "   SET status = 'CANCELLED', "
                        "       completed_at = NOW() "
                        " WHERE id = cast(:bid AS uuid) "
                        "   AND status = 'IN_PROGRESS'"
                    ),
                    {"bid": str(batch_id)},
                )
                await db.commit()
                print(
                    f"[JOB_SERVICE] cancel force-flipped batch "
                    f"{batch_id} -> CANCELLED (user-initiated cascade)"
                )
    except Exception as _be:
        print(
            f"[JOB_SERVICE] cancel batch finalize for job={job.id} "
            f"failed (non-fatal): {type(_be).__name__}: {_be}"
        )

    # Audit trail — emit a CANCELLED event whose action mirrors the
    # job kind so the Audit feed groups restore vs backup correctly.
    # Details now include the revert summary so the Activity drill-down
    # tells the user exactly what was rolled back.
    try:
        import httpx as _httpx
        action = "RESTORE_CANCELLED" if job.type == JobType.RESTORE else "BACKUP_CANCELLED"
        async with _httpx.AsyncClient(timeout=5.0) as _client:
            await _client.post(f"{settings.AUDIT_SERVICE_URL}/api/v1/audit/log", json={
                "action": action,
                "tenant_id": str(job.tenant_id) if job.tenant_id else None,
                "actor_type": "USER",
                "resource_id": str(job.resource_id) if job.resource_id else None,
                "outcome": "CANCELLED",
                "job_id": str(job.id),
                "details": {
                    "resource_count": len(resource_ids),
                    **revert_totals,
                },
            })
    except Exception:
        pass

    # Blob cleanup — for BACKUP jobs only (RESTORE jobs export ZIPs,
    # not snapshots). Fire-and-forget so the cancel endpoint returns
    # 204 immediately while bytes are reclaimed in the background. The
    # cleanup helper is idempotent: if a backup-worker beats us to it
    # (worker also runs cleanup when it sees CANCELLED status on its
    # next message pull), the second run is a no-op.
    if job.type == JobType.BACKUP:
        async def _cleanup_blobs(jid: UUID):
            try:
                from shared.backup_cleanup import (
                    cleanup_cancelled_snapshots,
                    default_container_resolver,
                )
                from shared.azure_storage import azure_storage_manager
                from shared.database import async_session_factory
                stats = await cleanup_cancelled_snapshots(
                    job_id=jid,
                    session_factory=async_session_factory,
                    shard=azure_storage_manager.get_default_shard(),
                    container_resolver=default_container_resolver,
                )
                print(f"[JOB_SERVICE] cancel-cleanup job={jid}: {stats}")
            except Exception as exc:
                import traceback as _tb
                print(
                    f"[JOB_SERVICE] cancel-cleanup job={jid} failed: "
                    f"{type(exc).__name__}: {exc} | jid_type={type(jid).__name__}\n"
                    f"{_tb.format_exc()}",
                    flush=True,
                )

        try:
            import asyncio as _aio
            _aio.create_task(_cleanup_blobs(job.id))
        except Exception:
            pass


@app.post("/api/v1/jobs/{job_id}/retry", response_model=JobResponse)
async def retry_job(job_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Job).where(Job.id == _parse_uuid(job_id, "job_id"))
    result = await db.execute(stmt)
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    job.status = JobStatus.QUEUED
    job.attempts = 0
    job.progress_pct = 0
    await db.flush()
    return JobResponse(
        id=str(job.id), type=job.type.value if hasattr(job.type, 'value') else str(job.type),
        status=job.status.value, progress=0,
        resourceId=str(job.resource_id) if job.resource_id else None,
        createdAt=job.created_at.isoformat(),
        updatedAt=job.updated_at.isoformat(),
    )


@app.get("/api/v1/jobs/{job_id}/logs")
async def get_job_logs(job_id: str, page: int = Query(1), size: int = Query(50), db: AsyncSession = Depends(get_db)):
    stmt = select(JobLog).where(JobLog.job_id == _parse_uuid(job_id, "job_id")).order_by(JobLog.timestamp.desc()).offset((page-1)*size).limit(size)
    result = await db.execute(stmt)
    logs = result.scalars().all()
    return [
        {"id": str(log.id), "jobId": str(log.job_id), "timestamp": log.timestamp.isoformat() if log.timestamp else "",
         "level": log.level, "message": log.message, "details": log.details}
        for log in logs
    ]


async def _route_single_trigger(
    resource: Resource,
    db: AsyncSession,
    *,
    full_backup: bool,
    priority: int,
    note: Optional[str] = None,
    trigger_label: str = "MANUAL",
) -> Optional[Dict]:
    """Single-resource trigger with Tier-2 auto-discovery gap-fill.

    For ENTRA_USER targets this mirrors the mass-backup flow's
    classify_scope behaviour: existing live USER_* children are
    included in the fan-out, and missing children get discovered +
    backed up via the standard discovery.tier2 chain (thenBackup=True).

    Returns the batch-result dict from _create_batch_backup_jobs when
    routing through the bulk path, or None when the caller should
    keep its legacy single-message publish (non-ENTRA_USER targets).
    """
    if resource.type != ResourceType.ENTRA_USER:
        return None

    excluded_statuses = [
        ResourceStatus.INACCESSIBLE,
        ResourceStatus.SUSPENDED,
        ResourceStatus.PENDING_DELETION,
    ]
    child_stmt = select(Resource).where(
        Resource.parent_resource_id == resource.id,
        Resource.type.in_([
            ResourceType.USER_MAIL,
            ResourceType.USER_ONEDRIVE,
            ResourceType.USER_CONTACTS,
            ResourceType.USER_CALENDAR,
            ResourceType.USER_CHATS,
        ]),
        Resource.status.notin_(excluded_statuses),
        Resource.archived_at.is_(None),
    )
    children = (await db.execute(child_stmt)).scalars().all()

    resources_map: Dict[str, Resource] = {str(resource.id): resource}
    for child in children:
        resources_map[str(child.id)] = child

    print(
        f"[JOB_SERVICE] single ENTRA_USER trigger via batch path: "
        f"user={resource.id} children_live={len(children)} "
        f"(tier-2 gap-fill {'skipped' if len(children) == 5 else 'will run'})"
    )

    return await _create_batch_backup_jobs(
        resources_map=resources_map,
        db=db,
        full_backup=full_backup,
        priority=priority,
        note=note,
        trigger_label=trigger_label,
        batch_id=str(uuid4()),
    )


@app.post("/api/v1/backups/trigger", response_model=JobResponse)
async def trigger_backup(request: TriggerBackupRequest, db: AsyncSession = Depends(get_db)):
    # Fetch resource to get tenant info
    resource_uuid = _parse_uuid(request.resourceId, "resourceId")
    resource_stmt = select(Resource).where(Resource.id == resource_uuid)
    resource_result = await db.execute(resource_stmt)
    resource = resource_result.scalar_one_or_none()
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")

    resource = await _redirect_teams_chat_to_export(db, resource)
    request.resourceId = str(resource.id)

    # Prevent backup on inaccessible/suspended/deleted resources
    status_val = resource.status.value if hasattr(resource.status, 'value') else str(resource.status)
    if status_val in ("INACCESSIBLE", "SUSPENDED", "PENDING_DELETION"):
        raise HTTPException(
            status_code=422,
            detail=f"Resource is {status_val} and cannot be backed up. "
                   f"Run discovery first to restore access or remove the resource."
        )

    # Require SLA policy assignment
    if not resource.sla_policy_id:
        raise HTTPException(
            status_code=400,
            detail="Resource must have an SLA policy assigned before triggering a backup"
        )

    # If fullBackup is True but resource already has a backup, set to False
    effective_full_backup = request.fullBackup or False
    if effective_full_backup and resource.last_backup_at is not None:
        print(f"[JOB_SERVICE] Resource {request.resourceId} has last_backup_at={resource.last_backup_at}, setting fullBackup=False")
        effective_full_backup = False
    else:
        print(f"[JOB_SERVICE] Resource {request.resourceId} first backup (last_backup_at={resource.last_backup_at}), fullBackup={effective_full_backup}")

    # Tier-2 gap-fill for ENTRA_USER targets: existing children join the
    # fan-out, missing ones get discovered + backed up via discovery.tier2.
    # Mirrors the mass-backup classify_scope behaviour so a single-user
    # click actually captures the user's mail/onedrive/calendar/contacts/
    # chats — not just the user profile.
    batched = await _route_single_trigger(
        resource, db,
        full_backup=effective_full_backup,
        priority=request.priority or 1,
        note=request.note,
        trigger_label="MANUAL",
    )
    if batched:
        # _create_batch_backup_jobs returns a list of BATCH-job dicts
        # ({"jobId","status","resourceId":"BATCH",...}). Synthesize a
        # JobResponse from the first BATCH job so /trigger's strict
        # response_model still validates. The frontend keys on jobId
        # for status polling, so the BATCH job id is what it needs to
        # track — the per-resource fan-out lives under it.
        first = batched[0] if isinstance(batched, list) and batched else None
        if isinstance(first, dict) and first.get("jobId"):
            now_iso = datetime.now(timezone.utc).isoformat()
            return JobResponse(
                id=str(first["jobId"]),
                type="BACKUP",
                status=str(first.get("status") or "QUEUED"),
                progress=0,
                resourceId=str(resource.id),
                createdAt=now_iso,
                updatedAt=now_iso,
            )
        # Fall through to legacy publish if the batch path returned an
        # unexpected shape.

    job = Job(
        id=uuid4(), type=JobType.BACKUP,
        tenant_id=resource.tenant_id,
        resource_id=resource_uuid,
        status=JobStatus.QUEUED, priority=request.priority or 1,
        progress_pct=0, items_processed=0, bytes_processed=0,
        spec={"fullBackup": effective_full_backup, "note": request.note, "triggered_by": "MANUAL"},
    )
    db.add(job)
    await db.commit()  # commit BEFORE publishing — worker must find the job in DB

    # Publish to RabbitMQ
    if settings.RABBITMQ_ENABLED:
        # AZ-4: Route Azure workload resources to dedicated queues
        resource_type = resource.type.value if hasattr(resource.type, 'value') else str(resource.type)
        routing_key = AZURE_WORKLOAD_QUEUES.get(resource_type, "backup.urgent")

        # Heavy-pool routing: file-content workloads (OneDrive / SharePoint /
        # Power BI) always route to the dedicated heavy pool so a single big
        # drive can't block MAILBOX/ENTRA work on the shared lanes. Light
        # types keep their existing trigger-path queue (urgent / Azure queues).
        from shared.export_routing import pick_backup_queue
        drive_bytes_estimate = int((resource.extra_data or {}).get("drive_quota_used", 0))
        routing_key = pick_backup_queue(
            drive_bytes_estimate=drive_bytes_estimate,
            resource_type=resource_type,
            default_queue=routing_key,
        )

        msg = create_backup_message(
            job_id=str(job.id), resource_id=request.resourceId,
            tenant_id=str(resource.tenant_id), full_backup=effective_full_backup
        )
        print(f"[JOB_SERVICE] Resource type={resource_type} → queue {routing_key}")
        print(f"[JOB_SERVICE] Publishing backup message to {routing_key}: {msg}")
        await message_bus.publish(routing_key, msg, priority=request.priority or 1)
        print(f"[JOB_SERVICE] Message published successfully")
    else:
        print(f"[JOB_SERVICE] RabbitMQ not enabled, skipping publish. RABBITMQ_ENABLED={settings.RABBITMQ_ENABLED}")

    await emit_backup_triggered(
        job=job, resource=resource,
        trigger_label="MANUAL", full_backup=effective_full_backup,
        extra_details={"note": request.note} if request.note else None,
    )

    return JobResponse(
        id=str(job.id), type="BACKUP", status="QUEUED", progress=0,
        resourceId=request.resourceId,
        createdAt=job.created_at.isoformat(),
        updatedAt=job.created_at.isoformat(),
    )


@app.post("/api/v1/backups/trigger-user/{resource_id}")
@app.post("/api/v1/backups/trigger-bulk")
async def trigger_bulk_backup(resource_id: str = None, request: TriggerBulkBackupRequest = None, db: AsyncSession = Depends(get_db)):
    if request and request.resourceIds:
        # Fetch all resources with tenant info
        resources_map = {}
        inaccessible_resources = []
        for rid in request.resourceIds:
            res_stmt = select(Resource).where(Resource.id == _parse_uuid(rid, "resourceId"))
            res_result = await db.execute(res_stmt)
            res = res_result.scalar_one_or_none()
            if res:
                res = await _redirect_teams_chat_to_export(db, res)
                status_val = res.status.value if hasattr(res.status, 'value') else str(res.status)
                if status_val in ("INACCESSIBLE", "SUSPENDED", "PENDING_DELETION"):
                    inaccessible_resources.append({"id": str(res.id), "status": status_val})
                else:
                    resources_map[str(res.id)] = res

        if not resources_map and inaccessible_resources:
            raise HTTPException(
                status_code=422,
                detail=f"All requested resources are inaccessible: "
                       f"{', '.join(r['id'] + '(' + r['status'] + ')' for r in inaccessible_resources)}"
            )

        # Always have a batch_id so Tier-1 + Tier-2 routing-key splits
        # collapse to one Activity row, even when the caller forgot to
        # supply one.
        return await _create_batch_backup_jobs(
            resources_map=resources_map,
            db=db,
            full_backup=request.fullBackup or False,
            priority=request.priority or 1,
            note=request.note,
            trigger_label="MANUAL_BATCH",
            batch_id=request.batchId or str(uuid4()),
            tier2=bool(getattr(request, "tier2", False)),
        )

    elif resource_id:
        single_resource_uuid = _parse_uuid(resource_id, "resource_id")
        res_stmt = select(Resource).where(Resource.id == single_resource_uuid)
        res_result = await db.execute(res_stmt)
        res = res_result.scalar_one_or_none()
        if not res:
            raise HTTPException(status_code=404, detail="Resource not found")

        res = await _redirect_teams_chat_to_export(db, res)
        resource_id = str(res.id)

        # Prevent backup on inaccessible/suspended/deleted resources
        status_val = res.status.value if hasattr(res.status, 'value') else str(res.status)
        if status_val in ("INACCESSIBLE", "SUSPENDED", "PENDING_DELETION"):
            raise HTTPException(
                status_code=422,
                detail=f"Resource is {status_val} and cannot be backed up. "
                       f"Run discovery first to restore access or remove the resource."
            )

        # Require SLA policy assignment
        if not res.sla_policy_id:
            raise HTTPException(
                status_code=400,
                detail="Resource must have an SLA policy assigned before triggering a backup"
            )

        # Determine fullBackup based on whether resource has been backed up before
        effective_full_backup = not (res.last_backup_at is not None)
        print(f"[JOB_SERVICE] Single resource backup for {resource_id}, fullBackup={effective_full_backup}")

        # Tier-2 gap-fill for ENTRA_USER (same as /trigger).
        batched = await _route_single_trigger(
            res, db,
            full_backup=effective_full_backup,
            priority=1,
            note=None,
            trigger_label="MANUAL",
        )
        if batched:
            first = batched[0] if isinstance(batched, list) and batched else None
            if isinstance(first, dict) and first.get("jobId"):
                return {
                    "jobId": str(first["jobId"]),
                    "status": str(first.get("status") or "QUEUED"),
                    "resourceId": resource_id,
                }

        job = Job(
            id=uuid4(), type=JobType.BACKUP,
            tenant_id=res.tenant_id,
            resource_id=single_resource_uuid,
            status=JobStatus.QUEUED, priority=1,
            progress_pct=0, items_processed=0, bytes_processed=0,
            spec={"triggered_by": "MANUAL", "fullBackup": effective_full_backup},
        )
        db.add(job)
        await db.commit()  # commit before publish so worker finds the job

        if settings.RABBITMQ_ENABLED:
            # Heavy-pool routing for oversized OneDrive drives (Tier 2
            # USER_ONEDRIVE children). Everything else keeps the
            # user-initiated urgent queue.
            from shared.export_routing import pick_backup_queue
            res_type = res.type.value if hasattr(res.type, "value") else str(res.type)
            drive_bytes = int((res.extra_data or {}).get("drive_quota_used", 0))
            routing_key = pick_backup_queue(
                drive_bytes_estimate=drive_bytes,
                resource_type=res_type,
                default_queue="backup.urgent",
            )
            print(f"[JOB_SERVICE] trigger-user {resource_id} type={res_type} bytes={drive_bytes} → {routing_key}")
            await message_bus.publish(routing_key, create_backup_message(
                job_id=str(job.id), resource_id=resource_id,
                tenant_id=str(res.tenant_id), full_backup=effective_full_backup
            ), priority=1)

        await emit_backup_triggered(
            job=job, resource=res,
            trigger_label="MANUAL", full_backup=effective_full_backup,
        )
        return {"jobId": str(job.id), "status": "QUEUED", "resourceId": resource_id}
    return {"error": "No resources provided"}


@app.post("/api/v1/backups/trigger-datasource")
async def trigger_datasource_backup(request: TriggerDatasourceBackupRequest, db: AsyncSession = Depends(get_db)):
    service_key = (request.serviceType or "").lower()
    resource_types = M365_RESOURCE_TYPES if service_key == "m365" else AZURE_RESOURCE_TYPES if service_key == "azure" else None
    if resource_types is None:
        raise HTTPException(status_code=400, detail="Unsupported serviceType. Expected 'm365' or 'azure'.")

    # One operator click = one batch_id. Threaded through:
    #   1. _create_batch_backup_jobs (parent-resource Jobs)
    #   2. discovery.tier2 message → discovery-worker → trigger-bulk POST
    #      (Tier-2 child Jobs created a few seconds later)
    # Both stages stamp `spec.batch_id`, so audit-service collapses them
    # into one Activity row regardless of timestamp drift or different
    # `triggered_by` labels.
    batch_id = str(uuid4())

    excluded_statuses = [
        ResourceStatus.INACCESSIBLE,
        ResourceStatus.SUSPENDED,
        ResourceStatus.PENDING_DELETION,
    ]
    tenant_uuid = _parse_uuid(request.tenantId, "tenantId")
    scoped_filters = [
        Resource.tenant_id == tenant_uuid,
        Resource.type.in_(resource_types),
    ]
    summary_stmt = select(
        func.count(Resource.id).label("total_discovered"),
        func.count(Resource.id).filter(
            Resource.sla_policy_id.is_not(None),
            Resource.status.notin_(excluded_statuses),
        ).label("eligible"),
        func.count(Resource.id).filter(Resource.sla_policy_id.is_(None)).label("skip_no_sla"),
        func.count(Resource.id).filter(
            Resource.sla_policy_id.is_not(None),
            Resource.status == ResourceStatus.INACCESSIBLE,
        ).label("skip_inaccessible"),
        func.count(Resource.id).filter(
            Resource.sla_policy_id.is_not(None),
            Resource.status == ResourceStatus.SUSPENDED,
        ).label("skip_suspended"),
        func.count(Resource.id).filter(
            Resource.sla_policy_id.is_not(None),
            Resource.status == ResourceStatus.PENDING_DELETION,
        ).label("skip_pending_deletion"),
    ).where(*scoped_filters)
    summary = (await db.execute(summary_stmt)).one()
    skipped_by_reason = {
        "no_sla": int(summary.skip_no_sla or 0),
        "inaccessible": int(summary.skip_inaccessible or 0),
        "suspended": int(summary.skip_suspended or 0),
        "pending_deletion": int(summary.skip_pending_deletion or 0),
    }
    print(
        f"[JOB_SERVICE] DATASOURCE_BACKUP_SUMMARY tenant={request.tenantId} service={service_key} "
        f"discovered={int(summary.total_discovered or 0)} eligible={int(summary.eligible or 0)} "
        f"skipped_total={sum(skipped_by_reason.values())} skipped_by_reason={skipped_by_reason}"
    )

    stmt = select(Resource).where(
        *scoped_filters,
        Resource.sla_policy_id.is_not(None),
        Resource.status.notin_(excluded_statuses),
    )
    result = await db.execute(stmt)
    resources = result.scalars().all()

    if not resources:
        raise HTTPException(
            status_code=404,
            detail=f"No backup-eligible {service_key.upper()} resources found for this datasource. Make sure discovery has run and SLA policies are assigned."
        )

    # Tier-2 gap-fill (M365 only). USER_MAIL / USER_ONEDRIVE / USER_CONTACTS /
    # USER_CALENDAR / USER_CHATS rows only exist for users who've been through
    # per-user "Backup now" discovery before. SLA'd ENTRA_USERs without those
    # children would otherwise only get their profile backed up while their
    # actual content (emails, chats, calendar, files, contacts) is silently
    # skipped — which is the bulk-vs-single dichotomy the operator reported.
    #
    # We don't block this HTTP request on Graph calls (5k users would be
    # ~25k Graph round-trips). Instead, enqueue a discovery.tier2 message per
    # user-batch with thenBackup=true. The discovery-worker creates the rows,
    # then calls trigger-bulk for the children. The operator sees the parent
    # ENTRA_USER backups start immediately + child backups arrive on the
    # next pass — all under one Activity row because of the (tenant_id,
    # triggered_by, created_at) grouping rule.
    # Tier-2 gap-fill enqueue is delegated to `_create_batch_backup_jobs`
    # below, which already publishes ``discovery.tier2`` for the
    # ``deferred`` users (classify_scope output) AFTER inserting their
    # ``batch_pending_users`` rows — the correct ordering, since the
    # discovery-worker's terminal-state UPDATE on batch_pending_users
    # requires the row to exist. This outer block used to ALSO publish
    # the same message (for ``users_missing_tier2``, an identical set
    # derived independently), which produced two ``discovery.tier2``
    # deliveries per click → two ``/trigger-bulk`` calls → duplicate
    # Tier-2 jobs and duplicate snapshots per (user, workload). The
    # 2026-05-16 incident surfaced this as half-empty snapshot rows
    # in the Activity breakdown (one snapshot drained the data, the
    # other was a no-op race). Only the inner publish is kept; we
    # still compute ``users_missing_tier2`` here purely to drive the
    # ``BULK_BACKUP_PENDING_DISCOVERY`` audit event below.
    users_missing_tier2: List[Resource] = []
    if service_key == "m365":
        from shared.tier2_discovery import find_users_missing_tier2
        sla_user_ids = [r.id for r in resources if r.type == ResourceType.ENTRA_USER]
        if sla_user_ids:
            users_missing_tier2 = await find_users_missing_tier2(
                db, user_resource_ids=sla_user_ids, require_sla=True,
            )

    resources_map = {str(resource.id): resource for resource in resources}
    jobs = await _create_batch_backup_jobs(
        resources_map=resources_map,
        db=db,
        full_backup=request.fullBackup or False,
        priority=request.priority or 1,
        note=request.note,
        trigger_label=f"MANUAL_DATASOURCE_{service_key.upper()}",
        batch_id=batch_id,
    )
    # Emit an audit event for the gap-fill rather than mutating the
    # response shape (callers expect a list). Operators can read the
    # count from the Audit feed under BULK_BACKUP_PENDING_DISCOVERY.
    if users_missing_tier2:
        try:
            from shared.audit import emit_audit_event
            await emit_audit_event(
                action="BULK_BACKUP_PENDING_DISCOVERY",
                tenant_id=str(tenant_uuid),
                resource_type=service_key.upper(),
                details={
                    "users_pending": len(users_missing_tier2),
                    "source": "BULK_TRIGGER",
                },
            )
        except Exception as _e:
            # Audit emit failure must not block the trigger response.
            print(f"[JOB_SERVICE] BULK_BACKUP_PENDING_DISCOVERY audit emit failed: {_e}")
    return jobs


@app.post("/api/v1/jobs/restore")
@app.post("/api/v1/jobs/restore/mailbox")
@app.post("/api/v1/jobs/restore/onedrive")
@app.post("/api/v1/jobs/restore/sharepoint")
@app.post("/api/v1/jobs/restore/entra-object")
async def trigger_restore(request: dict = None, db: AsyncSession = Depends(get_db)):
    """Trigger a restore job and publish to RabbitMQ"""
    if not request:
        raise HTTPException(status_code=400, detail="Request body is required")

    restore_type = request.get("restoreType", "IN_PLACE")
    snapshot_ids = request.get("snapshotIds", [])
    item_ids = request.get("itemIds", [])
    target_user_id = request.get("targetUserId")
    spec = {
        "targetUserId": target_user_id,
        "targetResourceId": request.get("targetResourceId"),
        "targetEnvironmentId": request.get("targetEnvironmentId"),
        "exportFormat": request.get("exportFormat"),
        # Folder-select intent: true when user chose a folder checkbox
        # (not individual files). Restore-worker uses this to skip the
        # single-file raw-stream shortcut so even a 1-item expansion is
        # zipped with its folder path preserved.
        "preserveTree": bool(request.get("preserveTree", False)),
        "targetFolder": request.get("targetFolder"),
        "overwrite": request.get("overwrite", False),
        "entraSections": request.get("entraSections"),
        "format": request.get("format"),
        "includeNestedDetail": bool(request.get("includeNestedDetail", False)),
        "recoverMode": request.get("recoverMode"),
        "includeGroupMembership": bool(request.get("includeGroupMembership", True)),
        "includeAuMembership": bool(request.get("includeAuMembership", True)),
        # RestoreModal sends labels like ["Mail","OneDrive","Contacts","Calendar","Chats"].
        # None / missing = restore everything (back-compat). Restore-worker maps each
        # label to the matching item_type values and skips anything else in the snapshot.
        "workloads": request.get("workloads"),
        # Files folder-select v2. When either list is non-empty the
        # restore-worker delegates to shared.folder_resolver instead of
        # treating itemIds as the authoritative selection.
        "folderPaths": request.get("folderPaths") or [],
        "excludedItemIds": request.get("excludedItemIds") or [],
        # SharePoint/OneDrive/Teams/Group restore conflict handling.
        # OVERWRITE replaces in-place; SEPARATE_FOLDER lands under
        # `Restored by TM/{date}/…`. Defaults to SEPARATE_FOLDER in the
        # restore engine when not set.
        "conflictMode": (request.get("conflictMode") or None),
        # Pass-through params consumed by Azure restore handlers (target RG, VM/DB name,
        # subscription, PITR time, firewall flag, disk name, etc). Kept as a nested
        # dict so non-Azure restores ignore it cleanly.
        "azureRestoreParams": request.get("azureRestoreParams", {}),
        # Sub-mode selector for Azure restores: VM → FULL_VM|DISK, SQL → FULL|PITR|SCHEMA_ONLY
        "azureRestoreMode": request.get("azureRestoreMode"),
        # Optional folder filter for USER_CONTACT exports. Empty/missing = all.
        "contactFolders": request.get("contactFolders"),
        # SharePoint "Recover to new site" mode: restore-worker provisions a
        # fresh Communication Site before replaying files. Alias / owner are
        # optional — restore-worker falls back to sensible defaults.
        "newSiteName": request.get("newSiteName"),
        "newSiteAlias": request.get("newSiteAlias"),
        "newSiteOwnerEmail": request.get("newSiteOwnerEmail"),
    }

    # Fetch tenant/resource info — try snapshot first, then fall back to item lookup.
    # Without this, item-driven restores (no snapshot_ids passed) end up with
    # NULL resource_id on the Job row → no audit linkage, no UI rendering, and
    # no way to know which resource was meant for restore if the queue message
    # is lost.
    from sqlalchemy import select as sa_select
    tenant_id = None
    resource_id = None
    snapshot = None
    if snapshot_ids:
        stmt = sa_select(Snapshot).where(Snapshot.id == _parse_uuid(snapshot_ids[0], "snapshotId"))
        snapshot = (await db.execute(stmt)).scalar_one_or_none()
    if not snapshot and item_ids:
        # Item-driven restore — derive snapshot from the first item, then
        # snapshot.resource_id gives us the source resource.
        item_stmt = sa_select(SnapshotItem).where(SnapshotItem.id == _parse_uuid(item_ids[0], "itemId"))
        first_item = (await db.execute(item_stmt)).scalar_one_or_none()
        if first_item:
            snapshot = await db.get(Snapshot, first_item.snapshot_id)
            # Backfill snapshot_ids on the spec so the worker can pull blobs.
            if snapshot and not snapshot_ids:
                snapshot_ids = [str(snapshot.id)]

    resource_type = None
    if snapshot:
        resource_id = str(snapshot.resource_id)
        resource = await db.get(Resource, snapshot.resource_id)
        if resource:
            tenant_id = str(resource.tenant_id)
            resource_type = resource.type.value if hasattr(resource.type, "value") else str(resource.type)

    # Estimate total selection size so the message_bus router can send
    # whale jobs (>HEAVY_EXPORT_THRESHOLD_BYTES, default 20 GB) to
    # restore.heavy automatically. Falls back to 0 (= normal queue) if
    # the snapshot has no item rows yet (rare race) or the query errors.
    total_bytes = 0
    try:
        from sqlalchemy import func as _func
        if item_ids:
            stmt = sa_select(_func.coalesce(_func.sum(SnapshotItem.content_size), 0)).where(
                SnapshotItem.id.in_([_parse_uuid(i, "itemId") for i in item_ids])
            )
        elif snapshot_ids:
            stmt = sa_select(_func.coalesce(_func.sum(SnapshotItem.content_size), 0)).where(
                SnapshotItem.snapshot_id.in_([_parse_uuid(s, "snapshotId") for s in snapshot_ids])
            )
        else:
            stmt = None
        if stmt is not None:
            total_bytes = int((await db.execute(stmt)).scalar_one() or 0)
        spec["totalBytes"] = total_bytes
    except Exception as size_exc:
        # Router will fall back to restore.normal — non-fatal.
        spec["totalBytes"] = 0
        print(f"[JOB_SERVICE] totalBytes estimate failed (non-fatal): {size_exc}")

    job = Job(
        id=uuid4(),
        type=JobType.RESTORE,
        tenant_id=UUID(tenant_id) if tenant_id else None,
        resource_id=UUID(resource_id) if resource_id else None,
        status=JobStatus.QUEUED,
        priority=1,
        spec={
            "restore_type": restore_type,
            "snapshot_ids": snapshot_ids,
            "item_ids": item_ids,
            **spec,
        }
    )
    db.add(job)
    await db.flush()
    # Commit BEFORE publishing — the worker can pick the message up
    # within milliseconds and SELECT for the job; without an explicit
    # commit it sees an empty result and aborts with "job not found".
    await db.commit()

    # Publish to RabbitMQ
    if settings.RABBITMQ_ENABLED:
        restore_message = create_restore_message(
            job_id=str(job.id),
            restore_type=restore_type,
            snapshot_ids=snapshot_ids,
            item_ids=item_ids,
            resource_id=resource_id,
            tenant_id=tenant_id,
            spec=spec,
            resource_type=resource_type,
        )
        from shared.export_routing import pick_restore_queue
        from sqlalchemy import func as sa_func
        total_bytes = 0
        try:
            # Folder-scope path: resolve the selection first so the
            # routing decision sees the same bytes the worker will
            # actually process. Legacy paths (itemIds only, or just a
            # snapshot) fall back to the simple aggregate.
            if (spec.get("folderPaths") or spec.get("excludedItemIds")) and snapshot_ids:
                from shared.folder_resolver import resolve_selection
                resolved = await resolve_selection(
                    db,
                    snapshot_id=snapshot_ids[0],
                    item_ids=item_ids,
                    folder_paths=spec.get("folderPaths") or [],
                    excluded_item_ids=spec.get("excludedItemIds") or [],
                )
                total_bytes = sum(int(r.content_size or 0) for r in resolved)
            elif item_ids:
                q = sa_select(sa_func.coalesce(sa_func.sum(SnapshotItem.content_size), 0)).where(
                    SnapshotItem.id.in_([uuid.UUID(x) for x in item_ids])
                )
                total_bytes = int((await db.execute(q)).scalar() or 0)
            elif snapshot_ids:
                q = sa_select(sa_func.coalesce(sa_func.sum(SnapshotItem.content_size), 0)).where(
                    SnapshotItem.snapshot_id.in_([uuid.UUID(x) for x in snapshot_ids])
                )
                total_bytes = int((await db.execute(q)).scalar() or 0)
        except Exception:
            total_bytes = 0
        queue = restore_message.get("queue") or pick_restore_queue(total_bytes=total_bytes)
        await message_bus.publish(queue, restore_message, priority=restore_message.get("priority", 5))

    # Audit trail — RESTORE_TRIGGERED. Mirrors the BACKUP_TRIGGERED
    # emission so the audit feed and the activity page have matching
    # lifecycle entries for restore jobs. Non-blocking: if audit-service
    # is down we don't fail the restore.
    try:
        import httpx as _httpx
        async with _httpx.AsyncClient(timeout=5.0) as _client:
            await _client.post(f"{settings.AUDIT_SERVICE_URL}/api/v1/audit/log", json={
                "action": "RESTORE_TRIGGERED",
                "tenant_id": tenant_id,
                "actor_type": "USER",
                "resource_id": resource_id,
                "resource_type": resource_type,
                "outcome": "PENDING",
                "job_id": str(job.id),
                "details": {
                    "restore_type": restore_type,
                    "snapshot_count": len(snapshot_ids),
                    "item_count": len(item_ids),
                    "azure_restore_mode": spec.get("azureRestoreMode"),
                },
            })
    except Exception:
        pass

    return {
        "jobId": str(job.id),
        "status": "QUEUED",
        "restoreType": restore_type,
        "snapshotCount": len(snapshot_ids),
        "itemCount": len(item_ids),
    }


@app.get("/api/v1/jobs/restore/{job_id}/status")
async def get_restore_status(job_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Job).where(Job.id == _parse_uuid(job_id, "job_id"))
    result = await db.execute(stmt)
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"jobId": str(job.id), "status": job.status.value if hasattr(job.status, 'value') else str(job.status), "progress": job.progress_pct or 0}


@app.get("/api/v1/jobs/restore/history")
async def get_restore_history(page: int = 1, size: int = 50, db: AsyncSession = Depends(get_db)):
    stmt = select(Job).where(Job.type == JobType.RESTORE).order_by(Job.created_at.desc()).offset((page-1)*size).limit(size)
    result = await db.execute(stmt)
    jobs = result.scalars().all()
    return {"content": [{"id": str(j.id), "status": j.status.value, "createdAt": j.created_at.isoformat()} for j in jobs], "totalPages": 1, "totalElements": len(jobs), "size": size, "number": page}


@app.post("/api/v1/jobs/export")
async def trigger_export(request: dict, db: AsyncSession = Depends(get_db)):
    """Queue an export job: persist the Job row, publish to RabbitMQ so
    the restore-worker picks it up, and fire an EXPORT_TRIGGERED audit.

    Scope fields (tenant_id / resource_id / resource_type) are derived by
    walking snapshot → resource in the DB rather than trusting whatever
    the frontend happened to send on the request body — gives us reliable
    audit scoping even when the caller omits those fields."""
    restore_type = request.get("restoreType", "EXPORT_ZIP")
    snapshot_ids = request.get("snapshotIds", [])
    item_ids = request.get("itemIds", [])

    tenant_id = None
    resource_id = None
    resource_type = None
    snapshot = None
    if snapshot_ids:
        snapshot = (await db.execute(
            select(Snapshot).where(Snapshot.id == _parse_uuid(snapshot_ids[0], "snapshotId"))
        )).scalar_one_or_none()
    if not snapshot and item_ids:
        first_item = (await db.execute(
            select(SnapshotItem).where(SnapshotItem.id == _parse_uuid(item_ids[0], "itemId"))
        )).scalar_one_or_none()
        if first_item:
            snapshot = await db.get(Snapshot, first_item.snapshot_id)
            if snapshot and not snapshot_ids:
                snapshot_ids = [str(snapshot.id)]
    if snapshot:
        resource_id = str(snapshot.resource_id)
        resource = await db.get(Resource, snapshot.resource_id)
        if resource:
            tenant_id = str(resource.tenant_id)
            resource_type = resource.type.value if hasattr(resource.type, "value") else str(resource.type)

    job = Job(
        id=uuid4(),
        type=JobType.EXPORT,
        tenant_id=UUID(tenant_id) if tenant_id else None,
        resource_id=UUID(resource_id) if resource_id else None,
        status=JobStatus.QUEUED,
        priority=5,
        spec={
            "restore_type": restore_type,
            "snapshot_ids": snapshot_ids,
            "item_ids": item_ids,
            **request,
        },
    )
    db.add(job)
    await db.commit()  # commit before publish so worker finds job

    if settings.RABBITMQ_ENABLED:
        # Reuse the restore pipeline: restore-worker already has EXPORT_ZIP,
        # EXPORT_PST, and DOWNLOAD handlers routed off its restore queues.
        # Publishing here means the dedicated export.normal queue stays
        # unused, which matches what the code actually supports today.
        restore_message = create_restore_message(
            job_id=str(job.id),
            restore_type=restore_type,
            snapshot_ids=snapshot_ids,
            item_ids=item_ids,
            resource_id=resource_id,
            tenant_id=tenant_id,
            spec=request,
            resource_type=resource_type,
        )

        # Total bytes for M5 preflight + Task 23 heavy pool routing.
        from sqlalchemy import func as sa_func, select as sa_select
        total_bytes = 0
        try:
            if item_ids:
                q = sa_select(sa_func.coalesce(sa_func.sum(SnapshotItem.content_size), 0)).where(
                    SnapshotItem.id.in_([UUID(x) for x in item_ids])
                )
                total_bytes = int((await db.execute(q)).scalar() or 0)
            elif snapshot_ids:
                q = sa_select(sa_func.coalesce(sa_func.sum(SnapshotItem.content_size), 0)).where(
                    SnapshotItem.snapshot_id.in_([UUID(x) for x in snapshot_ids])
                )
                total_bytes = int((await db.execute(q)).scalar() or 0)
        except Exception:
            total_bytes = 0

        from shared.export_routing import pick_export_queue
        queue = pick_export_queue(
            total_bytes=total_bytes,
            include_attachments=bool(request.get("includeAttachments", True)),
        )
        await message_bus.publish(queue, restore_message, priority=restore_message.get("priority", 5))
    else:
        print(f"[JOB_SERVICE] RabbitMQ not enabled, export job {job.id} will stay QUEUED")

    # Audit: EXPORT_TRIGGERED — captures who exported what, when, and which
    # items/snapshots are in scope so the trail is reconstructable for
    # compliance review. Non-blocking: if audit-service is down we don't
    # fail the export itself.
    try:
        import httpx as _httpx
        async with _httpx.AsyncClient(timeout=5.0) as _c:
            await _c.post(f"{settings.AUDIT_SERVICE_URL}/api/v1/audit/log", json={
                "action": "EXPORT_TRIGGERED",
                "tenant_id": tenant_id,
                "actor_type": "USER",
                "resource_id": resource_id,
                "resource_type": resource_type,
                "outcome": "SUCCESS",
                "job_id": str(job.id),
                "details": {
                    "restoreType": restore_type,
                    "snapshotIds": snapshot_ids,
                    "itemIds": item_ids,
                    "itemCount": len(item_ids),
                    "snapshotCount": len(snapshot_ids),
                },
            })
    except Exception:
        pass

    return {"jobId": str(job.id)}


# Workloads that share the folder_path column and can be mutually
# cross-restored. Teams = the team's backing SharePoint site drive;
# M365_GROUP carries the group's backing SharePoint drive under the
# same snapshot. USER_ONEDRIVE is the Tier-2 child under a discovered
# user. Adding a new family member means verifying its backup populates
# folder_path.
_FILES_FAMILY = {
    "ONEDRIVE",
    "USER_ONEDRIVE",
    "SHAREPOINT_SITE",
    "TEAMS_CHANNEL",
    "M365_GROUP",
}


def _resource_family(resource_type: str) -> str:
    """Collapse file-family resource types into one 'FILES' bucket for
    the cross-resource restore guard. Non-family types return their own
    name so the comparison is identity."""
    return "FILES" if (resource_type or "").upper() in _FILES_FAMILY else str(resource_type or "").upper()


@app.post("/api/v1/resources/{resource_id}/export-or-restore")
async def files_export_or_restore(
    resource_id: str,
    request: dict,
    db: AsyncSession = Depends(get_db),
):
    """Unified entry point for Files folder-select v2.

    Body shape (see docs/superpowers/specs/2026-04-20-files-folder-select-design.md):
      restoreType:      EXPORT_ZIP | IN_PLACE | CROSS_RESOURCE
      snapshotId:       UUID of the source snapshot (single)
      itemIds:          files individually ticked (optional)
      folderPaths:      folders ticked; server expands via shared resolver
      excludedItemIds:  files un-ticked inside a ticked folder
      conflictMode:     OVERWRITE | SEPARATE_FOLDER (IN_PLACE only)
      targetResourceId: required for CROSS_RESOURCE; same workload family
      preserveTree:     defaults true when folderPaths is non-empty

    Delegates to ``trigger_restore`` so queueing / auditing / size-cap
    logic stays in one place.
    """
    resource = await db.get(Resource, _parse_uuid(resource_id, "resourceId"))
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    resource_type = resource.type.value if hasattr(resource.type, "value") else str(resource.type)
    if resource_type.upper() not in _FILES_FAMILY:
        raise HTTPException(
            status_code=400,
            detail=f"Resource type {resource_type} is not a file workload",
        )

    restore_type = (request.get("restoreType") or "").upper()
    if restore_type not in ("EXPORT_ZIP", "IN_PLACE", "CROSS_RESOURCE"):
        raise HTTPException(
            status_code=400,
            detail="restoreType must be EXPORT_ZIP, IN_PLACE, or CROSS_RESOURCE",
        )

    snapshot_id = request.get("snapshotId")
    if not snapshot_id:
        raise HTTPException(status_code=400, detail="snapshotId is required")

    item_ids = list(request.get("itemIds") or [])
    folder_paths = list(request.get("folderPaths") or [])
    excluded_item_ids = list(request.get("excludedItemIds") or [])
    if not item_ids and not folder_paths:
        raise HTTPException(status_code=400, detail="Select at least one item or folder")

    preserve_tree = bool(request.get("preserveTree", bool(folder_paths)))

    conflict_mode = (request.get("conflictMode") or "").upper() or None
    if restore_type == "IN_PLACE":
        if conflict_mode not in ("OVERWRITE", "SEPARATE_FOLDER"):
            raise HTTPException(
                status_code=400,
                detail="conflictMode=OVERWRITE or SEPARATE_FOLDER required for IN_PLACE",
            )

    target_resource_id = request.get("targetResourceId")
    if restore_type == "CROSS_RESOURCE":
        if not target_resource_id:
            raise HTTPException(status_code=400, detail="targetResourceId required for CROSS_RESOURCE")
        target = await db.get(Resource, uuid.UUID(target_resource_id))
        if not target:
            raise HTTPException(status_code=404, detail="Target resource not found")
        if target.tenant_id != resource.tenant_id:
            raise HTTPException(status_code=400, detail="Cross-tenant restore is not supported")
        target_type = target.type.value if hasattr(target.type, "value") else str(target.type)
        if _resource_family(target_type) != _resource_family(resource_type):
            raise HTTPException(
                status_code=400,
                detail=f"Target workload family mismatch ({target_type} vs {resource_type})",
            )

    # Forward into the shared trigger_restore path so auditing, queueing,
    # and size-cap logic stay in one place.
    body = {
        "restoreType": restore_type,
        "snapshotIds": [snapshot_id],
        "itemIds": item_ids,
        "folderPaths": folder_paths,
        "excludedItemIds": excluded_item_ids,
        "preserveTree": preserve_tree,
        "conflictMode": conflict_mode,
        "targetResourceId": target_resource_id,
        "exportFormat": request.get("exportFormat")
            or ("ZIP" if restore_type == "EXPORT_ZIP" else None),
    }
    return await trigger_restore(body, db)


@app.post("/api/v1/sharepoint/{resource_id}/download")
async def sharepoint_download(resource_id: str, request: dict, db: AsyncSession = Depends(get_db)):
    """DEPRECATED — use ``POST /api/v1/resources/{resource_id}/export-or-restore``.

    This endpoint predates the Files folder-select v2 payload. It
    continues to work identically for the OneDrive / SharePoint callers
    that still use it; removal is scheduled for the release AFTER
    ``FILES_FOLDER_SELECT_V2`` is fully on in prod.

    Request body (unchanged):
      * ``scope``: ``"site"`` | ``"folder"`` | ``"file"``
      * ``snapshotId``: required for ``site``.
      * ``folderPath``: required for ``folder``.
      * ``itemId``: required for ``file``.

    Returns ``{jobId}``; poll ``/api/v1/jobs/export/{jobId}/status`` and
    then GET ``/api/v1/jobs/export/{jobId}/download`` to pull the bytes.
    """
    scope = (request.get("scope") or "").lower()
    if scope not in ("site", "folder", "file"):
        raise HTTPException(status_code=400, detail="scope must be site, folder, or file")

    resource = await db.get(Resource, _parse_uuid(resource_id, "resourceId"))
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    resource_type = resource.type.value if hasattr(resource.type, "value") else str(resource.type)
    if resource_type != "SHAREPOINT":
        raise HTTPException(status_code=400, detail=f"Resource is not SharePoint (got {resource_type})")

    snapshot_ids: List[str] = []
    item_ids: List[str] = []
    preserve_tree = True
    export_format = "ZIP"

    if scope == "site":
        snap_id = request.get("snapshotId")
        if not snap_id:
            # Pick the latest completed snapshot for the site.
            stmt = select(Snapshot).where(Snapshot.resource_id == resource.id).order_by(Snapshot.created_at.desc()).limit(1)
            latest = (await db.execute(stmt)).scalar_one_or_none()
            if not latest:
                raise HTTPException(status_code=404, detail="No snapshot found for SharePoint site")
            snap_id = str(latest.id)
        snapshot_ids = [snap_id]

    elif scope == "folder":
        folder_path = request.get("folderPath")
        snap_id = request.get("snapshotId")
        if not folder_path:
            raise HTTPException(status_code=400, detail="folderPath required for scope=folder")
        stmt = select(SnapshotItem).where(
            SnapshotItem.folder_path.startswith(folder_path),
        )
        if snap_id:
            stmt = stmt.where(SnapshotItem.snapshot_id == _parse_uuid(snap_id, "snapshotId"))
        else:
            # Restrict to snapshots of this resource so a bare folderPath
            # can't bleed across sites.
            sub = select(Snapshot.id).where(Snapshot.resource_id == resource.id)
            stmt = stmt.where(SnapshotItem.snapshot_id.in_(sub))
        items = (await db.execute(stmt)).scalars().all()
        if not items:
            raise HTTPException(status_code=404, detail=f"No items under folder {folder_path!r}")
        item_ids = [str(i.id) for i in items]
        # preserveTree stays True — a folder download must keep the tree even
        # if the folder happens to have a single file inside.

    else:  # scope == "file"
        item_id = request.get("itemId")
        if not item_id:
            raise HTTPException(status_code=400, detail="itemId required for scope=file")
        item_ids = [item_id]
        preserve_tree = False
        export_format = "ORIGINAL"

    # Reuse the existing export pipeline — same audit, same worker path,
    # same download endpoint — so this wrapper doesn't duplicate plumbing.
    body = {
        "restoreType": "EXPORT_ZIP",
        "snapshotIds": snapshot_ids,
        "itemIds": item_ids,
        "preserveTree": preserve_tree,
        "exportFormat": export_format,
        "scope": scope,
    }
    return await trigger_export(body, db)


@app.get("/api/v1/jobs/export/{job_id}/status")
async def get_export_status(job_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Job).where(Job.id == _parse_uuid(job_id, "job_id"))
    result = await db.execute(stmt)
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"jobId": str(job.id), "status": job.status.value if hasattr(job.status, 'value') else str(job.status), "progress": job.progress_pct or 0}


@app.get("/api/v1/jobs/export/{job_id}/download")
async def download_export_zip(job_id: str, db: AsyncSession = Depends(get_db)):
    """Stream the export ZIP back to the user.

    Restore-worker uploads the built ZIP to the `exports` Azure container at
    `exports/{job_id}/export_{timestamp}.zip` and stores the path in
    `Job.spec.result.blob_path`. We:
      1. Look up the job + verify it's COMPLETED + EXPORT_ZIP-typed
      2. Download the blob bytes from `exports`
      3. Stream them back as a ZIP attachment

    Without this, the frontend gets a 404 when it tries to download — what was
    happening on Recovery exports."""
    from fastapi.responses import StreamingResponse
    job = await db.get(Job, _parse_uuid(job_id, "job_id"))
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    status_val = job.status.value if hasattr(job.status, "value") else str(job.status)
    if status_val != "COMPLETED":
        raise HTTPException(status_code=409, detail=f"Export not ready (status={status_val})")

    # Worker persists upload metadata in Job.result (not Job.spec).
    result = job.result or {}
    output_mode = result.get("output_mode", "zip")

    from shared.azure_storage import azure_storage_manager
    shard = azure_storage_manager.get_default_shard()

    # Raw single-file shortcut — OneDrive export v2 writes output_mode='raw_single'
    # when exactly one file is exported in ORIGINAL format. Skip ZIP wrapper and
    # stream the source blob bytes directly with the original Content-Type +
    # filename so the browser saves it as the user expects.
    if output_mode == "raw_single":
        src_container = result.get("source_container")
        src_blob_path = result.get("source_blob_path")
        if not src_container or not src_blob_path:
            raise HTTPException(status_code=500, detail="raw_single job missing source blob info")
        content_type = result.get("content_type") or "application/octet-stream"
        original_name = result.get("original_name") or f"export_{job_id}.bin"
        size_bytes = int(result.get("size_bytes") or 0)

        async def _iter_raw():
            async for chunk in shard.download_blob_stream(src_container, src_blob_path):
                yield chunk

        raw_headers = {"Content-Disposition": f'attachment; filename="{original_name}"'}
        if size_bytes:
            raw_headers["Content-Length"] = str(size_bytes)
        return StreamingResponse(_iter_raw(), media_type=content_type, headers=raw_headers)

    blob_path = result.get("blob_path") or result.get("blobPath")
    if not blob_path:
        # COMPLETED but no file produced — usually means the export ran
        # but every group failed (e.g. PST converter unavailable). Surface
        # the real failure reason from result.skipped_groups so the
        # frontend can render a meaningful message instead of a generic
        # 500. Client-friendly status is 422 — the job state is consistent
        # but there's nothing to download.
        skipped = result.get("skipped_groups") or []
        first_err = (skipped[0].get("error") if skipped and isinstance(skipped[0], dict) else None) or ""
        exported = result.get("exported_count", 0)
        failed = result.get("failed_count", 0)
        # Truncate long stack traces to keep the response readable.
        if len(first_err) > 240:
            first_err = first_err[:240] + "…"
        detail = (
            f"Export produced no file (exported={exported}, failed={failed})"
            + (f". First error: {first_err}" if first_err else "")
        )
        raise HTTPException(status_code=422, detail=detail)

    # Reuse the same shard the workers use so credentials line up.
    # Container naming mirrors backup-worker / restore-worker:
    # `backup-exports-{tenant_hash}`. Fallbacks: literal "exports" (legacy) and
    # Job.result.container (if the worker recorded it explicitly).
    candidate_containers: list = []
    if result.get("container"):
        candidate_containers.append(str(result["container"]))
    if job.tenant_id:
        candidate_containers.append(azure_storage_manager.get_container_name(str(job.tenant_id), "exports"))
    candidate_containers.append("exports")
    # dedupe preserving order
    seen = set(); _uniq = []
    for c in candidate_containers:
        if c and c not in seen:
            seen.add(c); _uniq.append(c)
    candidate_containers = _uniq

    content = None
    last_err: Exception | None = None
    for cand in candidate_containers:
        try:
            content = await shard.download_blob(cand, blob_path)
            if content is not None:
                print(f"[JOB_SERVICE] export download: found in container={cand}")
                break
        except Exception as exc:
            last_err = exc
            print(f"[JOB_SERVICE] container={cand} download failed: {exc}")
    if content is None:
        if last_err:
            raise HTTPException(status_code=500, detail=f"Failed to fetch export blob: {last_err}")
        raise HTTPException(status_code=404, detail=f"Export blob not found in any of: {candidate_containers}")

    fname = blob_path.rsplit("/", 1)[-1] or f"export_{job_id}.zip"

    # Audit: EXPORT_DOWNLOADED — records the user actually pulled the built
    # zip (distinct from EXPORT_TRIGGERED, which only records the request).
    # Spec carries the original restoreType + items so the audit trail
    # matches what was queued earlier.
    try:
        import httpx as _httpx
        spec = job.spec or {}
        async with _httpx.AsyncClient(timeout=5.0) as _c:
            await _c.post(f"{settings.AUDIT_SERVICE_URL}/api/v1/audit/log", json={
                "action": "EXPORT_DOWNLOADED",
                "tenant_id": str(job.tenant_id) if job.tenant_id else spec.get("tenantId"),
                "actor_type": "USER",
                "resource_id": str(job.resource_id) if job.resource_id else spec.get("resourceId"),
                "resource_type": spec.get("resourceType"),
                "outcome": "SUCCESS",
                "job_id": str(job.id),
                "details": {
                    "blobPath": blob_path,
                    "filename": fname,
                    "byteSize": len(content),
                    "restoreType": spec.get("restoreType") or "EXPORT_ZIP",
                    "snapshotIds": spec.get("snapshotIds") or [],
                    "itemIds": spec.get("itemIds") or [],
                    "itemCount": len(spec.get("itemIds") or []),
                },
            })
    except Exception:
        pass

    return StreamingResponse(
        iter([content]),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@app.get("/api/v1/dlq/stats")
async def get_dlq_stats():
    return [
        {"dlqName": "backup.urgent.dlq", "messageCount": 0},
        {"dlqName": "backup.normal.dlq", "messageCount": 0},
        {"dlqName": "restore.urgent.dlq", "messageCount": 0},
    ]


@app.post("/api/v1/dlq/{dlq_name}/purge", status_code=204)
@app.post("/api/v1/dlq/{dlq_name}/requeue", status_code=204)
async def dlq_action(dlq_name: str):
    pass
