"""
DR Replication Worker — Asynchronous cross-region backup replication

Scans for real recovery points (COMPLETED *or* PARTIAL, excluding
skipped_concurrent placeholders) with dr_replication_status=pending, and
initiates blob-to-blob server-side copy from primary to DR region.
Azure handles the actual transfer over Microsoft's backbone fiber.

Catalogue snapshots instead stream exact captured dependencies into a verified
recovery archive. Automatic catalogue restore routing remains explicitly gated.

Key principles:
- Asynchronous: Primary backup succeeds BEFORE DR replication runs
- Server-side copy: Bytes flow via Azure backbone, never through this worker
- Retry: 5-minute scan interval re-picks failed snapshots (up to 10 attempts)
- Isolation: Primary backup status unaffected by DR failures
"""
import asyncio
import logging
import os
import time
import traceback
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import List
from uuid import UUID, uuid4

from azure.storage.blob.aio import BlobClient
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from sqlalchemy import and_, func, or_, select

from shared.config import settings
from shared.database import async_session_factory, init_db
from shared.models import Tenant, Resource, Snapshot, SnapshotItem, SlaPolicy, SnapshotStatus
from shared.azure_storage import (
    azure_storage_manager, apply_legal_hold, apply_lifecycle_policy, AzureStorageShard,
    RESOURCE_TYPE_TO_WORKLOADS,
)
from shared.security import decrypt_secret

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [dr-worker] %(message)s",
)
logger = logging.getLogger("dr-replication-worker")

# Container-workload mapping lives in shared/azure_storage.py (RESOURCE_TYPE_TO_WORKLOADS).
# Do not redeclare here — the previous hand-rolled copy drifted from the backup worker's
# writes (used "onedrive"/"sharepoint" while backup-worker writes to "files"; used
# "azure-sql" while backup-worker writes "azure-sql-db") and caused silent 404s on every
# DR replication. The canonical map is:
#   resource_type -> (primary_workload, *fallback_workloads)
# For DR purposes we use the primary (first) workload per type.

def _primary_workload(resource_type: str) -> str:
    """Return the primary container-workload suffix for a resource type.
    Falls back to "files" for unknown types to keep replication best-effort."""
    candidates = RESOURCE_TYPE_TO_WORKLOADS.get(str(resource_type or "").upper(), ())
    return candidates[0] if candidates else "files"


MAX_REPLICATION_ATTEMPTS = 10
COPY_TIMEOUT_SECONDS = 1800  # 30 min per blob
SCAN_BATCH_LIMIT = 500

# Snapshot states that hold real, restorable data. Mirrors
# snapshot-service's _real_restore_point() / _READABLE_SNAPSHOT_STATES —
# keep the two in step.
#
# PARTIAL belongs here. A PARTIAL snapshot lost some shards but still holds
# every item it DID capture, so it is a real recovery point: in production
# the only backup of Jason Rue's 1,114,848-item / 152 GB Exchange Online
# Archive is PARTIAL, and the lossy-shard derivation deployed 2026-08-04
# makes PARTIAL the normal outcome for any SharePoint/OneDrive run with
# files_failed > 0. Replicating COMPLETED only meant every one of those
# recovery points had NO DR copy at all.
_DR_REPLICABLE_SNAPSHOT_STATES = (SnapshotStatus.COMPLETED, SnapshotStatus.PARTIAL)

# How far back the scan looks. Bounded by default so a deploy of this fix
# can't turn the whole history of never-replicated PARTIAL snapshots into
# one enormous copy storm. Set DR_SCAN_LOOKBACK_DAYS=0 to disable the
# window entirely and deliberately drain the historical backlog (see the
# backlog WARNING this scan logs each pass).
try:
    _DR_SCAN_LOOKBACK_DAYS = int(os.getenv("DR_SCAN_LOOKBACK_DAYS", "2"))
except ValueError:
    # A typo'd env var must not crash-loop the worker into replicating nothing.
    logger.error(
        "[config] DR_SCAN_LOOKBACK_DAYS=%r is not an integer — using 2",
        os.getenv("DR_SCAN_LOOKBACK_DAYS"),
    )
    _DR_SCAN_LOOKBACK_DAYS = 2


def _dr_replicable_snapshot():
    """SQL predicate: terminal-with-data, and not a concurrency placeholder.

    Two halves, both load-bearing:

    * ``status IN (COMPLETED, PARTIAL)`` — a PARTIAL snapshot is a real
      recovery point holding real data and MUST get a DR copy.
    * ``skipped_concurrent`` rejection — backup-worker's cross-job
      concurrency guard (SKIP_CONCURRENT in workers/backup-worker/main.py)
      creates 0-item snapshot rows purely so a skipped job's
      FANOUT/FINALIZE counters balance, then marks them COMPLETED in
      ~19 ms. Those are fake successes holding no data; replicating them
      burns DR work and registers an empty DR copy.

    ``extra_data`` is a Postgres **json** column (not jsonb), so
    json_extract_path_text is the correct accessor — it is NULL-safe for
    absent keys, and jsonb_extract_path_text would raise at runtime.
    COALESCE is required: rows written before the flag existed hold NULL,
    and a bare ``NOT IN`` over NULL yields NULL, which would silently
    filter out every real snapshot. Compared as text so a non-boolean
    value can't raise the way an ->>::boolean cast would.
    """
    return and_(
        Snapshot.status.in_(_DR_REPLICABLE_SNAPSHOT_STATES),
        func.coalesce(
            func.json_extract_path_text(Snapshot.extra_data, "skipped_concurrent"),
            "false",
        ).notin_(("true", "t", "1")),
    )


def _dr_pending_retryable():
    """SQL predicate: DR-side state says this snapshot still needs a copy."""
    return and_(
        or_(and_(Snapshot.dr_replication_status.in_(("pending", "failed")),
                 Snapshot.dr_replication_attempts < MAX_REPLICATION_ATTEMPTS),
            and_(Snapshot.dr_replication_status == "in_progress",
                 func.json_extract_path_text(Snapshot.extra_data, "chat_inventory_format") == "1")),
        func.coalesce(func.json_extract_path_text(Snapshot.extra_data, "chat_catalogue_dr_nonretryable"),
                      "false").notin_(("true", "t", "1")),
    )


async def scan_and_replicate():
    """
    Scan for pending/failed DR snapshots and replicate them.
    Runs every 5 minutes.
    """
    logger.info("[scan_and_replicate] === START: Scanning for pending DR snapshots ===")

    async with async_session_factory() as session:
        try:
            # Snapshot.created_at is a naive-UTC column (default=utcnow),
            # so bind a naive-UTC cutoff. The old `NOW() - INTERVAL '2 days'`
            # compared it against timestamptz NOW(), which PG resolves using
            # the session TimeZone — a non-UTC session skewed the window.
            cutoff = None
            if _DR_SCAN_LOOKBACK_DAYS > 0:
                cutoff = (
                    datetime.now(timezone.utc).replace(tzinfo=None)
                    - timedelta(days=_DR_SCAN_LOOKBACK_DAYS)
                )

            stmt = select(Snapshot).where(
                _dr_replicable_snapshot(),
                _dr_pending_retryable(),
            )
            if cutoff is not None:
                stmt = stmt.where(Snapshot.created_at > cutoff)
            # Newest recovery points get their DR copy first; also makes the
            # batch deterministic when the candidate set exceeds the limit.
            stmt = stmt.order_by(Snapshot.created_at.desc()).limit(SCAN_BATCH_LIMIT)

            snapshots: List[Snapshot] = (await session.execute(stmt)).scalars().all()

            # Anything real and unreplicated that has already aged out of the
            # window will NEVER be reconsidered while the window is in force —
            # the gap is permanent, not just delayed. Surface the number so the
            # backlog is a decision rather than a silent hole. Draining it is an
            # explicit operator act (DR_SCAN_LOOKBACK_DAYS=0), not a side effect
            # of deploying this fix.
            if cutoff is not None:
                backlog = (await session.execute(
                    select(func.count(Snapshot.id)).where(
                        _dr_replicable_snapshot(),
                        _dr_pending_retryable(),
                        Snapshot.created_at <= cutoff,
                    )
                )).scalar() or 0
                if backlog:
                    logger.warning(
                        "[scan_and_replicate] BACKLOG: %d real recovery point(s) "
                        "older than the %d-day look-back have no DR copy and will "
                        "never be retried while the window stands. Set "
                        "DR_SCAN_LOOKBACK_DAYS=0 to drain them.",
                        backlog, _DR_SCAN_LOOKBACK_DAYS,
                    )

            if not snapshots:
                logger.info("[scan_and_replicate] No pending snapshots to replicate — scan complete")
                return

            logger.info(
                "[scan_and_replicate] Found %d snapshots pending DR replication",
                len(snapshots),
            )

            success_count = 0
            fail_count = 0
            skip_count = 0

            for snapshot in snapshots:
                try:
                    # Get resource first, then tenant (Snapshot doesn't have tenant_id directly)
                    resource = await session.get(Resource, snapshot.resource_id)
                    if not resource:
                        snapshot.dr_replication_status = "failed"
                        snapshot.dr_error = "Resource not found"
                        snapshot.dr_replication_attempts = (snapshot.dr_replication_attempts or 0) + 1
                        fail_count += 1
                        logger.error(
                            "[scan_and_replicate] Snapshot %s — resource %s not found",
                            snapshot.id, snapshot.resource_id,
                        )
                        continue

                    tenant = await session.get(Tenant, resource.tenant_id)
                    if not tenant:
                        snapshot.dr_replication_status = "skipped"
                        snapshot.dr_error = "Tenant not found"
                        skip_count += 1
                        logger.warning(
                            "[scan_and_replicate] Snapshot %s — tenant %s not found, marking skipped",
                            snapshot.id, resource.tenant_id,
                        )
                        continue

                    if not tenant.dr_region_enabled:
                        snapshot.dr_replication_status = "skipped"
                        snapshot.dr_error = "DR region not enabled for this tenant"
                        skip_count += 1
                        logger.info(
                            "[scan_and_replicate] Snapshot %s — DR disabled for tenant %s, marking skipped",
                            snapshot.id, tenant.id,
                        )
                        continue

                    if not tenant.dr_storage_account_name:
                        snapshot.dr_replication_status = "failed"
                        snapshot.dr_error = "DR storage account name not configured"
                        snapshot.dr_replication_attempts = (snapshot.dr_replication_attempts or 0) + 1
                        fail_count += 1
                        logger.error(
                            "[scan_and_replicate] Snapshot %s — DR storage account not configured for tenant %s",
                            snapshot.id, tenant.id,
                        )
                        continue

                    logger.info(
                        "[scan_and_replicate] Processing snapshot %s (tenant=%s, attempt=%d, status=%s)",
                        snapshot.id, tenant.id,
                        (snapshot.dr_replication_attempts or 0) + 1,
                        snapshot.dr_replication_status,
                    )

                    await replicate_snapshot(snapshot, tenant, session)

                    if snapshot.dr_replication_status == "replicated":
                        success_count += 1
                    else:
                        fail_count += 1

                except Exception as e:
                    snapshot.dr_replication_status = "failed"
                    snapshot.dr_error = str(e)[:1000]
                    snapshot.dr_replication_attempts = (snapshot.dr_replication_attempts or 0) + 1
                    fail_count += 1
                    logger.exception(
                        "[scan_and_replicate] UNEXPECTED ERROR processing snapshot %s: %s\n%s",
                        snapshot.id, e, traceback.format_exc(),
                    )

            await session.commit()
            logger.info(
                "[scan_and_replicate] === COMPLETE: success=%d, failed=%d, skipped=%d ===",
                success_count, fail_count, skip_count,
            )

        except Exception as e:
            logger.exception(
                "[scan_and_replicate] FATAL ERROR during scan: %s\n%s",
                e, traceback.format_exc(),
            )
            await session.rollback()


async def replicate_snapshot(snapshot: Snapshot, tenant: Tenant, session):
    """
    Replicate a single snapshot to DR region.
    Uses server-side copy — bytes flow primary → DR via Azure backbone.
    """
    logger.info(
        "[replicate_snapshot] START — snapshot=%s, tenant=%s, container_source=%s",
        snapshot.id, tenant.id, snapshot.id,
    )

    # Defense in depth: the scan predicate already excludes concurrency
    # placeholders, but this function is also reachable directly. A
    # placeholder has zero items, so it would fall through to the
    # "metadata-only" branch below and be marked `replicated` with a
    # dr_blob_path — registering an empty DR copy that looks like a real
    # recovery point. Mark it skipped instead, and set no dr_blob_path /
    # dr_replicated_at. Nothing is deleted or overwritten either way.
    if (getattr(snapshot, "extra_data", None) or {}).get("skipped_concurrent"):
        snapshot.dr_replication_status = "skipped"
        snapshot.dr_error = "Concurrency placeholder (skipped_concurrent) — no data to replicate"
        logger.info(
            "[replicate_snapshot] Snapshot %s is a skipped_concurrent placeholder "
            "(0 items) — not replicating",
            snapshot.id,
        )
        return

    if (getattr(snapshot, "extra_data", None) or {}).get("chat_inventory_format") == 1:
        await _replicate_catalogue_snapshot(snapshot, tenant, session)
        return

    snapshot.dr_replication_status = "in_progress"
    snapshot.dr_replication_attempts = (snapshot.dr_replication_attempts or 0) + 1
    await session.flush()

    # Get source shard and container. The source container depends on the snapshot's
    # resource type — backup-worker writes each workload to its own container, so DR
    # must replicate from the matching one. Previously hardcoded to "files", which
    # only worked for OneDrive/SharePoint and silently 404'd everything else.
    try:
        source_shard = azure_storage_manager.get_default_shard()
        if not source_shard:
            snapshot.dr_replication_status = "failed"
            snapshot.dr_error = "No source storage shard available"
            logger.error("[replicate_snapshot] No source storage shard available for snapshot %s", snapshot.id)
            return

        # Resolve the resource type for this snapshot to pick the right workload suffix.
        resource = await session.get(Resource, snapshot.resource_id)
        resource_type = resource.type.value if resource and hasattr(resource.type, "value") else (str(resource.type) if resource else "")
        workload = _primary_workload(resource_type)
        source_container = azure_storage_manager.get_container_name(str(tenant.id), workload)
        dr_container = f"{source_container}-dr"
    except Exception as e:
        snapshot.dr_replication_status = "failed"
        snapshot.dr_error = f"Failed to resolve storage shard: {e}"
        logger.error("[replicate_snapshot] Failed to resolve storage shard: %s", e)
        return

    # Get DR credentials
    try:
        dr_account_key = decrypt_secret(tenant.dr_storage_account_key_encrypted)
    except Exception as e:
        snapshot.dr_replication_status = "failed"
        snapshot.dr_error = f"Cannot decrypt DR storage key: {e}"
        logger.error(
            "[replicate_snapshot] DR credential decryption failed for tenant %s: %s",
            tenant.id, e,
        )
        return

    dr_account_name = tenant.dr_storage_account_name or ""
    if not dr_account_name:
        snapshot.dr_replication_status = "failed"
        snapshot.dr_error = "DR storage account name not configured"
        logger.error(
            "[replicate_snapshot] DR storage account name missing for tenant %s", tenant.id,
        )
        return

    logger.info(
        "[replicate_snapshot] DR target — account=%s, container=%s",
        dr_account_name, dr_container,
    )

    # Get snapshot items
    try:
        items_result = await session.execute(
            select(SnapshotItem).where(SnapshotItem.snapshot_id == snapshot.id)
        )
        snapshot_items = items_result.scalars().all()
    except Exception as e:
        snapshot.dr_replication_status = "failed"
        snapshot.dr_error = f"Failed to query snapshot items: {e}"
        logger.error("[replicate_snapshot] Failed to query snapshot items for %s: %s", snapshot.id, e)
        return

    if not snapshot_items:
        # Metadata-only snapshot, mark as replicated
        snapshot.dr_replication_status = "replicated"
        snapshot.dr_blob_path = f"{dr_container}/{snapshot.id}"
        snapshot.dr_replicated_at = datetime.now(timezone.utc)
        logger.info(
            "[replicate_snapshot] Snapshot %s has no items (metadata-only) — marking as replicated",
            snapshot.id,
        )
        return

    logger.info(
        "[replicate_snapshot] Replicating %d blob(s) for snapshot %s",
        len(snapshot_items), snapshot.id,
    )

    failed = 0
    total_bytes = 0
    replicated_items = 0

    for item in snapshot_items:
        if not item.blob_path:
            logger.debug(
                "[replicate_snapshot] Skipping item %s — no blob_path",
                item.id,
            )
            continue

        blob_path = item.blob_path
        item_start = time.monotonic()

        try:
            # Generate SAS URL for source blob (read-only, 4 hour expiry)
            source_sas = generate_blob_sas(
                account_name=source_shard.account_name,
                container_name=source_container,
                blob_name=blob_path,
                account_key=source_shard.account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + timedelta(hours=4),
            )
            source_url = (
                f"https://{source_shard.account_name}.blob.core.windows.net/"
                f"{source_container}/{blob_path}?{source_sas}"
            )

            # DR blob URL
            dr_blob_url = f"https://{dr_account_name}.blob.core.windows.net/{dr_container}/{blob_path}"
            dr_blob_client = BlobClient.from_blob_url(dr_blob_url, credential=dr_account_key)

            logger.info(
                "[replicate_snapshot] Copying blob %s → DR...",
                blob_path,
            )

            # Server-side copy — bytes flow via Azure backbone
            poller = await dr_blob_client.start_copy_from_url(
                source_url=source_url,
                metadata={
                    "source_snapshot_id": str(snapshot.id),
                    "source_region": "primary",
                    "replicated_at": datetime.now(timezone.utc).isoformat(),
                },
            )

            # Wait for copy to complete
            await _wait_for_copy(dr_blob_client, timeout_seconds=COPY_TIMEOUT_SECONDS)

            elapsed = time.monotonic() - item_start
            total_bytes += item.content_size or 0
            replicated_items += 1

            logger.info(
                "[replicate_snapshot] Blob %s replicated — %.1fs, %d bytes",
                blob_path, elapsed, item.content_size or 0,
            )

            # Replicate legal hold if enabled
            if tenant.extra_data and tenant.extra_data.get("legal_hold_enabled"):
                try:
                    dr_shard = AzureStorageShard(
                        account_name=dr_account_name,
                        account_key=dr_account_key,
                    )
                    await apply_legal_hold(dr_container, blob_path, shard=dr_shard)
                    logger.info(
                        "[replicate_snapshot] Legal hold applied to DR blob %s",
                        blob_path,
                    )
                except Exception as lh_exc:
                    logger.warning(
                        "[replicate_snapshot] Legal hold FAILED for DR blob %s: %s (non-fatal)",
                        blob_path, lh_exc,
                    )
                    # Non-fatal — don't count as replication failure

        except TimeoutError as te:
            failed += 1
            logger.error(
                "[replicate_snapshot] TIMEOUT — blob %s copy exceeded %ds: %s",
                blob_path, COPY_TIMEOUT_SECONDS, te,
            )
        except Exception as e:
            failed += 1
            logger.error(
                "[replicate_snapshot] FAILED — blob %s: %s\n%s",
                blob_path, e, traceback.format_exc(),
            )

    # Determine final status
    total_items = len([i for i in snapshot_items if i.blob_path])

    if failed == 0:
        snapshot.dr_replication_status = "replicated"
        snapshot.dr_blob_path = f"{dr_container}/{snapshot.id}"
        snapshot.dr_replicated_at = datetime.now(timezone.utc)
        tenant.dr_last_replicated_at = datetime.now(timezone.utc)
        logger.info(
            "[replicate_snapshot] SUCCESS — snapshot %s replicated: %d items, %d bytes, DR container=%s",
            snapshot.id, replicated_items, total_bytes, dr_container,
        )
    elif failed < total_items:
        snapshot.dr_replication_status = "failed"
        snapshot.dr_error = f"Partial: {failed}/{total_items} blobs failed"
        logger.warning(
            "[replicate_snapshot] PARTIAL FAILURE — snapshot %s: %d/%d blobs failed",
            snapshot.id, failed, total_items,
        )
    else:
        snapshot.dr_replication_status = "failed"
        snapshot.dr_error = f"All {total_items} blobs failed replication"
        logger.error(
            "[replicate_snapshot] TOTAL FAILURE — snapshot %s: all %d blobs failed",
            snapshot.id, total_items,
        )

    # Escalate if exceeded max attempts
    if snapshot.dr_replication_status == "failed" and snapshot.dr_replication_attempts >= MAX_REPLICATION_ATTEMPTS:
        logger.error(
            "[replicate_snapshot] MAX ATTEMPTS EXCEEDED — snapshot %s failed %d times, marking permanently failed",
            snapshot.id, snapshot.dr_replication_attempts,
        )
        snapshot.dr_error = f"Permanently failed after {MAX_REPLICATION_ATTEMPTS} attempts: {snapshot.dr_error}"


async def _catalogue_dr_target(tenant, container):
    from shared.chat_catalogue import ChatCatalogueIntegrityError
    from shared.chat_catalogue_dr import AzureCatalogueDRStore
    from shared.azure_storage import read_encryption_scope_state
    if not tenant.dr_region_enabled or not tenant.dr_storage_account_name or not tenant.dr_storage_account_key_encrypted:
        raise ChatCatalogueIntegrityError("catalogue DR target credentials/region are not configured")
    key = decrypt_secret(tenant.dr_storage_account_key_encrypted)
    shard = _create_dr_shard(tenant.dr_storage_account_name, key)
    target = AzureCatalogueDRStore(backend_id=f"dr-azure:{tenant.dr_storage_account_name}",
                                  name="catalogue-dr", shards=[shard])
    try:
        await target.ensure_container(container)
        props = await shard.get_async_client().get_container_client(container).get_container_properties()
        scope = getattr(getattr(props, "encryption_scope", None), "default_encryption_scope", None)
        if scope == "$account-encryption-key":
            scope = None
        uri = None
        if scope:
            observed = await read_encryption_scope_state(container, shard=shard)
            if (observed.get("error") or observed.get("scope") != scope or
                str(observed.get("state")).lower() != "enabled" or not observed.get("key_uri")):
                raise ChatCatalogueIntegrityError("DR customer encryption scope cannot be verified")
            uri = observed["key_uri"]
        encryption = {"mode": "azure_server_side", "scope": scope, "key_uri": uri}
        await target.verify_encryption(container, None, encryption)
        return target, encryption
    except BaseException:
        await target.close()
        raise


async def _replicate_catalogue_snapshot(snapshot, tenant, session):
    """Archive safely; never conflate verified archival with automated DR restore."""
    from types import SimpleNamespace
    from sqlalchemy.orm.attributes import set_committed_value
    from shared.chat_catalogue import ChatCatalogueIntegrityError
    from shared.chat_catalogue_dr import FORMAT, replicate_catalogue_snapshot
    from shared.db_retry import run_catalogue_transaction
    from shared.models import ChatCataloguePin
    from shared.storage.router import router

    if (snapshot.extra_data or {}).get("chat_catalogue_dr_nonretryable"):
        return
    snapshot_id, tenant_id = snapshot.id, tenant.id
    owner_id = uuid4()
    tenant_config = SimpleNamespace(**{name: getattr(tenant, name) for name in
        ("dr_region_enabled", "dr_storage_account_name", "dr_storage_account_key_encrypted")})
    # Release the scanner's read transaction before pins or external storage I/O.
    await session.commit()
    fields = ("dr_replication_status", "dr_replication_attempts", "dr_error", "dr_blob_path", "dr_replicated_at", "extra_data")

    async def mark(db, *, starting=False, archived=None, error=None):
        row = await db.scalar(select(Snapshot).where(Snapshot.id == snapshot_id).with_for_update())
        resource = await db.get(Resource, row.resource_id) if row else None
        if row is None or resource is None or resource.tenant_id != tenant_id:
            raise ChatCatalogueIntegrityError("catalogue DR snapshot ownership no longer exists")
        def state(claimed):
            return {**{name: getattr(row, name) for name in fields}, "claimed": claimed}
        metadata = row.extra_data or {}
        if starting:
            now = await db.scalar(select(func.clock_timestamp()))
            if row.dr_replication_status == "in_progress":
                previous, started = metadata.get("chat_catalogue_dr_attempt_id"), metadata.get("chat_catalogue_dr_started_at")
                try:
                    previous = UUID(previous) if previous else None
                except (ValueError, TypeError):
                    previous = None
                try:
                    started = datetime.fromisoformat(started) if started else None
                except (ValueError, TypeError):
                    started = None
                live_pin = await db.scalar(select(ChatCataloguePin.id).where(
                    ChatCataloguePin.owner_kind == "dr", ChatCataloguePin.owner_id == previous,
                    or_(ChatCataloguePin.expires_at.is_(None), ChatCataloguePin.expires_at > now)).limit(1)) if previous else None
                # Pins renew during long copies. Grace covers target setup before pin acquisition.
                if live_pin or (started and started.utcoffset() is not None and now - started < timedelta(minutes=5)):
                    return state(False)
            if (row.dr_replication_attempts or 0) >= MAX_REPLICATION_ATTEMPTS:
                row.dr_replication_status = "failed"
                row.dr_error = "Catalogue DR attempt budget exhausted after interrupted archival"
                await db.flush()
                return state(False)
            row.dr_replication_attempts = (row.dr_replication_attempts or 0) + 1
            row.extra_data = {**metadata, "chat_catalogue_dr_attempt_id": str(owner_id),
                              "chat_catalogue_dr_started_at": now.isoformat()}
        elif metadata.get("chat_catalogue_dr_attempt_id") != str(owner_id):
            return state(False)
        row.dr_replication_status = "in_progress" if starting else "failed"
        row.dr_error, row.dr_blob_path, row.dr_replicated_at = error, None, None
        if archived is not None:
            row.extra_data = {**(row.extra_data or {}), "chat_catalogue_dr_archive": archived}
        row.extra_data = {**(row.extra_data or {}), "chat_catalogue_dr_nonretryable":
                          bool(not starting and archived and archived.get("verified"))}
        await db.flush()
        return state(True)

    initial = await run_catalogue_transaction(async_session_factory, lambda db: mark(db, starting=True))
    for name in fields:
        set_committed_value(snapshot, name, initial[name])
    if not initial["claimed"]:
        return
    archived = (initial["extra_data"] or {}).get("chat_catalogue_dr_archive")
    target = None
    try:
        if not archived or not archived.get("verified"):
            container = azure_storage_manager.get_container_name(str(tenant_id), "teams") + "-dr"
            target, encryption = await _catalogue_dr_target(tenant_config, container)
            result = await replicate_catalogue_snapshot(session_factory=async_session_factory,
                snapshot_id=snapshot_id, tenant_id=tenant_id, source_router=router,
                target=target, target_container=container, encryption=encryption, owner_id=owner_id)
            archived = {**asdict(result), "format": FORMAT, "verified": True,
                        "verified_at": datetime.now(timezone.utc).isoformat()}
        error = "Catalogue DR archive verified; automatic restore routing is unsupported"
    except asyncio.CancelledError:
        try:
            interrupted = await asyncio.shield(run_catalogue_transaction(async_session_factory,
                lambda db: mark(db, error="Catalogue DR archive interrupted; primary retained")))
            for name in fields:
                set_committed_value(snapshot, name, interrupted[name])
        except Exception:
            logger.exception("[catalogue-dr] Could not record interrupted archive")
        raise
    except Exception as exc:
        error = f"Catalogue DR archive failed: {type(exc).__name__}: {exc}"[:1000]
        archived = None
        logger.warning("[catalogue-dr] snapshot %s archive failed: %s", snapshot_id, error)
    finally:
        if target is not None:
            try:
                await target.close()
            except Exception:
                logger.warning("[catalogue-dr] DR storage client cleanup failed")
    saved = await run_catalogue_transaction(async_session_factory,
        lambda db: mark(db, archived=archived, error=error))
    for name in fields:
        set_committed_value(snapshot, name, saved[name])


def _create_dr_shard(account_name: str, account_key: str) -> AzureStorageShard:
    """Create a temporary AzureStorageShard for the DR storage account."""
    return AzureStorageShard(account_name=account_name, account_key=account_key)


async def _wait_for_copy(blob_client: BlobClient, timeout_seconds: int = 1800):
    """Poll copy operation status until success, failure, or timeout."""
    start = time.monotonic()
    poll_count = 0

    while True:
        poll_count += 1
        elapsed = time.monotonic() - start

        try:
            props = await blob_client.get_blob_properties()
            status = getattr(props.copy, 'status', None)

            if status == "success":
                logger.debug("[_wait_for_copy] Copy succeeded after %.1fs (%d polls)", elapsed, poll_count)
                return
            if status in ("failed", "aborted"):
                status_desc = getattr(props.copy, 'status_description', 'unknown')
                raise RuntimeError(f"Copy {status}: {status_desc}")
            if elapsed > timeout_seconds:
                raise TimeoutError(f"Copy exceeded {timeout_seconds}s timeout (polls={poll_count})")

            # Log progress every 30 seconds
            if poll_count % 6 == 0:  # 6 * 5s = 30s
                pct = getattr(props.copy, 'progress', None)
                logger.info(
                    "[_wait_for_copy] Copy in progress — %.1fs elapsed, progress=%s",
                    elapsed, pct,
                )

        except (TimeoutError, RuntimeError):
            raise
        except Exception as e:
            logger.warning("[_wait_for_copy] Poll error (attempt %d, %.1fs): %s", poll_count, elapsed, e)

        await asyncio.sleep(5)


async def reconcile_dr_lifecycle_policies():
    """
    Every 6 hours: ensure DR containers have identical lifecycle policies as primary.
    """
    logger.info("[reconcile_dr_lifecycle_policies] === START: DR lifecycle reconciliation ===")

    async with async_session_factory() as session:
        try:
            tenants_result = await session.execute(select(Tenant).where(Tenant.dr_region_enabled == True))
            tenants = tenants_result.scalars().all()

            if not tenants:
                logger.info("[reconcile_dr_lifecycle_policies] No tenants with DR enabled — skipping")
                return

            logger.info("[reconcile_dr_lifecycle_policies] Found %d tenant(s) with DR enabled", len(tenants))

            success_count = 0
            fail_count = 0

            for tenant in tenants:
                try:
                    if not tenant.dr_storage_account_name:
                        logger.info(
                            "[reconcile_dr_lifecycle_policies] Tenant %s: DR storage account not configured, skipping",
                            tenant.id,
                        )
                        continue

                    try:
                        dr_key = decrypt_secret(tenant.dr_storage_account_key_encrypted)
                    except Exception as e:
                        logger.error(
                            "[reconcile_dr_lifecycle_policies] Tenant %s: Cannot decrypt DR key: %s",
                            tenant.id, e,
                        )
                        fail_count += 1
                        continue

                    dr_shard = _create_dr_shard(tenant.dr_storage_account_name, dr_key)

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

                    logger.info(
                        "[reconcile_dr_lifecycle_policies] Tenant %s: hot=%dd, cool=%dd, archive=%s",
                        tenant.id, hot, cool, "unlimited" if archive is None else f"{archive}d",
                    )

                    # Iterate every workload that backup-worker may have written to.
                    # Previously hardcoded ["files","azure-vm","azure-sql","azure-postgres"]
                    # which was missing mailbox/teams/entra/onenote/planner/todo/power-*
                    # and had wrong suffixes ("azure-sql" vs actual "azure-sql-db",
                    # "azure-postgres" vs actual "azure-postgresql").
                    all_workloads = sorted({w for candidates in RESOURCE_TYPE_TO_WORKLOADS.values() for w in candidates})
                    for workload in all_workloads:
                        container = f"{azure_storage_manager.get_container_name(str(tenant.id), workload)}-dr"
                        try:
                            result = await apply_lifecycle_policy(container, hot, cool, archive, dr_shard)
                            if result.get("success"):
                                logger.info(
                                    "[reconcile_dr_lifecycle_policies] DR container %s: %d rules applied",
                                    container, result.get("rules_count", 0),
                                )
                                success_count += 1
                            else:
                                logger.warning(
                                    "[reconcile_dr_lifecycle_policies] DR container %s: policy apply failed — %s",
                                    container, result.get("error", "unknown"),
                                )
                                fail_count += 1
                        except Exception as e:
                            logger.error(
                                "[reconcile_dr_lifecycle_policies] DR container %s: exception — %s\n%s",
                                container, e, traceback.format_exc(),
                            )
                            fail_count += 1

                except Exception as e:
                    logger.error(
                        "[reconcile_dr_lifecycle_policies] Tenant %s reconciliation failed: %s\n%s",
                        tenant.id, e, traceback.format_exc(),
                    )
                    fail_count += 1

            logger.info(
                "[reconcile_dr_lifecycle_policies] === COMPLETE: success=%d, failed=%d ===",
                success_count, fail_count,
            )

        except Exception as e:
            logger.exception(
                "[reconcile_dr_lifecycle_policies] FATAL ERROR: %s\n%s",
                e, traceback.format_exc(),
            )


# ============================================================================
# Plan P1 — Chat singleton table replication to DR PG.
#
# The Level 2 architecture moves message bodies + URL cache + chat metadata
# out of per-snapshot snapshot_items and into three tenant-singleton tables:
#   - chat_threads          (per-(tenant,chat) cursor + failure state)
#   - chat_thread_messages  (canonical message bodies, single copy per tenant)
#   - chat_url_cache        (URL → driveItem → blob mapping)
#
# Per-snapshot blob replication above doesn't replicate these PG rows. If
# primary PG is lost, the DR side has all the snapshot_items (replicated
# blob-by-blob to Azure Blob DR) but no chat bodies — every chat in every
# snapshot would render empty.
#
# This worker pulls deltas from each enabled tenant's primary PG and
# upserts into the DR PG (configured via DR_PG_DSN). Cadence: every 10 min,
# incremental for chat_thread_messages (created_at watermark), full upsert
# for the two small metadata tables.
# ============================================================================

import os as _os

_DR_PG_DSN = _os.getenv("DR_PG_DSN", "").strip()
_DR_CHAT_REPL_INTERVAL_S = int(_os.getenv("DR_CHAT_REPL_INTERVAL_S", "600"))


async def _dr_pg_connect():
    """Open a fresh asyncpg connection to the DR PG. Returns None if DSN
    not configured (typical for local dev — DR worker no-ops the chat
    replication loop). Caller is responsible for closing the connection.
    """
    if not _DR_PG_DSN:
        return None
    try:
        import asyncpg as _ap
        return await _ap.connect(_DR_PG_DSN, timeout=10)
    except Exception as e:
        logger.error("[dr-chat-repl] DR PG connect failed: %s", e)
        return None


async def replicate_chat_singletons_once():
    """One pass through the three singleton tables. Idempotent: rerun is
    safe (UPSERT on natural keys).

    Watermark for `chat_thread_messages` is stored on the DR side in a
    small table `dr_chat_replication_state` so worker restarts don't
    re-replicate everything. The other two tables are small (~hundreds
    to ~thousands of rows per tenant) and get a full upsert per pass.
    """
    if not _DR_PG_DSN:
        logger.debug("[dr-chat-repl] DR_PG_DSN unset — skipping pass")
        return
    dr_conn = await _dr_pg_connect()
    if dr_conn is None:
        return
    try:
        # Bootstrap the DR-side state table on first run.
        await dr_conn.execute(
            "CREATE TABLE IF NOT EXISTS dr_chat_replication_state ("
            "  table_name TEXT PRIMARY KEY, "
            "  last_replicated_at TIMESTAMPTZ NOT NULL "
            ")"
        )
        # Watermark read.
        wm_row = await dr_conn.fetchrow(
            "SELECT last_replicated_at FROM dr_chat_replication_state "
            "WHERE table_name = 'chat_thread_messages'"
        )
        watermark = wm_row["last_replicated_at"] if wm_row else None

        # ── Source-side pulls ──
        # Reuse the worker's existing async SQLAlchemy session factory
        # for the SOURCE side (primary PG that backups write to).
        from shared.database import async_session_factory as _src_factory
        from sqlalchemy import text as _text
        async with _src_factory() as src:
            # chat_threads — small, full snapshot.
            threads = (await src.execute(_text(
                "SELECT id, tenant_id, chat_id, chat_type, chat_topic, "
                "       member_names_json, last_updated_at, last_drained_at, "
                "       drain_cursor, drain_failure_state, created_at, "
                "       updated_at "
                "  FROM chat_threads"
            ))).all()
            # chat_url_cache — small, full snapshot.
            urls = (await src.execute(_text(
                "SELECT tenant_id, url_sha256, drive_item_id, content_hash, "
                "       blob_path, content_size, inline_b64, unreachable, "
                "       first_seen_at, last_used_at "
                "  FROM chat_url_cache"
            ))).all()
            # chat_thread_messages — large, incremental.
            if watermark is None:
                msgs = (await src.execute(_text(
                    "SELECT id, chat_thread_id, message_external_id, "
                    "       created_date_time, last_modified_date_time, "
                    "       from_user_id, from_display_name, body_content, "
                    "       body_content_type, deleted_date_time, "
                    "       metadata_raw, content_hash, content_size, "
                    "       created_at "
                    "  FROM chat_thread_messages"
                ))).all()
            else:
                msgs = (await src.execute(_text(
                    "SELECT id, chat_thread_id, message_external_id, "
                    "       created_date_time, last_modified_date_time, "
                    "       from_user_id, from_display_name, body_content, "
                    "       body_content_type, deleted_date_time, "
                    "       metadata_raw, content_hash, content_size, "
                    "       created_at "
                    "  FROM chat_thread_messages "
                    " WHERE created_at > :wm"
                ), {"wm": watermark})).all()
            new_max_created = max(
                (m.created_at for m in msgs if m.created_at is not None),
                default=watermark,
            )

        # ── DR-side upserts ──
        # chat_threads
        for t in threads:
            await dr_conn.execute(
                "INSERT INTO chat_threads ("
                "  id, tenant_id, chat_id, chat_type, chat_topic, "
                "  member_names_json, last_updated_at, last_drained_at, "
                "  drain_cursor, drain_failure_state, created_at, updated_at"
                ") VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9,$10::jsonb,$11,$12) "
                "ON CONFLICT (tenant_id, chat_id) DO UPDATE SET "
                "  chat_type = EXCLUDED.chat_type, "
                "  chat_topic = EXCLUDED.chat_topic, "
                "  member_names_json = EXCLUDED.member_names_json, "
                "  last_updated_at = EXCLUDED.last_updated_at, "
                "  last_drained_at = EXCLUDED.last_drained_at, "
                "  drain_cursor = EXCLUDED.drain_cursor, "
                "  drain_failure_state = EXCLUDED.drain_failure_state, "
                "  updated_at = EXCLUDED.updated_at",
                t.id, t.tenant_id, t.chat_id, t.chat_type, t.chat_topic,
                (t.member_names_json or None),
                t.last_updated_at, t.last_drained_at, t.drain_cursor,
                (t.drain_failure_state or None),
                t.created_at, t.updated_at,
            )
        # chat_url_cache
        for u in urls:
            await dr_conn.execute(
                "INSERT INTO chat_url_cache ("
                "  tenant_id, url_sha256, drive_item_id, content_hash, "
                "  blob_path, content_size, inline_b64, unreachable, "
                "  first_seen_at, last_used_at"
                ") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) "
                "ON CONFLICT (tenant_id, url_sha256) DO UPDATE SET "
                "  drive_item_id = EXCLUDED.drive_item_id, "
                "  content_hash = EXCLUDED.content_hash, "
                "  blob_path = EXCLUDED.blob_path, "
                "  content_size = EXCLUDED.content_size, "
                "  inline_b64 = EXCLUDED.inline_b64, "
                "  unreachable = EXCLUDED.unreachable, "
                "  last_used_at = EXCLUDED.last_used_at",
                u.tenant_id, u.url_sha256, u.drive_item_id, u.content_hash,
                u.blob_path, u.content_size, u.inline_b64, u.unreachable,
                u.first_seen_at, u.last_used_at,
            )
        # chat_thread_messages
        for m in msgs:
            await dr_conn.execute(
                "INSERT INTO chat_thread_messages ("
                "  id, chat_thread_id, message_external_id, "
                "  created_date_time, last_modified_date_time, "
                "  from_user_id, from_display_name, body_content, "
                "  body_content_type, deleted_date_time, metadata_raw, "
                "  content_hash, content_size, created_at"
                ") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13,$14) "
                "ON CONFLICT (chat_thread_id, message_external_id) DO UPDATE SET "
                "  last_modified_date_time = EXCLUDED.last_modified_date_time, "
                "  from_user_id = EXCLUDED.from_user_id, "
                "  from_display_name = EXCLUDED.from_display_name, "
                "  body_content = EXCLUDED.body_content, "
                "  body_content_type = EXCLUDED.body_content_type, "
                "  deleted_date_time = EXCLUDED.deleted_date_time, "
                "  metadata_raw = EXCLUDED.metadata_raw, "
                "  content_hash = EXCLUDED.content_hash, "
                "  content_size = EXCLUDED.content_size",
                m.id, m.chat_thread_id, m.message_external_id,
                m.created_date_time, m.last_modified_date_time,
                m.from_user_id, m.from_display_name, m.body_content,
                m.body_content_type, m.deleted_date_time,
                (m.metadata_raw or None),
                m.content_hash, m.content_size, m.created_at,
            )
        # Persist watermark for the next pass.
        if new_max_created is not None and new_max_created != watermark:
            await dr_conn.execute(
                "INSERT INTO dr_chat_replication_state (table_name, last_replicated_at) "
                "VALUES ('chat_thread_messages', $1) "
                "ON CONFLICT (table_name) DO UPDATE SET last_replicated_at = EXCLUDED.last_replicated_at",
                new_max_created,
            )
        logger.info(
            "[dr-chat-repl] pass complete — threads=%d urls=%d msgs=%d new_wm=%s",
            len(threads), len(urls), len(msgs), new_max_created,
        )
    except Exception as e:
        logger.exception("[dr-chat-repl] pass failed: %s", e)
    finally:
        try:
            await dr_conn.close()
        except Exception:
            pass


async def main():
    from shared.storage.startup import startup_router
    from shared import core_metrics
    core_metrics.init()
    await startup_router()
    logger.info("=== DR Replication Worker Starting ===")
    logger.info("DB: %s@%s:%s/%s", settings.DB_USERNAME, settings.DB_HOST, settings.DB_PORT, settings.DB_NAME)
    logger.info("RABBITMQ_ENABLED: %s", settings.RABBITMQ_ENABLED)
    logger.info("MAX_REPLICATION_ATTEMPTS: %d", MAX_REPLICATION_ATTEMPTS)
    logger.info("COPY_TIMEOUT_SECONDS: %d", COPY_TIMEOUT_SECONDS)

    # Ensure DB tables exist
    try:
        await init_db()
        logger.info("[main] Database initialization complete")
    except Exception as e:
        logger.error("[main] Database initialization failed: %s\n%s", e, traceback.format_exc())
        raise

    # Seaweed reads must retain their envelope-decryption seam in this worker.
    from shared.crypto.bootstrap import install as install_crypto
    await install_crypto(async_session_factory)

    # Start replication scan loop
    async def scan_loop():
        while True:
            try:
                await scan_and_replicate()
            except Exception as e:
                logger.exception("[scan_loop] Scan failed: %s\n%s", e, traceback.format_exc())
            await asyncio.sleep(300)  # every 5 min

    # Start DR lifecycle reconciliation (every 6 hours)
    async def dr_lifecycle_loop():
        while True:
            await asyncio.sleep(6 * 60 * 60)  # 6 hours
            try:
                await reconcile_dr_lifecycle_policies()
            except Exception as e:
                logger.exception("[dr_lifecycle_loop] DR lifecycle reconciliation failed: %s\n%s", e, traceback.format_exc())

    # Plan P1 — chat singleton replication loop (no-op until DR_PG_DSN is set).
    async def chat_singleton_repl_loop():
        if not _DR_PG_DSN:
            logger.info(
                "[dr-chat-repl] DR_PG_DSN not configured — chat replication "
                "loop will idle (per-snapshot blob replication still runs)"
            )
        while True:
            try:
                await replicate_chat_singletons_once()
            except Exception as e:
                logger.exception("[dr-chat-repl] loop iteration failed: %s", e)
            await asyncio.sleep(_DR_CHAT_REPL_INTERVAL_S)

    logger.info(
        "[main] Starting scan loop (5 min), chat-singleton loop (%ds), DR lifecycle loop (6h)",
        _DR_CHAT_REPL_INTERVAL_S,
    )
    await asyncio.gather(scan_loop(), dr_lifecycle_loop(), chat_singleton_repl_loop())


if __name__ == "__main__":
    asyncio.run(main())
