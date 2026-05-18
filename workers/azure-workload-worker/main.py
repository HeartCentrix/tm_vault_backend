"""Azure Workload Backup Worker — handles Azure VM, SQL, and PostgreSQL backups.

This worker is SEPARATE from the M365 backup-worker because:
- M365 backups operate at millisecond scale with thousands of concurrent Graph API calls
- Azure LROs (Long-Running Operations) are minute-to-hour scale
- Combining them would cause M365 throttle starvation when an Azure BACPAC export
  holds a slot for 2 hours

Queues consumed:
- azure.vm (Azure VM backups via Restore Points)
- azure.sql (Azure SQL Database backups via PITR/BACPAC)
- azure.postgres (Azure PostgreSQL backups via native API/pg_dump)
"""
import asyncio
import json
import logging
import uuid
from datetime import datetime
from typing import Dict, Any, Optional

import aio_pika
from sqlalchemy import select

from shared.config import settings
from shared.database import async_session_factory, init_db
from shared.models import Resource, Tenant, Job, Snapshot, SnapshotStatus, JobStatus
from shared.message_bus import message_bus
from shared.azure_storage import azure_storage_manager

from handlers.vm_handler import VmBackupHandler
from handlers.sql_handler import SqlBackupHandler
from handlers.sql_restore_handler import SqlRestoreHandler
from handlers.vm_restore_handler import VmRestoreHandler
from handlers.postgres_handler import PostgresBackupHandler
from handlers.postgres_restore_handler import PostgresRestoreHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("azure-workload-worker")

# Queue configuration. Restore queues are separate so a stuck BACPAC import
# can't stall new VM backups, and so restore and backup share an LRO budget
# cleanly without fighting restore-worker for generic restore.* slots.
BACKUP_QUEUES = [
    ("azure.vm", 5),       # Lower concurrency — VM backups are heavy LROs
    ("azure.sql", 3),      # Even lower — BACPAC exports can take hours
    ("azure.postgres", 3), # Similar to SQL
]
RESTORE_QUEUES = [
    ("azure.restore.vm", 3),
    ("azure.restore.sql", 2),
    ("azure.restore.postgres", 2),
]
QUEUES = BACKUP_QUEUES + RESTORE_QUEUES


class AzureWorkloadWorker:
    """Processes Azure workload backup messages from RabbitMQ queues."""

    def __init__(self, worker_id: str = "azure-worker"):
        self.worker_id = worker_id
        self.vm_handler = VmBackupHandler(worker_id)
        self.sql_handler = SqlBackupHandler(worker_id)
        self.pg_handler = PostgresBackupHandler(worker_id)
        self.vm_restore_handler = VmRestoreHandler(worker_id)
        self.sql_restore_handler = SqlRestoreHandler(worker_id)
        self.pg_restore_handler = PostgresRestoreHandler(worker_id)

    async def start(self):
        """Connect to RabbitMQ and start consuming from all queues."""
        logger.info("=== Azure Workload Worker Starting ===")
        logger.info("DB: %s@%s:%s/%s", settings.DB_USERNAME, settings.DB_HOST, settings.DB_PORT, settings.DB_NAME)
        logger.info("RabbitMQ: %s", settings.RABBITMQ_ENABLED)
        logger.info("ARM Client ID: %s", settings.EFFECTIVE_ARM_CLIENT_ID or "NOT SET (will fallback)")
        logger.info("Backup RG: %s", settings.AZURE_BACKUP_RESOURCE_GROUP)

        # Ensure DB tables exist
        await init_db()

        # Connect to RabbitMQ
        max_retries = 30
        for attempt in range(max_retries):
            try:
                await message_bus.connect()
                if message_bus.channel:
                    break
                raise RuntimeError("Channel is None after connect")
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning("RabbitMQ not ready (attempt %d/%d): %s", attempt + 1, max_retries, e)
                    await asyncio.sleep(5)
                else:
                    logger.error("Failed to connect to RabbitMQ after %d attempts", max_retries)
                    raise

        # Start consumers for all queues
        tasks = []
        for queue_name, prefetch in QUEUES:
            task = asyncio.create_task(self.consume_queue(queue_name, prefetch))
            tasks.append(task)
            logger.info("Started consumer for %s (prefetch=%d)", queue_name, prefetch)

        logger.info("Azure Workload Worker ready, consuming from %d queues", len(QUEUES))
        await asyncio.gather(*tasks)

    async def consume_queue(self, queue_name: str, prefetch_count: int):
        """Consume messages from a specific queue."""
        if not message_bus.channel:
            return

        queue = await message_bus.channel.get_queue(queue_name)
        logger.info("[%s] Listening on %s...", self.worker_id, queue_name)

        is_restore_queue = queue_name.startswith("azure.restore.")

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                try:
                    body = json.loads(message.body.decode())
                    if is_restore_queue:
                        await self.process_restore_message(queue_name, body)
                    else:
                        await self.process_backup_message(body)
                    await message.ack()
                except Exception as e:
                    logger.exception("[%s] Error processing message from %s: %s", self.worker_id, queue_name, e)
                    try:
                        headers = message.headers or {}
                        retry_count = int(headers.get("x-retry-count", 0))
                        if retry_count >= 5:
                            logger.error("[%s] Message exceeded max retries (5), routing to DLQ", self.worker_id)
                            await message.reject(requeue=False)
                        else:
                            await message.nack(requeue=True)
                    except Exception:
                        pass

    async def process_restore_message(self, queue_name: str, message: Dict[str, Any]):
        """Dispatch an Azure restore message to the matching handler.

        Expected shape (produced by job-service.create_restore_message):
            jobId, snapshotIds[], resourceId, tenantId, resourceType, restoreType,
            spec.azureRestoreMode ("FULL_VM" | "DISK" | "FULL" | "PITR" | ...),
            spec.azureRestoreParams (pass-through to handler)
        """
        job_id = uuid.UUID(message["jobId"])
        snapshot_ids = message.get("snapshotIds") or []
        spec = message.get("spec") or {}
        restore_params = spec.get("azureRestoreParams") or {}
        mode = (spec.get("azureRestoreMode") or "").upper()
        resource_type = (message.get("resourceType") or "").upper()

        if not snapshot_ids:
            logger.error("[%s] Restore message %s has no snapshotIds, skipping", self.worker_id, job_id)
            return

        snapshot_id = uuid.UUID(snapshot_ids[0])

        async with async_session_factory() as session:
            job = await session.get(Job, job_id)
            if not job:
                logger.warning("[%s] Restore job %s not found", self.worker_id, job_id)
                return

            # Drop CANCELLED messages at intake — see backup-worker for
            # the full reasoning. cancel_job only flips DB state; the
            # RMQ message can still arrive, and without this check we'd
            # flip status back to RUNNING and re-run the restore.
            _status_name = job.status.name if hasattr(job.status, "name") else str(job.status)
            if _status_name == "CANCELLED":
                logger.info("[%s] Skipping CANCELLED restore job %s", self.worker_id, job_id)
                return

            snapshot = await session.get(Snapshot, snapshot_id)
            if not snapshot:
                job.status = JobStatus.FAILED
                job.error_message = f"Snapshot {snapshot_id} not found"
                await session.commit()
                return

            resource = await session.get(Resource, snapshot.resource_id)
            if not resource:
                job.status = JobStatus.FAILED
                job.error_message = f"Resource {snapshot.resource_id} not found"
                await session.commit()
                return

            tenant_res = await session.execute(select(Tenant).where(Tenant.id == resource.tenant_id))
            tenant = tenant_res.scalar_one_or_none()
            if not tenant:
                job.status = JobStatus.FAILED
                job.error_message = f"Tenant {resource.tenant_id} not found"
                await session.commit()
                return

            job.status = JobStatus.RUNNING
            # Initial progress so the Activity bar moves the moment the
            # worker accepts the message — without this the row sits at
            # 0% until the handler returns 100%.
            job.progress_pct = 5
            await session.commit()

            logger.info("[%s] Restore start: job=%s resource=%s type=%s mode=%s queue=%s",
                        self.worker_id, job_id, resource.display_name, resource_type, mode, queue_name)

            # Audit trail — RUNNING transition. Lets the Audit feed
            # show the moment a worker actually started the restore,
            # distinct from the QUEUED moment captured by trigger_restore.
            await self._audit_restore_event(
                action="RESTORE_RUNNING", outcome="IN_PROGRESS",
                tenant=tenant, resource=resource, job_id=str(job_id), restore_params=restore_params,
            )

            try:
                # Pass job_id into the handlers so they can emit live
                # progress ticks via shared._progress.update_job_pct().
                restore_params = {**restore_params, "job_id": str(job_id)}

                if queue_name == "azure.restore.vm":
                    if mode == "DISK":
                        disk_name = restore_params.get("disk_name") or restore_params.get("target_disk_name")
                        if not disk_name:
                            raise ValueError("disk_name required for DISK restore mode")
                        result = await self.vm_restore_handler.restore_disk(
                            tenant, snapshot, disk_name, restore_params)
                    else:
                        result = await self.vm_restore_handler.restore_vm(
                            tenant, snapshot, restore_params)
                elif queue_name == "azure.restore.sql":
                    if mode == "PITR":
                        result = await self.sql_restore_handler.restore_pitr(
                            tenant, snapshot, restore_params)
                    elif mode == "SCHEMA_ONLY":
                        result = await self.sql_restore_handler.restore_schema_only(
                            tenant, snapshot, restore_params)
                    else:
                        result = await self.sql_restore_handler.restore_full(
                            tenant, snapshot, restore_params)
                elif queue_name == "azure.restore.postgres":
                    result = await self.pg_restore_handler.restore(
                        tenant, snapshot, restore_params)
                else:
                    result = {"success": False, "error": f"Unknown restore queue {queue_name}"}

                if result.get("success"):
                    job.status = JobStatus.COMPLETED
                    job.completed_at = datetime.utcnow()
                else:
                    job.status = JobStatus.FAILED
                    job.error_message = str(result.get("error") or "restore failed")[:1000]

                job.result = result
                job.progress_pct = 100
                await session.commit()
                logger.info("[%s] Restore %s: job=%s result=%s",
                            self.worker_id,
                            "completed" if result.get("success") else "failed",
                            job_id, result)

                await self._audit_restore_event(
                    action="RESTORE_COMPLETED" if result.get("success") else "RESTORE_FAILED",
                    outcome="SUCCESS" if result.get("success") else "FAILURE",
                    tenant=tenant, resource=resource, job_id=str(job_id),
                    restore_params=restore_params, result=result,
                )

            except Exception as e:
                logger.exception("[%s] Restore job %s crashed: %s", self.worker_id, job_id, e)
                job.status = JobStatus.FAILED
                job.error_message = str(e)[:1000]
                job.progress_pct = 100
                await session.commit()
                await self._audit_restore_event(
                    action="RESTORE_FAILED", outcome="FAILURE",
                    tenant=tenant, resource=resource, job_id=str(job_id),
                    restore_params=restore_params, result={"error": str(e)[:500]},
                )

    async def _audit_restore_event(
        self, *, action: str, outcome: str, tenant, resource, job_id: str,
        restore_params: Dict, result: Optional[Dict] = None,
    ) -> None:
        """Best-effort audit emission for a restore lifecycle event.
        Audit-service downtime never blocks the restore."""
        try:
            import httpx as _httpx
            from shared.config import settings as _settings
            payload = {
                "action": action,
                "tenant_id": str(tenant.id) if tenant else None,
                "actor_type": "WORKER",
                "resource_id": str(resource.id) if resource else None,
                "resource_type": (resource.type.value if hasattr(resource.type, "value")
                                  else str(resource.type)) if resource else None,
                "outcome": outcome,
                "job_id": job_id,
                "details": {
                    "target_server": restore_params.get("target_server_name") or restore_params.get("server"),
                    "target_database": restore_params.get("target_database_name") or restore_params.get("targetDatabaseName"),
                    "source_database": restore_params.get("source_database_name") or restore_params.get("sourceDatabase"),
                    **({"error": str(result.get("error"))[:500]} if result and result.get("error") else {}),
                },
            }
            async with _httpx.AsyncClient(timeout=5.0) as _client:
                await _client.post(f"{_settings.AUDIT_SERVICE_URL}/api/v1/audit/log", json=payload)
        except Exception:
            pass

    async def process_backup_message(self, message: Dict[str, Any]):
        """Process a single Azure workload backup message."""
        job_id = uuid.UUID(message["jobId"])
        resource_id = message.get("resourceId")

        if not resource_id:
            logger.error("[%s] No resourceId in message, skipping", self.worker_id)
            return

        async with async_session_factory() as session:
            # Verify job exists
            job = await session.get(Job, job_id)
            if not job:
                logger.warning("[%s] Job %s not found, skipping stale message for %s",
                               self.worker_id, job_id, resource_id)
                return

            # Drop CANCELLED messages at intake — cancel_job leaves the
            # RMQ message in the queue, and without this guard the
            # `status = RUNNING` assignment below would silently reverse
            # the user's cancel and re-run the backup.
            _status_name = job.status.name if hasattr(job.status, "name") else str(job.status)
            if _status_name == "CANCELLED":
                logger.info("[%s] Skipping CANCELLED Azure backup job %s for resource %s",
                            self.worker_id, job_id, resource_id)
                return

            # Fetch resource
            resource = await session.get(Resource, uuid.UUID(resource_id))
            if not resource:
                logger.warning("[%s] Resource %s not found, skipping", self.worker_id, resource_id)
                return

            # Fetch tenant
            result = await session.execute(select(Tenant).where(Tenant.id == resource.tenant_id))
            tenant = result.scalar_one_or_none()
            if not tenant:
                logger.warning("[%s] Tenant not found for resource %s, skipping", self.worker_id, resource_id)
                return

            resource_type = resource.type.value if hasattr(resource.type, 'value') else str(resource.type)
            logger.info("[%s] Processing %s backup for %s (%s, tenant=%s)",
                        self.worker_id, resource_type, resource.display_name, resource_id, tenant.id)

            # Flip job to RUNNING so the Protection page + Activity feed
            # can show "In Progress" while the backup is actually running.
            # Without this the job stayed QUEUED → COMPLETED/FAILED and
            # the UI never showed a running state (the server-side
            # last_backup_status read from jobs.status never hit RUNNING).
            # Denormalized resource.last_backup_status is updated too so
            # queries that read straight off the resource agree.
            job.status = JobStatus.RUNNING
            job.progress_pct = 5  # worker picked the message up
            resource.last_backup_status = "RUNNING"

            # Create snapshot
            snapshot = Snapshot(
                id=uuid.uuid4(),
                resource_id=resource.id,
                job_id=job_id,
                type=job.spec.get("fullBackup", False) and "FULL" or "INCREMENTAL",
                status=SnapshotStatus.IN_PROGRESS,
                started_at=datetime.utcnow(),
                snapshot_label=job.spec.get("note", "azure-workload-backup"),
            )
            session.add(snapshot)
            await session.commit()

            # Route to appropriate handler
            try:
                if resource_type == "AZURE_VM":
                    result = await self.vm_handler.backup(resource, tenant, snapshot, message)
                elif resource_type in ("AZURE_SQL_DB", "AZURE_SQL"):
                    result = await self.sql_handler.backup(resource, tenant, snapshot, message)
                elif resource_type in ("AZURE_POSTGRESQL", "AZURE_POSTGRESQL_SINGLE", "AZURE_PG"):
                    result = await self.pg_handler.backup(resource, tenant, snapshot, message)
                else:
                    logger.warning("[%s] Unknown Azure resource type: %s for %s",
                                   self.worker_id, resource_type, resource_id)
                    result = {"success": False, "error": f"Unsupported type: {resource_type}"}

                # Update snapshot. VM/SQL/PG handlers report the captured
                # blob size as `total_size_bytes`; VM also reports per-disk
                # `size_bytes`. Fall back gracefully so no backend change
                # is needed when a handler renames one but not the other.
                size_bytes = int(
                    result.get("total_size_bytes")
                    or result.get("size_bytes")
                    or 0
                )
                if result.get("success"):
                    snapshot.status = SnapshotStatus.COMPLETED
                    snapshot.item_count = (
                        result.get("disks_copied")
                        or result.get("tables_exported")
                        or 1
                    )
                    # bytes_added and bytes_total both get the captured
                    # size — the Recovery sparkline reads bytes_total and
                    # the per-day delta bars read bytes_added. Without
                    # this, both were 0 and the chart rendered flat.
                    snapshot.bytes_added = size_bytes
                    snapshot.bytes_total = size_bytes
                else:
                    snapshot.status = SnapshotStatus.FAILED

                # Update job
                job.status = JobStatus.COMPLETED if result.get("success") else JobStatus.FAILED
                job.result = result
                job.progress_pct = 100
                if job.status == JobStatus.COMPLETED:
                    job.completed_at = datetime.utcnow()

                # Update resource last backup info
                resource.last_backup_job_id = job_id
                resource.last_backup_at = datetime.utcnow()
                resource.last_backup_status = "COMPLETED" if result.get("success") else "FAILED"
                
                # Update storage_bytes from backup result
                bytes_added = result.get("size_bytes", 0) or result.get("total_size_bytes", 0) or 0
                bytes_removed = result.get("bytes_removed", 0) or 0
                net_change = bytes_added - bytes_removed
                current_storage = resource.storage_bytes or 0
                resource.storage_bytes = max(0, current_storage + net_change)
                
                logger.info("[%s] Updated storage_bytes for %s: %s -> %s bytes (added %s, removed %s)",
                            self.worker_id, resource.id, current_storage, resource.storage_bytes,
                            bytes_added, bytes_removed)

                await session.commit()
                logger.info("[%s] Backup %s for %s (%s)",
                            self.worker_id,
                            "completed" if result.get("success") else "failed",
                            resource.display_name, resource_id)

            except Exception as e:
                error_str = str(e).lower()
                # Detect 404/423 errors — resource no longer exists or is locked
                is_inaccessible = any(kw in error_str for kw in [
                    "not found", "404", "resourcenotfound", "parentresourcenotfound",
                    "locked", "423", "authorizationfailed",
                ])
                
                if is_inaccessible:
                    logger.warning("[%s] Resource %s is INACCESSIBLE (404/423) — marking to skip future backups",
                                   self.worker_id, resource_id)
                    resource.status = "INACCESSIBLE"
                
                snapshot.status = SnapshotStatus.FAILED
                job.status = JobStatus.FAILED
                job.error_message = str(e)
                await session.commit()


async def main():
    from shared.storage.startup import startup_router, shutdown_router
    from shared import core_metrics
    from shared.graph_rate_limiter import graph_rate_limiter
    core_metrics.init()
    await graph_rate_limiter.maybe_init_redis()
    await startup_router()
    try:
        worker = AzureWorkloadWorker()
        await worker.start()
    finally:
        await shutdown_router()


if __name__ == "__main__":
    asyncio.run(main())
