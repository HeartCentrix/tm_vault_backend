"""Shared message bus for inter-service communication via RabbitMQ"""
import asyncio
import json
from typing import Optional, Callable, Dict, Any
from datetime import datetime
import aio_pika
from aio_pika import Message, DeliveryMode

from shared.config import settings


class MessageBus:
    """RabbitMQ message bus for async communication between microservices"""
    
    def __init__(self):
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.exchange: Optional[aio_pika.Exchange] = None
    
    async def connect(self, max_retries: int = 0, retry_delay: int = 5):
        """Connect to RabbitMQ with retry.

        Default (``max_retries=0``) retries FOREVER with capped exponential
        backoff (5s → 10s → 15s → … capped at 30s). This replaces the old
        behavior of giving up after 10 attempts, which left services
        permanently dead if RabbitMQ had even a brief startup hiccup and
        required a container restart to recover.

        Pass ``max_retries > 0`` for a bounded retry that raises on exhaustion
        — only callers who genuinely want to fail-fast should use that."""
        if not settings.RABBITMQ_ENABLED:
            print("[MESSAGE_BUS] RabbitMQ disabled by config", flush=True)
            return

        # Idempotency guard: `connect()` is a singleton-level call invoked
        # from several APScheduler jobs on every interval tick (e.g.
        # _reconcile_stuck_queued_jobs, _sweep_tier2_gaps). aio_pika keeps
        # abandoned `connect_robust` connections alive, so overwriting
        # self.connection on each tick leaked one connection per tick
        # (the scheduler pod accumulated ~150 open connections). When a live
        # connection already exists, reuse it instead of opening another.
        if self.connection is not None and not self.connection.is_closed:
            return

        attempt = 0
        while True:
            attempt += 1
            try:
                # aio_pika.connect_robust has its own auto-reconnect logic
                # once the INITIAL connection is established, so this outer
                # loop only needs to cover the cold-boot window before the
                # first connect succeeds.
                # Heartbeat bumped to 1 week so hour-long backup jobs don't
                # trigger consumer redelivery — OneDrive drives in the 100 GB+
                # class can legitimately hold a message for many hours.
                self.connection = await aio_pika.connect_robust(
                    settings.RABBITMQ_URL,
                    heartbeat=settings.RABBITMQ_CONSUMER_HEARTBEAT_SECONDS,
                )
                self.channel = await self.connection.channel()
                self.exchange = await self.channel.declare_exchange(
                    "tm.exchange", aio_pika.ExchangeType.DIRECT, durable=True
                )

                # Declare queues with QoS settings for mass backup
                await self._declare_queue("backup.urgent", routing_key="backup.urgent")
                await self._declare_queue("backup.high", routing_key="backup.high")
                await self._declare_queue("backup.normal", routing_key="backup.normal")
                await self._declare_queue("backup.low", routing_key="backup.low")
                # Heavy queue routes oversized OneDrive drives to a dedicated
                # worker pool (backup-worker-heavy) so regular backups aren't
                # blocked waiting for 500 GB drives to finish.
                await self._declare_queue(
                    settings.BACKUP_HEAVY_QUEUE,
                    routing_key=settings.BACKUP_HEAVY_QUEUE,
                )
                # Cross-replica OneDrive partition split — one message per
                # shard, claimed by any free backup_worker replica. The
                # coordinator (initial USER_ONEDRIVE handler) publishes N
                # of these per partitioned drive; consumers each drain
                # their assigned file_ids slice.
                await self._declare_queue(
                    "backup.onedrive_partition",
                    routing_key="backup.onedrive_partition",
                )
                # USER_CHATS per-shard partition queue — same shape as
                # OneDrive's but the shards carry chat-id allowlists.
                # Workers re-enter the USER_CHATS coordinator pipeline
                # with a `chat_ids_filter` scoping the drain to the
                # shard's chats. Separate queue (vs reusing
                # backup.onedrive_partition) so per-lane prefetch can
                # differ — chat shards are I/O-light, OneDrive shards
                # are I/O-heavy.
                await self._declare_queue(
                    "backup.chats_partition",
                    routing_key="backup.chats_partition",
                )
                # Phase 3.2: Mail folder partition queue. Shards carry a
                # `folder_ids: [...]` allowlist; workers re-enter the
                # mailbox handler scoped to those folders. Covers all
                # four mailbox resource types (USER_MAIL, MAILBOX,
                # SHARED_MAILBOX, ROOM_MAILBOX) — they share the same
                # `backup_mailbox` handler.
                await self._declare_queue(
                    "backup.mail_partition",
                    routing_key="backup.mail_partition",
                )
                # Phase 3.3: SharePoint drive partition queue. Shards
                # carry a `drive_ids: [...]` allowlist for one site;
                # workers re-enter `backup_sharepoint` scoped to those
                # drives. SharePoint lists (non-doc libraries) stay
                # single-replica for now.
                await self._declare_queue(
                    "backup.sharepoint_partition",
                    routing_key="backup.sharepoint_partition",
                )
                # Phase 3.4: Groups (Teams channels) partition queue.
                # Shards carry a `channel_ids: [...]` allowlist for one
                # team; workers re-enter backup_teams_single scoped to
                # those channels. Mailbox + SP sub-backups are owned
                # by the coordinator and skipped on consumer entries.
                await self._declare_queue(
                    "backup.groups_partition",
                    routing_key="backup.groups_partition",
                )
                # Phase 3.5: Entra directory partition queue. Shards
                # carry a `category_ids: ["USERS","GROUPS",...]`
                # allowlist; workers re-enter backup_entra_directory
                # scoped to those categories. Each of the 8 directory
                # categories (Users / Groups / Roles / Security /
                # Audit / Applications / Intune / Admin Units) is an
                # independent Graph fetch + persist so partitioning
                # them across replicas is a near-pure speedup.
                await self._declare_queue(
                    "backup.entra_partition",
                    routing_key="backup.entra_partition",
                )
                await self._declare_queue("restore.urgent", routing_key="restore.urgent")
                await self._declare_queue("restore.normal", routing_key="restore.normal")
                await self._declare_queue("restore.low", routing_key="restore.low")
                # Archive preview-enrichment — dedicated queue drained by the
                # standalone enrichment-worker (opaque blob → previewable JSON),
                # kept off the restore pools so it scales independently. NO
                # x-consumer-timeout: enrichment messages are fast, and (critically)
                # this broker rejects that arg on a NEW queue's creation — applying
                # it here would fail every service's connect (see _declare_queue).
                await self._declare_queue("enrich.priority", routing_key="enrich.priority",
                                          apply_consumer_timeout=False)
                # Heavy restore pool — gated by HEAVY_EXPORT_ENABLED but
                # the queue must exist either way so restore-worker-heavy
                # can bind to it on startup without passive=True failing.
                await self._declare_queue(
                    settings.HEAVY_EXPORT_QUEUE,
                    routing_key=settings.HEAVY_EXPORT_QUEUE,
                )
                await self._declare_queue("discovery.m365", routing_key="discovery.m365")
                await self._declare_queue("discovery.azure", routing_key="discovery.azure")
                # Per-user Tier-2 content discovery (USER_MAIL / USER_ONEDRIVE /
                # USER_CONTACTS / USER_CALENDAR / USER_CHATS). Separate from
                # tenant-wide discovery.m365 because Tier-2 is a fan-out of
                # many small per-user jobs that mustn't block tenant rediscovery.
                await self._declare_queue("discovery.tier2", routing_key="discovery.tier2")
                await self._declare_queue("notification", routing_key="notification")
                await self._declare_queue("export.normal", routing_key="export.normal")
                await self._declare_queue("delete.low", routing_key="delete.low")
                await self._declare_queue("sla.monitor", routing_key="sla.monitor")
                await self._declare_queue("report.normal", routing_key="report.normal")
                await self._declare_queue("audit.events", routing_key="audit.events")

                # Azure workload queues (separate from M365 backup for LRO isolation)
                await self._declare_queue("azure.vm", routing_key="azure.vm")
                await self._declare_queue("azure.sql", routing_key="azure.sql")
                await self._declare_queue("azure.postgres", routing_key="azure.postgres")

                # Azure restore queues — routed to azure-workload-worker, not restore-worker
                await self._declare_queue("azure.restore.vm", routing_key="azure.restore.vm")
                await self._declare_queue("azure.restore.sql", routing_key="azure.restore.sql")
                await self._declare_queue("azure.restore.postgres", routing_key="azure.restore.postgres")

                # Set channel QoS (per-consumer prefetch)
                await self.channel.set_qos(prefetch_count=50)

                print(f"[MESSAGE_BUS] Connected to RabbitMQ successfully (attempt {attempt})", flush=True)
                return  # Success!

            except Exception as e:
                # Bounded retry hit its limit → bubble up the last error.
                if max_retries and attempt >= max_retries:
                    print(
                        f"[MESSAGE_BUS] Failed to connect after {max_retries} attempts: "
                        f"{type(e).__name__}: {e}",
                        flush=True,
                    )
                    raise

                # Capped exponential backoff: delay grows every 5 attempts,
                # maxing out at 30s so a long-down broker doesn't stretch
                # into multi-minute gaps between retries.
                delay = min(30, retry_delay * (1 + attempt // 5))
                print(
                    f"[MESSAGE_BUS] Connection attempt {attempt} failed "
                    f"({type(e).__name__}: {e}); retrying in {delay}s",
                    flush=True,
                )
                await asyncio.sleep(delay)
    
    async def disconnect(self):
        if self.connection:
            await self.connection.close()
    
    async def _declare_queue(self, queue_name: str, routing_key: str,
                             apply_consumer_timeout: bool = True):
        # Declare a Dead Letter Queue for failed messages
        dlq_name = f"{queue_name}.dlq"
        dlq = await self.channel.declare_queue(dlq_name, durable=True)
        dlq_routing_key = f"{routing_key}.dlq"
        await dlq.bind(self.exchange, dlq_routing_key)

        # Main queue routes rejected messages to the DLQ. Override
        # x-consumer-timeout at queue level so RabbitMQ's default (30 min on
        # 3.12+) doesn't redeliver week-long OneDrive backups mid-run.
        args = {
            "x-dead-letter-exchange": "tm.exchange",
            "x-dead-letter-routing-key": dlq_routing_key,
        }
        # This broker REJECTS x-consumer-timeout at queue *creation* (the
        # pre-existing queues were created before it and are grandfathered; a
        # brand-new queue with it fails PRECONDITION "invalid arg" and takes
        # down the whole connect for every service). Only apply it where it's
        # meaningful (long-running drains); fast queues opt out so a NEW queue
        # can always be created.
        if apply_consumer_timeout:
            args["x-consumer-timeout"] = settings.RABBITMQ_CONSUMER_TIMEOUT_MS
        queue = await self.channel.declare_queue(
            queue_name,
            durable=True,
            arguments=args,
        )
        await queue.bind(self.exchange, routing_key)
    
    async def publish(self, routing_key: str, message: Dict[str, Any], priority: int = 5):
        """Publish message to queue"""
        if not settings.RABBITMQ_ENABLED or not self.exchange:
            return
        
        message_body = json.dumps(message).encode()
        await self.exchange.publish(
            Message(
                message_body,
                delivery_mode=DeliveryMode.PERSISTENT,
                priority=priority,
                headers={"published_at": datetime.utcnow().isoformat()},
            ),
            routing_key=routing_key,
        )
    
    async def consume(self, queue_name: str, callback: Callable):
        """Consume messages from queue"""
        if not settings.RABBITMQ_ENABLED:
            return
        
        queue = await self.channel.get_queue(queue_name)
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                # The exception must escape `message.process()` — catching it
                # inside made the context manager exit cleanly and ACK a
                # message whose handler had raised, so audit events were
                # silently destroyed on any handler error.
                #
                # requeue=True + reject_on_redelivered=True gives bounded
                # redelivery: first failure is requeued, and if it fails
                # again the redelivery is rejected without requeue, which
                # the queue's x-dead-letter-exchange (see _declare_queue)
                # routes to <queue>.dlq. Two attempts, then the message is
                # parked for an operator — never a hot-loop, never a drop.
                try:
                    async with message.process(
                        requeue=True, reject_on_redelivered=True,
                    ):
                        body = json.loads(message.body.decode())
                        await callback(body)
                except Exception as e:
                    # Caught OUT here so one bad message doesn't tear down
                    # the consumer; process() has already nacked/DLQ'd it.
                    print(
                        f"Error processing message from {queue_name} "
                        f"(requeued or dead-lettered): {e}",
                        flush=True,
                    )


# Global message bus instance
message_bus = MessageBus()


def create_backup_message(job_id: str, resource_id: str, tenant_id: str, full_backup: bool = False) -> dict:
    return {
        "jobId": job_id,
        "resourceId": resource_id,
        "tenantId": tenant_id,
        "type": "FULL" if full_backup else "INCREMENTAL",
        "priority": 1 if full_backup else 5,
        "createdAt": datetime.utcnow().isoformat(),
    }


def create_chats_partition_message(
    *,
    partition_id: str,
    snapshot_id: str,
    job_id: str,
    tenant_id: str,
    resource_id: str,
    chat_ids: list,
) -> dict:
    """Envelope for one shard of a partitioned USER_CHATS snapshot.

    Consumed by `_process_chats_partition_message` in backup-worker.
    The consumer atomically claims the snapshot_partitions row, then
    re-enters the existing USER_CHATS coordinator pipeline (via the
    same `_backup_user_content_parallel` path) with `chat_ids_filter`
    scoping its drain to this shard's chats. Reusing the existing
    pipeline avoids re-implementing the complex chat metadata +
    member resolution + drain logic.
    """
    return {
        "messageType": "BACKUP_CHATS_PARTITION",
        "partitionId": partition_id,
        "snapshotId": snapshot_id,
        "jobId": job_id,
        "tenantId": tenant_id,
        "resourceId": resource_id,
        "chatIds": chat_ids,
        "createdAt": datetime.utcnow().isoformat(),
    }


def create_mail_partition_message(
    *,
    partition_id: str,
    snapshot_id: str,
    job_id: str,
    tenant_id: str,
    resource_id: str,
    folder_ids: list,
    resource_type: str = "USER_MAIL",
) -> dict:
    """Envelope for one shard of a partitioned mailbox snapshot.

    Covers all four mailbox resource types (USER_MAIL, MAILBOX,
    SHARED_MAILBOX, ROOM_MAILBOX) — they share the same handler. The
    consumer atomically claims the snapshot_partitions row, then runs
    the existing per-folder drain over `folder_ids` only. Per-folder
    delta tokens are written to `mail_folder_delta` (atomic per-row;
    no RMW race across shards).
    """
    return {
        "messageType": "BACKUP_MAIL_PARTITION",
        "partitionId": partition_id,
        "snapshotId": snapshot_id,
        "jobId": job_id,
        "tenantId": tenant_id,
        "resourceId": resource_id,
        "folderIds": folder_ids,
        "resourceType": resource_type,
        "createdAt": datetime.utcnow().isoformat(),
    }


def create_sharepoint_partition_message(
    *,
    partition_id: str,
    snapshot_id: str,
    job_id: str,
    tenant_id: str,
    resource_id: str,
    drive_ids: list,
    site_id: str,
) -> dict:
    """Envelope for one shard of a partitioned SharePoint site snapshot.

    The consumer atomically claims the snapshot_partitions row, then
    drains only the drives in `drive_ids`. Per-drive delta tokens are
    written to `sharepoint_drive_delta` (atomic per-row; no RMW race).
    """
    return {
        "messageType": "BACKUP_SHAREPOINT_PARTITION",
        "partitionId": partition_id,
        "snapshotId": snapshot_id,
        "jobId": job_id,
        "tenantId": tenant_id,
        "resourceId": resource_id,
        "driveIds": drive_ids,
        "siteId": site_id,
        "createdAt": datetime.utcnow().isoformat(),
    }


def create_groups_partition_message(
    *,
    partition_id: str,
    snapshot_id: str,
    job_id: str,
    tenant_id: str,
    resource_id: str,
    channel_ids: list,
    team_id: str,
) -> dict:
    """Envelope for one shard of a partitioned Teams-channel snapshot.

    The consumer atomically claims the snapshot_partitions row, then
    drains only the channels in `channel_ids` for `team_id`. Mirrors
    the SharePoint partition envelope — `team_id` plays the role
    `site_id` plays for SP partitions (lets the consumer scope the
    drain even when the M365_GROUP path supplies a proxy resource
    with a different external_id).
    """
    return {
        "messageType": "BACKUP_GROUPS_PARTITION",
        "partitionId": partition_id,
        "snapshotId": snapshot_id,
        "jobId": job_id,
        "tenantId": tenant_id,
        "resourceId": resource_id,
        "channelIds": channel_ids,
        "teamId": team_id,
        "createdAt": datetime.utcnow().isoformat(),
    }


def create_entra_partition_message(
    *,
    partition_id: str,
    snapshot_id: str,
    job_id: str,
    tenant_id: str,
    resource_id: str,
    category_ids: list,
    directory_id: str,
) -> dict:
    """Envelope for one shard of a partitioned Entra directory snapshot.

    The consumer atomically claims the snapshot_partitions row, then
    drains only the categories in `category_ids` for the directory
    resource (ENTRA_DIRECTORY). Categories are one of:
    USERS / GROUPS / ROLES / SECURITY / AUDIT / APPLICATIONS /
    INTUNE / ADMIN_UNITS. `directory_id` mirrors `team_id` /
    `site_id` semantics — pass the ENTRA_DIRECTORY resource's
    external_id so consumers can re-target if needed.
    """
    return {
        "messageType": "BACKUP_ENTRA_PARTITION",
        "partitionId": partition_id,
        "snapshotId": snapshot_id,
        "jobId": job_id,
        "tenantId": tenant_id,
        "resourceId": resource_id,
        "categoryIds": category_ids,
        "directoryId": directory_id,
        "createdAt": datetime.utcnow().isoformat(),
    }


def create_onedrive_partition_message(
    *,
    partition_id: str,
    snapshot_id: str,
    job_id: str,
    tenant_id: str,
    resource_id: str,
    drive_id: str,
) -> dict:
    """Envelope for one shard of a partitioned OneDrive snapshot.

    Consumed by `_consume_onedrive_partition` in backup-worker. The
    consumer atomically claims the snapshot_partitions row, loads the
    file_ids JSON column, builds a `file_items` list in the shape
    `_useronedrive_backup_files` expects, and drains the shard.
    """
    return {
        "messageType": "BACKUP_ONEDRIVE_PARTITION",
        "partitionId": partition_id,
        "snapshotId": snapshot_id,
        "jobId": job_id,
        "tenantId": tenant_id,
        "resourceId": resource_id,
        "driveId": drive_id,
        "createdAt": datetime.utcnow().isoformat(),
    }


def create_mass_backup_message(
    job_id: str,
    tenant_id: str,
    resource_type: str,
    resource_ids: list[str],
    sla_policy_id: str = None,
    full_backup: bool = False
) -> dict:
    """Create a mass backup message for batch processing"""
    return {
        "jobId": job_id,
        "tenantId": tenant_id,
        "resourceType": resource_type,
        "resourceIds": resource_ids,  # Batch of resource IDs
        "type": "FULL" if full_backup else "INCREMENTAL",
        "priority": 1 if sla_policy_id is None else 5,
        "slaPolicyId": sla_policy_id,
        "triggeredBy": "SCHEDULED",
        "snapshotLabel": "scheduled",
        "forceFullBackup": full_backup,
        "createdAt": datetime.utcnow().isoformat(),
        "batchSize": len(resource_ids),
    }


AZURE_RESTORE_QUEUE_BY_TYPE = {
    "AZURE_VM": "azure.restore.vm",
    "AZURE_SQL_DB": "azure.restore.sql",
    "AZURE_SQL": "azure.restore.sql",
    "AZURE_POSTGRESQL": "azure.restore.postgres",
    "AZURE_POSTGRESQL_SINGLE": "azure.restore.postgres",
    "AZURE_PG": "azure.restore.postgres",
}


def create_restore_message(
    job_id: str,
    restore_type: str = "IN_PLACE",
    snapshot_ids: list = None,
    item_ids: list = None,
    resource_id: str = None,
    tenant_id: str = None,
    spec: dict = None,
    resource_type: str = None,
) -> dict:
    """Create a restore message for the appropriate restore worker.

    Azure resources route to the azure-workload-worker's azure.restore.* queues
    so long-running Azure LROs don't starve M365 item restores on restore.normal.
    """
    priority_map = {
        "IN_PLACE": 5,
        "CROSS_USER": 3,
        "CROSS_RESOURCE": 4,
        "EXPORT_PST": 6,
        "EXPORT_ZIP": 7,
        "DOWNLOAD": 8,
        "ARCHIVE_ENRICH": 9,  # background preview-enrichment — lowest priority
    }

    queue_map = {
        "IN_PLACE": "restore.normal",
        "CROSS_USER": "restore.urgent",
        "CROSS_RESOURCE": "restore.normal",
        "EXPORT_PST": "restore.normal",
        "EXPORT_ZIP": "restore.low",
        "DOWNLOAD": "restore.low",
        # Archive preview-enrichment runs on its own dedicated service +
        # queue (workers/enrichment-worker), NOT the restore workers — so it
        # scales independently and never touches the restore pools. Publishers
        # normally send to enrich.priority directly; kept here so this router
        # stays correct if ever reused for it.
        "ARCHIVE_ENRICH": "enrich.priority",
    }

    queue = AZURE_RESTORE_QUEUE_BY_TYPE.get(resource_type or "")
    if not queue:
        queue = queue_map.get(restore_type, "restore.normal")

    # Whale-traffic routing: spec.totalBytes (when populated by job-service
    # from a snapshot size estimate) lets us route oversized exports /
    # restores to the dedicated heavy pool so they don't starve the normal
    # queue. Falls back silently when the field is missing.
    spec_dict = spec or {}
    total_bytes = int(spec_dict.get("totalBytes") or 0)
    if total_bytes > 0 and queue in ("restore.normal", "restore.low"):
        try:
            from shared.export_routing import pick_export_queue, pick_restore_queue
            include_attachments = bool(spec_dict.get("includeAttachments", True))
            if restore_type in ("EXPORT_PST", "EXPORT_ZIP"):
                heavy_queue = pick_export_queue(total_bytes, include_attachments)
            else:
                heavy_queue = pick_restore_queue(total_bytes)
            if heavy_queue:
                queue = heavy_queue
        except Exception:
            # pick_*_queue is best-effort routing; never block the publish
            # because of a config import quirk.
            pass

    return {
        "jobId": job_id,
        "restoreType": restore_type,
        "resourceType": resource_type,
        "snapshotIds": snapshot_ids or [],
        "itemIds": item_ids or [],
        "resourceId": resource_id,
        "tenantId": tenant_id,
        "spec": spec or {},
        "type": "RESTORE",
        "priority": priority_map.get(restore_type, 5),
        "queue": queue,
        "createdAt": datetime.utcnow().isoformat(),
    }


def create_discovery_message(tenant_id: str, tenant_type: str) -> dict:
    return {
        "tenantId": tenant_id,
        "type": tenant_type,
        "createdAt": datetime.utcnow().isoformat(),
    }


def create_notification_message(alert_id: str, severity: str, message: str) -> dict:
    return {
        "alertId": alert_id,
        "severity": severity,
        "message": message,
        "createdAt": datetime.utcnow().isoformat(),
    }


def create_audit_event_message(
    action: str,
    tenant_id: str,
    org_id: str = None,
    actor_type: str = "SYSTEM",
    actor_id: str = None,
    actor_email: str = None,
    resource_id: str = None,
    resource_type: str = None,
    resource_name: str = None,
    outcome: str = "SUCCESS",
    job_id: str = None,
    snapshot_id: str = None,
    details: dict = None,
) -> dict:
    """Create an audit event message for the audit.events queue"""
    return {
        "action": action,
        "tenantId": tenant_id,
        "orgId": org_id,
        "actorType": actor_type,
        "actorId": actor_id,
        "actorEmail": actor_email,
        "resourceId": resource_id,
        "resourceType": resource_type,
        "resourceName": resource_name,
        "outcome": outcome,
        "jobId": job_id,
        "snapshotId": snapshot_id,
        "details": details or {},
        "createdAt": datetime.utcnow().isoformat(),
    }
