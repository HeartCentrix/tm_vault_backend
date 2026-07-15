"""Enrichment worker — turns backed-up archive OPAQUE blobs into previewable
Graph JSON, as a dedicated, independently-scalable service.

Backup/restore/export never wait on this: an archive item is captured as a 1:1
opaque MAPI blob (lossless, but unparseable), so to PREVIEW it in the browser we
stage it — import the blob into a temp folder of the user's own PRIMARY mailbox,
read it back as normal Graph JSON, write that JSON onto the source ARCHIVE_ITEM
row, delete the temp copy (shared/archive_enrichment.py owns the per-resource
work; shared/archive_export.py owns the staging loop + guaranteed cleanup).

Two drivers, one unit of work (``enrich_resource``):
  • ON-DEMAND — a user opened their archive; snapshot-service published their
    resource to ``enrich.priority``. We do THEIRS first so preview lights up fast.
  • BULK — a background sweep finds every resource with un-enriched archive items
    and enriches them in parallel (bounded), so previews are ready before anyone
    even opens them.

Idempotent + incremental: only rows that still ``needs_enrichment`` are staged,
so on-demand and bulk are safe to overlap and safe to re-fire.
"""
import asyncio
import json
import logging
import os
from uuid import UUID

from shared.config import settings
from shared.database import async_session_factory, init_db
from shared.graph_client import GraphClient
from shared.message_bus import message_bus
from shared.models import Resource, Tenant, ResourceType
from shared.multi_app_manager import multi_app_manager
from shared.azure_storage import azure_storage_manager
from shared import archive_enrichment as eng

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("enrichment-worker")

WORKER_ID = os.environ.get("HOSTNAME") or os.environ.get("RAILWAY_REPLICA_ID") or "enrich"
ENRICH_QUEUE = "enrich.priority"

# In-process claim so on-demand and the bulk sweep don't stage the SAME resource
# twice concurrently (wasted imports + temp folders). enrich_resource is already
# idempotent, so this is an efficiency guard, not a correctness one.
# ponytail: in-memory set covers the single-replica demo. For a multi-replica
# fleet, promote to a DB lease (mark rows PROCESSING before staging — the pure
# needs_enrichment() already treats PROCESSING as "skip").
_inflight: set = set()
_inflight_lock = asyncio.Lock()


async def _claim(resource_id: str) -> bool:
    async with _inflight_lock:
        if resource_id in _inflight:
            return False
        _inflight.add(resource_id)
        return True


async def _release(resource_id: str) -> None:
    async with _inflight_lock:
        _inflight.discard(resource_id)


# ── injected edges (host-specific: Graph app rotation + blob store) ────────
async def _get_graph_client(tenant: Tenant) -> GraphClient:
    app = multi_app_manager.get_next_app()
    return GraphClient(
        client_id=app.client_id,
        client_secret=app.client_secret,
        tenant_id=tenant.external_tenant_id,
    )


def _make_download(shard):
    """Build the per-resource blob fetcher over an already-resolved ``shard``.

    Downloads go through a shard/facade's ``download_blob`` — the manager
    singleton has NO ``download_blob`` of its own (the proven idiom, used by
    snapshot-service + backup-scheduler, is
    ``get_shard_for_resource(resource_id, tenant_id).download_blob(container, path)``)."""
    async def _download(item):
        if not getattr(item, "blob_path", None):
            return None
        try:
            container = azure_storage_manager.get_container_name(str(item.tenant_id), "archive")
            return await shard.download_blob(container, item.blob_path)
        except Exception as e:  # noqa: BLE001
            logger.warning("[%s] archive blob download failed for %s: %s: %s",
                           WORKER_ID, item.blob_path, type(e).__name__, e)
            return None
    return _download


def _make_upload(shard):
    """Build the per-resource attachment-blob uploader over an already-resolved
    ``shard`` — the mirror of ``_make_download``. Archive fileAttachment bytes
    are stored as their own dedup-aware blobs (like EMAIL_ATTACHMENT) so export
    inlines them through the unchanged mail path."""
    from shared.azure_storage import upload_blob_with_dedup

    async def _upload(tenant_id, content_hash, container, blob_path, data):
        return await upload_blob_with_dedup(
            tenant_id, content_hash, container, blob_path, data, shard, max_retries=3,
        )
    return _upload


async def _with_retry(coro_factory, what: str, attempts: int = 4, base_delay: float = 0.5):
    """Retry a DB op on transient Postgres errors (deadlock / serialization).
    Each attempt gets a fresh session via the factory — the aborted txn from a
    deadlock can't be reused. Boot storms (all services redeploying → concurrent
    DDL) make a bare SELECT a deadlock victim; steady state won't."""
    for i in range(attempts):
        try:
            return await coro_factory()
        except Exception as e:  # noqa: BLE001
            msg = str(e).lower()
            transient = "deadlock" in msg or "serialization" in msg
            if transient and i < attempts - 1:
                await asyncio.sleep(base_delay * (2 ** i))
                logger.warning("[%s] %s transient DB error (retry %d): %s",
                               WORKER_ID, what, i + 1, type(e).__name__)
                continue
            raise


async def enrich_one_resource(resource_id) -> dict:
    """Enrich a single USER_ARCHIVE resource end-to-end. Never raises for the
    ordinary skip cases (missing resource/tenant, wrong type, no mailbox); those
    return a ``{"skipped": ...}`` dict so the bulk sweep keeps going."""
    rid = str(resource_id)
    if not await _claim(rid):
        return {"skipped": "in_flight"}
    try:
        async with async_session_factory() as session:
            resource = await session.get(Resource, UUID(rid))
            if not resource:
                return {"skipped": "no_resource"}
            if resource.type != ResourceType.USER_ARCHIVE:
                return {"skipped": "not_archive"}
            tenant = await session.get(Tenant, resource.tenant_id)
            if not tenant:
                return {"skipped": "no_tenant"}
            session.expunge_all()  # detach; loaded columns stay readable

        gc = await _get_graph_client(tenant)
        shard = azure_storage_manager.get_shard_for_resource(
            str(resource.id), str(resource.tenant_id))
        result = await eng.enrich_resource(
            session_factory=async_session_factory,
            resource=resource,
            gc=gc,
            download_blob=_make_download(shard),
            upload_attachment=_make_upload(shard),
            batch_size=int(os.getenv("ARCHIVE_ENRICH_BATCH", "50")),
            worker_id=WORKER_ID,
        )
        return result
    finally:
        await _release(rid)


# ── ON-DEMAND: a user opened their archive → do theirs first ───────────────
async def consume_enrich_priority():
    max_retries = 30
    for attempt in range(max_retries):
        try:
            await message_bus.connect()
            if message_bus.channel:
                break
            raise RuntimeError("Channel is None after connect")
        except Exception as e:  # noqa: BLE001
            if attempt < max_retries - 1:
                logger.warning("[%s] RabbitMQ not ready (%d/%d): %s", WORKER_ID, attempt + 1, max_retries, e)
                await asyncio.sleep(5)
            else:
                logger.error("[%s] Failed to connect to RabbitMQ after %d attempts", WORKER_ID, max_retries)
                raise

    queue = await message_bus.channel.get_queue(ENRICH_QUEUE)
    logger.info("[%s] Listening on %s (on-demand enrichment)...", WORKER_ID, ENRICH_QUEUE)

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                try:
                    body = json.loads(message.body.decode())
                    spec = body.get("spec") or {}
                    resource_id = body.get("resourceId") or spec.get("resourceId")
                    if not resource_id:
                        logger.warning("[%s] enrich message with no resourceId — skip", WORKER_ID)
                        continue
                    logger.info("[%s] on-demand enrich for resource %s", WORKER_ID, resource_id)
                    result = await enrich_one_resource(resource_id)
                    logger.info("[%s] on-demand enrich %s: %s", WORKER_ID, resource_id, result)
                except Exception as e:  # noqa: BLE001
                    logger.exception("[%s] error processing enrich message: %s", WORKER_ID, e)


# ── BULK: sweep every resource with pending archive items, in parallel ─────
async def bulk_sweep_loop():
    if os.getenv("ENRICH_BULK_ENABLED", "true").lower() not in ("1", "true", "yes", "on"):
        logger.info("[%s] bulk sweep disabled (ENRICH_BULK_ENABLED)", WORKER_ID)
        return
    interval = int(os.getenv("ENRICH_BULK_INTERVAL_S", "300"))
    concurrency = int(os.getenv("ENRICH_BULK_CONCURRENCY", "4"))
    limit = int(os.getenv("ENRICH_BULK_BATCH_LIMIT", "50"))
    sem = asyncio.Semaphore(concurrency)
    logger.info("[%s] bulk sweep every %ds, %d resources in parallel", WORKER_ID, interval, concurrency)

    async def _one(rid):
        async with sem:
            try:
                r = await enrich_one_resource(rid)
                if r.get("enriched"):
                    logger.info("[%s] bulk enriched %s: %s", WORKER_ID, rid, r)
            except Exception as e:  # noqa: BLE001
                logger.exception("[%s] bulk enrich failed for %s: %s", WORKER_ID, rid, e)

    async def _find():
        async with async_session_factory() as session:
            return await eng.find_pending_archive_resource_ids(session, limit=limit)

    while True:
        try:
            rids = await _with_retry(_find, "find_pending")
            if rids:
                logger.info("[%s] bulk sweep: %d resource(s) with pending archive items", WORKER_ID, len(rids))
                await asyncio.gather(*[_one(rid) for rid in rids])
        except Exception as e:  # noqa: BLE001
            logger.exception("[%s] bulk sweep error: %s", WORKER_ID, e)
        await asyncio.sleep(interval)


async def main():
    from shared.storage.startup import startup_router
    from shared import core_metrics
    from shared.graph_rate_limiter import graph_rate_limiter

    core_metrics.init()
    await graph_rate_limiter.maybe_init_redis()
    await startup_router()
    logger.info("=== Enrichment Worker Starting ===")
    logger.info("[%s] DB: %s@%s:%s/%s schema=%s", WORKER_ID,
                settings.DB_USERNAME, settings.DB_HOST, settings.DB_PORT, settings.DB_NAME, settings.DB_SCHEMA)
    logger.info("[%s] Graph apps: %d", WORKER_ID, multi_app_manager.app_count)

    await init_db()

    # At-rest encryption hook so enrichment can read encrypted archive blobs
    # (no-op unless BACKUP_ENCRYPTION_* env is configured).
    from shared.crypto import bootstrap as _enc_bootstrap
    if await _enc_bootstrap.install(async_session_factory):
        logger.info("[%s] at-rest encryption hook installed", WORKER_ID)

    await asyncio.gather(
        asyncio.create_task(consume_enrich_priority()),
        asyncio.create_task(bulk_sweep_loop()),
    )


if __name__ == "__main__":
    asyncio.run(main())
