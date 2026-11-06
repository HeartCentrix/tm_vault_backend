"""Fallback scheduled cleanup for the `exports` container. Run daily by
backup-scheduler as a safety net — primary mechanism is Azure lifecycle rules
(see ops/azure-lifecycle-exports.json)."""
from __future__ import annotations

import asyncio
import datetime
import logging
from datetime import datetime as dt, timedelta, timezone
from typing import Optional


async def cleanup_exports(
    *,
    shard,
    container: str = "exports",
    older_than: datetime.timedelta = datetime.timedelta(days=1),
    now: Optional[datetime.datetime] = None,
) -> list:
    """Delete every blob in `container` whose last_modified is older than
    `now - older_than`. Returns the list of deleted blob paths. Safe under
    concurrency — each delete is idempotent."""
    now = now or datetime.datetime.now(datetime.timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=datetime.timezone.utc)
    cutoff = now - older_than

    deleted: list = []
    async for name, props in shard.list_blobs_with_properties(container):
        last_mod = props.get("last_modified")
        if last_mod is None:
            continue
        if last_mod.tzinfo is None:
            last_mod = last_mod.replace(tzinfo=datetime.timezone.utc)
        if last_mod <= cutoff:
            await shard.delete_blob(container, name)
            deleted.append(name)
    return deleted


def azure_export_storage_active() -> bool:
    """False when the active storage backend is known and is not Azure
    (TM: on-prem S3/SeaweedFS), so the Azure export jobs have nothing to
    do. True when it is Azure or the router is not loaded yet."""
    try:
        from shared.storage.router import router
        active = str(router.active_backend_id())
        for backend in router.list_backends():
            if str(backend.backend_id) == active:
                return backend.kind == "azure_blob"
    except Exception:  # noqa: BLE001
        pass
    return True


async def apply_chat_export_lifecycle() -> None:
    """Delete expired exports (>TTL) and cool-tier anything >hot-threshold.

    Iterates every storage-account shard configured in
    ``settings.chat_export_blob_accounts`` and enforces:

    - **TTL**: delete blobs older than ``chat_export_blob_ttl_hours``.
    - **Hot->Cool**: demote blobs older than ``chat_export_hot_tier_hours``
      that are still in the Hot access tier.
    """
    # Import inside the function so the module stays importable in lightweight
    # test environments without Azure SDK installed (cleanup_exports above is
    # shard-agnostic and does not require these deps).
    from azure.storage.blob.aio import BlobServiceClient  # type: ignore
    from shared.config import settings  # type: ignore

    now = dt.now(timezone.utc)
    ttl_cutoff = now - timedelta(hours=settings.chat_export_blob_ttl_hours)
    cool_cutoff = now - timedelta(hours=settings.chat_export_hot_tier_hours)

    for account in settings.chat_export_blob_accounts:
        bsc = BlobServiceClient(
            account_url=f"https://{account}.blob.core.windows.net",
            credential=None,
        )
        container = bsc.get_container_client("exports")
        async for blob in container.list_blobs():
            created = blob.creation_time
            if not created:
                continue
            if created < ttl_cutoff:
                await container.delete_blob(blob.name)
                continue
            if created < cool_cutoff and (blob.blob_tier or "").upper() == "HOT":
                bc = container.get_blob_client(blob.name)
                await bc.set_standard_blob_tier("Cool")


async def _periodic_loop() -> None:
    """Run the chat-export lifecycle enforcement once per hour.

    Invoked from a long-running service (e.g. backup-scheduler) via
    ``asyncio.create_task(_periodic_loop())`` on startup. Not auto-wired from
    this module so callers opt in explicitly.
    """
    log = logging.getLogger(__name__)
    while True:
        try:
            await apply_chat_export_lifecycle()
        except Exception as e:  # noqa: BLE001
            log.warning("chat_export_lifecycle failed: %s", e)
        await asyncio.sleep(3600)
