"""Helpers for routing export jobs to the right RabbitMQ queue.

Lives in `shared/` so every service (job-service, restore-worker, tests) picks
it up via the standard `from shared.X import ...` package path without needing
docker-build-context gymnastics.
"""
import os
from typing import Optional

from shared.config import settings


def pick_export_queue(total_bytes: int = 0, include_attachments: bool = True) -> str:
    """Return the RabbitMQ queue for an export job.

    Heavy exports go to a dedicated worker pool with larger memory limits.
    Both keyword and positional arg forms are accepted for back-compat with
    existing callers.

    The ``include_attachments`` filter used to gate heavy routing — for PST
    exports we now route by total size regardless, since even no-attachment
    100GB exports are heavy by item count alone.
    """
    if not settings.HEAVY_EXPORT_ENABLED:
        return "restore.normal"
    if total_bytes >= settings.HEAVY_EXPORT_THRESHOLD_BYTES:
        return settings.HEAVY_EXPORT_QUEUE
    return "restore.normal"


# Always-heavy resource types: file-content workloads dominated by binary bytes.
# These bypass the normal pool entirely so MAILBOX / ENTRA / chat snapshots on
# the shared lanes don't starve behind a single 80 GB OneDrive. Type-based gate
# (not byte-threshold) because an in-progress OneDrive of unknown size still
# needs the heavy pool — we can't measure it until the worker calls into Graph.
_HEAVY_BACKUP_TYPES = {
    "USER_ONEDRIVE",
    "ONEDRIVE",
    "SHAREPOINT_SITE",
    "POWER_BI",
    # Online Archive (USER_ARCHIVE, jrue-only pilot ITO-9777) — full-item
    # export via exportItems is just as content-heavy as OneDrive; this
    # map (not shared.routing_fence's queue-by-workload map, which is
    # inert — nothing consults it) is the real production dispatch gate.
    "USER_ARCHIVE",
}


def pick_backup_queue(
    *,
    drive_bytes_estimate: int = 0,
    resource_type: str,
    default_queue: Optional[str] = None,
) -> str:
    """Return the RabbitMQ queue for a backup job.

    Heavy resource types (OneDrive / SharePoint / Power BI) always go to the
    dedicated heavy pool so the regular backup-worker replicas stay free for
    quick MAILBOX / ENTRA / USER_* work. Other types use ``default_queue``
    (or ``BACKUP_WORKER_QUEUE`` if omitted) so the trigger path's queue
    semantics (e.g. "backup.urgent" for user-initiated backups) are kept.
    """
    fallback = default_queue or settings.BACKUP_WORKER_QUEUE
    if not settings.BACKUP_HEAVY_ENABLED:
        return fallback
    if resource_type in _HEAVY_BACKUP_TYPES:
        return settings.BACKUP_HEAVY_QUEUE
    return fallback


_HEAVY_RESTORE_THRESHOLD = int(
    os.getenv("HEAVY_RESTORE_THRESHOLD_BYTES", str(20 * 1024**3))
)  # default 20 GiB — catches whales sooner on large workloads


def pick_restore_queue(total_bytes: int) -> str:
    """Route restore jobs by scope size.

    Large restores (>HEAVY_RESTORE_THRESHOLD_BYTES) run on restore.heavy
    so they don't block small quick restores on the shared restore.normal
    queue. Threshold is env-tunable for very large M365 deployments where
    50 GB used to be too high a bar.
    """
    if total_bytes and total_bytes > _HEAVY_RESTORE_THRESHOLD:
        # Same name the consumer subscribes to (restore_queue_plan) — a
        # literal here left >20GiB restores on an unconsumed queue when
        # HEAVY_EXPORT_QUEUE is overridden (ITO-10275 review).
        return settings.HEAVY_EXPORT_QUEUE
    return "restore.normal"
