"""PST export — planner + orchestrator.

PstGroupPlanner groups SnapshotItems into PstGroups by mailbox, folder, or
individual item.  PstExportOrchestrator plans the groups, dispatches the
correct per-type writer, ZIPs the resulting ``.pst`` files, uploads the
archive to blob storage, and reports progress via an optional async
callback.

PST file generation is delegated to the bundled ``pst_convert`` CLI
(see :mod:`shared.pstwriter_cli`); this module owns planning, ZIP
assembly, blob upload, checkpointing, and progress reporting.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path

# ---------------------------------------------------------------------------
# Tunables (all env-configurable for production deployment)
# ---------------------------------------------------------------------------
# PST_WRITE_CONCURRENCY        (default 1)    — per-process pst_convert semaphore
# PST_DISK_OVERHEAD_MULTIPLIER (default 2.0)  — how much disk per source byte
# PST_DISK_FLOOR_BYTES         (default 256MB)— min free bytes to reserve
# PST_MEMORY_LIMIT_MB          (default 0)    — RSS soft cap; 0=disabled
# PST_MEMORY_CHECK_INTERVAL    (default 50)   — items between RSS checks
# PST_RABBIT_HEARTBEAT_HINT    (default 1800) — informational; ack-timeout
#                                               must be set on RabbitMQ side
# PST_PROGRESS_GROUP_TIMEOUT_S (default 7200) — kill group writers exceeding
#                                               this wall-clock seconds
# ---------------------------------------------------------------------------
_PST_WRITE_SEMAPHORE: asyncio.Semaphore | None = None
_PST_WRITE_SEMAPHORE_LIMIT = int(os.environ.get("PST_WRITE_CONCURRENCY", "1"))

DISK_OVERHEAD_MULTIPLIER = float(os.environ.get("PST_DISK_OVERHEAD_MULTIPLIER", "2.0"))
DISK_FLOOR_BYTES = int(os.environ.get("PST_DISK_FLOOR_BYTES", str(256 * 1024 * 1024)))
MEMORY_LIMIT_MB = int(os.environ.get("PST_MEMORY_LIMIT_MB", "0"))
MEMORY_CHECK_INTERVAL = int(os.environ.get("PST_MEMORY_CHECK_INTERVAL", "50"))
GROUP_TIMEOUT_S = int(os.environ.get("PST_GROUP_TIMEOUT_S", "7200"))
# Concurrency cap per "rate limit scope". Defaults to PER-USER because
# that's the right shape for both single-tenant (5K users → 5K independent
# limits) and multi-tenant SaaS deployments. Override scope via
# ``PST_RATE_LIMIT_SCOPE`` to ``tenant`` for fair-share across customers
# on a shared pool, or ``disabled`` to turn off entirely.
RATE_LIMIT_PER_KEY = int(
    os.environ.get("PST_RATE_LIMIT_PER_KEY",
                   os.environ.get("PST_TENANT_CONCURRENCY", "3"))
)
RATE_LIMIT_SCOPE = os.environ.get("PST_RATE_LIMIT_SCOPE", "user").lower()
CANCEL_CHECK_INTERVAL = int(os.environ.get("PST_CANCEL_CHECK_INTERVAL", "10"))

# Workload prefixes PST export probes (in order) for attachment bytes. Mail
# bytes have shipped under "email"/"mailbox"/"group-mailbox" over time; "archive"
# is required so Online Archive fileAttachments — stored in backup-archive-<tenant>
# by shared/archive_enrichment.py — resolve too. Without it the export never
# searched the archive container, so mail.py got None and wrote a metadata-only
# (broken, ~270-byte) attachment. get_container_name maps each to a real container.
PST_SOURCE_WORKLOADS = ("email", "mailbox", "group-mailbox", "archive")


def _get_write_semaphore() -> asyncio.Semaphore:
    global _PST_WRITE_SEMAPHORE
    if _PST_WRITE_SEMAPHORE is None:
        _PST_WRITE_SEMAPHORE = asyncio.Semaphore(_PST_WRITE_SEMAPHORE_LIMIT)
    return _PST_WRITE_SEMAPHORE


def _check_disk_space(workdir: Path, required_bytes: int) -> tuple[bool, int]:
    """Return (has_space, free_bytes_at_target). Pre-flight guard so we
    fail fast instead of crashing mid-write when /tmp fills up."""
    try:
        check_at = workdir
        while not check_at.exists() and check_at.parent != check_at:
            check_at = check_at.parent
        stat = shutil.disk_usage(str(check_at))
        return (stat.free >= required_bytes, stat.free)
    except Exception as exc:
        logger.warning("disk_usage check failed: %s — assuming sufficient space", exc)
        return (True, 0)


def _current_rss_mb() -> int:
    """Current resident-set-size in MB. Returns 0 on platforms / configs
    where we can't read it (e.g. psutil missing). Used by the memory
    pressure check inside long-running write loops."""
    try:
        import psutil
        return int(psutil.Process().memory_info().rss / (1024 * 1024))
    except Exception:
        try:
            # Fallback: read /proc/self/status (Linux only)
            with open("/proc/self/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        kb = int(line.split()[1])
                        return kb // 1024
        except Exception:
            pass
        return 0


class JobCancelledError(Exception):
    """Raised when an admin/user cancels a running PST job via the
    cancel endpoint. The orchestrator catches this in the outer try
    and reports ``status='cancelled'`` instead of re-raising."""
    pass


class _RateLimit:
    """Async context manager that enforces ``RATE_LIMIT_PER_KEY`` PST
    jobs per key (where the key is a tenant id, user id, or fixed
    string depending on ``PST_RATE_LIMIT_SCOPE``) via a Redis-backed
    counter.

    Falls back to a no-op when Redis is unreachable, when the limit is
    set to 0, or when scope='disabled'. We never block exports because
    of an infrastructure hiccup — the cap is a noisy-neighbor guard,
    not a hard barrier.

    Key shape: ``pst:active:{scope}:{key}`` with TTL 4h so a crashed
    worker doesn't permanently leak a slot.

    Scope guidance:
        - ``user``   (default) — per-user concurrency. Right for both
          single-tenant SaaS (5K users → 5K independent limits) and
          multi-tenant.
        - ``tenant``  — per-tenant fair share. Useful when one tenant
          has many users and you want fair pool sharing.
        - ``disabled`` — no rate limiting.
    """

    def __init__(self, scope_key: str, limit: int, scope: str = "user"):
        self.scope_key = scope_key
        self.limit = limit
        self.scope = scope
        self._key = f"pst:active:{scope}:{scope_key}" if scope_key else None
        self._client = None
        self._held = False

    async def __aenter__(self):
        if self.scope == "disabled" or not self._key or self.limit <= 0:
            return self
        try:
            from shared.config import settings as _settings
            import redis.asyncio as _redis
            url = getattr(_settings, "REDIS_URL", None) or os.environ.get("REDIS_URL")
            if not url:
                return self
            self._client = _redis.from_url(url, decode_responses=True)
            current = int(await self._client.get(self._key) or 0)
            if current >= self.limit:
                logger.warning(
                    "[pst_export] %s=%s at concurrency cap (%d/%d) — "
                    "proceeding anyway (soft limit, raise PST_RATE_LIMIT_PER_KEY to relax)",
                    self.scope, self.scope_key, current, self.limit,
                )
            await self._client.incr(self._key)
            await self._client.expire(self._key, 4 * 3600)
            self._held = True
        except Exception as exc:
            logger.warning("[pst_export] rate limit unavailable: %s", exc)
            self._client = None
        return self

    async def __aexit__(self, *_exc):
        if self._held and self._client is not None:
            try:
                await self._client.decr(self._key)
            except Exception:
                pass
            try:
                await self._client.aclose()
            except Exception:
                pass


# Backwards-compat alias so external callers (tests, docs) referring to
# ``_TenantRateLimit`` keep working.  Prefer ``_RateLimit`` in new code.
_TenantRateLimit = _RateLimit


async def _memory_pressure_check(items_processed: int) -> None:
    """If MEMORY_LIMIT_MB is set and we've crossed it, log + GC + brief
    pause. Soft pressure signal — caller decides whether to continue.
    Called every PST_MEMORY_CHECK_INTERVAL items inside the writer."""
    if MEMORY_LIMIT_MB <= 0:
        return
    if items_processed and items_processed % MEMORY_CHECK_INTERVAL != 0:
        return
    rss = _current_rss_mb()
    if rss == 0:
        return
    if rss > MEMORY_LIMIT_MB:
        import gc
        gc.collect()
        post_rss = _current_rss_mb()
        logger.warning(
            "[pst_export] memory pressure: rss=%dMB > limit=%dMB after_gc=%dMB items=%d",
            rss, MEMORY_LIMIT_MB, post_rss, items_processed,
        )
        # Yield event loop so other tasks can drain queued work
        await asyncio.sleep(0.1)

# Per-type writers — imported at module top so tests can patch
# ``pst_export.MailPstWriter`` etc. They wrap the bundled ``pst_convert``
# CLI; spawning the binary is deferred to write() so importing this
# module doesn't require the binary to be present.
from pst_writers.mail import MailPstWriter
from pst_writers.calendar import CalendarPstWriter
from pst_writers.contact import ContactPstWriter

logger = logging.getLogger(__name__)


def _safe_name(s: str) -> str:
    """Replace non-alphanumeric chars (except ``-`` and ``_``) with ``_``,
    truncate to 64 characters."""
    sanitized = re.sub(r"[^A-Za-z0-9\-_]", "_", s)
    return sanitized[:64]


_ARCNAME_BAD_SEG = {"", ".", ".."}
_ARCNAME_RESERVED = '<>:"/\\|?*'


def safe_arcname(raw_path: str) -> str:
    """Confining ZIP arcname builder for attacker-controlled path components.

    Drops empty / ``.`` / ``..`` segments and reserved (Windows-forbidden)
    chars per component, strips trailing dots/spaces, and normalises
    backslashes so no ``..`` traversal or absolute path can survive.
    """
    if not raw_path:
        return ""
    segments = []
    for seg in raw_path.replace("\\", "/").split("/"):
        if seg.strip() in _ARCNAME_BAD_SEG:
            continue
        cleaned = "".join("_" if c in _ARCNAME_RESERVED else c for c in seg).rstrip(". ")
        if cleaned:
            segments.append(cleaned)
    return "/".join(segments)


def _cleanup_dir(path: Path) -> None:
    """Best-effort recursive delete of ``path``.  Swallows errors; the
    PST workdir lives under ``/tmp`` and we don't want a stray cleanup
    failure to mask an otherwise successful export."""
    try:
        shutil.rmtree(str(path), ignore_errors=True)
    except Exception:  # pragma: no cover - defensive
        pass


@dataclass
class PstGroup:
    key: tuple          # the grouping key
    item_type: str      # "EMAIL" | "CALENDAR_EVENT" | "USER_CONTACT"
    items: list = field(default_factory=list)   # list of SnapshotItem objects
    pst_filename: str = ""                      # e.g. "snapshot123-mail.pst"


def _archive_pst_exportable(item) -> bool:
    """ARCHIVE_ITEM converts to PST only once ENRICHED — the mail writer reads
    the Graph message JSON from ``extra_data.raw`` (via ``fetch_message``). An
    un-enriched archive row carries only the opaque MAPI blob; feeding it to the
    writer would blow up on ``json.loads``. Non-archive items always pass."""
    if getattr(item, "item_type", None) != "ARCHIVE_ITEM":
        return True
    meta = getattr(item, "extra_data", None) or getattr(item, "metadata", None) or {}
    return bool(isinstance(meta, dict) and meta.get("raw"))


class PstGroupPlanner:
    # ARCHIVE_ITEM rides the MAIL writer (enriched archive JSON is a Graph
    # message); its own "archive" suffix keeps the PST distinct from the
    # primary mailbox's "-mail.pst".
    PST_ITEM_TYPES = {"EMAIL", "CALENDAR_EVENT", "USER_CONTACT", "ARCHIVE_ITEM"}
    TYPE_SUFFIX = {
        "EMAIL": "mail",
        "CALENDAR_EVENT": "calendar",
        "USER_CONTACT": "contacts",
        "ARCHIVE_ITEM": "archive",
    }

    def plan(
        self,
        items: list,
        granularity: str,
        include_types: set,
    ) -> list:
        """Group items into PstGroups.

        Parameters
        ----------
        items:
            List of SnapshotItem-like objects.
        granularity:
            ``"MAILBOX"``, ``"FOLDER"``, or ``"ITEM"``.
        include_types:
            Subset of ``PST_ITEM_TYPES`` to include; anything else is silently
            skipped.  Pass the full ``PST_ITEM_TYPES`` set to include all.

        Returns
        -------
        list[PstGroup]
            Sorted by ``(item_type, key)``.

        Raises
        ------
        ValueError
            If *granularity* is unknown, or if ITEM granularity exceeds the
            100 k-group hard limit.
        """
        if granularity not in ("MAILBOX", "FOLDER", "ITEM"):
            raise ValueError(
                f"Unknown PST granularity: {granularity!r}. "
                "Expected MAILBOX, FOLDER, or ITEM."
            )

        effective_types = self.PST_ITEM_TYPES & include_types

        # Filter items first. Un-enriched ARCHIVE_ITEM rows are dropped here —
        # they have no Graph JSON to convert (only the opaque MAPI blob).
        filtered = [
            it for it in items
            if getattr(it, "item_type", None) in effective_types
            and _archive_pst_exportable(it)
        ]

        # ITEM granularity guardrails (checked before building the full dict)
        if granularity == "ITEM":
            count = len(filtered)
            if count > 100_000:
                raise ValueError(
                    f"PST ITEM granularity rejected: {count} items exceeds 100k limit"
                )
            if count > 10_000:
                logger.warning(
                    "PST ITEM granularity: %d groups will create ~%d MB of PST overhead",
                    count,
                    count * 2,
                )

        groups: dict[tuple, PstGroup] = {}

        for it in filtered:
            snap_prefix = str(it.snapshot_id)[:8]
            item_type = it.item_type
            suffix = self.TYPE_SUFFIX[item_type]

            if granularity == "MAILBOX":
                key = (str(it.snapshot_id), item_type)
                if key not in groups:
                    groups[key] = PstGroup(
                        key=key,
                        item_type=item_type,
                        pst_filename=f"{snap_prefix}-{suffix}.pst",
                    )

            elif granularity == "FOLDER":
                folder = it.folder_path or "root"
                key = (str(it.snapshot_id), item_type, folder)
                if key not in groups:
                    groups[key] = PstGroup(
                        key=key,
                        item_type=item_type,
                        pst_filename=f"{snap_prefix}-{_safe_name(folder)}-{suffix}.pst",
                    )

            else:  # ITEM
                key = (str(it.snapshot_id), item_type, it.external_id)
                if key not in groups:
                    groups[key] = PstGroup(
                        key=key,
                        item_type=item_type,
                        pst_filename=f"{snap_prefix}-{it.external_id[:16]}-{suffix}.pst",
                    )

            groups[key].items.append(it)

        result = sorted(groups.values(), key=lambda g: (g.item_type, g.key))
        return result


class PstExportOrchestrator:
    """Entry point for PST export.

    Plans groups, dispatches per-type writers, ZIPs the resulting PST
    files, uploads the archive to blob storage, cleans up the local
    workdir, and reports progress via an optional async callback.
    """

    def __init__(
        self,
        job_id: str,
        items: list,
        spec: dict,
        storage_shard=None,
        dest_container: str = "exports",
        source_container: str = "",
        tenant_id: str = "",
        update_progress=None,
        checkpoint_loader=None,    # async () -> dict | None
        checkpoint_saver=None,     # async (dict) -> None
        item_stream_factory=None,  # callable -> async-iterator of [SnapshotItem] batches
        attachment_index=None,     # dict[str, list[SnapshotItem]] keyed by parent EMAIL.external_id
    ):
        self.job_id = job_id
        self.items = items
        self.spec = spec
        self.storage_shard = storage_shard
        self.dest_container = dest_container or "exports"
        self.source_container = source_container
        self.tenant_id = tenant_id
        self.update_progress = update_progress
        # Resumability hooks. ``checkpoint_loader`` returns the prior
        # checkpoint dict (or None for a fresh run); ``checkpoint_saver``
        # is called after each completed group so a worker crash mid-job
        # can resume by skipping already-finished groups. The schema is:
        #   { "completed_keys": [<group_key_str>, ...],
        #     "pst_blobs": [{path, size, key}, ...] }
        # We persist intermediate PSTs to blob (durable) so they survive
        # the worker restart even though /tmp/<job_id> is wiped.
        self.checkpoint_loader = checkpoint_loader
        self.checkpoint_saver = checkpoint_saver
        self.item_stream_factory = item_stream_factory
        # Attachment index: { parent_email_external_id: [EMAIL_ATTACHMENT SnapshotItem, ...] }
        # Pre-fetched by the caller (one query per export) because the streaming
        # path orders rows by external_id, so attachments arrive AFTER their
        # parent email — by which time the parent may have already flushed if
        # we tried to build the index from the stream itself. Stamped onto
        # each EMAIL item as ``_email_attachment_items`` immediately before
        # the per-group writer.write() call.
        self.attachment_index: dict = attachment_index or {}
        # Optional cancellation hook — async fn that returns True if the
        # job has been cancelled (Job.status='CANCELLED' in DB). Polled
        # every PST_CANCEL_CHECK_INTERVAL items.
        self.cancel_check = None
        # Raw-file item types (OneDrive files, SharePoint files, etc.).
        # Items matching these flow into the final ZIP as plain blob
        # members alongside the PSTs — so a "Download all" with PST
        # format AND OneDrive checked produces ONE zip containing both.
        self.raw_file_types: set = set()
        self.granularity = spec.get("pstGranularity", "MAILBOX").upper()
        self.split_gb = float(spec.get("pstSplitSizeGb", 45))
        self.include_types = set(
            spec.get("pstIncludeTypes", ["EMAIL", "CALENDAR_EVENT", "USER_CONTACT", "ARCHIVE_ITEM"])
        )
        self.workdir = Path(f"/tmp/pst-export/{job_id}")
        # When the user picks MAILBOX granularity but the mailbox holds
        # more items than this, auto-promote to FOLDER. Each per-folder
        # PST stays small + uploads independently → bounded /tmp usage.
        self.auto_folder_threshold = int(
            os.environ.get("PST_AUTO_FOLDER_THRESHOLD", "5000")
        )
        # Optional overrides from caller — used for human-readable PST
        # filenames like ``AmitMishra-mail.pst`` instead of the
        # snapshot-id-prefix default. Caller (export_as_pst) populates
        # these from the resource's display_name + email.
        self.resource_label_by_snapshot: dict[str, str] = {}
        # snapshot_id → resource_id. With sibling-snapshot unioning,
        # MAILBOX/FOLDER group_key uses resource_id so all an item's
        # revisions across snapshots collapse into ONE PST.
        self.snapshot_to_resource: dict[str, str] = {}
        self.resource_label_by_resource: dict[str, str] = {}

    async def _safe_progress(self, pct: int) -> None:
        """Invoke ``update_progress`` swallowing any exception it raises."""
        if self.update_progress:
            try:
                await self.update_progress(pct)
            except Exception as exc:
                logger.warning("progress update failed at %d%%: %s", pct, exc)

    def _resolve_rate_limit_scope_key(self) -> str:
        """Pick the rate-limit key based on PST_RATE_LIMIT_SCOPE.

        ``user``   → resource_id (mailbox/user) when caller populated
                     ``rate_limit_user_key``; falls back to tenant.
        ``tenant`` → tenant_id.
        ``disabled`` → "" (no key, limiter no-ops).
        Anything else falls through to per-user behaviour.
        """
        if RATE_LIMIT_SCOPE == "disabled":
            return ""
        if RATE_LIMIT_SCOPE == "tenant":
            return self.tenant_id or ""
        # user (default) — caller may have set a more specific key
        return getattr(self, "rate_limit_user_key", "") or self.tenant_id or ""

    async def run(self) -> dict:
        """Entry point.  Routes to streaming mode when an
        ``item_stream_factory`` is provided (preferred for production
        durability — bounded memory regardless of mailbox size); falls
        back to the legacy bulk-list mode for callers passing
        ``items=...`` directly (tests, small inline cases)."""
        if self.item_stream_factory is not None:
            return await self._run_streaming()
        return await self._run_bulk()

    async def _run_streaming(self) -> dict:
        """Streaming PST export — fail-proof for 100GB+ mailboxes.

        Pulls items batch-by-batch from ``item_stream_factory`` so the
        worker never holds the full mailbox in RAM. Each group's PST is
        uploaded to blob and the local file deleted as soon as the group
        finishes, so /tmp never accumulates more than one in-flight
        group's worth of bytes. Final step zips the per-group PSTs into
        a single archive (the existing single-zip download contract).
        """
        from collections import defaultdict
        from shared.azure_storage import upload_blob_with_retry_from_file  # noqa: F401  (used by _flush_group)
        # Lazy import metrics module — module-level safe_* helpers no-op
        # when the prometheus_client dep is missing or the HTTP server
        # never came up (init() is called once at worker boot).
        from shared import pst_metrics as _m
        import time as _time
        _job_started_at = _time.monotonic()
        self.workdir.mkdir(parents=True, exist_ok=True)

        # Resolve storage shard if caller didn't pass one in.
        if self.storage_shard is None:
            from shared.azure_storage import azure_storage_manager
            self.storage_shard = azure_storage_manager.get_default_shard()
            if not self.dest_container or self.dest_container == "exports":
                self.dest_container = azure_storage_manager.get_container_name(
                    self.tenant_id, "exports"
                )
            await self.storage_shard.ensure_container(self.dest_container)

        await self._safe_progress(5)

        # Concurrency cap — scope is per-user by default (right for
        # single-tenant SaaS with thousands of users). Operators can flip
        # to per-tenant or disable via PST_RATE_LIMIT_SCOPE.
        scope_key = self._resolve_rate_limit_scope_key()
        rate_limit = _RateLimit(scope_key, RATE_LIMIT_PER_KEY, RATE_LIMIT_SCOPE)
        await rate_limit.__aenter__()

        # Resumability — load prior progress and skip done groups.
        # completed_keys doubles as the LIVE skip-filter on the stream, so
        # only groups whose stream was fully exhausted may ever enter it
        # (see _flush_group). flush_counts tracks how many PST "parts" a
        # group has already flushed — persisted so a crash-resume keeps
        # numbering (and blob keys) unique instead of overwriting prior
        # parts. Resume of a partially-flushed group re-exports the whole
        # group: duplicates across parts, never silent loss.
        completed_keys: set = set()
        prior_blobs: list = []
        flush_counts: dict = defaultdict(int)
        if self.checkpoint_loader is not None:
            try:
                prior = await self.checkpoint_loader()
                if prior:
                    completed_keys = {
                        tuple(k) if isinstance(k, list) else k
                        for k in prior.get("completed_keys", [])
                    }
                    prior_blobs = list(prior.get("pst_blobs", []) or [])
                    for _fp in prior.get("flushed_parts", []) or []:
                        try:
                            _fk, _fn = _fp
                            flush_counts[
                                tuple(_fk) if isinstance(_fk, list) else _fk
                            ] = int(_fn)
                        except Exception:
                            continue
                    logger.info(
                        "[pst_export] resumed: %d groups done, %d PSTs uploaded",
                        len(completed_keys), len(prior_blobs),
                    )
            except Exception as exc:
                logger.warning("[pst_export] checkpoint load failed: %s", exc)

        item_counts: dict = defaultdict(int)
        failed_counts: dict = defaultdict(int)
        # Items the writer deliberately dropped (series occurrences whose
        # master is in scope, dedup hits) — distinct from failed_counts
        # so the audit log doesn't flag intentional skips as data loss.
        skipped_counts: dict = defaultdict(int)
        skipped_groups: list = []
        pst_blob_paths: list = list(prior_blobs)
        # Per-group accumulator. Memory bound = Σ(items per active group).
        accumulated: dict[tuple, list] = defaultdict(list)
        # Map of group_key -> filename + item_type for flushing.
        group_metadata: dict[tuple, dict] = {}

        flush_threshold = int(os.environ.get("PST_GROUP_FLUSH_AT", "1000"))
        total_accumulated_cap = int(os.environ.get("PST_TOTAL_ACCUMULATED_CAP", "5000"))

        writer_map = {
            "EMAIL": MailPstWriter,
            "CALENDAR_EVENT": CalendarPstWriter,
            "USER_CONTACT": ContactPstWriter,
            # Enriched archive items are Graph message JSON → mail writer.
            "ARCHIVE_ITEM": MailPstWriter,
        }

        async def _flush_group(group_key: tuple, final: bool = False) -> None:
            """Write one group's accumulated items to a PST, upload to
            blob, delete the local file, update checkpoint. Memory and
            disk are reclaimed in this single function.

            ``final`` marks the end-of-stream flush. Mid-stream flushes
            (flush threshold / total-accumulated cap) emit a PST *part*
            and leave the group open — more items for it are still
            arriving on the stream."""
            print(
                f"[pst_export.FLUSH_TOP] group_key={group_key} "
                f"accumulated_size={len(accumulated.get(group_key, []))} "
                f"index_size={len(self.attachment_index)}",
                flush=True,
            )
            items_to_write = accumulated.pop(group_key, [])
            if not items_to_write:
                return
            meta = group_metadata.get(group_key)
            if not meta:
                logger.warning("flush_group: no metadata for %s", group_key)
                return
            item_type = meta["item_type"]
            WriterClass = writer_map.get(item_type)
            if WriterClass is None:
                skipped_groups.append({
                    "key": str(group_key), "reason": "unsupported_type"
                })
                failed_counts[item_type] += len(items_to_write)
                return

            # Stamp pre-fetched attachment siblings onto each EMAIL item so
            # MailPstWriter._collect_graph_item can inline their bytes.
            # Other item types are unaffected.
            print(
                f"[pst_export.flush_group] item_type={item_type} "
                f"attachment_index_size={len(self.attachment_index)} "
                f"items_to_write={len(items_to_write)}",
                flush=True,
            )
            if item_type in ("EMAIL", "ARCHIVE_ITEM") and self.attachment_index:
                for it in items_to_write:
                    matched = self.attachment_index.get(it.external_id, [])
                    it._email_attachment_items = matched
                    print(
                        f"[pst_export.flush_group] stamp ext_id={it.external_id[:40]}... "
                        f"matched_atts={len(matched)}",
                        flush=True,
                    )

            # Every flush of the same group is its own PST part. A group
            # larger than the flush threshold flushes multiple times and
            # each part needs a distinct filename — the blob key and the
            # final ZIP member name are both derived from it, so reusing
            # the name would overwrite the earlier part.
            flush_counts[group_key] += 1
            part_no = flush_counts[group_key]
            pst_filename = meta["pst_filename"]
            if part_no > 1:
                stem = (
                    pst_filename[:-4]
                    if pst_filename.endswith(".pst") else pst_filename
                )
                pst_filename = f"{stem}-part{part_no:03d}.pst"

            group = PstGroup(
                key=group_key,
                item_type=item_type,
                items=items_to_write,
                pst_filename=pst_filename,
            )

            import time as _time
            t0 = _time.monotonic()
            try:
                writer = WriterClass()
                async with _get_write_semaphore():
                    write_result = await asyncio.wait_for(
                        writer.write(
                            group=group,
                            workdir=self.workdir,
                            split_gb=self.split_gb,
                            shard=self.storage_shard,
                            source_container=self.source_container,
                        ),
                        timeout=GROUP_TIMEOUT_S,
                    )
                item_counts[item_type] += write_result.item_count
                failed_counts[item_type] += write_result.failed_count
                skipped_counts[item_type] += write_result.skipped_count

                # When a group produced zero PSTs, surface the skip
                # reason so the caller (job-service download endpoint,
                # UI) can show a useful error instead of "no file".
                if (
                    write_result.item_count == 0
                    and not write_result.pst_paths
                ):
                    skipped_groups.append({
                        "key": str(group_key),
                        "item_type": item_type,
                        "reason": "no_collectable_items",
                        "items_attempted": len(items_to_write),
                        "failed": write_result.failed_count,
                        "skipped": write_result.skipped_count,
                        "error": (
                            "All items skipped — usually means calendar "
                            "occurrences whose series-master is not "
                            "backed up, or items missing raw payload."
                        ),
                    })

                # Upload each produced PST to blob immediately, then
                # delete the local file. This bounds /tmp regardless of
                # total mailbox size.
                uploads_ok = True
                for pst_path in write_result.pst_paths:
                    if not pst_path.exists():
                        continue
                    blob_key = f"pst-exports/{self.job_id}/{pst_path.name}"
                    try:
                        size = pst_path.stat().st_size
                        upload_res = await upload_blob_with_retry_from_file(
                            container_name=self.dest_container,
                            blob_path=blob_key,
                            file_path=str(pst_path),
                            shard=self.storage_shard,
                            file_size=size,
                            metadata={
                                "job_id": self.job_id,
                                "type": item_type,
                            },
                        )
                        if upload_res.get("success"):
                            pst_blob_paths.append({
                                "blob_path": blob_key,
                                "filename": pst_path.name,
                                "size": size,
                                "item_type": item_type,
                            })
                        else:
                            uploads_ok = False
                            logger.error(
                                "PST upload failed for %s: %s",
                                pst_path.name, upload_res.get("error"),
                            )
                            skipped_groups.append({
                                "key": str(group_key),
                                "filename": pst_path.name,
                                "reason": "upload_failed",
                                "error": str(upload_res.get("error"))[:200],
                            })
                    finally:
                        # upload_blob_with_retry_from_file already unlinks
                        # on success; this is a defensive belt-and-braces
                        # for the failure path.
                        try:
                            pst_path.unlink(missing_ok=True)
                        except Exception:
                            pass

                # completed_keys doubles as the live skip-filter at the top
                # of the stream loop. A group may only enter it once its
                # stream is exhausted (final flush) AND its PSTs are durably
                # uploaded — marking it on a mid-stream threshold flush
                # silently dropped every remaining item of the group (a
                # 10k-item Inbox truncated to its first 1000 with status
                # 'done'). Failed uploads also stay un-completed so a crash
                # resume retries them instead of skipping the group.
                if final and uploads_ok:
                    completed_keys.add(group_key)
                _elapsed = _time.monotonic() - t0
                logger.info(
                    "[pst_export] group %s flushed: items=%d psts=%d in %.2fs",
                    group_key, write_result.item_count,
                    len(write_result.pst_paths), _elapsed,
                )
                _m.safe_observe(_m.group_duration_seconds, _elapsed, item_type)
                _m.safe_inc(_m.items_total, item_type, amount=write_result.item_count)
                if write_result.failed_count:
                    _m.safe_inc(
                        _m.items_failed_total, item_type, "build_or_insert",
                        amount=write_result.failed_count,
                    )

                # Persist checkpoint after each group. Survives crash.
                if self.checkpoint_saver is not None:
                    try:
                        await self.checkpoint_saver({
                            "completed_keys": [list(k) for k in completed_keys],
                            "pst_blobs": pst_blob_paths,
                            "flushed_parts": [
                                [list(k), n] for k, n in flush_counts.items()
                            ],
                        })
                    except Exception as exc:
                        logger.warning("checkpoint save failed: %s", exc)
            except Exception as exc:
                logger.exception(
                    "[pst_export] flush_group %s FAILED: %s", group_key, exc,
                )
                skipped_groups.append({
                    "key": str(group_key),
                    "items": len(items_to_write),
                    "error": str(exc)[:200],
                })
                failed_counts[item_type] += len(items_to_write)

        def _resource_key_for(item) -> str:
            """Map item → resource_id. With sibling-snapshot unioning,
            the same logical item may surface under different
            snapshot_ids; grouping by snapshot would scatter one user's
            mailbox across multiple PSTs."""
            sid = str(item.snapshot_id)
            return self.snapshot_to_resource.get(sid, sid)

        def _compute_group_key(item) -> tuple:
            it = item.item_type
            rkey = _resource_key_for(item)
            if self.granularity == "MAILBOX":
                return (rkey, it)
            if self.granularity == "FOLDER":
                return (rkey, it, item.folder_path or "root")
            # ITEM granularity stays at external_id — one PST per item
            # is the user's explicit choice.
            return (rkey, it, item.external_id)

        def _register_metadata(group_key: tuple, item) -> None:
            if group_key in group_metadata:
                return
            sid = str(item.snapshot_id)
            sid_short = sid[:8]
            rkey = _resource_key_for(item)
            # Prefer the human-readable label keyed by resource_id —
            # consistent across all sibling snapshots so every PST for
            # the same mailbox/calendar/contacts gets the same prefix.
            label = (
                self.resource_label_by_resource.get(rkey)
                or self.resource_label_by_snapshot.get(sid)
                or self.resource_label_by_snapshot.get(sid_short)
                or sid_short
            )
            label = _safe_name(label)
            it = item.item_type
            suffix = PstGroupPlanner.TYPE_SUFFIX.get(it, "items")
            if self.granularity == "MAILBOX":
                fn = f"{label}-{suffix}.pst"
            elif self.granularity == "FOLDER":
                folder = item.folder_path or "root"
                # Strip leading "Calendar/" or "Contacts/" prefix that
                # backups always include; the suffix already conveys type.
                pretty_folder = folder.lstrip("/").replace("/", "-")
                if pretty_folder.lower().startswith(("calendar-", "contacts-")):
                    pretty_folder = pretty_folder.split("-", 1)[1] or pretty_folder
                fn = f"{label}-{_safe_name(pretty_folder)}-{suffix}.pst"
            else:
                # ITEM granularity — use the item's own name when present,
                # else first 12 chars of external_id.
                base = (
                    _safe_name((item.name or "")[:32])
                    or _safe_name(item.external_id[:12])
                )
                fn = f"{label}-{base}-{suffix}.pst"
            group_metadata[group_key] = {
                "item_type": it,
                "pst_filename": fn,
            }

        # Stream items, accumulate per group, flush at threshold.
        items_seen = 0
        cancelled = False
        # Raw-file members for the final zip — populated when raw_file_types
        # is non-empty (e.g. OneDrive files alongside PSTs in a "Download
        # all" export). Each entry is (arcname, container, blob_path).
        raw_members: list = []
        raw_count = 0
        # Tenant-scoped container resolver (lazy: built on first use).
        _container_cache: dict = {}

        def _resolve_raw_container(item) -> str:
            """Pick the source container for a raw item based on its type."""
            it = item.item_type
            workload = "files"
            if it.startswith("SHAREPOINT"):
                workload = "sharepoint"
            elif it in {"FILE", "ONEDRIVE_FILE", "FILE_VERSION"}:
                workload = "files"
            cached = _container_cache.get(workload)
            if cached is not None:
                return cached
            try:
                from shared.azure_storage import azure_storage_manager
                tid = getattr(self, "tenant_id", "") or ""
                if tid:
                    cached = azure_storage_manager.get_container_name(tid, workload)
                else:
                    cached = workload
            except Exception:
                cached = workload
            _container_cache[workload] = cached
            return cached

        async for batch in self.item_stream_factory():
            for item in batch:
                # Raw-file path: don't go through the PST writer; collect
                # blob coordinates for the final zip stream.
                if item.item_type in self.raw_file_types:
                    blob_path = getattr(item, "blob_path", None)
                    if not blob_path:
                        continue
                    container = _resolve_raw_container(item)
                    name = item.name or item.external_id
                    folder = item.folder_path or "root"
                    workload_dir = (
                        "onedrive" if item.item_type in {"ONEDRIVE_FILE", "FILE", "FILE_VERSION"}
                        else "sharepoint" if item.item_type.startswith("SHAREPOINT")
                        else "files"
                    )
                    arcname = safe_arcname(f"{workload_dir}/{folder}/{name}")
                    assert ".." not in arcname.split("/") and not arcname.startswith("/"), (
                        f"unsafe arcname: {arcname!r}"
                    )
                    raw_members.append((arcname, container, blob_path))
                    raw_count += 1
                    items_seen += 1
                    continue

                if item.item_type not in self.include_types:
                    continue
                # Un-enriched archive rows have no Graph JSON to convert —
                # skip (counted) rather than crash the mail writer on the blob.
                if not _archive_pst_exportable(item):
                    skipped_counts["ARCHIVE_ITEM"] += 1
                    continue
                group_key = _compute_group_key(item)
                if group_key in completed_keys:
                    continue
                _register_metadata(group_key, item)
                accumulated[group_key].append(item)
                items_seen += 1

                # Per-group flush
                if len(accumulated[group_key]) >= flush_threshold:
                    await _flush_group(group_key)
                    await _memory_pressure_check(items_seen)

                # Cancellation poll: cheap DB check every N items, lets
                # an admin stop a runaway whale-mailbox job mid-flight.
                if (self.cancel_check is not None
                        and items_seen % CANCEL_CHECK_INTERVAL == 0):
                    try:
                        if await self.cancel_check():
                            logger.warning(
                                "[pst_export] job=%s cancelled at item=%d",
                                self.job_id, items_seen,
                            )
                            cancelled = True
                            break
                    except Exception as exc:
                        logger.debug("cancel check failed: %s", exc)
            if cancelled:
                break

            # Total-memory cap: if many small folders accumulate, flush
            # the largest group to bound RAM.
            total_acc = sum(len(v) for v in accumulated.values())
            while total_acc > total_accumulated_cap and accumulated:
                largest_key = max(accumulated, key=lambda k: len(accumulated[k]))
                await _flush_group(largest_key)
                await _memory_pressure_check(items_seen)
                total_acc = sum(len(v) for v in accumulated.values())

            # Progress: smear 5%→80% across the stream by item count.
            # We don't know the total upfront, so scale logarithmically.
            if items_seen > 0:
                # Cap at 80% — final 20% is for ZIP + upload.
                pct = min(80, 5 + int(items_seen ** 0.5 / 10))
                await self._safe_progress(pct)

        # Flush remaining accumulated groups. The stream is exhausted, so
        # these flushes are final — groups become resumable-complete here.
        for group_key in list(accumulated.keys()):
            await _flush_group(group_key, final=True)

        await self._safe_progress(85)

        # Final ZIP step: stream per-group PSTs from blob → zipstream-ng
        # → dest blob, never touching /tmp for the archive bytes. Memory
        # ceiling = block_size (~4MB) on Azure or zip-size on Seaweed.
        # Reuses the production-tested helper from mail_export so both
        # backends behave identically here.
        from mail_export import stream_zip_to_block_blob
        zip_blob_path = f"pst-exports/{self.job_id}/{self.job_id}.zip"
        total_size = 0
        pst_filenames: list = []
        members: list = []
        for entry in pst_blob_paths:
            members.append((entry["filename"], self.dest_container, entry["blob_path"]))
            pst_filenames.append(entry["filename"])
            total_size += int(entry.get("size") or 0)
        # Raw-file members go in the same zip — OneDrive/SharePoint files
        # land under their workload subfolder so end-users see a clear
        # tree: `mail.pst`, `calendar.pst`, `onedrive/path/to/file.docx`.
        for raw in raw_members:
            members.append(raw)

        manifest = {
            "job_id": self.job_id,
            "granularity": self.granularity,
            "pst_count": len(pst_filenames),
            "pst_files": [
                {"filename": e["filename"], "size": e.get("size"), "type": e.get("item_type")}
                for e in pst_blob_paths
            ],
            "raw_file_count": raw_count,
            "raw_workloads_included": sorted(self.raw_file_types) if self.raw_file_types else [],
            "item_counts_by_type": dict(item_counts),
            "failed_counts_by_type": dict(failed_counts),
            "skipped_counts_by_type": dict(skipped_counts),
            "skipped_groups": skipped_groups,
            "total_size_bytes": total_size,
        }
        import json as _json
        manifest_bytes = _json.dumps(manifest, indent=2).encode("utf-8")

        block_size = int(os.environ.get("PST_ZIP_BLOCK_SIZE", str(4 * 1024 * 1024)))

        try:
            if not members:
                # Empty selection — nothing to zip, just record the result.
                logger.info("[pst_export] no PSTs produced, skipping final ZIP")
                upload_success = True
            else:
                try:
                    await stream_zip_to_block_blob(
                        dest_shard=self.storage_shard,
                        dest_container=self.dest_container,
                        dest_blob_path=zip_blob_path,
                        members=members,
                        member_source_shard=self.storage_shard,
                        manifest_bytes=manifest_bytes,
                        block_size=block_size,
                    )
                    upload_success = True
                except Exception as zip_exc:
                    logger.exception("[pst_export] streaming ZIP failed: %s", zip_exc)
                    upload_success = False

            await self._safe_progress(95)

            if cancelled:
                status = "cancelled"
            elif not upload_success:
                status = "upload_failed"
            elif skipped_groups:
                status = "done_with_errors"
            elif not pst_filenames:
                status = "no_psts"
            else:
                status = "done"

            # Final job metrics — bucketed by granularity + status so the
            # Grafana dashboard can spot slow whales vs quick small jobs.
            _job_elapsed = _time.monotonic() - _job_started_at
            _m.safe_observe(_m.job_duration_seconds, _job_elapsed,
                            self.granularity, status)
            _m.safe_inc(_m.job_total, status, self.granularity)

            return {
                "job_id": self.job_id,
                "status": status,
                "blob_path": zip_blob_path if upload_success and pst_filenames else None,
                "container": self.dest_container,
                "pst_files": pst_filenames,
                "pst_count": len(pst_filenames),
                "total_size_bytes": total_size,
                "item_counts_by_type": dict(item_counts),
                "failed_counts_by_type": dict(failed_counts),
                "skipped_counts_by_type": dict(skipped_counts),
                "skipped_groups": skipped_groups,
                "granularity": self.granularity,
                "items_processed": items_seen,
            }
        finally:
            await rate_limit.__aexit__(None, None, None)
            _cleanup_dir(self.workdir)
            await self._safe_progress(100)

    async def _run_bulk(self) -> dict:
        """Legacy bulk-list mode — used by tests and small inline calls.

        Plan groups, dispatch writers, ZIP output, upload, return result.
        Steps:
          1. Plan groups via :class:`PstGroupPlanner`.
          2. For each group, dispatch the matching writer; collect PSTs.
          3. ZIP all ``.pst`` files into ``{job_id}.zip`` (STORED — the
             PSTs are already large/dense; deflate adds overhead).
          4. Upload the archive (built-in cleanup deletes the local zip).
          5. Cleanup local workdir.
          6. Return result dict.
        """
        import zipfile as _zipfile
        from shared.azure_storage import upload_blob_with_retry_from_file

        self.workdir.mkdir(parents=True, exist_ok=True)

        # Resolve storage shard if caller didn't pass one in.
        if self.storage_shard is None:
            from shared.azure_storage import azure_storage_manager
            self.storage_shard = azure_storage_manager.get_default_shard()
            if not self.dest_container or self.dest_container == "exports":
                self.dest_container = azure_storage_manager.get_container_name(
                    self.tenant_id, "exports"
                )
            await self.storage_shard.ensure_container(self.dest_container)

        # Step 1: Plan
        await self._safe_progress(5)
        # If the caller didn't pre-fetch the attachment index, build one from
        # any EMAIL_ATTACHMENT items that arrived inline in self.items. The
        # planner filters them out below, so without this they'd be lost.
        if not self.attachment_index:
            local_index: dict = {}
            for _it in self.items:
                if getattr(_it, "item_type", None) != "EMAIL_ATTACHMENT":
                    continue
                _ed = getattr(_it, "extra_data", None) or {}
                _parent = _ed.get("parent_item_id")
                if _parent:
                    local_index.setdefault(_parent, []).append(_it)
            if local_index:
                self.attachment_index = local_index
        planner = PstGroupPlanner()
        groups = planner.plan(self.items, self.granularity, self.include_types)

        # Resumability: load any prior checkpoint and skip already-completed
        # groups. Useful when a worker crashes mid-100GB-mailbox export —
        # restart picks up where the prior attempt left off.
        completed_keys: set = set()
        if self.checkpoint_loader is not None:
            try:
                prior = await self.checkpoint_loader()
                if prior and isinstance(prior.get("completed_keys"), list):
                    completed_keys = {tuple(k) if isinstance(k, list) else k
                                      for k in prior["completed_keys"]}
                    logger.info(
                        "[pst_export] checkpoint loaded — %d groups already done",
                        len(completed_keys),
                    )
            except Exception as exc:
                logger.warning("[pst_export] checkpoint load failed: %s", exc)

        # Disk-space pre-flight. Estimate 2x the inbound content_size:
        # one copy in the .pst, one in the zipped archive (worst case
        # before upload-and-delete reclaims the zip). Refuse the job
        # rather than crash mid-write with disk full.
        estimated_bytes = int(
            sum((getattr(it, "content_size", 0) or 1024) for it in self.items)
            * DISK_OVERHEAD_MULTIPLIER
        )
        # Add safety floor for .NET runtime tempfiles + PST headers.
        estimated_bytes = max(estimated_bytes, DISK_FLOOR_BYTES)
        has_space, free_bytes = _check_disk_space(self.workdir, estimated_bytes)
        if not has_space:
            msg = (
                f"insufficient disk space: estimated need={estimated_bytes/1024**3:.1f}GB, "
                f"free={free_bytes/1024**3:.1f}GB at {self.workdir.parent}"
            )
            logger.error("[pst_export] %s", msg)
            return {
                "job_id": self.job_id,
                "status": "disk_full",
                "blob_path": None,
                "container": self.dest_container,
                "pst_files": [],
                "pst_count": 0,
                "total_size_bytes": 0,
                "item_counts_by_type": {},
                "failed_counts_by_type": {},
                "skipped_counts_by_type": {},
                "skipped_groups": [],
                "granularity": self.granularity,
                "error": msg,
            }
        logger.info(
            "[pst_export] disk pre-check OK: estimated=%.2fGB free=%.2fGB groups=%d",
            estimated_bytes / 1024**3, free_bytes / 1024**3, len(groups),
        )

        # Step 2: Dispatch writers
        writer_map = {
            "EMAIL": MailPstWriter,
            "CALENDAR_EVENT": CalendarPstWriter,
            "USER_CONTACT": ContactPstWriter,
            # Enriched archive items are Graph message JSON → mail writer.
            "ARCHIVE_ITEM": MailPstWriter,
        }
        all_pst_paths: list = []
        item_counts: dict = {}
        failed_counts: dict = {}
        skipped_counts: dict = {}
        total_groups = len(groups)

        skipped_groups: list = []
        import time as _time
        try:
            for idx, group in enumerate(groups):
                if group.key in completed_keys:
                    logger.info(
                        "[pst_export] skipping completed group %d/%d key=%s (resumed)",
                        idx + 1, total_groups, group.key,
                    )
                    continue

                WriterClass = writer_map.get(group.item_type)
                if WriterClass is None:
                    logger.warning(
                        "skipping group with unsupported item_type=%s key=%s",
                        group.item_type, group.key,
                    )
                    skipped_groups.append({"key": str(group.key), "reason": "unsupported_type"})
                    continue

                t0 = _time.monotonic()
                logger.info(
                    "[pst_export] group %d/%d start type=%s items=%d filename=%s",
                    idx + 1, total_groups, group.item_type, len(group.items), group.pst_filename,
                )

                # Stamp pre-fetched attachment siblings onto each EMAIL item;
                # see _run_streaming._flush_group for the matching call site.
                if group.item_type in ("EMAIL", "ARCHIVE_ITEM") and self.attachment_index:
                    for _it in group.items:
                        _it._email_attachment_items = self.attachment_index.get(
                            _it.external_id, []
                        )

                # Per-group exception isolation: a failed group must not poison
                # the whole job. We log, skip, and continue. The job result
                # records skipped groups so the caller can decide on retry.
                # Acquire the per-process write semaphore so concurrent jobs
                # in the same worker process don't oversubscribe pst_convert
                # (each invocation forks a child process + holds the chunk
                # in memory). Wall-clock timeout so a single hung group
                # can't block the whole job indefinitely.
                try:
                    writer = WriterClass()
                    async with _get_write_semaphore():
                        write_result = await asyncio.wait_for(
                            writer.write(
                                group=group,
                                workdir=self.workdir,
                                split_gb=self.split_gb,
                                shard=self.storage_shard,
                                source_container=self.source_container,
                            ),
                            timeout=GROUP_TIMEOUT_S,
                        )
                    # Memory pressure check between groups so a long job
                    # can drain GC pressure before the next group ramps up.
                    await _memory_pressure_check(idx + 1)
                    all_pst_paths.extend(write_result.pst_paths)
                    item_counts[group.item_type] = (
                        item_counts.get(group.item_type, 0) + write_result.item_count
                    )
                    failed_counts[group.item_type] = (
                        failed_counts.get(group.item_type, 0) + write_result.failed_count
                    )
                    skipped_counts[group.item_type] = (
                        skipped_counts.get(group.item_type, 0) + write_result.skipped_count
                    )
                    elapsed = _time.monotonic() - t0
                    logger.info(
                        "[pst_export] group %d/%d done type=%s wrote=%d failed=%d skipped=%d psts=%d in %.2fs",
                        idx + 1, total_groups, group.item_type,
                        write_result.item_count, write_result.failed_count,
                        write_result.skipped_count,
                        len(write_result.pst_paths), elapsed,
                    )
                    completed_keys.add(group.key)
                    if self.checkpoint_saver is not None:
                        try:
                            await self.checkpoint_saver({
                                "completed_keys": [list(k) for k in completed_keys],
                            })
                        except Exception as exc:
                            logger.warning(
                                "[pst_export] checkpoint save failed: %s",
                                exc,
                            )
                except Exception as exc:
                    logger.exception(
                        "[pst_export] group %d/%d FAILED type=%s key=%s items=%d: %s",
                        idx + 1, total_groups, group.item_type,
                        group.key, len(group.items), exc,
                    )
                    skipped_groups.append({
                        "key": str(group.key),
                        "type": group.item_type,
                        "items": len(group.items),
                        "error": str(exc)[:200],
                    })
                    failed_counts[group.item_type] = (
                        failed_counts.get(group.item_type, 0) + len(group.items)
                    )

                # Progress: 5% start → 80% after all groups are written.
                pct = 5 + int(75 * (idx + 1) / max(total_groups, 1))
                await self._safe_progress(pct)

            # Step 3: ZIP all .pst files into a single archive.
            zip_path = self.workdir.parent / f"{self.job_id}.zip"
            total_size = 0
            pst_file_names: list = []
            with _zipfile.ZipFile(str(zip_path), "w", _zipfile.ZIP_STORED) as zf:
                for pst_path in all_pst_paths:
                    if pst_path.exists():
                        _arc = safe_arcname(pst_path.name)
                        assert _arc and ".." not in _arc.split("/") and not _arc.startswith("/"), (
                            f"unsafe arcname: {_arc!r}"
                        )
                        zf.write(str(pst_path), arcname=_arc)
                        total_size += pst_path.stat().st_size
                        pst_file_names.append(pst_path.name)

            # Step 4: Upload zip.  upload_blob_with_retry_from_file deletes
            # the local zip on success.
            blob_path = f"pst-exports/{self.job_id}/{self.job_id}.zip"
            zip_size = zip_path.stat().st_size if zip_path.exists() else 0
            upload_result = await upload_blob_with_retry_from_file(
                container_name=self.dest_container,
                blob_path=blob_path,
                file_path=str(zip_path),
                shard=self.storage_shard,
                file_size=zip_size,
                metadata={"job_id": self.job_id, "granularity": self.granularity},
            )

            if not upload_result.get("success"):
                # upload_blob_with_retry_from_file only auto-deletes on success
                try:
                    zip_path.unlink(missing_ok=True)
                except Exception:
                    pass

            await self._safe_progress(95)

            success = bool(upload_result.get("success"))
            # Status taxonomy:
            #   "done"            — upload OK, all groups OK
            #   "done_with_errors"— upload OK, some groups failed (partial)
            #   "upload_failed"   — PSTs written but blob upload failed
            #   "no_psts"         — no groups produced output (empty selection)
            if not success:
                status = "upload_failed"
            elif skipped_groups:
                status = "done_with_errors"
            elif not pst_file_names:
                status = "no_psts"
            else:
                status = "done"

            return {
                "job_id": self.job_id,
                "status": status,
                "blob_path": blob_path if success else None,
                "container": self.dest_container,
                "pst_files": pst_file_names,
                "pst_count": len(pst_file_names),
                "total_size_bytes": total_size,
                "item_counts_by_type": item_counts,
                "failed_counts_by_type": failed_counts,
                "skipped_counts_by_type": skipped_counts,
                "skipped_groups": skipped_groups,
                "granularity": self.granularity,
            }
        finally:
            # Step 5: Always clean up local workdir (PSTs etc.).
            _cleanup_dir(self.workdir)
            await self._safe_progress(100)
