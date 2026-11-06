"""Content-addressable dedup for blob uploads.

At enterprise scale (5k users × 400 TiB) the same binary content
appears many times across a tenant: one attachment forwarded to 50
mailboxes, the company policy PDF shared across 500 OneDrives, the
same screenshot pasted into 30 chats. Uploading each copy wastes
bandwidth, storage, and the NIC seconds the worker could spend on
new content.

This helper lets callers check whether (tenant, content_hash) is
already backed by a blob somewhere in the DB and, if so, reuse the
existing blob_path instead of uploading. The lookup hits the
existing (tenant_id, content_checksum) index — `idx_snapshot_items_
tenant_checksum` — so it's O(log n) even at hundreds of millions of
rows.

Scoped to same-tenant only so one customer's files never leak into
another customer's blob namespace, even under path-collision edge
cases.

In-process TTL-LRU layer
------------------------
File backups commonly hit the same content_hash repeatedly within a
short window: a chat attachment shared across 50 users, a tenant
templates folder, OneDrive auto-versions of slowly-evolving Office
docs. Each repeat lookup is one round-trip to PG's index. A small
in-memory TTL cache turns the repeat into O(1) memory access.

Defaults tuned for a worker holding ~10k unique hashes in working
set; entries expire after 5 minutes so transient PG state never
goes stale beyond the next backup tick. Per-tenant key scope.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import OrderedDict
from typing import Dict, Iterable, Optional, Tuple
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.models import SnapshotItem

log = logging.getLogger(__name__)


# Postgres caps bind parameters at 65,535, but real-world planners
# choke on IN-lists past ~1K. 500 keeps single-query latency under
# ~50ms even with the tenant_checksum index doing a bitmap scan, and
# lets us fan out 22 M hashes (5K users × ~4400 atts) in ~44K queries
# instead of 22 M one-at-a-time round-trips.
_BULK_CHUNK = int(os.getenv("BLOB_DEDUP_BULK_CHUNK", "500"))


# In-process TTL-LRU cache for find_existing_blob. Keyed on
# (tenant_id, content_checksum) so cross-tenant leakage is impossible.
# A negative result (None) is also cached for a shorter TTL so we
# don't pummel PG for unique content hashes that have no match.
_LRU_MAX_ENTRIES = int(os.getenv("BLOB_DEDUP_LRU_MAX_ENTRIES", "10000"))
_LRU_HIT_TTL_S = int(os.getenv("BLOB_DEDUP_LRU_HIT_TTL_S", "300"))
_LRU_MISS_TTL_S = int(os.getenv("BLOB_DEDUP_LRU_MISS_TTL_S", "30"))

# OrderedDict gives us LRU semantics: move_to_end on hit, popitem(last=False)
# to evict oldest. Wrapped under an asyncio.Lock so concurrent coroutines
# don't trample each other's mutations. Python dicts are technically thread-
# safe for atomic ops but multi-step LRU bookkeeping is not.
_lru_cache: "OrderedDict[Tuple[str, str], Tuple[Optional[str], float]]" = OrderedDict()
_lru_lock = asyncio.Lock()


def _lru_key(tenant_id: UUID, content_checksum: str) -> Tuple[str, str]:
    return (str(tenant_id), content_checksum)


async def _lru_get(tenant_id: UUID, content_checksum: str) -> Tuple[bool, Optional[str]]:
    """Return (hit, value) for the LRU. value may be None (cached miss)."""
    if not content_checksum:
        return False, None
    key = _lru_key(tenant_id, content_checksum)
    async with _lru_lock:
        entry = _lru_cache.get(key)
        if entry is None:
            return False, None
        value, expires_at = entry
        if expires_at < time.monotonic():
            # Expired — purge and report miss
            _lru_cache.pop(key, None)
            return False, None
        # Refresh LRU position
        _lru_cache.move_to_end(key)
        return True, value


async def _lru_put(tenant_id: UUID, content_checksum: str, value: Optional[str]) -> None:
    if not content_checksum:
        return
    key = _lru_key(tenant_id, content_checksum)
    ttl = _LRU_HIT_TTL_S if value else _LRU_MISS_TTL_S
    expires_at = time.monotonic() + ttl
    async with _lru_lock:
        _lru_cache[key] = (value, expires_at)
        _lru_cache.move_to_end(key)
        # Bound memory: evict oldest until under cap. _LRU_MAX_ENTRIES at
        # ~150 bytes/entry = ~1.5 MB worst case per worker, trivial.
        while len(_lru_cache) > _LRU_MAX_ENTRIES:
            _lru_cache.popitem(last=False)


def invalidate_dedup_cache_entry(tenant_id: UUID, content_checksum: str) -> None:
    """Manual eviction hook for callers that just wrote a new dedup-target
    blob. Synchronous because we never need to await; the OrderedDict
    `pop` is atomic enough for this read-after-write."""
    if not content_checksum:
        return
    _lru_cache.pop(_lru_key(tenant_id, content_checksum), None)


async def find_existing_blob(
    session: AsyncSession,
    tenant_id: UUID,
    content_checksum: str,
    min_size_bytes: int = 1024,
) -> Optional[str]:
    """Look up an existing blob_path for this tenant's content hash.

    Returns the blob_path of the first matching row that actually
    has blob-backed bytes, or None if we should proceed with upload.

    ``min_size_bytes`` filters out trivial content where dedup savings
    don't outweigh the DB round-trip cost (tiny icons, 1-pixel GIFs).
    Default 1 KB — anything below that we just upload.

    The hot path is served from a per-process TTL-LRU; cold lookups
    hit PG on the (tenant_id, content_checksum) index. Both hits and
    misses are cached (misses with a shorter TTL).
    """
    if not content_checksum:
        return None
    cached, value = await _lru_get(tenant_id, content_checksum)
    if cached:
        return value
    try:
        stmt = select(SnapshotItem.blob_path).where(
            SnapshotItem.tenant_id == tenant_id,
            SnapshotItem.content_checksum == content_checksum,
            SnapshotItem.blob_path.isnot(None),
            SnapshotItem.content_size >= min_size_bytes,
        ).limit(1)
        result = await session.execute(stmt)
        blob_path = result.scalar_one_or_none()
        await _lru_put(tenant_id, content_checksum, blob_path)
        return blob_path
    except Exception as exc:
        # Dedup is advisory — on DB hiccup we fall through to a
        # fresh upload rather than blocking the backup. Never
        # data-loss, at worst we miss a dedup opportunity.
        log.debug("find_existing_blob failed: %s", exc)
        return None


async def find_unchanged_file_blob(
    session: AsyncSession,
    *,
    resource_id: UUID,
    external_id: str,
    quickxor: str,
    size: int,
    exclude_snapshot_id: UUID,
) -> Optional[Tuple[str, str]]:
    """(blob_path, content_checksum) of this file's copy in the resource's latest COMPLETED or
    PARTIAL backup when its content is unchanged: same drive item, same Graph quickXorHash,
    same size. None means download it.

    The quickXorHash is stored in the item's metadata; content_checksum holds the SHA-256, so a
    quickXorHash lookup against content_checksum (find_existing_blob) never matches and a
    requested full backup re-downloaded and re-stored every file (Railway 2026-11-04, 142 GiB).
    Uses the (snapshot_id, external_id, item_type) index per candidate snapshot.

    SharePoint library rows record Graph's hashes under structured.file_hash, so both shapes
    match. The copy must be on this snapshot's storage backend: a blob path on another
    backend cannot be read back through this one."""
    if not quickxor or not external_id or size is None:
        return None
    from sqlalchemy import and_, func, or_
    from sqlalchemy.orm import aliased
    from shared.models import Snapshot, SnapshotStatus, StorageBackend
    try:
        current = aliased(Snapshot)
        current_backend = select(current.backend_id).where(current.id == exclude_snapshot_id).scalar_subquery()
        # The donor's bytes live on its item's backend, else its snapshot's. Legacy rows with
        # neither are on the active backend only provably when exactly one backend exists.
        donor_backend = func.coalesce(SnapshotItem.backend_id, Snapshot.backend_id)
        single_backend = and_(
            select(func.count()).select_from(StorageBackend).scalar_subquery() == 1,
            current_backend == select(StorageBackend.id).scalar_subquery(),
        )
        stmt = (select(SnapshotItem.blob_path, SnapshotItem.content_checksum)
                .join(Snapshot, Snapshot.id == SnapshotItem.snapshot_id)
                .where(Snapshot.resource_id == resource_id, Snapshot.id != exclude_snapshot_id,
                       Snapshot.status.in_([SnapshotStatus.COMPLETED, SnapshotStatus.PARTIAL]),
                       or_(donor_backend == current_backend,
                           and_(donor_backend.is_(None), single_backend)),
                       SnapshotItem.external_id == external_id, SnapshotItem.blob_path.isnot(None),
                       SnapshotItem.is_deleted.isnot(True),
                       SnapshotItem.content_size == size, SnapshotItem.content_checksum.isnot(None),
                       or_(SnapshotItem.extra_data["quickxor"].as_string() == quickxor,
                           SnapshotItem.extra_data[("structured", "file_hash", "quickXorHash")]
                           .as_string() == quickxor))
                .order_by(Snapshot.created_at.desc()).limit(1))
        row = (await session.execute(stmt)).first()
        return (row[0], row[1]) if row else None
    except Exception as exc:
        # Advisory like find_existing_blob: on a DB hiccup, download instead.
        log.debug("find_unchanged_file_blob failed: %s", exc)
        return None


async def bulk_find_existing_blobs(
    session: AsyncSession,
    tenant_id: UUID,
    content_checksums: Iterable[str],
    min_size_bytes: int = 1024,
) -> Dict[str, str]:
    """Bulk variant of find_existing_blob — one query per ~500 hashes
    instead of one per hash.

    Returns ``{content_checksum: existing_blob_path}`` only for hashes
    that already have a blob-backed row in this tenant. Hashes with no
    match are simply absent from the dict; callers should treat
    ``map.get(hash)`` as "upload if None".

    Scoped to single tenant. Uses the (tenant_id, content_checksum)
    composite index, so it remains O(log N) even at hundreds of
    millions of SnapshotItem rows.

    Dedup is advisory — on DB hiccup we return whatever partial map we
    built and let the caller fall through to fresh uploads. Never
    data-loss; at worst we miss a dedup opportunity on this run.
    """
    out: Dict[str, str] = {}
    unique = {h for h in content_checksums if h}
    if not unique:
        return out

    chunk: list[str] = []
    for h in unique:
        chunk.append(h)
        if len(chunk) >= _BULK_CHUNK:
            await _run_chunk(session, tenant_id, chunk, min_size_bytes, out)
            chunk = []
    if chunk:
        await _run_chunk(session, tenant_id, chunk, min_size_bytes, out)
    return out


async def _run_chunk(
    session: AsyncSession,
    tenant_id: UUID,
    chunk: list[str],
    min_size_bytes: int,
    out: Dict[str, str],
) -> None:
    try:
        stmt = select(
            SnapshotItem.content_checksum, SnapshotItem.blob_path,
        ).where(
            SnapshotItem.tenant_id == tenant_id,
            SnapshotItem.content_checksum.in_(chunk),
            SnapshotItem.blob_path.isnot(None),
            SnapshotItem.content_size >= min_size_bytes,
        )
        result = await session.execute(stmt)
        for h, path in result.all():
            if h and path and h not in out:
                out[h] = path
    except Exception as exc:
        log.debug("bulk_find_existing_blobs chunk failed: %s", exc)
