"""Retention cleanup — delete Snapshots that fall outside their SLA policy's
retention window.

Called daily from backup-scheduler. Safe to run multiple times (idempotent:
already-deleted snapshots are gone from the DB and won't be re-processed).

Retention modes (SlaPolicy.retention_mode):
  FLAT        — keep snapshots newer than retention_days
                (fallback: retention_hot_days + retention_cool_days + retention_archive_days)
  GFS         — keep N most recent daily + N weekly + N monthly + N yearly
  ITEM_LEVEL  — operates per-item (not in this module); snapshots kept indefinitely
                unless an outer FLAT cutoff is also set
  HYBRID      — FLAT for snapshots + item-level pruning inside kept snapshots
                (item-level pass is TODO; for now behaves like FLAT on snapshots)

Legal hold + immutability:
  - legal_hold_enabled + (legal_hold_until is NULL or in the future) → skip pruning
  - immutability_mode == "Locked" → skip pruning (honor WORM)
  - immutability_mode == "Unlocked" → prune normally (user-managed)
"""

from __future__ import annotations
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set, Tuple
import logging
import uuid

from sqlalchemy import select, delete, text
from sqlalchemy.ext.asyncio import AsyncSession

from shared.models import SlaPolicy, Snapshot, Resource, ResourceStatus, SnapshotItem, Tenant
from shared.config import settings


logger = logging.getLogger(__name__)


def _retention_deletes_permitted(delete_enabled: bool, snap_ids) -> bool:
    """Retention kill-switch gate.

    Destructive deletes are permitted ONLY when the global
    RETENTION_DELETE_ENABLED flag is on AND there is something to delete. When
    the flag is off (the safe default), retention runs as a DRY-RUN: it still
    computes what it WOULD delete (for audit/observability) but performs no
    destructive delete. Durable safety default so a mis-tuned selector or an
    un-migrated (sparse) resource can never silently destroy a base full again
    — the bug that deleted a 136 GB base full and orphaned its blobs.
    """
    return bool(delete_enabled and snap_ids)


def _is_archived(resource: Resource) -> bool:
    """A resource is 'archived' once its source (mailbox/user/drive) has been
    removed from M365. Discovery flips Resource.status to ARCHIVED — the
    backups stick around so the operator can still restore them, but the
    SLA policy's archived-resource branch (KEEP_LAST / KEEP_ALL / CUSTOM /
    SAME) decides how aggressively we prune them going forward."""
    s = resource.status
    val = s.value if hasattr(s, "value") else str(s)
    return val == ResourceStatus.ARCHIVED.value


def _archived_keep_ids(snapshots: List[Snapshot], policy: SlaPolicy) -> Set[uuid.UUID]:
    """Apply policy.archived_retention_mode for resources flagged ARCHIVED.

    SAME      → fall back to the policy's normal retention (caller handles).
    KEEP_ALL  → never prune; keep every snapshot.
    KEEP_LAST → keep only the single most recent snapshot.
    CUSTOM    → keep snapshots within `archived_retention_days` (None = unlimited).
    """
    if not snapshots:
        return set()
    mode = (policy.archived_retention_mode or "SAME").upper()
    if mode == "KEEP_ALL":
        return {s.id for s in snapshots}
    if mode == "KEEP_LAST":
        latest = max(snapshots, key=lambda s: s.started_at or s.created_at or datetime.min)
        return {latest.id}
    if mode == "CUSTOM":
        days = policy.archived_retention_days
        if days is None:
            return {s.id for s in snapshots}  # unlimited
        cutoff = datetime.utcnow() - timedelta(days=int(days))
        kept = {s.id for s in snapshots
                if (s.started_at or s.created_at or datetime.utcnow()) >= cutoff}
        # Always keep the most recent so a resource never goes "empty" silently.
        latest = max(snapshots, key=lambda s: s.started_at or s.created_at or datetime.min)
        kept.add(latest.id)
        return kept
    # SAME (default) → caller handles via the normal FLAT/GFS path.
    return None  # type: ignore[return-value]  # sentinel: caller branches


def _is_on_hold(policy: SlaPolicy) -> bool:
    """Legal hold / immutable policy — don't delete anything."""
    if policy.legal_hold_enabled:
        if policy.legal_hold_until is None:
            return True
        if policy.legal_hold_until > datetime.utcnow():
            return True
    if (policy.immutability_mode or "").lower() == "locked":
        return True
    return False


def _flat_keep_ids(snapshots: List[Snapshot], policy: SlaPolicy) -> Set[uuid.UUID]:
    """FLAT: keep snapshots within retention_days (or tiered hot+cool+archive sum)."""
    if not snapshots:
        return set()
    keep_days = policy.retention_days
    if not keep_days:
        keep_days = (policy.retention_hot_days or 0) + (policy.retention_cool_days or 0)
        if policy.retention_archive_days is not None:
            keep_days += policy.retention_archive_days
        else:
            # unlimited archive → keep everything
            return {s.id for s in snapshots}
    if keep_days <= 0:
        return {s.id for s in snapshots}
    cutoff = datetime.utcnow() - timedelta(days=keep_days)
    kept = {s.id for s in snapshots if (s.started_at or s.created_at or datetime.utcnow()) >= cutoff}
    # Always keep the most recent snapshot as a safety net
    latest = max(snapshots, key=lambda s: s.started_at or s.created_at or datetime.min)
    kept.add(latest.id)
    return kept


def _gfs_keep_ids(snapshots: List[Snapshot], policy: SlaPolicy) -> Set[uuid.UUID]:
    """GFS: keep N most-recent daily + N weekly (Sunday) + N monthly (1st) + N yearly (Jan 1).
    A snapshot can count toward multiple buckets; it's kept if *any* bucket claims it."""
    if not snapshots:
        return set()
    n_daily = policy.gfs_daily_count or 0
    n_weekly = policy.gfs_weekly_count or 0
    n_monthly = policy.gfs_monthly_count or 0
    n_yearly = policy.gfs_yearly_count or 0

    sorted_snaps = sorted(
        snapshots,
        key=lambda s: s.started_at or s.created_at or datetime.min,
        reverse=True,
    )

    def _ts(s: Snapshot) -> datetime:
        return s.started_at or s.created_at or datetime.min

    keep: Set[uuid.UUID] = set()
    # Always keep the most recent
    keep.add(sorted_snaps[0].id)

    # Daily: first snapshot per calendar day, cap at n_daily
    seen_days: Dict[str, uuid.UUID] = {}
    for s in sorted_snaps:
        key = _ts(s).strftime("%Y-%m-%d")
        if key not in seen_days:
            seen_days[key] = s.id
            if len(seen_days) >= n_daily:
                break
    keep.update(seen_days.values())

    # Weekly: first snapshot per ISO week
    seen_weeks: Dict[str, uuid.UUID] = {}
    for s in sorted_snaps:
        iso = _ts(s).isocalendar()
        key = f"{iso[0]}-W{iso[1]}"
        if key not in seen_weeks:
            seen_weeks[key] = s.id
            if len(seen_weeks) >= n_weekly:
                break
    keep.update(seen_weeks.values())

    # Monthly: first snapshot per calendar month
    seen_months: Dict[str, uuid.UUID] = {}
    for s in sorted_snaps:
        key = _ts(s).strftime("%Y-%m")
        if key not in seen_months:
            seen_months[key] = s.id
            if len(seen_months) >= n_monthly:
                break
    keep.update(seen_months.values())

    # Yearly: first snapshot per year
    seen_years: Dict[str, uuid.UUID] = {}
    for s in sorted_snaps:
        key = _ts(s).strftime("%Y")
        if key not in seen_years:
            seen_years[key] = s.id
            if len(seen_years) >= n_yearly:
                break
    keep.update(seen_years.values())

    return keep


async def _delete_snapshots(session: AsyncSession, snap_ids: Set[uuid.UUID]) -> int:
    """Delete snapshot rows and their items.

    Blob cleanup is handled by Azure lifecycle policies / SeaweedFS
    GC (applied separately) — we just drop the DB rows here.

    Reuse-chain safety: if any snapshot in ``snap_ids`` is the chain
    root for live descendants, we must FIRST rehydrate the rows into
    the next surviving descendant; otherwise the descendant snapshots
    would resolve to a dead pointer and lose their inventory. The
    rehydration step is atomic with the delete via the surrounding
    session transaction.

    Returns the count of Snapshot rows actually deleted.
    """
    if not snap_ids:
        return 0
    ids = list(snap_ids)
    # Per-id rehydration sweep BEFORE the bulk delete. Most ids will
    # have no descendants (the common case is full snapshots aging
    # out), so the EXISTS probe short-circuits cheaply.
    for sid in ids:
        await _rehydrate_reuse_heir(session, sid, doomed_ids=snap_ids)
    await session.execute(delete(SnapshotItem).where(SnapshotItem.snapshot_id.in_(ids)))
    result = await session.execute(delete(Snapshot).where(Snapshot.id.in_(ids)))
    return result.rowcount or 0


async def _rehydrate_reuse_heir(
    session: AsyncSession,
    doomed_id: uuid.UUID,
    *,
    doomed_ids: Set[uuid.UUID],
) -> Optional[uuid.UUID]:
    """If ``doomed_id`` is the chain root for any descendant that is
    NOT itself doomed, transfer its ``snapshot_items`` rows into the
    earliest surviving descendant ("heir"), repoint every other
    descendant at the heir, and promote the heir to a full snapshot.

    Idempotent and safe under retry: ON CONFLICT DO NOTHING on the
    item copy, and the UPDATE-WHERE-still-points-here pattern means a
    re-run after a partial failure converges.

    Returns the heir's id (when rehydration happened) or None
    (doomed_id had no surviving descendants).
    """
    # 1. Lock the heir candidate set. Use FOR UPDATE so a concurrent
    # backup-worker that's mid-settle and trying to point at this
    # chain blocks until we commit; on commit, its validation trigger
    # will see the new state (doomed row gone OR chain re-rooted) and
    # either succeed or fail loudly. Either way no corruption.
    heir_row = (await session.execute(text("""
        SELECT id
          FROM snapshots
         WHERE reuse_chain_root_id = CAST(:did AS UUID)
           AND id != CAST(:did AS UUID)
           AND id <> ALL(CAST(:doomed AS UUID[]))
           AND status::text = 'COMPLETED'
         ORDER BY COALESCE(started_at, created_at) ASC
         LIMIT 1
         FOR UPDATE
    """), {
        "did": str(doomed_id),
        "doomed": [str(x) for x in doomed_ids],
    })).first()
    if heir_row is None:
        return None
    heir_id = heir_row.id

    # 2. Copy the doomed row's snapshot_items into the heir. ON
    # CONFLICT keeps the copy idempotent under retry: a partial
    # earlier run that wrote some rows + crashed leaves the heir's
    # already-copied rows alone on the next attempt.
    await session.execute(text("""
        INSERT INTO snapshot_items
            (id, snapshot_id, tenant_id, external_id, parent_external_id,
             item_type, name, folder_path, content_hash, content_checksum,
             content_size, blob_path, encryption_key_id, backup_version,
             metadata, is_deleted, indexed_at, backend_id, created_at)
        SELECT gen_random_uuid(),
               CAST(:heir AS UUID),
               tenant_id, external_id, parent_external_id,
               item_type, name, folder_path, content_hash, content_checksum,
               content_size, blob_path, encryption_key_id, backup_version,
               metadata, is_deleted, indexed_at, backend_id, NOW()
          FROM snapshot_items
         WHERE snapshot_id = CAST(:did AS UUID)
        ON CONFLICT DO NOTHING
    """), {"heir": str(heir_id), "did": str(doomed_id)})

    # 3. Repoint every still-live descendant. Any descendant that
    # currently points its reuse_of_snapshot_id straight at the doomed
    # row gets pointed at the heir instead. Every descendant's
    # reuse_chain_root_id moves to the heir. The heir itself is
    # excluded from this UPDATE (it's about to be promoted in step 4).
    await session.execute(text("""
        UPDATE snapshots
           SET reuse_of_snapshot_id = CASE
                   WHEN reuse_of_snapshot_id = CAST(:did AS UUID) THEN CAST(:heir AS UUID)
                   ELSE reuse_of_snapshot_id
               END,
               reuse_chain_root_id = CAST(:heir AS UUID)
         WHERE reuse_chain_root_id = CAST(:did AS UUID)
           AND id != CAST(:heir AS UUID)
    """), {"did": str(doomed_id), "heir": str(heir_id)})

    # 4. Promote the heir to a full snapshot. Validation trigger
    # tolerates both columns going to NULL.
    await session.execute(text("""
        UPDATE snapshots
           SET reuse_of_snapshot_id = NULL,
               reuse_chain_root_id  = NULL,
               extra_data = COALESCE(extra_data::jsonb, '{}'::jsonb)
                          || jsonb_build_object(
                                'reuse_rehydrated_from', CAST(:did AS TEXT),
                                'reuse_rehydrated_at',   to_char(NOW() AT TIME ZONE 'UTC',
                                                                'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
                             )
         WHERE id = CAST(:heir AS UUID)
    """), {"did": str(doomed_id), "heir": str(heir_id)})

    logger.info(
        "[retention] rehydrated reuse heir snapshot=%s ← from doomed=%s",
        heir_id, doomed_id,
    )
    return heir_id


async def enforce_retention_for_tenant(session: AsyncSession, tenant_id: uuid.UUID) -> Dict[str, int]:
    """Walk all resources for a tenant, apply each resource's SLA policy,
    delete snapshots outside retention. Returns per-mode stats.

    Streamed: a 5,000-user M365 tenant has 25,000 resources (5 workloads
    each) and tens of millions of snapshots. Loading the full resource set
    into memory used to balloon Python heap to multi-GB on every cron run
    and stalled the scheduler. Server-side cursor + per-row commit keeps
    peak memory bounded to ~one-resource-worth-of-snapshots at a time.
    """
    stats = {"checked_resources": 0, "held": 0, "deleted_snapshots": 0, "kept_snapshots": 0}

    # Preload all policies for the tenant once — there are O(10) policies
    # per tenant even at scale, so this is cheap and avoids re-querying
    # for every resource.
    pol_rows = (await session.execute(
        select(SlaPolicy).where(SlaPolicy.tenant_id == tenant_id)
    )).scalars().all()
    policies_by_id = {p.id: p for p in pol_rows}
    default_policy = next((p for p in pol_rows if p.is_default), None)

    if not policies_by_id and default_policy is None:
        # Tenant has no policies at all — nothing to enforce. Bail before
        # the resource scan to save cursor work.
        return stats

    # Stream resources via server-side cursor to keep memory bounded.
    res_stream = await session.stream(
        select(Resource)
        .where(Resource.tenant_id == tenant_id)
        .execution_options(yield_per=500)
    )

    # Commit periodically so we don't hold an open transaction across
    # the entire tenant — at 25k resources this would block VACUUM and
    # any concurrent writers for the duration of the sweep.
    COMMIT_EVERY = 100
    since_commit = 0

    async for res in res_stream.scalars():
        stats["checked_resources"] += 1
        policy = policies_by_id.get(res.sla_policy_id) or default_policy
        if policy is None:
            continue
        if _is_on_hold(policy):
            stats["held"] += 1
            continue

        # Snapshots per resource are bounded (a single resource's
        # retention window) — load them in full, but only one resource
        # at a time.
        snaps = (await session.execute(
            select(Snapshot).where(Snapshot.resource_id == res.id)
        )).scalars().all()
        if not snaps:
            continue

        # ARCHIVED resources get a separate branch — operators have an
        # explicit dropdown for what to keep once the source is gone.
        # SAME falls through to the normal FLAT/GFS rule.
        keep = None
        if _is_archived(res):
            keep = _archived_keep_ids(snaps, policy)
        if keep is None:
            mode = (policy.retention_mode or "FLAT").upper()
            if mode == "GFS":
                keep = _gfs_keep_ids(snaps, policy)
            else:
                keep = _flat_keep_ids(snaps, policy)

        to_delete = {s.id for s in snaps} - keep
        if _retention_deletes_permitted(settings.RETENTION_DELETE_ENABLED, to_delete):
            deleted = await _delete_snapshots(session, to_delete)
        else:
            if to_delete:
                logger.warning(
                    "[retention] DRY-RUN (RETENTION_DELETE_ENABLED off): would "
                    "delete %d snapshot(s) for resource %s — skipping (data-loss "
                    "safety freeze until durable model is validated)",
                    len(to_delete), res.id,
                )
                stats["would_delete"] = stats.get("would_delete", 0) + len(to_delete)
            deleted = 0
        stats["deleted_snapshots"] += deleted
        stats["kept_snapshots"] += len(keep)

        since_commit += 1
        if since_commit >= COMMIT_EVERY:
            await session.commit()
            since_commit = 0

    await session.commit()
    return stats


async def enforce_retention_all_tenants(session_factory) -> Dict[str, Dict[str, int]]:
    """Entry point for the scheduler. Runs retention for every tenant."""
    results: Dict[str, Dict[str, int]] = {}
    async with session_factory() as session:
        tenants = (await session.execute(select(Tenant))).scalars().all()
    for t in tenants:
        async with session_factory() as session:
            try:
                results[str(t.id)] = await enforce_retention_for_tenant(session, t.id)
            except Exception as exc:
                results[str(t.id)] = {"error": str(exc)}
    return results
