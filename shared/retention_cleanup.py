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

Data-safety guarantees (the durable model behind the RETENTION_DELETE_ENABLED
freeze — see shared/config.py):
  - P3  every selector keeps the newest RESTORABLE (COMPLETED) snapshot via
        _latest_restorable_id — a failed latest run can never leave a resource
        with nothing to restore.
  - P4a build_retention_audit writes one forensic AuditEvent per deleted
        snapshot, in the SAME transaction as the delete.
  - P4b/P1 _delete_snapshots rehydrates any reuse chain-root into its heir BEFORE
        deleting it (promote heir → then repoint siblings, so the immediate reuse
        trigger never aborts on multi-descendant chains), and carries the doomed
        snapshot's delta cursor forward to the heir.
"""

from __future__ import annotations
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set, Tuple
import logging
import uuid

from sqlalchemy import select, delete, text
from sqlalchemy.ext.asyncio import AsyncSession

from shared.models import (
    AuditEvent, SlaPolicy, Snapshot, Resource, ResourceStatus, SnapshotItem,
    SnapshotStatus, Tenant,
)
from shared.config import settings


def _enum_val(v):
    return v.value if hasattr(v, "value") else (str(v) if v is not None else None)


def build_retention_audit(snapshot, resource, policy, *, reason: str) -> AuditEvent:
    """P4a: one forensic AuditEvent per snapshot retention deletes, so a delete is
    always reconstructable — WHICH snapshot of WHICH resource, when it was created,
    its size, and under which rule. AuditEvent.snapshot_id/resource_id are plain
    UUID columns (not FKs), so the audit row survives the snapshot row it records."""
    started = getattr(snapshot, "started_at", None)
    created = getattr(snapshot, "created_at", None)
    return AuditEvent(
        action="RETENTION_SNAPSHOT_DELETED",
        actor_type="SYSTEM",
        outcome="SUCCESS",
        org_id=getattr(resource, "org_id", None),
        tenant_id=getattr(resource, "tenant_id", None),
        snapshot_id=snapshot.id,
        resource_id=getattr(resource, "id", None),
        resource_type=_enum_val(getattr(resource, "type", None)),
        resource_name=getattr(resource, "display_name", None) or getattr(resource, "external_id", None),
        details={
            "reason": reason,
            "retention_mode": _enum_val(getattr(policy, "retention_mode", None)),
            "policy_id": str(getattr(policy, "id", "")) or None,
            "policy_name": getattr(policy, "name", None),
            "snapshot_type": _enum_val(getattr(snapshot, "type", None)),
            "snapshot_status": _enum_val(getattr(snapshot, "status", None)),
            "item_count": getattr(snapshot, "item_count", None),
            "bytes_total": getattr(snapshot, "bytes_total", None),
            "started_at": started.isoformat() if started else None,
            "created_at": created.isoformat() if created else None,
            "reuse_of_snapshot_id": str(snapshot.reuse_of_snapshot_id) if getattr(snapshot, "reuse_of_snapshot_id", None) else None,
            "reuse_chain_root_id": str(snapshot.reuse_chain_root_id) if getattr(snapshot, "reuse_chain_root_id", None) else None,
        },
    )


logger = logging.getLogger(__name__)


def _snap_status(s) -> Optional[str]:
    st = getattr(s, "status", None)
    return st.value if hasattr(st, "value") else (str(st) if st is not None else None)


def _snap_ts(s) -> datetime:
    return s.started_at or s.created_at or datetime.min


def _latest_restorable_id(snapshots: List[Snapshot]):
    """P3 safety net: the id of the newest RESTORABLE (COMPLETED) snapshot —
    the one retention must NEVER prune so a resource always has something to
    restore. Falls back to newest-by-time only when nothing is COMPLETED at all
    (nothing is restorable anyway; keep the newest so the resource is not
    silently emptied). Returns None for an empty list."""
    if not snapshots:
        return None
    completed = [s for s in snapshots if _snap_status(s) == SnapshotStatus.COMPLETED.value]
    pool = completed or list(snapshots)
    return max(pool, key=_snap_ts).id


def _audit_events_for_deletion(snaps, to_delete, resource, policy) -> List[AuditEvent]:
    """P4a: one RETENTION_SNAPSHOT_DELETED AuditEvent per snapshot in ``snaps``
    whose id is in ``to_delete`` — kept snapshots get none. The caller adds these
    to the session in the SAME transaction as the delete, so the audit trail can
    never disagree with what actually vanished."""
    mode = (getattr(policy, "retention_mode", None) or "FLAT").upper()
    reason = (f"archived-resource retention ({policy.archived_retention_mode})"
              if _is_archived(resource) else f"outside {mode} retention window")
    return [build_retention_audit(s, resource, policy, reason=reason)
            for s in snaps if s.id in to_delete]


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
        # Keep the newest RESTORABLE snapshot, not merely the newest by time.
        return {_latest_restorable_id(snapshots)}
    if mode == "CUSTOM":
        days = policy.archived_retention_days
        if days is None:
            return {s.id for s in snapshots}  # unlimited
        cutoff = datetime.utcnow() - timedelta(days=int(days))
        kept = {s.id for s in snapshots
                if (s.started_at or s.created_at or datetime.utcnow()) >= cutoff}
        # Always keep the newest RESTORABLE snapshot so a resource never goes
        # "empty" (or "un-restorable") silently.
        kept.add(_latest_restorable_id(snapshots))
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
    # P3 safety net: always keep the newest RESTORABLE (COMPLETED) snapshot.
    kept.add(_latest_restorable_id(snapshots))
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
    # P3 safety net: always keep the newest RESTORABLE (COMPLETED) snapshot.
    keep.add(_latest_restorable_id(snapshots))

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

    # 3. Promote the heir to a full snapshot FIRST — BEFORE repointing the
    # siblings. The reuse-validation trigger (BEFORE UPDATE, FOR EACH ROW —
    # immediate, NOT deferred) requires a row's reuse_chain_root_id to point at a
    # FULL snapshot (reuse_of_snapshot_id IS NULL). If we repointed the siblings
    # at the heir while the heir were still a reuse row, that trigger would abort
    # the whole retention transaction — a latent bug that only bites when a doomed
    # chain-root has >=2 surviving descendants (with 0-1 the repoint below matches
    # no rows). Promoting the heir to a full first makes it a valid chain root.
    # Setting both reuse columns to NULL is always allowed by the trigger.
    #
    # P4b (deltaLink handoff): also carry the doomed row's incremental cursor
    # forward to the heir when the heir has none. The delta-token resolver's DB
    # fallback (services/delta-token/main.py) reads the newest snapshot's
    # ``delta_token``; if retention deletes the row holding the active cursor
    # without this, the next incremental would fall back to a staler cursor or
    # re-baseline. COALESCE only fills a gap — a heir with its own (newer) cursor
    # keeps it. The doomed row (``:did``) still exists here (delete happens after
    # this rehydration), so the correlated FROM can read it.
    await session.execute(text("""
        UPDATE snapshots AS h
           SET reuse_of_snapshot_id = NULL,
               reuse_chain_root_id  = NULL,
               delta_token = COALESCE(h.delta_token, d.delta_token),
               delta_tokens_json = CASE
                     WHEN COALESCE(h.delta_tokens_json::jsonb, '{}'::jsonb) = '{}'::jsonb
                         THEN d.delta_tokens_json ELSE h.delta_tokens_json END,
               extra_data = COALESCE(h.extra_data::jsonb, '{}'::jsonb)
                          || jsonb_build_object(
                                'reuse_rehydrated_from', CAST(:did AS TEXT),
                                'reuse_rehydrated_at',   to_char(NOW() AT TIME ZONE 'UTC',
                                                                'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
                             )
          FROM snapshots AS d
         WHERE h.id = CAST(:heir AS UUID) AND d.id = CAST(:did AS UUID)
    """), {"did": str(doomed_id), "heir": str(heir_id)})

    # 4. Now repoint every OTHER still-live descendant at the (now-full) heir.
    # A descendant that pointed reuse_of_snapshot_id straight at the doomed row
    # gets pointed at the heir; every descendant's reuse_chain_root_id moves to
    # the heir. The heir is excluded (it was promoted in step 3). The heir is now
    # a full snapshot, so the chain-root validation passes.
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
            # P4a: write one forensic AuditEvent per doomed snapshot IN THE SAME
            # transaction as the delete — if the delete/rehydration rolls back,
            # so does the audit, so the record never disagrees with reality.
            for _ev in _audit_events_for_deletion(snaps, to_delete, res, policy):
                session.add(_ev)
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
