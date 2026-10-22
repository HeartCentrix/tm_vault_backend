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

Keep-set safety rules (all modes):
  - only a snapshot that actually holds recoverable data may WIN a
    retention bucket, and a fully-restorable row outranks a FAILED partial
    inside the same bucket (see _holds_recoverable_data / _restore_rank)
  - "holds recoverable data" is never decided from the item_count /
    bytes_total counters alone. They are running totals nothing restates
    when the owning process dies (25 prod rows read 0 over 270,885 real
    snapshot_items on 2026-08-11), so the sweep restates them from
    snapshot_items first and the delete transaction re-asserts it
    (see _restate_lying_counters / _delete_snapshots)
  - the delta chain's anchor baseline is kept regardless of age, in EVERY
    mode including the ARCHIVED branch. The anchor is a UNION of plausible
    anchors, not one heuristic — no single rule identified the real
    baseline for all production resources (see _baseline_keep_ids)
  - a row promoted out of a reuse chain owns that chain's whole inventory,
    so rehydration restates its counters and the keep set treats it as
    data-bearing (see _rehydrate_reuse_heir step 3 / _is_rehydrated_heir)
  - a snapshot a worker may still be writing to is never deleted, and the
    guard is re-asserted inside the delete transaction
    (see _is_protected_in_flight / _delete_snapshots)

LATENT (not live today, do not "fix" here): blob expiry is not derived from
this keep set. shared/azure_storage.apply_lifecycle_policy installs a
`{container}_expire` rule that deletes blobs `hot+cool+archive` days after
LAST MODIFICATION, with no knowledge of which snapshots the keep set
protects — so an age-immune baseline anchor or a GFS yearly row would keep
its DB row while its blobs were deleted underneath it (a phantom restore
point). This cannot fire in the current deployment: both callers
(reconcile_lifecycle_policies and sweep_dirty_lifecycle_policies in
services/backup-scheduler) early-return unless the router's ACTIVE backend
kind is `azure_blob`, and the active backend is seaweedfs (Wasabi S3) while
the azure_blob backend is disabled with a placeholder endpoint. Anyone
enabling the azure_blob backend must reconcile that expire rule with this
module's keep set FIRST. The mirror-image gap on the live backend is that
nothing reaps blobs for pruned snapshots at all — leaked storage, which is
the safe direction, but still unowned.
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
    SnapshotStatus, SnapshotType, Tenant,
)
from shared.config import settings


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

# A snapshot in one of these statuses is finished — no worker will write to
# it again, so retention may delete it. PENDING_DELETION is included: that
# status *asks* for the row to go away.
_TERMINAL_STATUSES = frozenset({
    SnapshotStatus.COMPLETED.value,
    SnapshotStatus.FAILED.value,
    SnapshotStatus.PARTIAL.value,
    SnapshotStatus.PENDING_DELETION.value,
})

# ...and in one of these its snapshot_items are restorable content. PARTIAL
# counts: it still holds everything it managed to ingest before giving up.
_RESTORABLE_STATUSES = frozenset({
    SnapshotStatus.COMPLETED.value,
    SnapshotStatus.PARTIAL.value,
})


def _snap_status(s) -> Optional[str]:
    st = getattr(s, "status", None)
    return st.value if hasattr(st, "value") else (str(st) if st is not None else None)


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


def _snap_ts(s: Snapshot) -> datetime:
    return s.started_at or s.created_at or datetime.min


def _enum_val(v) -> Optional[str]:
    if v is None:
        return None
    return v.value if hasattr(v, "value") else str(v)


def _int(v) -> int:
    """Counter columns are nullable — read them as 0 when unset."""
    try:
        return int(v or 0)
    except (TypeError, ValueError):
        return 0


def _holds_persisted_content(s: Snapshot) -> bool:
    """Positive evidence that this row owns real persisted snapshot_items,
    independent of its status.

    The stream-persist handlers write item_count / bytes_total MID-walk
    (workers/backup-worker/main.py ~4374 for mail, ~6929 for chats). If the
    walk then dies, EnumerationFailed flips the row to FAILED (~7826) but
    does NOT delete the items already committed. Such a row is a genuine
    (partial) restore point, so retention must treat these counters as
    proof of content even when the status says the run failed.

    A counter reading zero is NOT evidence of "no content" — it is the
    absence of evidence, because those same mid-walk writes are a running
    total that nothing restates when the owning process dies. Measured on
    prod 2026-08-11: 25 snapshots read item_count=0 while holding 270,885
    real snapshot_items rows. So every caller reaches this function only
    AFTER _restate_lying_counters has replaced a zero counter with a real
    COUNT(*), and _delete_snapshots re-asserts the same truth against the
    database inside the delete transaction. Do not make this function the
    single source of that decision again.
    """
    return _int(getattr(s, "item_count", None)) > 0 or \
        _int(getattr(s, "bytes_total", None)) > 0


# extra_data key _rehydrate_reuse_heir stamps on the row it promotes.
_REHYDRATED_MARKER = "reuse_rehydrated_from"


def _is_rehydrated_heir(s: Snapshot) -> bool:
    """True when this row was promoted out of a reuse chain by
    _rehydrate_reuse_heir — retention deleted the chain root and moved its
    snapshot_items onto this row.

    A promoted heir owns the chain's ENTIRE inventory while its reuse
    pointers are NULL, so the "a reuse row legitimately owns zero items"
    test below cannot see it. Step 4 of the rehydrate now restates
    item_count/bytes_total from snapshot_items, which is the primary
    evidence; this marker covers the rows promoted BEFORE it did. That
    rehydrate has been live since 118f25b and never wrote the counters, so
    production can already hold promoted heirs reading COMPLETED / 0 items
    / NULL reuse pointers — the exact shape this function would otherwise
    call worthless, which would make them the first thing the hardened
    selector prunes.
    """
    return bool((getattr(s, "extra_data", None) or {}).get(_REHYDRATED_MARKER))


def _holds_recoverable_data(s: Snapshot) -> bool:
    """True when a snapshot is a real restore point — i.e. it is allowed to
    WIN a retention bucket.

    Bucket winners used to be "whatever row is newest in the calendar
    period", status-blind. Gold dispatches 3x/day and routinely produces
    rows that hold nothing: FAILED attempts, and SKIP_CONCURRENT
    placeholders (backup-worker writes those COMPLETED with item_count=0 and
    extra_data.skipped_concurrent=true so the FANOUT/FINALIZE counters
    balance). A newer worthless row claimed the day's bucket, pushed the
    day's REAL backup outside the keep set, and it was hard-deleted with its
    snapshot_items.

    Unknown/absent attributes count as data-bearing: for a backup product
    the fail-safe direction is to protect the row, never to prune it.

    Positive evidence of persisted content (item_count/bytes_total > 0) wins
    over a non-restorable status — see _holds_persisted_content. Checking
    status FIRST was a net-new deletion path: a FAILED-mid-walk row that
    holds thousands of real snapshot_items forfeited its GFS bucket, and
    under GFS the buckets ARE the keep set (no age cutoff), so forfeiting a
    bucket means hard deletion. The 0-item placeholders that motivated this
    function are unaffected — they have nothing to show for themselves.
    Data-bearing is only bucket ELIGIBILITY; _restore_rank still ranks a
    fully-restorable row above a FAILED partial inside the same bucket.

    A row promoted out of a reuse chain by _rehydrate_reuse_heir is
    data-bearing too, even when its counters read zero: it owns the whole
    chain's snapshot_items and its reuse pointers are gone. See
    _is_rehydrated_heir.
    """
    if (getattr(s, "extra_data", None) or {}).get("skipped_concurrent"):
        return False
    status = _enum_val(getattr(s, "status", None))
    if status == SnapshotStatus.PENDING_DELETION.value:
        # An explicit deletion request outranks any evidence of content —
        # otherwise the largest-by-item_count anchor resurrects a row an
        # operator asked us to drop, forever.
        return False
    if _holds_persisted_content(s):
        return True
    if _is_rehydrated_heir(s):
        # Rehydration moved a whole chain's snapshot_items onto this row;
        # counters may predate the step-4 restatement. See
        # _is_rehydrated_heir.
        return True
    if status is not None and status not in _RESTORABLE_STATUSES:
        return False
    count = getattr(s, "item_count", None)
    if count is not None and count <= 0:
        # A reuse row legitimately owns zero snapshot_items — its inventory
        # resolves through reuse_chain_root_id — so it IS a restore point.
        return getattr(s, "reuse_chain_root_id", None) is not None
    return True


def _is_restorable(s: Snapshot) -> bool:
    """Status says this row's items are a complete, restorable set."""
    status = _enum_val(getattr(s, "status", None))
    return status is None or status in _RESTORABLE_STATUSES


def _restore_rank(s: Snapshot) -> int:
    """Preference order *within* one retention bucket. Higher wins.

    Every tier here is data-bearing and all of them are kept when they win
    different buckets — this only decides who owns a bucket when several rows
    fall in the same calendar period:
      3 — COMPLETED: the run finished and captured everything it set out to.
      2 — PARTIAL: restorable, but the run was degraded (a shard failed, a
          fan-out was truncated, an enumeration error was recorded).
      1 — holds persisted items but the run did not finish (FAILED mid-walk).
          A real restore point, but the least of the three.

    COMPLETED must strictly outrank PARTIAL, not tie with it. The worker
    honesty gates now settle a degraded run as PARTIAL instead of dishonestly
    COMPLETED, and under Gold (3 dispatches/day) a late PARTIAL would
    otherwise seat itself as the day's bucket winner on recency alone and
    push that day's earlier COMPLETED runs out of the keep set entirely —
    under GFS the buckets ARE the keep set, so forfeiting one means hard
    deletion. Ranking COMPLETED above PARTIAL keeps the best restore point
    for the period, which is the whole purpose of the keep set.
    """
    if not _is_restorable(s):
        return 1
    return 3 if _enum_val(getattr(s, "status", None)) == SnapshotStatus.COMPLETED.value else 2


def _bucket_candidates(snapshots: List[Snapshot]) -> List[Snapshot]:
    """The snapshots eligible to win a bucket.

    Falls back to the full list when none are data-bearing — a genuinely
    empty mailbox/drive produces real COMPLETED 0-item snapshots and that is
    the true state of the resource, so its keep set must not collapse to
    nothing just because there is no data to point at.
    """
    real = [s for s in snapshots if _holds_recoverable_data(s)]
    return real or list(snapshots)


def _latest_ids(snapshots: List[Snapshot]) -> Set[uuid.UUID]:
    """The "never let a resource go empty" safety net: the newest row, the
    newest row that holds recoverable data, the newest restorable row,
    and the newest COMPLETED row. These normally coincide; retaining the
    last COMPLETED row also preserves the local safety guarantee when a
    newer PARTIAL row exists."""
    candidates = _bucket_candidates(snapshots)
    restorable = [s for s in candidates if _is_restorable(s)] or candidates
    return {
        max(snapshots, key=_snap_ts).id,
        max(candidates, key=_snap_ts).id,
        max(restorable, key=_snap_ts).id,
        _latest_restorable_id(snapshots),
    }


def _pick_bucket_winners(sorted_snaps: List[Snapshot], key_of, limit: int) -> Set[uuid.UUID]:
    """Claim up to `limit` calendar buckets from `sorted_snaps` (newest
    first) and return the winning ids.

    One bucket per distinct `key_of(snapshot)`. Within a bucket the newest
    row wins, EXCEPT that a fully-restorable row outranks a FAILED partial
    (see _restore_rank) — the whole point of the keep set is to hold the
    best restore point for each period, not merely the last row written in
    it. Rows are date-sorted so all members of a bucket are contiguous:
    once `limit` buckets are open, the first row of a new bucket ends the
    scan.
    """
    if limit <= 0:
        return set()
    winners: Dict[str, Snapshot] = {}
    for s in sorted_snaps:
        key = key_of(s)
        current = winners.get(key)
        if current is None:
            if len(winners) >= limit:
                break
            winners[key] = s
        elif _restore_rank(s) > _restore_rank(current):
            winners[key] = s
    return {s.id for s in winners.values()}


def _baseline_keep_ids(snapshots: List[Snapshot]) -> Set[uuid.UUID]:
    """The anchor baseline every later delta resolves against — kept
    regardless of age.

    THE RULE: protect the UNION of the plausible anchors, never a single
    guess. For a data-bearing row set that is
      - the newest snapshot typed FULL, when any exists,
      - the LARGEST snapshot by item_count,
      - the LARGEST snapshot by bytes_total,
      - the OLDEST snapshot.
    Ties break to the older row (the chain root, not a later clone).

    Why a union and not one heuristic:
      - `type` cannot be trusted alone: production has zero rows typed FULL
        (create_snapshot defaults INCREMENTAL and no caller overrides it),
        so a type==FULL test on its own is a no-op and left four OneDrive
        baselines to age out of the daily window.
      - "oldest data-bearing" is wrong for a resource whose history starts
        with a tiny probe/partial run. Verified against production
        2026-07-31: it identifies the real baseline for 4 of 6 SLA'd
        OneDrive resources but picks a 5-item/17 MB stub over an
        8,239-item/124 GB baseline for one user, and a 2-item/31 MB stub
        over a 198,992-item/199 GB baseline for another.
      - "largest by item_count" identified all six correctly — a full walk
        inventories every item, an incremental only the changed ones — but
        it degrades when item_count was never written, hence the bytes_total
        companion, and it is still a heuristic, hence the union.

    Cost of the union is at most three extra rows per resource kept
    indefinitely; the cost of guessing wrong is a hard-deleted 124 GB
    baseline that strands every incremental resolving against it. For a
    backup product that trade is not close.

    The data-bearing filter on every branch stops a FAILED-empty row or a
    SKIP_CONCURRENT placeholder from being mistaken for the baseline and
    shadowing the real one out of protection.
    """
    # Oldest-first so max() — which returns the FIRST maximal element —
    # breaks every tie toward the older row.
    real = sorted(
        (s for s in snapshots if _holds_recoverable_data(s)),
        key=_snap_ts,
    )
    if not real:
        return set()
    keep = {
        real[0].id,                                              # oldest
        max(real, key=lambda s: _int(getattr(s, "item_count", None))).id,
        max(real, key=lambda s: _int(getattr(s, "bytes_total", None))).id,
    }
    # The declared FULL anchor is one member of the union — single source of
    # truth with the narrow contract _with_baseline applies.
    return keep | _full_anchor_ids(snapshots)


def _is_protected_in_flight(s: Snapshot) -> bool:
    """True when a worker may still be writing to this snapshot, in which
    case retention must never hard-delete it.

    Three independent guards:
      - non-terminal status — a multi-hour walk is still inserting
        snapshot_items; deleting the parent FK-fails those inserts and
        orphans blobs already uploaded (observed 2026-07-29). Rows abandoned
        by a dead worker are the reconciler's job to flip terminal, not
        retention's to delete.
      - live lease — lease_expires_at in the future means some worker
        (backup walk, finalizer, reconciler) still holds the row even if its
        status already reads terminal.
      - hc_drain_status == PENDING — with
        USER_CHATS_HC_BARRIER_DETACHED=true (prod default) the chats handler
        returns as soon as message bodies are persisted and the
        hostedContent drain keeps inserting items in the background. That
        row's status is already COMPLETED and its lease may already be
        released, so neither guard above covers it; restore paths gate on
        the same column (shared/models.snapshot_is_restore_ready).

    A live shard set (snapshot_partitions in QUEUED/CLAIMED/IN_PROGRESS) is
    the fourth signal the stale-sweeper uses, but it needs a query rather
    than a column on this row — it is enforced in _delete_snapshots, where
    it is both cheap and atomic with the delete.
    """
    status = _enum_val(getattr(s, "status", None))
    if status is not None and status not in _TERMINAL_STATUSES:
        return True
    if (getattr(s, "hc_drain_status", None) or "NOT_APPLICABLE") == "PENDING":
        return True
    expires = getattr(s, "lease_expires_at", None)
    if expires is not None:
        # lease_expires_at is DateTime(timezone=True) while started_at is
        # naive — compare against the matching flavour of "now" so a naive
        # value can't raise offset-naive vs offset-aware TypeError.
        now = datetime.now(timezone.utc) if expires.tzinfo else datetime.utcnow()
        if expires > now:
            return True
    return False


def _full_anchor_ids(snapshots: List[Snapshot]) -> Set[uuid.UUID]:
    """The newest data-bearing snapshot typed FULL, if any.

    This is the narrow 2026-07-30 contract: it is unioned into EVERY
    keep-selector (via _with_baseline for FLAT/GFS, and as one member of
    _baseline_keep_ids for the active and ARCHIVED paths) so no retention
    mode can delete a declared baseline. On its own it is a no-op in
    production — nothing there is typed FULL — which is why every path also
    carries the broadened anchors; see _baseline_keep_ids.
    """
    real = [s for s in snapshots if _holds_recoverable_data(s)]
    fulls = [
        s for s in real
        if _enum_val(getattr(s, "type", None)) == SnapshotType.FULL.value
    ]
    return {max(fulls, key=_snap_ts).id} if fulls else set()


def _with_baseline(keep, snapshots: List[Snapshot]):
    """Union the declared (FULL-typed) baseline into a selector's keep set.

    Keep-MORE only: it can add ids, never remove them. `None` is the
    _archived_keep_ids SAME sentinel meaning "fall back to normal retention",
    and must pass through untouched.

    This is the NARROW anchor. Callers of _flat_keep_ids / _gfs_keep_ids get
    the BROADENED anchors (largest / oldest data-bearing) unioned on top by
    enforce_retention_for_tenant; the archived path applies them itself in
    _archived_keep_ids because it returns before reaching that line.
    """
    if keep is None:
        return None
    return keep | _full_anchor_ids(snapshots)


def _archived_keep_ids(snapshots: List[Snapshot], policy: SlaPolicy) -> Set[uuid.UUID]:
    """Archived-resource keep set, plus the anchor its deltas resolve against.

    Re-decided 2026-07-31. This path used to union only the declared
    (FULL-typed) baseline, which is a provable no-op in production: every
    snapshot there is typed INCREMENTAL, so _full_anchor_ids is always
    empty and KEEP_LAST/CUSTOM hard-deleted the de-facto baseline (the
    largest / oldest data-bearing row — jrue 8,239 items/124 GB, jlucas
    198,992 items/199 GB) while retaining incrementals that cannot be
    restored without it.

    The earlier argument was that an operator who picked KEEP_LAST made an
    explicit choice and we should honour it exactly. But the choice was
    "keep the last backup", not "keep an unrestorable delta" — and ARCHIVED
    means the source is GONE from M365, so the vault holds the last
    remaining copy of that data. Losing a chain root is worse here than
    anywhere else, and the cost of being wrong the other way is at most
    three extra rows per resource.

    Still bounded: the anchors are the same data-aware union the active path
    uses, so a 0-item placeholder or an empty FAILED row can never be
    mistaken for the baseline, and KEEP_LAST does not become KEEP_ALL.
    The SAME sentinel (None) still passes through untouched.
    """
    keep = _archived_keep_ids_inner(snapshots, policy)
    if keep is None:
        return None
    return keep | _baseline_keep_ids(snapshots)


def _archived_keep_ids_inner(snapshots: List[Snapshot], policy: SlaPolicy) -> Set[uuid.UUID]:
    """Apply policy.archived_retention_mode for resources flagged ARCHIVED.

    SAME      → fall back to the policy's normal retention (caller handles).
    KEEP_ALL  → never prune; keep every snapshot.
    KEEP_LAST → keep the most recent snapshot. Up to three rows in the edge
                case where the newest row, the newest data-bearing row and
                the newest fully-restorable row differ (see _latest_ids) —
                keeping the newest placeholder alone would hand the operator
                an empty restore point.
    CUSTOM    → keep snapshots within `archived_retention_days` (None =
                unlimited), plus the same _latest_ids safety net.
    """
    if not snapshots:
        return set()
    mode = (policy.archived_retention_mode or "SAME").upper()
    if mode == "KEEP_ALL":
        return {s.id for s in snapshots}
    if mode == "KEEP_LAST":
        return _latest_ids(snapshots)
    if mode == "CUSTOM":
        days = policy.archived_retention_days
        if days is None:
            return {s.id for s in snapshots}  # unlimited
        cutoff = datetime.utcnow() - timedelta(days=int(days))
        kept = {s.id for s in snapshots
                if (s.started_at or s.created_at or datetime.utcnow()) >= cutoff}
        # Always keep the most recent so a resource never goes "empty" silently.
        kept |= _latest_ids(snapshots)
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
    return _with_baseline(_flat_keep_ids_inner(snapshots, policy), snapshots)


def _flat_keep_ids_inner(snapshots: List[Snapshot], policy: SlaPolicy) -> Set[uuid.UUID]:
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
    kept |= _latest_ids(snapshots)
    return kept


def _gfs_keep_ids(snapshots: List[Snapshot], policy: SlaPolicy) -> Set[uuid.UUID]:
    return _with_baseline(_gfs_keep_ids_inner(snapshots, policy), snapshots)


def _gfs_keep_ids_inner(snapshots: List[Snapshot], policy: SlaPolicy) -> Set[uuid.UUID]:
    """GFS: keep N most-recent daily + N weekly (Sunday) + N monthly (1st) + N yearly (Jan 1).
    A snapshot can count toward multiple buckets; it's kept if *any* bucket claims it.

    Only data-bearing snapshots compete for the buckets, so a 0-item FAILED
    row or a SKIP_CONCURRENT placeholder can never claim a calendar period and
    displace the real backup taken in that period. A FAILED row that DOES hold
    persisted items competes (it is a real, if lesser, restore point) but
    still loses its bucket to a fully-restorable row from the same period."""
    if not snapshots:
        return set()
    n_daily = policy.gfs_daily_count or 0
    n_weekly = policy.gfs_weekly_count or 0
    n_monthly = policy.gfs_monthly_count or 0
    n_yearly = policy.gfs_yearly_count or 0

    sorted_snaps = sorted(
        _bucket_candidates(snapshots),
        key=lambda s: s.started_at or s.created_at or datetime.min,
        reverse=True,
    )

    def _ts(s: Snapshot) -> datetime:
        return s.started_at or s.created_at or datetime.min

    def _iso_week(s: Snapshot) -> str:
        iso = _ts(s).isocalendar()
        return f"{iso[0]}-W{iso[1]}"

    keep: Set[uuid.UUID] = set()
    # Always keep the most recent
    keep |= _latest_ids(snapshots)

    # Daily: best snapshot per calendar day, cap at n_daily
    keep |= _pick_bucket_winners(sorted_snaps, lambda s: _ts(s).strftime("%Y-%m-%d"), n_daily)
    # Weekly: best snapshot per ISO week
    keep |= _pick_bucket_winners(sorted_snaps, _iso_week, n_weekly)
    # Monthly: best snapshot per calendar month
    keep |= _pick_bucket_winners(sorted_snaps, lambda s: _ts(s).strftime("%Y-%m"), n_monthly)
    # Yearly: best snapshot per year
    keep |= _pick_bucket_winners(sorted_snaps, lambda s: _ts(s).strftime("%Y"), n_yearly)

    return keep


async def _restate_lying_counters(
    session: AsyncSession,
    snapshots: List[Snapshot],
) -> int:
    """Replace zeroed item_count / bytes_total with the truth from
    snapshot_items, for the rows about to be judged by the keep set.

    THE BUG this closes (measured on prod 2026-08-11): item_count and
    bytes_total are written MID-walk by the stream-persist handlers as a
    per-process running total and are only restated from a real COUNT(*) on
    the success path. 25 snapshots read item_count=0 while holding 270,885
    real snapshot_items rows — 9 FAILED ROOM_MAILBOX runs (189,692 rows)
    and 16 COMPLETED USER_CHATS runs (81,193 rows) — none of them
    partitioned and every one with reuse_chain_root_id NULL, so no
    legitimate "this row owns zero items" explanation applies. Those
    counters are the whole basis of _holds_persisted_content, so a lying 0
    made every one of those rows look worthless: it could win no GFS bucket
    (_bucket_candidates), was invisible to the anchor rule
    (_baseline_keep_ids), and FAILED is not in _RESTORABLE_STATUSES — so it
    forfeited its bucket and _delete_snapshots hard-deleted the snapshot
    together with its items. Silent, irreversible data loss driven by a
    counter, not by data.

    Fail-safe direction: this only ever raises a counter (the query is
    scoped to rows whose counters read zero) so it cannot make a row look
    LESS data-bearing than the handler claimed, and it cannot revive a
    0-item placeholder — a SKIP_CONCURRENT row or a genuinely empty mailbox
    has no snapshot_items rows to find, so it stays exactly as worthless as
    before and the placeholder cleanup this module was written for keeps
    working.

    The correction is written back (these are live ORM rows, so the next
    flush persists them), which is also how the 25 rows already in prod get
    healed: the reconciler's settle statements can only fix rows they are
    settling, and all 25 are long since terminal. Restricted to rows that
    are NOT in flight so retention can never race a worker's own counter
    writes.

    PENDING_DELETION rows ARE restated even though they are about to be
    deleted on purpose. It looks like a write for nothing and is not: the
    _delete_snapshots guard refuses to delete a row whose counters read
    empty while snapshot_items exist, so leaving a purge-requested row's
    counters lying would defer that purge on every pass, forever. Restating
    cannot resurrect it either — _holds_recoverable_data rejects
    PENDING_DELETION before it ever consults the counters (see
    test_pending_deletion_row_is_never_resurrected_by_the_keep_set).

    Cost: ONE grouped query per resource, scoped to the suspect ids
    (``snapshot_id = ANY``), served by the snapshot_id-leading btree
    idx_snapshot_items_snapshot_type — verified present in prod
    2026-08-11. Resources with honest counters issue no query at all.

    Returns the number of rows corrected.
    """
    suspects = [
        s for s in snapshots
        if getattr(s, "id", None) is not None
        and not _holds_persisted_content(s)
        and not _is_protected_in_flight(s)
    ]
    if not suspects:
        return 0
    rows = (await session.execute(text("""
        SELECT si.snapshot_id,
               COUNT(*)                          AS n_items,
               COALESCE(SUM(si.content_size), 0) AS n_bytes
          FROM snapshot_items si
         WHERE si.snapshot_id = ANY(CAST(:ids AS UUID[]))
         GROUP BY si.snapshot_id
    """), {"ids": [str(s.id) for s in suspects]})).fetchall()
    truth = {str(r[0]): (_int(r[1]), _int(r[2])) for r in rows}
    corrected = 0
    for s in suspects:
        real = truth.get(str(s.id))
        if not real or real[0] <= 0:
            continue
        s.item_count, s.bytes_total = real
        corrected += 1
        # Loud: every one of these was a snapshot retention was about to
        # judge as holding nothing while it held real recoverable items.
        logger.warning(
            "[retention] snapshot=%s counters lied (item_count=0) while "
            "holding %d snapshot_items / %d bytes — restated before the "
            "keep set was computed",
            s.id, real[0], real[1],
        )
    return corrected


async def _delete_snapshots(
    session: AsyncSession,
    snap_ids: Set[uuid.UUID],
    *,
    audit_events: Optional[Dict[uuid.UUID, AuditEvent]] = None,
) -> int:
    """Delete snapshot rows and their items.

    Blob cleanup is handled by Azure lifecycle policies / SeaweedFS
    GC (applied separately) — we just drop the DB rows here. NOTE that
    on the SeaweedFS/Wasabi backend that is live today neither of those
    actually reaps: see the LATENT note in this module's docstring.

    In-flight guard, re-asserted here: the caller's guard is read-time
    only, and the resource's snapshots were SELECTed potentially many
    seconds earlier (the sweep commits every 100 resources). A worker
    can claim or re-open one of those rows in between — that is the
    2026-07-29 race. So the doomed set is re-qualified inside this
    transaction, FOR UPDATE, against the same predicates
    _is_protected_in_flight applies plus the live-shard check the
    stale-sweeper uses (services/backup-scheduler/main.py). Rows that
    went live are dropped from the batch and logged; retention will
    revisit them on the next daily pass.

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
    rows = (await session.execute(text("""
        SELECT id
          FROM snapshots
         WHERE id = ANY(CAST(:ids AS UUID[]))
           AND status::text = ANY(CAST(:terminal AS TEXT[]))
           AND (lease_expires_at IS NULL OR lease_expires_at <= NOW())
           AND COALESCE(hc_drain_status, 'NOT_APPLICABLE') <> 'PENDING'
           AND NOT EXISTS (
                 SELECT 1
                   FROM snapshot_partitions sp
                  WHERE sp.snapshot_id = snapshots.id
                    AND sp.status IN ('QUEUED', 'CLAIMED', 'IN_PROGRESS')
               )
           -- Counter-lie backstop (2026-08-11). The keep set decides
           -- "this row holds nothing" from item_count / bytes_total alone
           -- (_holds_persisted_content), and prod proved those counters
           -- lie: 25 snapshots read item_count=0 while holding 270,885
           -- real snapshot_items rows. enforce_retention_for_tenant now
           -- restates them before computing the keep set
           -- (_restate_lying_counters), and this is the assertion that
           -- makes "deleted real data because a counter lied" impossible
           -- rather than merely unlikely — it re-checks the claim against
           -- the actual rows, inside the delete transaction, for any
           -- caller (present or future) that reaches this function.
           --
           -- The predicate mirrors _holds_persisted_content exactly: only
           -- a row that showed NO counter evidence of content is
           -- second-guessed, so a row legitimately aging out with honest
           -- counters is unaffected and the 0-item placeholder cleanup
           -- (nothing in snapshot_items to find) still deletes. Cost is
           -- one index probe per doomed id on the snapshot_id-leading
           -- btree, and only for ids whose counters read zero.
           AND NOT (
                 COALESCE(snapshots.item_count, 0) = 0
                 AND COALESCE(snapshots.bytes_total, 0) = 0
                 AND EXISTS (
                       SELECT 1
                         FROM snapshot_items si
                        WHERE si.snapshot_id = snapshots.id
                     )
               )
         ORDER BY id
         FOR UPDATE
    """), {
        "ids": [str(x) for x in snap_ids],
        "terminal": sorted(_TERMINAL_STATUSES),
    })).fetchall()
    ids = [r[0] for r in rows]
    deferred = len(snap_ids) - len(ids)
    if deferred:
        # Loud: this is a real observed race, not routine noise.
        logger.warning(
            "[retention] delete-time guard deferred %d of %d snapshot(s) — "
            "no longer deletable (claimed/leased/HC-draining/live shards, "
            "counters that read empty while snapshot_items exist, or "
            "already gone) since the keep-set read; re-evaluated next pass",
            deferred, len(snap_ids),
        )
    if not ids:
        return 0
    doomed = set(ids)
    # Per-id rehydration sweep BEFORE the bulk delete. Most ids will
    # have no descendants (the common case is full snapshots aging
    # out), so the EXISTS probe short-circuits cheaply.
    for sid in ids:
        await _rehydrate_reuse_heir(session, sid, doomed_ids=doomed)
    # Both deletes are scoped to the SAME re-qualified set: deleting the
    # items of a row we then decline to delete would leave a surviving
    # snapshot with no inventory — worse than either outcome.
    await session.execute(delete(SnapshotItem).where(SnapshotItem.snapshot_id.in_(ids)))
    result = await session.execute(delete(Snapshot).where(Snapshot.id.in_(ids)))
    # Only rows that passed the locked guard were actually deleted. Recording
    # the original candidate set would falsely audit leased/deferred snapshots.
    for sid in ids:
        if audit_events and sid in audit_events:
            session.add(audit_events[sid])
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

    The promotion restates the heir's item_count / bytes_total from the
    inventory it just inherited, so the keep set recognises it as
    data-bearing on the next pass (step 3).

    The heir MUST be COMPLETED: step 4 repoints descendants'
    ``reuse_of_snapshot_id`` at it, and validate_snapshot_reuse_target()
    (shared/database.py) rejects a parent that is not COMPLETED. When no
    surviving descendant is COMPLETED this returns None and the caller's
    DELETE is refused by the reuse self-FK's ON DELETE RESTRICT — loud, and
    the data stays put.

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
               item_count = (SELECT COUNT(*)
                               FROM snapshot_items
                              WHERE snapshot_id = CAST(:heir AS UUID)),
               bytes_total = (SELECT COALESCE(SUM(content_size), 0)
                                FROM snapshot_items
                               WHERE snapshot_id = CAST(:heir AS UUID)),
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


# Resources loaded per keyset page in enforce_retention_for_tenant — the
# same bound the old yield_per=500 server-side cursor gave. Read at call
# time (not bound as a default) so a test can shrink it.
RESOURCE_PAGE_SIZE = 500


async def _iter_tenant_resources(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    page_size: Optional[int] = None,
):
    """Yield every Resource of ``tenant_id`` in primary-key order, one
    keyset page at a time, with NO server-side cursor.

    THE BUG this closes (prod, every daily run): the sweep streamed
    ``select(Resource)`` through ``session.stream(...yield_per=500)`` and
    committed every COMMIT_EVERY resources inside that loop. The pooler
    runs in transaction mode, so the COMMIT ended the transaction that
    owned the asyncpg cursor's portal and the very next fetch raised
    ``portal "__asyncpg_portal_..." does not exist``. The tenant was
    recorded as ``{"error": ...}``, the run ended with errors=1, and
    nothing was ever deleted.

    Each page here is a self-contained statement — ``SELECT id FROM
    resources WHERE tenant_id = :t AND id > :last ORDER BY id LIMIT :n``
    (a resources_pkey index scan, verified by EXPLAIN on prod 2026-10-14)
    followed by one SELECT of that page's rows — so nothing spans a
    commit and peak memory stays at one page of rows, the bound yield_per
    gave. Rows loaded before a commit stay usable after it because
    async_session_factory sets expire_on_commit=False.
    """
    size = page_size or RESOURCE_PAGE_SIZE
    last_id = None
    while True:
        stmt = select(Resource.id).where(Resource.tenant_id == tenant_id)
        if last_id is not None:
            stmt = stmt.where(Resource.id > last_id)
        ids = (await session.execute(
            stmt.order_by(Resource.id).limit(size)
        )).scalars().all()
        if not ids:
            return
        rows = (await session.execute(
            select(Resource).where(Resource.id.in_(ids)).order_by(Resource.id)
        )).scalars().all()
        for res in rows:
            yield res
        if len(ids) < size:
            return
        last_id = ids[-1]


async def enforce_retention_for_tenant(session: AsyncSession, tenant_id: uuid.UUID) -> Dict[str, int]:
    """Walk all resources for a tenant, apply each resource's SLA policy,
    delete snapshots outside retention. Returns per-mode stats.

    Paged: a 5,000-user M365 tenant has 25,000 resources (5 workloads
    each) and tens of millions of snapshots. Loading the full resource set
    into memory used to balloon Python heap to multi-GB on every cron run
    and stalled the scheduler. Keyset paging over resource ids
    (_iter_tenant_resources) + periodic commit keeps peak memory bounded
    to one page of resources plus one resource's snapshots at a time.
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

    # Page resources by keyset over the primary key (_iter_tenant_resources)
    # — NOT a server-side cursor: the periodic commit below ends the
    # transaction a cursor's portal lives in, which is what made every
    # daily run die on the first fetch after its first commit.

    # Commit periodically so we don't hold an open transaction across
    # the entire tenant — at 25k resources this would block VACUUM and
    # any concurrent writers for the duration of the sweep.
    COMMIT_EVERY = 100
    since_commit = 0

    async for res in _iter_tenant_resources(session, tenant_id):
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

        # Counter truth BEFORE any keep-set decision. Every selector below
        # reads item_count / bytes_total as its evidence of content, and
        # those counters are running totals that nothing restates when the
        # owning process dies — prod 2026-08-11 held 25 snapshots reading
        # item_count=0 over 270,885 real snapshot_items rows. Judging the
        # keep set on that is how a FAILED-mid-walk backup loses its bucket
        # and gets hard-deleted with its items. See
        # _restate_lying_counters.
        await _restate_lying_counters(session, snaps)

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
            # Each selector already force-keeps a declared FULL baseline
            # (_with_baseline). On the active-resource path also protect the
            # BROADENED anchors — largest/oldest data-bearing — because
            # production has zero rows typed FULL, so the FULL test alone is a
            # no-op and left four OneDrive baselines to age out.
            keep = keep | _baseline_keep_ids(snaps)

        to_delete = {s.id for s in snaps} - keep
        # A snapshot a worker may still be writing to is never deletable,
        # whatever the retention window says.
        in_flight = {s.id for s in snaps if _is_protected_in_flight(s)}
        skipped = to_delete & in_flight
        if skipped:
            logger.info(
                "[retention] resource=%s deferring %d non-terminal/leased snapshot(s)",
                res.id, len(skipped),
            )
            to_delete -= in_flight
        if _retention_deletes_permitted(settings.RETENTION_DELETE_ENABLED, to_delete):
            # P4a: write one forensic AuditEvent per doomed snapshot IN THE SAME
            # transaction as the delete — if the delete/rehydration rolls back,
            # so does the audit, so the record never disagrees with reality.
            audit_events = {
                event.snapshot_id: event
                for event in _audit_events_for_deletion(snaps, to_delete, res, policy)
            }
            deleted = await _delete_snapshots(
                session, to_delete, audit_events=audit_events,
            )
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
        stats["kept_snapshots"] += len(keep | skipped)

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
                # Loud, on its own line, with the exception TYPE: the
                # scheduler's summary only counts errors, so the daily
                # portal failure read as "errors=1" for weeks with nothing
                # saying what failed.
                print(f"[RETENTION] ERROR tenant={t.id}: {type(exc).__name__}: {exc}")
                results[str(t.id)] = {"error": str(exc)}
    return results
