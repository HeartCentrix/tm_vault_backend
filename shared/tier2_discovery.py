"""Tier-2 per-user content discovery (Mail / OneDrive / Contacts / Calendar / Chats).

Single source of truth for materialising the five fixed USER_* child rows
beneath an ENTRA_USER. Used by:

  * tenant-service: per-user "Backup now" flow + the discover-content endpoint.
  * job-service:    inline gap-fill before "Backup all M365 now" fans out.
  * discovery-worker: consumer for the discovery.tier2 queue
    (enqueued by SLA-assignment hook and the 7h scheduler backstop).
  * backup-scheduler: 7h sweep.

All callers go through `ensure_tier2_children`. Idempotent on re-call: if the
five rows already exist they're refreshed in-place (display_name, metadata,
external_id, status, parent-SLA inheritance) and no duplicate rows are
created. Returns the child Resource objects so callers can fan a bulk backup
out across them without a second SELECT.
"""
from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, TypeVar
from datetime import datetime
from uuid import uuid4

from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from shared.models import Resource, ResourceStatus, ResourceType


_T = TypeVar("_T")
_R = TypeVar("_R")


TIER2_CHILD_TYPES: tuple[ResourceType, ...] = (
    ResourceType.USER_MAIL,
    ResourceType.USER_ONEDRIVE,
    ResourceType.USER_CONTACTS,
    ResourceType.USER_CALENDAR,
    ResourceType.USER_CHATS,
)


# Maps the string keys that GraphClient.discover_user_content returns to the
# canonical ResourceType. Kept here so tenant-service no longer needs its own
# local TYPE_MAP for the Tier-2 subset.
_TIER2_TYPE_MAP: Dict[str, ResourceType] = {
    "USER_MAIL": ResourceType.USER_MAIL,
    "USER_ONEDRIVE": ResourceType.USER_ONEDRIVE,
    "USER_CONTACTS": ResourceType.USER_CONTACTS,
    "USER_CALENDAR": ResourceType.USER_CALENDAR,
    "USER_CHATS": ResourceType.USER_CHATS,
    # Exchange in-place archive — persistable Tier-2 child (inherits the parent
    # user's SLA, so it's backed up on the user's normal fire). Deliberately NOT
    # in TIER2_CHILD_TYPES: archive is opt-in per user, so requiring it would
    # make has_complete_tier2 never-complete and force endless re-discovery.
    "USER_ARCHIVE": ResourceType.USER_ARCHIVE,
}


def chunk_user_ids(user_ids: Iterable[Any], chunk_size: int) -> List[List[str]]:
    """Split user IDs into queue-friendly chunks.

    RabbitMQ distributes messages across discovery-worker replicas, so small
    bounded chunks make replica scaling useful without querying cloud-specific
    replica counts.
    """
    size = max(1, int(chunk_size or 1))
    ids = [str(user_id) for user_id in user_ids]
    return [ids[i:i + size] for i in range(0, len(ids), size)]


async def run_bounded_user_tasks(
    user_ids: Iterable[_T],
    *,
    concurrency: int,
    worker: Callable[[_T], Awaitable[_R]],
) -> List[_R]:
    """Run per-user discovery work concurrently with a hard local cap."""
    limit = max(1, int(concurrency or 1))
    semaphore = asyncio.Semaphore(limit)

    async def _run_one(user_id: _T) -> _R:
        async with semaphore:
            return await worker(user_id)

    return await asyncio.gather(*(_run_one(user_id) for user_id in user_ids))


def _parent_lock_id(parent_id) -> int:
    """Stable 63-bit advisory-lock key derived from the parent ENTRA_USER's
    UUID (same folding the discovery worker uses for its tenant lock)."""
    v = parent_id.int
    return ((v >> 64) ^ (v & 0x7FFFFFFFFFFFFFFF)) & 0x7FFFFFFFFFFFFFFF


async def _find_existing_child(
    db: AsyncSession,
    tenant_id,
    rtype: ResourceType,
    parent_resource_id,
) -> Optional[Resource]:
    """The child row for (tenant, type, parent) — tolerant of duplicates.

    resources has no unique constraint on (tenant_id, type,
    parent_resource_id) yet (alembic 0007 adds it) and four callers race this
    code for the same user, so duplicate USER_* children DO exist in prod.
    `scalar_one_or_none()` raised MultipleResultsFound on them, which
    permanently poisoned that user's Tier-2 discovery. Pick the oldest row so
    concurrent callers converge on the same survivor, and never delete the
    extras — they may own snapshots.
    """
    rows = (await db.execute(
        select(Resource).where(
            Resource.tenant_id == tenant_id,
            Resource.type == rtype,
            Resource.parent_resource_id == parent_resource_id,
        ).order_by(Resource.created_at.asc(), Resource.id.asc())
    )).scalars().all()
    if not rows:
        return None
    # Re-sorted here, not just in SQL: legacy rows can carry a NULL
    # created_at, and the survivor must not depend on which caller ran the
    # query or how the DB happened to order the rows.
    rows = sorted(
        rows,
        key=lambda r: (r.created_at is None, r.created_at or datetime.min, str(r.id)),
    )
    if len(rows) > 1:
        print(
            f"[tier2_discovery] {len(rows)} duplicate {rtype.value} rows under "
            f"parent {parent_resource_id} — refreshing oldest ({rows[0].id}); "
            "extras left in place for operator cleanup"
        )
    return rows[0]


async def _insert_child_or_get_winner(
    db: AsyncSession,
    new_row: Resource,
    tenant_id,
    rtype: ResourceType,
    parent_resource_id,
) -> Resource:
    """INSERT `new_row` inside a SAVEPOINT; on a duplicate-key conflict adopt
    the winner's row instead of poisoning the caller's transaction.

    Callers pass `commit=False` and batch many users into one transaction, so
    an unhandled IntegrityError would fail the whole batch. Once the unique
    index on (tenant_id, type, parent_resource_id) exists the loser of a
    four-caller race lands here; without the index the flush just succeeds.
    """
    try:
        async with db.begin_nested():
            db.add(new_row)
            await db.flush()
        return new_row
    except IntegrityError:
        winner = await _find_existing_child(db, tenant_id, rtype, parent_resource_id)
        if winner is None:
            # Not the duplicate-child race (bad FK, …) — fail loudly rather
            # than silently dropping a resource we were asked to create.
            raise
        return winner


async def has_complete_tier2(db: AsyncSession, user_resource_id) -> bool:
    """Cheap precheck — does every one of the five USER_* types exist under
    this ENTRA_USER? Used by the bulk trigger and the backstop sweep to skip
    Graph discovery when nothing needs to be done."""
    result = await db.execute(
        select(Resource.type).where(
            Resource.parent_resource_id == user_resource_id,
            Resource.type.in_(TIER2_CHILD_TYPES),
        )
    )
    present = {row[0] for row in result.all()}
    return all(t in present for t in TIER2_CHILD_TYPES)


async def ensure_tier2_children(
    db: AsyncSession,
    user_resource: Resource,
    graph_client: Any,
    *,
    commit: bool = True,
) -> List[Resource]:
    """Materialise the five Tier-2 child rows under `user_resource`.

    `user_resource` must be a loaded ENTRA_USER. `graph_client` must expose
    `discover_user_content(user_external_id, user_principal_name,
    user_display_name)` returning a list of dicts with keys:
    `type`, `external_id`, `display_name`, `email?`, `metadata?`.

    Idempotent: existing children are updated in-place (display_name, email,
    extra_data merged so backup-worker-written delta tokens survive,
    external_id, parent SLA, status). License-missing children land as
    INACCESSIBLE so the UI renders them as "No license" rows but the bulk
    trigger excludes them from backup fan-out.

    Set `commit=False` when the caller manages the transaction (e.g.
    discovery-worker batching multiple users). Default commits per-call so
    short-lived callers (HTTP handlers) don't need to remember.

    Returns the child Resource objects in the order they appear in Graph's
    response — INCLUDING any INACCESSIBLE ones. Callers that fan out
    backups should filter on `status != ResourceStatus.INACCESSIBLE`
    themselves; readers (UI, audit) want the full list.
    """
    upn = (user_resource.extra_data or {}).get("user_principal_name") or user_resource.email
    # License coverage learned at Tier-1 discovery (assignedPlans) — lets
    # discover_user_content skip probing workloads the user isn't licensed for
    # and mark them license_missing instantly.
    licensed_services = (user_resource.extra_data or {}).get("licensed_services")
    raw_children = await graph_client.discover_user_content(
        user_external_id=user_resource.external_id,
        user_principal_name=upn,
        user_display_name=user_resource.display_name,
        licensed_services=licensed_services,
        # Archive discovery below owns the allowlist and performs one probe.
        include_archive=False,
    )

    # `resources` has no unique constraint on (tenant_id, parent_resource_id,
    # type), and four independent callers (tenant-service Backup-now,
    # job-service gap-fill, the discovery.tier2 consumer, the 7h backstop)
    # run this concurrently for the same user. Two racers interleaving the
    # check-then-insert below created duplicate USER_* children, after which
    # every later lookup raised MultipleResultsFound and the user was stuck
    # DISCOVERY_FAILED. When any child row is still missing, serialize on a
    # per-user advisory lock (xact-scoped: released at the caller's
    # commit/rollback). The steady-state refresh path — all rows already
    # exist — stays lock-free so a long-running batch (commit=False) never
    # blocks an interactive Backup-now.
    try:
        _dialect = db.get_bind().dialect.name
    except Exception:
        _dialect = ""
    if _dialect == "postgresql":
        existing_types = {
            row[0]
            for row in (
                await db.execute(
                    select(Resource.type).where(
                        Resource.tenant_id == user_resource.tenant_id,
                        Resource.parent_resource_id == user_resource.id,
                    )
                )
            ).all()
        }
        needs_lock = any(t not in existing_types for t in TIER2_CHILD_TYPES)
        if not needs_lock and ResourceType.USER_ARCHIVE not in existing_types:
            # Mirror _ensure_archive_child's gate so archive-eligible users
            # get the same protection on their first archive materialisation.
            from shared.config import settings as _settings
            _archive_upn = (
                (user_resource.extra_data or {}).get("user_principal_name")
                or user_resource.email
                or ""
            ).lower()
            _allowed = _settings.ARCHIVE_BACKUP_ALLOWED_UPNS
            needs_lock = _settings.ARCHIVE_BACKUP_ENABLED and (
                not _allowed or _archive_upn in _allowed
            )
        if needs_lock:
            await db.execute(
                text("SELECT pg_advisory_xact_lock(:lock_key)"),
                {"lock_key": _parent_lock_id(user_resource.id)},
            )

    out: List[Resource] = []
    for c in raw_children:
        rtype = _TIER2_TYPE_MAP.get(c.get("type"))
        if rtype is None:
            # Graph returned a type we don't model — ignore rather than crash;
            # forward-compat with future Graph categories.
            continue

        meta = c.get("metadata") or {}
        is_license_missing = bool(meta.get("license_missing"))
        # `discovery_pending` is the new marker for transient Graph
        # failures (504/timeout) after retries are exhausted. We still
        # land an INACCESSIBLE row so the resource appears in the list
        # and backup fan-out skips it; the next discovery cycle will
        # overwrite metadata with real data once Graph recovers. Without
        # this, a single transient blip would silently lose a user's
        # entire workload (the bug that hit Amit's USER_CHATS).
        is_discovery_pending = bool(meta.get("discovery_pending"))
        child_status = (
            ResourceStatus.INACCESSIBLE
            if (is_license_missing or is_discovery_pending)
            else ResourceStatus.DISCOVERED
        )

        # Duplicate-tolerant lookup: rows duplicated BEFORE the advisory lock
        # above existed (or by a lost race) must not wedge the user in
        # DISCOVERY_FAILED forever — the lock only serialises new inserts, it
        # cannot un-duplicate history.
        existing = await _find_existing_child(
            db, user_resource.tenant_id, rtype, user_resource.id
        )

        if existing is None:
            new_row = Resource(
                id=uuid4(),
                tenant_id=user_resource.tenant_id,
                type=rtype,
                external_id=c["external_id"],
                display_name=c["display_name"],
                email=c.get("email"),
                extra_data=meta,
                parent_resource_id=user_resource.id,
                sla_policy_id=user_resource.sla_policy_id,
                status=child_status,
            )
            existing = await _insert_child_or_get_winner(
                db, new_row, user_resource.tenant_id, rtype, user_resource.id
            )
            if existing is new_row:
                out.append(new_row)
                continue
            # Lost the insert race — fall through and refresh the row the
            # winner committed instead of duplicating it.

        existing.display_name = c["display_name"]
        existing.email = c.get("email")
        # Merge, don't replace — preserves backup-worker-written state
        # (delta_token, mail_delta_token, calendar_delta_token,
        # channel_delta_tokens, chat cursors). Graph discovery keys
        # (drive_id, user_id, …) are stable and won't collide.
        existing.extra_data = {**(existing.extra_data or {}), **meta}
        # If we just recovered from a transient discovery error,
        # drop the stale pending markers — otherwise they'd stick
        # in extra_data forever and the row would look "broken" in
        # the UI even though it's healthy now. Only clear when the
        # current probe was actually successful (i.e. neither flag
        # is set in this round's meta).
        if not is_license_missing and not is_discovery_pending:
            existing.extra_data.pop("discovery_pending", None)
            existing.extra_data.pop("last_discovery_error", None)
            existing.extra_data.pop("license_missing", None)
            existing.extra_data.pop("license_hint", None)
            existing.extra_data.pop("probe_status", None)
        existing.external_id = c["external_id"]
        # Re-inherit parent SLA so a parent re-policy reaches children
        # on next discovery sweep without manual fan-out.
        existing.sla_policy_id = user_resource.sla_policy_id
        existing.status = child_status
        out.append(existing)

    archive_child = await _ensure_archive_child(db, user_resource, graph_client)
    if archive_child is not None:
        out.append(archive_child)

    if commit:
        await db.commit()
    return out


async def _ensure_archive_child(
    db: AsyncSession,
    user_resource: Resource,
    graph_client: Any,
) -> Optional[Resource]:
    """USER_ARCHIVE (Task 1.2) — jrue-only pilot, allowlist-gated.

    Unlike the five fixed Tier-2 types above (materialised in bulk via
    `discover_user_content`), the Exchange Online Archive probe is a
    separate, explicitly-gated call so non-pilot users incur ZERO Graph
    traffic for it — the Mailbox Import/Export API is metered.

    Order matters:
      1. `ARCHIVE_BACKUP_ENABLED` and the allowlist are checked FIRST,
         entirely without a Graph call. A disabled feature or a
         non-allowlisted UPN short-circuits before `get_exchange_settings`
         is ever invoked.
      2. Only then do we call `get_exchange_settings`.
      3. Probe failures (429/5xx, or any other exception) are logged and
         SKIPPED quietly this run — never create a child from a failed
         probe (create-then-orphan is a prior-defect class in this
         codebase). An unlicensed/no-archive user naturally 404s here too
         and is treated the same way: no child.
      4. No archive in the payload ⇒ no child.

    Returns the child Resource if created/updated this call, else None.
    Never deletes an existing child (e.g. if an archive is later
    unassigned) — that's out of scope here and left to a future sweep.
    """
    from shared.config import settings as _settings

    if not _settings.ARCHIVE_BACKUP_ENABLED:
        return None

    upn = ((user_resource.extra_data or {}).get("user_principal_name") or user_resource.email or "").lower()
    allowed = _settings.ARCHIVE_BACKUP_ALLOWED_UPNS
    if allowed and upn not in allowed:
        return None

    try:
        payload = await graph_client.get_exchange_settings(user_resource.external_id)
    except Exception as exc:
        print(
            f"[tier2_discovery] archive probe failed for "
            f"{upn or user_resource.external_id}: {type(exc).__name__}: {exc} "
            "— skipping this run"
        )
        return None

    if not payload:
        return None

    # exchangeSettings (Graph beta) is a flat object: primaryMailboxId is
    # always present; inPlaceArchiveMailboxId is present ONLY when the
    # user has an In-Place Archive. No nested "archiveMailbox" object —
    # do not confuse the two ids.
    archive_id = payload.get("inPlaceArchiveMailboxId")
    if not archive_id:
        return None

    display = user_resource.display_name or upn or user_resource.external_id
    display_name = f"Archive — {display}"
    meta = {
        "archiveMailboxId": archive_id,
        "archive_mailbox_id": archive_id,
        "primary_mailbox_id": payload.get("primaryMailboxId"),
        "user_id": user_resource.external_id,
        "user_principal_name": upn,
        "graph_version": _settings.ARCHIVE_GRAPH_VERSION,
    }

    # Same duplicate-tolerant lookup as the five fixed children above.
    existing = await _find_existing_child(
        db, user_resource.tenant_id, ResourceType.USER_ARCHIVE, user_resource.id
    )

    if existing is None:
        new_row = Resource(
            id=uuid4(),
            tenant_id=user_resource.tenant_id,
            type=ResourceType.USER_ARCHIVE,
            external_id=archive_id,
            display_name=display_name,
            email=None,
            extra_data=meta,
            parent_resource_id=user_resource.id,
            sla_policy_id=user_resource.sla_policy_id,
            status=ResourceStatus.DISCOVERED,
        )
        existing = await _insert_child_or_get_winner(
            db, new_row, user_resource.tenant_id, ResourceType.USER_ARCHIVE, user_resource.id
        )
        if existing is new_row:
            return new_row
        # Lost the insert race — refresh the winner's row below.

    existing.display_name = display_name
    existing.external_id = archive_id
    existing.extra_data = {**(existing.extra_data or {}), **meta}
    existing.sla_policy_id = user_resource.sla_policy_id
    existing.status = ResourceStatus.DISCOVERED
    return existing


async def find_users_missing_tier2(
    db: AsyncSession,
    user_resource_ids: Optional[List] = None,
    *,
    require_sla: bool = True,
) -> List[Resource]:
    """Return ENTRA_USER rows that lack one or more of the five Tier-2 types.

    `user_resource_ids` scopes the check; pass None to sweep every user (used
    by the 7h backstop). `require_sla=True` is the default — only users that
    operators actually expect to be backed up surface as gaps. Set False for
    audit-style scans that want every user.
    """
    stmt = select(Resource).where(Resource.type == ResourceType.ENTRA_USER)
    if require_sla:
        stmt = stmt.where(Resource.sla_policy_id.is_not(None))
    if user_resource_ids:
        stmt = stmt.where(Resource.id.in_(user_resource_ids))
    users = (await db.execute(stmt)).scalars().all()

    if not users:
        return []

    # One COUNT-by-type query for all users in scope — cheap regardless of
    # tenant size.
    user_ids = [u.id for u in users]
    children_q = await db.execute(
        select(Resource.parent_resource_id, Resource.type).where(
            Resource.parent_resource_id.in_(user_ids),
            Resource.type.in_(TIER2_CHILD_TYPES),
        )
    )
    present: Dict[Any, set] = {}
    for parent_id, rtype in children_q.all():
        present.setdefault(parent_id, set()).add(rtype)

    missing: List[Resource] = []
    for u in users:
        have = present.get(u.id, set())
        if not all(t in have for t in TIER2_CHILD_TYPES):
            missing.append(u)
    return missing
