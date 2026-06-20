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
from uuid import uuid4

from sqlalchemy import select
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
    raw_children = await graph_client.discover_user_content(
        user_external_id=user_resource.external_id,
        user_principal_name=upn,
        user_display_name=user_resource.display_name,
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

        existing = (await db.execute(
            select(Resource).where(
                Resource.tenant_id == user_resource.tenant_id,
                Resource.type == rtype,
                Resource.parent_resource_id == user_resource.id,
            )
        )).scalar_one_or_none()

        if existing:
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
        else:
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
            db.add(new_row)
            out.append(new_row)

    if commit:
        await db.commit()
    return out


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
