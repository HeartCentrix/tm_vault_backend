"""Snapshot-reuse chain helpers.

A "reuse" snapshot owns zero ``snapshot_items`` rows; it points (via
``reuse_of_snapshot_id``) at the immediately-prior stable snapshot of
the same resource, and (via ``reuse_chain_root_id``) at the terminal
full snapshot whose rows actually represent the inventory.

Every reader of ``snapshot_items`` must resolve through
``resolve_snapshot_items_target`` before issuing the query, so the
chain is transparent to callers. For pre-deploy snapshots and every
full (non-reuse) snapshot, both columns are NULL and the resolver
returns ``snapshot_id`` unchanged — behaviour is identical to today.

Design: docs/superpowers/specs/2026-05-15-snapshot-reuse-pointer-design.md
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Optional, Union

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


UUIDLike = Union[str, uuid.UUID]


class SnapshotNotFound(Exception):
    """Raised when a resolver is asked about a snapshot id that no
    longer exists. Callers should surface 404 to the operator rather
    than silently returning an empty row set — empty inventories are a
    legitimate state for first backups, so distinguishing missing from
    empty is load-bearing for the Recovery UI."""


@dataclass(frozen=True)
class SnapshotReuseInfo:
    """Resolver result. ``target_id`` is what the caller should query
    against ``snapshot_items`` (and any other per-snapshot detail
    tables that own rows for the inventory). ``is_reuse`` lets the
    caller distinguish "this snapshot wrote its own rows" from "this
    snapshot reuses an ancestor's rows" for log lines / metrics.
    """
    target_id: uuid.UUID
    is_reuse: bool
    chain_depth: int  # 0 for a full snapshot, ≥1 for a reuse


async def resolve_snapshot_items_target(
    session: AsyncSession,
    snapshot_id: UUIDLike,
) -> SnapshotReuseInfo:
    """Return the snapshot id whose ``snapshot_items`` rows represent
    the inventory for ``snapshot_id``.

    Implementation: one indexed PK lookup. The chain root id is
    denormalised (``reuse_chain_root_id``) so this is *not* a recursive
    walk. Full snapshots have NULL ``reuse_chain_root_id`` and resolve
    to themselves.

    Raises ``SnapshotNotFound`` if the row doesn't exist. Callers that
    legitimately query stale ids (e.g. just-cancelled snapshots) must
    catch and translate.
    """
    sid = _coerce_uuid(snapshot_id)
    row = (await session.execute(text("""
        SELECT id,
               reuse_of_snapshot_id,
               reuse_chain_root_id
          FROM snapshots
         WHERE id = :sid
    """), {"sid": str(sid)})).first()
    if row is None:
        raise SnapshotNotFound(str(sid))
    if row.reuse_chain_root_id is None:
        return SnapshotReuseInfo(
            target_id=_coerce_uuid(row.id),
            is_reuse=False,
            chain_depth=0,
        )
    # The chain_depth field is informational; we only need a single
    # extra row read to compute it for log/telemetry contexts that
    # care, so query when asked. Most read sites won't inspect it.
    depth = await _measure_chain_depth(session, sid)
    return SnapshotReuseInfo(
        target_id=_coerce_uuid(row.reuse_chain_root_id),
        is_reuse=True,
        chain_depth=depth,
    )


async def resolve_many(
    session: AsyncSession,
    snapshot_ids: list[UUIDLike],
) -> dict[uuid.UUID, uuid.UUID]:
    """Bulk variant: returns ``{snapshot_id: target_id}`` for every
    input id. One query for the lot — used by the Recovery grid + the
    Activity per-resource panel which already fan out across N
    snapshots. Missing ids are silently dropped from the result; the
    caller is expected to compare keys against its input set when that
    distinction matters.
    """
    if not snapshot_ids:
        return {}
    ids_str = [str(_coerce_uuid(s)) for s in snapshot_ids]
    rows = (await session.execute(text("""
        SELECT id,
               COALESCE(reuse_chain_root_id, id) AS target_id
          FROM snapshots
         WHERE id = ANY(CAST(:sids AS UUID[]))
    """), {"sids": ids_str})).all()
    return {
        _coerce_uuid(r.id): _coerce_uuid(r.target_id)
        for r in rows
    }


async def _measure_chain_depth(
    session: AsyncSession,
    snapshot_id: uuid.UUID,
) -> int:
    """Recursive CTE over the reuse chain. Bounded by
    ``SNAPSHOT_REUSE_MAX_CHAIN_DEPTH`` (set by the settle path so a
    runaway chain can never form); we still cap at 1024 here so a
    misconfigured deployment can't hang a read.
    """
    row = (await session.execute(text("""
        WITH RECURSIVE chain AS (
            SELECT id, reuse_of_snapshot_id, 0 AS depth
              FROM snapshots
             WHERE id = :sid
            UNION ALL
            SELECT s.id, s.reuse_of_snapshot_id, c.depth + 1
              FROM snapshots s
              JOIN chain c ON s.id = c.reuse_of_snapshot_id
             WHERE c.depth < 1024
        )
        SELECT MAX(depth) AS depth FROM chain
    """), {"sid": str(snapshot_id)})).first()
    return int(row.depth or 0) if row else 0


def _coerce_uuid(value: UUIDLike) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(str(value))


async def chat_rows(session: AsyncSession, snapshot_ids):
    """``(entity, scope)`` for reading the snapshot_items rows of ``snapshot_ids`` (already resolved to
    the snapshots that hold rows). Full snapshots: ``(SnapshotItem, snapshot_id IN ids)``, today's plan.
    When any is an overlay: an aliased SnapshotItem over snapshot_items_effective(), every row labelled
    with the snapshot it was asked for (a chat overlay's inherited chats are its base's rows, which
    readers matching rows to snapshot_chat_inventories or to the requested scope must see as its own).
    Read only: rows of the alias are never flushed (populate_existing keeps labels per query)."""
    from sqlalchemy import literal, select, union_all
    from sqlalchemy.dialects.postgresql import UUID as PGUUID
    from sqlalchemy.orm import aliased
    from shared.models import SnapshotItem
    ids = [_coerce_uuid(s) for s in snapshot_ids]
    overlays = set((await session.execute(text(
        "SELECT s.id FROM snapshots s JOIN snapshots t ON t.id = COALESCE(s.reuse_chain_root_id, s.id) "
        "WHERE s.id = ANY(CAST(:ids AS uuid[])) AND t.overlay_base_id IS NOT NULL"),
        {"ids": [str(i) for i in ids]})).scalars().all()) if ids else set()
    if not overlays:
        return SnapshotItem, SnapshotItem.snapshot_id.in_(ids)
    columns = [c for c in SnapshotItem.__table__.columns]
    parts = []
    for sid in ids:
        source = func_effective(sid)
        parts.append(select(*[(literal(sid, PGUUID(as_uuid=True)) if c.name == "snapshot_id" else source.c[c.name]).label(c.name)
                              for c in columns]))
    rows = (union_all(*parts) if len(parts) > 1 else parts[0]).subquery()   # anonymous: two chat_rows entities may share a statement
    entity = aliased(SnapshotItem, rows, adapt_on_names=True)
    return entity, entity.snapshot_id.in_(ids)


def func_effective(snapshot_id):
    from sqlalchemy import column, func
    from shared.models import SnapshotItem
    # Typed columns: readers apply JSON operators (extra_data["chat_id"]) to the alias.
    return func.snapshot_items_effective(snapshot_id).table_valued(
        *[column(c.name, c.type) for c in SnapshotItem.__table__.columns]).alias()


__all__ = [
    "SnapshotReuseInfo",
    "SnapshotNotFound",
    "resolve_snapshot_items_target",
    "resolve_many",
    "items_scope",
    "items_scope_any",
    "item_in_snapshot",
    "chat_rows",
]


async def items_scope(session: AsyncSession, snapshot_id: UUIDLike):
    """WHERE clause on ``SnapshotItem`` selecting ``snapshot_id``'s inventory. A full snapshot (or a
    pointer to one) keeps today's ``snapshot_id = target`` plan; an overlay, or a pointer to one,
    selects ``snapshot_items_effective`` (base + own rows, docs/mail-overlay-snapshots.md).
    Raises ``SnapshotNotFound``."""
    from sqlalchemy import func, select
    from shared.models import SnapshotItem
    sid = _coerce_uuid(snapshot_id)
    row = (await session.execute(text("""
        SELECT t.id AS target_id, t.overlay_base_id
          FROM snapshots s JOIN snapshots t ON t.id = COALESCE(s.reuse_chain_root_id, s.id)
         WHERE s.id = :sid
    """), {"sid": str(sid)})).first()
    if row is None:
        raise SnapshotNotFound(str(sid))
    target = _coerce_uuid(row.target_id)
    if row.overlay_base_id is None:
        return SnapshotItem.snapshot_id == target
    effective = func.snapshot_items_effective(target).table_valued("id")
    return SnapshotItem.id.in_(select(effective.c.id))


async def items_scope_any(session: AsyncSession, snapshot_ids):
    """``items_scope`` OR-ed over several snapshots; an unknown id selects nothing."""
    from sqlalchemy import false, or_
    scopes = []
    for sid in snapshot_ids:
        try:
            scopes.append(await items_scope(session, sid))
        except (SnapshotNotFound, ValueError, TypeError, AttributeError):
            continue
    return or_(*scopes) if scopes else false()


async def item_in_snapshot(session: AsyncSession, item_id: UUIDLike, snapshot_id: UUIDLike) -> bool:
    """True when the row ``item_id`` is part of ``snapshot_id``'s inventory - its own row, the
    row of the full snapshot a pointer names, or a base row an overlay inherits."""
    try:
        iid, sid = _coerce_uuid(item_id), _coerce_uuid(snapshot_id)
    except (ValueError, TypeError, AttributeError):
        return False
    return bool(await session.scalar(text(
        "SELECT EXISTS (SELECT 1 FROM snapshot_items_effective(CAST(:sid AS uuid)) e WHERE e.id = CAST(:iid AS uuid))"),
        {"sid": str(sid), "iid": str(iid)}))


async def items_snapshot_id(session: AsyncSession, snapshot_id: UUIDLike) -> uuid.UUID:
    """``resolve_snapshot_items_target(...).target_id``, or the id itself when the snapshot row is
    gone (callers keep their own not-found handling)."""
    try:
        return (await resolve_snapshot_items_target(session, snapshot_id)).target_id
    except SnapshotNotFound:
        return _coerce_uuid(snapshot_id)


async def point_at(session: AsyncSession, *, target_snapshot_id: UUIDLike, prior_snapshot_id: UUIDLike,
                   marker: str) -> bool:
    """Make ``target`` a reuse snapshot of the full snapshot ``prior``: no rows of its own, the prior's
    totals, nothing new; ``extra_data[marker]`` names the prior. False, with nothing written, when the
    target already owns rows or points elsewhere. The validation trigger requires ``prior`` to be a
    COMPLETED full snapshot of the same resource, started earlier."""
    row = (await session.execute(text("""
        UPDATE snapshots t SET reuse_of_snapshot_id = p.id, reuse_chain_root_id = p.id,
            item_count = p.item_count, bytes_total = p.bytes_total, new_item_count = 0, bytes_added = 0,
            extra_data = (COALESCE(t.extra_data::jsonb, '{}'::jsonb) || jsonb_build_object(CAST(:marker AS text), p.id::text))::json
        FROM snapshots p
        WHERE t.id = CAST(:target AS UUID) AND p.id = CAST(:prior AS UUID) AND t.reuse_of_snapshot_id IS NULL
          AND NOT EXISTS (SELECT 1 FROM snapshot_items WHERE snapshot_id = t.id)
        RETURNING t.id"""), {"target": str(target_snapshot_id), "prior": str(prior_snapshot_id), "marker": marker})).first()
    return row is not None


async def resolve_snapshot_ids(session: AsyncSession, snapshot_ids) -> list:
    """Each id mapped to the snapshot whose rows it reads (itself, or the full snapshot a reuse snapshot
    names), order kept; unknown or non-UUID ids are kept as given."""
    def parsed(value):
        try:
            return _coerce_uuid(value)
        except (ValueError, TypeError, AttributeError):
            return None
    values = list(snapshot_ids)
    ids = [parsed(value) for value in values]
    known = [str(i) for i in ids if i is not None]
    rows = dict((await session.execute(text(
        "SELECT id, COALESCE(reuse_chain_root_id, id) FROM snapshots WHERE id = ANY(CAST(:ids AS uuid[]))"),
        {"ids": known})).all()) if known else {}
    return [value if i is None else _coerce_uuid(rows.get(i, i)) for value, i in zip(values, ids)]
