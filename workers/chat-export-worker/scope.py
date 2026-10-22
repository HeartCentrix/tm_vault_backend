"""Resolve job payload -> (messages, attachment_map, hosted_map, layout).

SnapshotItem.resource_id does not exist -- JOIN Snapshot to filter by resource.

Chat message bodies were relocated to ``chat_thread_messages`` in the 2026-05-13
Level 2 refactor. snapshot_items now carries thin pointer rows; we hydrate
each row's ``extra_data['raw']`` from ``chat_thread_messages.metadata_raw``
before handing the list to the renderer.
"""
from dataclasses import dataclass
import logging
import mimetypes
from sqlalchemy import select, text
from sqlalchemy.orm import aliased
from shared.models import SnapshotItem, Snapshot, Resource

log = logging.getLogger("chat-export.scope")


@dataclass
class ResolvedScope:
    layout: str
    thread_path: str
    messages: list
    attachment_map: dict
    hosted_map: dict


async def resolve(sess, *, resource_id, snapshot_ids, thread_path: str | None,
                  item_ids: list[str]) -> ResolvedScope:
    if thread_path and item_ids:
        raise ValueError("INVALID_SELECTION")
    if not thread_path and not item_ids:
        raise ValueError("SCOPE_EMPTY")

    Sn = aliased(Snapshot)
    CHAT_TYPES = ["TEAMS_CHAT_MESSAGE", "TEAMS_MESSAGE", "TEAMS_MESSAGE_REPLY"]

    # Accept parent ENTRA_USER or child USER_CHATS id interchangeably.
    child_ids = [
        r[0] for r in (await sess.execute(
            select(Resource.id).where(Resource.parent_resource_id == resource_id)
        ))
    ]
    resource_ids = [resource_id, *child_ids]

    if thread_path:
        q = (select(SnapshotItem)
             .join(Sn, Sn.id == SnapshotItem.snapshot_id)
             .where(Sn.resource_id.in_(resource_ids))
             .where(SnapshotItem.snapshot_id.in_(snapshot_ids))
             .where(SnapshotItem.item_type.in_(CHAT_TYPES))
             .where(SnapshotItem.folder_path == thread_path)
             .order_by(SnapshotItem.created_at.asc()))
        msgs = list((await sess.execute(q)).scalars())
        layout = "single_thread"
        effective = thread_path
    else:
        q = (select(SnapshotItem)
             .join(Sn, Sn.id == SnapshotItem.snapshot_id)
             .where(Sn.resource_id.in_(resource_ids))
             .where(SnapshotItem.id.in_(item_ids))
             .order_by(SnapshotItem.created_at.asc()))
        msgs = list((await sess.execute(q)).scalars())
        paths = {m.folder_path for m in msgs}
        if len(paths) > 1:
            raise ValueError("MULTI_THREAD_NOT_SUPPORTED_YET")
        if not paths:
            raise ValueError("SCOPE_EMPTY")
        effective = next(iter(paths))
        layout = "per_message"

    # Hydrate each message's raw payload from chat_thread_messages. The
    # renderer expects ``extra_data['raw']`` to carry the full Graph
    # message dict (body, from, attachments, mentions, reactions, etc.).
    # Without this step every exported message would render as "(empty)".
    if msgs:
        ext_ids_raw = [m.external_id for m in msgs if m.external_id]
        if ext_ids_raw:
            hydrate_rows = (await sess.execute(
                text(
                    "SELECT ct.tenant_id, ct.chat_id, "
                    "       ctm.message_external_id, ctm.metadata_raw "
                    "  FROM chat_thread_messages ctm "
                    "  JOIN chat_threads ct ON ct.id = ctm.chat_thread_id "
                    " WHERE ctm.message_external_id = ANY(:ext_ids) "
                    "   AND ct.archived_at IS NULL "
                    "   AND ctm.archived_at IS NULL"
                ),
                {"ext_ids": ext_ids_raw},
            )).all()
            # Key by (tenant_id, chat_id, message_external_id) — message_external_id
            # alone collides across tenants in theory, and we already have the
            # other identifiers on each SnapshotItem.
            raw_by_key: dict = {}
            for hr in hydrate_rows:
                raw_by_key[(str(hr.tenant_id), hr.chat_id, hr.message_external_id)] = hr.metadata_raw
            _hydrate_missing = 0
            _hydrate_total = 0
            _gap_tenant_id: str | None = None
            _gap_chat_id: str | None = None
            for m in msgs:
                _hydrate_total += 1
                raw = raw_by_key.get(
                    (str(m.tenant_id), m.parent_external_id, m.external_id)
                )
                if raw is None:
                    _hydrate_missing += 1
                    if _gap_tenant_id is None and getattr(m, "tenant_id", None):
                        _gap_tenant_id = str(m.tenant_id)
                    if _gap_chat_id is None and m.parent_external_id:
                        _gap_chat_id = m.parent_external_id
                    continue
                ed = dict(m.extra_data or {})
                ed["raw"] = raw if isinstance(raw, dict) else {}
                # SQLAlchemy lets us mutate the JSON column even on a
                # detached row; the renderer reads it as a plain dict.
                m.extra_data = ed

            # P6: integrity-gap alert from the export hot path. If >20% of
            # pointer rows in this export scope have no backing
            # chat_thread_messages row, the export will render placeholder
            # bodies — fire an INTEGRITY_GAP audit so ops can re-drain.
            if _hydrate_total >= 5 and _hydrate_missing > 0:
                _miss_pct = (_hydrate_missing / float(_hydrate_total)) * 100.0
                if _miss_pct >= 20.0:
                    try:
                        import os as _os
                        import httpx as _httpx_alert
                        from shared.audit import internal_audit_headers
                        _audit_url = _os.getenv(
                            "AUDIT_SERVICE_URL", "http://audit-service:8012"
                        )
                        async with _httpx_alert.AsyncClient(timeout=2.0) as _c:
                            await _c.post(
                                f"{_audit_url}/api/v1/audit/log",
                                headers=internal_audit_headers(),
                                json={
                                    "action": "INTEGRITY_GAP",
                                    "actor_type": "SYSTEM",
                                    "tenant_id": _gap_tenant_id,
                                    "resource_type": "CHAT_THREAD",
                                    "resource_id": _gap_chat_id,
                                    "outcome": "DETECTED",
                                    "details": {
                                        "chat_id": _gap_chat_id,
                                        "scope_total": _hydrate_total,
                                        "missing_count": _hydrate_missing,
                                        "miss_pct": round(_miss_pct, 2),
                                        "source": "chat-export-worker:scope.resolve",
                                    },
                                },
                            )
                    except Exception:
                        pass

    ext_ids = [m.external_id for m in msgs]
    att_map: dict = {}
    hosted_map: dict = {}
    if ext_ids:
        # Child-row scoping. Two real defects in the pre-fix query
        # (item_type + parent_external_id only, matching the entire
        # snapshot_items table):
        #   * cross-TENANT bleed — Teams message ids are creation-timestamp
        #     strings that recur across chats and tenants, so another
        #     tenant's attachment row could be pulled into this ZIP;
        #   * N-fold duplication — chat pointer rows are carried forward into
        #     every later snapshot (backup-worker
        #     `_carry_forward_prior_chat_snapshot_items`), so one logical
        #     attachment exists once per snapshot that ever held its message
        #     and the ZIP got N copies of the same file.
        #
        # What must NOT be scoped away is the SNAPSHOT. In a shared chat only
        # the FIRST drainer persists CHAT_ATTACHMENT / CHAT_HOSTED_CONTENT
        # rows; claim-skipped users get the message pointer row while the
        # child row stays under the first drainer's snapshot (and resource) —
        # see backup-worker/main.py:1230-1237 and the same invariant spelled
        # out at services/snapshot-service/main.py:3158-3170, which is why
        # that read path scopes CHAT_ATTACHMENT by tenant, not by snapshot.
        # Restricting children to the exporting resource's snapshots would
        # DROP attachments that were exported before this fix.
        #
        # Tenant scoping cannot trust SnapshotItem.tenant_id alone:
        # `_userchats_backup_hosted_contents` never passes tenant_id
        # (backup-worker/main.py:10120), carry-forward copies src.tenant_id so
        # the NULL propagates, and P8's back-fill can't reach those rows (its
        # source predicate is `src.tenant_id = :tid`). Every
        # CHAT_HOSTED_CONTENT row therefore has tenant_id NULL, and a plain
        # `tenant_id IN (...)` silently drops every inline image. Rows with no
        # tenant_id get their tenancy resolved through the owning snapshot's
        # resource instead; anything still unattributable is excluded but
        # LOGGED — never dropped in silence.
        msg_tenant_ids = {m.tenant_id for m in msgs if m.tenant_id is not None}

        def _child_query(item_type: str):
            # Newest first: retention / blob reap walks the oldest snapshots
            # first, so when several copies carry a blob the newest one is
            # the least likely to have been reaped out from under us.
            return (select(SnapshotItem)
                    .where(SnapshotItem.item_type == item_type)
                    .where(SnapshotItem.parent_external_id.in_(ext_ids))
                    .order_by(SnapshotItem.created_at.desc()))

        async def _tenant_by_snapshot(snapshot_ids: set) -> dict:
            """snapshot_id -> owning resource's tenant_id. Fallback tenancy
            for child rows whose own tenant_id column is NULL."""
            if not snapshot_ids:
                return {}
            rows = (await sess.execute(
                select(Snapshot.id, Resource.tenant_id)
                .join(Resource, Resource.id == Snapshot.resource_id)
                .where(Snapshot.id.in_(list(snapshot_ids)))
            )).all()
            return {r[0]: r[1] for r in rows}

        def _one_per_child(rows) -> list:
            best: dict = {}
            for r in rows:
                key = (r.parent_external_id, r.external_id)
                cur = best.get(key)
                # First writer wins (= newest, given the ORDER BY), but a
                # blob-less incumbent is upgraded to any copy that actually
                # has content: a pointer/card-only capture would otherwise
                # export as .MISSING.txt even though another snapshot holds
                # the resolved bytes.
                if cur is None or (not cur.blob_path and r.blob_path):
                    best[key] = r
            # Stable, created_at-independent output order.
            return sorted(
                best.values(),
                key=lambda r: (r.parent_external_id or "", r.external_id or ""),
            )

        async def _scoped_children(item_type: str) -> list:
            rows = list((await sess.execute(_child_query(item_type))).scalars())
            if not rows:
                return []
            if not msg_tenant_ids:
                # No resolved message carries a tenant_id (pointer rows do set
                # it, so this is a data anomaly). Keep the rows rather than
                # ship an export that quietly lost its attachments, and say so.
                log.warning(
                    "chat-export child rows could not be tenant-scoped: "
                    "item_type=%s rows=%d — no resolved message carries a "
                    "tenant_id, so every candidate row is being kept",
                    item_type, len(rows),
                )
                return _one_per_child(rows)
            fallback = await _tenant_by_snapshot({
                r.snapshot_id for r in rows
                if r.tenant_id is None and r.snapshot_id is not None
            })
            kept: list = []
            dropped: list = []
            for r in rows:
                tid = r.tenant_id if r.tenant_id is not None else fallback.get(r.snapshot_id)
                (kept if (tid is not None and tid in msg_tenant_ids) else dropped).append(r)
            if dropped:
                # Loud on purpose: this is the only signal that a row matching
                # a message in this thread is NOT in the ZIP. Expected when a
                # Teams message id collides across tenants; a surprise
                # otherwise, and the packager cannot emit a .MISSING.txt for
                # a child it never receives.
                log.warning(
                    "chat-export child rows excluded by tenant scope: "
                    "item_type=%s excluded=%d kept=%d export_tenants=%s "
                    "excluded_item_ids=%s — these rows matched a message id in "
                    "this thread but resolve to another tenant (or to no "
                    "tenant at all) and are NOT in the export",
                    item_type, len(dropped), len(kept),
                    sorted(str(t) for t in msg_tenant_ids),
                    [str(r.id) for r in dropped[:10]],
                )
            return _one_per_child(kept)

        for a in await _scoped_children("CHAT_ATTACHMENT"):
            ed = a.extra_data or {}
            att_map.setdefault(a.parent_external_id, []).append({
                "name": ed.get("name") or a.name or a.external_id,
                "blob_path": a.blob_path,
                "content_type": ed.get("content_type"),
                "size": a.content_size or 0,
            })
        for h in await _scoped_children("CHAT_HOSTED_CONTENT"):
            ed = h.extra_data or {}
            ctype = ed.get("content_type") or "application/octet-stream"
            ext = mimetypes.guess_extension(ctype) or ".bin"
            hc_id = h.external_id.split(":", 1)[1] if ":" in h.external_id else h.external_id
            hosted_map.setdefault(h.parent_external_id, []).append({
                "hc_id": hc_id,
                "blob_path": h.blob_path,
                "ext": ext,
                "content_type": ctype,
            })
    return ResolvedScope(layout=layout, thread_path=effective, messages=msgs,
                         attachment_map=att_map, hosted_map=hosted_map)
