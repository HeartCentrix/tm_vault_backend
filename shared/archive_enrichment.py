"""Archive PREVIEW-ENRICHMENT engine — the per-resource unit of work, DI-shaped.

Backup stores archive items as OPAQUE MAPI blobs (see shared/archive_export.py);
they can't be parsed, so to make an item previewable we STAGE it: import the
blob into a temp folder of the user's own PRIMARY mailbox, read it back as normal
Graph JSON, write that JSON onto the source ``ARCHIVE_ITEM`` row, delete the temp
copy. This module owns the *orchestration* of that for ONE resource; the pure
transform lives in ``archive_enrich`` and the staging loop in ``archive_export``.

``enrich_resource`` is the single unit reused by BOTH callers in the enrichment
service: the on-demand consumer (a user opened their archive → do theirs first)
and the bulk-parallel loop (background sweep across every user). It's idempotent
and incremental — only rows that still ``needs_enrichment`` are staged, so
re-firing is safe and cheap.

The impure edges (row load, staging construction, DB write, mailbox resolution)
are injectable so the orchestration is unit-tested with fakes. The two SQL
helpers are DB-bound and proven by the live round-trip.
"""
from __future__ import annotations

import base64 as _b64
import hashlib as _hashlib
import uuid as _uuid
from typing import Any, Awaitable, Callable, Dict, List, Optional

from sqlalchemy import select, or_, delete as _sa_delete

from shared.models import Snapshot, SnapshotItem
from shared import archive_enrich as ae
from shared import archive_graph
from shared.archive_export import (
    ArchiveExportStager,
    LiveStagingOps,
    post_json_no_auth,
)

ARCHIVE_ITEM_TYPE = "ARCHIVE_ITEM"
ARCHIVE_ATTACHMENT_TYPE = "ARCHIVE_ATTACHMENT"
ARCHIVE_WORKLOAD = "archive"


def plan_archive_attachments(attachments) -> List[Dict[str, Any]]:
    """Decode + hash the fileAttachments that carry ``contentBytes``. Pure — no
    DB, no storage, no network. These become sibling ARCHIVE_ATTACHMENT rows
    (mirroring EMAIL_ATTACHMENT) so export can inline the real bytes via the
    unchanged mail attachment path. itemAttachment (nested email → carried in
    ``item``, not contentBytes) and referenceAttachment (cloud link, no bytes)
    are skipped — their content rides inside the lossless opaque MAPI blob."""
    out: List[Dict[str, Any]] = []
    for a in (attachments or []):
        if not isinstance(a, dict):
            continue
        cb = a.get("contentBytes")
        if not cb:
            continue
        try:
            data = _b64.b64decode(cb, validate=True)
        except Exception:
            continue
        if not data:
            continue
        out.append({
            "att_id": a.get("id") or _hashlib.sha256((a.get("name") or "att").encode()).hexdigest()[:16],
            "name": a.get("name") or "attachment",
            "content_type": a.get("contentType"),
            "is_inline": bool(a.get("isInline")),
            "content_id": a.get("contentId"),
            "data": data,
            "content_hash": _hashlib.sha256(data).hexdigest(),
        })
    return out


def delete_archive_attachments_stmt(snapshot_id, parent_external_id):
    """Idempotency: drop any prior ARCHIVE_ATTACHMENT siblings of this parent
    before re-inserting, so a forced re-enrich never duplicates them. Parent
    link lives in ``extra_data.parent_item_id`` (generic JSON column 'metadata';
    ``.as_string()`` emits ``->>`` — NOT ``.astext``, which JSON lacks)."""
    return _sa_delete(SnapshotItem).where(
        SnapshotItem.snapshot_id == snapshot_id,
        SnapshotItem.item_type == ARCHIVE_ATTACHMENT_TYPE,
        SnapshotItem.extra_data["parent_item_id"].as_string() == str(parent_external_id),
    )


# ── DB helpers (Postgres; statement builders are compile-tested, no DB) ────
def pending_archive_rows_stmt(resource_id):
    """The CURRENT set of ARCHIVE_ITEM rows for one resource — newest row per
    ``external_id`` (delta backups re-emit a changed item on a later snapshot;
    we want its latest captured blob). Enrich-status is filtered in Python by
    the caller via ``needs_enrichment`` so the SQL stays a plain newest-wins
    dedupe. Excludes tombstones."""
    return (
        select(SnapshotItem)
        .join(Snapshot, SnapshotItem.snapshot_id == Snapshot.id)
        .where(
            Snapshot.resource_id == resource_id,
            SnapshotItem.item_type == ARCHIVE_ITEM_TYPE,
            SnapshotItem.is_deleted.is_(False),
        )
        # DISTINCT ON (external_id) keeping the row from the newest snapshot.
        .order_by(SnapshotItem.external_id, Snapshot.created_at.desc())
        .distinct(SnapshotItem.external_id)
    )


def find_pending_archive_resource_ids_stmt(limit: int = 50):
    """Resource ids that have at least one ARCHIVE_ITEM row still needing
    enrichment — the work-list for the bulk sweep. A row is pending when its
    ``enrich_status`` is NULL/absent or FAILED (retry); ENRICHED/PROCESSING are
    excluded.

    ``extra_data`` is a generic ``JSON`` column (not JSONB), so ``.astext`` is
    unavailable — ``.as_string()`` is the portable accessor and emits ``->>``
    (quote-stripped text) so the equality against 'enriched'/'processing' is
    correct (``cast(-> , str)`` would compare against '"enriched"' and never
    match). ponytail: filters on ``metadata->>'enrich_status'`` with no index —
    fine at demo scale; add a partial expression index if the sweep gets hot."""
    enrich_status = SnapshotItem.extra_data["enrich_status"].as_string()
    return (
        select(Snapshot.resource_id)
        .join(SnapshotItem, SnapshotItem.snapshot_id == Snapshot.id)
        .where(
            SnapshotItem.item_type == ARCHIVE_ITEM_TYPE,
            SnapshotItem.is_deleted.is_(False),
            or_(
                enrich_status.is_(None),
                enrich_status.notin_([ae.ENRICHED, ae.PROCESSING]),
            ),
        )
        .distinct()
        .limit(limit)
    )


async def pending_archive_rows(session, resource_id) -> List[SnapshotItem]:
    return list((await session.execute(pending_archive_rows_stmt(resource_id))).scalars().all())


async def find_pending_archive_resource_ids(session, limit: int = 50) -> List[Any]:
    rows = (await session.execute(find_pending_archive_resource_ids_stmt(limit))).all()
    return [row[0] for row in rows]


# ── per-resource orchestration ────────────────────────────────────────────
def _build_stager(*, gc, user_id, primary_id, folder_name, download_blob,
                  batch_size, worker_id, post_json):
    ops = LiveStagingOps(
        gc, user_id=user_id, primary_mailbox_id=primary_id,
        folder_name=folder_name, post_json=post_json,
    )
    return ArchiveExportStager(
        ops=ops, download_blob=download_blob,
        batch_size=batch_size, worker_id=worker_id,
    )


async def enrich_resource(
    *,
    session_factory,
    resource,
    gc,
    download_blob: Callable[[Any], Awaitable[Optional[bytes]]],
    upload_attachment: Optional[Callable[..., Awaitable[Any]]] = None,
    batch_size: int = 50,
    worker_id: str = "enrich",
    resolve_primary_mailbox_id: Optional[Callable[..., Awaitable[Optional[str]]]] = None,
    post_json=None,
    _load_pending: Optional[Callable[..., Awaitable[List[Any]]]] = None,
    _stager_factory: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    """Enrich ONE USER_ARCHIVE resource. Returns ``{"enriched": n, "pending": m}``
    (plus ``"skipped"`` when the mailbox can't be resolved). Never raises for the
    ordinary "no archive / no permission / nothing to do" cases — those skip
    gracefully so the bulk loop keeps going."""
    load = _load_pending or pending_archive_rows
    resolve = resolve_primary_mailbox_id or archive_graph.get_primary_mailbox_id
    post = post_json or post_json_no_auth

    # ── rows still needing enrichment (detached; loaded columns stay readable) ──
    async with session_factory() as session:
        rows = await load(session, resource.id)
        try:
            session.expunge_all()
        except Exception:  # noqa: BLE001 - fakes/detached sessions
            pass
    pending = [
        r for r in rows
        if ae.needs_enrichment(getattr(r, "extra_data", None) or {})
    ]
    if not pending:
        return {"enriched": 0, "pending": 0}

    # ── resolve the user's PRIMARY mailbox (the only one we can stage into) ──
    meta = dict(getattr(resource, "extra_data", None) or {})
    user_id = meta.get("user_id")
    primary_id = meta.get("primary_mailbox_id")
    if not primary_id and user_id:
        primary_id = await resolve(gc, user_id)
    if not (user_id and primary_id):
        return {"enriched": 0, "pending": len(pending), "skipped": "no_primary_mailbox"}

    folder_name = f"_TMvault_enrich_{str(resource.id)[:8]}_{_uuid.uuid4().hex[:6]}"
    factory = _stager_factory or _build_stager
    stager = factory(
        gc=gc, user_id=user_id, primary_id=primary_id, folder_name=folder_name,
        download_blob=download_blob, batch_size=batch_size,
        worker_id=worker_id, post_json=post,
    )

    async def _apply(row, name, patch):
        # Attachments follow the MAILBOX model: the row stays lean (contentBytes
        # stripped) while each fileAttachment's real bytes are stored as their
        # own blob + a sibling ARCHIVE_ATTACHMENT row (like EMAIL_ATTACHMENT), so
        # export inlines them through the unchanged mail attachment path. Bytes
        # are still live in ``patch.raw.attachments[i].contentBytes`` here, just
        # before the strip — decode + upload them now.
        raw = patch.get("raw") or {}
        plans = plan_archive_attachments(raw.get("attachments"))
        async with session_factory() as s:
            db_row = await s.get(SnapshotItem, row.id)
            if db_row is None:
                return
            if plans and upload_attachment is not None:
                from shared.azure_storage import azure_storage_manager
                # Idempotent on forced re-enrich: drop prior siblings first.
                try:
                    await s.execute(delete_archive_attachments_stmt(db_row.snapshot_id, db_row.external_id))
                except Exception as e:  # noqa: BLE001 - fakes without execute()
                    print(f"[{worker_id}] archive attachment cleanup skipped: {type(e).__name__}")
                container = azure_storage_manager.get_container_name(str(db_row.tenant_id), ARCHIVE_WORKLOAD)
                for p in plans:
                    blob_path = azure_storage_manager.build_blob_path(
                        str(db_row.tenant_id), str(resource.id), str(db_row.snapshot_id),
                        f"{db_row.external_id}::{p['att_id']}",
                    )
                    up = await upload_attachment(db_row.tenant_id, p["content_hash"], container, blob_path, p["data"])
                    # Dedup-aware: on a dedup_hit the uploader writes NOTHING new
                    # and returns the EXISTING blob's path (different from the one
                    # we built). Store the path the bytes ACTUALLY live at, exactly
                    # like EMAIL_ATTACHMENT (workers/backup-worker/main.py:9328) —
                    # else the row points at a never-written key and /content
                    # returns source:none.
                    if isinstance(up, dict) and up.get("blob_path"):
                        blob_path = up["blob_path"]
                    # TEMP DIAGNOSTIC (remove after root-cause): reveal exactly
                    # what the uploader did — dedup_hit vs real upload vs failure
                    # — and the container + stored path, so a source:none read can
                    # be traced to write-side vs read-side.
                    try:
                        _m = up.get("method") if isinstance(up, dict) else None
                        _ok = up.get("success") if isinstance(up, dict) else None
                        _err = up.get("error") if isinstance(up, dict) else None
                        print(f"[{worker_id}] ARCH_ATT_DIAG att={p['att_id'][:16]} bytes={len(p['data'])} "
                              f"container={container} method={_m} success={_ok} err={_err} "
                              f"stored_tail=...{str(blob_path)[-46:]}", flush=True)
                    except Exception:
                        pass
                    s.add(SnapshotItem(
                        id=_uuid.uuid4(),
                        snapshot_id=db_row.snapshot_id,
                        tenant_id=db_row.tenant_id,
                        external_id=f"{db_row.external_id}::{p['att_id']}",
                        parent_external_id=db_row.external_id,
                        item_type=ARCHIVE_ATTACHMENT_TYPE,
                        name=p["name"],
                        folder_path=db_row.folder_path,
                        content_hash=p["content_hash"],
                        content_checksum=p["content_hash"],
                        content_size=len(p["data"]),
                        blob_path=blob_path,
                        backend_id=db_row.backend_id,
                        extra_data={
                            "parent_item_id": db_row.external_id,
                            "attachment_kind": "fileAttachment",
                            "content_type": p["content_type"],
                            "is_inline": p["is_inline"],
                            "content_id": p["content_id"],
                        },
                    ))
            # Keep the persisted raw lean — the heavy bytes now live in blobs.
            for a in (raw.get("attachments") or []):
                if isinstance(a, dict):
                    a.pop("contentBytes", None)
            ed = dict(db_row.extra_data or {})
            ed.update(patch)
            db_row.extra_data = ed
            db_row.name = name
            await s.commit()

    n = await ae.enrich_items(stager, pending, _apply)
    return {"enriched": n, "pending": len(pending)}
