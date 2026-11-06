"""Cross-mailbox mail attachment reuse.

One email delivered to N mailboxes (To/CC/distribution list) used to pull every attachment's
bytes from Graph N times; blob storage then deduped them by content hash. The first copy that
downloads an attachment records it in mail_attachment_contents (one row per distinct content);
later copies of the same message (same internetMessageId, name, Graph size, content type,
inline flag; blob still readable in this writer's container and backend) list attachment
metadata only and point their own rows at the stored blob.

The lookup is a primary-key probe, O(distinct attachments); snapshot_items is never read.
Two workers on the same message at once may both download: correct, just no saving.
A row whose blob was GC'd fails the HEAD probe, is deleted, and the copy downloads.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import text

from shared.database import async_session_factory

log = logging.getLogger(__name__)

# Without contentBytes; contentId (fileAttachment-only) is copied from the stored row.
METADATA_SELECT = "id,name,contentType,size,isInline"

_CANDIDATES_SQL = text("""
    SELECT internet_message_id AS imid, name, graph_size, content_type, is_inline, content_id,
           blob_path, content_hash, content_size, backend_id
      FROM mail_attachment_contents
     WHERE tenant_id = :tenant AND internet_message_id = ANY(:imids)
""")
_INSERT_SQL = text("""
    INSERT INTO mail_attachment_contents (tenant_id, internet_message_id, name, graph_size,
        content_type, is_inline, content_id, blob_path, content_hash, content_size, backend_id)
    SELECT :tenant, r.imid, r.name, r.graph_size, r.content_type, r.is_inline, r.content_id,
           r.blob_path, r.content_hash, r.content_size, CAST(:backend AS uuid)
      FROM jsonb_to_recordset(CAST(:rows AS jsonb)) AS r(imid text, name text, graph_size bigint,
           content_type text, is_inline boolean, content_id text, blob_path text,
           content_hash text, content_size bigint)
    ON CONFLICT DO NOTHING
""")
_DELETE_SQL = text("""
    DELETE FROM mail_attachment_contents
     WHERE tenant_id = :tenant AND internet_message_id = :imid AND name = :name
       AND graph_size = :graph_size AND content_type = :content_type AND is_inline = :is_inline
       AND blob_path = :blob_path
""")
_CHUNK = 500
_table_ready: Optional[bool] = None


def reuse_enabled() -> bool:
    return os.getenv("MAIL_ATTACHMENT_CROSS_MAILBOX_REUSE", "true").strip().lower() not in ("0", "false", "no", "off")


def key(att: Dict[str, Any]) -> Optional[tuple]:
    """Primary-key tail for a Graph file attachment; None when it cannot be keyed."""
    if not str(att.get("@odata.type", "")).endswith("fileAttachment") or not att.get("id") or not att.get("size"):
        return None
    return ((att.get("name") or att["id"])[:255], int(att["size"]), att.get("contentType") or "",
            bool(att.get("isInline", False)))


async def _ready(session) -> bool:
    global _table_ready
    if _table_ready is None:
        # ponytail: checked once per process; a worker started before the table existed skips
        # reuse until its next restart.
        _table_ready = bool(await session.scalar(text("SELECT to_regclass('mail_attachment_contents') IS NOT NULL")))
    return _table_ready


async def load_candidates(tenant_id, internet_message_ids: Iterable[Optional[str]]) -> Dict[str, List[Any]]:
    """Stored contents of this tenant, by internetMessageId. Advisory: {} on any error."""
    imids = sorted({i for i in internet_message_ids if i})
    if not imids:
        return {}
    out: Dict[str, List[Any]] = {}
    try:
        async with async_session_factory() as session:
            if not await _ready(session):
                return {}
            for start in range(0, len(imids), _CHUNK):
                rows = await session.execute(_CANDIDATES_SQL, {"tenant": tenant_id, "imids": imids[start:start + _CHUNK]})
                for row in rows:
                    out.setdefault(row.imid, []).append(row)
    except Exception as exc:  # a lookup failure only means this copy downloads
        log.warning("mail attachment reuse lookup failed: %s", type(exc).__name__)
        return {}
    return out


def match(attachments: List[Dict[str, Any]], candidates: List[Any]) -> Optional[Dict[str, Any]]:
    """{attachment id: stored row} when EVERY listed attachment is a file attachment with a
    stored content and no two share a key; otherwise None and the message downloads as before."""
    stored = {(c.name, int(c.graph_size), c.content_type, bool(c.is_inline)): c for c in candidates}
    keys = [key(a) for a in attachments]
    if not keys or None in keys or len(set(keys)) != len(keys):
        return None
    plan = {a["id"]: stored.get(k) for a, k in zip(attachments, keys)}
    return None if None in plan.values() else plan


async def usable(shard, container: str, tenant_id, imid: str, row, cache: Dict[str, bool]) -> bool:
    """Same probe as upload_blob_with_dedup: readable from THIS writer's container (same tenant
    container, so same encryption scope / immutability policy) on the same backend. A missing
    blob deletes its content row so the next copy re-records it."""
    backend = getattr(shard, "backend_id", None)
    if row.backend_id and backend and str(row.backend_id) != str(backend):
        return False
    path = row.blob_path
    if path not in cache:
        try:
            props = await shard.get_blob_properties(container, path)
            size = None if props is None else (props.get("size") if isinstance(props, dict) else getattr(props, "size", None))
            cache[path] = bool(size and size > 0)
        except Exception:
            return False  # probe error is not proof the blob is gone: just don't reuse
        if not cache[path]:
            try:
                async with async_session_factory() as session:
                    await session.execute(_DELETE_SQL, {"tenant": tenant_id, "imid": imid, "name": row.name,
                        "graph_size": row.graph_size, "content_type": row.content_type,
                        "is_inline": row.is_inline, "blob_path": path})
                    await session.commit()
            except Exception as exc:
                log.warning("mail attachment content delete failed: %s", type(exc).__name__)
    return cache[path]


async def remember(tenant_id, backend_id, rows: List[Dict[str, Any]]) -> None:
    """Record first-downloaded contents: one multi-row INSERT .. ON CONFLICT DO NOTHING."""
    if not rows:
        return
    import json
    try:
        async with async_session_factory() as session:
            if not await _ready(session):
                return
            await session.execute(_INSERT_SQL, {"tenant": tenant_id, "rows": json.dumps(rows),
                                                "backend": str(backend_id) if backend_id else None})
            await session.commit()
    except Exception as exc:
        log.warning("mail attachment content insert failed: %s", type(exc).__name__)
