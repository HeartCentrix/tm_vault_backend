"""Cross-mailbox mail attachment reuse.

One email delivered to N mailboxes (To/CC/distribution list) used to pull every attachment's
bytes from Graph N times; blob storage then deduped them by content hash. A copy whose
attachments the same tenant already stored (same internetMessageId, name, Graph size, content
type and inline flag, blob still readable in this writer's container) now lists attachment
metadata only and points its own rows at the stored blob.

Two workers on the same message at once may both download: correct, just no saving.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import text

from shared.database import async_session_factory

log = logging.getLogger(__name__)

INDEX_NAME = "ix_snapshot_items_mail_att_imid"
# Same DDL as alembic 0023 and scripts/2026_11_06_mail_attachment_reuse.sql (CONCURRENTLY there).
INDEX_SQL = (f"CREATE INDEX IF NOT EXISTS {INDEX_NAME} ON snapshot_items "
             "(tenant_id, (metadata->>'internet_message_id')) WHERE item_type = 'EMAIL_ATTACHMENT'")
# Without contentBytes; contentId (fileAttachment-only) is copied from the stored row.
METADATA_SELECT = "id,name,contentType,size,isInline"

_CANDIDATES_SQL = text(f"""
    SELECT DISTINCT metadata->>'internet_message_id' AS imid, name,
           metadata->>'graph_size' AS graph_size, metadata->>'content_type' AS content_type,
           metadata->>'is_inline' AS is_inline, metadata->>'content_id' AS content_id,
           blob_path, content_hash, content_size
      FROM snapshot_items
     WHERE item_type = 'EMAIL_ATTACHMENT' AND tenant_id = :tenant
       AND metadata->>'internet_message_id' = ANY(:imids)
       AND metadata->>'attachment_kind' LIKE '%fileAttachment'
       AND blob_path IS NOT NULL AND content_hash IS NOT NULL
""")
# ponytail: reads every stored copy of a message (one row per mailbox snapshot that holds it);
# an all-company mail in 5k mailboxes reads 5k index entries. Bound per imid with a LATERAL
# LIMIT if the lookup shows up in worker timings.
_CHUNK = 500
_index_ready: Optional[bool] = None


def reuse_enabled() -> bool:
    return os.getenv("MAIL_ATTACHMENT_CROSS_MAILBOX_REUSE", "true").strip().lower() not in ("0", "false", "no", "off")


def _key(name, graph_size, content_type, is_inline) -> tuple:
    return (name, str(graph_size), content_type, str(is_inline).lower() == "true")


async def load_candidates(tenant_id, internet_message_ids: Iterable[Optional[str]]) -> Dict[str, List[Any]]:
    """Stored file attachments of this tenant, by internetMessageId. Empty without the index:
    an unindexed JSON filter over a tenant's snapshot_items would cost more than it saves."""
    global _index_ready
    imids = sorted({i for i in internet_message_ids if i})
    if not imids:
        return {}
    out: Dict[str, List[Any]] = {}
    try:
        async with async_session_factory() as session:
            if _index_ready is None:
                # ponytail: checked once per process; a worker started before the index was built
                # skips reuse until its next restart.
                _index_ready = bool(await session.scalar(text(
                    "SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass(:n)"), {"n": INDEX_NAME}))
            if not _index_ready:
                return {}
            for start in range(0, len(imids), _CHUNK):
                rows = await session.execute(_CANDIDATES_SQL, {"tenant": tenant_id, "imids": imids[start:start + _CHUNK]})
                for row in rows:
                    out.setdefault(row.imid, []).append(row)
    except Exception as exc:  # advisory: a lookup failure only means this copy downloads
        log.warning("mail attachment reuse lookup failed: %s", type(exc).__name__)
        return {}
    return out


def match(attachments: List[Dict[str, Any]], candidates: List[Any]) -> Optional[Dict[str, Any]]:
    """{attachment id: stored row} when EVERY listed attachment is a file attachment matching
    exactly one stored content; otherwise None and the message is downloaded as before."""
    by_key: Dict[tuple, Dict[str, Any]] = {}
    for c in candidates:
        by_key.setdefault(_key(c.name, c.graph_size, c.content_type, c.is_inline), {})[c.content_hash] = c
    plan = {}
    for att in attachments:
        if not str(att.get("@odata.type", "")).endswith("fileAttachment") or not att.get("id") or not att.get("size"):
            return None
        hits = by_key.get(_key((att.get("name") or att["id"])[:255], att["size"], att.get("contentType"),
                               att.get("isInline", False)))
        if not hits or len(hits) != 1:
            return None
        plan[att["id"]] = next(iter(hits.values()))
    return plan or None


async def blob_readable(shard, container: str, path: str, cache: Dict[str, bool]) -> bool:
    """Same probe as upload_blob_with_dedup: the blob must be readable from THIS writer's
    container (same tenant container, so the same encryption scope / immutability policy)."""
    if path not in cache:
        try:
            props = await shard.get_blob_properties(container, path)
            size = None if props is None else (props.get("size") if isinstance(props, dict) else getattr(props, "size", None))
            cache[path] = bool(size and size > 0)
        except Exception:
            cache[path] = False
    return cache[path]
