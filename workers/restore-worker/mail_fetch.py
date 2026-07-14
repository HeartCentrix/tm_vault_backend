"""Shared helpers for fetching backed-up mail bodies + attachments from blob
storage.

Originally lived as private methods on ``FolderExportTask`` in
``mail_export.py`` (the MBOX/EML pipeline).  Extracted here so the new PST
writers (``pst_writers/mail.py``) can reuse the exact same fetch logic
without dragging in the whole folder-export task.

Both functions are kept dependency-free apart from ``shared.mime_builder`` —
the caller passes the shard, container, and (for attachments) the
``include_attachments`` toggle explicitly.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from typing import List, Optional, Sequence, Tuple, Union

ContainerCandidates = Union[str, Sequence[str]]


def _normalize_containers(source_container: ContainerCandidates,
                          extra: Optional[Sequence[str]] = None) -> Tuple[str, ...]:
    """Build an ordered, de-duplicated tuple of containers to try.

    ``source_container`` may be a single string (back-compat) or a sequence;
    ``extra`` (e.g. ``item._source_container_candidates``) is appended after
    the primary candidates so per-item dedup-relocations can override the
    orchestrator default. Empty entries are dropped.
    """
    out: List[str] = []
    seen = set()
    seq: Tuple[str, ...]
    if isinstance(source_container, str):
        seq = (source_container,) if source_container else ()
    else:
        seq = tuple(source_container or ())
    for c in seq:
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    if extra:
        for c in extra:
            if c and c not in seen:
                seen.add(c)
                out.append(c)
    return tuple(out)


async def _download_first_hit(shard, containers: Sequence[str], blob_path: str):
    """Try each container in order; return (bytes, container) on first hit
    or (None, None) on miss across all candidates. ``download_blob`` returns
    None for NoSuchKey/NoSuchBucket so callers can branch on data is None."""
    for c in containers:
        try:
            data = await shard.download_blob(c, blob_path)
        except Exception as exc:
            logger.debug("download miss container=%s path=%s: %s: %s",
                         c, blob_path, type(exc).__name__, exc)
            continue
        if data is not None:
            return data, c
    return None, None

# The restore-worker runtime mounts /app/shared, so `from shared.X import ...`
# works in production. Tests import via shim that preloads the module; we
# also allow repo-root imports when PYTHONPATH=.
_HERE = os.path.dirname(__file__)
_SHARED = os.path.abspath(os.path.join(_HERE, "..", "..", "shared"))
if os.path.isdir(_SHARED) and os.path.dirname(_SHARED) not in sys.path:
    sys.path.insert(0, os.path.dirname(_SHARED))

from shared.mime_builder import AttachmentRef  # noqa: E402

logger = logging.getLogger(__name__)


async def fetch_message(item, shard, source_container: ContainerCandidates) -> Optional[dict]:
    """Fetch raw Graph message JSON for a SnapshotItem.

    Resolution priority:

    1. ``item.extra_data["raw"]`` (or ``item.metadata["raw"]``) when the
       backup pipeline stored the message body inline on the row (Tier 2 —
       USER_MAIL).  No Azure round-trip.
    2. ``item.blob_path`` — download from ``shard``/``source_container``.
    3. Legacy fallback — list blobs under the snapshot prefix and match by
       external_id suffix.  Used for SnapshotItem rows created before
       ``blob_path`` was persisted.

    Returns ``None`` if all three fail (caller treats as a per-item failure).
    """
    # Fast path — Tier 2 USER_MAIL backup stores the Graph message JSON
    # directly on the SnapshotItem row in extra_data.raw; no Azure blob is
    # written for the message body (only attachments go to Azure). Use the
    # inline payload when it's present to skip Azure entirely.
    meta = getattr(item, "extra_data", None) or getattr(item, "metadata", None) or {}
    raw_inline = meta.get("raw") if isinstance(meta, dict) else None
    if raw_inline:
        ext_id = getattr(item, "external_id", "") or ""
        logger.debug(
            "fetch_message: using inline extra_data.raw ext_id=%s fields=%s",
            ext_id[:40],
            list(raw_inline.keys())[:8],
        )
        return raw_inline

    blob_path = getattr(item, "blob_path", None)

    # Container candidates: orchestrator-resolved primary + secondary list
    # (RESOURCE_TYPE_TO_WORKLOADS), plus any per-item fallback the call
    # site stamped (legacy `_mailbox_fallback_container` is still honored
    # but the canonical attribute is `_source_container_candidates`).
    item_extra = list(getattr(item, "_source_container_candidates", None) or ())
    legacy_fb = getattr(item, "_mailbox_fallback_container", None)
    if legacy_fb:
        item_extra.append(legacy_fb)
    container_candidates = _normalize_containers(source_container, item_extra)

    # Legacy fallback — some SnapshotItem rows were created before blob_path
    # was persisted. Reconstruct by listing blobs under the snapshot's prefix
    # and matching on external_id (the last path segment of the canonical
    # backup path: tenant/resource/snapshot/timestamp/external_id).
    resolved_container = container_candidates[0] if container_candidates else ""
    if not blob_path:
        ext_id = getattr(item, "external_id", "") or ""
        snap_id = str(getattr(item, "snapshot_id", "") or "")
        logger.debug(
            "fetch_message: blob_path=NULL, attempting list-and-match ext_id=%s",
            ext_id[:40],
        )

        matched = None
        matched_container = None
        for cand in container_candidates:
            try:
                async for name in shard.list_blobs(cand):
                    if snap_id and snap_id not in name:
                        continue
                    if name.endswith(f"/{ext_id}"):
                        matched = name
                        matched_container = cand
                        break
                if matched:
                    break
                logger.debug("fetch_message: no match in container=%s", cand)
            except Exception as exc:
                logger.debug(
                    "fetch_message: list container=%s failed: %s: %s",
                    cand,
                    type(exc).__name__,
                    exc,
                )

        if matched:
            blob_path = matched
            resolved_container = matched_container
            logger.debug(
                "fetch_message: recovered blob_path=%s container=%s",
                blob_path,
                resolved_container,
            )
        else:
            logger.debug(
                "fetch_message: no blob matched ext_id=%s under snapshot=%s",
                ext_id[:40],
                snap_id,
            )

    if not blob_path:
        ext_id = getattr(item, "external_id", "") or ""
        logger.debug(
            "fetch_message: blob_path resolution FAILED ext_id=%s",
            ext_id[:40],
        )
        return None

    logger.debug(
        "fetch_message: containers=%s path=%s",
        list(container_candidates),
        blob_path,
    )
    raw, hit_container = await _download_first_hit(shard, container_candidates, blob_path)
    if raw is None:
        logger.debug(
            "fetch_message: blob MISSING across all candidates path=%s",
            blob_path,
        )
        return None
    logger.debug(
        "fetch_message: blob fetched size=%d container=%s path=%s",
        len(raw), hit_container, blob_path,
    )
    return json.loads(raw.decode("utf-8"))


async def gather_item_attachments(
    email_att_rows: list,
    shard,
    source_container: ContainerCandidates,
    include_attachments: bool = True,
) -> List[AttachmentRef]:
    """Rebuild ``itemAttachment`` (mail-in-mail) rows as ``message/rfc822``
    :class:`AttachmentRef` objects so a nested email embeds natively in the
    parent's EML/MBOX.

    ``email_att_rows`` are the rich ``EMAIL_ATTACHMENT`` SnapshotItem-like
    siblings stamped on the parent EMAIL as ``_email_attachment_items``.  Only
    ``itemAttachment`` rows are handled here — ``fileAttachment`` keeps riding
    the existing ``attachment_blob_paths`` -> :func:`gather_attachments`
    channel unchanged, and ``referenceAttachment`` stays skipped.

    The nested email's stored JSON bytes come from ``blob_path`` (downloaded via
    the same ``_download_first_hit`` machinery used for file attachments) or,
    when the backup inlined a small nested message, from
    ``extra_data.inline_b64``.  A failure on any single row is logged and that
    row skipped — never crash the whole message/export.  Rows over
    ``PST_MAX_ATTACHMENT_BYTES`` are skipped to respect the memory cap.
    """
    import base64 as _b64
    import json as _json
    import os as _os

    from shared.mime_builder import build_nested_message_attachment

    if not include_attachments or not email_att_rows:
        return []

    max_bytes = int(
        _os.environ.get("PST_MAX_ATTACHMENT_BYTES", str(50 * 1024 * 1024))
    )
    out: List[AttachmentRef] = []
    for att in email_att_rows:
        ed = getattr(att, "extra_data", None) or {}
        kind = (ed.get("attachment_kind") or "").lower()
        if "itemattachment" not in kind:
            continue  # fileAttachment / referenceAttachment handled elsewhere
        name = getattr(att, "name", "?")
        try:
            blob_path = getattr(att, "blob_path", None)
            inline_b64 = ed.get("inline_b64")
            raw: Optional[bytes] = None
            if blob_path:
                # ponytail: uses the folder shard for nested-email blobs;
                # add per-att backend resolution if cross-backend toggles
                # ever split a nested-email blob from its parent message.
                item_extra = list(
                    getattr(att, "_source_container_candidates", None) or ()
                )
                legacy_fb = getattr(att, "_mailbox_fallback_container", None)
                if legacy_fb:
                    item_extra.append(legacy_fb)
                containers = _normalize_containers(source_container, item_extra)
                raw, _hit = await _download_first_hit(shard, containers, blob_path)
            elif inline_b64:
                raw = _b64.b64decode(inline_b64)
            if not raw:
                logger.warning(
                    "gather_item_attachments: no bytes for itemAttachment %s", name
                )
                continue
            if len(raw) > max_bytes:
                logger.warning(
                    "gather_item_attachments: skipping oversize itemAttachment "
                    "%s (%d bytes > %d cap)", name, len(raw), max_bytes,
                )
                continue
            nested = _json.loads(raw.decode("utf-8"))
            out.append(build_nested_message_attachment(nested))
        except Exception as exc:
            logger.warning(
                "gather_item_attachments: skipping itemAttachment %s: %s: %s",
                name, type(exc).__name__, exc,
            )
    return out


async def gather_attachments(
    att_paths: list,
    shard,
    source_container: ContainerCandidates,
    include_attachments: bool = True,
) -> List[AttachmentRef]:
    """Build streaming :class:`AttachmentRef` objects for each attachment path.

    Returns ``[]`` when ``include_attachments`` is False or ``att_paths`` is
    empty.  Each returned ref carries an async generator that lazily streams
    the bytes from ``shard.download_blob_stream`` on demand — no bytes are
    downloaded until the consumer iterates.

    Memory guard: if a single attachment exceeds ``PST_MAX_ATTACHMENT_BYTES``
    we replace it with a small text placeholder explaining the truncation,
    so a 100GB mailbox containing one 5GB attachment can't OOM the worker.
    """
    import os as _os
    max_bytes = int(
        _os.environ.get("PST_MAX_ATTACHMENT_BYTES", str(50 * 1024 * 1024))
    )
    if not include_attachments or not att_paths:
        return []
    container_candidates = _normalize_containers(source_container)
    primary_container = container_candidates[0] if container_candidates else ""
    out: List[AttachmentRef] = []
    for path in att_paths:
        # Probe size before streaming. If shard exposes blob metadata,
        # use it; otherwise fall through and let the streaming consumer
        # see the bytes (the mail writer base64-inlines them, so a single
        # huge attachment can balloon the JSON payload — but at least
        # the rest of the message processes).
        size = -1
        try:
            if hasattr(shard, "get_blob_size"):
                size = await shard.get_blob_size(primary_container, path)
            elif hasattr(shard, "get_blob_properties"):
                props = await shard.get_blob_properties(primary_container, path)
                size = (
                    getattr(props, "size", None)
                    or (props or {}).get("size", -1) if isinstance(props, dict)
                    else -1
                )
        except Exception:
            size = -1

        name = path.rsplit("/", 1)[-1]
        if size > max_bytes:
            placeholder = (
                f"[Attachment '{name}' was {size / 1024**2:.1f} MB, exceeds "
                f"PST_MAX_ATTACHMENT_BYTES ({max_bytes / 1024**2:.0f} MB). "
                f"Original blob path: {path}]"
            ).encode("utf-8")
            logger.warning(
                "gather_attachments: skipping oversize attachment %s (%d bytes)",
                name, size,
            )
            async def _placeholder_gen(data=placeholder):
                yield data
            out.append(AttachmentRef(
                name=f"{name}.SKIPPED.txt",
                content_type="text/plain",
                data_stream=_placeholder_gen(),
            ))
            continue

        # Stream the bytes from whichever candidate container has a
        # readable copy. We probe each container with a HEAD-style call
        # (download_blob with a 1-byte Range would be ideal, but the
        # shared shim doesn't expose it). Falling back to download_blob
        # one-shot is acceptable — shard.download_blob returns None on
        # NoSuchKey / NoSuchBucket without throwing.
        async def _gen(p=path, candidates=container_candidates):
            for cand in candidates:
                try:
                    async for chunk in shard.download_blob_stream(cand, p):
                        yield chunk
                    # If we got at least one chunk we're done; the
                    # generator naturally returns when the iterator
                    # exhausts.
                    return
                except Exception as exc:
                    logger.debug(
                        "attachment stream miss container=%s path=%s: %s: %s",
                        cand, p, type(exc).__name__, exc,
                    )
            # All candidates failed — yield nothing so the writer sees an
            # empty stream rather than crashing.
            return
        out.append(AttachmentRef(
            name=name,
            content_type="application/octet-stream",
            data_stream=_gen(),
        ))
    return out
