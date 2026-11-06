"""Move a mail carry-forward prior's inline attachment bytes to blob storage, in place.

Attachments <=256 KB used to be stored in ``snapshot_items.metadata.inline_b64``, and every changed
mailbox's carry-forward copied every prior row, bytes included (Railway 2026-11-06 incremental #1:
Rohit 7,085 rows, 247 MB inline, 245 s of copy). Before a carry copies its prior, that prior's
inline EMAIL_ATTACHMENT rows are uploaded with the capture path's content-hash dedup and rewritten
to exactly what a blob-backed attachment row holds; every other metadata key is kept. ``content_size``,
``external_id`` and ``created_at`` never change, so snapshot totals and the protected-size ledger
(identity = external_id, size = content_size) are untouched.

Bounded batches, each its own transaction. Idempotent and resumable: a row is rewritten only while it
is still inline, a failed upload leaves it inline (the copy still carries it, as before) and the next
carry retries it. Racing runs agree: the blob path is a function of the prior snapshot and the content
hash, and rows are locked in id order before the conditional update.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os

from sqlalchemy import text

from shared.mail_snapshot_reuse import MAIL_PRIOR_SNAPSHOT_SQL

_INLINE = "COALESCE(si.metadata->>'inline_b64', '') <> ''"


async def externalize_prior_mail_attachments(factory, *, tenant_id, resource_id, target_snapshot_id,
                                             current_job_id, shard, container, batch_rows=32) -> int:
    """Externalize the inline attachments of the snapshot the mail carry will copy into
    ``target_snapshot_id`` (MAIL_PRIOR_SNAPSHOT_SQL, the carry's own source). Returns rows moved."""
    from shared.azure_storage import azure_storage_manager, upload_blob_with_dedup

    # Rows at or under MAIL_ATTACHMENT_INLINE_MAX_BYTES are what the operator chose to keep inline.
    keep = int(os.getenv("MAIL_ATTACHMENT_INLINE_MAX_BYTES", "0"))
    dedup_min = int(os.getenv("BLOB_DEDUP_MIN_SIZE_BYTES", "1024"))
    async with factory() as session:
        prior = (await session.execute(text(
            "SELECT p.id, p.created_at FROM snapshots p WHERE p.id = " + MAIL_PRIOR_SNAPSHOT_SQL),
            {"rid": str(resource_id), "tsid": str(target_snapshot_id),
             "jid": str(current_job_id) if current_job_id else None})).first()
        if prior is None:
            return 0
        # Inline rows have no blob_path; this never reads (detoasts) a blob row's metadata, so an
        # already-externalized prior costs one index scan. Each batch re-checks inline_b64 itself.
        ids = [row[0] for row in await session.execute(text(
            "SELECT si.id FROM snapshot_items si WHERE si.snapshot_id = :sid "
            "AND si.item_type = 'EMAIL_ATTACHMENT' AND si.blob_path IS NULL "
            "AND COALESCE(si.content_size, 0) > :keep ORDER BY si.id"), {"sid": prior.id, "keep": keep})]
    stamp = prior.created_at.strftime("%Y%m%dT%H%M%S")
    paths: dict = {}  # content hash -> blob path, or None when its upload failed this run

    async def upload(digest, data):
        path = azure_storage_manager.build_blob_path(
            str(tenant_id), str(resource_id), str(prior.id), f"att_{digest[:40]}", timestamp=stamp)
        try:
            result = await upload_blob_with_dedup(tenant_id, digest, container, path, data, shard,
                                                  max_retries=3, min_size_bytes=dedup_min)
        except Exception as exc:
            print(f"[MAIL-EXTERNALIZE] upload {digest[:12]} failed: {type(exc).__name__}: {exc}")
            result = None
        paths[digest] = (result or {}).get("blob_path", path) if (result or {}).get("success") else None

    moved = 0
    for start in range(0, len(ids), batch_rows):
        async with factory() as session:
            rows = (await session.execute(text(
                "SELECT si.id, si.metadata->>'inline_b64' FROM snapshot_items si "
                "WHERE si.snapshot_id = :sid AND si.id = ANY(CAST(:ids AS uuid[])) AND " + _INLINE),
                {"sid": prior.id, "ids": [str(i) for i in ids[start:start + batch_rows]]})).all()
        contents = {}
        for item_id, encoded in rows:
            try:
                data = base64.b64decode(encoded)
            except Exception:
                continue  # left inline; the copy carries it as before
            contents[item_id] = (hashlib.sha256(data).hexdigest(), data)
        pending = {digest: data for digest, data in contents.values() if digest not in paths}
        await asyncio.gather(*(upload(digest, data) for digest, data in pending.items()))
        done = [{"id": str(item_id), "blob_path": paths[digest], "sha": digest}
                for item_id, (digest, _) in contents.items() if paths.get(digest)]
        outage = bool(pending) and not any(paths[digest] for digest in pending)
        if done:
            moved += await _rewrite(factory, prior.id, done, shard)
        if outage:
            print(f"[MAIL-EXTERNALIZE] snapshot {prior.id}: every upload in a batch failed; "
                  f"stopping, the next carry resumes")
            break
    return moved


async def _rewrite(factory, snapshot_id, done, shard) -> int:
    """Point still-inline rows at their blob, as a capture-time blob attachment row looks."""
    async with factory.begin() as session:
        result = await session.execute(text(
            "WITH locked AS (SELECT si.id FROM snapshot_items si WHERE si.snapshot_id = :sid "
            "  AND si.id = ANY(CAST(:ids AS uuid[])) AND " + _INLINE + " ORDER BY si.id FOR UPDATE) "
            "UPDATE snapshot_items si SET blob_path = x.blob_path, content_hash = x.sha, "
            "  content_checksum = x.sha, encryption_key_id = NULL, "
            "  backend_id = COALESCE(CAST(:backend AS uuid), si.backend_id), "
            "  metadata = (si.metadata::jsonb - 'inline_b64')::json "
            "FROM locked, jsonb_to_recordset(CAST(:rows AS jsonb)) AS x(id uuid, blob_path text, sha text) "
            "WHERE si.snapshot_id = :sid AND si.id = locked.id AND si.id = x.id AND " + _INLINE),
            {"sid": snapshot_id, "ids": [row["id"] for row in done], "rows": json.dumps(done),
             "backend": str(shard.backend_id) if getattr(shard, "backend_id", None) else None})
        return result.rowcount or 0
