"""Bounded-width selection for legacy chat attachment inventories."""
from sqlalchemy import text


class LegacyChatProvenanceGap(RuntimeError):
    """A legacy child cannot safely be attributed to the requested chat."""


# Resolve payload provenance only for the newest candidate of each key. Older
# candidates are inspected only when the newer row belongs to another chat.
_SOURCE_CHAT = """COALESCE(
    NULLIF(src.metadata->>'chat_id', ''), NULLIF(src.metadata->>'chatId', ''),
    (SELECT CASE WHEN min(msg.parent_external_id) = max(msg.parent_external_id)
                 THEN min(msg.parent_external_id) END
       FROM snapshot_items msg
      WHERE msg.snapshot_id = r.snapshot_id AND msg.external_id = r.message_id
        AND msg.item_type IN ('TEAMS_CHAT_MESSAGE', 'TEAMS_MESSAGE', 'TEAMS_MESSAGE_REPLY'))
)"""

LEGACY_CHAT_ATTACHMENT_COPY_SQL = f"""
WITH RECURSIVE target_scope AS MATERIALIZED (
    SELECT s.id FROM snapshots s JOIN resources r ON r.id = s.resource_id
     WHERE s.id = :tsid AND r.tenant_id = :tid
), message_ids AS MATERIALIZED (
    SELECT DISTINCT m.message_external_id
      FROM chat_threads t JOIN chat_thread_messages m ON m.chat_thread_id = t.id
     WHERE t.tenant_id = :tid AND t.chat_id = :cid
), candidates AS NOT MATERIALIZED (
    SELECT src.id, src.snapshot_id, src.external_id, src.item_type,
           src.parent_external_id AS message_id, src.created_at
      FROM message_ids mids
      JOIN snapshot_items src ON src.parent_external_id = mids.message_external_id
      JOIN snapshots owner_s ON owner_s.id = src.snapshot_id
      JOIN resources owner_r ON owner_r.id = owner_s.resource_id
     WHERE owner_r.tenant_id = :tid
       AND (src.tenant_id = :tid OR src.tenant_id IS NULL)
       AND src.snapshot_id <> :tsid
       AND src.item_type IN ('CHAT_ATTACHMENT', 'CHAT_HOSTED_CONTENT')
       AND EXISTS (SELECT 1 FROM target_scope)
       AND NOT EXISTS (
           SELECT 1 FROM snapshot_items dst
            WHERE dst.snapshot_id = :tsid AND dst.external_id = src.external_id
              AND dst.item_type = src.item_type
       )
    UNION ALL
    SELECT src.id, src.snapshot_id, src.external_id, src.item_type,
           split_part(src.external_id, '::', 1) AS message_id, src.created_at
      FROM message_ids mids
      JOIN snapshot_items src ON split_part(src.external_id, '::', 1) = mids.message_external_id
      JOIN snapshots owner_s ON owner_s.id = src.snapshot_id
      JOIN resources owner_r ON owner_r.id = owner_s.resource_id
     WHERE owner_r.tenant_id = :tid
       AND (src.tenant_id = :tid OR src.tenant_id IS NULL)
       AND src.snapshot_id <> :tsid
       AND src.item_type IN ('CHAT_ATTACHMENT', 'CHAT_HOSTED_CONTENT')
       AND src.parent_external_id IS NULL
       AND EXISTS (SELECT 1 FROM target_scope)
       AND NOT EXISTS (
           SELECT 1 FROM snapshot_items dst
            WHERE dst.snapshot_id = :tsid AND dst.external_id = src.external_id
              AND dst.item_type = src.item_type
       )
), ranked AS MATERIALIZED (
    SELECT c.*, row_number() OVER (
        PARTITION BY external_id, item_type
        ORDER BY created_at DESC NULLS LAST, id DESC
    ) AS candidate_rank
      FROM candidates c
), heads AS MATERIALIZED (
    SELECT r.*, {_SOURCE_CHAT} AS source_chat_id
      FROM ranked r JOIN snapshot_items src ON src.id = r.id
     WHERE r.candidate_rank = 1
), fallback AS (
    SELECT * FROM heads WHERE source_chat_id IS NOT NULL AND source_chat_id <> :cid
    UNION ALL
    SELECT r.*, {_SOURCE_CHAT} AS source_chat_id
      FROM fallback previous
      JOIN ranked r ON r.external_id = previous.external_id
                   AND r.item_type = previous.item_type
                   AND r.candidate_rank = previous.candidate_rank + 1
      JOIN snapshot_items src ON src.id = r.id
     WHERE previous.source_chat_id IS NOT NULL AND previous.source_chat_id <> :cid
), inspected AS MATERIALIZED (
    SELECT * FROM heads
    UNION ALL
    SELECT * FROM fallback WHERE candidate_rank > 1
), chosen AS MATERIALIZED (
    SELECT id, external_id, item_type, message_id
      FROM inspected WHERE source_chat_id = :cid
), payloads AS MATERIALIZED (
    SELECT src.*, chosen.message_id
      FROM chosen JOIN snapshot_items src ON src.id = chosen.id
), guard AS MATERIALIZED (
    SELECT EXISTS (SELECT 1 FROM inspected WHERE source_chat_id IS NULL) AS provenance_gap
), inserted AS (
    INSERT INTO snapshot_items (
        id, snapshot_id, tenant_id, item_type, external_id, parent_external_id,
        name, folder_path, content_hash, content_checksum, content_size,
        blob_path, encryption_key_id, backup_version, metadata, is_deleted,
        indexed_at, backend_id, created_at
    )
    SELECT gen_random_uuid(), :tsid, :tid, p.item_type, p.external_id,
           p.message_id, p.name, p.folder_path, p.content_hash,
           p.content_checksum, p.content_size, p.blob_path, p.encryption_key_id,
           COALESCE(p.backup_version, 1), p.metadata, COALESCE(p.is_deleted, false),
           p.indexed_at, p.backend_id, NOW()
      FROM payloads p
     WHERE NOT (SELECT provenance_gap FROM guard)
     ORDER BY p.external_id, p.item_type
    ON CONFLICT (snapshot_id, external_id, item_type) DO NOTHING
    RETURNING id
)
SELECT (SELECT count(*) FROM inserted) AS inserted_count,
       (SELECT provenance_gap FROM guard) AS provenance_gap,
       EXISTS (SELECT 1 FROM target_scope) AS target_valid
"""


async def copy_legacy_chat_attachments(session, *, tenant_id, target_snapshot_id, chat_id) -> int:
    """Copy only attributable children; caller owns admission and transaction.

    Unversioned histories retain a deterministic newest-row fallback. A copied
    row's timestamp does not establish its original content version; only a
    fresh source reconciliation can establish a complete versioned baseline.
    """
    row = (await session.execute(text(LEGACY_CHAT_ATTACHMENT_COPY_SQL), {
        "tid": tenant_id, "tsid": target_snapshot_id, "cid": chat_id,
    })).one()
    if not row.target_valid:
        raise LegacyChatProvenanceGap("target snapshot tenant provenance is unavailable")
    if row.provenance_gap:
        raise LegacyChatProvenanceGap("legacy chat attachment provenance is ambiguous")
    return int(row.inserted_count)
