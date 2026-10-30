"""Bounded-width selection for legacy chat attachment inventories."""
from sqlalchemy import text


class LegacyChatProvenanceGap(RuntimeError):
    """A legacy child cannot safely be attributed to the requested chat."""


LEGACY_CHAT_ATTACHMENT_COPY_SQL = """
WITH target_scope AS MATERIALIZED (
    SELECT s.id FROM snapshots s JOIN resources r ON r.id = s.resource_id
     WHERE s.id = :tsid AND r.tenant_id = :tid
), message_ids AS MATERIALIZED (
    SELECT DISTINCT m.message_external_id
      FROM chat_threads t JOIN chat_thread_messages m ON m.chat_thread_id = t.id
     WHERE t.tenant_id = :tid AND t.chat_id = :cid
), candidates AS MATERIALIZED (
    SELECT src.id, src.snapshot_id, src.external_id, src.item_type,
           src.parent_external_id AS message_id, src.created_at
      FROM snapshot_items src
      JOIN snapshots owner_s ON owner_s.id = src.snapshot_id
      JOIN resources owner_r ON owner_r.id = owner_s.resource_id
     WHERE owner_r.tenant_id = :tid
       AND (src.tenant_id = :tid OR src.tenant_id IS NULL)
       AND src.snapshot_id <> :tsid
       AND src.item_type IN ('CHAT_ATTACHMENT', 'CHAT_HOSTED_CONTENT')
       AND src.parent_external_id = ANY(ARRAY(SELECT message_external_id FROM message_ids))
       AND EXISTS (SELECT 1 FROM target_scope)
       AND NOT EXISTS (
           SELECT 1 FROM snapshot_items dst
            WHERE dst.snapshot_id = :tsid AND dst.external_id = src.external_id
              AND dst.item_type = src.item_type
       )
    UNION ALL
    SELECT src.id, src.snapshot_id, src.external_id, src.item_type,
           split_part(src.external_id, '::', 1) AS message_id, src.created_at
      FROM snapshot_items src
      JOIN snapshots owner_s ON owner_s.id = src.snapshot_id
      JOIN resources owner_r ON owner_r.id = owner_s.resource_id
     WHERE owner_r.tenant_id = :tid
       AND (src.tenant_id = :tid OR src.tenant_id IS NULL)
       AND src.snapshot_id <> :tsid
       AND src.item_type IN ('CHAT_ATTACHMENT', 'CHAT_HOSTED_CONTENT')
       AND src.parent_external_id IS NULL
       AND split_part(src.external_id, '::', 1) = ANY(ARRAY(SELECT message_external_id FROM message_ids))
       AND EXISTS (SELECT 1 FROM target_scope)
       AND NOT EXISTS (
           SELECT 1 FROM snapshot_items dst
            WHERE dst.snapshot_id = :tsid AND dst.external_id = src.external_id
              AND dst.item_type = src.item_type
       )
), source_pairs AS MATERIALIZED (
    SELECT DISTINCT snapshot_id, message_id FROM candidates
), source_chats AS MATERIALIZED (
    SELECT pair.snapshot_id, pair.message_id,
           CASE WHEN origin.first_chat = origin.last_chat THEN origin.first_chat END AS chat_id
      FROM source_pairs pair
      CROSS JOIN LATERAL (
          SELECT min(msg.parent_external_id) AS first_chat,
                 max(msg.parent_external_id) AS last_chat
            FROM snapshot_items msg
           WHERE msg.snapshot_id = pair.snapshot_id
             AND msg.external_id = pair.message_id
             AND msg.item_type IN ('TEAMS_CHAT_MESSAGE', 'TEAMS_MESSAGE', 'TEAMS_MESSAGE_REPLY')
      ) origin
), scoped AS MATERIALIZED (
    SELECT c.*, COALESCE(origin.chat_id,
               NULLIF(src.metadata->>'chat_id', ''),
               NULLIF(src.metadata->>'chatId', '')) AS source_chat_id
      FROM candidates c
      JOIN source_chats origin ON origin.snapshot_id = c.snapshot_id
                              AND origin.message_id = c.message_id
      JOIN snapshot_items src ON src.id = c.id
), chosen AS MATERIALIZED (
    SELECT DISTINCT ON (external_id, item_type)
           id, external_id, item_type, message_id
      FROM scoped
     WHERE source_chat_id = :cid
     ORDER BY external_id, item_type, created_at DESC NULLS LAST, id DESC
), payloads AS MATERIALIZED (
    SELECT src.*, chosen.message_id
      FROM chosen JOIN snapshot_items src ON src.id = chosen.id
), guard AS MATERIALIZED (
    SELECT EXISTS (SELECT 1 FROM scoped WHERE source_chat_id IS NULL)
        OR EXISTS (
            SELECT 1 FROM payloads p
             WHERE COALESCE(NULLIF(p.metadata->>'chat_id', ''),
                            NULLIF(p.metadata->>'chatId', ''), :cid) <> :cid
        ) AS provenance_gap
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
