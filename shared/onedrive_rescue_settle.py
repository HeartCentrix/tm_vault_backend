"""Settle a partitioned OneDrive snapshot whose lost files the per-file retry queue rescued.

A shard that loses even one file finalizes FAILED (EnumerationFailed "OneDrive required file
content failed for N file(s)") and hands the file to onedrive_file_retries, which later writes
the bytes into the SAME snapshot. Nothing re-derived the snapshot afterwards, so on Railway
2026-11-06 "OneDrive — Amit Mishra" stayed FAILED with 788/788 files blobbed: hidden from
Recovery (COMPLETED/PARTIAL only), delta cursor never promoted, and AUTO_RETRY re-copied 127 GiB.

Settles only on proof: every retry row RESCUED, every shard either COMPLETED clean or FAILED
for file content alone, and every non-empty ONEDRIVE_FILE row carrying a blob.
"""
from sqlalchemy import text

_ELIGIBLE = text("""
    SELECT s.id, s.job_id, s.resource_id
      FROM snapshots s
     WHERE s.id = :sid
       AND s.status IN ('FAILED', 'PARTIAL')
       AND coalesce(s.extra_data::jsonb ->> 'cancelled_at', '') = ''
       AND EXISTS (SELECT 1 FROM onedrive_file_retries q WHERE q.snapshot_id = s.id AND q.status = 'RESCUED')
       AND NOT EXISTS (SELECT 1 FROM onedrive_file_retries q WHERE q.snapshot_id = s.id AND q.status <> 'RESCUED')
       AND EXISTS (SELECT 1 FROM snapshot_partitions p WHERE p.snapshot_id = s.id)
       AND NOT EXISTS (
             SELECT 1 FROM snapshot_partitions p
              WHERE p.snapshot_id = s.id
                AND (p.partition_type <> 'ONEDRIVE_FILES'
                     OR p.status NOT IN ('COMPLETED', 'FAILED')
                     OR (p.status = 'FAILED'
                         AND coalesce(p.failure_state::jsonb ->> 'message', '')
                             NOT LIKE 'OneDrive required file content failed for %')
                     OR coalesce((p.failure_state::jsonb ->> 'enumeration_incomplete')::boolean, false)
                     OR (jsonb_typeof(p.failure_state::jsonb -> 'files_failed') = 'number'
                         AND (p.failure_state::jsonb ->> 'files_failed')::bigint > 0)))
       AND NOT EXISTS (
             SELECT 1 FROM snapshot_items i
              WHERE i.snapshot_id = s.id
                AND i.item_type = 'ONEDRIVE_FILE'
                AND i.blob_path IS NULL
                AND NOT coalesce(i.is_deleted, false)
                -- a 0-byte OneDrive file has nothing to store (Graph size 0, quickXor all zeros)
                AND coalesce((i.metadata::jsonb -> 'raw' ->> 'size')::bigint, i.content_size, 1) > 0)
     FOR UPDATE OF s
""")

_SNAPSHOT = text("""
    UPDATE snapshots
       SET status = 'COMPLETED',
           extra_data = ((coalesce(extra_data::jsonb, '{}'::jsonb) - 'failure_reason' - 'degraded_reason')
                         || jsonb_build_object('rescued_after_finalize', now()::text))::json
     WHERE id = :sid
""")

# promote_pending_delta_link semantics: only the owning snapshot's pending link moves.
_CURSOR = text("""
    UPDATE resources
       SET metadata = ((metadata::jsonb - 'onedrive_delta_link_pending' - 'onedrive_delta_link_pending_snapshot')
                       || jsonb_build_object('onedrive_delta_link', metadata::jsonb -> 'onedrive_delta_link_pending'))::json
     WHERE id = :rid
       AND metadata::jsonb ->> 'onedrive_delta_link_pending_snapshot' = :sid_text
       AND coalesce(metadata::jsonb ->> 'onedrive_delta_link_pending', '') <> ''
""")

_STAMP = text("""
    UPDATE resources SET last_backup_status = 'COMPLETED'
     WHERE id = :rid AND last_backup_job_id = :jid
""")

# Same mapping as _finalize_bulk_parent_if_complete, re-applied to an already-terminal job.
_JOB = text("""
    WITH c AS (
        SELECT count(*) AS n,
               count(*) FILTER (WHERE status::text IN ('COMPLETED', 'SKIPPED')) AS completed,
               count(*) FILTER (WHERE status::text = 'PARTIAL') AS partial,
               count(*) FILTER (WHERE status::text NOT IN ('COMPLETED', 'SKIPPED', 'PARTIAL', 'FAILED')) AS pending
          FROM snapshots WHERE job_id = :jid)
    UPDATE jobs j
       SET status = (CASE WHEN c.completed = 0 AND c.partial = 0 THEN 'FAILED'
                          WHEN c.completed = c.n THEN 'COMPLETED'
                          ELSE 'PARTIAL' END)::jobstatus
      FROM c
     WHERE j.id = :jid
       AND j.status::text IN ('PARTIAL', 'FAILED')
       AND c.pending = 0
       AND c.n >= greatest(coalesce(cardinality(j.batch_resource_ids), 0), 1)
""")


async def settle_rescued_onedrive_snapshot(session, snapshot_id) -> bool:
    """FAILED/PARTIAL → COMPLETED once every lost file is rescued. Caller commits."""
    row = (await session.execute(_ELIGIBLE, {"sid": snapshot_id})).first()
    if row is None:
        return False
    params = {"sid": snapshot_id, "sid_text": str(snapshot_id), "rid": row.resource_id, "jid": row.job_id}
    await session.execute(_SNAPSHOT, params)
    await session.execute(_CURSOR, params)
    await session.execute(_STAMP, params)
    await session.execute(_JOB, params)
    return True
