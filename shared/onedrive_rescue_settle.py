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

_PARENT = text("""
    SELECT j.id FROM jobs j JOIN snapshots s ON s.job_id = j.id
     WHERE s.id = :sid AND j.status::text NOT IN ('CANCELLED', 'CANCELLING')
     FOR UPDATE OF j
""")

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
                AND coalesce(i.blob_path, '') = ''
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

# The reconciler's STEP D re-derives every terminal snapshot from its shards (~60 s); a shard left FAILED
# flipped a settled snapshot straight back (Railway 2026-11-06). Eligibility already proved these shards
# failed only on file content that is now rescued; keep their old failure_state for the record.
_SHARDS = text("""
    UPDATE snapshot_partitions
       SET status = 'COMPLETED',
           failure_state = jsonb_build_object('rescued_after_finalize', now()::text,
                                              'previous', failure_state::jsonb)::json
     WHERE snapshot_id = :sid AND status = 'FAILED'
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
          FROM snapshots WHERE job_id = :jid),
    missing AS (
        SELECT count(*) AS n FROM jobs j, unnest(j.batch_resource_ids) AS rid
         WHERE j.id = :jid AND NOT EXISTS (
             SELECT 1 FROM snapshots s WHERE s.resource_id = rid
               AND (s.job_id = j.id OR s.created_at >= j.created_at)))
    UPDATE jobs j
       SET status = (CASE WHEN c.completed = 0 AND c.partial = 0 THEN 'FAILED'
                          WHEN c.completed = c.n AND missing.n = 0 THEN 'COMPLETED'
                          ELSE 'PARTIAL' END)::jobstatus
      FROM c, missing
     WHERE j.id = :jid
       AND j.status::text IN ('PARTIAL', 'FAILED')
       AND c.pending = 0
       AND c.n > 0
       AND NOT EXISTS (SELECT 1 FROM snapshot_partitions p WHERE p.job_id = j.id
                       AND p.status NOT IN ('COMPLETED', 'FAILED'))
""")


# The Activity batch row reads backup_batches.status, set once by batch_rollup._finalize_batch_if_complete
# (PARTIAL if any snapshot of its resources created after the batch start is PARTIAL/FAILED). Re-apply that
# rule to PARTIAL batches covering this resource; batches of the resource's later runs are included because
# the rule also counts snapshots (e.g. an AUTO_RETRY) taken after the batch began.
_BATCH = text("""
    UPDATE backup_batches b
       SET status = 'COMPLETED'
     WHERE b.status = 'PARTIAL'
       AND b.created_at <= (SELECT created_at FROM snapshots WHERE id = :sid)
       AND EXISTS (SELECT 1 FROM jobs j JOIN snapshots s ON s.job_id = j.id
                    WHERE coalesce(j.spec::jsonb ->> 'batch_id', '') = b.id::text
                      AND s.resource_id = :rid)
       AND NOT EXISTS (
             SELECT 1 FROM snapshots s2
              WHERE s2.created_at > b.created_at
                AND s2.status::text IN ('PARTIAL', 'FAILED')
                AND s2.resource_id IN (SELECT s3.resource_id FROM jobs j3 JOIN snapshots s3 ON s3.job_id = j3.id
                                        WHERE coalesce(j3.spec::jsonb ->> 'batch_id', '') = b.id::text))
       AND NOT EXISTS (
             SELECT 1 FROM jobs j4
              WHERE coalesce(j4.spec::jsonb ->> 'batch_id', '') = b.id::text
                AND j4.status::text <> 'COMPLETED')
""")


async def reroll_batches_for_resource(session, snapshot_id, resource_id) -> int:
    """PARTIAL batch rows covering ``resource_id`` → COMPLETED once nothing in them is PARTIAL/FAILED."""
    res = await session.execute(_BATCH, {"sid": snapshot_id, "rid": resource_id})
    return res.rowcount or 0


async def settle_rescued_onedrive_snapshot(session, snapshot_id) -> bool:
    """FAILED/PARTIAL → COMPLETED once every lost file is rescued. Caller commits."""
    # Lock parent first: concurrent last rescues must count committed siblings,
    # and cancellation must fence snapshot/cursor promotion in the same order.
    if (await session.execute(_PARENT, {"sid": snapshot_id})).first() is None:
        return False
    row = (await session.execute(_ELIGIBLE, {"sid": snapshot_id})).first()
    if row is None:
        return False
    params = {"sid": snapshot_id, "sid_text": str(snapshot_id), "rid": row.resource_id, "jid": row.job_id}
    await session.execute(_SNAPSHOT, params)
    await session.execute(_SHARDS, params)
    await session.execute(_CURSOR, params)
    await session.execute(_STAMP, params)
    await session.execute(_JOB, params)
    await reroll_batches_for_resource(session, snapshot_id, row.resource_id)
    return True
