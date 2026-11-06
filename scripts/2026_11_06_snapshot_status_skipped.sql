-- TM log issue #1: SKIPPED snapshot status (alembic 0022), for Railway where
-- alembic does not run. Run with psql in autocommit (the default): the enum value
-- must be committed before the backfill can use it, so do NOT wrap this in BEGIN.
-- Apply BEFORE deploying the code that writes SKIPPED. Idempotent.

ALTER TYPE snapshotstatus ADD VALUE IF NOT EXISTS 'SKIPPED';

UPDATE snapshots SET status = 'SKIPPED'
     WHERE status = 'COMPLETED'
       AND COALESCE(json_extract_path_text(extra_data, 'skipped_concurrent'), 'false') IN ('true', 't', '1');

-- Rollback (code before 0022 cannot read SKIPPED):
-- UPDATE snapshots SET status = 'COMPLETED' WHERE status = 'SKIPPED';
