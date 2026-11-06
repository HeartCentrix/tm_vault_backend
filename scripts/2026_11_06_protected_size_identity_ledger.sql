-- Protected-size identity ledger (alembic 0024) for databases without alembic (Railway) and for
-- any database whose protected_size_refs was too big for the migration to convert inline (TM).
--
-- Converts per-item refs (~1 row per retained snapshot item) into one anchor per winner, then
-- drops protected_size_refs. Online: no long lock on any big table. Batched (5000 winners per
-- committed batch) and resumable: rerun after any interruption, finished rows are skipped.
-- Deploy the 0024 code first or with it: the refresher idles ("converting") while
-- protected_size_refs exists and resumes by itself once this script drops it.
--
-- Run OUTSIDE a transaction (CREATE INDEX CONCURRENTLY and per-batch COMMIT), as the schema owner:
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c 'SET search_path TO <schema>' -f this_file
-- If interrupted during the index build: DROP INDEX CONCURRENTLY protected_size_winner_anchor; rerun.
-- Does not touch alembic_version.
SET lock_timeout = '5s';
ALTER TABLE protected_size_winners ADD COLUMN IF NOT EXISTS snapshot_id uuid,ADD COLUMN IF NOT EXISTS item_id uuid,ADD COLUMN IF NOT EXISTS created_at timestamp;
CREATE INDEX CONCURRENTLY IF NOT EXISTS protected_size_winner_anchor ON protected_size_winners(resource_id,snapshot_id,item_id);
DO $fn$
  DECLARE cur_r uuid := '00000000-0000-0000-0000-000000000000'; cur_i bigint := -9223372036854775808;
    last record; batches bigint := 0;
  BEGIN
    IF to_regclass('protected_size_refs') IS NULL THEN RETURN; END IF;
    LOOP
      -- Keyset over the winners primary key; the anchor is the newest ref (its winner).
      WITH page AS (SELECT resource_id,identity_id FROM protected_size_winners
          WHERE (resource_id,identity_id)>(cur_r,cur_i) AND item_id IS NULL
          ORDER BY resource_id,identity_id LIMIT 5000),
        anchored AS (UPDATE protected_size_winners w SET snapshot_id=f.snapshot_id,item_id=f.item_id,created_at=f.created_at
          FROM page p CROSS JOIN LATERAL (SELECT snapshot_id,item_id,created_at FROM protected_size_refs r
            WHERE r.resource_id=p.resource_id AND r.identity_id=p.identity_id
            ORDER BY created_at DESC,item_id DESC LIMIT 1) f
          WHERE w.resource_id=p.resource_id AND w.identity_id=p.identity_id RETURNING 1)
      SELECT resource_id,identity_id,(SELECT count(*) FROM anchored) n INTO last
        FROM page ORDER BY resource_id DESC,identity_id DESC LIMIT 1;
      EXIT WHEN NOT FOUND;
      cur_r := last.resource_id; cur_i := last.identity_id; batches := batches+1;
      IF batches % 200 = 0 THEN RAISE NOTICE 'protected-size anchors: % batches, at resource %',batches,cur_r; END IF;
      COMMIT;
    END LOOP;
    -- A pre-0024 refresher still running writes unanchored winners behind the cursor.
    IF EXISTS(SELECT 1 FROM protected_size_winners w WHERE w.item_id IS NULL AND EXISTS(SELECT 1 FROM protected_size_refs r
        WHERE r.resource_id=w.resource_id AND r.identity_id=w.identity_id)) THEN
      RAISE EXCEPTION 'protected-size: winners changed during conversion (pre-0024 refresher still running?); stop it and rerun';
    END IF;
  END $fn$;
DROP TABLE IF EXISTS protected_size_refs;
