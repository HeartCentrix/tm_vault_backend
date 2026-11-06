-- Read-only pre-flight before setting RETENTION_DELETE_ENABLED=true.
-- Every row printed must say PASS. Safe to run any time: SELECT only.
-- psql -v ON_ERROR_STOP=1 -f scripts/2026_11_06_retention_preflight.sql

-- 1. Retention's fold-forward probes snapshot_items by (snapshot_id, external_id, item_type).
SELECT 'index uq_snapshot_items_snap_ext_type' AS check,
       CASE WHEN EXISTS (SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
                          WHERE c.relname = 'uq_snapshot_items_snap_ext_type' AND i.indisvalid)
            THEN 'PASS' ELSE 'FAIL: index missing or invalid' END AS result;

-- 2. Legacy (pre-catalogue) chat snapshots are sparse and are NOT folded by retention.
--    Each chat resource's newest usable snapshot must be catalogue format.
WITH newest AS (
    SELECT DISTINCT ON (s.resource_id) s.resource_id, s.extra_data::jsonb AS x
      FROM snapshots s JOIN resources r ON r.id = s.resource_id
     WHERE r.type::text = 'USER_CHATS' AND s.status::text IN ('COMPLETED', 'PARTIAL')
     ORDER BY s.resource_id, s.created_at DESC)
SELECT 'chat resources on catalogue format' AS check,
       CASE WHEN count(*) FILTER (WHERE coalesce((x ->> 'chat_inventory_format')::int, 0) <> 1) = 0
            THEN 'PASS (' || count(*) || ' resources)'
            ELSE 'FAIL: ' || count(*) FILTER (WHERE coalesce((x ->> 'chat_inventory_format')::int, 0) <> 1)
                 || ' of ' || count(*) || ' need one backup on the new version' END AS result
  FROM newest;

-- 3. Every active resource has finished at least one backup since the upgrade.
--    :'upgraded_at' = deploy time of this release, e.g. -v upgraded_at='2026-11-07 00:00+00'
SELECT 'resources backed up since upgrade' AS check,
       CASE WHEN count(*) = 0 THEN 'PASS'
            ELSE 'FAIL: ' || count(*) || ' resources have no COMPLETED/PARTIAL backup since upgrade' END AS result
  FROM resources r
 WHERE r.last_backup_at IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM snapshots s
                    WHERE s.resource_id = r.id AND s.status::text IN ('COMPLETED', 'PARTIAL')
                      AND s.created_at >= CAST(:'upgraded_at' AS timestamptz));
