-- Chat overlay snapshots — alembic revision 0023. snapshot_items_effective() also inherits Teams-chat
-- base rows (TEAMS_CHAT_MESSAGE, CHAT_ATTACHMENT, CHAT_HOSTED_CONTENT). Railway (DB_SCHEMA_MODE=verify,
-- no alembic): apply once as the schema owner, with search_path set to the application schema, BEFORE
-- deploying code that builds chat overlays (CHAT_OVERLAY_ENABLED). Idempotent; generated from
-- shared.database.SNAPSHOT_ITEMS_EFFECTIVE_FUNCTION (tests/shared/test_mail_overlay_schema.py).
BEGIN;
SET LOCAL lock_timeout = '5s';

CREATE OR REPLACE FUNCTION snapshot_items_effective(p_snapshot_id uuid)
        RETURNS SETOF snapshot_items LANGUAGE sql STABLE AS $$
        SELECT o.* FROM snapshot_items o
         WHERE o.snapshot_id = (SELECT COALESCE(s.reuse_chain_root_id, s.id) FROM snapshots s
                                 WHERE s.id = p_snapshot_id)
        UNION ALL
        SELECT b.* FROM snapshot_items b
         WHERE b.snapshot_id = (SELECT t.overlay_base_id FROM snapshots s
                                  JOIN snapshots t ON t.id = COALESCE(s.reuse_chain_root_id, s.id)
                                 WHERE s.id = p_snapshot_id)
           AND b.item_type IN ('EMAIL', 'EMAIL_ATTACHMENT', 'TEAMS_CHAT_MESSAGE', 'CHAT_ATTACHMENT', 'CHAT_HOSTED_CONTENT')
           AND NOT EXISTS (SELECT 1 FROM snapshot_items o
                            WHERE o.snapshot_id = (SELECT COALESCE(s.reuse_chain_root_id, s.id) FROM snapshots s
                                                    WHERE s.id = p_snapshot_id)
                              AND o.item_type = b.item_type AND o.external_id = b.external_id)
           AND NOT EXISTS (SELECT 1 FROM snapshot_overlay_removals r
                            WHERE r.snapshot_id = (SELECT COALESCE(s.reuse_chain_root_id, s.id) FROM snapshots s
                                                    WHERE s.id = p_snapshot_id)
                              AND r.item_type = b.item_type AND r.external_id = b.external_id)
           AND (b.item_type <> 'EMAIL_ATTACHMENT'
                OR EXISTS (SELECT 1 FROM snapshot_items o
                            WHERE o.snapshot_id = (SELECT COALESCE(s.reuse_chain_root_id, s.id) FROM snapshots s
                                                    WHERE s.id = p_snapshot_id)
                              AND o.item_type = 'EMAIL'
                              AND o.external_id = COALESCE(b.parent_external_id, split_part(b.external_id, '::', 1)))
                OR (EXISTS (SELECT 1 FROM snapshot_items p
                             WHERE p.snapshot_id = (SELECT t.overlay_base_id FROM snapshots s
                                                      JOIN snapshots t ON t.id = COALESCE(s.reuse_chain_root_id, s.id)
                                                     WHERE s.id = p_snapshot_id)
                               AND p.item_type = 'EMAIL'
                               AND p.external_id = COALESCE(b.parent_external_id, split_part(b.external_id, '::', 1)))
                    AND NOT EXISTS (SELECT 1 FROM snapshot_overlay_removals r
                                     WHERE r.snapshot_id = (SELECT COALESCE(s.reuse_chain_root_id, s.id)
                                                              FROM snapshots s WHERE s.id = p_snapshot_id)
                                       AND r.item_type = 'EMAIL'
                                       AND r.external_id = COALESCE(b.parent_external_id,
                                                                    split_part(b.external_id, '::', 1)))))
        $$;;

COMMIT;
