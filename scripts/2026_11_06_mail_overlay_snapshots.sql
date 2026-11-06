-- Overlay mail snapshots (docs/mail-overlay-snapshots.md) — alembic revision 0021.
-- Railway runs DB_SCHEMA_MODE=verify (no alembic): apply this once, as the schema owner (no
-- superuser needed), BEFORE deploying code that has the overlay column in its models.
-- Idempotent. Generated from shared.database.MAIL_OVERLAY_DDL (tests/shared/test_mail_overlay_schema.py
-- fails if the two drift). Run with search_path set to the application schema (settings.DB_SCHEMA),
-- e.g. psql "$DATABASE_URL" -c 'SET search_path TO <schema>' -f this_file. Does not touch alembic_version.
BEGIN;
SET LOCAL lock_timeout = '5s';

ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS overlay_base_id UUID REFERENCES snapshots(id) ON DELETE RESTRICT;

CREATE INDEX IF NOT EXISTS ix_snapshots_overlay_base ON snapshots (overlay_base_id) WHERE overlay_base_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS snapshot_overlay_removals (snapshot_id UUID NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE, item_type VARCHAR NOT NULL, external_id VARCHAR NOT NULL, PRIMARY KEY (snapshot_id, item_type, external_id));

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

CREATE OR REPLACE FUNCTION validate_snapshot_overlay_base()
        RETURNS TRIGGER AS $$
        BEGIN
            IF NEW.overlay_base_id IS NULL THEN
                RETURN NEW;
            END IF;
            IF NEW.reuse_of_snapshot_id IS NOT NULL THEN
                RAISE EXCEPTION 'snapshot % cannot be both a reuse pointer and an overlay', NEW.id;
            END IF;
            -- Single level: the base is a FULL snapshot (neither overlay nor pointer) of the same resource.
            PERFORM 1 FROM snapshots b
              WHERE b.id = NEW.overlay_base_id AND b.id <> NEW.id
                AND b.resource_id = NEW.resource_id
                AND b.overlay_base_id IS NULL AND b.reuse_of_snapshot_id IS NULL;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'overlay_base_id % invalid for snapshot % (must be a full snapshot of the same resource)',
                    NEW.overlay_base_id, NEW.id;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;;

DROP TRIGGER IF EXISTS snapshots_overlay_validate ON snapshots;

CREATE TRIGGER snapshots_overlay_validate BEFORE INSERT OR UPDATE OF overlay_base_id, reuse_of_snapshot_id ON snapshots FOR EACH ROW EXECUTE FUNCTION validate_snapshot_overlay_base();

COMMIT;
