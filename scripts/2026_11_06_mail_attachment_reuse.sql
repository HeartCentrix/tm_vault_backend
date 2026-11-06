-- Cross-mailbox mail attachment reuse index (alembic 0023), for Railway where alembic does not
-- run. psql autocommit (the default): CONCURRENTLY cannot run inside BEGIN. Idempotent.
-- Workers check for a VALID index once per process: restart them after it is built.

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_snapshot_items_mail_att_imid ON snapshot_items
    (tenant_id, (metadata->>'internet_message_id')) WHERE item_type = 'EMAIL_ATTACHMENT';

-- Verify: SELECT indisvalid FROM pg_index WHERE indexrelid = 'ix_snapshot_items_mail_att_imid'::regclass;
-- If false: DROP INDEX CONCURRENTLY ix_snapshot_items_mail_att_imid; and re-run.
-- Rollback: DROP INDEX CONCURRENTLY IF EXISTS ix_snapshot_items_mail_att_imid;
