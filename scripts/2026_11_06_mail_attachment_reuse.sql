-- Cross-mailbox mail attachment reuse table (alembic 0026), for Railway where alembic does not
-- run. Service boot's create_all also creates it; this is for creating it ahead of the deploy.
-- Idempotent. Workers check for the table once per process: restart them after creating it.

CREATE TABLE IF NOT EXISTS mail_attachment_contents (
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    internet_message_id VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    graph_size BIGINT NOT NULL,
    content_type VARCHAR NOT NULL,
    is_inline BOOLEAN NOT NULL,
    content_id VARCHAR,
    blob_path VARCHAR NOT NULL,
    content_hash VARCHAR NOT NULL,
    content_size BIGINT NOT NULL,
    backend_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, internet_message_id, name, graph_size, content_type, is_inline)
);

-- Rollback (reuse turns itself off without the table): DROP TABLE IF EXISTS mail_attachment_contents;
