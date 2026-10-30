"""Add immutable chat catalogue records and nullable snapshot version links.

Revision 0011 follows 0010. Existing snapshot rows remain legacy/null-version.
The hot-table index is built concurrently, including partition leaves, outside
the DDL transaction. Invalid indexes fail readiness rather than being skipped.
Downgrade refuses while catalogue links or captures remain; materialize/remove
them through a separately reviewed procedure before removing the schema.
"""
from alembic import op
import sqlalchemy as sa

revision = "0011"
down_revision = "0010"
branch_labels = None
depends_on = None


_TABLES = (
    ("chat_capture_attempts", """
        id uuid PRIMARY KEY, snapshot_id uuid REFERENCES snapshots(id) ON DELETE SET NULL,
        resource_id uuid NOT NULL REFERENCES resources(id), tenant_id uuid NOT NULL REFERENCES tenants(id),
        chat_id text NOT NULL, folder_path text NOT NULL, principal_id text NOT NULL, permission_scope_hash varchar(64) NOT NULL,
        authentication_context_sha256 varchar(64) NOT NULL, backend_id uuid NOT NULL REFERENCES storage_backends(id),
        partition_id uuid, snapshot_lease_token bigint, job_lease_token bigint,
        partition_lease_token bigint, generation bigint NOT NULL, state varchar(24) NOT NULL DEFAULT 'CREATED',
        expires_at timestamptz NOT NULL, staged_inventory_id uuid NOT NULL, evidence json,
        evidence_history json NOT NULL DEFAULT '[]', failure_reason text, reused_inventory boolean NOT NULL DEFAULT false,
        created_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT uq_chat_attempt_generation UNIQUE(snapshot_id,chat_id,generation),
        CONSTRAINT ck_chat_attempt_generation CHECK(generation >= 0),
        CONSTRAINT ck_chat_attempt_state CHECK(state IN ('CREATED','ENUMERATING','CONTENT_PENDING','VERIFYING','READY','PUBLISHED','PARTIAL','FAILED','SUPERSEDED'))
    """),
    ("chat_blob_objects", """
        id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id),
        backend_id uuid NOT NULL REFERENCES storage_backends(id), container text NOT NULL, path text NOT NULL,
        sha256 varchar(64) NOT NULL, size bigint NOT NULL, encryption_key_id text, backup_version integer,
        state varchar(16) NOT NULL DEFAULT 'STAGING', capture_attempt_id uuid REFERENCES chat_capture_attempts(id) ON DELETE SET NULL,
        uploaded_bytes bigint NOT NULL DEFAULT 0, upload_deadline timestamptz, quiescent_after timestamptz,
        deletion_task_id uuid, deletion_requested_at timestamptz, deletion_attempts integer NOT NULL DEFAULT 0,
        deletion_error text, deleted_at timestamptz, created_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT uq_chat_blob_location UNIQUE(backend_id,container,path),
        CONSTRAINT ck_chat_blob_state CHECK(state IN ('STAGING','LIVE','DELETING','DELETED')),
        CONSTRAINT ck_chat_blob_counts CHECK(size >= 0 AND uploaded_bytes >= 0 AND deletion_attempts >= 0)
    """),
    ("chat_item_versions", """
        id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id), chat_id text NOT NULL,
        logical_key varchar(64) NOT NULL, item_type text NOT NULL, graph_id text NOT NULL, parent_graph_id text,
        payload_sha256 varchar(64) NOT NULL, version_sha256 varchar(64) NOT NULL, payload json NOT NULL, content json,
        object_id uuid REFERENCES chat_blob_objects(id) ON DELETE RESTRICT, created_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT uq_chat_item_version UNIQUE(tenant_id,chat_id,logical_key,version_sha256),
        CONSTRAINT ck_chat_version_parent CHECK(item_type NOT IN ('CHAT_ATTACHMENT','CHAT_HOSTED_CONTENT') OR parent_graph_id IS NOT NULL)
    """),
    ("chat_inventories", """
        id uuid PRIMARY KEY, capture_attempt_id uuid NOT NULL UNIQUE REFERENCES chat_capture_attempts(id),
        tenant_id uuid NOT NULL REFERENCES tenants(id), chat_id text NOT NULL, authentication_context_sha256 varchar(64) NOT NULL,
        state varchar(16) NOT NULL DEFAULT 'STAGING', membership_sha256 varchar(64), representation_sha256 varchar(64),
        item_count bigint NOT NULL DEFAULT 0, logical_bytes bigint NOT NULL DEFAULT 0, evidence json, failure_reason text,
        created_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT ck_chat_inventory_state CHECK(state IN ('STAGING','PARTIAL','COMPLETE')),
        CONSTRAINT ck_chat_inventory_counts CHECK(item_count >= 0 AND logical_bytes >= 0),
        CONSTRAINT ck_chat_inventory_complete_evidence CHECK(state <> 'COMPLETE' OR (membership_sha256 IS NOT NULL AND representation_sha256 IS NOT NULL AND evidence IS NOT NULL))
    """),
    ("chat_inventory_items", """
        inventory_id uuid NOT NULL REFERENCES chat_inventories(id) ON DELETE CASCADE, logical_key varchar(64) NOT NULL,
        version_id uuid NOT NULL REFERENCES chat_item_versions(id) ON DELETE RESTRICT, PRIMARY KEY(inventory_id,logical_key)
    """),
    ("snapshot_chat_inventories", """
        snapshot_id uuid NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE, chat_id text NOT NULL,
        tenant_id uuid NOT NULL REFERENCES tenants(id), inventory_id uuid NOT NULL REFERENCES chat_inventories(id) ON DELETE RESTRICT,
        capture_attempt_id uuid NOT NULL REFERENCES chat_capture_attempts(id), coverage_status varchar(16) NOT NULL,
        started_at timestamptz NOT NULL, finished_at timestamptz NOT NULL, failure_reason text,
        uploaded_bytes bigint NOT NULL DEFAULT 0,
        PRIMARY KEY(snapshot_id,chat_id), CONSTRAINT ck_snapshot_chat_coverage CHECK(coverage_status IN ('PARTIAL','COMPLETE'))
    """),
    ("chat_catalogue_pins", """
        id uuid PRIMARY KEY, inventory_id uuid REFERENCES chat_inventories(id) ON DELETE RESTRICT,
        object_id uuid REFERENCES chat_blob_objects(id) ON DELETE RESTRICT, owner_kind varchar(32) NOT NULL,
        owner_id uuid NOT NULL, generation bigint NOT NULL, expires_at timestamptz,
        created_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT ck_chat_pin_target CHECK((inventory_id IS NULL) <> (object_id IS NULL)),
        CONSTRAINT ck_chat_pin_generation CHECK(generation >= 0)
    """),
)


def _constraint(conn, table, name, ddl):
    exists = conn.scalar(sa.text("SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=to_regclass(:table) AND conname=:name)"), {"table": table, "name": name})
    if not exists:
        op.execute(ddl)


def upgrade():
    conn = op.get_bind()
    for table, columns in _TABLES:
        op.execute(f"CREATE TABLE IF NOT EXISTS {table} ({columns})")
    _constraint(conn, "chat_capture_attempts", "fk_chat_attempt_staged_inventory",
        "ALTER TABLE chat_capture_attempts ADD CONSTRAINT fk_chat_attempt_staged_inventory "
        "FOREIGN KEY(staged_inventory_id) REFERENCES chat_inventories(id) DEFERRABLE INITIALLY DEFERRED")
    op.execute("ALTER TABLE snapshot_items ADD COLUMN IF NOT EXISTS chat_version_id uuid")
    partitioned = conn.scalar(sa.text("SELECT relkind='p' FROM pg_class WHERE oid=to_regclass('snapshot_items')"))
    if not partitioned:
        _constraint(conn, "snapshot_items", "fk_snapshot_items_chat_version",
            "ALTER TABLE snapshot_items ADD CONSTRAINT fk_snapshot_items_chat_version "
            "FOREIGN KEY(chat_version_id) REFERENCES chat_item_versions(id) ON DELETE RESTRICT NOT VALID")
    indexes = (
        ("ix_chat_capture_attempts_snapshot_id", "chat_capture_attempts(snapshot_id)"),
        ("ix_chat_blob_objects_capture_attempt_id", "chat_blob_objects(capture_attempt_id)"),
        ("ix_chat_item_versions_object_id", "chat_item_versions(object_id)"),
        ("ix_chat_item_versions_parent_lookup", "chat_item_versions(tenant_id,chat_id,md5(graph_id),item_type)"),
        ("ix_chat_inventory_items_version_id", "chat_inventory_items(version_id)"),
        ("ix_snapshot_chat_inventories_inventory_id", "snapshot_chat_inventories(inventory_id)"),
        ("ix_chat_catalogue_pins_inventory_id", "chat_catalogue_pins(inventory_id)"),
        ("ix_chat_catalogue_pins_object_id", "chat_catalogue_pins(object_id)"),
    )
    for name, target in indexes:
        op.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {target}")
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_chat_complete_inventory ON chat_inventories "
        "(tenant_id,chat_id,authentication_context_sha256,membership_sha256,representation_sha256) WHERE state='COMPLETE'")
    if partitioned:
        leaves = conn.execute(sa.text("SELECT n.nspname,c.relname,c.oid FROM pg_partition_tree('snapshot_items') p "
            "JOIN pg_class c ON c.oid=p.relid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE p.isleaf")).all()
        quote = conn.dialect.identifier_preparer.quote_identifier
        valid = conn.scalar(sa.text("SELECT indisvalid FROM pg_index WHERE indexrelid=to_regclass('ix_snapshot_items_chat_version_id')"))
        if not valid:
            # ONLY creates the empty parent; leaves build without blocking writes.
            op.execute("CREATE INDEX IF NOT EXISTS ix_snapshot_items_chat_version_id ON ONLY snapshot_items(chat_version_id)")
            for schema, table, oid in leaves:
                name = f"ix_chat_version_leaf_{oid}"
                with op.get_context().autocommit_block():
                    op.execute(f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {quote(name)} ON {quote(schema)}.{quote(table)}(chat_version_id)")
                op.execute(f"ALTER INDEX ix_snapshot_items_chat_version_id ATTACH PARTITION {quote(schema)}.{quote(name)}")
    else:
        with op.get_context().autocommit_block():
            op.execute("CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_snapshot_items_chat_version_id ON snapshot_items(chat_version_id)")
    if not conn.scalar(sa.text("SELECT indisvalid FROM pg_index WHERE indexrelid=to_regclass('ix_snapshot_items_chat_version_id')")):
        raise RuntimeError("chat_version_id index is invalid; repair the index before retrying migration 0011")
    if partitioned:
        # PostgreSQL disallows NOT VALID on the partitioned parent. Validate
        # leaves first so parent creation attaches their existing constraints.
        exists = conn.scalar(sa.text("SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=to_regclass('snapshot_items') AND conname='fk_snapshot_items_chat_version')"))
        if not exists:
            for schema, table, oid in leaves:
                target, name = f"{quote(schema)}.{quote(table)}", f"fk_chat_version_leaf_{oid}"
                with op.get_context().autocommit_block():
                    _constraint(conn, target, name, f"ALTER TABLE {target} ADD CONSTRAINT {quote(name)} "
                        "FOREIGN KEY(chat_version_id) REFERENCES chat_item_versions(id) ON DELETE RESTRICT NOT VALID")
                with op.get_context().autocommit_block():
                    op.execute(f"ALTER TABLE {target} VALIDATE CONSTRAINT {quote(name)}")
            _constraint(conn, "snapshot_items", "fk_snapshot_items_chat_version",
                "ALTER TABLE snapshot_items ADD CONSTRAINT fk_snapshot_items_chat_version "
                "FOREIGN KEY(chat_version_id) REFERENCES chat_item_versions(id) ON DELETE RESTRICT")
    else:
        # The concurrent-index boundary released initial ADD COLUMN/FK locks.
        op.execute("ALTER TABLE snapshot_items VALIDATE CONSTRAINT fk_snapshot_items_chat_version")


def downgrade():
    conn = op.get_bind()
    if conn.scalar(sa.text("SELECT EXISTS(SELECT 1 FROM chat_capture_attempts) OR EXISTS(SELECT 1 FROM snapshot_items WHERE chat_version_id IS NOT NULL)")):
        raise RuntimeError("Cannot remove a populated chat catalogue; materialize retained snapshots first")
    op.execute("ALTER TABLE snapshot_items DROP CONSTRAINT IF EXISTS fk_snapshot_items_chat_version")
    op.execute("ALTER TABLE snapshot_items DROP COLUMN IF EXISTS chat_version_id")
    op.execute("ALTER TABLE chat_capture_attempts DROP CONSTRAINT IF EXISTS fk_chat_attempt_staged_inventory")
    for table in ("chat_catalogue_pins", "snapshot_chat_inventories", "chat_inventory_items", "chat_inventories",
                  "chat_item_versions", "chat_blob_objects", "chat_capture_attempts"):
        op.execute(f"DROP TABLE {table}")
