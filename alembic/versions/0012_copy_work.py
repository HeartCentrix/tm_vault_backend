"""Durable database-copy continuations, frozen candidates and fair admission."""
from alembic import op
from sqlalchemy import text

revision = '0012'
down_revision = '0011'
branch_labels = None
depends_on = None

_DDL = (
    """CREATE TABLE IF NOT EXISTS copy_workflows (
	id UUID NOT NULL,
	tenant_id UUID NOT NULL,
	resource_id UUID NOT NULL,
	job_id UUID NOT NULL,
	target_snapshot_id UUID NOT NULL,
	partition_id UUID,
	leaf_key VARCHAR(80) NOT NULL,
	state VARCHAR(16) NOT NULL,
	generation BIGINT NOT NULL,
	parent_lease_token BIGINT NOT NULL,
	snapshot_lease_token BIGINT NOT NULL,
	job_lease_token BIGINT NOT NULL,
	continuation JSON NOT NULL,
	lease_owner_id UUID,
	lease_expires_at TIMESTAMP WITH TIME ZONE,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	completed_at TIMESTAMP WITH TIME ZONE,
	PRIMARY KEY (id),
	CONSTRAINT ck_copy_workflow_state CHECK (state IN ('DRAINING','WAITING','RESUMING','COMPLETED','PARTIAL','FAILED','CANCELLED')),
	FOREIGN KEY(tenant_id) REFERENCES tenants (id) ON DELETE CASCADE,
	FOREIGN KEY(resource_id) REFERENCES resources (id) ON DELETE CASCADE,
	FOREIGN KEY(job_id) REFERENCES jobs (id) ON DELETE CASCADE,
	FOREIGN KEY(target_snapshot_id) REFERENCES snapshots (id) ON DELETE CASCADE,
	FOREIGN KEY(partition_id) REFERENCES snapshot_partitions (id) ON DELETE CASCADE,
	UNIQUE (leaf_key)
)""",
    """CREATE INDEX IF NOT EXISTS ix_copy_workflow_job_state ON copy_workflows (job_id, state)""",
    """CREATE INDEX IF NOT EXISTS ix_copy_workflow_partition_state ON copy_workflows (partition_id, state)""",
    """CREATE INDEX IF NOT EXISTS ix_copy_workflow_snapshot_state ON copy_workflows (target_snapshot_id, state)""",
    """CREATE TABLE IF NOT EXISTS copy_work_queues (
	key VARCHAR(64) NOT NULL,
	scope VARCHAR(12) NOT NULL,
	parent_key VARCHAR(64),
	tenant_id UUID NOT NULL,
	sla_policy_id UUID,
	resource_id UUID,
	lane VARCHAR(8) NOT NULL,
	weight INTEGER NOT NULL,
	virtual_finish FLOAT NOT NULL,
	PRIMARY KEY (key),
	CONSTRAINT ck_copy_queue_weight CHECK (weight BETWEEN 1 AND 8),
	CONSTRAINT ck_copy_queue_scope_lane CHECK (scope IN ('TENANT','POLICY','RESOURCE') AND lane IN ('micro','giant'))
)""",
    """CREATE INDEX IF NOT EXISTS ix_copy_queue_parent_finish ON copy_work_queues (parent_key, virtual_finish, key)""",
    """CREATE TABLE IF NOT EXISTS copy_work_tasks (
	id UUID NOT NULL,
	workflow_id UUID NOT NULL,
	tenant_id UUID NOT NULL,
	resource_id UUID NOT NULL,
	sla_policy_id UUID,
	queue_key VARCHAR(64) NOT NULL,
	resource_queue_key VARCHAR(64) NOT NULL,
	tenant_queue_key VARCHAR(64) NOT NULL,
	idempotency_key VARCHAR(64) NOT NULL,
	kind VARCHAR(32) NOT NULL,
	lane VARCHAR(8) NOT NULL,
	params JSON NOT NULL,
	state VARCHAR(16) NOT NULL,
	generation BIGINT NOT NULL,
	prepared BOOLEAN NOT NULL,
	checkpoint BIGINT NOT NULL,
	row_count BIGINT NOT NULL,
	bytes_count BIGINT NOT NULL,
	lease_owner_id UUID,
	lease_expires_at TIMESTAMP WITH TIME ZONE,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	ready_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	queue_entered_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	cumulative_wait_s FLOAT NOT NULL,
	wait_max_s FLOAT NOT NULL,
	attempts INTEGER NOT NULL,
	error JSON,
	PRIMARY KEY (id),
	CONSTRAINT ck_copy_task_state CHECK (state IN ('READY','RUNNING','COMPLETED','FAILED','CANCELLED')),
	CONSTRAINT ck_copy_task_counts CHECK (lane IN ('micro','giant') AND checkpoint >= 0 AND generation >= 0 AND row_count >= 0 AND bytes_count >= 0),
	FOREIGN KEY(workflow_id) REFERENCES copy_workflows (id) ON DELETE CASCADE,
	FOREIGN KEY(queue_key) REFERENCES copy_work_queues (key),
	FOREIGN KEY(resource_queue_key) REFERENCES copy_work_queues (key),
	FOREIGN KEY(tenant_queue_key) REFERENCES copy_work_queues (key),
	UNIQUE (idempotency_key)
)""",
    """CREATE INDEX IF NOT EXISTS ix_copy_task_expired ON copy_work_tasks (state, lease_expires_at)""",
    """CREATE INDEX IF NOT EXISTS ix_copy_task_ready ON copy_work_tasks (state, ready_at, resource_queue_key)""",
    """CREATE INDEX IF NOT EXISTS ix_copy_task_workflow_state ON copy_work_tasks (workflow_id, state)""",
    """CREATE TABLE IF NOT EXISTS copy_work_items (
	task_id UUID NOT NULL,
	ordinal BIGINT NOT NULL,
	payload JSON NOT NULL,
	source_snapshot_id UUID,
	phase INTEGER NOT NULL,
	PRIMARY KEY (task_id, ordinal),
	FOREIGN KEY(task_id) REFERENCES copy_work_tasks (id) ON DELETE CASCADE,
	FOREIGN KEY(source_snapshot_id) REFERENCES snapshots (id) ON DELETE RESTRICT
)""",
    """CREATE INDEX IF NOT EXISTS ix_copy_work_items_source_snapshot_id ON copy_work_items (source_snapshot_id)""",
    """CREATE TABLE IF NOT EXISTS copy_work_control (
	id VARCHAR(16) NOT NULL,
	enabled BOOLEAN NOT NULL,
	epoch BIGINT NOT NULL,
	micro_capacity INTEGER NOT NULL,
	giant_capacity INTEGER NOT NULL,
	total_capacity INTEGER NOT NULL,
	tenant_capacity INTEGER NOT NULL,
	virtual_clock FLOAT NOT NULL,
	PRIMARY KEY (id),
	CONSTRAINT ck_copy_control_capacity CHECK (micro_capacity > 0 AND giant_capacity > 0 AND total_capacity > 0 AND tenant_capacity > 0)
)""",
 )

_INPUT_DDL = """CREATE TABLE IF NOT EXISTS copy_work_inputs (
	task_id UUID NOT NULL,
	input_key VARCHAR(32) NOT NULL,
	ordinal BIGINT NOT NULL,
	payload JSON NOT NULL,
	PRIMARY KEY (task_id, input_key, ordinal),
	FOREIGN KEY(task_id) REFERENCES copy_work_tasks (id) ON DELETE CASCADE
)"""


def upgrade():
    for statement in _DDL:
        op.execute(statement)
    op.execute(_INPUT_DDL)
    op.execute('ALTER TABLE copy_workflows ADD COLUMN IF NOT EXISTS capture_attempt bigint NOT NULL DEFAULT 0')
    op.execute('ALTER TABLE copy_work_tasks ADD COLUMN IF NOT EXISTS capture_attempt bigint NOT NULL DEFAULT 0')
    op.execute('ALTER TABLE copy_work_tasks ADD COLUMN IF NOT EXISTS execution_s double precision NOT NULL DEFAULT 0')
    op.execute('CREATE INDEX IF NOT EXISTS ix_copy_workflow_recovery ON copy_workflows(state,created_at,id)')
    op.execute("""CREATE OR REPLACE FUNCTION tmvault_copy_workflow_delete_guard() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF OLD.state IN ('DRAINING','WAITING','RESUMING') THEN
            RAISE EXCEPTION 'active copy workflow must settle before parent deletion' USING ERRCODE='55000';
          END IF;
          RETURN OLD;
        END $$""")
    op.execute('DROP TRIGGER IF EXISTS copy_workflow_delete_guard ON copy_workflows')
    op.execute('CREATE TRIGGER copy_workflow_delete_guard BEFORE DELETE ON copy_workflows FOR EACH ROW EXECUTE FUNCTION tmvault_copy_workflow_delete_guard()')

def downgrade():
    connection = op.get_bind()
    active = connection.execute(text("SELECT EXISTS(SELECT 1 FROM copy_workflows WHERE state IN ('DRAINING','WAITING','RESUMING')) OR EXISTS(SELECT 1 FROM copy_work_control WHERE enabled)" )).scalar()
    if active:
        raise RuntimeError('active copy work must be drained and disabled before downgrade')
    for table in ('copy_work_inputs','copy_work_items','copy_work_tasks','copy_work_queues','copy_workflows','copy_work_control'):
        op.execute('DROP TABLE '+table)
    op.execute('DROP FUNCTION IF EXISTS tmvault_copy_workflow_delete_guard()')
