"""Captured chat names, separate from message and attachment rows."""
from alembic import op
from sqlalchemy import text

revision = '0013'
down_revision = '0012'
branch_labels = None
depends_on = None

_DDL = (
    """CREATE TABLE IF NOT EXISTS chat_presentation_plans (
	snapshot_id UUID NOT NULL, 
	state VARCHAR(16) NOT NULL, 
	task_id UUID, 
	expected_count BIGINT NOT NULL, 
	published_count BIGINT NOT NULL, 
	PRIMARY KEY (snapshot_id), 
	CONSTRAINT ck_chat_presentation_plan_state CHECK (state IN ('PENDING','PUBLISHING','PUBLISHED','ABORTED')), 
	FOREIGN KEY(snapshot_id) REFERENCES snapshots (id) ON DELETE CASCADE
)""",
    """CREATE TABLE IF NOT EXISTS chat_presentation_drafts (
	snapshot_id UUID NOT NULL, 
	workflow_id UUID NOT NULL, 
	capture_attempt BIGINT NOT NULL, 
	chat_id TEXT NOT NULL, 
	folder_path TEXT NOT NULL, 
	quality INTEGER NOT NULL, 
	provenance VARCHAR(64) NOT NULL, 
	conflicted BOOLEAN NOT NULL, 
	accepted_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (snapshot_id, workflow_id, capture_attempt, chat_id), 
	CONSTRAINT ck_chat_presentation_draft_quality CHECK (quality BETWEEN 0 AND 3), 
	FOREIGN KEY(snapshot_id) REFERENCES snapshots (id) ON DELETE CASCADE, 
	FOREIGN KEY(workflow_id) REFERENCES copy_workflows (id) ON DELETE CASCADE
)""",
    """CREATE TABLE IF NOT EXISTS snapshot_chat_presentations (
	snapshot_id UUID NOT NULL, 
	chat_id TEXT NOT NULL, 
	folder_path TEXT NOT NULL, 
	quality INTEGER NOT NULL, 
	provenance VARCHAR(64) NOT NULL, 
	conflicted BOOLEAN NOT NULL, 
	policy_version INTEGER NOT NULL, 
	PRIMARY KEY (snapshot_id, chat_id), 
	FOREIGN KEY(snapshot_id) REFERENCES snapshots (id) ON DELETE CASCADE
)""",
)


def upgrade():
    for statement in _DDL:
        op.execute(statement)


def downgrade():
    retained = op.get_bind().execute(text("SELECT EXISTS(SELECT 1 FROM chat_presentation_plans)")).scalar()
    if retained:
        raise RuntimeError('retain presentation readers and tables while captured snapshots or work remain')
    for table in ('chat_presentation_drafts','snapshot_chat_presentations','chat_presentation_plans'):
        op.execute('DROP TABLE '+table)
