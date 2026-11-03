"""Version observation proofs while preserving immutable captured payloads."""
from alembic import op

revision = "0015"
down_revision = "0014"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TABLE chat_inventories ADD COLUMN IF NOT EXISTS observation_hash_version integer NOT NULL DEFAULT 1")
    op.execute("""DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint
                       WHERE conrelid = 'chat_inventories'::regclass
                       AND conname = 'ck_chat_inventory_observation_version') THEN
            ALTER TABLE chat_inventories ADD CONSTRAINT ck_chat_inventory_observation_version
                CHECK (observation_hash_version IN (1, 2));
        END IF;
    END $$""")


def downgrade():
    raise RuntimeError("Observation proof versions must remain available to readers; use a forward migration")
