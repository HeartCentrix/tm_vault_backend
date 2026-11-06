"""Continuous protected-size retained references and transactional change capture."""

from alembic import context, op
from sqlalchemy import text
from shared.protected_size_schema import ONLINE_INDEXES, build_index_online, statements

revision = "0014"
down_revision = "0013"
branch_labels = None
depends_on = None


def upgrade():
    # Build the big-table indexes first without blocking writes (TM: snapshot_items ~532M rows in
    # 16 partitions); the plain CREATE INDEX IF NOT EXISTS in statements() then skips them.
    with op.get_context().autocommit_block():
        for name, table, columns in ONLINE_INDEXES:
            build_index_online(op.get_bind(), name, table, columns)
    connection = op.get_bind()
    schema = connection.scalar(text("SELECT current_schema()"))
    runtime_role = context.get_x_argument(as_dictionary=True).get(
        "protected_size_runtime_role"
    )
    for statement in statements(schema, runtime_role=runtime_role):
        op.execute(statement)


def downgrade():
    raise RuntimeError(
        "Protected-size journal and ledger are durable; forward migration required"
    )
