"""Continuous protected-size retained references and transactional change capture."""

from alembic import context, op
from sqlalchemy import text
from shared.protected_size_schema import ONLINE_INDEXES, build_index_online, statements

revision = "0014"
down_revision = "0013"
branch_labels = None
depends_on = None


def upgrade():
    # Build the big-table indexes first without blocking writes (TM: snapshot_items ~806M rows,
    # 1.45 TB, one plain table); statements() then skips them, and skips any an existing index
    # already covers (TM's PK btree(id) serves the (id) lookups).
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
