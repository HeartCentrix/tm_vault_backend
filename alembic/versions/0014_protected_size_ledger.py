"""Continuous protected-size retained references and transactional change capture."""
from alembic import op
from sqlalchemy import text
from shared.protected_size_schema import statements

revision = '0014'
down_revision = '0013'
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()
    schema = connection.scalar(text('SELECT current_schema()'))
    for statement in statements(schema):
        op.execute(statement)


def downgrade():
    raise RuntimeError('Protected-size journal and ledger are durable; forward migration required')
