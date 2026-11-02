"""Durable provider continuation, receipts and fair service clocks."""
from alembic import op
revision = '0017'
down_revision = '0016'
branch_labels = None
depends_on = None


def upgrade():
    from shared.models import ProviderWorkQueue, ProviderWorkControl, ProviderWork, ProviderWorkUnit
    for model in (ProviderWorkQueue, ProviderWorkControl, ProviderWork, ProviderWorkUnit):
        model.__table__.create(op.get_bind(), checkfirst=True)


def downgrade():
    raise RuntimeError('provider receipts require an explicit retention-safe forward migration')
