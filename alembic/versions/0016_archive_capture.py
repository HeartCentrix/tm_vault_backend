"""Persist archive folder coverage, observations and exact content receipts."""
from alembic import op

revision = '0016'
down_revision = '0015'
branch_labels = None
depends_on = None


def upgrade():
    # Clone only these additive tables; referenced tables already exist.
    from shared.models import ArchiveFolderWork, ArchiveItemWork
    bind = op.get_bind()
    for model in (ArchiveFolderWork, ArchiveItemWork):
        model.__table__.create(bind, checkfirst=True)


def downgrade():
    raise RuntimeError('Archive capture receipts must remain available; use a forward migration')
