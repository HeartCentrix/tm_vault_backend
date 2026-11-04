"""The batch finalizer must not wait for children that job dispatch never backs up.

Railway 2026-11-04 10-user full: job-service's SLA workload filter skipped Amit's USER_ARCHIVE
(Manual policy, backup_exchange_archive=False - the column default), so it never got a
snapshot and the finalizer kept the batch IN_PROGRESS forever after every job completed."""
from datetime import datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select

from test_chat_catalogue_postgres import store  # noqa: F401
from shared import models

pytestmark = pytest.mark.integration


async def _batch_with_archive(store, *, archive_enabled):
    now = datetime.utcnow()
    async with store.factory.begin() as db:
        policy = models.SlaPolicy(id=uuid4(), tenant_id=store.tenant, name="Manual-test",
                                  backup_exchange=True, backup_exchange_archive=archive_enabled)
        db.add(policy)
        await db.flush()
        user = models.Resource(id=uuid4(), tenant_id=store.tenant, type=models.ResourceType.ENTRA_USER,
                               external_id="user-1", display_name="User One", sla_policy_id=policy.id)
        db.add(user)
        await db.flush()
        mail = models.Resource(id=uuid4(), tenant_id=store.tenant, type=models.ResourceType.USER_MAIL,
                               external_id="user-1:mail", display_name="Mail", parent_resource_id=user.id,
                               sla_policy_id=policy.id)
        archive = models.Resource(id=uuid4(), tenant_id=store.tenant, type=models.ResourceType.USER_ARCHIVE,
                                  external_id="user-1:archive", display_name="Archive", parent_resource_id=user.id,
                                  sla_policy_id=policy.id)
        db.add_all([mail, archive])
        batch = models.BackupBatch(id=uuid4(), tenant_id=store.tenant, scope_user_ids=[user.id], source="manual_bulk",
                                   status="IN_PROGRESS", created_at=now - timedelta(minutes=30))
        db.add(batch)
        await db.flush()
        for resource in (user, mail):
            db.add(models.Snapshot(id=uuid4(), resource_id=resource.id, backend_id=store.backend,
                                   status=models.SnapshotStatus.COMPLETED, created_at=now - timedelta(minutes=10)))
    return batch.id


@pytest.mark.parametrize("archive_enabled, expected", [(False, "COMPLETED"), (True, None)])
async def test_finalizer_ignores_children_their_sla_does_not_back_up(store, archive_enabled, expected):
    from shared.batch_rollup import _finalize_batch_if_complete
    batch_id = await _batch_with_archive(store, archive_enabled=archive_enabled)
    async with store.factory() as session:
        assert await _finalize_batch_if_complete(batch_id, session) == expected
    async with store.factory() as session:
        status = await session.scalar(select(models.BackupBatch.status).where(models.BackupBatch.id == batch_id))
    assert status == (expected or "IN_PROGRESS")
