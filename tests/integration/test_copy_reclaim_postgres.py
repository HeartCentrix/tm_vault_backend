"""Startup reclaim must leave required-copy ownership generations intact."""
import uuid

import pytest
from sqlalchemy import text

from test_copy_guard_wiring_postgres import copy_guard_db


pytestmark = pytest.mark.integration


@pytest.mark.parametrize('state,pending', [
    ('DRAINING', True), ('WAITING', True), ('RESUMING', True), ('COMPLETED', False),
])
async def test_startup_reclaim_preserves_pending_copy_fences_and_reclaims_unrelated_work(
    copy_guard_db, monkeypatch, state, pending,
):
    from shared import copy_workflow, reclaim

    db = copy_guard_db
    old_worker, current_worker = uuid.uuid4(), uuid.uuid4()
    replica = 'backup-worker-reclaim-regression'
    unrelated = {table: uuid.uuid4() for table in ('jobs', 'snapshots', 'snapshot_partitions')}
    protected = {'jobs': db.job, 'snapshots': db.snapshot, 'snapshot_partitions': db.partition}
    before = {}
    async with db.factory.begin() as session:
        await session.execute(text('ALTER TABLE worker_heartbeats ADD COLUMN replica_id text'))
        await session.execute(text("INSERT INTO worker_heartbeats(worker_id,last_seen_at,replica_id) "
                                   "VALUES (:old,NOW(),:rep),(:current,NOW(),:rep)"),
                              {'old': old_worker, 'current': current_worker, 'rep': replica})
        await session.execute(text('UPDATE copy_workflows SET state=:state WHERE id=:w'),
                              {'state': state, 'w': db.workflow})
        await session.execute(text("INSERT INTO jobs(id,status) VALUES (:id,'RUNNING')"),
                              {'id': unrelated['jobs']})
        await session.execute(text("INSERT INTO snapshots(id,resource_id,job_id,status) "
                                   "VALUES (:id,:resource,:job,'IN_PROGRESS')"),
                              {'id': unrelated['snapshots'], 'resource': db.resource, 'job': unrelated['jobs']})
        await session.execute(text("INSERT INTO snapshot_partitions(id,snapshot_id,job_id,status) "
                                   "VALUES (:id,:snapshot,:job,'IN_PROGRESS')"),
                              {'id': unrelated['snapshot_partitions'], 'snapshot': unrelated['snapshots'],
                               'job': unrelated['jobs']})
        for index, table in enumerate(protected):
            for row_id, token in ((protected[table], 100 + index), (unrelated[table], 200 + index)):
                await session.execute(text(f"UPDATE {table} SET lease_owner_id=:owner, "
                                           "lease_expires_at=NOW()+INTERVAL '10 minutes',lease_token=:token "
                                           "WHERE id=:id"), {'owner': old_worker, 'token': token, 'id': row_id})
            before[table] = (await session.execute(text(
                f'SELECT lease_owner_id,lease_expires_at,lease_token FROM {table} WHERE id=:id'
            ), {'id': protected[table]})).one()

    # Exercise the real startup latch with producers disabled after a rollback.
    monkeypatch.setattr(copy_workflow, '_COPY_GUARDS_ACTIVE', False)
    await copy_workflow.initialize_copy_guards(db.factory)
    monkeypatch.setattr(reclaim, 'async_session_factory', db.factory)
    touched = await reclaim.reclaim_for_replica(replica_id=replica, worker_uuid=current_worker)
    assert touched == (3 if pending else 6)
    async with db.factory() as session:
        for index, table in enumerate(protected):
            row = (await session.execute(text(
                f'SELECT lease_owner_id,lease_expires_at,lease_token FROM {table} WHERE id=:id'
            ), {'id': protected[table]})).one()
            if pending:
                assert tuple(row) == tuple(before[table]), table
            else:
                assert row.lease_owner_id is None
                assert row.lease_token == 101 + index
            other = (await session.execute(text(
                f'SELECT lease_owner_id,lease_token,lease_expires_at<NOW() AS expired FROM {table} WHERE id=:id'
            ), {'id': unrelated[table]})).one()
            assert other.lease_owner_id is None
            assert other.lease_token == 201 + index
            assert other.expired is True
        heartbeats = dict((await session.execute(text(
            "SELECT worker_id,last_seen_at<NOW()-INTERVAL '59 minutes' AS tombstoned "
            'FROM worker_heartbeats'
        ))).all())
        assert heartbeats == {old_worker: True, current_worker: False}
    assert await reclaim.reclaim_for_replica(replica_id=replica, worker_uuid=current_worker) == 0
