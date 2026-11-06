"""Run legacy guard callers against durable waits in disposable PostgreSQL."""
import json
import os
from types import SimpleNamespace
import uuid

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def copy_guard_db(monkeypatch):
    url = os.getenv("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires an explicitly configured disposable PostgreSQL database")
    schema = "copy_guard_test_" + uuid.uuid4().hex
    engine = create_async_engine(url, connect_args={
        "server_settings": {"search_path": schema}, "statement_cache_size": 0,
    })
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        for ddl in (
            "CREATE TYPE snapshotstatus AS ENUM ('IN_PROGRESS','COMPLETED','PARTIAL','FAILED','PENDING_DELETION')",
            "CREATE TYPE jobstatus AS ENUM ('QUEUED','RUNNING','COMPLETED','PARTIAL','FAILED','CANCELLED','CANCELLING','RETRYING')",
            "CREATE TABLE resources (id uuid PRIMARY KEY, type text DEFAULT 'USER_MAILBOX', archived_at timestamp, sla_policy_id uuid)",
            "CREATE TABLE backup_batches (id uuid PRIMARY KEY, status text DEFAULT 'IN_PROGRESS', "
            "created_at timestamp DEFAULT NOW()-INTERVAL '3 days', scope_user_ids uuid[], completed_at timestamp)",
            "CREATE TABLE jobs (id uuid PRIMARY KEY, status jobstatus DEFAULT 'RUNNING', "
            "lease_owner_id uuid, lease_expires_at timestamp, lease_token bigint DEFAULT 0, "
            "created_at timestamp DEFAULT NOW()-INTERVAL '2 days', completed_at timestamp, spec json DEFAULT '{}')",
            "CREATE TABLE snapshots (id uuid PRIMARY KEY, resource_id uuid, job_id uuid, "
            "status snapshotstatus DEFAULT 'IN_PROGRESS', lease_owner_id uuid, lease_expires_at timestamp, "
            "lease_token bigint DEFAULT 0, started_at timestamp DEFAULT NOW()-INTERVAL '2 days', "
            "created_at timestamp DEFAULT NOW()-INTERVAL '2 days', completed_at timestamp, duration_secs int, "
            "item_count bigint DEFAULT 1, bytes_total bigint DEFAULT 1, hc_drain_status text, "
            "reuse_chain_root_id uuid, reuse_of_snapshot_id uuid, extra_data json DEFAULT '{}')",
            "CREATE TABLE snapshot_partitions (id uuid PRIMARY KEY, snapshot_id uuid, job_id uuid, "
            "tenant_id uuid, resource_id uuid, status text DEFAULT 'IN_PROGRESS', lease_owner_id uuid, "
            "lease_expires_at timestamp, lease_token bigint DEFAULT 0, worker_id uuid, "
            "started_at timestamp DEFAULT NOW()-INTERVAL '2 days', completed_at timestamp, "
            "enqueued_at timestamp DEFAULT NOW()-INTERVAL '2 days', retry_count int DEFAULT 0, "
            "requeue_count int DEFAULT 0, partition_type text DEFAULT 'ONEDRIVE_FILES', "
            "partition_index int DEFAULT 0, drive_id text, file_ids json, payload json, failure_state json DEFAULT '{}')",
            "CREATE TABLE worker_heartbeats (worker_id uuid, last_seen_at timestamp)",
            "CREATE TABLE snapshot_items (id uuid PRIMARY KEY, snapshot_id uuid REFERENCES snapshots(id), "
            "tenant_id uuid, blob_path text, content_checksum text, content_size bigint, created_at timestamp)",
            "CREATE TABLE copy_workflows (id uuid PRIMARY KEY, job_id uuid, target_snapshot_id uuid, "
            "partition_id uuid, state text DEFAULT 'WAITING', parent_settle_pending boolean DEFAULT false)",
            "CREATE TABLE copy_work_tasks (id uuid PRIMARY KEY, workflow_id uuid, state text DEFAULT 'READY', "
            "params json DEFAULT '{}', lease_expires_at timestamptz, generation bigint DEFAULT 0, error json)",
            "CREATE TABLE copy_work_items (task_id uuid, ordinal bigint, source_snapshot_id uuid, payload json)",
            "CREATE TABLE copy_work_control (id text PRIMARY KEY, enabled boolean DEFAULT false, "
            "epoch bigint DEFAULT 0, micro_capacity int DEFAULT 3, giant_capacity int DEFAULT 3, "
            "total_capacity int DEFAULT 6, tenant_capacity int DEFAULT 2, virtual_clock float DEFAULT 0)",
        ):
            await conn.execute(text(ddl))
    factory = async_sessionmaker(engine, expire_on_commit=False)
    job, snapshot, partition, source, workflow, task, resource, tenant = [uuid.uuid4() for _ in range(8)]
    async with factory() as session:
        await session.execute(text("INSERT INTO resources(id) VALUES (:r)"), {"r": resource})
        await session.execute(text("INSERT INTO jobs(id) VALUES (:j)"), {"j": job})
        await session.execute(text("INSERT INTO snapshots(id,job_id,resource_id) VALUES (:s,:j,:r),(:src,NULL,:r)"),
                              {"s": snapshot, "j": job, "r": resource, "src": source})
        await session.execute(text("UPDATE snapshots SET status='COMPLETED' WHERE id=:s"), {"s": source})
        await session.execute(text("INSERT INTO snapshot_partitions(id,snapshot_id,job_id) VALUES (:p,:s,:j)"),
                              {"p": partition, "s": snapshot, "j": job})
        await session.execute(text("INSERT INTO copy_workflows(id,job_id,target_snapshot_id,partition_id) "
                                   "VALUES (:w,:j,:s,:p)"), {"w": workflow, "j": job, "s": snapshot, "p": partition})
        await session.execute(text("INSERT INTO copy_work_tasks(id,workflow_id,params) "
                                   "VALUES (:t,:w,CAST(:params AS json))"),
                              {"t": task, "w": workflow, "params": json.dumps({"prior_snapshot_id": str(source)})})
        await session.commit()
    import shared.copy_workflow as workflow_api
    # Restart after rollback must discover pending work with producers disabled.
    monkeypatch.setenv("COPY_WORK_ENABLED", "false")
    monkeypatch.setattr(workflow_api, "_COPY_GUARDS_ACTIVE", False)
    try:
        await workflow_api.initialize_copy_guards(factory)
        yield SimpleNamespace(factory=factory, job=job, snapshot=snapshot, partition=partition,
                              source=source, workflow=workflow, task=task, tenant=tenant, resource=resource)
    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await engine.dispose()


@pytest.mark.parametrize("table,id_attr,status", [
    ("jobs", "job", "RUNNING"), ("snapshots", "snapshot", "IN_PROGRESS"),
    ("snapshot_partitions", "partition", "IN_PROGRESS"),
])
async def test_claim_rejects_waiting_copy_leaf_and_parent(copy_guard_db, table, id_attr, status):
    from shared.lease import claim
    db = copy_guard_db
    async with db.factory() as session:
        result = await claim(session, table=table, work_id=getattr(db, id_attr),
                             owner_id=str(uuid.uuid4()), in_progress_status=status)
        assert result is None
        assert await session.scalar(text(f"SELECT lease_token FROM {table} WHERE id=:id"),
                                    {"id": getattr(db, id_attr)}) == 0


@pytest.mark.parametrize("status,requeues,unclaimed_count", [
    ("IN_PROGRESS", 0, 0), ("IN_PROGRESS", 3, 0),
    ("QUEUED", 0, 0), ("QUEUED", 0, 5),
])
async def test_reconciler_does_not_requeue_or_finalize_waiting_work(
    copy_guard_db, status, requeues, unclaimed_count,
):
    from shared.reconciler import sweep_orphans
    db = copy_guard_db
    async with db.factory() as session:
        await session.execute(text("UPDATE snapshot_partitions SET status=:status, "
                                   "requeue_count=:rq, started_at=CASE WHEN :status='QUEUED' "
                                   "THEN NULL ELSE started_at END, failure_state=CAST(:failure AS json) "
                                   "WHERE id=:id"),
                              {"id": db.partition, "status": status, "rq": requeues,
                               "failure": json.dumps({"unclaimed_republish_count": unclaimed_count})})
        await session.commit()
        result = await sweep_orphans(session)
        assert result.total() == 0
        assert await session.scalar(text("SELECT status FROM snapshot_partitions WHERE id=:id"),
                                    {"id": db.partition}) == status
        assert await session.scalar(text("SELECT status::text FROM snapshots WHERE id=:id"),
                                    {"id": db.snapshot}) == "IN_PROGRESS"
        # Even a prematurely terminal snapshot cannot finalize its parent job.
        await session.execute(text("UPDATE snapshots SET status='PARTIAL' WHERE id=:s"), {"s": db.snapshot})
        await session.execute(text("UPDATE snapshot_partitions SET status='COMPLETED' WHERE id=:p"), {"p": db.partition})
        await session.commit()
        assert (await sweep_orphans(session)).jobs_finalized == 0
        assert await session.scalar(text("SELECT status::text FROM jobs WHERE id=:id"), {"id": db.job}) == "RUNNING"
        assert await session.scalar(text("SELECT status::text FROM snapshots WHERE id=:id"),
                                    {"id": db.snapshot}) == "PARTIAL"


async def test_reconciler_does_not_fail_nonpartitioned_copy_wait(copy_guard_db):
    from shared.reconciler import sweep_orphans
    db = copy_guard_db
    async with db.factory() as session:
        await session.execute(text("DELETE FROM snapshot_partitions WHERE id=:p"), {"p": db.partition})
        await session.execute(text("UPDATE copy_workflows SET partition_id=NULL WHERE id=:w"), {"w": db.workflow})
        await session.commit()
        assert (await sweep_orphans(session)).snapshots_finalized == 0
        assert await session.scalar(text("SELECT status::text FROM snapshots WHERE id=:s"),
                                    {"s": db.snapshot}) == "IN_PROGRESS"


@pytest.mark.parametrize("source_param", ["prior_snapshot_id", "prior_snap_id"])
async def test_retention_defers_unprepared_explicit_prior_and_waiting_target(copy_guard_db, source_param):
    from shared.retention_cleanup import _delete_snapshots
    db = copy_guard_db
    async with db.factory() as session:
        await session.execute(text("UPDATE copy_work_tasks SET params=CAST(:params AS json) WHERE id=:t"),
                              {"params": json.dumps({source_param: str(db.source)}), "t": db.task})
        await session.execute(text("UPDATE snapshots SET status='COMPLETED' WHERE id=:s"), {"s": db.snapshot})
        await session.execute(text("UPDATE snapshot_partitions SET status='COMPLETED' WHERE id=:p"), {"p": db.partition})
        await session.commit()
        assert await _delete_snapshots(session, {db.source, db.snapshot}) == 0
        assert await session.scalar(text("SELECT count(*) FROM snapshots")) == 2


async def test_blob_manifest_pin_survives_source_item_removal(copy_guard_db):
    from shared.backup_cleanup import BlobCandidate, select_deletable_blob_paths
    db = copy_guard_db
    path = "frozen/content-addressed-payload"
    async with db.factory() as session:
        await session.execute(text("INSERT INTO copy_work_items VALUES (:t,1,:s,CAST(:p AS json))"),
                              {"t": db.task, "s": db.source, "p": json.dumps({"blob_path": path})})
        await session.commit()
        plan = await select_deletable_blob_paths(
            session, doomed_snapshot_ids=[db.source], protected_roots=set(),
            candidates=[BlobCandidate(snapshot_id=db.source, tenant_id=db.tenant,
                                       blob_path=path, content_checksum="checksum")],
        )
        assert plan.deletable == frozenset()
        assert plan.retained[path] == "pending_copy"


@pytest.mark.parametrize("target", ["snapshot", "source"])
async def test_cancel_cleanup_preserves_pending_target_and_pinned_source(copy_guard_db, target):
    from shared.backup_cleanup import cleanup_cancelled_snapshots
    db = copy_guard_db
    cancelled_job = uuid.uuid4()
    snapshot_id = getattr(db, target)
    async with db.factory() as session:
        await session.execute(text("INSERT INTO jobs(id,status) VALUES (:j,'CANCELLED')"), {"j": cancelled_job})
        await session.execute(text("UPDATE snapshots SET job_id=:j WHERE id=:s"),
                              {"j": cancelled_job, "s": snapshot_id})
        await session.commit()

    class Shard:
        async def delete_blob(self, *args):
            pytest.fail("cancel cleanup must not delete bytes beneath required copy work")

    result = await cleanup_cancelled_snapshots(
        job_id=cancelled_job, session_factory=db.factory, shard=Shard(),
        container_resolver=lambda item: "backup-test",
    )
    assert result["snapshots"] == 0
    async with db.factory() as session:
        assert await session.scalar(text("SELECT count(*) FROM snapshots WHERE id=:s"), {"s": snapshot_id}) == 1


async def test_batch_finalizer_waits_for_required_copy_even_if_snapshot_is_terminal(copy_guard_db):
    from shared.batch_rollup import _finalize_batch_if_complete
    db = copy_guard_db
    batch = uuid.uuid4()
    async with db.factory() as session:
        await session.execute(text("INSERT INTO backup_batches(id,scope_user_ids) VALUES (:b,ARRAY[:r]::uuid[])"),
                              {"b": batch, "r": db.resource})
        await session.execute(text("UPDATE jobs SET spec=CAST(:spec AS json) WHERE id=:j"),
                              {"spec": json.dumps({"batch_id": str(batch)}), "j": db.job})
        await session.execute(text("UPDATE snapshots SET status='COMPLETED' WHERE id=:s"), {"s": db.snapshot})
        await session.execute(text("UPDATE snapshot_partitions SET status='COMPLETED' WHERE id=:p"), {"p": db.partition})
        await session.commit()
        assert await _finalize_batch_if_complete(batch, session) is None
        assert await session.scalar(text("SELECT status FROM backup_batches WHERE id=:b"), {"b": batch}) == "IN_PROGRESS"
        await session.execute(text("UPDATE copy_workflows SET state='COMPLETED' WHERE id=:w"), {"w": db.workflow})
        await session.commit()
        assert await _finalize_batch_if_complete(batch, session) == "COMPLETED"
