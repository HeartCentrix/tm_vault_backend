"""A partitioned OneDrive run whose lost files the per-file retry queue rescued is a complete
restore point. Railway 2026-11-06: a ~60 s SeaweedFS "No more writable volumes" gap failed one
file in each of Amit's 8 shards → snapshot FAILED ("no reason recorded"); the queue rescued all
8 within 6 min, but nothing re-derived the snapshot — 788/788 files blobbed yet hidden from
Recovery (it lists COMPLETED/PARTIAL only), delta cursor never promoted, and AUTO_RETRY
re-copied all 127 GiB."""
import ast
import asyncio
import os
import pathlib
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from shared.database import Base
import shared.models as models
from shared.onedrive_rescue_settle import settle_rescued_onedrive_snapshot
import shared.onedrive_rescue_settle as rescue_settle

pytestmark = pytest.mark.integration
REPO = pathlib.Path(__file__).resolve().parents[2]
LOST = "OneDrive required file content failed for 1 file(s)"


@pytest_asyncio.fixture
async def factory():
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires explicitly configured disposable PostgreSQL")
    schema = "od_settle_" + uuid4().hex
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}, "statement_cache_size": 0})
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await conn.run_sync(Base.metadata.create_all)
    yield async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    await engine.dispose()


async def scenario(factory, *, retry_status="RESCUED", shard_message=LOST, unblobbed_size=0):
    ids = dict(org=uuid4(), tenant=uuid4(), resource=uuid4(), backend=uuid4(), job=uuid4(), snap=uuid4())
    async with factory.begin() as s:
        s.add(models.Organization(id=ids["org"], name="Synthetic", slug=uuid4().hex))
        await s.flush()
        s.add(models.Tenant(id=ids["tenant"], org_id=ids["org"], display_name="T", type=models.TenantType.M365))
        s.add(models.StorageBackend(id=ids["backend"], kind="s3", name=uuid4().hex, endpoint="https://s3.invalid", secret_ref="none"))
        await s.flush()
        s.add(models.Resource(id=ids["resource"], tenant_id=ids["tenant"], type=models.ResourceType.USER_ONEDRIVE,
            external_id="drive", display_name="OneDrive — Synthetic", last_backup_job_id=None, last_backup_status="FAILED",
            extra_data={"onedrive_delta_link_pending": "https://graph/delta?token=new",
                        "onedrive_delta_link_pending_snapshot": str(ids["snap"]),
                        "onedrive_delta_link": "https://graph/delta?token=old"}))
        s.add(models.Job(id=ids["job"], type=models.JobType.BACKUP, status=models.JobStatus.PARTIAL,
            tenant_id=ids["tenant"], batch_resource_ids=[ids["resource"]]))
        await s.flush()
        await s.execute(text("UPDATE resources SET last_backup_job_id = :j WHERE id = :r"), {"j": ids["job"], "r": ids["resource"]})
        s.add(models.Snapshot(id=ids["snap"], resource_id=ids["resource"], job_id=ids["job"], backend_id=ids["backend"],
            status=models.SnapshotStatus.FAILED, item_count=3,
            extra_data={"partition_count": 2, "failure_reason": "2 of 2 shard(s) failed outright",
                        "degraded_reason": "2 of 2 shard(s) failed outright"}))
        await s.flush()
        for i, status, fs in ((0, "COMPLETED", None), (1, "FAILED", {"reason": "EnumerationFailed", "message": shard_message})):
            s.add(models.SnapshotPartition(id=uuid4(), snapshot_id=ids["snap"], tenant_id=ids["tenant"], resource_id=ids["resource"],
                job_id=ids["job"], partition_type="ONEDRIVE_FILES", drive_id="d", partition_index=i, file_ids=[],
                total_files=1, total_bytes_est=1, status=status, failure_state=fs))
        rescued = uuid4()
        for iid, name, blob, size in ((uuid4(), "kept.bin", "b/kept", 10), (rescued, "seed_2g.bin", "b/retry-1", 20),
                                      (uuid4(), "Default.rdp", None, unblobbed_size)):
            s.add(models.SnapshotItem(id=iid, snapshot_id=ids["snap"], tenant_id=ids["tenant"], external_id=name,
                item_type="ONEDRIVE_FILE", name=name, blob_path=blob, content_size=1431,
                backend_id=ids["backend"], extra_data={"raw": {"size": size}}))
        s.add(models.OneDriveFileRetry(id=uuid4(), tenant_id=ids["tenant"], resource_id=ids["resource"], snapshot_id=ids["snap"],
            file_external_id="seed_2g.bin", file_name="seed_2g.bin", drive_id="d", file_payload={"id": "seed_2g.bin"},
            attempt_count=1, status=retry_status, rescued_snapshot_item_id=rescued if retry_status == "RESCUED" else None))
    return ids


async def state(factory, ids):
    async with factory() as s:
        snap = await s.get(models.Snapshot, ids["snap"])
        res = await s.get(models.Resource, ids["resource"])
        job = await s.get(models.Job, ids["job"])
        return snap, res, job


async def test_fully_rescued_snapshot_becomes_a_complete_restore_point(factory):
    ids = await scenario(factory)
    async with factory.begin() as s:
        assert await settle_rescued_onedrive_snapshot(s, ids["snap"]) is True
    snap, res, job = await state(factory, ids)
    assert snap.status == models.SnapshotStatus.COMPLETED
    assert "failure_reason" not in snap.extra_data and "degraded_reason" not in snap.extra_data
    assert snap.extra_data["rescued_after_finalize"]
    assert res.extra_data["onedrive_delta_link"] == "https://graph/delta?token=new"
    assert "onedrive_delta_link_pending" not in res.extra_data
    assert "onedrive_delta_link_pending_snapshot" not in res.extra_data
    assert res.last_backup_status == "COMPLETED"
    assert job.status == models.JobStatus.COMPLETED
    async with factory.begin() as s:  # idempotent
        assert await settle_rescued_onedrive_snapshot(s, ids["snap"]) is False


@pytest.mark.parametrize("kw", [
    {"retry_status": "PENDING"},                                   # rescue still owed
    {"retry_status": "FAILED_PERMANENT"},                          # rescue gave up
    {"shard_message": "Graph delta enumeration failed"},           # not a file-content loss
    {"unblobbed_size": 5},                                         # non-empty file still has no bytes
])
async def test_anything_short_of_full_rescue_is_left_alone(factory, kw):
    ids = await scenario(factory, **kw)
    async with factory.begin() as s:
        assert await settle_rescued_onedrive_snapshot(s, ids["snap"]) is False
    snap, res, job = await state(factory, ids)
    assert snap.status == models.SnapshotStatus.FAILED
    assert res.extra_data["onedrive_delta_link"] == "https://graph/delta?token=old"
    assert job.status == models.JobStatus.PARTIAL


async def test_duplicate_snapshot_does_not_cover_an_undispatched_resource(factory):
    ids = await scenario(factory)
    missing = uuid4()
    async with factory.begin() as s:
        s.add(models.Resource(id=missing, tenant_id=ids['tenant'], type=models.ResourceType.USER_ONEDRIVE,
            external_id='missing-drive', display_name='Missing OneDrive'))
        job = await s.get(models.Job, ids['job'])
        job.batch_resource_ids = [ids['resource'], missing]
        s.add(models.Snapshot(resource_id=ids['resource'], job_id=ids['job'], backend_id=ids['backend'],
            status=models.SnapshotStatus.COMPLETED, extra_data={}))
    async with factory.begin() as s:
        assert await settle_rescued_onedrive_snapshot(s, ids['snap'])
    snap, _, job = await state(factory, ids)
    assert snap.status == models.SnapshotStatus.COMPLETED
    assert job.status == models.JobStatus.PARTIAL


async def test_cancelled_parent_cannot_promote_snapshot_or_pending_cursor(factory):
    ids = await scenario(factory)
    async with factory.begin() as s:
        (await s.get(models.Job, ids['job'])).status = models.JobStatus.CANCELLED
    async with factory.begin() as s:
        assert not await settle_rescued_onedrive_snapshot(s, ids['snap'])
    snap, resource, job = await state(factory, ids)
    assert snap.status == models.SnapshotStatus.FAILED
    assert resource.extra_data['onedrive_delta_link'] == 'https://graph/delta?token=old'
    assert job.status == models.JobStatus.CANCELLED


async def test_last_concurrent_rescues_converge_the_parent_job(factory, monkeypatch):
    first, second = await scenario(factory), await scenario(factory)
    async with factory.begin() as s:
        (await s.get(models.Resource, second['resource'])).tenant_id = first['tenant']
        (await s.get(models.Snapshot, second['snap'])).job_id = first['job']
        (await s.get(models.Job, first['job'])).batch_resource_ids = [first['resource'], second['resource']]
        await s.execute(text('UPDATE snapshot_partitions SET job_id=:j WHERE snapshot_id=:sid'),
                        {'j': first['job'], 'sid': second['snap']})
    first_rollup_written, second_rollup_started, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    from sqlalchemy.ext.asyncio import AsyncSession
    original = AsyncSession.execute

    async def execute(session, statement, params=None, **kw):
        is_rollup = statement is rescue_settle._JOB
        if is_rollup and params['sid'] == second['snap']:
            second_rollup_started.set()
        result = await original(session, statement, params, **kw)
        if is_rollup and params['sid'] == first['snap']:
            first_rollup_written.set()
            await release.wait()
        return result

    monkeypatch.setattr(AsyncSession, 'execute', execute)

    async def settle(ids):
        async with factory.begin() as s:
            return await settle_rescued_onedrive_snapshot(s, ids['snap'])

    a = asyncio.create_task(settle(first))
    b = None
    try:
        await asyncio.wait_for(first_rollup_written.wait(), timeout=3)
        b = asyncio.create_task(settle(second))
        try:
            await asyncio.wait_for(second_rollup_started.wait(), timeout=.2)
            await asyncio.sleep(.05)  # Let the old unlocked rollup reach its DB lock wait.
        except asyncio.TimeoutError:
            pass  # Correct serialization holds the second call before this statement.
    finally:
        release.set()
        assert await asyncio.wait_for(a, timeout=3)
        if b is not None:
            assert await asyncio.wait_for(b, timeout=3)
    _, _, job = await state(factory, first)
    assert job.status == models.JobStatus.COMPLETED


def _calls(func_name, callee):
    tree = ast.parse((REPO / "workers/backup-worker/main.py").read_text())
    fn = next(n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef) and n.name == func_name)
    return callee in ast.unparse(fn)


def test_worker_settles_after_each_rescue_and_at_partition_finalize():
    assert _calls("_retry_one_onedrive_file", "_settle_rescued_onedrive_snapshot")
    assert _calls("_finalize_partitioned_snapshot", "_settle_rescued_onedrive_snapshot")


def test_auto_retry_waits_for_outstanding_file_rescues():
    sweep = (REPO / "services/backup-scheduler/main.py").read_text()
    body = sweep[sweep.index("async def _job_retry_tick"):sweep.index("async def start_job_retry_loop")]
    assert "onedrive_file_retries" in body and "'PENDING','IN_PROGRESS'" in body.replace(" ", "")
