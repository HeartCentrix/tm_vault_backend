"""An unchanged USER_MAIL incremental points at its prior full snapshot instead of copying it.

Railway 2026-11-05 (incremental #9): all 10 mailboxes had zero changes, yet each copied its whole
mailbox forward (Rohit 7,085 rows, 280 s wall for 31 s of copying)."""
from __future__ import annotations

from datetime import datetime, timedelta
import os
from pathlib import Path
import re
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

from shared.database import Base
import shared.models as models

pytestmark = pytest.mark.integration


def _reuse_trigger_ddl():
    """The production trigger (shared/database.py) that validates every reuse pointer."""
    source = (Path(__file__).parents[2] / "shared" / "database.py").read_text()
    function = re.search(r'"""(CREATE OR REPLACE FUNCTION validate_snapshot_reuse_target\(\).*?)"""', source, re.S).group(1)
    return [function, "CREATE TRIGGER snapshots_reuse_validate BEFORE INSERT OR UPDATE OF reuse_of_snapshot_id, "
            "reuse_chain_root_id ON snapshots FOR EACH ROW EXECUTE FUNCTION validate_snapshot_reuse_target();"]


@pytest_asyncio.fixture
async def mail():
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires explicitly configured disposable PostgreSQL")
    schema = "mail_reuse_test_" + uuid4().hex
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}, "statement_cache_size": 0})
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await conn.run_sync(Base.metadata.create_all)
        for ddl in _reuse_trigger_ddl():
            await conn.execute(text(ddl))
    factory = async_sessionmaker(engine, expire_on_commit=False)
    tenant, resource, backend = uuid4(), uuid4(), uuid4()
    async with factory.begin() as session:
        org = models.Organization(name="Synthetic", slug=uuid4().hex)
        session.add(org)
        await session.flush()
        session.add(models.Tenant(id=tenant, org_id=org.id, display_name="Synthetic"))
        session.add(models.StorageBackend(id=backend, kind="azure", name=uuid4().hex,
                    endpoint="https://storage.invalid", config={}, secret_ref="synthetic-reference"))
        await session.flush()
        session.add(models.Resource(id=resource, tenant_id=tenant, type=models.ResourceType.USER_MAIL,
                                    external_id="mailbox-alpha", display_name="Synthetic mailbox"))
    clock = [datetime(2026, 11, 5, 10)]

    async def snapshot(status=models.SnapshotStatus.IN_PROGRESS, rows=0, item_count=0, bytes_total=0, extra=None):
        job, sid = uuid4(), uuid4()
        clock[0] += timedelta(minutes=10)
        async with factory.begin() as session:
            session.add(models.Job(id=job, type=models.JobType.BACKUP, tenant_id=tenant, resource_id=resource,
                                   status=models.JobStatus.RUNNING))
            await session.flush()
            session.add(models.Snapshot(id=sid, resource_id=resource, job_id=job, backend_id=backend, status=status, started_at=clock[0],
                                        created_at=clock[0], item_count=item_count, bytes_total=bytes_total,
                                        new_item_count=item_count, bytes_added=bytes_total, extra_data=extra or {}))
            await session.flush()
            for n in range(rows):
                session.add(models.SnapshotItem(snapshot_id=sid, tenant_id=tenant, item_type="EMAIL", backend_id=backend,
                                                external_id=f"message-{n}", name=f"message {n}", content_size=10))
        return sid, job

    async def row(sid):
        async with factory() as session:
            return await session.get(models.Snapshot, sid)

    async def reuse(target, job):
        from shared.mail_snapshot_reuse import reuse_unchanged_mail_snapshot
        async with factory.begin() as session:
            return await reuse_unchanged_mail_snapshot(session, resource_id=resource, target_snapshot_id=target,
                                                       current_job_id=job)

    yield type("Mail", (), {"factory": factory, "snapshot": staticmethod(snapshot), "row": staticmethod(row),
                            "reuse": staticmethod(reuse)})
    await engine.dispose()


async def test_unchanged_mailbox_points_at_its_prior_full_snapshot(mail):
    from shared.snapshot_reuse import resolve_snapshot_items_target
    prior, _ = await mail.snapshot(models.SnapshotStatus.COMPLETED, rows=5, item_count=5, bytes_total=500)
    target, job = await mail.snapshot()
    assert await mail.reuse(target, job) == prior
    reused = await mail.row(target)
    assert (reused.reuse_of_snapshot_id, reused.reuse_chain_root_id) == (prior, prior)
    assert (reused.item_count, reused.bytes_total, reused.new_item_count, reused.bytes_added) == (5, 500, 0, 0)
    async with mail.factory() as session:
        assert (await resolve_snapshot_items_target(session, target)).target_id == prior
        assert await session.scalar(select(models.SnapshotItem.id).where(models.SnapshotItem.snapshot_id == target)) is None


async def test_a_later_unchanged_run_points_at_the_full_snapshot_not_at_a_pointer(mail):
    prior, _ = await mail.snapshot(models.SnapshotStatus.COMPLETED, rows=3, item_count=3, bytes_total=30)
    first, job = await mail.snapshot()
    await mail.reuse(first, job)
    async with mail.factory.begin() as session:
        await session.execute(text("UPDATE snapshots SET status = 'COMPLETED' WHERE id = :s"), {"s": first})
    second, job = await mail.snapshot()
    assert await mail.reuse(second, job) == prior
    assert (await mail.row(second)).reuse_of_snapshot_id == prior


async def test_newest_prior_partial_is_not_reused(mail):
    """The database only lets a pointer target a COMPLETED snapshot; an older COMPLETED one would
    miss the PARTIAL run's mail, so the run copies forward as before."""
    await mail.snapshot(models.SnapshotStatus.COMPLETED, rows=3, item_count=3)
    await mail.snapshot(models.SnapshotStatus.PARTIAL, rows=4, item_count=4)
    target, job = await mail.snapshot()
    assert await mail.reuse(target, job) is None
    assert (await mail.row(target)).reuse_of_snapshot_id is None


async def test_failed_carry_prior_is_skipped_like_the_carry_does(mail):
    good, _ = await mail.snapshot(models.SnapshotStatus.COMPLETED, rows=3, item_count=3)
    await mail.snapshot(models.SnapshotStatus.COMPLETED, rows=1, item_count=1, extra={"mail_carry_forward_failed": True})
    target, job = await mail.snapshot()
    assert await mail.reuse(target, job) == good


async def test_no_prior_or_rows_already_written_is_not_reused(mail):
    target, job = await mail.snapshot()
    assert await mail.reuse(target, job) is None
    await mail.snapshot(models.SnapshotStatus.COMPLETED, rows=2, item_count=2)
    written, job = await mail.snapshot(rows=1)
    assert await mail.reuse(written, job) is None
    assert (await mail.row(written)).reuse_of_snapshot_id is None


def _worker():
    import importlib.util
    import sys
    name = "bw_mail_reuse_counts"
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, Path(__file__).parents[2] / "workers" / "backup-worker" / "main.py")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return sys.modules[name].BackupWorker


async def _complete(mail, sid, minutes):
    async with mail.factory.begin() as session:
        await session.execute(text("UPDATE snapshots SET status = 'COMPLETED', completed_at = :at WHERE id = :s"),
                              {"s": sid, "at": datetime(2026, 11, 6) + timedelta(minutes=minutes)})


async def test_pointer_reports_nothing_new_and_the_next_run_compares_with_the_original(mail):
    """The next incremental after a pointer must count only its genuinely new mail: comparing
    against the pointer's own (zero) rows would report the whole mailbox as new and inflate the
    resource's storage bytes."""
    delta = _worker()._compute_snapshot_delta_from_prior
    prior, _ = await mail.snapshot(models.SnapshotStatus.COMPLETED, rows=5, item_count=5, bytes_total=50)
    await _complete(mail, prior, 1)
    pointer, job = await mail.snapshot()
    await mail.reuse(pointer, job)
    async with mail.factory() as session:
        resource = (await mail.row(pointer)).resource_id
        assert await delta(session, pointer, resource, 5, 50, current_job_id=job) == (0, 0)
    await _complete(mail, pointer, 2)
    grown, job = await mail.snapshot(rows=6, item_count=6, bytes_total=60)   # 5 carried + 1 new
    async with mail.factory() as session:
        assert await delta(session, grown, resource, 6, 60, current_job_id=job) == (1, 10)


def _snapshot_api():
    import importlib.util
    import sys
    name = "mail_reuse_snapshot_api"
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, Path(__file__).parents[2] / "services" / "snapshot-service" / "main.py")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return sys.modules[name]


async def test_recovery_reads_of_a_pointer_show_the_prior_snapshots_mail(mail):
    """Opening, downloading, listing attachments of and searching an unchanged run's restore point
    must show the mail it points at, not 'not found'."""
    import base64
    api = _snapshot_api()
    prior, _ = await mail.snapshot(models.SnapshotStatus.COMPLETED, item_count=1, bytes_total=20)
    async with mail.factory.begin() as session:
        resource = (await session.get(models.Snapshot, prior)).resource_id
        tenant = (await session.get(models.Resource, resource)).tenant_id
        backend = (await session.get(models.Snapshot, prior)).backend_id
        email = models.SnapshotItem(snapshot_id=prior, tenant_id=tenant, item_type="EMAIL", backend_id=backend,
            external_id="message-needle", name="Quarterly needle", folder_path="Inbox", content_size=10,
            extra_data={"raw": {"subject": "Quarterly needle"}})
        attachment = models.SnapshotItem(snapshot_id=prior, tenant_id=tenant, item_type="EMAIL_ATTACHMENT",
            backend_id=backend, external_id="message-needle:attachment", name="notes.txt", folder_path="Inbox",
            content_size=10, extra_data={"parent_item_id": "message-needle",
                                         "inline_b64": base64.b64encode(b"note").decode()})
        session.add_all([email, attachment])
    pointer, job = await mail.snapshot()
    assert await mail.reuse(pointer, job) == prior
    async with mail.factory() as db:
        assert (await api.get_snapshot_item_detail(str(pointer), str(email.id), db)).id == str(email.id)
        await api.get_item_content(str(pointer), str(email.id), download=False, db=db)
        assert [a["name"] for a in await api.get_item_attachments(str(pointer), str(email.id), db)] == ["notes.txt"]
        found = await api.search_snapshot_items(str(resource), query="needle", item_type=None, folder_path=None,
                                                snapshot_id=str(pointer), page=1, size=50, db=db)
        assert found.totalElements >= 1


async def test_folder_selection_of_a_pointer_sizes_the_prior_snapshots_rows(mail):
    """Restore/export queue routing sizes a selection; a pointer sized at 0 B sends a huge
    mailbox restore to the normal queue."""
    from shared.folder_resolver import resolve_selection
    from shared.snapshot_reuse import items_snapshot_id
    prior, _ = await mail.snapshot(models.SnapshotStatus.COMPLETED, rows=3, item_count=3)
    pointer, job = await mail.snapshot()
    await mail.reuse(pointer, job)
    async with mail.factory.begin() as session:
        await session.execute(text("UPDATE snapshot_items SET folder_path = 'Inbox' WHERE snapshot_id = :s"), {"s": prior})
    async with mail.factory() as session:
        assert await items_snapshot_id(session, pointer) == prior
        missing = uuid4()
        assert await items_snapshot_id(session, missing) == missing
        rows = await resolve_selection(session, snapshot_id=str(pointer), item_ids=[], folder_paths=["Inbox"],
                                       excluded_item_ids=[])
    assert sum(r.content_size for r in rows) == 30
