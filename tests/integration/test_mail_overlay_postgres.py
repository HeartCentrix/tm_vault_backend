"""Overlay mail snapshots: a changed mailbox's incremental owns only what changed since a full base
snapshot, and every reader sees base + overlay through snapshot_items_effective().

Rohit, 2026-11-05: 7,085 rows copied forward for one new message (245 s of a 464 s run)."""
from __future__ import annotations

from datetime import datetime, timedelta
import os
from pathlib import Path
import re
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

from shared.database import Base
import shared.models as models

pytestmark = pytest.mark.integration


def _reuse_trigger_ddl():
    source = (Path(__file__).parents[2] / "shared" / "database.py").read_text()
    function = re.search(r'"""(CREATE OR REPLACE FUNCTION validate_snapshot_reuse_target\(\).*?)"""', source, re.S).group(1)
    return [function, "CREATE TRIGGER snapshots_reuse_validate BEFORE INSERT OR UPDATE OF reuse_of_snapshot_id, "
            "reuse_chain_root_id ON snapshots FOR EACH ROW EXECUTE FUNCTION validate_snapshot_reuse_target();"]


@pytest_asyncio.fixture
async def db():
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires explicitly configured disposable PostgreSQL")
    schema = "mail_overlay_test_" + uuid4().hex
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema},
                                                    "statement_cache_size": 0})
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await conn.run_sync(Base.metadata.create_all)
        for ddl in _reuse_trigger_ddl():
            await conn.execute(text(ddl))
    factory = async_sessionmaker(engine, expire_on_commit=False)
    tenant, backend = uuid4(), uuid4()
    async with factory.begin() as session:
        org = models.Organization(name="Synthetic", slug=uuid4().hex)
        session.add(org)
        await session.flush()
        session.add(models.Tenant(id=tenant, org_id=org.id, display_name="Synthetic"))
        session.add(models.StorageBackend(id=backend, kind="azure", name=uuid4().hex,
                    endpoint="https://storage.invalid", config={}, secret_ref="synthetic-reference"))
    clock = [datetime(2026, 11, 5, 10)]

    async def resource(kind=models.ResourceType.USER_MAIL):
        rid = uuid4()
        async with factory.begin() as session:
            session.add(models.Resource(id=rid, tenant_id=tenant, type=kind, external_id=uuid4().hex,
                                        display_name="Synthetic mailbox"))
        return rid

    async def snapshot(rid, status=models.SnapshotStatus.IN_PROGRESS, **columns):
        job, sid = uuid4(), uuid4()
        clock[0] += timedelta(minutes=10)
        async with factory.begin() as session:
            session.add(models.Job(id=job, type=models.JobType.BACKUP, tenant_id=tenant, resource_id=rid,
                                   status=models.JobStatus.RUNNING))
            await session.flush()
            session.add(models.Snapshot(id=sid, resource_id=rid, job_id=job, backend_id=backend, status=status,
                                        started_at=clock[0], created_at=clock[0], extra_data={}, **columns))
        return sid, job

    async def item(sid, external_id, item_type="EMAIL", *, folder="Inbox", parent=None, size=10, name=None,
                   metadata=None, blob=None):
        async with factory.begin() as session:
            session.add(models.SnapshotItem(
                snapshot_id=sid, tenant_id=tenant, backend_id=backend, item_type=item_type,
                external_id=external_id, parent_external_id=parent, name=name or external_id, folder_path=folder,
                content_size=size, blob_path=blob, extra_data=metadata or {}))

    async def effective(sid):
        async with factory() as session:
            rows = (await session.execute(text(
                "SELECT item_type, external_id, name, folder_path FROM snapshot_items_effective(:s) "
                "ORDER BY item_type, external_id"), {"s": sid})).all()
        return [tuple(r) for r in rows]

    yield type("Db", (), {"factory": factory, "tenant": tenant, "backend": backend,
                          "resource": staticmethod(resource), "snapshot": staticmethod(snapshot),
                          "item": staticmethod(item), "effective": staticmethod(effective)})
    async with engine.begin() as conn:
        await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    await engine.dispose()


async def _base_with_mail(db):
    rid = await db.resource()
    base, _ = await db.snapshot(rid, models.SnapshotStatus.COMPLETED)
    for m in "abcd":
        await db.item(base, m)
    for m in "abc":
        await db.item(base, f"{m}::1", "EMAIL_ATTACHMENT", parent=m)
    return rid, base


async def test_full_snapshot_and_pointer_read_their_own_or_roots_rows(db):
    rid, base = await _base_with_mail(db)
    pointer, _ = await db.snapshot(rid, reuse_of_snapshot_id=base, reuse_chain_root_id=base)
    assert await db.effective(base) == await db.effective(pointer)
    assert len(await db.effective(base)) == 7


async def test_overlay_reads_own_rows_plus_base_minus_overrides_and_removals(db):
    rid, base = await _base_with_mail(db)
    overlay, _ = await db.snapshot(rid, models.SnapshotStatus.COMPLETED, overlay_base_id=base)
    await db.item(overlay, "b", name="b changed", folder="Archive")
    await db.item(overlay, "e")
    await db.item(overlay, "e::1", "EMAIL_ATTACHMENT", parent="e")
    async with db.factory.begin() as session:
        await session.execute(text(
            "INSERT INTO snapshot_overlay_removals (snapshot_id, item_type, external_id) VALUES "
            "(:o, 'EMAIL', 'c'), (:o, 'EMAIL_ATTACHMENT', 'a::1')"), {"o": overlay})
    assert await db.effective(overlay) == [
        ("EMAIL", "a", "a", "Inbox"),
        ("EMAIL", "b", "b changed", "Archive"),
        ("EMAIL", "d", "d", "Inbox"),
        ("EMAIL", "e", "e", "Inbox"),
        # base attachment of a message the overlay re-captured stays (the carry keeps it today)
        ("EMAIL_ATTACHMENT", "b::1", "b::1", "Inbox"),
        ("EMAIL_ATTACHMENT", "e::1", "e::1", "Inbox"),
    ]
    pointer, _ = await db.snapshot(rid, reuse_of_snapshot_id=overlay, reuse_chain_root_id=overlay)
    assert await db.effective(pointer) == await db.effective(overlay)


async def test_overlay_inherits_only_mail_rows_of_its_base(db):
    """The carry copies EMAIL/EMAIL_ATTACHMENT only; any other row type in the base is not part of the
    next run's inventory."""
    rid, base = await _base_with_mail(db)
    await db.item(base, "settings", "MAILBOX_SETTINGS")
    overlay, _ = await db.snapshot(rid, overlay_base_id=base)
    assert ("MAILBOX_SETTINGS", "settings", "settings", "Inbox") not in await db.effective(overlay)


async def test_database_refuses_an_overlay_of_an_overlay_a_pointer_or_another_resource(db):
    from sqlalchemy.exc import DBAPIError
    rid, base = await _base_with_mail(db)
    overlay, _ = await db.snapshot(rid, overlay_base_id=base)
    other = await db.resource()
    foreign, _ = await db.snapshot(other, models.SnapshotStatus.COMPLETED)
    pointer, _ = await db.snapshot(rid, reuse_of_snapshot_id=base, reuse_chain_root_id=base)
    for bad_base in (overlay, foreign, pointer):
        with pytest.raises(DBAPIError):
            await db.snapshot(rid, overlay_base_id=bad_base)
    with pytest.raises(DBAPIError):
        await db.snapshot(rid, overlay_base_id=base, reuse_of_snapshot_id=base, reuse_chain_root_id=base)


async def test_manual_railway_script_applies_twice_as_a_non_superuser_owner():
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires explicitly configured disposable PostgreSQL")
    suffix = uuid4().hex
    role, schema = "overlay_owner_" + suffix, "overlay_script_" + suffix
    admin = create_async_engine(url, connect_args={"statement_cache_size": 0})
    async with admin.begin() as conn:
        await conn.execute(text(f'CREATE ROLE "{role}" NOLOGIN NOSUPERUSER'))
        await conn.execute(text(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{role}"'))
    script = (Path(__file__).parents[2] / "scripts" / "2026_11_06_mail_overlay_snapshots.sql").read_text()
    try:
        async with admin.connect() as conn:
            await conn.execute(text(f'SET ROLE "{role}"'))
            await conn.execute(text(f'SET search_path TO "{schema}"'))
            await conn.run_sync(Base.metadata.create_all)
            for ddl in ("DROP TRIGGER snapshots_overlay_validate ON snapshots",
                        "DROP FUNCTION validate_snapshot_overlay_base()", "DROP FUNCTION snapshot_items_effective(uuid)",
                        "DROP TABLE snapshot_overlay_removals", "ALTER TABLE snapshots DROP COLUMN overlay_base_id"):
                await conn.execute(text(ddl))
            await conn.commit()
            raw = (await conn.get_raw_connection()).driver_connection
            await raw.execute(script)
            await raw.execute(script)
            assert await raw.fetchval("SELECT to_regprocedure('snapshot_items_effective(uuid)') IS NOT NULL")
            assert not await raw.fetchval("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")
            await conn.execute(text("RESET ROLE"))
            await conn.commit()
    finally:
        async with admin.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
            await conn.execute(text(f'DROP ROLE "{role}"'))
        await admin.dispose()
