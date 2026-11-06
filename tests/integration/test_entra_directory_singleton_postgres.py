"""Discovery must (re)create the tenant's singleton "Azure Active Directory" resource (ENTRA_DIRECTORY,
which carries apps, roles, CA policies, audit). Only the retired in-process tenant-service discovery
created it; the discovery-worker path never did, so after a reset the dashboard's Entra ID tile read 0
and no directory backup existed (Railway 2026-11-06: 41 users, 20 groups, 85 app registrations)."""
import os
import pathlib
import re
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from shared.database import Base
import shared.models as models
from shared.entra_directory import ensure_entra_directory

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def factory():
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires explicitly configured disposable PostgreSQL")
    schema = "entra_dir_" + uuid4().hex
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}, "statement_cache_size": 0})
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await conn.run_sync(Base.metadata.create_all)
    yield async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    await engine.dispose()


async def _tenant(s, kind):
    org = models.Organization(name="Synthetic", slug=uuid4().hex)
    s.add(org)
    await s.flush()
    t = models.Tenant(id=uuid4(), org_id=org.id, display_name="Synthetic", type=kind, external_tenant_id=str(uuid4()))
    s.add(t)
    await s.flush()
    return t


async def test_m365_tenant_gets_exactly_one_directory_resource(factory):
    async with factory.begin() as s:
        tenant = await _tenant(s, models.TenantType.M365)
        assert await ensure_entra_directory(s, tenant) is True
        assert await ensure_entra_directory(s, tenant) is False  # idempotent
        rows = (await s.execute(select(models.Resource).where(models.Resource.tenant_id == tenant.id))).scalars().all()
    assert [(r.type, r.external_id, r.display_name) for r in rows] == [
        (models.ResourceType.ENTRA_DIRECTORY, tenant.external_tenant_id, "Azure Active Directory")]


async def test_non_m365_tenant_gets_none(factory):
    async with factory.begin() as s:
        tenant = await _tenant(s, models.TenantType.AZURE)
        assert await ensure_entra_directory(s, tenant) is False
        assert await s.scalar(select(func.count()).select_from(models.Resource)) == 0


def test_discovery_worker_finalize_ensures_the_directory():
    src = (pathlib.Path(__file__).resolve().parents[2] / "workers" / "discovery-worker" / "main.py").read_text()
    body = src[src.index("async def _finalize_discovery("):]
    body = body[:body.index("\nasync def ", 10)]
    assert re.search(r"await ensure_entra_directory\(db, tenant\)", body)
