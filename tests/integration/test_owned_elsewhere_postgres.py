"""Group and channel backups walked the team's whole SharePoint site again (Railway 2026-11-06:
2,144 of 2,144 group/channel SharePoint files were already in the SharePoint site backups). Content
another protected resource owns is skipped: a resource is the owner only while it is not archived,
has an SLA, and that SLA enables its workload."""
import os
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from shared.database import Base
import shared.models as models
from shared.sla_workloads import protected_owner_id

pytestmark = pytest.mark.integration
SITE = "tenant.sharepoint.com/aaaa/bbbb"


@pytest_asyncio.fixture
async def db():
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires explicitly configured disposable PostgreSQL")
    schema = "owned_elsewhere_" + uuid4().hex
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}, "statement_cache_size": 0})
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    tenant = uuid4()
    async with factory.begin() as s:
        org = models.Organization(name="Synthetic", slug=uuid4().hex)
        s.add(org)
        await s.flush()
        s.add(models.Tenant(id=tenant, org_id=org.id, display_name="Synthetic"))
    yield factory, tenant
    async with engine.begin() as conn:
        await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    await engine.dispose()


async def _site(factory, tenant, *, sharepoint=True, sla=True, archived=False):
    async with factory.begin() as s:
        policy = None
        if sla:
            policy = models.SlaPolicy(id=uuid4(), tenant_id=tenant, name=uuid4().hex, backup_sharepoint=sharepoint)
            s.add(policy)
            await s.flush()
        rid = uuid4()
        s.add(models.Resource(id=rid, tenant_id=tenant, type=models.ResourceType.SHAREPOINT_SITE, external_id=SITE,
                              display_name="QFION", sla_policy_id=policy.id if policy else None,
                              archived_at=__import__("datetime").datetime.utcnow() if archived else None))
    return rid


async def test_a_site_with_an_sla_that_backs_up_sharepoint_owns_its_content(db):
    factory, tenant = db
    rid = await _site(factory, tenant)
    async with factory() as s:
        assert await protected_owner_id(s, tenant, "SHAREPOINT_SITE", SITE) == str(rid)


@pytest.mark.parametrize("kw", [dict(sla=False), dict(sharepoint=False), dict(archived=True)])
async def test_unprotected_sites_do_not_own_it(db, kw):
    factory, tenant = db
    await _site(factory, tenant, **kw)
    async with factory() as s:
        assert await protected_owner_id(s, tenant, "SHAREPOINT_SITE", SITE) is None
