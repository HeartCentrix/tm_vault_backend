"""The provider/copy control rows are created on first use with the model defaults, and TM sets
no overrides. The old defaults (provider 8 total / 4 per tenant, copy 2 per tenant) would cap TM's
single tenant at 4 concurrent backups across 20 worker replicas; Railway runs 40/40 and copy 6."""
import os
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from shared.database import Base
from shared.models import CopyWorkControl, ProviderWorkControl

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def factory():
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires explicitly configured disposable PostgreSQL")
    schema = "work_control_" + uuid4().hex
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}, "statement_cache_size": 0})
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await conn.run_sync(lambda c: Base.metadata.create_all(c, tables=[ProviderWorkControl.__table__, CopyWorkControl.__table__]))
    yield async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    await engine.dispose()


async def _first_use(factory):
    async with factory.begin() as s:  # exactly how provider_work / copy_work create the rows
        await s.execute(insert(ProviderWorkControl).values(id='global', enabled=True).on_conflict_do_nothing())
        await s.execute(insert(CopyWorkControl).values(id='global').on_conflict_do_nothing())
    async with factory() as s:
        p = await s.scalar(select(ProviderWorkControl))
        c = await s.scalar(select(CopyWorkControl))
    return (p.total_capacity, p.tenant_capacity), (c.micro_capacity, c.giant_capacity, c.total_capacity, c.tenant_capacity)


async def test_first_use_gets_capacities_that_do_not_throttle_a_single_tenant(factory, monkeypatch):
    for name in ("PROVIDER_TOTAL_CAPACITY", "PROVIDER_TENANT_CAPACITY", "COPY_TENANT_CAPACITY"):
        monkeypatch.delenv(name, raising=False)
    assert await _first_use(factory) == ((40, 40), (3, 3, 6, 6))


async def test_env_overrides_the_first_use_capacities(factory, monkeypatch):
    monkeypatch.setenv("PROVIDER_TOTAL_CAPACITY", "64")
    monkeypatch.setenv("PROVIDER_TENANT_CAPACITY", "48")
    monkeypatch.setenv("COPY_TENANT_CAPACITY", "4")
    assert await _first_use(factory) == ((64, 48), (3, 3, 6, 4))
