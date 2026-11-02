"""Real scoped summary/history reads, without historical item queries in HTTP."""
import importlib.util
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4
from zoneinfo import ZoneInfo

os.environ.setdefault('ALLOW_DEV_JWT_SECRETS', 'true')
import pytest
import pytest_asyncio
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def dashboard_store(monkeypatch):
    url = os.getenv('TMVAULT_TEST_DATABASE_URL')
    if not url:
        pytest.skip('requires explicitly disposable PostgreSQL')
    schema = 'dashboard_size_test_' + uuid4().hex
    engine = create_async_engine(url, connect_args={'statement_cache_size': 0})
    queries = []

    @event.listens_for(engine.sync_engine, 'begin')
    def isolated(connection):
        connection.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')

    @event.listens_for(engine.sync_engine, 'before_cursor_execute')
    def capture(connection, cursor, statement, parameters, context, executemany):
        queries.append(statement)

    async with engine.begin() as db:
        await db.execute(text(f'CREATE SCHEMA "{schema}"'))
        await db.execute(text('CREATE TABLE tenants(id uuid PRIMARY KEY,org_id uuid)'))
        await db.execute(text('CREATE TABLE resources(id uuid PRIMARY KEY,tenant_id uuid,type text)'))
        await db.execute(text('CREATE TABLE snapshots(id uuid PRIMARY KEY,resource_id uuid,status text,created_at timestamp,completed_at timestamp,bytes_added bigint)'))
        await db.execute(text('CREATE TABLE snapshot_items(id uuid,snapshot_id uuid,external_id text,item_type text,content_size bigint,is_deleted boolean,created_at timestamp)'))
        await db.execute(text('CREATE TABLE resource_protected_bytes(resource_id uuid PRIMARY KEY REFERENCES resources(id) ON DELETE CASCADE,latest_snapshot_id uuid NOT NULL,bytes bigint NOT NULL DEFAULT 0,computed_at timestamptz NOT NULL DEFAULT now())'))
        from shared.protected_size_schema import install
        await install(db)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    path = Path(__file__).resolve().parents[2] / 'services/dashboard-service/main.py'
    spec = importlib.util.spec_from_file_location('dashboard_size_postgres', path)
    main = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(main)
    monkeypatch.setattr(main.settings, 'DB_SCHEMA', schema)

    async def sql(statement, **params):
        async with factory.begin() as db:
            return await db.execute(text(statement), params)

    async def drain():
        from shared.protected_size import refresh_once
        for _ in range(500):
            result = await refresh_once(factory, page_size=7)
            if result['status'] == 'idle':
                return
        pytest.fail('dashboard fixture did not catch up')

    async def call(principal, tenant=None, service=None, tz='UTC'):
        class Request:
            scope = {'tmvault_user': principal}
            async def is_disconnected(self):
                return False
        async with factory() as db:
            return await main.get_backup_size(Request(), tenantId=tenant, serviceType=service, tz=tz, db=db)

    async def resource(tenant, kind, size, *, added=1, completed=None):
        rid, sid = uuid4(), uuid4()
        completed = completed or datetime.now(timezone.utc).replace(tzinfo=None)
        await sql('INSERT INTO resources VALUES(:r,:t,:kind)', r=rid, t=tenant, kind=kind)
        await sql("INSERT INTO snapshots VALUES(:s,:r,'COMPLETED',now(),:at,:added)", s=sid, r=rid, at=completed, added=added)
        await sql("INSERT INTO snapshot_items VALUES(:i,:s,'item','data',:size,false,now())", i=uuid4(), s=sid, size=size)
        return rid, sid

    yield SimpleNamespace(sql=sql, drain=drain, call=call, resource=resource, queries=queries, main=main, factory=factory, engine=engine)
    await main._refresh_engine.dispose()
    if main._ro_engine:
        await main._ro_engine.dispose()
    async with engine.begin() as db:
        # Explicit global event-trigger cleanup accompanies schema cleanup.
        from shared.protected_size_schema import event_trigger_names
        for name in event_trigger_names(schema):
            await db.execute(text('DROP EVENT TRIGGER IF EXISTS "' + name.replace('"', '""') + '"'))
        # Additional schema-owned event triggers (e.g. sql_drop) are removed by CASCADE.
        await db.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    await engine.dispose()


async def test_scoped_http_total_and_own_child_history_without_item_queries(dashboard_store):
    s = dashboard_store
    own, other, org = uuid4(), uuid4(), uuid4()
    await s.sql('INSERT INTO tenants VALUES(:t,:o),(:other,:other_org)', t=own, o=org, other=other, other_org=uuid4())
    await s.resource(own, 'ENTRA_USER', 0, added=3)
    await s.resource(own, 'USER_MAIL', 7, added=11)
    await s.resource(own, 'USER_ARCHIVE', 9, added=13)
    await s.resource(other, 'USER_MAIL', 1000, added=10000)
    await s.drain()
    s.queries.clear()
    data = await s.call({'roles': ['TENANT_ADMIN'], 'tenant_ids': [str(own)]}, service='m365')
    assert data['totalBytes'] == 16
    assert data['freshness'] == 'fresh'
    assert sum(row['bytes'] for row in data['dailyData']) == 27
    assert len(data['dailyData']) == 30
    assert not any('snapshot_items' in query for query in s.queries)
    assert len(s.queries) <= 5
    org_data = await s.call({'roles': ['ORG_ADMIN'], 'org_id': str(org), 'tenant_ids': []})
    assert org_data['totalBytes'] == 16
    empty = await s.call({'roles': [], 'tenant_ids': []})
    assert empty['totalBytes'] == 0 and empty['eligibleCount'] == 0


async def test_local_window_keeps_first_day_after_local_midnight(dashboard_store):
    s = dashboard_store
    tenant = uuid4()
    zone = ZoneInfo('Asia/Kolkata')
    today = datetime.now(timezone.utc).astimezone(zone).date()
    cutoff = datetime.combine(today - timedelta(days=29), datetime.min.time(), zone).astimezone(timezone.utc).replace(tzinfo=None)
    await s.resource(tenant, 'USER_ARCHIVE', 1, added=17, completed=cutoff + timedelta(minutes=1))
    await s.resource(tenant, 'USER_ARCHIVE', 1, added=100, completed=cutoff - timedelta(minutes=1))
    await s.drain()
    data = await s.call({'roles': [], 'tenant_ids': [str(tenant)]}, service='m365', tz='Asia/Kolkata')
    assert data['dailyData'][0]['bytes'] == 17
    assert sum(row['bytes'] for row in data['dailyData']) == 17


async def test_real_pending_change_keeps_last_size_and_avoids_uncertain_projection(dashboard_store):
    s = dashboard_store
    tenant = uuid4()
    _, snap = await s.resource(tenant, 'USER_CHATS', 900)
    await s.drain()
    await s.sql('UPDATE snapshot_items SET content_size=2 WHERE snapshot_id=:s', s=snap)
    data = await s.call({'roles': [], 'tenant_ids': [str(tenant)]})
    assert data['totalBytes'] == 900
    assert data['stale'] is True
    assert data['projectedBytes1y'] is None
    await s.drain()
    assert (await s.call({'roles': [], 'tenant_ids': [str(tenant)]}))['totalBytes'] == 2
