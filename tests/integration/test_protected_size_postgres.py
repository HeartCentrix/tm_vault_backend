"""Retained-reference ledger against an explicitly disposable PostgreSQL database."""
import importlib
import os
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture(params=[False, True], ids=['plain', 'partitioned'])
async def ledger(request):
    url = os.environ.get('TMVAULT_TEST_DATABASE_URL')
    if not url:
        pytest.skip('requires an explicitly disposable PostgreSQL database')
    schema = 'protected_size_test_' + uuid4().hex
    engine = create_async_engine(url, connect_args={'statement_cache_size': 0})
    @event.listens_for(engine.sync_engine, 'begin')
    def isolated(conn):
        conn.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await conn.execute(text('CREATE TABLE resources(id uuid PRIMARY KEY,tenant_id uuid,type text)'))
        await conn.execute(text('CREATE TABLE snapshots(id uuid PRIMARY KEY,resource_id uuid REFERENCES resources(id) ON DELETE CASCADE,status text,created_at timestamp)'))
        await conn.execute(text('CREATE TABLE snapshot_items(id uuid NOT NULL,snapshot_id uuid NOT NULL,external_id text,item_type text,content_size bigint,is_deleted boolean,created_at timestamp)' + (' PARTITION BY HASH(snapshot_id)' if request.param else '')))
        if request.param:
            for n in range(2):
                await conn.execute(text(f'CREATE TABLE snapshot_items_p{n} PARTITION OF snapshot_items FOR VALUES WITH(MODULUS 2,REMAINDER {n})'))
        await conn.execute(text('CREATE TABLE resource_protected_bytes(resource_id uuid PRIMARY KEY REFERENCES resources(id) ON DELETE CASCADE,latest_snapshot_id uuid NOT NULL,bytes bigint NOT NULL DEFAULT 0,computed_at timestamptz NOT NULL DEFAULT now())'))
        module = importlib.import_module('shared.protected_size_schema')
        await module.install(conn)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    tenant, resource = uuid4(), uuid4()
    async with factory.begin() as db:
        await db.execute(text('INSERT INTO resources VALUES(:r,:t,\'USER_MAIL\')'), {'r': resource, 't': tenant})
    async def sql(query, **params):
        async with factory.begin() as db:
            return await db.execute(text(query), params)
    async def snapshot(status='COMPLETED', resource_id=resource):
        sid = uuid4()
        await sql('INSERT INTO snapshots VALUES(:s,:r,:status,now())', s=sid, r=resource_id, status=status)
        return sid
    async def item(sid, key='a', size=10, *, item_id=None, deleted=False, when='2026-01-01', kind='mail', table='snapshot_items'):
        iid = item_id or uuid4()
        await sql(f'INSERT INTO {table} VALUES(:i,:s,:k,:kind,:z,:d,:when)', i=iid, s=sid,k=key,kind=kind,z=size,d=deleted,when=__import__('datetime').datetime.fromisoformat(when))
        return iid
    async def summary(tenants=(tenant,), types=None):
        from shared.protected_size import read_summary
        async with factory.begin() as db:
            return await read_summary(db, tenant_ids=tenants, service_types=types)
    async def drain(limit=100, page_size=2):
        from shared.protected_size import refresh_once
        turns=[]
        for _ in range(limit):
            turn=await refresh_once(factory,page_size=page_size)
            turns.append(turn)
            if turn['status']=='idle':
                return turns
        pytest.fail(f'ledger did not converge: {turns[-5:]}')
    yield SimpleNamespace(factory=factory,sql=sql,snapshot=snapshot,item=item,summary=summary,drain=drain,resource=resource,tenant=tenant,schema=schema,partitioned=request.param)
    async with engine.begin() as conn:
        # Schema-specific event trigger must be removed before its function.
        await conn.execute(text(f'DROP EVENT TRIGGER IF EXISTS "protected_size_{schema}"'))
        await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    await engine.dispose()


async def test_retained_history_parity_and_live_deltas(ledger):
    a,b=await ledger.snapshot(),await ledger.snapshot('PARTIAL')
    first=await ledger.item(a,'same',100)
    newer=await ledger.item(b,'same',25,when='2026-02-01')
    await ledger.item(b,'sparse',7)
    await ledger.item(b,'same',999,deleted=True,when='2026-03-01')
    # Identity deliberately ignores item_type, like existing protected metric.
    await ledger.item(a,'sparse',4,kind='other',when='2025-01-01')
    assert (await ledger.summary())['totalBytes'] is None
    turns=await ledger.drain()
    result=await ledger.summary()
    assert result['totalBytes']==32 and result['freshness']=='fresh'
    assert max(t.get('referencesTouched',0) for t in turns)<=2
    await ledger.sql('DELETE FROM snapshot_items WHERE id=:i',i=newer)
    stale=await ledger.summary()
    assert stale['totalBytes']==32 and stale['stale'] is True
    await ledger.drain()
    assert (await ledger.summary())['totalBytes']==107
    await ledger.sql('UPDATE snapshot_items SET content_size=3 WHERE id=:i',i=first)
    await ledger.drain()
    assert (await ledger.summary())['totalBytes']==10
    from shared.protected_size import optimized_rollup
    expected=(await ledger.sql(optimized_rollup,rid=ledger.resource)).scalar()
    assert expected==10


async def test_snapshot_eligibility_reparent_and_rollback(ledger):
    s=await ledger.snapshot('IN_PROGRESS')
    await ledger.item(s,'x',9)
    await ledger.drain()
    assert (await ledger.summary())['eligibleCount']==0
    await ledger.sql("UPDATE snapshots SET status='COMPLETED' WHERE id=:s",s=s)
    assert (await ledger.summary())['missingCount']==1
    await ledger.drain()
    assert (await ledger.summary())['totalBytes']==9
    before=(await ledger.sql('SELECT count(*) FROM protected_size_journal')).scalar()
    async with ledger.factory() as db:
        await db.execute(text('UPDATE snapshot_items SET content_size=123 WHERE snapshot_id=:s'),{'s':s})
        await db.rollback()
    assert (await ledger.sql('SELECT count(*) FROM protected_size_journal')).scalar()==before
    await ledger.sql("UPDATE snapshots SET status=status WHERE id=:s",s=s)
    assert (await ledger.sql('SELECT count(*) FROM protected_size_journal')).scalar()==before
    r2=uuid4()
    await ledger.sql("INSERT INTO resources VALUES(:r,:t,'USER_CHATS')",r=r2,t=ledger.tenant)
    await ledger.sql('UPDATE snapshots SET resource_id=:r WHERE id=:s',r=r2,s=s)
    await ledger.drain()
    assert (await ledger.summary(types=('USER_MAIL',)))['totalBytes']==0
    assert (await ledger.summary(types=('USER_CHATS',)))['totalBytes']==9
    assert (await ledger.summary(tenants=()))['totalBytes']==0
    await ledger.sql('DELETE FROM resources WHERE id=:r',r=r2)
    assert (await ledger.summary())['totalBytes']==0


async def test_ties_null_identity_and_journal_batches(ledger):
    s=await ledger.snapshot()
    lo,hi=__import__('uuid').UUID(int=1),__import__('uuid').UUID(int=2)
    await ledger.item(s,'tie',2,item_id=lo)
    await ledger.item(s,'tie',8,item_id=hi)
    await ledger.item(s,None,5)
    await ledger.drain()
    assert (await ledger.summary())['totalBytes']==13
    await ledger.sql("INSERT INTO snapshot_items SELECT gen_random_uuid(),:s,'bulk-'||n,'mail',1,false,now() FROM generate_series(1,2001) n",s=s)
    assert (await ledger.sql('SELECT max(cardinality(item_ids)) FROM protected_size_journal')).scalar()<=1000
    await ledger.drain(limit=1100,page_size=1000)
    assert (await ledger.summary())['totalBytes']==2014


async def test_future_partition_direct_writes_are_captured(ledger):
    if not ledger.partitioned:
        return
    await ledger.sql('ALTER TABLE snapshot_items DETACH PARTITION snapshot_items_p0')
    await ledger.sql('DROP TABLE snapshot_items_p0')
    await ledger.sql('CREATE TABLE snapshot_items_p0 PARTITION OF snapshot_items FOR VALUES WITH(MODULUS 2,REMAINDER 0)')
    s=await ledger.snapshot()
    table=(await ledger.sql("SELECT tableoid::regclass::text FROM snapshot_items LIMIT 1")).scalar()
    # Find routing without inserting, then perform a direct leaf write.
    table=(await ledger.sql("SELECT CASE WHEN satisfies_hash_partition('snapshot_items'::regclass,2,0,:s) THEN 'snapshot_items_p0' ELSE 'snapshot_items_p1' END",s=s)).scalar()
    await ledger.item(s,'direct',19,table=table)
    await ledger.drain()
    assert (await ledger.summary())['totalBytes']==19
