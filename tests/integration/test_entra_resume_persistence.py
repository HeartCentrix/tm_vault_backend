"""An Entra replay must preserve the original snapshot entries without duplicates."""
import ast
from pathlib import Path
from typing import List, Dict, Any
from uuid import uuid4
import pytest
from sqlalchemy import select, func
from shared.models import SnapshotItem
from test_chat_catalogue_postgres import store

pytestmark = pytest.mark.integration


def helper():
    source = Path(__file__).resolve().parents[2] / 'workers/backup-worker/main.py'
    tree = ast.parse(source.read_text())
    names = {'_insert_snapshot_items', '_bulk_upsert_snapshot_items', '_persist_entra_snapshot_items'}
    ns = dict(List=List, Dict=Dict, Any=Any, SnapshotItem=SnapshotItem,
              _BULK_INSERT_CHUNK=2000, _PG_MAX_BIND_PARAMS=32767, SnapshotVanishedError=RuntimeError)
    exec(compile(ast.Module(body=[n for n in tree.body if getattr(n, 'name', None) in names], type_ignores=[]), str(source), 'exec'), ns)
    return ns['_persist_entra_snapshot_items']


async def test_entra_replay_keeps_first_capture_and_adds_missing_items(store):
    scope = await store.new_scope()
    def item(key, content):
        return SnapshotItem(snapshot_id=scope.snapshot_id, tenant_id=store.tenant, backend_id=scope.backend_id,
            external_id=key, item_type='USER_PROFILE', name=key,
            content_size=len(content), blob_path=content, extra_data={'raw': content})
    persist = helper()
    async with store.factory() as db:
        await persist(db, [item('a','first')])
    async with store.factory() as db:
        await persist(db, [item('a','replacement'), item('b','second')])
    async with store.factory() as db:
        rows = (await db.execute(select(SnapshotItem).where(SnapshotItem.snapshot_id == scope.snapshot_id).order_by(SnapshotItem.external_id))).scalars().all()
    assert [(r.external_id,r.blob_path,r.extra_data) for r in rows] == [('a','first',{'raw':'first'}),('b','second',{'raw':'second'})]

async def test_deferred_entra_turn_does_not_write(store, monkeypatch):
    from shared import provider_runtime
    from unittest.mock import AsyncMock
    def deferred():
        raise provider_runtime.ProviderBudgetExhausted('synthetic exhausted turn')
    monkeypatch.setattr(provider_runtime, 'check_provider_turn', deferred)
    session = AsyncMock()
    with pytest.raises(provider_runtime.ProviderBudgetExhausted):
        await helper()(session, [])
    session.execute.assert_not_called()


