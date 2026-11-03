"""A chat resource keeps a fresh provider turn long enough to do real work.

On 2026-11-03 Railway logs showed chat resources yielding after every single
chat (``fair_checkpoint``) because some other work was always waiting; each
resume re-claimed the turn and re-listed every chat, so 10 users finished
~3 chats per minute on 20 replicas. A turn now yields only after
``CHAT_TURN_MIN_SECONDS`` of its own work.
"""
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from test_chat_catalogue_postgres import store
from test_chat_catalogue_entry_postgres import source_adapter
from shared import models
from shared import provider_work as work

pytestmark = pytest.mark.integration


async def test_fresh_turn_captures_every_chat_before_yielding(store, source_adapter, monkeypatch):
    from shared.provider_runtime import ProviderWorkRunner
    source = source_adapter.source
    source.chats = [{"id": "a"}, {"id": "b"}, {"id": "c"}]
    scope = await store.new_scope()
    async with store.factory.begin() as db:
        await work.ensure_work(db, snapshot_id=scope.snapshot_id, snapshot_token=0, job_token=0)
    monkeypatch.setattr("shared.provider_runtime.other_provider_work_waiting", AsyncMock(return_value=True))

    async def resume(owner):
        return await source_adapter.run(scope, concurrency=2,
            snapshot_lease_token=owner.snapshot_token, job_lease_token=owner.job_token)

    runner = ProviderWorkRunner(store.factory, resume, AsyncMock())
    result = await runner.run_once()
    assert result.get("outcome") == "complete", result
    async with store.factory() as db:
        assert len((await db.scalars(select(models.SnapshotChatInventory))).all()) == 3
    assert source.list_calls == 2  # one listing plus the final visibility check, no re-list
