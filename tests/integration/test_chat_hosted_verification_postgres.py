"""Reduce redundant Graph reads without weakening catalogue publication."""
from copy import deepcopy
from contextlib import asynccontextmanager
import logging

import pytest
from sqlalchemy import delete, func, select

from test_chat_capture_postgres import adapters, message, run_capture, store
from test_chat_catalogue_reads import snapshot_api
from shared.models import Snapshot, SnapshotItem

pytestmark = pytest.mark.integration


async def test_versioned_plain_messages_list_hosted_once_and_publish_both_passes(store, adapters, caplog):
    caplog.set_level(logging.INFO, logger='shared.chat_capture')
    adapters.source.messages = [dict(message(), id=str(i), etag=f'version-{i}') for i in range(1000)]
    listed = []
    original = adapters.source.iter_hosted_contents

    async def hosted(chat, mid):
        listed.append(mid)
        async for child in original(chat, mid):
            yield child

    adapters.source.iter_hosted_contents = hosted
    scope, result = await run_capture(store, adapters)
    assert result.outcome == 'complete' and result.item_count == 1000
    assert adapters.source.passes == 2
    assert len(listed) == 1000 and len(set(listed)) == 1000
    progress = [record.getMessage() for record in caplog.records if record.name == 'shared.chat_capture']
    assert len(progress) == 40
    assert 'messages_observed=1000 hosted_list_requests=0 hosted_empty_reused=1000' in progress[-1]
    async with store.factory() as session:
        assert await session.scalar(select(func.count()).select_from(SnapshotItem).where(
            SnapshotItem.snapshot_id == scope.snapshot_id)) == 1000


@pytest.mark.parametrize('tag', [None, '', 7])
async def test_missing_or_invalid_message_version_keeps_both_hosted_checks(store, adapters, tag):
    adapters.source.messages = [dict(message(), etag=tag)]
    calls = []

    async def hosted(chat, mid):
        calls.append(mid)
        if False:
            yield

    adapters.source.iter_hosted_contents = hosted
    _, result = await run_capture(store, adapters)
    assert result.outcome == 'complete' and calls == ['message', 'message']


async def test_nonempty_hosted_content_is_still_independently_checked(store, adapters):
    adapters.source.messages = [dict(message(hosted=True), etag='stable')]
    lists, reads = [], []
    original_list, original_open = adapters.source.iter_hosted_contents, adapters.source.open_hosted

    async def hosted(chat, mid):
        lists.append(mid)
        async for child in original_list(chat, mid):
            yield child

    def opened(chat, mid, cid):
        reads.append(cid)
        return original_open(chat, mid, cid)

    adapters.source.iter_hosted_contents, adapters.source.open_hosted = hosted, opened
    _, result = await run_capture(store, adapters)
    assert result.outcome == 'complete' and result.item_count == 2
    assert lists == ['message', 'message'] and reads == ['image', 'image']
    assert len(adapters.storage.uploads) == 1


async def test_failed_hosted_list_does_not_seed_empty_proof(store, adapters):
    adapters.source.messages = [dict(message(), etag='stable')]
    calls = []

    async def hosted(chat, mid):
        calls.append(mid)
        if len(calls) == 1:
            raise RuntimeError('first enumeration failed')
        if False:
            yield

    adapters.source.iter_hosted_contents = hosted
    _, result = await run_capture(store, adapters)
    assert result.outcome == 'partial' and calls == ['message', 'message']


@pytest.mark.parametrize('field,value', [('etag', 'v2'), ('body', {'content': 'edited'})])
async def test_changed_parent_cannot_reuse_old_empty_proof(store, adapters, field, value):
    adapters.source.messages = [dict(message(), etag='v1')]
    calls = []

    def movement(observation):
        value_now = deepcopy(adapters.source.messages[0])
        if observation >= 2:
            value_now[field] = value
        return [value_now]

    async def hosted(chat, mid):
        calls.append(adapters.source.passes)
        if False:
            yield

    adapters.source.movement = movement
    adapters.source.iter_hosted_contents = hosted
    _, result = await run_capture(store, adapters)
    assert result.outcome == 'complete'
    assert adapters.source.passes == 4  # changed first pair is reconciled again
    assert calls == [1, 2, 3]


async def test_hosted_404_remains_partial_with_read_progress(store, adapters, caplog):
    from shared.chat_capture_graph import ChatGraphError
    caplog.set_level(logging.INFO, logger='shared.chat_capture')
    adapters.source.messages = [dict(message(hosted=True), etag='stable')]

    @asynccontextmanager
    async def unavailable(*args):
        raise ChatGraphError('Graph capture content failed with HTTP 404', status_code=404)
        yield

    adapters.source.open_hosted = unavailable
    _, result = await run_capture(store, adapters)
    assert result.outcome == 'partial' and result.item_count == 1
    progress = [record.getMessage() for record in caplog.records if record.name == 'shared.chat_capture']
    assert len(progress) == 2
    assert all('required_binary=1 verified_binary=0 unresolved=1' in value for value in progress)


async def test_captured_binaries_restore_after_previous_snapshot_is_deleted(store, adapters, snapshot_api, monkeypatch):
    from shared.storage.router import router
    adapters.source.messages[0]['etag'] = 'stable'
    old_scope, first = await run_capture(store, adapters)
    new_scope, second = await run_capture(store, adapters)
    assert first.outcome == second.outcome == 'complete'
    assert second.reused and second.uploaded_bytes == 0
    async with store.factory.begin() as db:
        await db.execute(delete(SnapshotItem).where(SnapshotItem.snapshot_id == old_scope.snapshot_id))
        await db.execute(delete(Snapshot).where(Snapshot.id == old_scope.snapshot_id))
    monkeypatch.setattr(router, 'get_store_by_id', adapters.storage.get_store_by_id)
    monkeypatch.setattr(snapshot_api, 'async_session_factory', store.factory)
    for item_type, expected in [('CHAT_ATTACHMENT', b'PDF'), ('CHAT_HOSTED_CONTENT', b'PNG')]:
        async with store.factory.begin() as db:
            item = await db.scalar(select(SnapshotItem).where(
                SnapshotItem.snapshot_id == new_scope.snapshot_id, SnapshotItem.item_type == item_type))
            response = await snapshot_api.get_item_content(str(new_scope.snapshot_id), str(item.id), download=False, db=db)
        assert b''.join([chunk async for chunk in response.body_iterator]) == expected
