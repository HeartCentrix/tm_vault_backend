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

HOSTED_REF = '<img src="https://graph.microsoft.com/v1.0/chats/chat/messages/message/hostedContents/image/$value">'


def referencing(**changes):
    """A message whose body references hosted content; the listing itself is patched per test."""
    value = message(hosted=True)
    del value["_hosted"]
    return dict(value, **changes)


async def test_overlapping_pages_observe_each_message_once_per_independent_pass(store, adapters, caplog):
    caplog.set_level(logging.INFO, logger='shared.chat_capture')
    messages = [referencing(id=str(i)) for i in range(1000)]
    adapters.source.messages = messages + deepcopy(messages)
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
    assert len(listed) == 2000 and len(set(listed)) == 1000
    progress = [record.getMessage() for record in caplog.records if record.name == 'shared.chat_capture']
    assert len(progress) == 80
    assert 'messages_observed=1000 hosted_enumerations=1000 duplicate_messages_skipped=1000' in progress[-1]
    async with store.factory() as session:
        assert await session.scalar(select(func.count()).select_from(SnapshotItem).where(
            SnapshotItem.snapshot_id == scope.snapshot_id)) == 1000


@pytest.mark.parametrize('tag', [None, '', 7, 'stable'])
async def test_message_version_never_skips_independent_hosted_observation(store, adapters, tag):
    adapters.source.messages = [referencing(etag=tag)]
    calls = []

    async def hosted(chat, mid):
        calls.append(mid)
        if False:
            yield

    adapters.source.iter_hosted_contents = hosted
    _, result = await run_capture(store, adapters)
    assert result.outcome == 'complete' and calls == ['message', 'message']


async def test_hosted_content_of_an_unchanged_message_is_read_once_per_attempt(store, adapters):
    # Like files with an unchanged etag, hosted content is tagged by its parent message
    # version; pass 2 reuses pass 1's verified bytes instead of downloading again.
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
    assert lists == ['message', 'message'] and reads == ['image']
    assert len(adapters.storage.uploads) == 1


async def test_hosted_content_without_a_message_version_is_still_independently_checked(store, adapters):
    adapters.source.messages = [message(hosted=True)]
    reads = []
    original_open = adapters.source.open_hosted

    def opened(chat, mid, cid):
        reads.append(cid)
        return original_open(chat, mid, cid)

    adapters.source.open_hosted = opened
    _, result = await run_capture(store, adapters)
    assert result.outcome == 'complete' and reads == ['image', 'image']


async def test_failed_hosted_list_remains_partial_and_second_pass_rechecks(store, adapters):
    adapters.source.messages = [referencing(etag='stable')]
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


@pytest.mark.parametrize('field,value', [('etag', 'v2'), ('body', {'contentType': 'html', 'content': 'edited' + HOSTED_REF})])
async def test_changed_parent_reconciles_new_observations(store, adapters, field, value):
    adapters.source.messages = [referencing(etag='v1')]
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
    assert calls == [1, 2, 3, 4]


async def test_hosted_visibility_change_with_same_parent_version_is_reconciled(store, adapters):
    adapters.source.messages = [referencing(etag='stable')]
    calls = []

    async def hosted(chat, mid):
        calls.append(adapters.source.passes)
        if adapters.source.passes >= 2:
            yield {'id': 'image', 'contentType': 'image/png'}

    adapters.source.iter_hosted_contents = hosted
    _, result = await run_capture(store, adapters)
    assert result.outcome == 'complete' and result.item_count == 2
    assert adapters.source.passes == 4 and calls == [1, 2, 3, 4]


async def test_refreshed_attachment_url_on_overlapping_page_is_verified_without_false_movement(store, adapters):
    first = message(attached=True)
    first['attachments'][0]['contentUrl'] = 'https://tenant.sharepoint.com/file.pdf?sig=before'
    second = deepcopy(first)
    second['attachments'][0]['contentUrl'] = 'https://tenant.sharepoint.com/file.pdf?sig=after'
    adapters.source.messages = [first, second]
    _, result = await run_capture(store, adapters)
    assert result.outcome == 'complete' and result.item_count == 2
    assert adapters.source.passes == 2


def _hosted_unavailable(adapters, status):
    from shared.chat_capture_graph import ChatGraphError

    @asynccontextmanager
    async def unavailable(*args):
        raise ChatGraphError(f'Graph capture content failed with HTTP {status}', status_code=status)
        yield

    adapters.source.open_hosted = unavailable


@pytest.mark.parametrize('status', [404, 500])
async def test_hosted_content_microsoft_cannot_serve_is_recorded_not_partial(store, adapters, caplog, status):
    """Microsoft lists the hosted item but answers its bytes with gone (404) or a persistent
    server error (500, e.g. Teams /views/video clips). That is not our failure: the chat is
    COMPLETE, the item is kept as a byte-less record marked unavailable at the source, and
    the failure is still logged with its status."""
    from shared.models import ChatItemVersion
    caplog.set_level(logging.INFO, logger='shared.chat_capture')
    adapters.source.messages = [dict(message(hosted=True), etag='stable')]
    _hosted_unavailable(adapters, status)
    _, result = await run_capture(store, adapters)
    assert result.outcome == 'complete' and result.item_count == 2
    async with store.factory() as session:
        hosted = (await session.scalars(select(ChatItemVersion).where(
            ChatItemVersion.item_type == 'CHAT_HOSTED_CONTENT'))).all()
    assert len(hosted) == 1 and hosted[0].object_id is None
    assert hosted[0].payload['chat_catalogue_source_unavailable'] is True
    lines = [record.getMessage() for record in caplog.records if record.name == 'shared.chat_capture']
    failures = [value for value in lines if value.startswith('chat_capture_failure')]
    assert len(failures) == 2 and all(f'status={status}' in v for v in failures)


async def test_hosted_content_permission_denial_stays_partial(store, adapters):
    """A 403 can be our app's missing or misconfigured permission, so it stays PARTIAL."""
    adapters.source.messages = [dict(message(hosted=True), etag='stable')]
    _hosted_unavailable(adapters, 403)
    _, result = await run_capture(store, adapters)
    assert result.outcome == 'partial' and result.item_count == 1


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
