"""Exact captured chat reads through real handlers and disposable PostgreSQL."""
from __future__ import annotations

import base64
from dataclasses import replace
from datetime import datetime, timedelta
import hashlib
import importlib.util
import importlib
import os
from pathlib import Path
import sys
from uuid import uuid4
from types import ModuleType

import pytest
from fastapi import HTTPException
from sqlalchemy import select, update

from test_chat_catalogue_postgres import store, message, parent_for
from shared.chat_catalogue_identity import CapturedChatItem, ChatContentDescriptor
from shared import models

pytestmark = pytest.mark.integration
_ROOT = Path(__file__).resolve().parents[2]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, _ROOT / path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def snapshot_api():
    return load("catalogue_snapshot_api", "services/snapshot-service/main.py")


@pytest.fixture(scope="module")
def export_scope():
    return load("catalogue_export_scope", "workers/chat-export-worker/scope.py")


@pytest.fixture(scope="module")
def export_api():
    service_path = str(_ROOT / "services/job-service")
    if service_path not in sys.path:
        sys.path.insert(0, service_path)
    return load("catalogue_export_api", "services/job-service/chat_export.py")


def captured_message(body, graph_id="message-alpha", created="2026-10-01T10:00:00Z"):
    source = message(body, graph_id)
    return replace(source, payload=source.payload | {"createdDateTime": created,
        "chatTopic": "Captured chat", "from": {"user": {"displayName": "Captured sender"}},
        "body": {"content": body, "contentType": "html"}})


def inline_child(data=b"captured bytes", *, graph_id="attachment-alpha", parent="message-alpha", hosted=False):
    descriptor = ChatContentDescriptor(None, None, None, None,
        hashlib.sha256(data).hexdigest(), len(data), None, 1,
        base64.b64encode(data).decode(), None, None, None, None)
    return CapturedChatItem("CHAT_HOSTED_CONTENT" if hosted else "CHAT_ATTACHMENT", graph_id, parent,
        {"name": "captured.png", "contentType": "image/png", "contentUrl": "https://source.invalid/file"}, descriptor)


async def links(session, snapshot_id):
    return list((await session.scalars(select(models.SnapshotItem).where(
        models.SnapshotItem.snapshot_id == snapshot_id))).all())


async def test_messages_use_exact_old_versions_and_filter_before_pagination(store, snapshot_api):
    older = captured_message("older needle")
    newer = captured_message("other body", "message-beta", "2026-10-02T10:00:00Z")
    scope, _, _ = await store.capture([older, newer])
    await store.capture([captured_message("edited latest")])
    async with store.factory.begin() as db:
        result = await snapshot_api.list_snapshot_messages(str(scope.snapshot_id), page=1, size=1,
            chatId=None, folder=None, search="needle", db=db)
        assert result["totalElements"] == 1
        assert result["content"][0]["body"] == "older needle"
        assert result["content"][0]["externalId"] == "message-alpha"
        assert "chat_catalogue_expected_children" not in result["content"][0]["metadata"]["raw"]
        raw = await db.scalar(select(models.SnapshotItem).where(
            models.SnapshotItem.id == result["content"][0]["id"]))
        assert raw.extra_data == {"chat_id": scope.chat_id}
        assert raw.external_id != "message-alpha"


async def test_content_hydrates_inline_and_rejects_wrong_url_snapshot(store, snapshot_api):
    child = inline_child()
    scope, _, _ = await store.capture([parent_for(child, parent=captured_message("body")), child])
    other, _, _ = await store.capture([])
    async with store.factory.begin() as db:
        item = next(i for i in await links(db, scope.snapshot_id) if i.item_type == "CHAT_ATTACHMENT")
        response = await snapshot_api.get_item_content(str(scope.snapshot_id), str(item.id), download=False, db=db)
        assert response.body == b"captured bytes"
        assert response.media_type == "image/png"
        with pytest.raises(HTTPException) as caught:
            await snapshot_api.get_item_content(str(other.snapshot_id), str(item.id), download=False, db=db)
        assert caught.value.status_code == 404


async def test_export_materializes_selected_snapshot_and_exact_children(store, export_scope):
    child = inline_child()
    scope, _, _ = await store.capture([parent_for(child, parent=captured_message("old body")), child])
    await store.capture([captured_message("latest body")])
    async with store.factory.begin() as db:
        parent = next(i for i in await links(db, scope.snapshot_id) if i.item_type == "TEAMS_CHAT_MESSAGE")
        resolved = await export_scope.resolve(db, resource_id=scope.resource_id,
            snapshot_ids=[scope.snapshot_id], thread_path=None, item_ids=[parent.id])
        assert resolved.messages[0].extra_data["raw"]["body"]["content"] == "old body"
        attachment = resolved.attachment_map["message-alpha"][0]
        assert base64.b64decode(attachment["inline_b64"]) == b"captured bytes"
        assert attachment["sha256"] == hashlib.sha256(b"captured bytes").hexdigest()
        assert str(attachment["snapshot_id"]) == str(scope.snapshot_id)


async def test_browse_detail_and_search_return_captured_payloads(store, snapshot_api):
    scope, _, _ = await store.capture([captured_message("old searchable needle"),
        captured_message("another", "message-beta")])
    async with store.factory.begin() as db:
        result = await snapshot_api.list_snapshot_items(str(scope.snapshot_id), page=1, size=1,
            itemType="TEAMS_CHAT_MESSAGE", db=db)
        assert result.totalElements == 2
        assert result.content[0].metadata["raw"]["body"]["content"]
        detail = await snapshot_api.get_snapshot_item_detail(str(scope.snapshot_id), result.content[0].id, db=db)
        assert detail.metadata == result.content[0].metadata
        search = await snapshot_api.search_snapshot_items(str(scope.resource_id), query="needle",
            item_type=None, folder_path=None, snapshot_id=str(scope.snapshot_id), page=1, size=1, db=db)
        assert search.totalElements == 1
        assert search.content[0].externalId == "message-alpha"


async def test_folders_groups_reject_broken_new_format_links(store, snapshot_api):
    scope, _, _ = await store.capture([captured_message("body")])
    async with store.factory.begin() as db:
        await db.execute(update(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == scope.snapshot_id)
            .values(chat_version_id=None))
    async with store.factory.begin() as db:
        with pytest.raises(Exception, match="captured version|integrity"):
            await snapshot_api.list_snapshot_chat_groups(str(scope.snapshot_id), db=db)
        with pytest.raises(Exception, match="captured version|integrity"):
            await snapshot_api.get_snapshot_folders(str(scope.snapshot_id), item_type="TEAMS_CHAT_MESSAGE",
                category=None, page=1, size=50, db=db)


async def test_hosted_image_uses_selected_snapshot_content_url(store, snapshot_api):
    child = inline_child(b"image bytes", graph_id="hosted-alpha", hosted=True)
    parent = parent_for(child, parent=captured_message(
        '<img src="https://graph.microsoft.com/v1.0/chats/chat-alpha/messages/message-alpha/hostedContents/hosted-alpha/$value">'))
    scope, _, _ = await store.capture([parent, child])
    async with store.factory.begin() as db:
        result = await snapshot_api.list_snapshot_messages(str(scope.snapshot_id), page=1, size=50,
            chatId=None, folder=None, search=None, db=db)
        hosted = next(i for i in await links(db, scope.snapshot_id) if i.item_type == "CHAT_HOSTED_CONTENT")
        assert f"/snapshots/{scope.snapshot_id}/items/{hosted.id}/content" in result["content"][0]["body"]
        assert "/teams/hosted-content/" not in result["content"][0]["body"]


async def test_search_without_snapshot_selects_latest_captured_restore_point(store, snapshot_api):
    first, _, _ = await store.capture([captured_message("old needle")])
    latest, _, _ = await store.capture([captured_message("latest needle")])
    async with store.factory.begin() as db:
        await db.execute(update(models.Snapshot).where(models.Snapshot.id.in_([first.snapshot_id, latest.snapshot_id]))
            .values(status=models.SnapshotStatus.COMPLETED))
        result = await snapshot_api.search_snapshot_items(str(latest.resource_id), query="needle",
            item_type=None, folder_path=None, snapshot_id=None, page=1, size=50, db=db)
        assert result.totalElements == 1
        assert result.content[0].snapshotId == str(latest.snapshot_id)
        assert result.content[0].metadata["raw"]["body"]["content"] == "latest needle"


async def test_cross_workload_navigation_never_reads_future_chat_backup(store, snapshot_api):
    future, _, _ = await store.capture([captured_message("future private state")])
    parent_id, mail_id, mail_job_id, mail_snapshot_id = [uuid4() for _ in range(4)]
    async with store.factory.begin() as db:
        db.add(models.Resource(id=parent_id, tenant_id=store.tenant, type=models.ResourceType.ENTRA_USER,
            external_id="principal-alpha", display_name="Synthetic user"))
        await db.flush()
        await db.execute(update(models.Resource).where(models.Resource.id == store.resource)
            .values(parent_resource_id=parent_id))
        db.add(models.Resource(id=mail_id, tenant_id=store.tenant, type=models.ResourceType.USER_MAIL,
            external_id="mail-alpha", display_name="Synthetic mail", parent_resource_id=parent_id))
        db.add(models.Job(id=mail_job_id, type=models.JobType.BACKUP, tenant_id=store.tenant,
            status=models.JobStatus.COMPLETED))
        await db.flush()
        db.add(models.Snapshot(id=mail_snapshot_id, resource_id=mail_id, job_id=mail_job_id,
            backend_id=store.backend, status=models.SnapshotStatus.COMPLETED, created_at=datetime(2020, 1, 1)))
        await db.execute(update(models.Snapshot).where(models.Snapshot.id == future.snapshot_id)
            .values(status=models.SnapshotStatus.COMPLETED))
    async with store.factory.begin() as db:
        with pytest.raises(HTTPException) as caught:
            await snapshot_api.list_snapshot_messages(str(mail_snapshot_id), page=1, size=50,
                chatId=None, folder=None, search=None, db=db)
        assert caught.value.status_code == 404


async def test_blob_response_streams_exact_backend_with_durable_pin(store, snapshot_api, monkeypatch):
    from shared.storage.router import router
    scope = await store.new_scope()
    lease = await store.begin(scope)
    child = await store.binary(lease)
    await store.stage(lease, [parent_for(child, parent=captured_message("body")), child])
    await store.observe(scope, lease, [parent_for(child, parent=captured_message("body")), child])
    await store.publish(lease)
    calls = []
    class Storage:
        async def get_properties(self, container, path):
            calls.append(("properties", container, path))
            return object()
        async def download_stream(self, container, path):
            calls.append(("stream", container, path))
            yield b"a"
            yield b"bc"
        async def download(self, *_args):
            pytest.fail("catalogue HTTP response buffered a blob")
    monkeypatch.setattr(router, "get_store_by_id", lambda backend:
        Storage() if backend == str(scope.backend_id) else pytest.fail("wrong backend"))
    monkeypatch.setattr(snapshot_api, "async_session_factory", store.factory)
    async with store.factory.begin() as db:
        item = next(i for i in await links(db, scope.snapshot_id) if i.item_type == "CHAT_ATTACHMENT")
        response = await snapshot_api.get_item_content(str(scope.snapshot_id), str(item.id), download=False, db=db)
    async with store.factory.begin() as db:
        assert len(list((await db.scalars(select(models.ChatCataloguePin))).all())) == 1
    data = b"".join([part async for part in response.body_iterator])
    assert data == b"abc"
    assert response.headers["content-length"] == "3"
    assert calls == [("properties", child.content.container, child.content.path),
        ("stream", child.content.container, child.content.path)]
    async with store.factory.begin() as db:
        assert not list((await db.scalars(select(models.ChatCataloguePin))).all())


@pytest.mark.parametrize("pipeline_fails", [False, True])
async def test_export_holds_durable_pin_through_pipeline_and_releases_it(store, monkeypatch, pipeline_fails):
    package_name = "workers.chat_export_worker"
    if package_name not in sys.modules:
        package = ModuleType(package_name)
        package.__path__ = [str(_ROOT / "workers/chat-export-worker")]
        sys.modules[package_name] = package
    consumer = importlib.import_module(package_name + ".consumers.thread")
    scope, _, _ = await store.capture([captured_message("body")])
    export_id = uuid4()
    async with store.factory.begin() as db:
        parent = next(i for i in await links(db, scope.snapshot_id) if i.item_type == "TEAMS_CHAT_MESSAGE")
        spec = {"resourceId": str(scope.resource_id), "snapshotIds": [str(scope.snapshot_id)],
            "itemIds": [str(parent.id)]}
        db.add(models.Job(id=export_id, type=models.JobType.EXPORT, tenant_id=store.tenant,
            status=models.JobStatus.RUNNING, spec=spec))
    seen = []
    async def pipeline(sess, job_id, given_spec, resolved, job, *, read_lease=None):
        assert resolved.messages[0].extra_data["raw"]["body"]["content"] == "body"
        assert read_lease is not None
        read_lease.assert_valid()
        async with store.factory.begin() as db:
            pins = list((await db.scalars(select(models.ChatCataloguePin))).all())
            assert len(pins) == 1 and pins[0].owner_id == export_id
        seen.append(True)
        if pipeline_fails:
            raise RuntimeError("injected packaging failure")
    async def publish(*_args, **_kwargs):
        pass
    monkeypatch.setattr(consumer, "async_session_factory", store.factory)
    monkeypatch.setattr(consumer, "_run_pipeline", pipeline)
    monkeypatch.setattr(consumer, "publish", publish)
    async with store.factory() as db:
        job = await db.get(models.Job, export_id)
        await consumer._execute_export_job(db, str(export_id), spec, job)
    assert seen == [True]
    async with store.factory.begin() as db:
        assert not list((await db.scalars(select(models.ChatCataloguePin))).all())
        if pipeline_fails:
            assert (await db.get(models.Job, export_id)).status == models.JobStatus.FAILED


async def test_estimate_counts_selected_attachment_version_only(store, export_api):
    old = inline_child(b"old")
    scope, _, _ = await store.capture([parent_for(old, parent=captured_message("body")), old])
    latest = inline_child(b"new" * 10000)
    await store.capture([parent_for(latest, parent=captured_message("body")), latest])
    async with store.factory.begin() as db:
        rows = await links(db, scope.snapshot_id)
        parent = next(i for i in rows if i.item_type == "TEAMS_CHAT_MESSAGE")
        count, estimated_bytes, *_ = await export_api._resolve_counts(db, resource_id=str(scope.resource_id),
            snapshot_ids=[str(scope.snapshot_id)], thread_path=None, item_ids=[str(parent.id)])
        assert count == 1
        assert estimated_bytes == sum(i.content_size or 0 for i in rows)


async def test_metadata_only_card_exports_captured_json_not_missing_binary(store, export_scope, snapshot_api):
    card = CapturedChatItem("CHAT_ATTACHMENT", "card-alpha", "message-alpha",
        {"contentType": "application/vnd.microsoft.card.adaptive", "content": '{"type":"AdaptiveCard"}'}, None)
    scope, _, _ = await store.capture([parent_for(card, parent=captured_message("body")), card])
    async with store.factory.begin() as db:
        parent = next(i for i in await links(db, scope.snapshot_id) if i.item_type == "TEAMS_CHAT_MESSAGE")
        resolved = await export_scope.resolve(db, resource_id=scope.resource_id,
            snapshot_ids=[scope.snapshot_id], thread_path=None, item_ids=[parent.id])
        card_file = resolved.attachment_map["message-alpha"][0]
        assert card_file["inline_b64"]
        assert b"AdaptiveCard" in base64.b64decode(card_file["inline_b64"])
        assert card_file["name"].endswith(".metadata.json")
        child = next(i for i in await links(db, scope.snapshot_id) if i.item_type == "CHAT_ATTACHMENT")
        content = await snapshot_api.get_item_content(str(scope.snapshot_id), str(child.id), download=False, db=db)
        assert content["source"] == "metadata"
        assert content["content"]["content"] == '{"type":"AdaptiveCard"}'


async def test_message_sort_uses_timestamp_value_and_groups_use_captured_folder(store, snapshot_api):
    scope = await store.new_scope()
    from test_chat_catalogue_postgres import api
    async with store.factory.begin() as db:
        lease = await api().begin_capture(db, scope=scope, folder_path="chats/Frozen friendly name")
    messages = [captured_message("whole second", "whole", "2026-10-01T10:00:00Z"),
        captured_message("newer millisecond", "fractional", "2026-10-01T10:00:00.001Z")]
    await store.stage(lease, messages)
    await store.observe(scope, lease, messages)
    await store.publish(lease)
    async with store.factory.begin() as db:
        response = await snapshot_api.list_snapshot_messages(str(scope.snapshot_id), page=1, size=1,
            chatId=None, folder="chats/Frozen friendly name", search=None, db=db)
        assert response["content"][0]["body"] == "newer millisecond"
        groups = await snapshot_api.list_snapshot_chat_groups(str(scope.snapshot_id), db=db)
        assert groups[0]["count"] == 2
        assert groups[0]["displayName"] == "Frozen friendly name"
        folders = await snapshot_api.get_snapshot_folders(str(scope.snapshot_id), item_type="TEAMS_CHAT_MESSAGE",
            category=None, page=1, size=50, db=db)
        assert folders["content"] == [{"path": "chats/Frozen friendly name", "count": 2}]


async def test_identical_selected_chat_versions_allow_unselected_chat_changes(store, export_scope):
    from test_chat_catalogue_postgres import api
    alpha = captured_message("same alpha")
    first, _, first_result = await store.capture([alpha])
    second, _, second_result = await store.capture([alpha])
    assert first_result.inventory_id == second_result.inventory_id
    for source, body in ((first, "old beta"), (second, "changed beta")):
        beta = replace(source, chat_id="chat-beta")
        lease = await store.begin(beta)
        messages = [captured_message(body)]
        await store.stage(lease, messages)
        await store.observe(beta, lease, messages)
        await store.publish(lease)
    async with store.factory.begin() as db:
        parents = [item for sid in (first.snapshot_id, second.snapshot_id)
            for item in await links(db, sid) if (item.extra_data or {}).get("chat_id") == first.chat_id]
        resolved = await export_scope.resolve(db, resource_id=first.resource_id,
            snapshot_ids=[first.snapshot_id, second.snapshot_id], thread_path=None,
            item_ids=[parent.id for parent in parents])
        assert len(resolved.messages) == 1
        assert resolved.messages[0].extra_data["raw"]["body"]["content"] == "same alpha"
