"""Search must match and render the exact captured chat version, not its cache."""
from __future__ import annotations

from dataclasses import replace
import importlib.util
import os
from pathlib import Path
from uuid import uuid4

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

import httpx
import pytest
from sqlalchemy import select, text

import shared.models as models
from shared.security import create_access_token

_root = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("search_catalogue_fixture", Path(__file__).with_name("test_chat_catalogue_postgres.py"))
_fixture = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_fixture)
store = _fixture.store
message = _fixture.message

_spec = importlib.util.spec_from_file_location("search_catalogue_service", _root / "services/search-service/main.py")
search = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(search)

pytestmark = pytest.mark.integration


@pytest.fixture
def search_app(store):
    async def database():
        async with store.factory() as session:
            yield session

    search.app.dependency_overrides[search.get_db] = database
    yield search.app
    search.app.dependency_overrides.clear()


def headers(tenant):
    token = create_access_token({"sub": str(uuid4()), "roles": ["USER"], "tenantIds": [str(tenant)]})
    return {"Authorization": f"Bearer {token}"}


async def foreign_scope(store):
    tenant, resource, job, snapshot = uuid4(), uuid4(), uuid4(), uuid4()
    async with store.factory.begin() as session:
        org_id = await session.scalar(select(models.Tenant.org_id).where(models.Tenant.id == store.tenant))
        session.add(models.Tenant(id=tenant, org_id=org_id, display_name="Other tenant"))
        await session.flush()
        session.add(models.Resource(id=resource, tenant_id=tenant, type=models.ResourceType.USER_CHATS,
            external_id="foreign-principal", display_name="Other chats", extra_data={"chat_inventory_format": 1}))
        await session.flush()
        session.add(models.Job(id=job, type=models.JobType.BACKUP, tenant_id=tenant,
            resource_id=resource, status=models.JobStatus.RUNNING))
        await session.flush()
        session.add(models.Snapshot(id=snapshot, resource_id=resource, job_id=job,
            backend_id=store.backend, extra_data={"chat_inventory_format": 1}))
    return replace(await store.new_scope(), snapshot_id=snapshot, resource_id=resource,
        tenant_id=tenant, principal_id="foreign-principal")


async def add_misleading_cache(session, item, body):
    thread = models.ChatThread(tenant_id=item.tenant_id, chat_id=item.parent_external_id)
    session.add(thread)
    await session.flush()
    session.add(models.ChatThreadMessage(chat_thread_id=thread.id, message_external_id=item.external_id,
        body_content=body, metadata_raw={"body": {"content": body}}))


async def test_route_filters_captured_body_before_count_and_page_and_preserves_history(store, search_app):
    prefix = "old prefix " * 15
    old_scope, _, _ = await store.capture([
        message(prefix + "needlephrase historical alpha", graph_id="message-alpha"),
        message(prefix + "needlephrase historical beta", graph_id="message-beta"),
    ])
    await store.capture([message(prefix + "current replacement", graph_id="message-alpha")])
    other_chat = replace(await store.new_scope(), chat_id="chat-beta")
    await store.capture([message(prefix + "different chat collision", graph_id="message-alpha")], scope=other_chat)
    # Same chat/message IDs in another tenant must neither match nor hydrate our hit.
    await store.capture([message(prefix + "needlephrase historical foreign secret", graph_id="message-alpha")],
        scope=await foreign_scope(store))
    async with store.factory.begin() as session:
        item = await session.scalar(select(models.SnapshotItem).where(
            models.SnapshotItem.snapshot_id == old_scope.snapshot_id).limit(1))
        await add_misleading_cache(session, item, "MUTABLE WRONG PREVIEW")

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=search_app), base_url="http://search") as client:
        first = await client.get("/api/v1/search", params={"q": "needlephrase historical", "size": 1},
            headers=headers(store.tenant))
        assert first.status_code == 200, first.text
        result = first.json()
        assert result["totalResults"] == 2 and result["totalPages"] == 2
        assert len(result["results"]) == 1
        hit = result["results"][0]
        assert hit["snapshotId"] == str(old_scope.snapshot_id)
        assert hit["externalId"] in {"message-alpha", "message-beta"}
        assert hit["preview"].startswith(prefix + "needlephrase historical")
        assert "MUTABLE" not in hit["preview"] and "foreign" not in hit["preview"]
        assert hit["source"]["tenantId"] == str(store.tenant)
        second = await client.get("/api/v1/search", params={"q": "needlephrase historical", "size": 1, "page": 2},
            headers=headers(store.tenant))
        assert second.status_code == 200, second.text
        assert second.json()["totalResults"] == 2
        assert second.json()["results"][0]["externalId"] != hit["externalId"]
        mismatch = await client.get("/api/v1/search", params={"q": "needlephrase collision"}, headers=headers(store.tenant))
        assert mismatch.status_code == 200 and mismatch.json()["totalResults"] == 0


async def test_route_without_named_tenant_never_leaks_foreign_legacy_results(store, search_app):
    own_scope = await store.new_scope()
    other_scope = await foreign_scope(store)
    async with store.factory.begin() as session:
        for scope in [own_scope, other_scope]:
            snapshot = await session.get(models.Snapshot, scope.snapshot_id)
            snapshot.extra_data = {}
            session.add(models.SnapshotItem(snapshot_id=scope.snapshot_id, tenant_id=scope.tenant_id,
                backend_id=scope.backend_id, external_id="legacy", item_type="EMAIL", name="sharedneedle",
                extra_data={"raw": {"subject": "sharedneedle", "bodyPreview": str(scope.tenant_id)}}))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=search_app), base_url="http://search") as client:
        response = await client.get("/api/v1/search", params={"q": "sharedneedle"}, headers=headers(store.tenant))
    assert response.status_code == 200, response.text
    assert response.json()["totalResults"] == 1
    assert response.json()["results"][0]["source"]["tenantId"] == str(store.tenant)


async def test_index_uses_captured_version_despite_misleading_mutable_cache(store):
    scope, _, _ = await store.capture([message("prefix " * 25 + "historicalneedle")])
    async with store.factory.begin() as session:
        await session.execute(text("ALTER TABLE snapshot_items ADD COLUMN search_vector tsvector"))
        item = await session.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == scope.snapshot_id))
        stored_id, stored_metadata = item.external_id, dict(item.extra_data)
        await add_misleading_cache(session, item, "wrongmutableword")
        await session.flush()
        await search.index_snapshot_item(item, session)
        result = (await session.execute(text("SELECT search_vector @@ plainto_tsquery('english', 'historicalneedle'), "
            "search_vector @@ plainto_tsquery('english', 'wrongmutableword') FROM snapshot_items WHERE id=:id"),
            {"id": item.id})).one()
        assert result == (True, False)
        assert item.external_id == stored_id and item.extra_data == stored_metadata


async def test_new_format_missing_version_cannot_fall_back_to_mutable_cache(store):
    scope = await store.new_scope()
    async with store.factory.begin() as session:
        await session.execute(text("ALTER TABLE snapshot_items ADD COLUMN search_vector tsvector"))
        item = models.SnapshotItem(snapshot_id=scope.snapshot_id, tenant_id=scope.tenant_id,
            backend_id=scope.backend_id, external_id="message", parent_external_id="chat-alpha",
            item_type="TEAMS_CHAT_MESSAGE", name="message", extra_data={})
        session.add(item)
        await session.flush()
        await add_misleading_cache(session, item, "wrongmutableword")
        await session.flush()
        await search.index_snapshot_item(item, session)
        vector = await session.scalar(text("SELECT search_vector FROM snapshot_items WHERE id=:id"), {"id": item.id})
        assert vector is None


async def test_legacy_index_still_hydrates_shared_message_cache(store):
    scope = await store.new_scope()
    async with store.factory.begin() as session:
        await session.execute(text("ALTER TABLE snapshot_items ADD COLUMN search_vector tsvector"))
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        snapshot.extra_data = {}
        item = models.SnapshotItem(snapshot_id=scope.snapshot_id, tenant_id=scope.tenant_id,
            backend_id=scope.backend_id, external_id="legacy-message", parent_external_id="chat-alpha",
            item_type="TEAMS_CHAT_MESSAGE", name="message", extra_data={})
        session.add(item)
        await session.flush()
        await add_misleading_cache(session, item, "legacyneedle")
        await session.flush()
        await search.index_snapshot_item(item, session)
        assert await session.scalar(text("SELECT search_vector @@ plainto_tsquery('english', 'legacyneedle') "
            "FROM snapshot_items WHERE id=:id"), {"id": item.id}) is True
