"""Activity drilldown must show live progress for an IN_PROGRESS resource.

The chat path publishes snapshots.item_count / bytes_total while running and settles
new_item_count / bytes_added only at the end, so the drilldown (which prefers the
delta columns) showed "Chats 0 —" against 1,231 items in the database
(Railway, 2026-11-04). In-progress rows must display what has been captured so far.
"""
import importlib.util
import os
import pathlib
import sys
from uuid import uuid4

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

import pytest
from sqlalchemy import text

from test_chat_catalogue_postgres import store
from shared import models

pytestmark = pytest.mark.integration

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
_SVC_DIR = _REPO_ROOT / "services" / "audit-service"
for path in (_REPO_ROOT, _SVC_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
_spec = importlib.util.spec_from_file_location("audit_main_children_under_test", _SVC_DIR / "main.py")
_as = importlib.util.module_from_spec(_spec)
sys.modules["audit_main_children_under_test"] = _as
_spec.loader.exec_module(_as)


async def test_in_progress_child_shows_items_and_bytes_so_far(store, monkeypatch):
    scope = await store.new_scope()
    batch_id = str(uuid4())
    parent_id, child_id, job_id, snap_id = uuid4(), uuid4(), uuid4(), uuid4()
    async with store.factory.begin() as session:
        tenant_id = await session.scalar(text("SELECT tenant_id FROM resources WHERE id=:id"), {"id": scope.resource_id})
        session.add(models.Resource(id=parent_id, tenant_id=tenant_id, type=models.ResourceType.ENTRA_USER,
                                    external_id="aad-user", display_name="Bhavya"))
        await session.flush()
        session.add(models.Resource(id=child_id, tenant_id=tenant_id, type=models.ResourceType.USER_CHATS,
                                    external_id="aad-user", display_name="Chats", parent_resource_id=parent_id))
        await session.flush()
        session.add(models.Job(id=job_id, type=models.JobType.BACKUP, tenant_id=tenant_id, resource_id=child_id,
                               status=models.JobStatus.RUNNING, spec={"batch_id": batch_id},
                               batch_resource_ids=[parent_id, child_id]))
        await session.flush()
        session.add(models.Snapshot(id=snap_id, resource_id=child_id, job_id=job_id, backend_id=scope.backend_id,
                                    status=models.SnapshotStatus.IN_PROGRESS, type=models.SnapshotType.FULL,
                                    item_count=1231, new_item_count=0, bytes_total=78419186, bytes_added=0))
    monkeypatch.setattr(_as, "async_session_factory", store.factory)
    body = await _as.get_batch_children(batch_id)
    children = [c for r in body["resources"] for c in (r.get("children") or [])]
    chats = next(c for c in children if c["type"] == "USER_CHATS")
    assert chats["status"] == "IN_PROGRESS"
    assert chats["itemCount"] == 1231
    assert chats["bytesAdded"] == 78419186


async def test_parentless_resources_of_any_type_are_listed_in_the_drilldown(store, monkeypatch):
    # Railway 2026-11-06: a batch of 6 TEAMS_CHANNEL + 16 M365_GROUP expanded to nothing — only five
    # types were top-level and everything else waited for a parent user it never had.
    scope = await store.new_scope()
    batch_id = str(uuid4())
    channel_id, group_id, job_id = uuid4(), uuid4(), uuid4()
    async with store.factory.begin() as session:
        tenant_id = await session.scalar(text("SELECT tenant_id FROM resources WHERE id=:id"), {"id": scope.resource_id})
        session.add(models.Resource(id=channel_id, tenant_id=tenant_id, type=models.ResourceType.TEAMS_CHANNEL,
                                    external_id="team-1", display_name="Axsaro"))
        session.add(models.Resource(id=group_id, tenant_id=tenant_id, type=models.ResourceType.M365_GROUP,
                                    external_id="group-1", display_name="QFION"))
        await session.flush()
        session.add(models.Job(id=job_id, type=models.JobType.BACKUP, tenant_id=tenant_id, resource_id=channel_id,
                               status=models.JobStatus.RUNNING, spec={"batch_id": batch_id},
                               batch_resource_ids=[channel_id, group_id]))
    monkeypatch.setattr(_as, "async_session_factory", store.factory)
    body = await _as.get_batch_children(batch_id)
    assert sorted((r["type"], r["displayName"]) for r in body["resources"]) == [
        ("M365_GROUP", "QFION"), ("TEAMS_CHANNEL", "Axsaro")]
