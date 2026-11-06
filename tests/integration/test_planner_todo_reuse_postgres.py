"""Planner / To Do incrementals cost O(changes): unchanged items reuse the prior blob, and a run
where nothing changed points at the prior full snapshot instead of re-inserting every row.

Runs the production ``_backup_todo`` / ``_backup_planner`` (extracted from main.py) against
PostgreSQL with a fake Graph and blob store."""
from __future__ import annotations

import ast
import asyncio
import hashlib
import json
import os
import re
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from types import MethodType, SimpleNamespace
from typing import Dict, List

import pytest
import pytest_asyncio
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

from shared.database import Base
import shared.models as models

pytestmark = pytest.mark.integration
MAIN = Path(__file__).parents[2] / "workers/backup-worker/main.py"


def _reuse_trigger_ddl():
    source = (Path(__file__).parents[2] / "shared" / "database.py").read_text()
    function = re.search(r'"""(CREATE OR REPLACE FUNCTION validate_snapshot_reuse_target\(\).*?)"""', source, re.S).group(1)
    return [function, "CREATE TRIGGER snapshots_reuse_validate BEFORE INSERT OR UPDATE OF reuse_of_snapshot_id, "
            "reuse_chain_root_id ON snapshots FOR EACH ROW EXECUTE FUNCTION validate_snapshot_reuse_target();"]


class Graph:
    def __init__(self):
        self.lists = {"L1": {"id": "L1", "displayName": "Groceries"}}
        self.tasks = {"L1": [{"id": "T1", "title": "milk"}, {"id": "T2", "title": "eggs"}]}
        self.checklist = {"T1": [{"id": "c1", "displayName": "2%"}]}
        self.plans = [{"id": "P1", "title": "Launch"}]
        self.plan_tasks = {"P1": [{"id": "PT1", "title": "ship"}, {"id": "PT2", "title": "docs"}]}

    async def get_user_todo_lists(self, user):
        return {"value": list(self.lists.values())}

    async def get_user_todo_tasks(self, user, list_id):
        return {"value": self.tasks[list_id]}

    async def get_user_todo_task_checklist(self, user, list_id, task_id):
        return {"value": self.checklist.get(task_id, [])}

    async def get_user_todo_task_linked_resources(self, user, list_id, task_id):
        return {"value": []}

    async def get_planner_plans_for_group(self, group):
        return {"value": self.plans}

    async def get_planner_tasks(self, plan_id=None):
        return {"value": self.plan_tasks[plan_id]}

    async def get_planner_task_details(self, task_id):
        return {"id": task_id, "description": "d-" + task_id}


@pytest_asyncio.fixture
async def env():
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires explicitly configured disposable PostgreSQL")
    schema = "item_reuse_test_" + uuid.uuid4().hex
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}, "statement_cache_size": 0})
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await conn.run_sync(Base.metadata.create_all)
        for ddl in _reuse_trigger_ddl():
            await conn.execute(text(ddl))
    factory = async_sessionmaker(engine, expire_on_commit=False)
    tenant, backend = uuid.uuid4(), uuid.uuid4()
    async with factory.begin() as session:
        org = models.Organization(name="Synthetic", slug=uuid.uuid4().hex)
        session.add(org)
        await session.flush()
        session.add(models.Tenant(id=tenant, org_id=org.id, display_name="Synthetic"))
        session.add(models.StorageBackend(id=backend, kind="azure", name=uuid.uuid4().hex,
                    endpoint="https://storage.invalid", config={}, secret_ref="synthetic-reference"))
    # Production added snapshot_items.backend_id by ALTER (nullable); these handlers leave it unset.
    async with factory.begin() as session:
        await session.execute(text("ALTER TABLE snapshot_items ALTER COLUMN backend_id DROP NOT NULL"))

    uploads: List[str] = []

    async def upload(container, path, data, shard, **kwargs):
        uploads.append(path)
        return {"success": True}

    tree = ast.parse(MAIN.read_text())
    nodes = [n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef) and n.name in ("_backup_todo", "_backup_planner")]
    ns = dict(asyncio=asyncio, json=json, hashlib=hashlib, uuid=uuid, time=time, Dict=Dict, List=List,
              GraphClient=object, Resource=object, Snapshot=object, Tenant=object, SnapshotItem=models.SnapshotItem,
              async_session_factory=factory, upload_blob_with_retry=upload,
              azure_storage_manager=SimpleNamespace(get_shard_for_resource=lambda *a: None,
                                                    get_container_name=lambda *a: "c",
                                                    build_blob_path=lambda *parts: "/".join(map(str, parts))))
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(MAIN), "exec"), ns)
    worker = SimpleNamespace(worker_id="test")
    for name in ("_backup_todo", "_backup_planner"):
        setattr(worker, name, MethodType(ns[name], worker))
    clock = [datetime(2026, 11, 5, 10)]

    async def run(kind, graph, rtype):
        resource = env.resources.get(kind)
        if resource is None:
            resource = SimpleNamespace(id=uuid.uuid4(), display_name=kind, external_id="ext-" + kind)
            async with factory.begin() as session:
                session.add(models.Resource(id=resource.id, tenant_id=tenant, type=rtype,
                                            external_id=resource.external_id, display_name=kind))
            env.resources[kind] = resource
        clock[0] += timedelta(hours=1)
        sid, job = uuid.uuid4(), uuid.uuid4()
        async with factory.begin() as session:
            session.add(models.Job(id=job, type=models.JobType.BACKUP, tenant_id=tenant, resource_id=resource.id,
                                   status=models.JobStatus.RUNNING))
            await session.flush()
            session.add(models.Snapshot(id=sid, resource_id=resource.id, job_id=job, backend_id=backend,
                                        status=models.SnapshotStatus.IN_PROGRESS, started_at=clock[0], created_at=clock[0]))
        snapshot = SimpleNamespace(id=sid, delta_tokens_json=None)
        uploads.clear()
        handler = worker._backup_todo if kind == "todo" else worker._backup_planner
        result = await handler(graph, resource, snapshot, SimpleNamespace(id=tenant), resource.external_id)
        async with factory.begin() as session:
            await session.execute(text("UPDATE snapshots SET status='COMPLETED', item_count=:n WHERE id=:s"),
                                  {"s": sid, "n": result["item_count"]})
            own = (await session.execute(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == sid))).scalars().all()
            snap = await session.get(models.Snapshot, sid)
        return SimpleNamespace(id=sid, result=result, uploads=list(uploads), rows=own, snap=snap)

    env = SimpleNamespace(run=run, resources={}, factory=factory)
    yield env
    await engine.dispose()


async def _inventory(env, sid):
    from shared.snapshot_reuse import items_scope
    async with env.factory() as session:
        rows = (await session.execute(select(models.SnapshotItem).where(await items_scope(session, sid)))).scalars().all()
    return {(r.item_type, r.external_id): r.blob_path for r in rows}


@pytest.mark.parametrize("kind,rtype", [("todo", models.ResourceType.TODO), ("planner", models.ResourceType.PLANNER)])
async def test_unchanged_run_points_and_changed_run_uploads_only_changes(env, kind, rtype):
    graph = Graph()
    first = await env.run(kind, graph, rtype)
    assert first.uploads and len(first.rows) == len(first.uploads)
    full = await _inventory(env, first.id)

    second = await env.run(kind, graph, rtype)
    assert second.uploads == [] and second.rows == []
    assert second.snap.reuse_chain_root_id == first.id
    assert second.result["item_count"] == len(first.rows) and second.result["new_item_count"] == 0
    assert await _inventory(env, second.id) == full

    if kind == "todo":
        graph.tasks["L1"] = [{"id": "T1", "title": "oat milk"}]      # T1 changed, T2 deleted
        changed = {("TODO_TASK", "T1")}
        gone = {("TODO_TASK", "T2")}
    else:
        graph.plan_tasks["P1"] = [{"id": "PT1", "title": "ship v2"}]  # PT1 changed, PT2 deleted
        changed = {("PLANNER_TASK", "PT1")}
        gone = {("PLANNER_TASK", "PT2"), ("PLANNER_TASK_DETAILS", "PT2:details")}
    third = await env.run(kind, graph, rtype)
    assert len(third.uploads) == len(changed)
    after = await _inventory(env, third.id)
    assert set(after) == set(full) - gone
    for key, path in after.items():
        assert (path != full[key]) == (key in changed)
