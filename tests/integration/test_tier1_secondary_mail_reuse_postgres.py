"""Tier-1 shared/room mailbox archive + recoverable tiers are re-listed in full every run (no delta
on those roots). A message whose changeKey and folder match the prior snapshot's row must not be
re-uploaded nor have its attachments re-fetched: its prior rows (and blobs) are copied forward.
Changed / new messages persist fresh; messages gone from the listing are not carried."""
from __future__ import annotations

from datetime import datetime, timedelta
import importlib.util
import os
from pathlib import Path
import re
import sys
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

from shared.database import Base, SNAPSHOT_ITEMS_EFFECTIVE_FUNCTION
import shared.models as models

pytestmark = pytest.mark.integration
ROOT = Path(__file__).parents[2]


def _worker_module():
    name = "bw_tier1_secondary_reuse"
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, ROOT / "workers" / "backup-worker" / "main.py")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return sys.modules[name]


@pytest_asyncio.fixture
async def db(monkeypatch):
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires explicitly configured disposable PostgreSQL")
    schema = "tier1_secondary_test_" + uuid4().hex
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema},
                                                    "statement_cache_size": 0})
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(text("CREATE UNIQUE INDEX uq_snapshot_items_snap_ext_type "
                                "ON snapshot_items (snapshot_id, external_id, item_type)"))
        await conn.execute(text("ALTER TABLE snapshot_items ALTER COLUMN backend_id DROP NOT NULL"))
        await conn.execute(text(SNAPSHOT_ITEMS_EFFECTIVE_FUNCTION))
    factory = async_sessionmaker(engine, expire_on_commit=False)
    tenant, backend, rid = uuid4(), uuid4(), uuid4()
    async with factory.begin() as session:
        org = models.Organization(name="Synthetic", slug=uuid4().hex)
        session.add(org)
        await session.flush()
        session.add(models.Tenant(id=tenant, org_id=org.id, display_name="Synthetic"))
        session.add(models.StorageBackend(id=backend, kind="azure", name=uuid4().hex,
                    endpoint="https://storage.invalid", config={}, secret_ref="synthetic-reference"))
        await session.flush()
        session.add(models.Resource(id=rid, tenant_id=tenant, type=models.ResourceType.SHARED_MAILBOX,
                                    external_id="shared-box", display_name="Shared"))

    worker_module = _worker_module()
    monkeypatch.setattr(worker_module, "async_session_factory", factory)
    uploads, att_lists = [], []

    async def upload(container, path, data, shard, **kwargs):
        uploads.append(path)
        return {"success": True}
    monkeypatch.setattr(worker_module, "upload_blob_with_retry", upload)
    monkeypatch.setattr(worker_module, "azure_storage_manager", SimpleNamespace(
        get_shard_for_resource=lambda *a: None, get_container_name=lambda *a: "email",
        build_blob_path=lambda *parts: "/".join(map(str, parts))))

    class Graph:
        async def list_message_attachments(self, user, msg_id):
            att_lists.append(msg_id)
            return [{"id": "a1", "@odata.type": "#microsoft.graph.fileAttachment", "name": "f.txt",
                     "size": 3, "contentBytes": "YWJj"}]

    clock = [datetime(2026, 11, 5, 10)]

    async def run(messages):
        clock[0] += timedelta(hours=1)
        sid, job = uuid4(), uuid4()
        async with factory.begin() as session:
            session.add(models.Job(id=job, type=models.JobType.BACKUP, tenant_id=tenant, resource_id=rid,
                                   status=models.JobStatus.RUNNING))
            await session.flush()
            session.add(models.Snapshot(id=sid, resource_id=rid, job_id=job, backend_id=backend,
                                        status=models.SnapshotStatus.IN_PROGRESS, started_at=clock[0], created_at=clock[0]))
        uploads.clear()
        att_lists.clear()
        fake = SimpleNamespace(worker_id="test")
        fake._backup_message_attachments = worker_module.BackupWorker._backup_message_attachments.__get__(fake)
        count, _bytes, failed = await worker_module.BackupWorker._persist_tier1_mail_items(
            fake, Graph(), SimpleNamespace(id=rid, external_id="shared-box"),
            SimpleNamespace(id=sid, job_id=job), SimpleNamespace(id=tenant), [dict(m) for m in messages])
        async with factory.begin() as session:
            await session.execute(text("UPDATE snapshots SET status='COMPLETED' WHERE id=:s"), {"s": sid})
            rows = (await session.execute(text(
                "SELECT item_type, external_id, blob_path FROM snapshot_items WHERE snapshot_id=:s"), {"s": sid})).all()
        return SimpleNamespace(count=count, failed=failed, uploads=list(uploads), att_lists=list(att_lists),
                               rows={(r.item_type, r.external_id): r.blob_path for r in rows})

    yield run
    await engine.dispose()


def msg(mid, ck, source="archive", folder="AF1", path="Archive/Inbox", att=False):
    return {"id": mid, "changeKey": ck, "subject": mid, "parentFolderId": folder, "hasAttachments": att,
            "_source_mailbox": source, "_source_folder_id": folder, "_full_folder_path": path}


async def test_unchanged_secondary_messages_reuse_prior_rows_and_skip_attachment_fetch(db):
    base = [msg("A", "1", att=True), msg("B", "1"), msg("R", "1", source="recoverable", folder="RF", path="Deletions"),
            msg("P", "1", source="primary", folder="PF", path="Inbox")]
    first = await db(base)
    assert first.failed == set() and len(first.uploads) == 5 and first.att_lists == ["A"]

    # A, R unchanged; B changed; C new; the archive listing no longer has nothing else; P primary.
    second = await db([msg("A", "1", att=True), msg("B", "2"), msg("C", "1"),
                       msg("R", "1", source="recoverable", folder="RF", path="Deletions"),
                       msg("P", "1", source="primary", folder="PF", path="Inbox")])
    assert second.failed == set()
    assert second.att_lists == []                          # A's attachment not re-listed
    assert sorted(p.rsplit("/", 1)[-1] for p in second.uploads) == ["B", "C", "P"]
    assert set(second.rows) == set(first.rows) | {("EMAIL", "C")}
    for key in (("EMAIL", "A"), ("EMAIL", "R")):
        assert second.rows[key] == first.rows[key]         # prior blob reused
    att = [k for k in first.rows if k[0] == "EMAIL_ATTACHMENT"]
    assert att and second.rows[att[0]] == first.rows[att[0]]
    assert second.count == len(second.rows)

    # Deleted (gone from listing) and moved (folder changed) messages are not carried.
    third = await db([msg("A", "1", folder="AF2", path="Archive/Old", att=True)])
    assert set(third.rows) == {("EMAIL", "A")} | set(k for k in first.rows if k[0] == "EMAIL_ATTACHMENT")
    assert third.att_lists == ["A"]
