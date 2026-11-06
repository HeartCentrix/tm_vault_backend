"""One email in N mailboxes (To/CC/distribution list) downloaded each attachment N times.

Railway 2026-11-06, 10 mailboxes: 15,864 EMAIL copies of 9,728 distinct messages; 18,646
EMAIL_ATTACHMENT rows but 1,720 distinct contents, 991 MB pulled from Graph for 413 MB of distinct
bytes. Blob storage was already deduped by content hash, but only AFTER every copy's bytes had
come down from Graph. Now a copy whose attachments the tenant already stored (same
internetMessageId, name, Graph size, content type, inline flag; blob still readable) lists
attachment metadata only and points its own rows at the stored blob.
"""
from __future__ import annotations

import ast
import asyncio
import base64
import hashlib
import json
import os
import time
from datetime import datetime
from pathlib import Path
from types import MethodType, SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4
import uuid

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

from shared.database import Base
from shared.graph_batch import BatchResponse
import shared.models as models
from shared.provider_admission import ProviderDeferred
from shared.provider_runtime import ProviderBudgetExhausted, ProviderTurnDeferred

pytestmark = pytest.mark.integration

REPORT = b"%PDF" + b"r" * 40_000
LOGO = b"\x89PNG" + b"l" * 3_000


def _graph_attachments(mailbox: str) -> List[Dict[str, Any]]:
    """Graph gives every mailbox copy its own attachment ids; size is Graph's, not len(bytes)."""
    return [
        {"id": f"{mailbox}-att-report", "@odata.type": "#microsoft.graph.fileAttachment",
         "name": "Q3 report.pdf", "contentType": "application/pdf", "size": len(REPORT) + 180,
         "isInline": False, "contentId": None, "contentBytes": base64.b64encode(REPORT).decode()},
        {"id": f"{mailbox}-att-logo", "@odata.type": "#microsoft.graph.fileAttachment",
         "name": "image001.png", "contentType": "image/png", "size": len(LOGO) + 150,
         "isInline": True, "contentId": "image001.png@01DB", "contentBytes": base64.b64encode(LOGO).decode()},
    ]


class Graph:
    """Fake Graph that counts list/content sub-requests and attachment bytes it sent."""

    def __init__(self, mailbox: str, attachments=None):
        self.attachments = attachments if attachments is not None else _graph_attachments(mailbox)
        self.calls = 0
        self.bytes = 0

    async def batch(self, requests):
        out = {}
        for r in requests:
            self.calls += 1
            metadata_only = "$select=" in r.url
            value = []
            for att in self.attachments:
                att = dict(att)
                if metadata_only:
                    selected = r.url.split("$select=", 1)[1].split("&")[0].split(",")
                    att = {k: v for k, v in att.items() if k in selected or k == "@odata.type"}
                else:
                    self.bytes += len(base64.b64decode(att["contentBytes"]))
                value.append(att)
            out[r.id] = BatchResponse(r.id, 200, {}, {"value": value})
        return out

    async def get_message_attachment_content(self, user_id, message_id, attachment_id):
        self.calls += 1
        raise AssertionError("contentBytes were listed; no $value fetch expected")


class Store:
    def __init__(self):
        self.blobs: Dict[str, bytes] = {}

    async def get_blob_properties(self, container, path):
        data = self.blobs.get(f"{container}/{path}")
        return None if data is None else {"size": len(data)}


@pytest_asyncio.fixture
async def env(monkeypatch):
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires explicitly configured disposable PostgreSQL")
    schema = "mail_att_reuse_" + uuid4().hex
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}, "statement_cache_size": 0})
    import shared.mail_attachment_reuse as reuse
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(text(reuse.INDEX_SQL))
    factory = async_sessionmaker(engine, expire_on_commit=False)
    monkeypatch.setattr(reuse, "async_session_factory", factory)
    monkeypatch.setattr(reuse, "_index_ready", None)
    backend = uuid4()
    async with factory.begin() as s:
        s.add(models.StorageBackend(id=backend, kind="azure", name=uuid4().hex,
              endpoint="https://storage.invalid", config={}, secret_ref="synthetic-reference"))

    store = Store()

    async def put(container, path, content, shard, **kwargs):
        store.blobs[f"{container}/{path}"] = content
        return {"success": True, "blob_path": path}

    async def put_dedup(tenant_id, content_hash, container, path, content, shard, **kwargs):
        return await put(container, path, content, shard)

    import shared.azure_storage as storage
    monkeypatch.setattr(storage, "upload_blob_with_dedup", put_dedup)

    path = Path(__file__).resolve().parents[2] / "workers/backup-worker/main.py"
    tree = ast.parse(path.read_text())
    names = {"_usermail_backup_attachments", "EnumerationFailed", "mailbox_batch_concurrency"}
    nodes = [n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
             and n.name in names]
    ns = dict(asyncio=asyncio, os=os, hashlib=hashlib, time=time, json=json, uuid=uuid, datetime=datetime,
              Dict=Dict, List=List, Optional=Optional, Tuple=Tuple, Any=Any, GraphClient=object, Resource=object,
              Snapshot=object, Tenant=object, SnapshotItem=models.SnapshotItem,
              azure_storage_manager=SimpleNamespace(get_shard_for_resource=lambda *a: store,
                                                   get_container_name=lambda tenant, kind: f"backup-{kind}-{str(tenant)[:8]}",
                                                   build_blob_path=lambda *parts: "/".join(map(str, parts))),
              upload_blob_with_retry=put, _MAIL_ATTACHMENT_INLINE_MAX_BYTES=0,
              _maybe_inline_attachment=lambda content, max_bytes=None: None,
              _extract_cid_refs_from_body=lambda body: set(),
              ProviderDeferred=ProviderDeferred, ProviderBudgetExhausted=ProviderBudgetExhausted,
              ProviderTurnDeferred=ProviderTurnDeferred)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec"), ns)
    worker = SimpleNamespace(worker_id="test-mail-reuse")
    worker._usermail_backup_attachments = MethodType(ns["_usermail_backup_attachments"], worker)

    tenants: Dict[str, uuid.UUID] = {}

    async def tenant(label="tm"):
        if label not in tenants:
            tid = uuid4()
            async with factory.begin() as s:
                org = models.Organization(name="Synthetic", slug=uuid4().hex)
                s.add(org)
                await s.flush()
                s.add(models.Tenant(id=tid, org_id=org.id, display_name=label))
            tenants[label] = tid
        return tenants[label]

    async def backup(mailbox, graph, internet_message_id="<q3-report@tm.example>", tenant_label="tm"):
        """Capture one mailbox copy of the message and persist its rows like the worker does."""
        tid = await tenant(tenant_label)
        rid, jid, sid = uuid4(), uuid4(), uuid4()
        async with factory.begin() as s:
            s.add(models.Resource(id=rid, tenant_id=tid, type=models.ResourceType.USER_MAIL,
                                  external_id=mailbox, display_name=mailbox))
            await s.flush()
            s.add(models.Job(id=jid, type=models.JobType.BACKUP, tenant_id=tid, resource_id=rid,
                             status=models.JobStatus.RUNNING))
            await s.flush()
            s.add(models.Snapshot(id=sid, resource_id=rid, job_id=jid, backend_id=backend,
                                  status=models.SnapshotStatus.IN_PROGRESS))
        msg = {"id": f"{mailbox}-msg", "hasAttachments": True, "subject": "Q3 report"}
        if internet_message_id is not None:
            msg["internetMessageId"] = internet_message_id
        items, size = await worker._usermail_backup_attachments(
            graph, SimpleNamespace(id=rid), SimpleNamespace(id=sid), SimpleNamespace(id=tid), mailbox, [msg])
        async with factory.begin() as s:
            for item in items:
                item.backend_id = backend
                s.add(item)
        return items

    yield SimpleNamespace(backup=backup, store=store, factory=factory)
    async with engine.begin() as conn:
        await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    await engine.dispose()


def _restorable(items, mailbox, store):
    by_id = {i.external_id: i for i in items}
    assert set(by_id) == {f"{mailbox}-msg::{mailbox}-att-report", f"{mailbox}-msg::{mailbox}-att-logo"}
    report, logo = by_id[f"{mailbox}-msg::{mailbox}-att-report"], by_id[f"{mailbox}-msg::{mailbox}-att-logo"]
    for item, content in ((report, REPORT), (logo, LOGO)):
        assert item.extra_data["parent_item_id"] == f"{mailbox}-msg"
        assert item.extra_data["resolved"] is True
        assert item.content_hash == item.content_checksum == hashlib.sha256(content).hexdigest()
        assert item.content_size == len(content)
        assert any(path.endswith(item.blob_path) and data == content for path, data in store.blobs.items())
    assert logo.extra_data["is_inline"] is True and logo.extra_data["content_id"] == "image001.png@01DB"
    assert logo.name == "image001.png" and logo.extra_data["content_type"] == "image/png"
    assert report.extra_data["is_inline"] is False and report.name == "Q3 report.pdf"


async def _three_mailboxes(env):
    graphs = [Graph(m) for m in ("alice", "bob", "carol")]
    for mailbox, graph in zip(("alice", "bob", "carol"), graphs):
        _restorable(await env.backup(mailbox, graph), mailbox, env.store)
    return graphs


async def test_same_message_in_three_mailboxes_downloads_attachments_once(env):
    graphs = await _three_mailboxes(env)
    one_copy = len(REPORT) + len(LOGO)
    # Before: 3 list sub-requests carrying 3 copies of the bytes. After: 3 list sub-requests,
    # only the first carries bytes; bob and carol list metadata only.
    assert [g.bytes for g in graphs] == [one_copy, 0, 0]
    assert [g.calls for g in graphs] == [1, 1, 1]
    async with env.factory() as s:
        rows = (await s.execute(text("SELECT external_id, blob_path FROM snapshot_items "
                                     "WHERE item_type = 'EMAIL_ATTACHMENT'"))).all()
    assert len(rows) == 6 and len({r.blob_path for r in rows}) == 2


async def test_kill_switch_restores_per_copy_download(env, monkeypatch):
    monkeypatch.setenv("MAIL_ATTACHMENT_CROSS_MAILBOX_REUSE", "false")
    graphs = await _three_mailboxes(env)
    assert [g.bytes for g in graphs] == [len(REPORT) + len(LOGO)] * 3


async def test_size_mismatch_downloads(env):
    await env.backup("alice", Graph("alice"))
    changed = _graph_attachments("bob")
    changed[0]["size"] += 1
    bob = Graph("bob", changed)
    await env.backup("bob", bob)
    assert bob.bytes == len(REPORT) + len(LOGO)


async def test_name_mismatch_downloads(env):
    await env.backup("alice", Graph("alice"))
    changed = _graph_attachments("bob")
    changed[0]["name"] = "Q3 report (1).pdf"
    bob = Graph("bob", changed)
    await env.backup("bob", bob)
    assert bob.bytes == len(REPORT) + len(LOGO)


@pytest.mark.parametrize("imid", [None, ""])
async def test_missing_internet_message_id_downloads(env, imid):
    await env.backup("alice", Graph("alice"), internet_message_id=imid)
    bob = Graph("bob")
    _restorable(await env.backup("bob", bob, internet_message_id=imid), "bob", env.store)
    assert bob.bytes == len(REPORT) + len(LOGO)


async def test_reuse_never_crosses_tenants(env):
    await env.backup("alice", Graph("alice"), tenant_label="tm")
    other = Graph("bob")
    _restorable(await env.backup("bob", other, tenant_label="other"), "bob", env.store)
    assert other.bytes == len(REPORT) + len(LOGO)


async def test_missing_blob_downloads(env):
    await env.backup("alice", Graph("alice"))
    env.store.blobs.clear()
    bob = Graph("bob")
    _restorable(await env.backup("bob", bob), "bob", env.store)
    assert bob.bytes == len(REPORT) + len(LOGO)


async def test_lookup_failure_downloads_instead_of_failing_the_backup(env, monkeypatch):
    import shared.mail_attachment_reuse as reuse
    await env.backup("alice", Graph("alice"))

    def broken():
        raise ConnectionError("pool exhausted")

    monkeypatch.setattr(reuse, "async_session_factory", broken)
    bob = Graph("bob")
    _restorable(await env.backup("bob", bob), "bob", env.store)
    assert bob.bytes == len(REPORT) + len(LOGO)


async def test_without_the_index_reuse_stays_off(env, monkeypatch):
    """An unindexed JSON filter over a tenant's snapshot_items (87 GB on Railway) costs more than
    the download it saves."""
    import shared.mail_attachment_reuse as reuse
    await env.backup("alice", Graph("alice"))
    async with env.factory.begin() as s:
        await s.execute(text(f"DROP INDEX {reuse.INDEX_NAME}"))
    monkeypatch.setattr(reuse, "_index_ready", None)  # a worker started without the index
    bob = Graph("bob")
    await env.backup("bob", bob)
    assert bob.bytes == len(REPORT) + len(LOGO)
