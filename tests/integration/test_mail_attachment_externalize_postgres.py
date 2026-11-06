"""A mail carry-forward copies pointer rows, not attachment bytes.

Railway 2026-11-06 incremental #1: attachments <=256 KB lived in snapshot_items.metadata.inline_b64,
so every changed mailbox re-copied them (Rohit 7,085 rows, 247 MB of inline bytes, 245 s of copy).
Before a carry copies its prior snapshot, that prior's inline EMAIL_ATTACHMENT rows move to blob
storage in place, and recovery must keep showing and streaming every one of them."""
from __future__ import annotations

import asyncio
import base64
from datetime import datetime, timedelta
import hashlib
import importlib.util
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

from shared.database import Base
import shared.models as models

pytestmark = pytest.mark.integration
ROOT = Path(__file__).parents[2]

REPORT = b"%PDF-" + b"r" * 3000
LOGO = b"\x89PNG\r\n" + b"l" * 1500
SIG = b"sig-bytes"
CAPTURED = b"captured-as-blob" * 100


class Store:
    """Object store with the shard surface the upload/download helpers use."""

    def __init__(self, backend_id):
        self.backend_id, self.blobs, self.puts, self.fail = backend_id, {}, [], set()

    async def upload_blob(self, container, path, content, overwrite=True, metadata=None, **kwargs):
        self.puts.append(path)
        if hashlib.sha256(content).hexdigest() in self.fail:
            return {"success": False, "error": "synthetic outage"}
        self.blobs[(container, path)] = bytes(content)
        return {"success": True, "blob_path": path}

    async def get_blob_properties(self, container, path):
        data = self.blobs.get((container, path))
        return None if data is None else {"size": len(data)}

    async def download_blob(self, container, path):
        return self.blobs.get((container, path))


@pytest_asyncio.fixture
async def vault(monkeypatch):
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires explicitly configured disposable PostgreSQL")
    schema = "mail_externalize_test_" + uuid4().hex
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}, "statement_cache_size": 0})
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    # upload_blob_with_dedup's tenant content-hash lookup reads through the process factory.
    import shared.database
    monkeypatch.setattr(shared.database, "async_session_factory", factory)
    from shared.config import settings
    monkeypatch.setattr(settings, "RETRY_DELAY_MS", 0)
    # Rows were captured on `backend`; the active store (where uploads land) is `active`.
    tenant, resource, backend, active = uuid4(), uuid4(), uuid4(), uuid4()
    async with factory.begin() as session:
        org = models.Organization(name="Synthetic", slug=uuid4().hex)
        session.add(org)
        await session.flush()
        session.add(models.Tenant(id=tenant, org_id=org.id, display_name="Synthetic"))
        for backend_id in (backend, active):
            session.add(models.StorageBackend(id=backend_id, kind="seaweedfs", name=uuid4().hex,
                        endpoint="https://storage.invalid", config={}, secret_ref="synthetic-reference"))
        await session.flush()
        session.add(models.Resource(id=resource, tenant_id=tenant, type=models.ResourceType.USER_MAIL,
                                    external_id="mailbox-alpha", display_name="Synthetic mailbox"))
    clock = [datetime(2026, 11, 6, 10)]

    async def snapshot(status=models.SnapshotStatus.IN_PROGRESS):
        job, sid = uuid4(), uuid4()
        clock[0] += timedelta(minutes=10)
        async with factory.begin() as session:
            session.add(models.Job(id=job, type=models.JobType.BACKUP, tenant_id=tenant, resource_id=resource,
                                   status=models.JobStatus.RUNNING))
            await session.flush()
            session.add(models.Snapshot(id=sid, resource_id=resource, job_id=job, backend_id=backend, status=status,
                                        started_at=clock[0], created_at=clock[0], item_count=9, bytes_total=4600,
                                        new_item_count=9, bytes_added=4600, extra_data={}))
        return sid, job

    store = Store(active)
    from shared.azure_storage import azure_storage_manager
    container = azure_storage_manager.get_container_name(str(tenant), "email")

    def meta(parent, kind="#microsoft.graph.fileAttachment", content_type="application/pdf", inline=None, **extra):
        data = {"parent_item_id": parent, "attachment_kind": kind, "content_type": content_type,
                "is_inline": False, "content_id": None, "source_url": None, "resolved": True}
        data.update(extra)
        if inline is not None:
            data["inline_b64"] = base64.b64encode(inline).decode()
        return data

    async def mailbox(sid):
        """Two messages: a PDF, a cid inline image (Graph fileAttachment), a MIME-recovered cid image,
        the PDF forwarded, a tiny signature, a capture-time blob attachment and an unresolved link."""
        captured_path = f"{tenant}/{resource}/{sid}/20261106T000000/att_captured"
        store.blobs[(container, captured_path)] = CAPTURED
        rows = {
            "m1": dict(item_type="EMAIL", name="Quarterly", extra_data={"raw": {"id": "m1"}}),
            "m2": dict(item_type="EMAIL", name="Fwd: Quarterly", extra_data={"raw": {"id": "m2"}}),
            "m1::report": dict(name="report.pdf", content_size=len(REPORT), extra_data=meta("m1", inline=REPORT)),
            "m1::logo": dict(name="logo.png", content_size=len(LOGO), extra_data=meta(
                "m1", content_type="image/png", inline=LOGO, is_inline=True, content_id="logo@x")),
            "m1::mime::banner@x": dict(name="inline_banner.png", content_size=len(LOGO), extra_data=meta(
                "m1", kind="mime_inline", content_type="image/png", inline=LOGO, is_inline=True,
                content_id="banner@x")),
            "m2::report": dict(name="report.pdf", content_size=len(REPORT), extra_data=meta("m2", inline=REPORT)),
            "m2::sig": dict(name="sig.txt", content_size=len(SIG), extra_data=meta("m2", content_type="text/plain",
                                                                                    inline=SIG)),
            "m2::captured": dict(name="captured.bin", content_size=len(CAPTURED), blob_path=captured_path,
                                 extra_data=meta("m2", content_type="application/octet-stream", inline_b64=None)),
            "m2::link": dict(name="Plan.docx", content_size=0, extra_data=meta(
                "m2", kind="#microsoft.graph.referenceAttachment", resolved=False, source_url="https://sp/doc")),
        }
        async with factory.begin() as session:
            for external_id, row in rows.items():
                inline = (row["extra_data"].get("inline_b64") or "").encode()
                digest = (hashlib.sha256(base64.b64decode(inline)).hexdigest() if inline else
                          hashlib.sha256(CAPTURED).hexdigest() if row.get("blob_path") else None)
                session.add(models.SnapshotItem(snapshot_id=sid, tenant_id=tenant, backend_id=backend,
                    item_type=row.pop("item_type", "EMAIL_ATTACHMENT"), external_id=external_id,
                    folder_path="Inbox", content_hash=digest, content_checksum=digest,
                    created_at=datetime(2026, 11, 6, 9), **row))

    async def items(sid):
        from sqlalchemy import inspect, select
        async with factory() as session:
            found = (await session.scalars(select(models.SnapshotItem).where(
                models.SnapshotItem.snapshot_id == sid))).all()
            columns = [attr.key for attr in inspect(models.SnapshotItem).column_attrs]
            return {row.external_id: {c: getattr(row, c) for c in columns} | {"metadata": row.extra_data}
                    for row in found}

    async def externalize(target, job, **kwargs):
        from shared.mail_inline_attachments import externalize_prior_mail_attachments
        return await externalize_prior_mail_attachments(
            factory, tenant_id=tenant, resource_id=resource, target_snapshot_id=target, current_job_id=job,
            shard=store, container=container, **kwargs)

    yield SimpleNamespace(factory=factory, snapshot=snapshot, mailbox=mailbox, items=items, store=store,
                          externalize=externalize, container=container, tenant=tenant, resource=resource)
    await engine.dispose()


INLINE = ("m1::logo", "m1::mime::banner@x", "m1::report", "m2::report", "m2::sig")
BYTES = {"m1::report": REPORT, "m2::report": REPORT, "m1::logo": LOGO, "m1::mime::banner@x": LOGO,
         "m2::sig": SIG, "m2::captured": CAPTURED}


async def test_prior_inline_attachments_move_to_blob_in_place(vault):
    prior, _ = await vault.snapshot(models.SnapshotStatus.COMPLETED)
    await vault.mailbox(prior)
    before = await vault.items(prior)
    target, job = await vault.snapshot()

    assert await vault.externalize(target, job) == len(INLINE)

    after = await vault.items(prior)
    for external_id in INLINE:
        old, new = before[external_id], after[external_id]
        digest = hashlib.sha256(BYTES[external_id]).hexdigest()
        # Exactly what a capture-time blob attachment row holds.
        assert new["blob_path"] and vault.store.blobs[(vault.container, new["blob_path"])] == BYTES[external_id]
        assert (new["content_hash"], new["content_checksum"]) == (digest, digest)
        assert new["encryption_key_id"] is None and new["backend_id"] == vault.store.backend_id
        # Every other metadata key survives: resolved, content_id, parent_item_id, attachment_kind...
        assert "inline_b64" not in new["metadata"]
        assert new["metadata"] == {k: v for k, v in old["metadata"].items() if k != "inline_b64"}
    for external_id in ("m1", "m2", "m2::captured", "m2::link"):
        assert after[external_id] == before[external_id]
    # Protected-size identity/size (its UPDATE trigger compares exactly these) and dashboard bytes stay.
    tracked = ("id", "snapshot_id", "external_id", "created_at", "content_size", "is_deleted")
    for external_id in before:
        assert [after[external_id][c] for c in tracked] == [before[external_id][c] for c in tracked]
    async with vault.factory() as session:
        row = await session.get(models.Snapshot, prior)
        assert (row.item_count, row.bytes_total, row.bytes_added) == (9, 4600, 4600)
    # Content-hash dedup: the forwarded PDF and the repeated logo are one object each.
    assert after["m1::report"]["blob_path"] == after["m2::report"]["blob_path"]
    assert after["m1::logo"]["blob_path"] == after["m1::mime::banner@x"]["blob_path"]
    assert len(vault.store.puts) == 3


async def test_externalizing_again_is_a_no_op_and_an_outage_resumes_next_time(vault):
    prior, _ = await vault.snapshot(models.SnapshotStatus.COMPLETED)
    await vault.mailbox(prior)
    target, job = await vault.snapshot()
    vault.store.fail = {hashlib.sha256(LOGO).hexdigest()}
    assert await vault.externalize(target, job) == 3          # both logo rows stay inline, copy still correct
    assert "inline_b64" in (await vault.items(prior))["m1::logo"]["metadata"]
    vault.store.fail = set()
    assert await vault.externalize(target, job) == 2
    puts = len(vault.store.puts)
    assert await vault.externalize(target, job) == 0
    assert len(vault.store.puts) == puts
    after = await vault.items(prior)
    assert all("inline_b64" not in after[e]["metadata"] for e in INLINE)


async def test_storage_outage_stops_after_one_failed_batch(vault):
    prior, _ = await vault.snapshot(models.SnapshotStatus.COMPLETED)
    await vault.mailbox(prior)
    target, job = await vault.snapshot()
    vault.store.fail = {hashlib.sha256(content).hexdigest() for content in (REPORT, LOGO, SIG)}
    assert await vault.externalize(target, job, batch_rows=2) == 0
    # ids sort randomly; the first two-row batch holds at most two distinct contents, three tries each.
    assert len(vault.store.puts) <= 2 * 3


async def test_racing_externalizers_update_each_row_once(vault):
    prior, _ = await vault.snapshot(models.SnapshotStatus.COMPLETED)
    await vault.mailbox(prior)
    target, job = await vault.snapshot()
    counts = await asyncio.gather(vault.externalize(target, job, batch_rows=2),
                                  vault.externalize(target, job, batch_rows=3))
    assert sum(counts) == len(INLINE)
    after = await vault.items(prior)
    assert all("inline_b64" not in after[e]["metadata"] and after[e]["blob_path"] for e in INLINE)


async def test_only_the_carry_prior_is_touched(vault):
    older, _ = await vault.snapshot(models.SnapshotStatus.COMPLETED)
    await vault.mailbox(older)
    prior, _ = await vault.snapshot(models.SnapshotStatus.COMPLETED)
    await vault.mailbox(prior)
    target, job = await vault.snapshot()
    assert await vault.externalize(target, job) == len(INLINE)
    untouched = await vault.items(older)
    assert all("inline_b64" in untouched[e]["metadata"] for e in INLINE)


def _load(name, relative):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, ROOT / relative)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return sys.modules[name]


async def test_recovery_lists_and_streams_the_same_attachments_before_and_after(vault, monkeypatch):
    """The preview chips (resolved + cid) and the bytes behind them, for externalized rows and for a
    row captured straight to blob storage; restore/PST reads the same bytes and Content-ID."""
    api = _load("mail_externalize_snapshot_api", "services/snapshot-service/main.py")
    mail_fetch = _load("mail_externalize_mail_fetch", "workers/restore-worker/mail_fetch.py")
    from shared.azure_storage import azure_storage_manager
    monkeypatch.setattr(api, "azure_storage_manager", SimpleNamespace(
        shards=[vault.store], get_shard_for_resource=lambda *a: vault.store,
        get_container_name=azure_storage_manager.get_container_name))
    monkeypatch.setattr(azure_storage_manager, "get_shard_for_item", lambda item: vault.store)
    prior, _ = await vault.snapshot(models.SnapshotStatus.COMPLETED)
    await vault.mailbox(prior)

    async def recovery():
        rows = await vault.items(prior)
        seen = {}
        async with vault.factory() as db:
            for parent in ("m1", "m2"):
                chips = await api.get_item_attachments(str(prior), str(rows[parent]["id"]), db)
                for chip in chips:
                    body = None
                    if chip["resolved"]:
                        response = await api.get_item_content(str(prior), chip["id"], download=False, db=db)
                        body = response.body
                    seen[chip["name"], parent] = (chip["resolved"], chip["contentId"], chip["isInline"], body)
            for external_id in ("m1::logo", "m2::captured"):
                item = await db.get(models.SnapshotItem, rows[external_id]["id"])
                ref = await mail_fetch._file_attachment_ref(item, item.extra_data, (vault.container,), 10 ** 7)
                seen[external_id] = (ref.data_bytes, ref.content_id, ref.inline)
        return seen

    before = await recovery()
    target, job = await vault.snapshot()
    assert await vault.externalize(target, job) == len(INLINE)
    after = await recovery()
    assert after == before
    assert after["logo.png", "m1"] == (True, "logo@x", True, LOGO)
    assert after["inline_banner.png", "m1"] == (True, "banner@x", True, LOGO)
    assert after["report.pdf", "m2"] == (True, None, False, REPORT)
    assert after["captured.bin", "m2"] == (True, None, False, CAPTURED)
    assert after["Plan.docx", "m2"][0] is False
    assert after["m1::logo"] == (LOGO, "logo@x", True)
    assert after["m2::captured"] == (CAPTURED, None, False)
