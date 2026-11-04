"""The Recovery Site view must show a SharePoint site's full library content at every restore point.

Railway 2026-11-04 (9eedfbe): after an incremental backup the latest snapshot held only changed library
files (Graph delta), and /resources/snapshots/{id}/files reads one snapshot -> Donor Desk Documents 2 -> 0,
QFION 26 -> 0. TM's two backed-up sites run incremental every 8 h, so their view was empty the same way,
and their 180 snapshots store library files under Graph's raw '/drives/<id>/root:' path."""
from __future__ import annotations

import importlib.util
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from shared import models
from shared.database import Base

pytestmark = pytest.mark.integration
_ROOT = Path(__file__).resolve().parents[2]
SITE, LIB_URL = "Donor Desk", "https://t.sharepoint.com/sites/DonorDesk/Shared%20Documents"


@pytest.fixture(scope="module")
def api():
    spec = importlib.util.spec_from_file_location("sp_view_snapshot_api", _ROOT / "services/snapshot-service/main.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules["sp_view_snapshot_api"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
async def site():
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires explicitly configured disposable PostgreSQL")
    schema = "sp_view_" + uuid4().hex
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}, "statement_cache_size": 0})
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_snapshot_items_snap_ext_type "
                                "ON snapshot_items(snapshot_id, external_id, item_type)"))
    factory = async_sessionmaker(engine, expire_on_commit=False)
    tenant, resource, backend = uuid4(), uuid4(), uuid4()
    async with factory.begin() as s:
        org = models.Organization(name="Synthetic", slug=uuid4().hex); s.add(org); await s.flush()
        s.add(models.Tenant(id=tenant, org_id=org.id, display_name="Synthetic"))
        s.add(models.StorageBackend(id=backend, kind="azure", name=uuid4().hex, endpoint="https://storage.invalid",
                                    config={}, secret_ref="synthetic"))
        await s.flush()
        s.add(models.Resource(id=resource, tenant_id=tenant, type=models.ResourceType.SHAREPOINT_SITE,
                              external_id="t.sharepoint.com/a/b", display_name=SITE))
    base = datetime(2026, 11, 4, 12)
    state = {"n": 0}

    async def snapshot(items, *, status=models.SnapshotStatus.COMPLETED):
        sid, jid = uuid4(), uuid4(); state["n"] += 1
        async with factory.begin() as s:
            s.add(models.Job(id=jid, type=models.JobType.BACKUP, tenant_id=tenant, resource_id=resource,
                             status=models.JobStatus.COMPLETED))
            await s.flush()
            s.add(models.Snapshot(id=sid, resource_id=resource, job_id=jid, backend_id=backend, status=status,
                                  created_at=base + timedelta(hours=state["n"])))
            await s.flush()
            for kind, ext, name, folder, extra in items:
                s.add(models.SnapshotItem(snapshot_id=sid, tenant_id=tenant, backend_id=backend, external_id=ext, item_type=kind, name=name,
                    folder_path=folder, content_size=1, blob_path=None if extra.get("deleted") else "b/" + ext,
                    is_deleted=bool(extra.get("deleted")), extra_data=extra.get("metadata", {})))
        return sid

    async def view(sid):
        async with factory() as s:
            body = await api_ref["api"].list_snapshot_files(snapshot_id=str(sid), page=1, size=5000, search=None, db=s)
        return {(row.name, row.folderPath) for row in body["content"]}

    api_ref = {"factory": factory, "tenant": tenant}
    yield snapshot, view, api_ref
    await engine.dispose()


def library_list():
    return ("SHAREPOINT_LIST", "list-docs", "Documents", f"{SITE}/lists", {"metadata": {"web_url": LIB_URL, "site_label": SITE}})


def site_asset(ext="sa-1"):
    return ("SHAREPOINT_FILE", ext, "logo.png", f"{SITE}/lists/Site Assets", {})


def doc(ext, name, *, sub="", deleted=False):
    return ("FILE", ext, name, f"{SITE}/lists/Documents{sub}",
            {"deleted": deleted, "metadata": {"structured": {"drive_id": "drive-1", "site_label": SITE, "library": "Documents"}}})


async def test_incremental_restore_point_still_shows_unchanged_library_files(site, api):
    snapshot, view, ref = site; ref["api"] = api
    await snapshot([library_list(), site_asset(), doc("f1", "plan.docx"), doc("f2", "notes.txt", sub="/General")])
    latest = await snapshot([library_list(), site_asset()])              # delta: nothing changed
    assert await view(latest) == {("Documents", f"{SITE}/lists"), ("logo.png", f"{SITE}/lists/Site Assets"),
                                  ("plan.docx", f"{SITE}/lists/Documents"),
                                  ("notes.txt", f"{SITE}/lists/Documents/General")}


async def test_newest_copy_wins_and_deleted_files_disappear(site, api):
    snapshot, view, ref = site; ref["api"] = api
    await snapshot([library_list(), doc("f1", "plan.docx"), doc("f2", "notes.txt")])
    latest = await snapshot([library_list(), doc("f1", "plan-v2.docx"), doc("f2", "notes.txt", deleted=True)])
    assert await view(latest) == {("Documents", f"{SITE}/lists"), ("plan-v2.docx", f"{SITE}/lists/Documents")}


async def test_an_older_restore_point_does_not_show_later_content(site, api):
    snapshot, view, ref = site; ref["api"] = api
    first = await snapshot([library_list(), doc("f1", "plan.docx")])
    await snapshot([library_list(), doc("f9", "later.docx")])
    assert ("later.docx", f"{SITE}/lists/Documents") not in await view(first)


async def test_list_rows_come_only_from_the_restore_point_itself(site, api):
    snapshot, view, ref = site; ref["api"] = api
    await snapshot([library_list(), site_asset("old-asset")])
    latest = await snapshot([library_list()])                             # the REST pass re-reads every list
    assert ("logo.png", f"{SITE}/lists/Site Assets") not in await view(latest)


async def test_legacy_raw_drive_paths_are_shown_under_their_library(site, api):
    snapshot, view, ref = site; ref["api"] = api
    legacy = ("FILE", "f1", "plan.docx", "/drives/b!x/root:/General",
              {"metadata": {"structured": {"drive_id": "b!x", "site_label": SITE,
                                           "web_url": LIB_URL + "/General/plan.docx"}}})
    latest = await snapshot([library_list(), legacy])
    assert ("plan.docx", f"{SITE}/lists/Documents/General") in await view(latest)


async def test_a_full_relisting_hides_files_it_no_longer_finds(site, api, monkeypatch):
    """A library listed without a delta token (first run, reset, Graph resync) reports no deletions: the
    worker tombstones files that earlier restore points show but the listing no longer returns."""
    snapshot, view, ref = site; ref["api"] = api
    spec = importlib.util.spec_from_file_location("sp_view_backup_worker", _ROOT / "workers/backup-worker/main.py")
    worker_module = importlib.util.module_from_spec(spec)
    sys.modules["sp_view_backup_worker"] = worker_module
    spec.loader.exec_module(worker_module)
    monkeypatch.setattr(worker_module, "async_session_factory", ref["factory"])
    await snapshot([library_list(), doc("f1", "plan.docx"), doc("f2", "gone.docx")])
    latest = await snapshot([library_list(), doc("f1", "plan.docx")])
    async with ref["factory"]() as s:
        snap = await s.get(models.Snapshot, latest)
    worker = object.__new__(worker_module.BackupWorker)
    tenant = type("T", (), {"id": ref["tenant"]})()
    assert await worker._tombstone_unseen_library_files(snap, tenant, ["drive-1"], {"drive-1": {"f1"}}) == 1
    assert await view(latest) == {("Documents", f"{SITE}/lists"), ("plan.docx", f"{SITE}/lists/Documents")}
