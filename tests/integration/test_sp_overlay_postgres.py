"""SharePoint site overlays: an incremental owns only the list rows that changed; unchanged rows are
inherited from the full base through snapshot_items_effective() instead of copied.

Railway 2026-11-06, a no-change incremental over 19 sites wrote 4,255 SHAREPOINT_FILE + 1,158
SHAREPOINT_FOLDER + 333 SHAREPOINT_LIST + 17 SHAREPOINT_LIST_ITEM rows, every one identical to the
prior snapshot's (TM: 100-200 TiB, millions of rows a day)."""
from __future__ import annotations

import importlib.util
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from shared import models
from shared.database import Base

pytestmark = pytest.mark.integration
_ROOT = Path(__file__).resolve().parents[2]
SITE = "site-1"
PAYLOAD = ("item_type, external_id, parent_external_id, name, folder_path, content_hash, content_size, blob_path, "
           "metadata::jsonb::text, is_deleted")


def _load(name, rel):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, _ROOT / rel)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return sys.modules[name]


@pytest.fixture(autouse=True)
def defaults(monkeypatch):
    for name in ("SHAREPOINT_OVERLAY_ENABLED", "SHAREPOINT_OVERLAY_MAX_AGE_DAYS"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("SHAREPOINT_OVERLAY_MAX_RATIO", "1")   # 8-row test site; the 0.25 default is for real ones


@pytest.fixture
async def site(monkeypatch):
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires explicitly configured disposable PostgreSQL")
    schema = "sp_overlay_" + uuid4().hex
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}, "statement_cache_size": 0})
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_snapshot_items_snap_ext_type "
                                "ON snapshot_items(snapshot_id, external_id, item_type)"))
    factory = async_sessionmaker(engine, expire_on_commit=False)
    worker_module = _load("bw_sp_overlay", "workers/backup-worker/main.py")
    monkeypatch.setattr(worker_module, "async_session_factory", factory)
    tenant, resource, backend = uuid4(), uuid4(), uuid4()
    async with factory.begin() as s:
        org = models.Organization(name="Synthetic", slug=uuid4().hex); s.add(org); await s.flush()
        s.add(models.Tenant(id=tenant, org_id=org.id, display_name="Synthetic"))
        s.add(models.StorageBackend(id=backend, kind="azure", name=uuid4().hex, endpoint="https://storage.invalid",
                                    config={}, secret_ref="synthetic"))
        await s.flush()
        s.add(models.Resource(id=resource, tenant_id=tenant, type=models.ResourceType.SHAREPOINT_SITE,
                              external_id=SITE, display_name="Site"))
    clock = {"t": datetime(2026, 11, 6, 0)}

    async def snapshot():
        sid, jid = uuid4(), uuid4()
        clock["t"] += timedelta(hours=8)
        async with factory.begin() as s:
            s.add(models.Job(id=jid, type=models.JobType.BACKUP, tenant_id=tenant, resource_id=resource,
                             status=models.JobStatus.COMPLETED))
            await s.flush()
            s.add(models.Snapshot(id=sid, resource_id=resource, job_id=jid, backend_id=backend,
                                  status=models.SnapshotStatus.IN_PROGRESS, created_at=clock["t"]))
        return sid

    async def write(sid, rows):
        async with factory.begin() as s:
            for kind, ext, name, folder, md in rows:
                md = dict(md)
                await s.execute(pg_insert(models.SnapshotItem.__table__).values(
                    id=uuid4(), snapshot_id=sid, tenant_id=tenant, backend_id=backend, external_id=ext,
                    item_type=kind, name=name, folder_path=folder, content_size=1,
                    blob_path=("b/" + ext) if kind in ("SHAREPOINT_FILE", "FILE") else None,
                    content_hash=md.pop("hash", None), metadata=md).on_conflict_do_nothing())

    async def settle(sid, status="COMPLETED"):
        async with factory.begin() as s:
            await s.execute(text("UPDATE snapshots SET status = CAST(:st AS snapshotstatus), item_count = 1 WHERE id = :s"),
                            {"s": sid, "st": status})

    async def q(sql, **p):
        async with factory() as s:
            return (await s.execute(text(sql), p)).all()

    async def effective(sid):
        return sorted(await q(f"SELECT {PAYLOAD} FROM snapshot_items_effective(:s) "
                              "WHERE item_type LIKE 'SHAREPOINT%'", s=sid))

    async def own(sid):
        return (await q("SELECT count(*) FROM snapshot_items WHERE snapshot_id = :s AND item_type LIKE 'SHAREPOINT%'",
                        s=sid))[0][0]

    async def removed(sid):
        return (await q("SELECT count(*) FROM snapshot_overlay_removals WHERE snapshot_id = :s", s=sid))[0][0]

    async def base_of(sid):
        return (await q("SELECT overlay_base_id FROM snapshots WHERE id = :s", s=sid))[0][0]

    worker = worker_module.BackupWorker.__new__(worker_module.BackupWorker)
    try:
        yield type("Site", (), dict(locals()))
    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await engine.dispose()


# ── a two-list site and the writes one backup_sharepoint run makes ───────────────────────────────

def list_row(list_id, name, **extra):
    return ("SHAREPOINT_LIST", list_id, name, "Site/lists",
            {"site_id": SITE, "rest_signature": {"count": 3}, "rest_walked_at": "2026-11-05", **extra})


def rest(kind, list_id, n, name, folder, **extra):
    return (kind, f"{list_id}:{n}", name, folder,
            {"source": "sp_rest", "site_id": SITE, "list_id": list_id, "row_version": "1", **extra})


def tasks():
    return [rest("SHAREPOINT_LIST_ITEM", "tasks", i, f"task {i}", "Site/lists/Tasks") for i in (1, 2, 3)]


def assets(folder="Logos", drop=None):
    rows = {0: rest("SHAREPOINT_FOLDER", "assets", 0, folder, "Site/lists/Site Assets"),
            1: rest("SHAREPOINT_FILE", "assets", 1, "logo.png", f"Site/lists/Site Assets/{folder}", hash="h1"),
            2: rest("SHAREPOINT_FILE", "assets", 2, "icon.png", f"Site/lists/Site Assets/{folder}", hash="h2")}
    rows.pop(drop, None)
    return list(rows.values())


LISTS = [list_row("tasks", "Tasks"), list_row("assets", "Site Assets")]


async def run(site, prior, *, reused=(), delta=None, read=None, lists=LISTS, settle=True):
    """One run's DB writes, as backup_sharepoint makes them: every list row; a reused list carried;
    a delta list's changed rows read and the rest carried; a re-read list's rows written whole."""
    sid = await site.snapshot()
    overlay = site.worker_module.sp_overlay_plan()
    await site.write(sid, lists)
    for list_id in reused:
        await site.worker._copy_forward_sp_list_rows(sid, prior, SITE, list_id, overlay=overlay)
    for list_id, rows in (delta or {}).items():
        await site.write(sid, rows)
        await site.worker._copy_forward_sp_list_rows(sid, prior, SITE, list_id, overlay=overlay,
                                                     skip_external_ids=[r[1] for r in rows])
    for rows in (read or {}).values():
        await site.write(sid, rows)
    await site.worker._finish_sp_overlay(sid, overlay)
    if settle:
        await site.settle(sid)
    return sid


async def full(site, **kwargs):
    os.environ["SHAREPOINT_OVERLAY_ENABLED"] = "0"
    try:
        return await run(site, **kwargs)
    finally:
        del os.environ["SHAREPOINT_OVERLAY_ENABLED"]


async def first(site):
    sid = await site.snapshot()
    await site.write(sid, LISTS + tasks() + assets())
    await site.settle(sid)
    return sid


SCENARIOS = {
    "no change": dict(reused=("tasks", "assets")),
    "file edit": dict(reused=("tasks",), delta={"assets": [
        rest("SHAREPOINT_FILE", "assets", 1, "logo.png", "Site/lists/Site Assets/Logos", hash="h1-v2", row_version="2")]}),
    "file delete": dict(reused=("tasks",), read={"assets": assets(drop=2)},
                        lists=[list_row("tasks", "Tasks"), list_row("assets", "Site Assets", rest_walked_at="2026-11-06")]),
    "list item change": dict(reused=("assets",), delta={"tasks": [
        rest("SHAREPOINT_LIST_ITEM", "tasks", 2, "task 2 (done)", "Site/lists/Tasks", row_version="2")]}),
    "folder rename": dict(reused=("tasks",), read={"assets": assets(folder="Brand")}),
    "list removed": dict(reused=("tasks",), lists=[list_row("tasks", "Tasks")]),
}


@pytest.mark.parametrize("scenario", list(SCENARIOS))
async def test_overlay_holds_what_a_full_copy_holds(site, scenario):
    base = await first(site)
    copy = await full(site, prior=base, settle=False, **SCENARIOS[scenario])
    overlay = await run(site, prior=base, settle=False, **SCENARIOS[scenario])
    assert await site.base_of(copy) is None
    assert await site.base_of(overlay) == base
    assert await site.effective(overlay) == await site.effective(copy)


@pytest.mark.parametrize("scenario", list(SCENARIOS))
async def test_overlay_of_an_overlay_holds_what_a_full_copy_holds(site, scenario):
    # Second incremental: the prior is itself an overlay (its own rows are carried, its removals kept).
    base = await first(site)
    prior = await run(site, prior=base, reused=("tasks",), delta={"assets": [
        rest("SHAREPOINT_FILE", "assets", 2, "icon.png", "Site/lists/Site Assets/Logos", hash="h2-v2", row_version="2")]})
    assert await site.base_of(prior) == base and await site.own(prior) == 1
    copy = await full(site, prior=prior, settle=False, **SCENARIOS[scenario])
    overlay = await run(site, prior=prior, settle=False, **SCENARIOS[scenario])
    assert await site.base_of(overlay) == base
    assert await site.effective(overlay) == await site.effective(copy)


async def test_no_change_incremental_writes_no_rows(site):
    base = await first(site)
    sid = await run(site, prior=base, **SCENARIOS["no change"])
    assert (await site.own(sid), await site.removed(sid), await site.base_of(sid)) == (0, 0, base)
    one = await run(site, prior=sid, reused=("tasks",), delta={"assets": [
        rest("SHAREPOINT_FILE", "assets", 1, "logo.png", "Site/lists/Site Assets/Logos", hash="h1-v2", row_version="2")]})
    assert (await site.own(one), await site.removed(one)) == (1, 0)


async def test_over_the_ratio_cap_the_snapshot_is_a_full_copy(site, monkeypatch):
    base = await first(site)
    monkeypatch.setenv("SHAREPOINT_OVERLAY_MAX_RATIO", "0.1")   # 1 changed row of 8 > 0.1 x 8
    sid = await run(site, prior=base, **SCENARIOS["list item change"])
    assert await site.base_of(sid) is None and await site.removed(sid) == 0
    assert await site.own(sid) == 8


async def test_a_base_older_than_the_age_cap_is_compacted(site, monkeypatch):
    base = await first(site)
    monkeypatch.setenv("SHAREPOINT_OVERLAY_MAX_AGE_DAYS", "0.1")   # snapshots are 8 h apart
    sid = await run(site, prior=base, **SCENARIOS["no change"])
    assert await site.base_of(sid) is None and await site.own(sid) == 8


async def test_kill_switch_copies_from_an_overlay_prior(site):
    base = await first(site)
    prior = await run(site, prior=base, **SCENARIOS["list item change"])
    sid = await full(site, prior=prior, **SCENARIOS["no change"])
    assert await site.base_of(sid) is None and await site.own(sid) == 8
    assert await site.effective(sid) == await site.effective(prior)


async def test_previous_rows_and_lists_are_read_through_the_overlay(site):
    base = await first(site)
    sid = await run(site, prior=base, **SCENARIOS["list item change"])
    async with site.factory() as s:
        snap = await s.get(models.Snapshot, await site.snapshot())
    rows = await site.worker._previous_sp_rest_rows(snap, SITE)
    assert sorted(rows) == ["assets:0", "assets:1", "assets:2", "tasks:1", "tasks:2", "tasks:3"]
    assert rows["tasks:2"]["row_version"] == "2"
    prior_id, lists = await site.worker._previous_sp_lists(snap, SITE)
    assert prior_id == sid and sorted(lists) == ["assets", "tasks"]


async def test_site_view_and_restore_read_inherited_rows(site):
    base = await first(site)
    sid = await run(site, prior=base, **SCENARIOS["list item change"])
    api = _load("sp_overlay_snapshot_api", "services/snapshot-service/main.py")
    async with site.factory() as s:
        body = await api.list_snapshot_files(snapshot_id=str(sid), page=1, size=500, search=None, db=s)
    assert sorted(r.name for r in body["content"]) == sorted(
        ["Tasks", "Site Assets", "task 1", "task 2 (done)", "task 3", "Logos", "logo.png", "icon.png"])
    # Restore: a row this overlay owns finds its library's list row, which it inherits from the base.
    sys.path.insert(0, str(_ROOT / "workers" / "restore-worker"))
    restore = _load("restore_worker_sp_overlay", "workers/restore-worker/main.py")
    rw = restore.RestoreWorker.__new__(restore.RestoreWorker)
    async with site.factory.begin() as s:
        await s.execute(text("UPDATE snapshot_items SET metadata = (metadata::jsonb || "
                             "'{\"web_url\": \"https://t.sharepoint.com/sites/a/Lists/Tasks\"}')::json "
                             "WHERE snapshot_id = :b AND external_id = 'tasks'"), {"b": base})
    async with site.factory() as s:
        item = (await s.execute(text("SELECT id FROM snapshot_items WHERE snapshot_id = :s AND external_id = 'tasks:2'"),
                                {"s": sid})).scalar_one()
        item = await s.get(models.SnapshotItem, item)
        assert await rw._sharepoint_library_root(s, item) == "/sites/a/Lists/Tasks"


async def test_retiring_the_base_materialises_the_overlay(site):
    from shared.retention_cleanup import _delete_snapshots
    base = await first(site)
    await site.write(base, [("FILE", "drive-file", "plan.docx", "Site/lists/Documents", {})])
    added = rest("SHAREPOINT_LIST_ITEM", "tasks", 4, "task 4", "Site/lists/Tasks")
    first_overlay = await run(site, prior=base, reused=("assets",), read={"tasks": tasks() + [added]})
    second = await run(site, prior=first_overlay, reused=("assets",), read={"tasks": tasks()})   # task 4 deleted
    before = [await site.effective(first_overlay), await site.effective(second)]
    async with site.factory.begin() as s:
        assert await _delete_snapshots(s, {base}) == 1
    assert [await site.effective(first_overlay), await site.effective(second)] == before
    assert await site.base_of(first_overlay) is None and await site.base_of(second) == first_overlay


async def test_a_retried_run_of_an_overlay_recomputes_it(site):
    base = await first(site)
    sid = await run(site, prior=base, settle=False, **SCENARIOS["file delete"])
    assert await site.removed(sid) == 1
    overlay = site.worker_module.sp_overlay_plan()   # retry: the item came back, both lists are carried
    await site.write(sid, LISTS)
    for list_id in ("tasks", "assets"):
        await site.worker._copy_forward_sp_list_rows(sid, base, SITE, list_id, overlay=overlay)
    await site.worker._finish_sp_overlay(sid, overlay)
    assert await site.base_of(sid) == base and await site.removed(sid) == 0
    assert "assets:2" in {row[1] for row in await site.effective(sid)}
