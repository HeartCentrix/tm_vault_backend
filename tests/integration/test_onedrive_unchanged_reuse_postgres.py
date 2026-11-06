"""A OneDrive file whose content has not changed since the resource's last backup reuses that
stored copy instead of being downloaded and uploaded again.

Railway 2026-11-04: a requested full backup of 10 users re-downloaded and re-stored all 142 GiB.
The pre-download dedup looked up the file's quickXorHash in content_checksum, which holds the
SHA-256, so it never matched; the quickXorHash lives in the item's metadata."""
from uuid import uuid4

import pytest
from sqlalchemy import update

from shared import models
from test_chat_catalogue_postgres import store

pytestmark = pytest.mark.integration


async def add_file(store, scope, *, external_id="file-1", quickxor="QX1=", size=5000, sha="a" * 64, blob="onedrive/f-1"):
    async with store.factory.begin() as session:
        session.add(models.SnapshotItem(snapshot_id=scope.snapshot_id, tenant_id=store.tenant, external_id=external_id,
            item_type="ONEDRIVE_FILE", name="f.bin", content_size=size, content_checksum=sha, blob_path=blob,
            backend_id=store.backend, extra_data={"quickxor": quickxor, "sha256": sha}))


async def test_unchanged_file_reuses_its_previous_copy(store):
    from shared.blob_dedup import find_unchanged_file_blob
    first = await store.new_scope()
    await add_file(store, first)
    async with store.factory.begin() as session:
        await session.execute(update(models.Snapshot).where(models.Snapshot.id == first.snapshot_id)
                              .values(status=models.SnapshotStatus.COMPLETED))
    current = await store.new_scope()
    async with store.factory() as session:
        found = lambda **kw: find_unchanged_file_blob(session, **{"resource_id": current.resource_id,
            "external_id": "file-1", "quickxor": "QX1=", "size": 5000, "exclude_snapshot_id": current.snapshot_id, **kw})
        assert await found() == ("onedrive/f-1", "a" * 64)
        assert await found(quickxor="QX2=") is None          # content changed
        assert await found(size=5001) is None                # size changed
        assert await found(external_id="file-2") is None     # another file
        assert await found(quickxor="") is None              # Graph gave no hash: download


async def test_copy_from_a_failed_or_other_resource_snapshot_is_not_reused(store):
    from shared.blob_dedup import find_unchanged_file_blob
    failed = await store.new_scope()
    await add_file(store, failed)
    async with store.factory.begin() as session:
        await session.execute(update(models.Snapshot).where(models.Snapshot.id == failed.snapshot_id)
                              .values(status=models.SnapshotStatus.FAILED))
    async with store.factory() as session:
        assert await find_unchanged_file_blob(session, resource_id=failed.resource_id, external_id="file-1",
            quickxor="QX1=", size=5000, exclude_snapshot_id=uuid4()) is None
        assert await find_unchanged_file_blob(session, resource_id=uuid4(), external_id="file-1",
            quickxor="QX1=", size=5000, exclude_snapshot_id=uuid4()) is None


from test_copy_worker_handoff_postgres import bw  # noqa: E402
from test_onedrive_resume_postgres import setup_worker, file_item  # noqa: E402


async def test_full_backup_does_not_download_an_unchanged_file_again(store, bw, monkeypatch):
    from copy import deepcopy
    from sqlalchemy import select
    previous = await store.new_scope()
    await add_file(store, previous, size=1024, sha="sha-previous", blob="previous/file-1")
    async with store.factory.begin() as session:
        await session.execute(update(models.Snapshot).where(models.Snapshot.id == previous.snapshot_id)
                              .values(status=models.SnapshotStatus.COMPLETED))
    env = await setup_worker(store, bw, monkeypatch, [file_item()])
    hashes = {"file-1": "QX1="}

    async def urls(drive_id, ids, *, include_source_metadata=False):
        return {key: (f"https://download.invalid/{key}", 1024, hashes[key], deepcopy(file_item(key)))
                if include_source_metadata else (f"https://download.invalid/{key}", 1024, hashes[key]) for key in ids}

    streamed = []

    async def stream(**kwargs):
        streamed.append(kwargs.get("file_name"))
        return {"success": True, "content_sha256": "sha-new"}

    env.graph.get_download_urls_batch = urls
    env.worker._stream_graph_to_backend = stream
    assert (await env.run())[0] == 1
    assert streamed == []
    async with store.factory() as session:
        row = await session.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == env.scope.snapshot_id))
    assert (row.blob_path, row.content_checksum) == ("previous/file-1", "sha-previous")

    changed = await setup_worker(store, bw, monkeypatch, [file_item()])
    hashes["file-1"] = "QX2="
    changed.graph.get_download_urls_batch = urls
    changed.worker._stream_graph_to_backend = stream
    assert (await changed.run())[0] == 1
    assert streamed == ["file-1"]


async def test_sharepoint_library_row_is_matched_by_its_recorded_graph_hash(store):
    """SharePoint FILE rows keep Graph's hashes under structured.file_hash, not a top-level quickxor."""
    from shared.blob_dedup import find_unchanged_file_blob
    first = await store.new_scope()
    async with store.factory.begin() as session:
        session.add(models.SnapshotItem(snapshot_id=first.snapshot_id, tenant_id=store.tenant, external_id="sp-1",
            item_type="FILE", name="f.docx", content_size=5000, content_checksum="b" * 64, blob_path="files/sp-1",
            backend_id=store.backend, extra_data={"structured": {"file_hash": {"quickXorHash": "QX1="}}}))
        await session.execute(update(models.Snapshot).where(models.Snapshot.id == first.snapshot_id)
                              .values(status=models.SnapshotStatus.COMPLETED))
    current = await store.new_scope()
    async with store.factory() as session:
        assert await find_unchanged_file_blob(session, resource_id=current.resource_id, external_id="sp-1",
            quickxor="QX1=", size=5000, exclude_snapshot_id=current.snapshot_id) == ("files/sp-1", "b" * 64)
        assert await find_unchanged_file_blob(session, resource_id=current.resource_id, external_id="sp-1",
            quickxor="QX2=", size=5000, exclude_snapshot_id=current.snapshot_id) is None


async def test_copy_on_another_storage_backend_is_not_reused(store):
    from shared.blob_dedup import find_unchanged_file_blob
    first = await store.new_scope()
    await add_file(store, first)
    other = uuid4()
    async with store.factory.begin() as session:
        session.add(models.StorageBackend(id=other, kind="azure", name=uuid4().hex,
                    endpoint="https://storage.invalid", config={}, secret_ref="synthetic-reference"))
        await session.flush()
        await session.execute(update(models.Snapshot).where(models.Snapshot.id == first.snapshot_id)
                              .values(status=models.SnapshotStatus.COMPLETED, backend_id=other))
        await session.execute(update(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == first.snapshot_id)
                              .values(backend_id=other))
    current = await store.new_scope()
    async with store.factory() as session:
        assert await find_unchanged_file_blob(session, resource_id=current.resource_id, external_id="file-1",
            quickxor="QX1=", size=5000, exclude_snapshot_id=current.snapshot_id) is None


async def _sp_full_rerun(store, bw, monkeypatch, *, donor_present=True, donor_deleted=False):
    """A FULL re-run of a site (no delta cursor): backup_single_file sees every file again."""
    from types import SimpleNamespace
    first = await store.new_scope()
    async with store.factory.begin() as session:
        session.add(models.SnapshotItem(snapshot_id=first.snapshot_id, tenant_id=store.tenant, external_id="sp-1",
            item_type="FILE", name="f.docx", content_size=5000, content_checksum="b" * 64, blob_path="files/sp-1",
            backend_id=store.backend, is_deleted=donor_deleted,
            extra_data={"structured": {"file_hash": {"quickXorHash": "QX1="}}}))
        await session.execute(update(models.Snapshot).where(models.Snapshot.id == first.snapshot_id)
                              .values(status=models.SnapshotStatus.COMPLETED))
    current = await store.new_scope()
    monkeypatch.setattr(bw, "async_session_factory", store.factory)

    class Shard:
        async def get_blob_properties(self, container, path):
            return {"size": 5000} if donor_present and path == "files/sp-1" else None

    monkeypatch.setattr(bw.azure_storage_manager, "get_shard_for_resource", lambda *a: Shard())
    streamed = []

    async def stream(*a, **k):
        streamed.append(a)
        return {"success": True, "method": "streaming"}

    async def no_versions(*a, **k):
        return None

    created = []

    async def create(*a, **k):
        created.append(a)

    worker = SimpleNamespace(worker_id="t", _stream_file_snapshot=stream, _capture_file_version_history=no_versions,
                             _create_file_snapshot_item=create)
    async with store.factory() as session:
        snapshot = await session.get(models.Snapshot, current.snapshot_id)
        tenant = await session.get(models.Tenant, store.tenant)
    resource = SimpleNamespace(id=current.resource_id, external_id="site")
    item = {"id": "sp-1", "name": "f.docx", "size": 5000, "eTag": "e2", "_drive_id": "drive-1",
            "file": {"hashes": {"quickXorHash": "QX1="}}}
    result = await bw.BackupWorker.backup_single_file(worker, resource, tenant, snapshot, item, None, None)
    return result, streamed, created[0] if created else None


async def test_sharepoint_full_rerun_reuses_an_unchanged_file(store, bw, monkeypatch):
    result, streamed, row = await _sp_full_rerun(store, bw, monkeypatch)
    assert streamed == [] and result["method"] == "reused_unchanged"
    assert row[4:7] == (5000, "files/sp-1", {"sha256": "b" * 64, "quickxor": "QX1="})


async def test_sharepoint_rerun_downloads_when_the_stored_copy_is_gone(store, bw, monkeypatch):
    result, streamed, row = await _sp_full_rerun(store, bw, monkeypatch, donor_present=False)
    assert len(streamed) == 1 and row is None


async def test_sharepoint_rerun_does_not_reuse_a_deleted_row(store, bw, monkeypatch):
    result, streamed, row = await _sp_full_rerun(store, bw, monkeypatch, donor_deleted=True)
    assert len(streamed) == 1 and row is None


from test_copy_worker_handoff_postgres import bw as bw  # noqa: E402,F811


async def test_sharepoint_file_row_records_its_snapshot_backend(store, bw, monkeypatch):
    """SP FILE rows were written with backend_id NULL; restore resolves the store by item.backend_id."""
    from types import SimpleNamespace
    from sqlalchemy import select
    current = await store.new_scope()
    monkeypatch.setattr(bw, "async_session_factory", store.factory)
    async with store.factory() as session:
        snapshot = await session.get(models.Snapshot, current.snapshot_id)
    await bw.BackupWorker._create_file_snapshot_item(
        SimpleNamespace(worker_id="t"), snapshot, SimpleNamespace(id=store.tenant), "sp-9", "f.docx", 10,
        "files/sp-9", {"sha256": "c" * 64}, {"id": "sp-9", "name": "f.docx"}, "drive-1")
    async with store.factory() as session:
        row = await session.scalar(select(models.SnapshotItem).where(
            models.SnapshotItem.snapshot_id == current.snapshot_id))
    assert row.backend_id == store.backend


async def _legacy_null_backend_donor(store, *, snapshot_null):
    """Prod rows predating backend stamping: snapshot_items.backend_id (and old snapshots') NULL."""
    from sqlalchemy import text as _t
    first = await store.new_scope()
    async with store.factory.begin() as session:
        await session.execute(_t("ALTER TABLE snapshot_items ALTER COLUMN backend_id DROP NOT NULL"))
        await session.execute(_t("ALTER TABLE snapshots ALTER COLUMN backend_id DROP NOT NULL"))
        session.add(models.SnapshotItem(snapshot_id=first.snapshot_id, tenant_id=store.tenant, external_id="sp-1",
            item_type="FILE", name="f.docx", content_size=5000, content_checksum="b" * 64, blob_path="files/sp-1",
            backend_id=None, extra_data={"structured": {"file_hash": {"quickXorHash": "QX1="}}}))
        await session.execute(update(models.Snapshot).where(models.Snapshot.id == first.snapshot_id)
                              .values(status=models.SnapshotStatus.COMPLETED,
                                      **({"backend_id": None} if snapshot_null else {})))
    return await store.new_scope()


async def _found(store, current):
    from shared.blob_dedup import find_unchanged_file_blob
    async with store.factory() as session:
        return await find_unchanged_file_blob(session, resource_id=current.resource_id, external_id="sp-1",
            quickxor="QX1=", size=5000, exclude_snapshot_id=current.snapshot_id)


async def test_null_item_backend_donor_uses_its_snapshot_backend(store):
    current = await _legacy_null_backend_donor(store, snapshot_null=False)
    assert await _found(store, current) == ("files/sp-1", "b" * 64)


async def test_null_backend_donor_is_reused_when_only_one_backend_exists(store):
    current = await _legacy_null_backend_donor(store, snapshot_null=True)
    assert await _found(store, current) == ("files/sp-1", "b" * 64)


async def test_null_backend_donor_is_not_reused_when_backends_are_ambiguous(store):
    current = await _legacy_null_backend_donor(store, snapshot_null=True)
    async with store.factory.begin() as session:
        session.add(models.StorageBackend(id=uuid4(), kind="azure", name=uuid4().hex,
                    endpoint="https://storage.invalid", config={}, secret_ref="synthetic-reference"))
    assert await _found(store, current) is None


async def test_onedrive_full_walk_tombstones_files_it_no_longer_sees(store, bw, monkeypatch):
    """A full OneDrive walk (first run, 410 re-bootstrap, force-full) reports no deletions; a file the
    last restore point still shows but the walk did not return was deleted."""
    from types import SimpleNamespace
    from sqlalchemy import select
    first = await store.new_scope()
    await add_file(store, first, external_id="kept")
    await add_file(store, first, external_id="gone", blob="onedrive/gone")
    await add_file(store, first, external_id="old-tomb", blob="onedrive/x")
    async with store.factory.begin() as session:
        await session.execute(update(models.SnapshotItem).where(models.SnapshotItem.external_id == "old-tomb")
                              .values(is_deleted=True))
        await session.execute(update(models.Snapshot).where(models.Snapshot.id == first.snapshot_id)
                              .values(status=models.SnapshotStatus.COMPLETED))
    current = await store.new_scope()
    monkeypatch.setattr(bw, "async_session_factory", store.factory)
    async with store.factory() as session:
        snapshot = await session.get(models.Snapshot, current.snapshot_id)
    worker = SimpleNamespace(worker_id="t")
    worker._insert_snapshot_item_rows = lambda rows: bw.BackupWorker._insert_snapshot_item_rows(worker, rows)
    n = await bw.BackupWorker._tombstone_unseen_onedrive_files(worker, snapshot, store.tenant, {"kept"})
    async with store.factory() as session:
        rows = (await session.scalars(select(models.SnapshotItem).where(
            models.SnapshotItem.snapshot_id == current.snapshot_id))).all()
    assert n == 1
    assert [(r.external_id, r.is_deleted, r.item_type, r.blob_path, r.backend_id) for r in rows] == \
        [("gone", True, "ONEDRIVE_FILE", None, store.backend)]
