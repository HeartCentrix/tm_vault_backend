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
