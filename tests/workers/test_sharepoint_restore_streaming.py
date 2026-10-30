"""Regression tests for the streaming SharePoint file restore (ITO-10282).

Pins the fix for ``_restore_file_to_sharepoint`` buffering entire files
into RAM and PUTting them via Graph's simple upload: files over 250 MB
exceed the simple-upload cap outright, and buffering a multi-GB body can
OOM-kill the single-replica restore-worker — collapsing every concurrent
restore.

Pinned behavior:
  * blobs larger than SHAREPOINT_RESTORE_STREAMING_THRESHOLD_BYTES are
    streamed from object storage straight into a Graph uploadSession
    (never fully materialised, never simple-PUT);
  * the blob's HEAD size — not the (possibly stale) row content_size —
    decides the regime and is the declared uploadSession total;
  * the stream reads from the SAME container the HEAD probe hit;
  * a probe ERROR (storage blip, backend unreachable) fails the item
    loudly — it must never be mistaken for blob-absent and silently
    reroute a multi-GB file onto the buffered path;
  * files at or below the threshold keep the simple buffered PUT, with
    path segments quoted identically to the streaming helper so both
    size regimes target the same item;
  * a missing blob still raises loudly and uploads nothing;
  * ACL replay runs with the driveItem the upload returns — and when a
    final-chunk resync returns {}, the item is recovered by path.
"""
from __future__ import annotations

import asyncio
import importlib.util
import pathlib
import sys
from types import SimpleNamespace

import pytest

_WORKER_DIR = pathlib.Path(__file__).resolve().parents[2] / "workers" / "restore-worker"
if str(_WORKER_DIR) not in sys.path:
    sys.path.insert(0, str(_WORKER_DIR))

THRESHOLD = 1024  # keep test payloads tiny; patched onto settings below
CHUNK = 320 * 1024
TENANT = "11111111-1111-1111-1111-111111111111"


def _load_main():
    name = "restore_worker_main_for_tests"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, _WORKER_DIR / "main.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class _FakeGraph:
    GRAPH_URL = "https://graph.test/v1.0"

    def __init__(self, stream_result=None):
        self.puts = []             # (url, content)
        self.stream_uploads = []   # dict per upload_large_file_stream_to_drive call
        self.drive_id_lookups = []
        self.item_lookups = []     # (drive_id, drive_path)
        self._stream_result = (
            {"id": "streamed-item"} if stream_result is None else stream_result
        )

    async def _put(self, url, content, headers=None):
        self.puts.append((url, content))
        return {"id": "put-item"}

    async def get_sharepoint_site_default_drive_id(self, site_id):
        self.drive_id_lookups.append(site_id)
        return f"drive-of-{site_id}"

    async def get_drive_item_by_path(self, drive_id, drive_path):
        self.item_lookups.append((drive_id, drive_path))
        return {"id": "looked-up-item"}

    async def upload_large_file_stream_to_drive(
        self, drive_id, drive_path, byte_iter, total_size,
        chunk_size, conflict_behavior="rename",
    ):
        body = bytearray()
        async for piece in byte_iter:
            body.extend(piece)
        self.stream_uploads.append({
            "drive_id": drive_id,
            "drive_path": drive_path,
            "total_size": total_size,
            "chunk_size": chunk_size,
            "conflict_behavior": conflict_behavior,
            "body": bytes(body),
        })
        return self._stream_result


class _FakeShard:
    """Container-aware fake: blobs keyed on (container, blob_path) so a
    probe/stream container mismatch fails the suite instead of passing
    silently (production REST-walked blobs live ONLY in 'sharepoint')."""

    def __init__(self, blobs, head_error=None):
        self.blobs = blobs  # {(container, blob_path): bytes}
        self.head_error = head_error  # Exception to raise on every HEAD
        self.downloads = []
        self.streams = []
        self.heads = []

    async def download_blob(self, container, blob_path):
        self.downloads.append((container, blob_path))
        return self.blobs.get((container, blob_path))

    async def head_blob(self, container, blob_path):
        self.heads.append((container, blob_path))
        if self.head_error is not None:
            raise self.head_error
        data = self.blobs.get((container, blob_path))
        if data is None:
            return None
        return {"name": blob_path, "size": len(data)}

    async def download_blob_stream(self, container, blob_path, chunk_size=4 * 1024 * 1024):
        self.streams.append((container, blob_path, chunk_size))
        data = self.blobs.get((container, blob_path))
        if data is None:
            return
        for i in range(0, len(data), chunk_size):
            yield data[i : i + chunk_size]


def _mk_item(blob_path="tenant/res/snap/ts/item1", content_size=0):
    return SimpleNamespace(
        item_type="SHAREPOINT_FILE",
        external_id="lst:it1",
        name="doc.docx",
        folder_path="/drive/root:/Shared Documents",
        blob_path=blob_path,
        content_size=content_size,
        tenant_id=TENANT,
        extra_data={},
    )


def _containers(main_mod):
    mgr = main_mod.azure_storage_manager
    return (
        mgr.get_container_name(TENANT, "files"),
        mgr.get_container_name(TENANT, "sharepoint"),
    )


def _mk_worker(main_mod, shard, monkeypatch):
    worker = main_mod.RestoreWorker()
    worker._acl_replays = []

    async def _record_perms(graph_client, item, restore_response):
        worker._acl_replays.append(restore_response)

    monkeypatch.setattr(worker, "_replay_file_permissions", _record_perms)
    monkeypatch.setattr(
        main_mod.azure_storage_manager, "get_shard_for_item",
        lambda item: shard,
    )
    monkeypatch.setattr(
        main_mod.settings,
        "SHAREPOINT_RESTORE_STREAMING_THRESHOLD_BYTES", THRESHOLD,
        raising=False,
    )
    monkeypatch.setattr(
        main_mod.settings, "SHAREPOINT_RESTORE_CHUNK_BYTES", CHUNK,
        raising=False,
    )
    return worker


def test_large_blob_streams_via_upload_session(monkeypatch):
    main_mod = _load_main()
    files_c, _ = _containers(main_mod)
    item = _mk_item()
    payload = b"x" * (THRESHOLD + 100)
    shard = _FakeShard({(files_c, item.blob_path): payload})
    worker = _mk_worker(main_mod, shard, monkeypatch)
    graph = _FakeGraph()
    resource = SimpleNamespace(external_id="site-1")

    asyncio.run(worker._restore_file_to_sharepoint(
        graph, resource, item, conflict_mode="OVERWRITE",
    ))

    # Streamed — never simple-PUT, never fully read via download_blob.
    assert graph.puts == []
    assert shard.downloads == []
    assert len(graph.stream_uploads) == 1
    up = graph.stream_uploads[0]
    assert up["drive_id"] == "drive-of-site-1"
    assert up["drive_path"] == "Shared Documents/doc.docx"
    assert up["total_size"] == len(payload)
    assert up["body"] == payload
    assert up["conflict_behavior"] == "replace"
    # ACL replay got the streamed driveItem.
    assert worker._acl_replays == [{"id": "streamed-item"}]


def test_stream_reads_from_the_container_the_probe_hit(monkeypatch):
    """REST-walked SharePoint blobs live only in the 'sharepoint'
    container — the stream must follow the probe, not assume 'files'."""
    main_mod = _load_main()
    _, sp_c = _containers(main_mod)
    item = _mk_item()
    payload = b"s" * (THRESHOLD + 5)
    shard = _FakeShard({(sp_c, item.blob_path): payload})
    worker = _mk_worker(main_mod, shard, monkeypatch)
    graph = _FakeGraph()
    resource = SimpleNamespace(external_id="site-1")

    asyncio.run(worker._restore_file_to_sharepoint(
        graph, resource, item, conflict_mode="OVERWRITE",
    ))

    assert graph.stream_uploads[0]["body"] == payload
    assert shard.streams == [(sp_c, item.blob_path, CHUNK)]


def test_head_size_decides_not_stale_content_size(monkeypatch):
    """content_size lies small — the blob's real size must still route
    the restore onto the streaming path (the OOM this ticket pins)."""
    main_mod = _load_main()
    files_c, _ = _containers(main_mod)
    item = _mk_item(content_size=10)  # stale row metadata
    payload = b"y" * (THRESHOLD * 3)
    shard = _FakeShard({(files_c, item.blob_path): payload})
    worker = _mk_worker(main_mod, shard, monkeypatch)
    graph = _FakeGraph()
    resource = SimpleNamespace(external_id="site-1")

    asyncio.run(worker._restore_file_to_sharepoint(
        graph, resource, item, conflict_mode="OVERWRITE",
    ))

    assert graph.puts == []
    assert len(graph.stream_uploads) == 1
    assert graph.stream_uploads[0]["total_size"] == len(payload)


def test_probe_error_fails_item_never_buffers(monkeypatch):
    """A transient HEAD failure must fail the item loudly (job retry
    handles it) — not silently reroute a multi-GB blob onto the
    fully-buffered fallback the ticket exists to remove."""
    main_mod = _load_main()
    files_c, _ = _containers(main_mod)
    item = _mk_item()
    payload = b"x" * (THRESHOLD + 100)
    shard = _FakeShard(
        {(files_c, item.blob_path): payload},
        head_error=RuntimeError("storage 503"),
    )
    worker = _mk_worker(main_mod, shard, monkeypatch)
    graph = _FakeGraph()
    resource = SimpleNamespace(external_id="site-1")

    with pytest.raises(RuntimeError, match="cannot pick a size regime"):
        asyncio.run(worker._restore_file_to_sharepoint(
            graph, resource, item, conflict_mode="OVERWRITE",
        ))

    assert graph.puts == []
    assert graph.stream_uploads == []
    assert shard.downloads == []  # never fell back to the buffered read


def test_small_blob_keeps_simple_put_with_quoted_path(monkeypatch):
    main_mod = _load_main()
    files_c, _ = _containers(main_mod)
    item = _mk_item()
    payload = b"small document"
    shard = _FakeShard({(files_c, item.blob_path): payload})
    worker = _mk_worker(main_mod, shard, monkeypatch)
    graph = _FakeGraph()
    resource = SimpleNamespace(external_id="site-1")

    asyncio.run(worker._restore_file_to_sharepoint(
        graph, resource, item, conflict_mode="OVERWRITE",
    ))

    assert graph.stream_uploads == []
    assert graph.drive_id_lookups == []
    assert len(graph.puts) == 1
    url, content = graph.puts[0]
    assert content == payload
    # Path segments quoted identically to the streaming helper so both
    # size regimes land on the same item.
    assert url == (
        "https://graph.test/v1.0/sites/site-1/drive/root:"
        "/Shared%20Documents/doc.docx:/content"
    )
    assert worker._acl_replays == [{"id": "put-item"}]


def test_separate_folder_prefix_applies_to_streamed_path(monkeypatch):
    main_mod = _load_main()
    files_c, _ = _containers(main_mod)
    item = _mk_item()
    payload = b"z" * (THRESHOLD + 1)
    shard = _FakeShard({(files_c, item.blob_path): payload})
    worker = _mk_worker(main_mod, shard, monkeypatch)
    graph = _FakeGraph()
    resource = SimpleNamespace(external_id="site-1")

    asyncio.run(worker._restore_file_to_sharepoint(
        graph, resource, item, conflict_mode="SEPARATE_FOLDER",
    ))

    up = graph.stream_uploads[0]
    assert up["drive_path"].startswith("Restored by TM/")
    assert up["drive_path"].endswith("/Shared Documents/doc.docx")


def test_missing_blob_still_raises_and_uploads_nothing(monkeypatch):
    main_mod = _load_main()
    item = _mk_item()
    shard = _FakeShard({})
    worker = _mk_worker(main_mod, shard, monkeypatch)
    graph = _FakeGraph()
    resource = SimpleNamespace(external_id="site-1")

    with pytest.raises(Exception):
        asyncio.run(worker._restore_file_to_sharepoint(
            graph, resource, item, conflict_mode="OVERWRITE",
        ))

    assert graph.puts == []
    assert graph.stream_uploads == []


def test_drive_id_resolved_once_per_site(monkeypatch):
    main_mod = _load_main()
    files_c, _ = _containers(main_mod)
    payload = b"w" * (THRESHOLD + 1)
    item1 = _mk_item(blob_path="t/r/s/ts/item1")
    item2 = _mk_item(blob_path="t/r/s/ts/item2")
    shard = _FakeShard({
        (files_c, item1.blob_path): payload,
        (files_c, item2.blob_path): payload,
    })
    worker = _mk_worker(main_mod, shard, monkeypatch)
    graph = _FakeGraph()
    resource = SimpleNamespace(external_id="site-1")

    asyncio.run(worker._restore_file_to_sharepoint(graph, resource, item1, "OVERWRITE"))
    asyncio.run(worker._restore_file_to_sharepoint(graph, resource, item2, "OVERWRITE"))

    assert graph.drive_id_lookups == ["site-1"]
    assert len(graph.stream_uploads) == 2


def test_target_site_override_streams_to_that_site(monkeypatch):
    main_mod = _load_main()
    files_c, _ = _containers(main_mod)
    item = _mk_item()
    payload = b"v" * (THRESHOLD + 1)
    shard = _FakeShard({(files_c, item.blob_path): payload})
    worker = _mk_worker(main_mod, shard, monkeypatch)
    graph = _FakeGraph()
    resource = SimpleNamespace(external_id="site-src")

    asyncio.run(worker._restore_file_to_sharepoint(
        graph, resource, item, conflict_mode="OVERWRITE",
        target_site_id="site-target",
    ))

    assert graph.drive_id_lookups == ["site-target"]
    assert graph.stream_uploads[0]["drive_id"] == "drive-of-site-target"


def test_empty_stream_result_recovers_drive_item_for_acl_replay(monkeypatch):
    """A final-chunk 416/409 resync (or 202) returns {} from the
    uploadSession even though the file committed — ACL replay must get
    the real driveItem, recovered by path, not be silently skipped."""
    main_mod = _load_main()
    files_c, _ = _containers(main_mod)
    item = _mk_item()
    payload = b"q" * (THRESHOLD + 1)
    shard = _FakeShard({(files_c, item.blob_path): payload})
    worker = _mk_worker(main_mod, shard, monkeypatch)
    graph = _FakeGraph(stream_result={})
    resource = SimpleNamespace(external_id="site-1")

    asyncio.run(worker._restore_file_to_sharepoint(
        graph, resource, item, conflict_mode="OVERWRITE",
    ))

    assert graph.item_lookups == [("drive-of-site-1", "Shared Documents/doc.docx")]
    assert worker._acl_replays == [{"id": "looked-up-item"}]
