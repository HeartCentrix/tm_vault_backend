"""Regression tests for SharePoint file restore content sourcing.

Pins the incident where ``_restore_file_to_sharepoint`` took its body
from ``metadata.raw.content`` — a key backup-worker never writes for
SHAREPOINT_FILE (bytes live in object storage at ``item.blob_path``) —
so every restore PUT an empty body. Under conflictMode=OVERWRITE that
replaced live documents with 0-byte files while the job reported full
success.

Pinned behavior:
  * captured bytes are read from the item's blob and PUT verbatim;
  * a missing/unreadable blob raises (the per-item caller counts it as
    failed) and nothing is PUT — never an empty fallback body;
  * a genuinely empty captured file (blob exists, b"") still restores.
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

    def __init__(self):
        self.puts = []  # (url, content)

    async def _put(self, url, content, headers=None):
        self.puts.append((url, content))
        return {"id": "new-item"}


class _FakeShard:
    def __init__(self, blobs):
        self.blobs = blobs  # {blob_path: bytes}

    async def download_blob(self, container, blob_path):
        return self.blobs.get(blob_path)

    async def get_blob_properties(self, container, blob_path):
        if blob_path in self.blobs:
            return {"name": blob_path, "size": len(self.blobs[blob_path])}
        return None


def _mk_item(blob_path="tenant/res/snap/ts/item1"):
    return SimpleNamespace(
        item_type="SHAREPOINT_FILE",
        external_id="lst:it1",
        name="doc.docx",
        folder_path="/drive/root:/Shared Documents",
        blob_path=blob_path,
        tenant_id="11111111-1111-1111-1111-111111111111",
        extra_data={},
    )


def _mk_worker(main_mod, blobs, monkeypatch):
    worker = main_mod.RestoreWorker()

    async def _noop_perms(*a, **k):
        return None

    monkeypatch.setattr(worker, "_replay_file_permissions", _noop_perms)
    monkeypatch.setattr(
        main_mod.azure_storage_manager, "get_shard_for_item",
        lambda item: _FakeShard(blobs),
    )
    return worker


def test_restore_puts_captured_blob_bytes(monkeypatch):
    main_mod = _load_main()
    item = _mk_item()
    worker = _mk_worker(main_mod, {item.blob_path: b"real document bytes"}, monkeypatch)
    graph = _FakeGraph()
    resource = SimpleNamespace(external_id="site-1")

    asyncio.run(worker._restore_file_to_sharepoint(
        graph, resource, item, conflict_mode="OVERWRITE",
    ))

    assert len(graph.puts) == 1
    url, content = graph.puts[0]
    assert content == b"real document bytes"
    assert "site-1" in url and "doc.docx" in url


def test_missing_blob_raises_and_never_puts(monkeypatch):
    main_mod = _load_main()
    item = _mk_item()
    worker = _mk_worker(main_mod, {}, monkeypatch)  # no blobs readable
    graph = _FakeGraph()
    resource = SimpleNamespace(external_id="site-1")

    with pytest.raises(Exception):
        asyncio.run(worker._restore_file_to_sharepoint(
            graph, resource, item, conflict_mode="OVERWRITE",
        ))

    # The failure must be loud AND leave the live document untouched —
    # pre-fix an empty body was PUT over it and counted as restored.
    assert graph.puts == []


def test_empty_captured_file_still_restores(monkeypatch):
    """A genuinely empty source file (blob exists, zero bytes) is a
    valid restore — only *missing* content refuses to upload."""
    main_mod = _load_main()
    item = _mk_item()
    worker = _mk_worker(main_mod, {item.blob_path: b""}, monkeypatch)
    graph = _FakeGraph()
    resource = SimpleNamespace(external_id="site-1")

    asyncio.run(worker._restore_file_to_sharepoint(
        graph, resource, item, conflict_mode="OVERWRITE",
    ))

    assert len(graph.puts) == 1
    assert graph.puts[0][1] == b""
