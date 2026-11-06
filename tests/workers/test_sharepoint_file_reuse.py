"""A SharePoint library file whose content is unchanged since the resource's last backup
(same drive item, Graph quickXorHash and size) points at that stored copy instead of being
downloaded and stored again. The old "already present" HEAD used this snapshot's blob path,
so it only ever hit on a retry within the same run."""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import uuid
from types import SimpleNamespace

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
_spec = importlib.util.spec_from_file_location("bw_sp_reuse", _REPO_ROOT / "workers" / "backup-worker" / "main.py")
_bw = importlib.util.module_from_spec(_spec)
sys.modules["bw_sp_reuse"] = _bw
try:
    _spec.loader.exec_module(_bw)
except Exception as exc:  # pragma: no cover
    pytest.skip(f"backup-worker module failed to import: {exc}", allow_module_level=True)


def _run(monkeypatch, hit):
    import shared.blob_dedup as dedup
    lookups, created, streamed, versions = [], [], [], []

    async def find(session, **kw):
        lookups.append(kw)
        return hit

    class _Sess:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *e):
            return False

    class _Shard:
        async def get_blob_properties(self, *a):
            return None

    async def create(*a, **k):
        created.append(a)

    async def stream(*a, **k):
        streamed.append(a)
        return {"success": True, "method": "streaming"}

    async def capture(*a, **k):
        versions.append(a)

    monkeypatch.setattr(dedup, "find_unchanged_file_blob", find)
    monkeypatch.setattr(_bw, "async_session_factory", _Sess)
    monkeypatch.setattr(_bw.azure_storage_manager, "get_shard_for_resource", lambda *a: _Shard())
    monkeypatch.setattr(_bw.azure_storage_manager, "get_container_name", lambda *a: "c")
    monkeypatch.setattr(_bw.azure_storage_manager, "build_blob_path", lambda *a: "/".join(a))
    worker = SimpleNamespace(worker_id="t", _create_file_snapshot_item=create, _stream_file_snapshot=stream,
                             _capture_file_version_history=capture)
    resource = SimpleNamespace(id=uuid.uuid4(), external_id="site")
    snapshot = SimpleNamespace(id=uuid.uuid4())
    item = {"id": "f1", "name": "a.docx", "size": 5000, "eTag": "e", "_drive_id": "d1",
            "file": {"hashes": {"quickXorHash": "QX1="}}}
    r = asyncio.run(_bw.BackupWorker.backup_single_file(
        worker, resource, SimpleNamespace(id=uuid.uuid4()), snapshot, item, None, None))
    return r, lookups, created, streamed, versions


def test_unchanged_library_file_reuses_its_previous_copy(monkeypatch):
    r, lookups, created, streamed, versions = _run(monkeypatch, ("prev/f1", "sha-prev"))
    assert streamed == []
    assert lookups[0]["external_id"] == "f1" and lookups[0]["quickxor"] == "QX1=" and lookups[0]["size"] == 5000
    assert r["success"] and r["method"] == "reused_unchanged"
    assert created[0][3:6] == ("a.docx", 5000, "prev/f1")
    assert created[0][6]["sha256"] == "sha-prev"
    assert len(versions) == 1


def test_changed_library_file_is_downloaded(monkeypatch):
    r, lookups, created, streamed, versions = _run(monkeypatch, None)
    assert len(streamed) == 1 and created == []
