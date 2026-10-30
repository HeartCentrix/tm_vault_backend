"""Real ZIP bytes from captured inline and routed encrypted storage objects."""
from __future__ import annotations

import base64
from contextlib import asynccontextmanager
from datetime import datetime
import hashlib
import importlib
import io
import json
import os
import re
from pathlib import Path
import posixpath
import sys
from types import ModuleType, SimpleNamespace
from uuid import uuid4
from unittest.mock import AsyncMock
import zipfile

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
_ROOT = Path(__file__).resolve().parents[2]
_PACKAGE = "workers.chat_export_worker"
if _PACKAGE not in sys.modules:
    package = ModuleType(_PACKAGE)
    package.__path__ = [str(_ROOT / "workers/chat-export-worker")]
    sys.modules[_PACKAGE] = package

consumer = importlib.import_module(_PACKAGE + ".consumers.thread")
packager = importlib.import_module(_PACKAGE + ".packager.thread_packager")
normalizer = importlib.import_module(_PACKAGE + ".render.normalizer")


def descriptor(data, *, name, backend=None, container=None, path=None):
    return {"catalogue": True, "name": name, "sha256": hashlib.sha256(data).hexdigest(),
        "size": len(data), "backend_id": backend, "container": container, "blob_path": path,
        "inline_b64": base64.b64encode(data).decode() if path is None else None,
        "content_type": "application/octet-stream"}


def message():
    item = SimpleNamespace(id=str(uuid4()), external_id="message-alpha",
        extra_data={"raw": {"body": {"content": "Captured body", "contentType": "text"},
            "createdDateTime": "2026-10-01T10:00:00Z"}})
    return normalizer.normalize_messages([item], attachments_by_msg={}, hosted_by_msg={}, layout="single_thread")


@pytest.mark.asyncio
async def test_zip_preserves_inline_and_decrypts_explicit_backend_bytes(monkeypatch):
    from shared.crypto import blob_cipher, runtime
    from shared.storage.seaweedfs import SeaweedStore
    from shared.storage.router import router

    key, key_id = b"k" * 32, b"i" * 16
    encrypted_bytes = b"encrypted captured attachment" * 100
    plain_bytes = b"different backend attachment"
    calls = []

    class Keys:
        async def dek_for_key_id(self, seen):
            assert seen == key_id
            return key
    monkeypatch.setattr(runtime, "_dek_manager", Keys())

    def storage(backend, raw):
        store = object.__new__(SeaweedStore)
        store._bucket = lambda container: container
        store._key = lambda container, path: path
        class Body:
            async def iter_chunks(self, size):
                for start in range(0, len(raw), 13):
                    yield raw[start:start + 13]
        class Client:
            async def get_object(self, **kw):
                calls.append((backend, kw["Bucket"], kw["Key"]))
                return {"Body": Body()}
        @asynccontextmanager
        async def client():
            yield Client()
        store._client_ctx = client
        return store

    stores = {"backend-encrypted": storage("backend-encrypted", blob_cipher.seal(encrypted_bytes, key, key_id)),
        "backend-plain": storage("backend-plain", plain_bytes)}
    monkeypatch.setattr(router, "get_store_by_id", lambda backend: stores[backend])
    # A catalogue descriptor must never probe the unrelated default shard.
    monkeypatch.setattr(consumer.azure_storage_manager, "get_default_shard",
        lambda: pytest.fail("catalogue export used default shard"))
    inline = b"inline captured attachment"
    attachments = [descriptor(inline, name="inline.bin"),
        descriptor(encrypted_bytes, name="encrypted.bin", backend="backend-encrypted", container="exact-container-a", path="chat-catalogue/a"),
        descriptor(plain_bytes, name="plain.bin", backend="backend-plain", container="exact-container-b", path="chat-catalogue/b")]
    out = io.BytesIO()
    result = await packager.ThreadPackager("single_thread", "Captured chat", "chats/Captured chat", "JSON",
        consumer.BlobAttachmentSource()).write(message(), attachment_map={"message-alpha": attachments},
            hosted_map={}, out=out)
    assert result["attachment_count"] == 3
    with zipfile.ZipFile(io.BytesIO(out.getvalue())) as zipped:
        for name, data in (("inline.bin", inline), ("encrypted.bin", encrypted_bytes), ("plain.bin", plain_bytes)):
            assert zipped.read("Captured chat-attachments/" + name) == data
        manifest = json.loads(zipped.read("_meta/manifest.json"))
        for record in manifest["files"]:
            if record["path"].endswith(".bin"):
                assert hashlib.sha256(zipped.read(record["path"])).hexdigest() == record["sha256"]
        assert not any(name.endswith(".MISSING.txt") for name in zipped.namelist())
    assert calls == [("backend-encrypted", "exact-container-a", "chat-catalogue/a"),
        ("backend-plain", "exact-container-b", "chat-catalogue/b")]


@pytest.mark.asyncio
async def test_catalogue_export_fails_on_wrong_captured_checksum():
    bad = descriptor(b"captured", name="bad.bin")
    bad["inline_b64"] = base64.b64encode(b"different").decode()
    with pytest.raises(Exception, match="checksum|integrity|captured"):
        await packager.ThreadPackager("single_thread", "Chat", "chats/Chat", "JSON",
            consumer.BlobAttachmentSource()).write(message(), attachment_map={"message-alpha": [bad]},
                hosted_map={}, out=io.BytesIO())


@pytest.mark.parametrize("layout", ["single_thread", "per_message"])
@pytest.mark.parametrize("descriptor_kind", ["catalogue-key", "catalogue-fallback", "legacy-fallback"])
async def test_actual_pipeline_namespaces_same_hosted_id_by_message_and_preserves_html_links(monkeypatch, descriptor_kind, layout):
    from shared.chat_catalogue_identity import qualified_chat_item_key
    messages, hosted = [], {}
    contents = {"message-alpha": b"first captured image", "message-beta": b"second captured image"}
    for mid, data in contents.items():
        raw = {"id": mid, "body": {"contentType": "html", "content":
            f'<p>{mid}</p><img src="https://graph.microsoft.com/v1.0/chats/chat-alpha/messages/{mid}/hostedContents/1/$value">'},
            "createdDateTime": "2026-10-01T10:00:00Z"}
        messages.append(SimpleNamespace(id=str(uuid4()), external_id=mid, extra_data={"raw": raw}))
        captured = descriptor(data, name="1.png") | {"hc_id": "1", "ext": ".png", "content_type": "image/png"}
        if descriptor_kind == "catalogue-key":
            captured["archive_key"] = qualified_chat_item_key("chat-alpha", "CHAT_HOSTED_CONTENT", "1", mid)
        elif descriptor_kind == "legacy-fallback":
            captured = {"hc_id": "1", "ext": ".png", "blob_path": mid, "content_type": "image/png"}
        hosted[mid] = [captured]
    scope = SimpleNamespace(layout=layout, thread_path="chats/Chat", messages=messages,
        attachment_map={}, hosted_map=hosted)
    resource = SimpleNamespace(tenant_id=uuid4(), display_name="Synthetic")
    sess = SimpleNamespace(execute=AsyncMock(side_effect=[SimpleNamespace(scalar_one=lambda: resource),
        SimpleNamespace(rowcount=1)]), commit=AsyncMock())
    uploaded = []
    async def upload(_container, _path, filename, **kwargs):
        uploaded.append(Path(filename).read_bytes())
        return {"success": True}
    monkeypatch.setattr(consumer, "_check_cancelled", AsyncMock(return_value=False))
    monkeypatch.setattr(consumer, "publish", AsyncMock())
    monkeypatch.setattr(consumer, "upload_blob_with_retry_from_file", upload)
    monkeypatch.setattr(consumer.azure_storage_manager, "get_default_shard", lambda: object())
    async def legacy_stream(mid):
        async def chunks():
            yield contents[mid]
        return chunks(), "image/png", len(contents[mid])
    monkeypatch.setattr(consumer.azure_storage_manager, "open_stream", legacy_stream, raising=False)
    await consumer._run_pipeline(sess, str(uuid4()), {"resourceId": str(uuid4()), "exportFormat": "HTML"},
        scope, SimpleNamespace(created_at=datetime(2026, 10, 1)))
    assert len(uploaded) == 1
    with zipfile.ZipFile(io.BytesIO(uploaded[0])) as zipped:
        names = zipped.namelist()
        images = [name for name in names if "/inline/" in name]
        assert len(images) == len(set(images)) == 2
        assert len(names) == len(set(names))
        paths = []
        for index, (mid, expected) in enumerate(contents.items()):
            html_path = "Chat.html" if layout == "single_thread" else f"per-message/Chat/{mid}/{mid}.html"
            html = zipped.read(html_path).decode()
            image_paths = re.findall(r'<img[^>]+src="([^\"]+)"', html)
            path = image_paths[index] if layout == "single_thread" else image_paths[0]
            paths.append(path)
            archive_path = posixpath.normpath(posixpath.join(posixpath.dirname(html_path), path))
            assert zipped.read(archive_path) == expected
            if descriptor_kind == "catalogue-key":
                assert hosted[mid][0]["archive_key"] in path
        assert paths[0] != paths[1]
        manifest = json.loads(zipped.read("_meta/manifest.json"))
        for record in manifest["files"]:
            if record["path"] in images:
                assert hashlib.sha256(zipped.read(record["path"])).hexdigest() == record["sha256"]


def test_legacy_hosted_archive_keys_preserve_distinct_raw_ids_after_filename_sanitization():
    ids = ("part/one", "part_one", "part:one", "part\\one")
    keys = [packager._hosted_archive_key("parent", {"hc_id": child}, thread_path="chats/Chat") for child in ids]
    assert len(set(keys)) == len(ids)
    assert all(re.fullmatch("[0-9a-f]{64}", key) for key in keys)
    assert packager._hosted_archive_key("another-parent", {"hc_id": ids[0]}, thread_path="chats/Chat") != keys[0]
    assert packager._hosted_archive_key("parent", {"hc_id": ids[0]}, thread_path="chats/Other") != keys[0]
