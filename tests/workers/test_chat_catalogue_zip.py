"""Real ZIP bytes from captured inline and routed encrypted storage objects."""
from __future__ import annotations

import base64
from contextlib import asynccontextmanager
import hashlib
import importlib
import io
import json
import os
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
from uuid import uuid4
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
