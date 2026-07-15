"""TDD — SeaweedStore.upload_stream encrypts the multipart (large-file) path.

Drives the real upload_stream through a fake S3 that records the multipart parts,
proving: (1) the stored bytes are a TMVE envelope that decrypts to the original
plaintext, and (2) content_md5 stays the PLAINTEXT sha256 (dedup must key on
plaintext, not on per-blob-salted ciphertext). Also proves writes-off is a
verbatim passthrough.
"""
from __future__ import annotations

import hashlib
from datetime import datetime
from typing import AsyncIterator, Dict, List

import pytest

from shared.crypto import envelope, kek, runtime
from shared.crypto.dek_manager import DekManager, DekRecord, DekStore
from shared.storage.seaweedfs import SeaweedStore

MASTER = b"\x71" * 32


class MemStore(DekStore):
    def __init__(self):
        self.by_kid: Dict[bytes, DekRecord] = {}
        self.active: Dict[str, bytes] = {}

    async def get_active(self, t):
        kid = self.active.get(t)
        return self.by_kid.get(kid) if kid else None

    async def get_by_key_id(self, k):
        return self.by_kid.get(k)

    async def insert(self, rec):
        self.by_kid[rec.key_id] = rec
        self.active[rec.tenant_id] = rec.key_id


class FakeS3:
    def __init__(self, sink: Dict[str, bytearray]):
        self.sink = sink
        self.parts: Dict[str, List] = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def create_multipart_upload(self, Bucket, Key, Metadata=None):
        self.parts[Key] = []
        return {"UploadId": "u1"}

    async def upload_part(self, Bucket, Key, PartNumber, UploadId, Body):
        self.parts[Key].append((PartNumber, bytes(Body)))
        return {"ETag": f'"{PartNumber}"'}

    async def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload):
        ordered = sorted(self.parts[Key], key=lambda x: x[0])
        self.sink[Key] = b"".join(b for _, b in ordered)
        return {}

    async def abort_multipart_upload(self, **kw):
        return {}

    async def head_object(self, Bucket, Key):
        return {"ContentLength": len(self.sink[Key]), "ETag": '"x"', "LastModified": datetime(2026, 1, 1)}


def _store(sink):
    st = SeaweedStore(
        backend_id="b", name="n", endpoint="http://x", access_key="a", secret_key="s",
        buckets=["bucket"], region="us-east-1", verify_tls=False,
    )
    st._forced_bucket = "bucket"
    st._tenant_id = "t1"
    st._client_ctx = lambda: FakeS3(sink)
    return st


async def _feed(data: bytes, size: int) -> AsyncIterator[bytes]:
    for i in range(0, len(data), size):
        yield data[i : i + size]


@pytest.fixture
def mgr():
    m = DekManager(kek.EnvKek(MASTER, kek_id="e1"), MemStore())
    runtime.configure(m, write_enabled=True, verify_readback=False)
    yield m
    runtime.configure(None)


async def test_upload_stream_encrypts_and_keeps_plaintext_hash(mgr):
    sink: Dict[str, bytearray] = {}
    st = _store(sink)
    plaintext = b"O" * 20_000_000  # ~20 MB -> multiple 8 MiB parts + multi framer chunks
    info = await st.upload_stream("c", "big.bin", _feed(plaintext, 1_000_000), len(plaintext))

    stored = sink["c/big.bin"]
    assert envelope.is_encrypted(stored)                       # sealed at rest
    kid, dek = await mgr.active_dek("t1")
    assert envelope.decrypt_blob(stored, dek) == plaintext     # decrypts back
    assert info.content_md5 == hashlib.sha256(plaintext).hexdigest()  # PLAINTEXT hash


async def test_upload_stream_plaintext_when_writes_off():
    runtime.configure(None)  # encryption OFF
    sink: Dict[str, bytearray] = {}
    st = _store(sink)
    plaintext = b"P" * 12_000_000
    info = await st.upload_stream("c", "big.bin", _feed(plaintext, 1_000_000), len(plaintext))
    stored = sink["c/big.bin"]
    assert not envelope.is_encrypted(stored)
    assert stored == plaintext
    assert info.content_md5 == hashlib.sha256(plaintext).hexdigest()
