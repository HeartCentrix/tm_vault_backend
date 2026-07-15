"""TDD — presigned_url must refuse to sign an encrypted (TMVE) blob.

A presigned URL is fetched directly by the browser from the object store, so it
bypasses the app's decrypt seam entirely. For a TMVE-framed blob that means
serving raw ciphertext. presigned_url must fail LOUD instead — unless the caller
explicitly opts into a server-side use (allow_encrypted=True), where the copied
bytes stay a valid envelope.
"""
from __future__ import annotations

import pytest

from shared.crypto import envelope
from shared.storage.seaweedfs import SeaweedStore


class _Body:
    def __init__(self, data: bytes):
        self._d = data

    async def read(self, n: int = -1) -> bytes:
        return self._d


class FakeS3:
    def __init__(self, stored: bytes):
        self.stored = stored

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def get_object(self, Bucket, Key, Range=None):
        data = self.stored
        if Range and Range.startswith("bytes="):
            lo, hi = Range.split("=")[1].split("-")
            data = self.stored[int(lo):int(hi) + 1]
        return {"Body": _Body(data)}

    async def generate_presigned_url(self, *a, **kw):
        return "http://public/presigned?sig=abc"


def _store(stored: bytes):
    st = SeaweedStore(
        backend_id="b", name="n", endpoint="http://x", access_key="a", secret_key="s",
        buckets=["bucket"], region="us-east-1", verify_tls=False,
    )
    st._forced_bucket = "bucket"
    st._public_endpoint = st._endpoint.rstrip("/")  # skip host-swap in the test
    st._client_ctx = lambda: FakeS3(stored)
    return st


TMVE_BLOB = envelope.encrypt_blob(b"secret", b"\x01" * 32, b"\x02" * 16)  # real envelope
PLAIN_BLOB = b"just-plaintext-bytes-here"


async def test_refuses_to_sign_encrypted_blob():
    st = _store(TMVE_BLOB)
    with pytest.raises(Exception):
        await st.presigned_url("c", "a.bin")


async def test_allows_encrypted_when_explicitly_server_side():
    st = _store(TMVE_BLOB)
    url = await st.presigned_url("c", "a.bin", allow_encrypted=True)
    assert url.startswith("http")


async def test_signs_plaintext_blob_normally():
    st = _store(PLAIN_BLOB)
    url = await st.presigned_url("c", "a.bin")
    assert url.startswith("http")
