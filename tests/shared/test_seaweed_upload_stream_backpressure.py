"""Regression test: SeaweedStore.upload_stream must apply back-pressure.

Incident: the part-upload semaphore was acquired inside the per-part
task, so it throttled concurrent upload_part calls but never blocked
task *creation*. A fast source (Graph download) racing a slow backend
(Wasabi throttling PUTs) queued every 8 MB part in worker memory —
growing toward whole-file size and OOM-killing the pod mid-multipart.

The pinned behavior: while the backend is stalled, the producer may
drain at most max_parallel_parts worth of parts (plus the chunk being
assembled) from the source stream — never the whole stream.
"""
from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime

from shared.storage.seaweedfs import SeaweedStore

_CHUNK = 1024
_TOTAL_CHUNKS = 64
_MAX_PARALLEL = 2


class _FakeS3:
    """Minimal S3 stub whose upload_part blocks until released."""

    def __init__(self, gate: asyncio.Event):
        self.gate = gate
        self.parts_started = 0
        self.completed_parts = None
        self.aborted = False

    async def create_multipart_upload(self, **kw):
        return {"UploadId": "mp-1"}

    async def upload_part(self, **kw):
        self.parts_started += 1
        await self.gate.wait()
        return {"ETag": f'"etag-{kw["PartNumber"]}"'}

    async def complete_multipart_upload(self, **kw):
        self.completed_parts = kw["MultipartUpload"]["Parts"]

    async def abort_multipart_upload(self, **kw):
        self.aborted = True

    async def head_object(self, **kw):
        return {
            "ContentLength": _CHUNK * _TOTAL_CHUNKS,
            "ETag": '"final-etag"',
            "LastModified": datetime(2026, 1, 1),
        }


class _Ctx:
    def __init__(self, fake):
        self._fake = fake

    async def __aenter__(self):
        return self._fake

    async def __aexit__(self, *exc):
        return False


def _make_store(fake: _FakeS3) -> SeaweedStore:
    store = SeaweedStore(
        backend_id="b1", name="test", endpoint="http://seaweedfs:8333",
        access_key="ak", secret_key="sk", buckets=["tmvault-shard-0"],
    )
    store._client_ctx = lambda: _Ctx(fake)  # type: ignore[method-assign]
    return store


def test_upload_stream_bounds_inflight_parts_and_completes():
    async def scenario():
        gate = asyncio.Event()
        fake = _FakeS3(gate)
        store = _make_store(fake)

        yielded = {"count": 0}
        payload = b"".join(
            bytes([i % 256]) * _CHUNK for i in range(_TOTAL_CHUNKS)
        )

        async def stream():
            for i in range(_TOTAL_CHUNKS):
                yielded["count"] += 1
                yield payload[i * _CHUNK:(i + 1) * _CHUNK]

        task = asyncio.create_task(store.upload_stream(
            "cont", "path/file.bin", stream(),
            total_size=len(payload),
            chunk_size=_CHUNK,
            max_parallel_parts=_MAX_PARALLEL,
        ))

        # Let the producer run until it blocks on the stalled backend.
        for _ in range(3):
            await asyncio.sleep(0.02)

        # Core invariant: with all upload_part calls stalled, the
        # producer must NOT have drained the source. It may hold
        # max_parallel_parts parts in flight plus one chunk in
        # assembly — nothing more.
        assert not task.done()
        assert yielded["count"] <= _MAX_PARALLEL + 2, (
            f"producer drained {yielded['count']} chunks while backend "
            f"was stalled — in-flight parts are unbounded (OOM regression)"
        )
        assert fake.parts_started <= _MAX_PARALLEL

        # Unblock the backend: the healthy path must still complete.
        gate.set()
        info = await asyncio.wait_for(task, timeout=10)

        assert yielded["count"] == _TOTAL_CHUNKS
        assert not fake.aborted
        assert fake.completed_parts is not None
        assert [p["PartNumber"] for p in fake.completed_parts] == list(
            range(1, _TOTAL_CHUNKS + 1)
        )
        assert info.content_md5 == hashlib.sha256(payload).hexdigest()
        assert info.size == len(payload)

    asyncio.run(scenario())


def test_upload_stream_part_failure_releases_slot_and_aborts():
    """A failing part must release its slot (no producer deadlock) and
    the multipart must be aborted with the error raised, not swallowed."""
    async def scenario():
        gate = asyncio.Event()
        gate.set()
        fake = _FakeS3(gate)

        async def _failing_upload_part(**kw):
            fake.parts_started += 1
            raise RuntimeError("backend rejected part")

        fake.upload_part = _failing_upload_part  # type: ignore[method-assign]
        store = _make_store(fake)

        async def stream():
            for i in range(8):
                yield bytes([i]) * _CHUNK

        try:
            await asyncio.wait_for(store.upload_stream(
                "cont", "path/file.bin", stream(),
                total_size=8 * _CHUNK,
                chunk_size=_CHUNK,
                max_parallel_parts=_MAX_PARALLEL,
            ), timeout=10)
        except RuntimeError as e:
            assert "backend rejected part" in str(e)
        else:
            raise AssertionError("upload_stream swallowed the part failure")

        assert fake.aborted

    asyncio.run(scenario())
