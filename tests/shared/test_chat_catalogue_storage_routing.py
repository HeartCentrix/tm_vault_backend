"""Catalogue S3 requests stay outside legacy sharding/lifecycle buckets."""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from io import BytesIO

from botocore.exceptions import ClientError
from shared.storage.seaweedfs import SeaweedStore


class S3:
    def __init__(self):
        self.buckets = set()
        self.content = {}
        self.deleted = []

    async def head_bucket(self, Bucket):
        if Bucket not in self.buckets:
            raise ClientError({"Error": {"Code": "404"}}, "HeadBucket")

    async def create_bucket(self, Bucket):
        self.buckets.add(Bucket)

    async def upload_fileobj(self, file, bucket, key, **kwargs):
        self.content[bucket, key] = file.read()

    async def head_object(self, Bucket, Key):
        return {"ContentLength": len(self.content[Bucket, Key]), "ETag": '"etag"', "LastModified": datetime(2026, 1, 1)}

    async def get_object(self, Bucket, Key):
        return {"Body": Body(self.content[Bucket, Key])}

    async def delete_object(self, Bucket, Key):
        self.deleted.append((Bucket, Key))
        self.content.pop((Bucket, Key), None)


class Body(BytesIO):
    async def read(self, n=-1):
        return super().read(n)


def test_catalogue_container_is_created_and_uses_same_exact_route_for_read_and_delete(tmp_path, monkeypatch):
    from shared.crypto import runtime
    monkeypatch.setattr(runtime, "is_write_enabled", lambda: False)
    s3 = S3()
    store = SeaweedStore(backend_id="test-backend", name="test", endpoint="http://storage.invalid",
                         access_key="test", secret_key="test", buckets=["legacy-shard-0", "legacy-shard-1"])

    @asynccontextmanager
    async def client():
        yield s3
    monkeypatch.setattr(store, "_client_ctx", client)
    file = tmp_path / "content"
    file.write_bytes(b"immutable chat bytes")
    container, path = "chat-catalogue-aabbccdd", "chat-catalogue/v1/tenant/generation"

    async def run():
        await store.ensure_container(container)
        await store.upload_from_file(container, path, str(file), file.stat().st_size)
        assert await store.download(container, path) == file.read_bytes()
        assert (container, path) in s3.content
        assert container in s3.buckets
        assert not any(bucket.startswith("legacy-") for bucket, _ in s3.content)
        await store.delete(container, path)
        assert s3.deleted == [(container, path)]
    asyncio.run(run())


def test_legacy_routes_remain_sharded():
    store = SeaweedStore(backend_id="test-backend", name="test", endpoint="http://storage.invalid",
                         access_key="test", secret_key="test", buckets=["legacy-shard-0", "legacy-shard-1"])
    clone = store.shard_for("tenant", "resource")
    assert clone._bucket("backup-teams-tenant") in {"legacy-shard-0", "legacy-shard-1"}
    assert clone._key("backup-teams-tenant", "legacy/path") == "backup-teams-tenant/legacy/path"
