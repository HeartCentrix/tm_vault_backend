"""AzureBlobStore — fulfills BackendStore by delegating to the existing
AzureStorageShard / AzureStorageManager code in shared.azure_storage.

Semantics are unchanged from today: SSC, multipart, WORM, lifecycle all
work exactly as they did before the abstraction.
"""
from __future__ import annotations

import hashlib
import os
from datetime import datetime
from typing import AsyncIterator, Optional

from shared.azure_storage import AzureStorageShard
from shared.storage.base import BlobInfo, BlobProps


class AzureBlobStore:
    """Thin adapter over AzureStorageShard/AzureStorageManager."""

    kind = "azure_blob"

    def __init__(
        self,
        backend_id: str,
        name: str,
        shards: list[AzureStorageShard],
    ):
        self.backend_id = backend_id
        self.name = name
        self._shards = shards
        self._default = shards[0] if shards else None

    # --- factories

    @classmethod
    def from_connection_string(
        cls, conn_str: str, backend_id: str, name: str,
    ) -> "AzureBlobStore":
        shard = AzureStorageShard.from_connection_string(conn_str)
        return cls(backend_id=backend_id, name=name, shards=[shard])

    @classmethod
    def from_config(cls, backend_id: str, name: str, config: dict) -> "AzureBlobStore":
        """Build shards from a backend's config row.

        Two formats accepted:
          1. {'shards': [{'account': ..., 'key_ref': 'env://...'}]}
             Uses AzureStorageShard(account, key) and the global
             settings.AZURE_STORAGE_BLOB_ENDPOINT for the suffix.
          2. {'connection_string': 'env://TM_AZ_CONN'}
             Uses AzureStorageShard.from_connection_string — honors the
             full endpoint (useful for Azurite/non-prod endpoints).
        """
        conn_ref = config.get("connection_string")
        if conn_ref:
            if conn_ref.startswith("env://"):
                conn = os.getenv(conn_ref[len("env://"):], "")
            else:
                conn = conn_ref
            if not conn:
                raise ValueError(f"connection_string ref {conn_ref!r} resolved to empty")
            return cls(backend_id=backend_id, name=name,
                       shards=[AzureStorageShard.from_connection_string(conn)])

        shards: list[AzureStorageShard] = []
        for idx, s in enumerate(config.get("shards", [])):
            key_ref: str = s["key_ref"]
            if key_ref.startswith("env://"):
                key = os.getenv(key_ref[len("env://"):], "")
            else:
                raise ValueError(f"Unsupported key_ref scheme: {key_ref}")
            shards.append(AzureStorageShard(
                account_name=s["account"], account_key=key, shard_index=idx,
            ))
        return cls(backend_id=backend_id, name=name, shards=shards)

    # --- sharding

    def shard_for(self, tenant_id: str, resource_id: str) -> "AzureBlobStore":
        if not self._shards:
            raise RuntimeError("no shards configured")
        hash_input = f"{tenant_id}:{resource_id}"
        idx = int(hashlib.md5(hash_input.encode()).hexdigest(), 16) % len(self._shards)
        return AzureBlobStore(
            backend_id=self.backend_id, name=self.name, shards=[self._shards[idx]],
        )

    # --- helpers

    def _default_shard(self) -> AzureStorageShard:
        if not self._default:
            raise RuntimeError("no default shard")
        return self._default

    # --- BackendStore impl

    async def upload(self, container, path, content, metadata=None, overwrite=True, *, tenant_id=None) -> BlobInfo:
        # tenant_id ignored: the Azure backend relies on server-side (CMK)
        # encryption, not the app-layer TMVE envelope used by SeaweedFS.
        shard = self._default_shard()
        result = await shard.upload_blob(container, path, content,
                                         overwrite=overwrite, metadata=metadata)
        if not result.get("success"):
            raise RuntimeError(result.get("error", "upload failed"))
        props = await shard.get_blob_properties(container, path)
        return BlobInfo(
            backend_id=self.backend_id, container=container, path=path,
            size=result.get("size_bytes", len(content)),
            etag=props.get("etag", "") if props else "",
            url=result["blob_url"],
            content_md5=None,
            last_modified=props["last_modified"] if props else datetime.utcnow(),
        )

    async def upload_stream(
        self, container, path,
        byte_stream, total_size: int,
        metadata=None,
        chunk_size: int = 8 * 1024 * 1024,
        max_parallel_parts: int = 4,
    ) -> BlobInfo:
        """Stream-upload via Azure Blob stage_block / commit_block_list —
        the Azure equivalent of S3 multipart. Bounded-memory — never
        spills to /tmp. Mirrors SeaweedStore.upload_stream."""
        import hashlib as _hl
        import base64 as _b64
        import uuid as _uuid
        shard = self._default_shard()
        await shard._ensure_container(container)

        hasher = _hl.sha256()
        total_written = 0
        buf = bytearray()
        block_ids: list[str] = []
        blk_index = 0

        async def _stage(block_id: str, data: bytes):
            await shard.stage_block(container, path, block_id, data)

        async for chunk in byte_stream:
            if not chunk:
                continue
            hasher.update(chunk)
            total_written += len(chunk)
            buf.extend(chunk)
            while len(buf) >= chunk_size:
                blk_index += 1
                # Azure block IDs must be same-length base64 strings.
                bid = _b64.b64encode(
                    f"{blk_index:08d}-{_uuid.uuid4().hex[:8]}".encode(),
                ).decode()
                block_ids.append(bid)
                await _stage(bid, bytes(buf[:chunk_size]))
                del buf[:chunk_size]

        if buf or not block_ids:
            blk_index += 1
            bid = _b64.b64encode(
                f"{blk_index:08d}-{_uuid.uuid4().hex[:8]}".encode(),
            ).decode()
            block_ids.append(bid)
            await _stage(bid, bytes(buf))

        await shard.commit_block_list_manual(
            container, path, block_ids, metadata=metadata,
        )
        props = await shard.get_blob_properties(container, path)
        return BlobInfo(
            backend_id=self.backend_id, container=container, path=path,
            size=total_written,
            etag=props.get("etag", "") if props else "",
            url=f"{path}",
            content_md5=hasher.hexdigest(),
            last_modified=props["last_modified"] if props else datetime.utcnow(),
        )

    async def upload_from_file(self, container, path, file_path, size,
                               metadata=None, overwrite=True) -> BlobInfo:
        shard = self._default_shard()
        result = await shard.upload_blob_from_file(
            container, path, file_path, file_size=size,
            overwrite=overwrite, metadata=metadata,
        )
        if not result.get("success"):
            raise RuntimeError(result.get("error", "upload_from_file failed"))
        props = await shard.get_blob_properties(container, path)
        return BlobInfo(
            backend_id=self.backend_id, container=container, path=path,
            size=size,
            etag=props.get("etag", "") if props else "",
            url=result["blob_url"],
            content_md5=None,
            last_modified=props["last_modified"] if props else datetime.utcnow(),
        )

    async def download(self, container, path) -> Optional[bytes]:
        shard = self._default_shard()
        return await shard.download_blob(container, path)

    async def download_stream(self, container, path, chunk_size=4 * 1024 * 1024):
        shard = self._default_shard()
        async for chunk in shard.download_blob_stream(container, path, chunk_size=chunk_size):
            yield chunk

    async def stage_block(self, container, path, block_id, data) -> None:
        await self._default_shard().stage_block(container, path, block_id, data)

    async def commit_blocks(self, container, path, block_ids, metadata=None) -> None:
        await self._default_shard().commit_block_list_manual(
            container, path, block_ids, metadata=metadata,
        )

    async def put_block_from_url(self, container, path, block_id, source_url) -> None:
        await self._default_shard().put_block_from_url(
            container, path, block_id, source_url,
        )

    async def server_side_copy(self, source_url, container, path, size,
                               metadata=None) -> BlobInfo:
        shard = self._default_shard()
        await shard.copy_from_url_sync(source_url, container, path, size, metadata)
        props = await shard.get_blob_properties(container, path)
        return BlobInfo(
            backend_id=self.backend_id, container=container, path=path,
            size=props["size"] if props else size,
            etag=props.get("etag", "") if props else "",
            url=await shard.get_blob_url(container, path),
            content_md5=None,
            last_modified=props["last_modified"] if props else datetime.utcnow(),
        )

    async def list_blobs(self, container, prefix: Optional[str] = None) -> AsyncIterator[str]:
        async for name in self._default_shard().list_blobs(
            container, name_starts_with=prefix,
        ):
            yield name

    async def list_with_props(self, container, prefix: Optional[str] = None) -> AsyncIterator[tuple[str, BlobProps]]:
        async for name, p in self._default_shard().list_blobs_with_properties(
            container, name_starts_with=prefix,
        ):
            yield name, BlobProps(
                size=p["size"], content_type=None,
                last_modified=p["last_modified"], metadata={},
                copy_status=None, copy_progress=None,
                retention_until=None, legal_hold=False,
            )

    async def get_properties(self, container, path) -> Optional[BlobProps]:
        p = await self._default_shard().get_blob_properties(container, path)
        if not p:
            return None
        return BlobProps(
            size=p["size"], content_type=p.get("content_type"),
            last_modified=p["last_modified"], metadata=p.get("metadata", {}) or {},
            copy_status=p.get("copy_status"), copy_progress=p.get("copy_progress"),
            retention_until=None, legal_hold=False,
        )

    async def delete(self, container, path) -> None:
        await self._default_shard().delete_blob(container, path)

    async def presigned_url(self, container, path, valid_hours=6, *, allow_encrypted=False) -> str:
        # allow_encrypted is a no-op for Azure (its uploads never TMVE-encrypt).
        return await self._default_shard().get_blob_sas_url(container, path, valid_hours)

    async def apply_immutability(self, container, path, until, mode="Unlocked") -> None:
        from shared.azure_storage import apply_blob_immutability
        await apply_blob_immutability(container, path, until, mode=mode, shard=self._default_shard())

    async def apply_legal_hold(self, container, path, tag="tmvault-legal-hold") -> None:
        from shared.azure_storage import apply_legal_hold
        await apply_legal_hold(container, path, tag=tag, shard=self._default_shard())

    async def remove_legal_hold(self, container, path) -> None:
        shard = self._default_shard()
        async_client = shard.get_async_client()
        blob_client = async_client.get_blob_client(container, path)
        await blob_client.set_legal_hold(legal_hold=False)

    async def apply_lifecycle(self, container, hot_days, cool_days, archive_days=None) -> None:
        from shared.azure_storage import apply_lifecycle_policy
        await apply_lifecycle_policy(
            container, hot_days=hot_days, cool_days=cool_days,
            archive_days=archive_days, shard=self._default_shard(),
        )

    async def ensure_container(self, container) -> None:
        await self._default_shard().ensure_container(container)

    async def close(self) -> None:
        for s in self._shards:
            await s.close()
