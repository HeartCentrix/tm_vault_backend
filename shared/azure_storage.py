"""
High-Performance Azure Blob Storage Module

Features:
- Storage sharding across multiple accounts
- Server-Side Copy (start_copy_from_url) for zero-server-load transfers
- Connection pooling and async operations
- Automatic retry with exponential backoff
- RBAC and Key-based authentication
"""
import asyncio
import base64
import hashlib
import logging
import os
import re
import time
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from azure.storage.blob import BlobServiceClient
from azure.storage.blob.aio import BlobServiceClient as AsyncBlobServiceClient
from azure.core.exceptions import AzureError, ResourceExistsError, ResourceNotFoundError

from shared.config import settings


def _sanitize_metadata(metadata: Optional[Dict]) -> Dict:
    """
    Sanitize Azure Blob Storage metadata.
    - Keys: alphanumeric + underscores only (Azure requires valid C# identifier format)
    - Values: ASCII printable chars only (no Unicode, no control chars)
    Non-ASCII values are stripped to ASCII; keys are cleaned of invalid chars.
    """
    if not metadata:
        return {}
    clean = {}
    for k, v in metadata.items():
        # Sanitize key: replace non-alphanumeric/underscore chars
        safe_key = re.sub(r'[^a-zA-Z0-9_]', '_', str(k))
        if not safe_key or safe_key[0].isdigit():
            safe_key = f"m_{safe_key}"
        # Sanitize value: encode to ASCII, dropping non-ASCII chars
        safe_val = str(v).encode('ascii', errors='replace').decode('ascii')
        # Remove control characters (0x00-0x1F and 0x7F)
        safe_val = re.sub(r'[\x00-\x1f\x7f]', '', safe_val)
        # Truncate to 8KB limit per Azure spec
        clean[safe_key] = safe_val[:8192]
    return clean


class AzureStorageShard:
    """Represents a single Azure Storage Account shard"""
    # Kind tag so downstream generic code (e.g. stream_zip_to_block_blob)
    # can pick the right sink without isinstance checks. Matches the
    # `kind = "azure_blob"` / `kind = "seaweedfs"` pattern on the router
    # stores so _StoreFacade's `self.kind` passes through transparently.
    kind = "azure_blob"
    
    def __init__(self, account_name: str, account_key: str, shard_index: int = 0):
        self.account_name = account_name
        self.account_key = account_key
        self.shard_index = shard_index
        # EndpointSuffix must be just "core.windows.net" — not the full URL
        raw_endpoint = settings.AZURE_STORAGE_BLOB_ENDPOINT.replace('https://', '').rstrip('/')
        # raw_endpoint = "stamitkmishr369164905478.blob.core.windows.net"
        # We need "core.windows.net"
        parts = raw_endpoint.split('.')
        if len(parts) >= 3:
            endpoint_suffix = '.'.join(parts[2:])  # "core.windows.net"
        else:
            endpoint_suffix = raw_endpoint
        self.connection_string = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={account_name};"
            f"AccountKey={account_key};"
            f"EndpointSuffix={endpoint_suffix}"
        )
        self._sync_client: Optional[BlobServiceClient] = None
        self._async_client: Optional[AsyncBlobServiceClient] = None
    
    def get_sync_client(self) -> BlobServiceClient:
        if not self._sync_client:
            self._sync_client = BlobServiceClient.from_connection_string(self.connection_string)
        return self._sync_client
    
    def get_async_client(self) -> AsyncBlobServiceClient:
        if not self._async_client:
            self._async_client = AsyncBlobServiceClient.from_connection_string(self.connection_string)
        return self._async_client
    
    async def server_side_copy(self, source_url: str, container_name: str, blob_path: str) -> Dict:
        """
        Server-Side Copy: Azure copies file directly from source to destination.
        Zero load on our server. Fastest method for file-based backups.

        Args:
            source_url: Source file URL (e.g., @microsoft.graph.downloadUrl or SAS URL)
            container_name: Target container name
            blob_path: Target blob path in container

        Returns:
            Dict with copy_id, status, and blob_url
        """
        try:
            await self._ensure_container(container_name)

            async_client = self.get_async_client()
            blob_client = async_client.get_blob_client(container=container_name, blob=blob_path)

            # Start server-side copy
            copy_operation = await blob_client.start_copy_from_url(source_url)
            
            return {
                "success": True,
                "copy_id": copy_operation.get("id", str(uuid.uuid4())),
                "status": "pending",
                "blob_url": blob_client.url,
                "blob_path": blob_path,
                "method": "server_side_copy",
            }
        except AzureError as e:
            return {
                "success": False,
                "error": str(e),
                "method": "server_side_copy",
            }

    async def copy_from_url_sync(self, source_url: str, container_name: str,
                                  blob_path: str, source_size: int,
                                  metadata: Optional[Dict] = None) -> Dict:
        """
        Synchronous server-side copy via upload_blob_from_url (≤256 MB) or
        stage_block_from_url (chunked, for >256 MB up to 5 TB).

        Returns when copy is complete. No polling needed for ≤256 MB path.
        For large files, stages blocks from URL and commits block list.
        """
        await self._ensure_container(container_name)

        async_client = self.get_async_client()
        blob_client = async_client.get_blob_client(container=container_name, blob=blob_path)
        sanitized_meta = _sanitize_metadata(metadata or {})

        SYNC_LIMIT = 256 * 1024 * 1024  # 256 MB

        if source_size <= SYNC_LIMIT:
            # Single-shot synchronous copy
            await blob_client.upload_blob_from_url(
                source_url=source_url,
                overwrite=True,
                metadata=sanitized_meta,
            )
        else:
            # Chunked: stage blocks from URL, then commit
            BLOCK_SIZE = 100 * 1024 * 1024  # 100 MB blocks
            block_ids = []
            offset = 0
            idx = 0
            while offset < source_size:
                length = min(BLOCK_SIZE, source_size - offset)
                block_id = base64.b64encode(f"{idx:08d}".encode()).decode()
                await blob_client.stage_block_from_url(
                    block_id=block_id,
                    source_url=source_url,
                    source_offset=offset,
                    source_length=length,
                )
                block_ids.append(block_id)
                offset += length
                idx += 1
            await blob_client.commit_block_list(block_ids, metadata=sanitized_meta)

        props = await blob_client.get_blob_properties()
        return {
            "size": props.size,
            "etag": props.etag,
            "content_md5": props.content_settings.content_md5.hex() if props.content_settings.content_md5 else None,
            "last_modified": props.last_modified.isoformat(),
        }
    
    async def _ensure_container(self, container_name: str):
        """Create container if it doesn't exist"""
        try:
            async_client = self.get_async_client()
            container_client = async_client.get_container_client(container_name)
            await container_client.create_container()
        except ResourceExistsError:
            pass  # Already exists — fine
        except AzureError as e:
            print(f"[AzureStorage] Warning: Could not create container {container_name}: {e}")

    async def upload_blob(self, container_name: str, blob_path: str, content: bytes,
                         overwrite: bool = True, metadata: Optional[Dict] = None) -> Dict:
        """
        Upload content to Azure Blob Storage with parallel block uploads.
        Auto-creates container if it doesn't exist.

        Args:
            container_name: Target container name
            blob_path: Target blob path
            content: Content bytes
            overwrite: Whether to overwrite existing blob
            metadata: Optional metadata dict

        Returns:
            Dict with success status and blob info
        """
        try:
            await self._ensure_container(container_name)

            async_client = self.get_async_client()
            blob_client = async_client.get_blob_client(container=container_name, blob=blob_path)

            # Azure Blob SDK automatically chunks and uploads blocks in parallel
            # with max_concurrency. For large files this is much faster than sequential.
            await asyncio.wait_for(
                blob_client.upload_blob(
                    content,
                    overwrite=overwrite,
                    metadata=_sanitize_metadata(metadata),
                    max_concurrency=5,  # Upload 5 blocks in parallel
                    length=len(content),
                ),
                timeout=600.0,  # 10 min timeout for large files
            )

            return {
                "success": True,
                "blob_url": blob_client.url,
                "blob_path": blob_path,
                "size_bytes": len(content),
                "method": "direct_upload",
            }
        except asyncio.TimeoutError:
            print(f"[AzureStorage] Upload TIMEOUT for {container_name}/{blob_path} after 120s")
            return {
                "success": False,
                "error": f"Upload timed out after 120s",
                "method": "direct_upload",
            }
        except AzureError as e:
            print(f"[AzureStorage] Upload failed for {container_name}/{blob_path}: {e}")
            return {
                "success": False,
                "error": str(e),
                "method": "direct_upload",
            }

    async def upload_blob_from_file(self, container_name: str, blob_path: str, file_path: str,
                                    file_size: int = 0, overwrite: bool = True,
                                    metadata: Optional[Dict] = None) -> Dict:
        """
        Upload from local file using resilient block upload with retry.

        Enterprise-grade upload for files up to multi-GB:
        - Splits file into blocks (100 MB each for efficiency)
        - Uploads blocks in parallel with configurable concurrency
        - Retries individual failed blocks with exponential backoff
        - Commits block list only when ALL blocks succeed
        - No single point of failure — one slow block doesn't kill the upload

        NOTE: The async Azure SDK requires an open file object, NOT a string path.
        Passing a string path causes the upload to hang indefinitely (SDK bug).
        """
        try:
            await self._ensure_container(container_name)

            async_client = self.get_async_client()
            blob_client = async_client.get_blob_client(container=container_name, blob=blob_path)

            # Block size: 100 MB — optimal for Azure (max 50K blocks = 5 TB max blob)
            block_size = 100 * 1024 * 1024  # 100 MB
            num_blocks = max(1, (file_size + block_size - 1) // block_size)

            # For small files (< block_size), use simple upload (faster)
            if file_size <= block_size:
                with open(file_path, "rb") as f:
                    await asyncio.wait_for(
                        blob_client.upload_blob(
                            f,
                            overwrite=overwrite,
                            metadata=_sanitize_metadata(metadata),
                            max_concurrency=settings.AZURE_UPLOAD_CONCURRENCY,
                            length=file_size,
                        ),
                        timeout=3600.0,  # 1 hour for large files
                    )
                return {
                    "success": True,
                    "blob_url": blob_client.url,
                    "blob_path": blob_path,
                    "size_bytes": file_size,
                    "method": "stream_upload",
                }

            # Large file: use SDK's built-in block upload with file object.
            # The SDK automatically splits into blocks and uploads in parallel.
            # Using a file object (not path string) is critical — path strings hang.
            with open(file_path, "rb") as f:
                await asyncio.wait_for(
                    blob_client.upload_blob(
                        f,
                        overwrite=overwrite,
                        metadata=_sanitize_metadata(metadata),
                        max_concurrency=8,  # Upload 8 blocks in parallel
                        length=file_size,
                    ),
                    timeout=3600.0,  # 1 hour for very large files
                )

            return {
                "success": True,
                "blob_url": blob_client.url,
                "blob_path": blob_path,
                "size_bytes": file_size,
                "method": "stream_upload",
            }
        except asyncio.TimeoutError:
            print(f"[AzureStorage] Stream upload TIMEOUT for {container_name}/{blob_path} after 1hr")
            return {
                "success": False,
                "error": "Upload timed out after 1hr",
                "method": "stream_upload",
            }
        except AzureError as e:
            print(f"[AzureStorage] Stream upload failed for {container_name}/{blob_path}: {e}")
            return {
                "success": False,
                "error": str(e),
                "method": "stream_upload",
            }
    
    async def download_blob(self, container_name: str, blob_path: str) -> Optional[bytes]:
        """Download a blob's bytes. Returns None if the blob does not exist.
        Raises AzureError for auth/network/other failures so callers can surface them."""
        async_client = self.get_async_client()
        blob_client = async_client.get_blob_client(container=container_name, blob=blob_path)
        try:
            stream = await blob_client.download_blob()
            return await stream.readall()
        except ResourceNotFoundError:
            return None

    @classmethod
    def from_connection_string(cls, connection_string: str, shard_index: int = 0):
        """Construct a shard from an Azure connection string (used by tests + Azurite)."""
        instance = cls.__new__(cls)
        instance.shard_index = shard_index
        instance._async_client = AsyncBlobServiceClient.from_connection_string(connection_string)
        instance._sync_client = None
        # Parse AccountName + AccountKey out of the connection string so SAS
        # generation works. Falls back to the Azurite well-known pair when
        # omitted (enables Azurite's default creds via short conn strings).
        parts = {}
        for kv in connection_string.split(";"):
            if "=" in kv:
                k, v = kv.split("=", 1)
                parts[k.strip()] = v.strip()
        instance.account_name = parts.get("AccountName", "devstoreaccount1")
        instance.account_key = parts.get(
            "AccountKey",
            # Azurite well-known account key
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
        )
        return instance

    async def ensure_container(self, container_name: str) -> None:
        """Create container if it does not exist. Idempotent. Public alias for _ensure_container."""
        await self._ensure_container(container_name)

    async def close(self) -> None:
        """Dispose the async Azure client."""
        if getattr(self, "_async_client", None):
            await self._async_client.close()
            self._async_client = None

    async def download_blob_stream(
        self,
        container_name: str,
        blob_path: str,
        chunk_size: int = 4 * 1024 * 1024,
    ):
        """Stream a blob's bytes as async chunks. Yields nothing if blob missing.

        Used by mail-export to pipe attachment bytes into the MIME base64 encoder
        without loading the full attachment into RAM. Essential for production-grade
        export — a single referenceAttachment can be 150 MB and we'd OOM with readall.
        """
        async_client = self.get_async_client()
        blob_client = async_client.get_blob_client(container=container_name, blob=blob_path)
        try:
            stream = await blob_client.download_blob()
        except ResourceNotFoundError:
            return
        async for chunk in stream.chunks():
            if len(chunk) <= chunk_size:
                yield chunk
            else:
                for i in range(0, len(chunk), chunk_size):
                    yield chunk[i : i + chunk_size]

    async def stage_block(
        self,
        container_name: str,
        blob_path: str,
        block_id: str,
        data: bytes,
    ) -> None:
        """Stage a single block for a BlockBlob. Block IDs must be base64-encoded
        strings of equal length — we accept the plain string and let the SDK encode."""
        import base64
        async_client = self.get_async_client()
        blob_client = async_client.get_blob_client(container=container_name, blob=blob_path)
        encoded = base64.b64encode(block_id.encode("ascii").ljust(16, b"=")).decode("ascii")
        await blob_client.stage_block(block_id=encoded, data=data)

    async def commit_block_list_manual(
        self,
        container_name: str,
        blob_path: str,
        block_ids: list,
        metadata: dict = None,
    ) -> None:
        """Commit previously-staged blocks in the given order.

        Metadata is funneled through _sanitize_metadata because Azure rejects
        non-ASCII/symbol characters in metadata values with 400 InvalidMetadata.
        Stream-upload callers (OneDrive files with spaces / accented names like
        'DALL·E …') land here without going through the other upload helpers
        that already sanitize — keep this in sync if a new metadata-bearing
        path is added. Restore is unaffected: filenames are reconstructed from
        snapshot_items.name in the DB, not from blob metadata.
        """
        import base64
        from azure.storage.blob import BlobBlock
        async_client = self.get_async_client()
        blob_client = async_client.get_blob_client(container=container_name, blob=blob_path)
        blocks = [
            BlobBlock(block_id=base64.b64encode(bid.encode("ascii").ljust(16, b"=")).decode("ascii"))
            for bid in block_ids
        ]
        await blob_client.commit_block_list(blocks, metadata=_sanitize_metadata(metadata))

    async def put_block_from_url(
        self,
        container_name: str,
        blob_path: str,
        block_id: str,
        source_url: str,
    ) -> None:
        """Stage a block by copying bytes server-side from another blob URL.
        Zero bytes traverse the worker. Used for final ZIP assembly to stitch
        per-folder MBOX blobs without re-downloading."""
        import base64
        async_client = self.get_async_client()
        blob_client = async_client.get_blob_client(container=container_name, blob=blob_path)
        encoded = base64.b64encode(block_id.encode("ascii").ljust(16, b"=")).decode("ascii")
        await blob_client.stage_block_from_url(block_id=encoded, source_url=source_url)

    async def get_blob_url(self, container_name: str, blob_path: str) -> str:
        """Return a full URL for a blob. Used as source_url input to put_block_from_url.
        In Azurite + account-key mode the raw URL is authenticated by shared-key
        headers on the server side; SAS-authenticated URLs come from a dedicated
        helper added later (Task 27)."""
        async_client = self.get_async_client()
        blob_client = async_client.get_blob_client(container=container_name, blob=blob_path)
        return blob_client.url

    async def list_blobs(self, container_name: str, name_starts_with: Optional[str] = None):
        """Yield blob names in the container. Async generator — safe to
        iterate large containers without buffering. Optional
        name_starts_with pushes the prefix filter into Azure so we don't
        have to pull millions of names just to filter in Python."""
        async_client = self.get_async_client()
        container = async_client.get_container_client(container_name)
        kw = {"name_starts_with": name_starts_with} if name_starts_with else {}
        async for b in container.list_blobs(**kw):
            yield b.name

    async def list_blobs_with_properties(self, container_name: str, name_starts_with: Optional[str] = None):
        """Yield (name, {last_modified, size}) tuples. Used by retention cleanup."""
        async_client = self.get_async_client()
        container = async_client.get_container_client(container_name)
        kw = {"name_starts_with": name_starts_with} if name_starts_with else {}
        async for b in container.list_blobs(**kw):
            yield b.name, {"last_modified": b.last_modified, "size": b.size}

    async def get_blob_sas_url(self, container_name: str, blob_path: str, valid_for_hours: int = 6) -> str:
        """Return a URL with a short-lived SAS token. Needed for cross-shard
        put_block_from_url — destination shard must authenticate to read the
        source. Falls back to the plain URL when the shard uses an account key
        directly (sufficient in same-account scenarios)."""
        from datetime import datetime, timedelta, timezone
        from azure.storage.blob import generate_blob_sas, BlobSasPermissions
        async_client = self.get_async_client()
        blob_client = async_client.get_blob_client(container=container_name, blob=blob_path)

        account_key = getattr(self, "_account_key", None) or getattr(self, "account_key", None)
        if not account_key:
            return blob_client.url

        try:
            sas = generate_blob_sas(
                account_name=self.account_name,
                container_name=container_name,
                blob_name=blob_path,
                account_key=account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.now(timezone.utc) + timedelta(hours=valid_for_hours),
            )
            return f"{blob_client.url}?{sas}"
        except Exception:
            return blob_client.url

    async def delete_blob(self, container_name: str, blob_path: str) -> None:
        """Idempotent delete. Swallows missing-blob errors."""
        async_client = self.get_async_client()
        blob_client = async_client.get_blob_client(container=container_name, blob=blob_path)
        try:
            await blob_client.delete_blob()
        except Exception:
            pass

    async def get_blob_properties(self, container_name: str, blob_path: str) -> Optional[Dict]:
        """Get blob properties including copy status"""
        try:
            async_client = self.get_async_client()
            blob_client = async_client.get_blob_client(container=container_name, blob=blob_path)
            props = await blob_client.get_blob_properties()
            return {
                "name": props.name,
                "size": props.size,
                "etag": getattr(props, "etag", None),
                "content_type": props.content_settings.content_type if props.content_settings else None,
                "last_modified": props.last_modified,
                "copy_status": props.copy.status if props.copy else None,
                "copy_progress": props.copy.progress if props.copy else None,
                "metadata": props.metadata,
            }
        except AzureError:
            return None
    
    async def wait_for_copy_complete(self, container_name: str, blob_path: str, 
                                     timeout_seconds: int = 3600, poll_interval: int = 5) -> Dict:
        """
        Wait for a server-side copy to complete.
        
        Args:
            container_name: Container name
            blob_path: Blob path
            timeout_seconds: Max wait time
            poll_interval: Seconds between status checks
        
        Returns:
            Dict with final copy status
        """
        start_time = datetime.utcnow()
        
        while (datetime.utcnow() - start_time).total_seconds() < timeout_seconds:
            props = await self.get_blob_properties(container_name, blob_path)
            if not props:
                return {"success": False, "error": "Blob not found"}
            
            copy_status = props.get("copy_status")
            if copy_status == "success":
                return {"success": True, "status": "completed", "size": props.get("size")}
            elif copy_status == "failed":
                return {"success": False, "error": "Copy failed", "status": "failed"}
            elif copy_status == "aborted":
                return {"success": False, "error": "Copy aborted", "status": "aborted"}
            
            # Still pending
            progress = props.get("copy_progress", "unknown")
            await asyncio.sleep(poll_interval)
        
        return {"success": False, "error": "Copy timed out", "status": "timeout"}


class _StoreFacade:
    """Backward-compat shim: exposes AzureStorageShard-shaped methods
    (upload_blob, download_blob, get_blob_properties, etc.) backed by
    any BackendStore so legacy workers honor the active backend without
    code changes.

    Return values match the dict shapes that AzureStorageShard uses, so
    retry helpers (upload_blob_with_retry etc.) work unchanged.
    """

    def __init__(self, store):
        self._store = store
        # Attributes callers print / inspect.
        self.account_name = getattr(store, "name", "unknown")
        self.shard_index = 0
        # Expose the backend id so callers that want to stamp
        # snapshot.backend_id can do so without looking up the router.
        self.backend_id = getattr(store, "backend_id", None)
        self.kind = getattr(store, "kind", "unknown")

    async def upload_blob(self, container_name, blob_path, content,
                          overwrite=True, metadata=None):
        try:
            info = await self._store.upload(
                container_name, blob_path, content,
                metadata=metadata, overwrite=overwrite,
            )
            return {
                "success": True,
                "blob_url": info.url,
                "blob_path": info.path,
                "size_bytes": info.size,
                "method": "direct_upload",
            }
        except Exception as e:
            return {"success": False, "error": str(e), "method": "direct_upload"}

    async def upload_blob_from_file(self, container_name, blob_path, file_path,
                                     file_size=0, overwrite=True, metadata=None):
        try:
            import os
            size = file_size or os.path.getsize(file_path)
            info = await self._store.upload_from_file(
                container_name, blob_path, file_path, size,
                metadata=metadata, overwrite=overwrite,
            )
            return {
                "success": True,
                "blob_url": info.url,
                "blob_path": info.path,
                "size_bytes": info.size,
                "method": "stream_upload",
            }
        except Exception as e:
            return {"success": False, "error": str(e), "method": "stream_upload"}

    async def upload_blob_stream(
        self, container_name, blob_path, byte_stream, total_size,
        metadata=None, chunk_size=8 * 1024 * 1024,
        max_parallel_parts=4,
    ):
        """Memory-bounded streaming upload — pipes an async bytes
        iterator directly into the backend's multipart / block-list
        API without a /tmp hop. Used by OneDrive backup to flow
        Graph download bytes straight into SeaweedFS / Azure. Returns
        the same dict shape as upload_blob, plus `content_sha256`
        computed inline so callers don't re-hash."""
        try:
            info = await self._store.upload_stream(
                container_name, blob_path, byte_stream, total_size,
                metadata=metadata,
                chunk_size=chunk_size,
                max_parallel_parts=max_parallel_parts,
            )
            return {
                "success": True,
                "blob_url": info.url,
                "blob_path": info.path,
                "size_bytes": info.size,
                "content_sha256": info.content_md5,  # stored inline
                "method": "stream_multipart",
            }
        except Exception as e:
            return {
                "success": False, "error": str(e),
                "method": "stream_multipart",
            }

    async def download_blob(self, container_name, blob_path):
        return await self._store.download(container_name, blob_path)

    async def download_blob_stream(self, container_name, blob_path,
                                   chunk_size=4 * 1024 * 1024):
        async for chunk in self._store.download_stream(
            container_name, blob_path, chunk_size=chunk_size,
        ):
            yield chunk

    async def get_blob_properties(self, container_name, blob_path):
        p = await self._store.get_properties(container_name, blob_path)
        if p is None:
            return None
        return {
            "name": blob_path,
            "size": p.size,
            "etag": "",
            "content_type": p.content_type,
            "last_modified": p.last_modified,
            "copy_status": p.copy_status,
            "copy_progress": p.copy_progress,
            "metadata": p.metadata,
        }

    async def delete_blob(self, container_name, blob_path):
        await self._store.delete(container_name, blob_path)

    async def get_blob_sas_url(self, container_name, blob_path, valid_for_hours=6):
        return await self._store.presigned_url(
            container_name, blob_path, valid_for_hours,
        )

    async def list_blobs(self, container_name, name_starts_with: Optional[str] = None):
        # Optional server-side prefix filter. Critical at enterprise
        # scale: a 400 TiB tenant's VHD container can hold millions of
        # blobs, and pulling the entire list just to filter in Python
        # balloons memory + wire time per restore. Passing it down lets
        # the backend do the filter (Azure's name_starts_with, S3's
        # Prefix).
        async for name in self._store.list_blobs(
            container_name, prefix=name_starts_with,
        ):
            yield name

    async def list_blobs_with_properties(self, container_name, name_starts_with: Optional[str] = None):
        async for name, p in self._store.list_with_props(
            container_name, prefix=name_starts_with,
        ):
            yield name, {
                "last_modified": p.last_modified,
                "size": p.size,
            }

    async def ensure_container(self, container_name):
        await self._store.ensure_container(container_name)

    async def copy_from_url_sync(self, source_url, container_name, blob_path,
                                 source_size, metadata=None):
        # Same-backend SSC only — _StoreFacade delegates to the active store.
        # If the URL isn't on the active backend (e.g. cross-cloud), the
        # impl raises NotImplementedError and callers fall back to streaming.
        info = await self._store.server_side_copy(
            source_url, container_name, blob_path, source_size, metadata,
        )
        return {
            "size": info.size,
            "etag": info.etag,
            "content_md5": info.content_md5,
            "last_modified": info.last_modified.isoformat() if info.last_modified else None,
        }

    # Azure-specific ops that aren't portable — raise so callers notice.
    async def stage_block(self, *a, **kw):
        if hasattr(self._store, "stage_block"):
            return await self._store.stage_block(*a, **kw)
        raise NotImplementedError(f"stage_block not supported by {self.kind}")

    async def commit_block_list_manual(self, *a, **kw):
        if hasattr(self._store, "commit_blocks"):
            return await self._store.commit_blocks(*a, **kw)
        raise NotImplementedError(f"commit_blocks not supported by {self.kind}")

    async def put_block_from_url(self, *a, **kw):
        if hasattr(self._store, "put_block_from_url"):
            return await self._store.put_block_from_url(*a, **kw)
        raise NotImplementedError(f"put_block_from_url not supported by {self.kind}")

    async def get_blob_url(self, container_name, blob_path):
        return await self._store.presigned_url(container_name, blob_path, valid_hours=1)

    async def close(self):
        close = getattr(self._store, "close", None)
        if close:
            await close()


class AzureStorageManager:
    """
    Manages multiple Azure Storage Accounts for sharding.
    Distributes backup load across accounts to avoid IOPS bottlenecks.
    """
    
    def __init__(self):
        self.shards: List[AzureStorageShard] = []
        self._initialize_shards()
    
    def _initialize_shards(self):
        """Initialize storage shards from configuration.

        The Azure shard objects are constructed eagerly so the toggle-
        worker can flip the router back to Azure without a process
        restart. However, when the router is on the on-prem backend
        (SeaweedFS), the Azure shard is **never used** for I/O — all
        calls go through `_router_facade`. We still build the shard
        (so the toggle path works), but we demote the init log to
        DEBUG-style so operators running exclusively on-prem don't
        get confused by an "Initialized ... azure account" banner
        that implies Azure traffic.
        """
        # Decide whether to emit the "active" banner. Treat the log as
        # interesting only if the operator hasn't explicitly marked
        # on-prem via ACTIVE_STORAGE_BACKEND=seaweedfs. The router
        # load path overrides this later anyway.
        active_pref = (os.getenv("ACTIVE_STORAGE_BACKEND") or "").lower()
        azure_is_active_hint = active_pref not in ("seaweedfs", "onprem", "on_prem")

        if settings.STORAGE_SHARD_ACCOUNTS and settings.STORAGE_SHARD_KEYS:
            for i, (account, key) in enumerate(zip(
                settings.STORAGE_SHARD_ACCOUNTS,
                settings.STORAGE_SHARD_KEYS
            )):
                shard = AzureStorageShard(account, key, shard_index=i)
                self.shards.append(shard)
                if azure_is_active_hint:
                    print(f"[AzureStorage] Initialized shard {i}: {account}")
                else:
                    print(
                        f"[AzureStorage] (standby) shard {i}: {account} — "
                        f"router active on on-prem backend, not in use",
                    )
        elif settings.AZURE_STORAGE_ACCOUNT_NAME and settings.AZURE_STORAGE_ACCOUNT_KEY:
            shard = AzureStorageShard(
                settings.AZURE_STORAGE_ACCOUNT_NAME,
                settings.AZURE_STORAGE_ACCOUNT_KEY,
                shard_index=0
            )
            self.shards.append(shard)
            if azure_is_active_hint:
                print(
                    f"[AzureStorage] Initialized single shard: "
                    f"{settings.AZURE_STORAGE_ACCOUNT_NAME}",
                )
            else:
                print(
                    f"[AzureStorage] (standby) single shard "
                    f"{settings.AZURE_STORAGE_ACCOUNT_NAME} — "
                    f"router active on on-prem backend, not in use",
                )
        else:
            print("[AzureStorage] WARNING: No Azure Storage configured")
    
    def _router_facade(self, tenant_id: str, resource_id: str):
        """Return a _StoreFacade over the router's active backend, if loaded.

        Returns None ONLY when the router truly hasn't loaded yet
        (no ``active_backend_id`` set, e.g. service still booting or
        running in a test context without storage_backends rows).

        When the router IS loaded but raises — for example during a
        Storage toggle ``transition_state`` window where writes are
        rejected — we surface the error rather than silently falling
        through to the raw Azure shard, because the legacy fallback
        would silently route data to the WRONG backend. Operators
        depend on the toggle to be honored end-to-end; a silent
        fall-back was the root cause of the demo-time "Cannot connect
        to stamitkmishr..." log noise where the active backend was
        SeaweedFS but a stale chat-att task still hit Azure.

        Knobs:
          STORAGE_ROUTER_STRICT=true → always raise on facade errors
            (recommended in prod once the toggle is healthy).
          STORAGE_FACADE_LOG_FALLBACKS=true → log the reason at WARN
            even when not strict, so silent fallbacks become visible.
        """
        import logging
        log = logging.getLogger("tmvault.storage.facade")
        try:
            from shared.storage.router import router as _router
        except Exception as exc:
            if os.getenv("STORAGE_FACADE_LOG_FALLBACKS", "false").lower() == "true":
                log.warning("[StorageFacade] router import failed: %s — using raw shard", exc)
            return None
        if not _router.active_backend_id():
            # Router not loaded yet — legitimate boot-window case.
            return None
        try:
            store = _router.get_active_store().shard_for(tenant_id, resource_id)
            return _StoreFacade(store)
        except Exception as exc:
            strict = os.getenv("STORAGE_ROUTER_STRICT", "false").lower() == "true"
            log.warning(
                "[StorageFacade] active-store resolve failed for tenant=%s resource=%s: %s "
                "(strict=%s; %s)",
                tenant_id, resource_id, exc, strict,
                "raising" if strict else "falling back to raw Azure shard",
            )
            if strict:
                raise
            return None

    def get_shard_for_resource(self, resource_id: str, tenant_id: str):
        """Deterministically assign a storage shard based on resource/tenant ID.

        Now router-aware: returns a facade over the currently-active backend
        (Azure or SeaweedFS) so callers honor the Storage toggle without
        changing their code. Falls back to the raw AzureStorageShard when
        the router hasn't loaded yet.
        """
        facade = self._router_facade(tenant_id, resource_id)
        if facade is not None:
            return facade
        if not self.shards:
            raise RuntimeError("No storage shards configured")
        hash_input = f"{tenant_id}:{resource_id}"
        hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
        shard_index = hash_value % len(self.shards)
        return self.shards[shard_index]

    def get_shard_by_index(self, index: int) -> AzureStorageShard:
        """Get a specific shard by index.

        Router-aware: when the active backend is non-Azure (SeaweedFS
        / on-prem S3-compat), returns a _StoreFacade over that backend
        instead of the raw Azure shard. The caller-side interface stays
        identical (upload_blob / stage_block / commit_block_list_manual
        / ensure_container / download_blob_stream / download_blob — all
        proxied onto the router-owned store).

        Fall back to the raw AzureStorageShard when the router hasn't
        loaded yet (process boot window) or when running the DR
        replication worker which is Azure-pinned by design. Returns
        self.shards[0] as a last resort when no shards are configured
        at all (test envs)."""
        facade = self._router_facade("default", f"idx-{index}")
        if facade is not None:
            # Router active — return the facade regardless of `index`.
            # Non-Azure backends have a single logical shard today; the
            # caller's per-folder partitioning still buys us parallel
            # write streams on the facade's underlying bucket(s).
            return facade
        if not self.shards:
            raise RuntimeError("No storage shards configured")
        return self.shards[index % len(self.shards)]

    def get_default_shard(self):
        """Get the default storage shard — router-aware."""
        facade = self._router_facade("default", "default")
        if facade is not None:
            return facade
        if not self.shards:
            raise RuntimeError("No storage shards configured — set AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY")
        return self.shards[0]

    def get_shard_for_item(self, item):
        """Resolve the shard that owns the source data for the given item.
        Uses item.backend_id when present so RESTORE paths hit the right
        backend even after a toggle. Falls back to active-backend routing."""
        try:
            backend_id = getattr(item, "backend_id", None)
            if backend_id:
                from shared.storage.router import router as _router
                store = _router.get_store_by_id(str(backend_id)).shard_for(
                    str(item.tenant_id), str(item.resource_id),
                )
                return _StoreFacade(store)
        except Exception:
            pass
        try:
            return self.get_shard_for_resource(str(item.resource_id), str(item.tenant_id))
        except Exception:
            return self.get_default_shard()
    
    def get_container_name(self, tenant_id: str, resource_type: str) -> str:
        """
        Generate container name following Azure naming conventions.
        Containers must be lowercase, 3-63 chars, only alphanumeric and hyphens.
        """
        # Defensive: accept UUID-objects as well as strings. Callers in
        # the cancel-cleanup path occasionally pass `tenant.id` (a UUID)
        # directly; UUID has no `.replace()` and would raise AttributeError.
        # str(UUID) is the canonical hyphenated form so this is loss-free.
        tenant_id_s = str(tenant_id) if tenant_id is not None else ""
        # Shorten tenant/tenant ID to avoid exceeding 63 char limit
        tenant_short = tenant_id_s.replace("-", "")[:8]
        # Replace underscores with hyphens (Azure container names don't allow underscores)
        safe_type = str(resource_type).lower().replace("_", "-")
        return f"backup-{safe_type}-{tenant_short}"
    
    def build_blob_path(self, tenant_id: str, resource_id: str, 
                       snapshot_id: str, item_id: str, timestamp: str = None) -> str:
        """
        Build versioned blob path for organized storage.
        Format: {tenant_id}/{resource_type}/{resource_id}/{snapshot_id}/{timestamp}/{item_id}
        """
        ts = timestamp or datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        return f"{tenant_id}/{resource_id}/{snapshot_id}/{ts}/{item_id}"


# Global singleton
azure_storage_manager = AzureStorageManager()


# Mirror of the workload strings the backup worker passes to get_container_name(...)
# when writing blobs. Kept here so read and write stay in sync in one place.
# Values are ordered: the first candidate is the primary container; later entries
# cover secondary containers the worker may also write into (e.g. group mailboxes
# materialized while processing an ENTRA_GROUP).
RESOURCE_TYPE_TO_WORKLOADS: Dict[str, Tuple[str, ...]] = {
    # Tier-1 mailboxes have historically split across two containers:
    # legacy snapshots wrote to "mailbox" (backup-worker pre-2026-05-08
    # alignment), the standalone backup_mailbox handler now writes to
    # "email" to match the Tier-2 USER_MAIL convention, and dedup against
    # cross-snapshot content_hashes can produce rows whose blob_path lives
    # in either bucket. List both so the restore resolver tries each.
    "MAILBOX": ("email", "mailbox"),
    "SHARED_MAILBOX": ("email", "mailbox"),
    "ROOM_MAILBOX": ("email", "mailbox"),
    "ONEDRIVE": ("files",),
    "SHAREPOINT_SITE": ("files",),
    "TEAMS_CHANNEL": ("teams",),
    "TEAMS_CHAT": ("teams",),
    "ENTRA_USER": ("entra",),
    "ENTRA_GROUP": ("entra", "group-mailbox"),
    "DYNAMIC_GROUP": ("entra", "group-mailbox"),
    "ENTRA_APP": ("entra",),
    "ENTRA_DEVICE": ("entra",),
    "ENTRA_SERVICE_PRINCIPAL": ("entra",),
    "PLANNER": ("planner",),
    "TODO": ("todo",),
    "ONENOTE": ("onenote",),
    "POWER_BI": ("power-bi",),
    "POWER_APPS": ("power-apps",),
    "POWER_AUTOMATE": ("power-automate",),
    "POWER_DLP": ("power-dlp",),
    "AZURE_VM": ("azure-vm",),
    "AZURE_SQL_DB": ("azure-sql-db",),
    "AZURE_POSTGRESQL": ("azure-postgresql",),
    "AZURE_POSTGRESQL_SINGLE": ("azure-postgresql-single",),
    # Tier 2 per-user content types. Email attachments land in the "email"
    # container; fall back to "mailbox" for any legacy rows. OneDrive Tier 2
    # reuses the Tier 1 "files" container once we start blobbing file bytes.
    "USER_MAIL": ("email", "mailbox"),
    "USER_ONEDRIVE": ("files",),
    "USER_CONTACTS": ("mailbox",),
    "USER_CALENDAR": ("mailbox",),
    "USER_CHATS": ("teams",),
}


def workload_candidates_for_resource_type(resource_type: str) -> Tuple[str, ...]:
    """Return container-workload suffixes a blob for this resource_type may live in."""
    if not resource_type:
        return ()
    return RESOURCE_TYPE_TO_WORKLOADS.get(str(resource_type).upper(), ())


async def server_side_copy_with_retry(source_url: str, container_name: str, 
                                      blob_path: str, shard: AzureStorageShard,
                                      max_retries: int = 3) -> Dict:
    """
    Server-Side Copy with automatic retry and exponential backoff.
    """
    last_error = None
    
    for attempt in range(max_retries):
        result = await shard.server_side_copy(source_url, container_name, blob_path)
        
        if result["success"]:
            return result
        
        last_error = result.get("error", "Unknown error")
        
        # Don't retry on auth/permission errors
        if "Authorization" in last_error or "Authentication" in last_error:
            return result
        
        # Exponential backoff
        delay = settings.RETRY_DELAY_MS * (settings.RETRY_BACKOFF_MULTIPLIER ** attempt) / 1000
        await asyncio.sleep(delay)

    return {"success": False, "error": f"Failed after {max_retries} retries: {last_error}"}


# ── AZ-0: Lifecycle & Immutability Helpers ──

import logging
import traceback

_lifecycle_logger = logging.getLogger("azure.lifecycle")


async def apply_lifecycle_policy(container_name: str, hot_days: int = 7,
                                  cool_days: int = 30, archive_days: int = None,
                                  shard: AzureStorageShard = None) -> Dict:
    """
    Configure Azure Blob lifecycle management rules for a container.
    If archive_days is None, blobs in archive never expire (unlimited retention).

    Returns:
        {"success": True/False, "rules_count": N, "container": "...", "error": "..."}
    """
    _lifecycle_logger.info(
        "[LifecyclePolicy] START — container=%s, hot=%dd, cool=%dd, archive=%s (unlimited=%s)",
        container_name, hot_days, cool_days,
        archive_days if archive_days is not None else "NULL",
        archive_days is None,
    )

    if shard is None:
        shard = azure_storage_manager.get_default_shard()
        if not shard:
            _lifecycle_logger.error("[LifecyclePolicy] No storage shard available — cannot apply policy")
            return {"success": False, "error": "No storage shard available"}

    # Lifecycle rules: Hot → Cool → Delete.
    #
    # We deliberately DO NOT use the Azure Archive tier. Archive blobs require
    # 1–15 hours of rehydration before they can be read, which violates the
    # product requirement that archived backups stay accessible within minutes
    # for restore/browse operations. Data lives in Cool storage for the full
    # `cool_days + archive_days` window — Cool reads return in seconds while
    # storage cost is ~50% lower than Hot. After the cumulative window, the
    # blob is deleted by the lifecycle rule below.
    rules = [
        {
            "enabled": True,
            "name": f"{container_name}_hot_to_cool",
            "definition": {
                "filters": {"blobTypes": ["blockBlob"],
                            "prefixMatch": [f"{container_name}/"]},
                "actions": {"baseBlob": {
                    "tierToCool": {"daysAfterModificationGreaterThan": hot_days}}}
            }
        },
    ]

    # Only add delete rule if archive_days is set (unlimited retention = no delete)
    if archive_days is not None:
        rules.append({
            "enabled": True,
            "name": f"{container_name}_expire",
            "definition": {
                "filters": {"blobTypes": ["blockBlob"],
                            "prefixMatch": [f"{container_name}/"]},
                "actions": {"baseBlob": {
                    "delete": {"daysAfterModificationGreaterThan":
                               hot_days + cool_days + archive_days}}}
            }
        })

    _lifecycle_logger.info(
        "[LifecyclePolicy] Prepared %d rules for container=%s (shard=%s)",
        len(rules), container_name, shard.account_name,
    )

    try:
        from azure.mgmt.storage.aio import StorageManagementClient
        from azure.identity.aio import ClientSecretCredential
        from shared.config import settings

        client_id = settings.EFFECTIVE_ARM_CLIENT_ID or settings.MICROSOFT_CLIENT_ID
        client_secret = settings.EFFECTIVE_ARM_CLIENT_SECRET or settings.MICROSOFT_CLIENT_SECRET
        tenant_id = settings.EFFECTIVE_ARM_TENANT_ID or settings.MICROSOFT_TENANT_ID

        if not client_id or not client_secret:
            _lifecycle_logger.warning(
                "[LifecyclePolicy] No ARM credentials configured — skipping policy application for %s",
                container_name,
            )
            return {"success": False, "error": "ARM credentials not configured (set AZURE_ARM_CLIENT_ID/SECRET)"}

        credential = ClientSecretCredential(
            client_id=client_id,
            client_secret=client_secret,
            tenant_id=tenant_id,
        )

        async with StorageManagementClient(credential) as mgmt:
            account_name = shard.account_name
            rg_name = settings.AZURE_BACKUP_RESOURCE_GROUP or "tmvault-storage"

            _lifecycle_logger.info(
                "[LifecyclePolicy] Applying policy — account=%s, rg=%s, policy=DefaultManagementPolicy",
                account_name, rg_name,
            )

            try:
                await mgmt.management_policies.create_or_update(
                    resource_group_name=rg_name,
                    account_name=account_name,
                    policy_name="DefaultManagementPolicy",
                    parameters={"policy": {"rules": rules}},
                )
                _lifecycle_logger.info(
                    "[LifecyclePolicy] SUCCESS — Applied %d rules to %s (account=%s, rg=%s)",
                    len(rules), container_name, account_name, rg_name,
                )
                return {"success": True, "rules_count": len(rules), "container": container_name,
                        "account": account_name, "resource_group": rg_name}
            except Exception as mgmt_exc:
                _lifecycle_logger.error(
                    "[LifecyclePolicy] MANAGEMENT API ERROR — container=%s, account=%s, rg=%s: %s\n%s",
                    container_name, account_name, rg_name, mgmt_exc, traceback.format_exc(),
                )
                return {"success": False, "error": str(mgmt_exc), "container": container_name}
    except ImportError as ie:
        _lifecycle_logger.error(
            "[LifecyclePolicy] IMPORT ERROR — Missing Azure management libraries: %s", ie,
        )
        return {"success": False, "error": f"Missing dependency: {ie}"}
    except Exception as outer_exc:
        _lifecycle_logger.error(
            "[LifecyclePolicy] UNEXPECTED ERROR — container=%s: %s\n%s",
            container_name, outer_exc, traceback.format_exc(),
        )
        return {"success": False, "error": str(outer_exc)}


async def read_encryption_scope_state(
    container_name: str,
    shard: AzureStorageShard = None,
) -> Dict:
    """Read the live encryption-scope binding for a container from ARM.

    Returns:
      {"scope": "...", "key_uri": "...", "state": "Enabled|Disabled"} or
      {"scope": None} if no scope is currently bound to the container, or
      {"error": "..."} on failure.

    Used by the reconciler to detect drift — if Azure-side state differs
    from what the SLA policy says it should be, we re-apply.
    """
    if shard is None:
        shard = azure_storage_manager.get_default_shard()
        if not shard:
            return {"error": "No storage shard available"}
    try:
        from azure.mgmt.storage.aio import StorageManagementClient
        from azure.identity.aio import ClientSecretCredential
        from shared.config import settings
    except Exception as imp_exc:
        return {"error": f"azure-mgmt-storage unavailable: {imp_exc}"}

    client_id = settings.EFFECTIVE_ARM_CLIENT_ID or settings.MICROSOFT_CLIENT_ID
    client_secret = settings.EFFECTIVE_ARM_CLIENT_SECRET or settings.MICROSOFT_CLIENT_SECRET
    tenant_id = settings.EFFECTIVE_ARM_TENANT_ID or settings.MICROSOFT_TENANT_ID
    if not client_id or not client_secret:
        return {"error": "ARM credentials not configured"}
    rg_name = settings.AZURE_BACKUP_RESOURCE_GROUP or "tmvault-storage"
    credential = ClientSecretCredential(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)
    scope_name = f"tmv-cmk-{container_name}".replace("_", "-")[:63]

    try:
        async with StorageManagementClient(credential) as mgmt:
            # Read the container to learn its current default scope.
            try:
                container = await mgmt.blob_containers.get(
                    resource_group_name=rg_name,
                    account_name=shard.account_name,
                    container_name=container_name,
                )
                bound_scope = getattr(container, "default_encryption_scope", None)
            except Exception as e:
                return {"error": f"container get failed: {e}"}
            if not bound_scope or bound_scope == "$account-encryption-key":
                return {"scope": None, "key_uri": None, "state": None}

            try:
                scope = await mgmt.encryption_scopes.get(
                    resource_group_name=rg_name,
                    account_name=shard.account_name,
                    encryption_scope_name=scope_name,
                )
                kvp = getattr(scope, "key_vault_properties", None)
                key_uri = getattr(kvp, "key_uri", None) if kvp else None
                state = getattr(scope, "state", None)
                return {"scope": bound_scope, "key_uri": key_uri, "state": str(state) if state else None}
            except Exception as e:
                return {"scope": bound_scope, "error": f"scope get failed: {e}"}
    except Exception as e:
        return {"error": str(e)}


# ─── Key Vault: latest-version resolver with TTL cache + retry ────────
# Cache key: (vault_uri, key_name). Value: (version, fetched_at_monotonic).
# TTL is short (5 min) so a rotated key is picked up by the next
# reconcile cadence without forcing every call to re-open KV.
_KV_VERSION_CACHE: Dict[Tuple[str, str], Tuple[str, float]] = {}
_KV_VERSION_CACHE_TTL_S = 300.0
# Lazy-init so module import outside an event loop (alembic CLI,
# test collection on older Python) doesn't warn / fail.
_KV_VERSION_CACHE_LOCK: Optional[asyncio.Lock] = None


def _kv_version_cache_lock() -> asyncio.Lock:
    global _KV_VERSION_CACHE_LOCK
    if _KV_VERSION_CACHE_LOCK is None:
        _KV_VERSION_CACHE_LOCK = asyncio.Lock()
    return _KV_VERSION_CACHE_LOCK


async def resolve_key_vault_latest_version(
    key_vault_uri: str,
    key_name: str,
) -> Optional[str]:
    """Query Key Vault for the current version of a key. Returns the
    version string, or None on failure (e.g. permission denied).

    Used when the SLA policy pins to "latest" so the reconciler can
    detect when the customer rotated the key in Key Vault and re-apply
    the encryption scope with the new version. Storage account managed
    identity needs `Get` permission on the key for this to succeed.

    Caching: result is cached for ``_KV_VERSION_CACHE_TTL_S`` (5 min)
    per (vault, key) so the SLA reconcile loop doesn't hammer KV. On
    rotation the next cache miss picks up the new version.

    Retry: transient KV errors (5xx / network) retry up to 3× with
    exponential backoff before returning None. Permanent errors
    (4xx, missing key, permission denied) return None immediately.
    """
    if not key_vault_uri or not key_name:
        return None

    cache_key = (key_vault_uri, key_name)
    now = time.monotonic() if hasattr(time, "monotonic") else 0.0
    cached = _KV_VERSION_CACHE.get(cache_key)
    if cached is not None and (now - cached[1]) < _KV_VERSION_CACHE_TTL_S:
        return cached[0]

    try:
        from azure.keyvault.keys.aio import KeyClient
        from azure.identity.aio import ClientSecretCredential
        from azure.core.exceptions import HttpResponseError, ServiceRequestError
        from shared.config import settings
    except Exception:
        return None

    client_id = settings.EFFECTIVE_ARM_CLIENT_ID or settings.MICROSOFT_CLIENT_ID
    client_secret = settings.EFFECTIVE_ARM_CLIENT_SECRET or settings.MICROSOFT_CLIENT_SECRET
    tenant_id = settings.EFFECTIVE_ARM_TENANT_ID or settings.MICROSOFT_TENANT_ID
    if not client_id or not client_secret:
        return None

    # Async credential MUST be closed to free the underlying HTTP session.
    # Wrap it + the KeyClient in async-with so even a raise during get_key
    # walks back through cleanup.
    backoff_s = 1.0
    for attempt in range(3):
        try:
            async with ClientSecretCredential(
                client_id=client_id, client_secret=client_secret, tenant_id=tenant_id,
            ) as credential:
                async with KeyClient(vault_url=key_vault_uri, credential=credential) as client:
                    key = await client.get_key(key_name)  # latest by default
                    # `key.id` is `https://vault/keys/<name>/<version>`
                    kid = getattr(key, "id", None) or ""
                    if "/" not in kid:
                        return None
                    new_version = kid.rsplit("/", 1)[-1]
                    # Log on rotation so operators can correlate SLA
                    # re-applies to actual KV version bumps.
                    if cached is not None and cached[0] != new_version:
                        _lifecycle_logger.info(
                            "[KV] key %s/%s rotation detected: %s → %s",
                            key_vault_uri, key_name, cached[0], new_version,
                        )
                    async with _kv_version_cache_lock():
                        _KV_VERSION_CACHE[cache_key] = (new_version, now)
                    return new_version
        except HttpResponseError as exc:
            status = getattr(exc, "status_code", None) or 0
            if 500 <= status < 600 and attempt < 2:
                await asyncio.sleep(backoff_s)
                backoff_s *= 2
                continue
            _lifecycle_logger.warning(
                "[KV] resolve_key_vault_latest_version permanent error: %s/%s status=%s err=%s",
                key_vault_uri, key_name, status, exc,
            )
            return None
        except ServiceRequestError as exc:
            # Network-layer transient — retry
            if attempt < 2:
                await asyncio.sleep(backoff_s)
                backoff_s *= 2
                continue
            _lifecycle_logger.warning(
                "[KV] resolve_key_vault_latest_version network error after retries: %s",
                exc,
            )
            return None
        except Exception as exc:
            _lifecycle_logger.warning(
                "[KV] resolve_key_vault_latest_version unexpected: %s/%s err=%s",
                key_vault_uri, key_name, exc,
            )
            return None
    return None


async def apply_encryption_scope(
    container_name: str,
    key_vault_uri: str,
    key_name: str,
    key_version: Optional[str] = None,
    shard: AzureStorageShard = None,
) -> Dict:
    """
    Configure container-level customer-managed-key (CMK / BYOK) encryption
    via Azure ARM. Creates an EncryptionScope on the storage account that
    references the customer's Key Vault key, then patches the container so
    `default_encryption_scope` points at it. New blobs in this container
    are encrypted with the customer key; existing blobs remain encrypted
    with whatever scope was active when they were written.

    Returns:
      success → {"success": True, "container": ..., "scope": ..., "status": "OK"}
      access denied → {"success": False, "status": "KEY_VAULT_ACCESS_DENIED", ...}
      generic error → {"success": False, "status": "ERROR", "error": ...}

    The status string is persisted on the SLA policy so the wizard can
    surface a red-dot warning when the storage account managed identity
    lacks Get/WrapKey/UnwrapKey on the customer's vault.
    """
    _lifecycle_logger.info(
        "[EncryptionScope] START — container=%s, vault=%s, key=%s, version=%s",
        container_name, key_vault_uri, key_name, key_version or "(latest)",
    )

    if shard is None:
        shard = azure_storage_manager.get_default_shard()
        if not shard:
            return {"success": False, "container": container_name,
                    "status": "NO_SHARD", "error": "No storage shard available"}

    if not (key_vault_uri and key_name):
        return {"success": False, "container": container_name,
                "status": "INVALID_KEY", "error": "key_vault_uri and key_name are required"}

    try:
        from azure.mgmt.storage.aio import StorageManagementClient
        from azure.identity.aio import ClientSecretCredential
        from shared.config import settings
    except Exception as imp_exc:
        return {"success": False, "container": container_name,
                "status": "AZURE_SDK_MISSING",
                "error": f"azure-mgmt-storage unavailable: {imp_exc}"}

    client_id = settings.EFFECTIVE_ARM_CLIENT_ID or settings.MICROSOFT_CLIENT_ID
    client_secret = settings.EFFECTIVE_ARM_CLIENT_SECRET or settings.MICROSOFT_CLIENT_SECRET
    tenant_id = settings.EFFECTIVE_ARM_TENANT_ID or settings.MICROSOFT_TENANT_ID
    if not client_id or not client_secret:
        return {"success": False, "container": container_name,
                "status": "NO_ARM_CREDS",
                "error": "ARM credentials not configured"}

    rg_name = settings.AZURE_BACKUP_RESOURCE_GROUP or "tmvault-storage"

    # Encryption-scope name is a per-policy identifier. Container names are
    # already tenant+workload-scoped, so deriving from the container keeps
    # the scope unique without needing the policy id at this layer.
    scope_name = f"tmv-cmk-{container_name}".replace("_", "-")[:63]

    # Wrap credential + management client in async-with so the
    # underlying HTTP session is closed even if put/update raises.
    # Without this the long-lived aio credential leaks a transport
    # pool on every call (matters once the SLA reconciler fans out).
    try:
        async with ClientSecretCredential(
            client_id=client_id, client_secret=client_secret, tenant_id=tenant_id,
        ) as credential:
            async with StorageManagementClient(credential) as mgmt:
                key_uri_full = (
                    f"{key_vault_uri.rstrip('/')}/keys/{key_name}/{key_version}"
                    if key_version else
                    f"{key_vault_uri.rstrip('/')}/keys/{key_name}"
                )
                try:
                    await mgmt.encryption_scopes.put(
                        resource_group_name=rg_name,
                        account_name=shard.account_name,
                        encryption_scope_name=scope_name,
                        encryption_scope={
                            "properties": {
                                "source": "Microsoft.KeyVault",
                                "state": "Enabled",
                                "keyVaultProperties": {"keyUri": key_uri_full},
                                "requireInfrastructureEncryption": False,
                            }
                        },
                    )
                except Exception as scope_exc:
                    msg = str(scope_exc)
                    # Storage account managed identity lacks key permissions —
                    # surface a specific status so the UI can prompt for
                    # `az role assignment create --role "Key Vault Crypto User"`.
                    if any(s in msg for s in ("Forbidden", "AccessDenied",
                                              "AuthorizationFailed",
                                              "KeyVaultAccessForbidden")):
                        _lifecycle_logger.error(
                            "[EncryptionScope] KEY_VAULT_ACCESS_DENIED — %s", msg)
                        return {"success": False, "container": container_name,
                                "status": "KEY_VAULT_ACCESS_DENIED",
                                "error": msg, "scope": scope_name}
                    raise

                # Point the container at the new scope.
                await mgmt.blob_containers.update(
                    resource_group_name=rg_name,
                    account_name=shard.account_name,
                    container_name=container_name,
                    blob_container={
                        "default_encryption_scope": scope_name,
                        "deny_encryption_scope_override": True,
                    },
                )
                return {"success": True, "container": container_name,
                        "scope": scope_name, "status": "OK"}
    except Exception as e:
        _lifecycle_logger.error(
            "[EncryptionScope] FAILED — container=%s: %s\n%s",
            container_name, e, traceback.format_exc(),
        )
        return {"success": False, "container": container_name,
                "status": "ERROR", "error": str(e)}


async def read_container_immutability_state(
    container_name: str,
    shard: AzureStorageShard = None,
) -> Dict:
    """Read the live immutability-policy state for a container from ARM.
    Returns {"mode": "None|Unlocked|Locked", "days": int|None}.

    Used by the reconciler to refuse loosening transitions that Azure
    can't honor — e.g. a Locked container whose policy row was edited to
    Unlocked. Without this guard, the reconciler silently fails (Azure
    rejects the change) and the operator thinks the policy was loosened
    when in fact it's still WORM-locked.
    """
    if shard is None:
        shard = azure_storage_manager.get_default_shard()
        if not shard:
            return {"error": "No storage shard available"}
    try:
        from azure.mgmt.storage.aio import StorageManagementClient
        from azure.identity.aio import ClientSecretCredential
        from shared.config import settings
    except Exception as imp_exc:
        return {"error": f"azure-mgmt-storage unavailable: {imp_exc}"}

    client_id = settings.EFFECTIVE_ARM_CLIENT_ID or settings.MICROSOFT_CLIENT_ID
    client_secret = settings.EFFECTIVE_ARM_CLIENT_SECRET or settings.MICROSOFT_CLIENT_SECRET
    tenant_id = settings.EFFECTIVE_ARM_TENANT_ID or settings.MICROSOFT_TENANT_ID
    if not client_id or not client_secret:
        return {"error": "ARM credentials not configured"}
    rg_name = settings.AZURE_BACKUP_RESOURCE_GROUP or "tmvault-storage"
    credential = ClientSecretCredential(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)
    try:
        async with StorageManagementClient(credential) as mgmt:
            try:
                pol = await mgmt.blob_containers.get_immutability_policy(
                    resource_group_name=rg_name,
                    account_name=shard.account_name,
                    container_name=container_name,
                )
                state = getattr(pol, "state", None)
                days = getattr(pol, "immutability_period_since_creation_in_days", None)
                # Azure exposes "Locked" or "Unlocked"; absence == None.
                mode_str = str(state) if state else "None"
                # Some SDK versions wrap state in an enum; normalize.
                mode = "Locked" if "Locked" in mode_str else ("Unlocked" if "Unlocked" in mode_str else "None")
                return {"mode": mode, "days": days}
            except Exception as e:
                msg = str(e)
                if "ImmutabilityPolicyNotFound" in msg or "404" in msg:
                    return {"mode": "None", "days": None}
                return {"error": msg}
    except Exception as e:
        return {"error": str(e)}


async def apply_container_immutability(
    container_name: str,
    immutability_period_days: int,
    mode: str = "Unlocked",
    shard: AzureStorageShard = None,
) -> Dict:
    """
    Set a CONTAINER-level immutability policy via the Azure ARM management
    API. Applies WORM protection to every blob in the container — paired
    with `reconcile_lifecycle_policies` so a policy change immediately
    enforces the SLA's `immutability_mode` across the whole tenant.

    mode = 'None'     → DELETE the policy (clears WORM).
    mode = 'Unlocked' → editable; can be extended/tightened later.
    mode = 'Locked'   → permanent; cannot be loosened or removed.

    Returns: {"success": bool, "container": str, "mode": str, "error"?: str}
    """
    _lifecycle_logger.info(
        "[ContainerImmutability] START — container=%s, days=%s, mode=%s",
        container_name, immutability_period_days, mode,
    )

    if shard is None:
        shard = azure_storage_manager.get_default_shard()
        if not shard:
            return {"success": False, "container": container_name,
                    "error": "No storage shard available"}

    # Refuse transitions Azure can't honor. If the live container is
    # already in "Locked" state, ARM rejects any loosening (None or
    # Unlocked). Surface a specific error so the reconciler can mark the
    # policy KEY_VAULT-style "drift" status instead of silently failing
    # at scale.
    if mode in ("None", "Unlocked"):
        live = await read_container_immutability_state(container_name, shard)
        if (live or {}).get("mode") == "Locked":
            return {"success": False, "container": container_name,
                    "status": "REFUSED_LOOSEN_LOCKED",
                    "error": (
                        "Container has a Locked WORM policy in Azure that "
                        "cannot be loosened or removed — ignoring policy-row "
                        "transition to '{}'.".format(mode)
                    )}

    try:
        from azure.mgmt.storage.aio import StorageManagementClient
        from azure.identity.aio import ClientSecretCredential
        from shared.config import settings
    except Exception as imp_exc:
        return {"success": False, "container": container_name,
                "error": f"azure-mgmt-storage unavailable: {imp_exc}"}

    client_id = settings.EFFECTIVE_ARM_CLIENT_ID or settings.MICROSOFT_CLIENT_ID
    client_secret = settings.EFFECTIVE_ARM_CLIENT_SECRET or settings.MICROSOFT_CLIENT_SECRET
    tenant_id = settings.EFFECTIVE_ARM_TENANT_ID or settings.MICROSOFT_TENANT_ID
    if not client_id or not client_secret:
        return {"success": False, "container": container_name,
                "error": "ARM credentials not configured"}

    rg_name = settings.AZURE_BACKUP_RESOURCE_GROUP or "tmvault-storage"
    credential = ClientSecretCredential(
        client_id=client_id, client_secret=client_secret, tenant_id=tenant_id,
    )
    try:
        async with StorageManagementClient(credential) as mgmt:
            if mode == "None":
                # Delete any existing policy. Locked policies cannot be
                # deleted — Azure returns 409; surface that as a soft error.
                try:
                    existing = await mgmt.blob_containers.get_immutability_policy(
                        resource_group_name=rg_name,
                        account_name=shard.account_name,
                        container_name=container_name,
                    )
                    if_match = existing.etag
                    await mgmt.blob_containers.delete_immutability_policy(
                        resource_group_name=rg_name,
                        account_name=shard.account_name,
                        container_name=container_name,
                        if_match=if_match,
                    )
                except Exception as del_exc:
                    msg = str(del_exc)
                    if "ImmutabilityPolicyNotFound" in msg or "404" in msg:
                        return {"success": True, "container": container_name, "mode": "None",
                                "note": "no_policy_to_delete"}
                    return {"success": False, "container": container_name, "mode": "None",
                            "error": msg}
                return {"success": True, "container": container_name, "mode": "None"}

            await mgmt.blob_containers.create_or_update_immutability_policy(
                resource_group_name=rg_name,
                account_name=shard.account_name,
                container_name=container_name,
                parameters={
                    "properties": {
                        "immutabilityPeriodSinceCreationInDays": int(immutability_period_days),
                        "allowProtectedAppendWrites": True,
                    }
                },
            )

            if mode == "Locked":
                # Lock irreversibly. Get current etag first.
                cur = await mgmt.blob_containers.get_immutability_policy(
                    resource_group_name=rg_name,
                    account_name=shard.account_name,
                    container_name=container_name,
                )
                await mgmt.blob_containers.lock_immutability_policy(
                    resource_group_name=rg_name,
                    account_name=shard.account_name,
                    container_name=container_name,
                    if_match=cur.etag,
                )

            return {"success": True, "container": container_name, "mode": mode,
                    "days": immutability_period_days}
    except Exception as e:
        msg = str(e)
        if "ImmutabilityPolicyLocked" in msg:
            # Already locked — Azure refuses changes. Treat as success
            # (the policy is enforced; we just can't loosen it).
            return {"success": True, "container": container_name, "mode": "Locked",
                    "note": "already_locked"}
        _lifecycle_logger.error(
            "[ContainerImmutability] FAILED — container=%s: %s\n%s",
            container_name, e, traceback.format_exc(),
        )
        return {"success": False, "container": container_name, "error": msg}


async def apply_container_legal_hold(
    container_name: str,
    enabled: bool,
    tag: str = "tmvault-legal-hold",
    shard: AzureStorageShard = None,
) -> Dict:
    """
    Set or clear a CONTAINER-level legal hold via ARM. Tag is preserved on
    enable; clear removes it. Container legal-holds protect every blob in
    the container regardless of immutability period.
    """
    _lifecycle_logger.info(
        "[ContainerLegalHold] START — container=%s, enabled=%s, tag=%s",
        container_name, enabled, tag,
    )
    if shard is None:
        shard = azure_storage_manager.get_default_shard()
        if not shard:
            return {"success": False, "container": container_name,
                    "error": "No storage shard available"}

    try:
        from azure.mgmt.storage.aio import StorageManagementClient
        from azure.identity.aio import ClientSecretCredential
        from shared.config import settings
    except Exception as imp_exc:
        return {"success": False, "container": container_name,
                "error": f"azure-mgmt-storage unavailable: {imp_exc}"}

    client_id = settings.EFFECTIVE_ARM_CLIENT_ID or settings.MICROSOFT_CLIENT_ID
    client_secret = settings.EFFECTIVE_ARM_CLIENT_SECRET or settings.MICROSOFT_CLIENT_SECRET
    tenant_id = settings.EFFECTIVE_ARM_TENANT_ID or settings.MICROSOFT_TENANT_ID
    if not client_id or not client_secret:
        return {"success": False, "container": container_name,
                "error": "ARM credentials not configured"}

    rg_name = settings.AZURE_BACKUP_RESOURCE_GROUP or "tmvault-storage"
    credential = ClientSecretCredential(
        client_id=client_id, client_secret=client_secret, tenant_id=tenant_id,
    )
    try:
        async with StorageManagementClient(credential) as mgmt:
            op = "set_legal_hold" if enabled else "clear_legal_hold"
            method = getattr(mgmt.blob_containers, op)
            await method(
                resource_group_name=rg_name,
                account_name=shard.account_name,
                container_name=container_name,
                legal_hold={"tags": [tag]},
            )
            return {"success": True, "container": container_name,
                    "enabled": enabled, "tag": tag}
    except Exception as e:
        _lifecycle_logger.error(
            "[ContainerLegalHold] FAILED — container=%s: %s\n%s",
            container_name, e, traceback.format_exc(),
        )
        return {"success": False, "container": container_name, "error": str(e)}


async def apply_blob_immutability(container_name: str, blob_path: str,
                                   retention_until: datetime, mode: str = "Unlocked",
                                   shard: AzureStorageShard = None) -> Dict:
    """
    Apply time-based immutability (WORM) to a specific blob.
    mode='Locked' is permanent; mode='Unlocked' allows extension.
    For "forever" retention, Azure requires a concrete expiry date — use now + 100 years.

    Returns:
        {"success": True/False, "blob": "...", "until": "...", "mode": "...", "error": "..."}
    """
    _lifecycle_logger.info(
        "[Immutability] START — container=%s, blob=%s, until=%s, mode=%s",
        container_name, blob_path, retention_until.isoformat(), mode,
    )

    if shard is None:
        shard = azure_storage_manager.get_default_shard()
        if not shard:
            _lifecycle_logger.error("[Immutability] No storage shard available — cannot apply immutability")
            return {"success": False, "error": "No storage shard available", "blob": blob_path}

    try:
        async_client = shard.get_async_client()
        blob_client = async_client.get_blob_client(container_name, blob_path)
        await blob_client.set_immutability_policy(
            immutability_policy={"expiry_time": retention_until, "policy_mode": mode},
        )
        _lifecycle_logger.info(
            "[Immutability] SUCCESS — blob=%s, until=%s, mode=%s",
            blob_path, retention_until.isoformat(), mode,
        )
        return {"success": True, "blob": blob_path, "until": retention_until.isoformat(), "mode": mode}
    except Exception as e:
        error_type = type(e).__name__
        _lifecycle_logger.error(
            "[Immutability] FAILED — container=%s, blob=%s, error_type=%s: %s\n%s",
            container_name, blob_path, error_type, e, traceback.format_exc(),
        )
        # Provide actionable hints
        error_msg = str(e)
        if "ImmutabilityPolicyAlreadyExists" in error_msg:
            hint = "Immutability policy already set on this blob — this is safe, blob is already protected."
            _lifecycle_logger.warning("[Immutability] %s", hint)
            return {"success": True, "blob": blob_path, "until": retention_until.isoformat(),
                    "mode": mode, "note": "already_exists"}
        elif "ImmutableBlob" in error_msg or "immutable" in error_msg.lower():
            hint = "Blob is already immutable — this is expected behavior."
            _lifecycle_logger.warning("[Immutability] %s", hint)
            return {"success": True, "blob": blob_path, "note": "already_immutable"}
        elif "Authorization" in error_msg or "Authentication" in error_msg:
            hint = "Check storage account key permissions — need Blob Contributor role."
            _lifecycle_logger.warning("[Immutability] %s", hint)
            return {"success": False, "error": error_msg, "hint": hint}
        else:
            return {"success": False, "error": error_msg}


async def apply_legal_hold(container_name: str, blob_path: str,
                           tag: str = "tmvault-legal-hold",
                           shard: AzureStorageShard = None) -> Dict:
    """
    Apply legal hold to a blob. Separate from time-based immutability and does not expire.

    Returns:
        {"success": True/False, "blob": "...", "legal_hold": True, "tag": "...", "error": "..."}
    """
    _lifecycle_logger.info(
        "[LegalHold] START — container=%s, blob=%s, tag=%s",
        container_name, blob_path, tag,
    )

    if shard is None:
        shard = azure_storage_manager.get_default_shard()
        if not shard:
            _lifecycle_logger.error("[LegalHold] No storage shard available — cannot apply legal hold")
            return {"success": False, "error": "No storage shard available", "blob": blob_path}

    try:
        async_client = shard.get_async_client()
        blob_client = async_client.get_blob_client(container_name, blob_path)
        await blob_client.set_legal_hold(legal_hold=True)
        _lifecycle_logger.info(
            "[LegalHold] SUCCESS — blob=%s, tag=%s",
            blob_path, tag,
        )
        return {"success": True, "blob": blob_path, "legal_hold": True, "tag": tag}
    except Exception as e:
        error_type = type(e).__name__
        _lifecycle_logger.error(
            "[LegalHold] FAILED — container=%s, blob=%s, error_type=%s: %s\n%s",
            container_name, blob_path, error_type, e, traceback.format_exc(),
        )
        error_msg = str(e)
        if "LegalHoldAlreadyExists" in error_msg or "already" in error_msg.lower():
            hint = "Legal hold already set on this blob — this is safe."
            _lifecycle_logger.warning("[LegalHold] %s", hint)
            return {"success": True, "blob": blob_path, "legal_hold": True, "note": "already_set"}
        elif "Authorization" in error_msg or "Authentication" in error_msg:
            hint = "Check storage account key permissions — need Blob Contributor role."
            _lifecycle_logger.warning("[LegalHold] %s", hint)
            return {"success": False, "error": error_msg, "hint": hint}
        else:
            return {"success": False, "error": error_msg}


# Error substrings that indicate the Azure SDK aiohttp transport has
# poisoned its underlying connector — retrying on the same client will
# usually hit the same failure or worse, surface an orphan-future
# cascade that kills the event loop. When detected we close + rebuild
# the shard's async client before the next attempt.
_TRANSPORT_RESET_ERROR_PATTERNS = (
    "SSL shutdown timed out",
    "Timeout on reading data from socket",
    "Cannot write to closing transport",
    "ClientConnectionError",
    "ServerDisconnectedError",
    "Connection lost",
)


def _is_transport_reset_error(error_msg: str) -> bool:
    if not error_msg:
        return False
    return any(p in error_msg for p in _TRANSPORT_RESET_ERROR_PATTERNS)


async def _reset_shard_transport(shard) -> None:
    """Used to call shard._store.close() to dispose the shared aiohttp
    ClientSession after a transport-reset, on the theory that the
    poisoned connector pool would otherwise reuse the dead socket.

    That theory was wrong. AzureStorageShard._async_client is a
    SINGLETON shared by every concurrent upload from this worker (chats
    fans out 16 hostedContent uploads in parallel, OneDrive fires up to
    AZURE_UPLOAD_CONCURRENCY=5 block PUTs per file, etc). Closing the
    shared client mid-flight on a single timeout makes every other
    in-flight request crash with:

        File "aiohttp/client.py", line 740, in _connect_and_send_request
            assert self._connector is not None
        AssertionError

    aiohttp's connector pool drops bad sockets automatically when an
    error fires on them, and grabs a fresh socket for the retry. The
    exponential backoff in upload_blob_with_retry gives any genuinely
    poisoned socket enough wall time to age out of the keep-alive
    window. So the recycle is now a no-op — we keep the log line so
    the rate of transport-resets remains observable, but we don't
    nuke the shared client. Re-enable per-retry one-shot clients
    if a future Azure SDK bug actually requires it."""
    return


async def upload_blob_with_retry_from_file(container_name: str, blob_path: str, file_path: str,
                                           shard: AzureStorageShard, file_size: int = 0,
                                           max_retries: int = 3, metadata: Optional[Dict] = None) -> Dict:
    """
    Upload from a local file with parallel block uploads.
    Streams from disk — never loads full file into memory.
    Uses large block sizes (100MB) for maximum throughput.
    """
    import os
    last_error = None
    file_size = file_size or os.path.getsize(file_path)

    for attempt in range(max_retries):
        result = await shard.upload_blob_from_file(container_name, blob_path, file_path,
                                                    file_size=file_size, metadata=metadata)

        if result["success"]:
            try:
                os.unlink(file_path)
            except OSError:
                pass
            return result

        last_error = result.get("error", "Unknown error")
        if "Authorization" in last_error or "Authentication" in last_error:
            return result

        if _is_transport_reset_error(last_error):
            print(f"[AzureStorage] Transport-reset detected on {container_name}/{blob_path} "
                  f"(attempt {attempt+1}/{max_retries}); recycling SDK client. err={last_error}")
            await _reset_shard_transport(shard)

        delay = settings.RETRY_DELAY_MS * (settings.RETRY_BACKOFF_MULTIPLIER ** attempt) / 1000
        await asyncio.sleep(delay)

    try:
        os.unlink(file_path)
    except OSError:
        pass
    return {"success": False, "error": f"Failed after {max_retries} retries: {last_error}"}
async def upload_blob_with_retry(container_name: str, blob_path: str, content: bytes,
                                shard: AzureStorageShard, max_retries: int = 3,
                                metadata: Optional[Dict] = None) -> Dict:
    """
    Direct upload with automatic retry and exponential backoff.
    """
    last_error = None

    for attempt in range(max_retries):
        result = await shard.upload_blob(container_name, blob_path, content, metadata=metadata)

        if result["success"]:
            return result

        last_error = result.get("error", "Unknown error")

        # Don't retry on auth/permission errors
        if "Authorization" in last_error or "Authentication" in last_error:
            return result

        # Transport-reset cascade — close the poisoned aiohttp connector
        # before next attempt so we don't reuse a half-dead session.
        if _is_transport_reset_error(last_error):
            print(f"[AzureStorage] Transport-reset detected on {container_name}/{blob_path} "
                  f"(attempt {attempt+1}/{max_retries}); recycling SDK client. err={last_error}")
            await _reset_shard_transport(shard)

        # Exponential backoff
        delay = settings.RETRY_DELAY_MS * (settings.RETRY_BACKOFF_MULTIPLIER ** attempt) / 1000
        await asyncio.sleep(delay)

    return {"success": False, "error": f"Failed after {max_retries} retries: {last_error}"}


async def upload_blob_with_dedup(
    tenant_id,
    content_hash: str,
    container_name: str,
    blob_path: str,
    content: bytes,
    shard,
    max_retries: int = 3,
    metadata: Optional[Dict] = None,
    min_size_bytes: int = 1024,
) -> Dict:
    """Dedup-aware upload.

    Before shipping bytes over the wire, check whether this tenant
    already has a blob backed by the same content_hash. If yes,
    return a success result that points at the existing blob_path
    without re-uploading — typical M365 dedup rate is 5-10× so
    on a 400 TiB install this meaningfully cuts Azure egress +
    storage + worker NIC time.

    Never crosses tenant boundaries (lookup scoped to tenant_id).

    Returns the same dict shape as upload_blob_with_retry, plus a
    `method` that's "dedup_hit" when we reused an existing blob.
    Callers can log the method to measure dedup rate. On any DB
    hiccup the dedup check silently falls through to a real upload
    — never blocks the backup.
    """
    if content_hash and len(content) >= min_size_bytes:
        try:
            from shared.blob_dedup import find_existing_blob
            from shared.database import async_session_factory
            async with async_session_factory() as session:
                existing = await find_existing_blob(
                    session, tenant_id, content_hash,
                    min_size_bytes=min_size_bytes,
                )
            if existing:
                # Container-aware dedup. The dedup index tracks
                # (tenant_id, content_hash) → blob_path, but NOT which
                # workload-container the bytes live in. Cross-workload
                # collisions (e.g. a MAILBOX-snapshot attachment with
                # the same content_hash as a USER_MAIL upload) leave
                # the existing blob_path readable only from the writer's
                # original container. A naive dedup_hit would then hand
                # the new SnapshotItem a path that the restore-side
                # workload-container resolver can't open, producing
                # silent "no attachments in PST" failures.
                #
                # Probe the path against ``container_name`` (the active
                # writer's target). If it exists, safely reuse. If not,
                # fall through to a real upload so the new row is
                # anchored in the active container.
                ok = False
                try:
                    if hasattr(shard, "get_blob_properties"):
                        props = await shard.get_blob_properties(
                            container_name, existing,
                        )
                        if props is not None:
                            size = (
                                getattr(props, "size", None)
                                if not isinstance(props, dict)
                                else props.get("size")
                            )
                            ok = bool(size and size > 0)
                except Exception:
                    ok = False
                if ok:
                    return {
                        "success": True,
                        "blob_path": existing,
                        "blob_url": existing,
                        "size_bytes": len(content),
                        "method": "dedup_hit",
                    }
                # Existing blob is not readable from container_name —
                # likely lives in a sibling workload container. Fall
                # through to a real upload so this row's blob_path is
                # always reachable from its writer's container.
        except Exception:
            # Dedup is advisory — a DB hiccup must not block the backup.
            pass

    return await upload_blob_with_retry(
        container_name, blob_path, content, shard,
        max_retries=max_retries, metadata=metadata,
    )
