"""SeaweedStore — S3-compat backend via aioboto3.

Bucket convention: one bucket per shard (e.g. ``tmvault-shard-0``). Keys are
path-prefixed: ``{container}/{path}``.

All buckets MUST be created with ObjectLockEnabledForBucket=True +
versioning enabled (see scripts/create_seaweedfs_buckets.py). Retrofit
not possible.
"""
from __future__ import annotations

import asyncio
import hashlib
import os
from datetime import datetime
from typing import Any, AsyncIterator, Dict, List, Optional

import aioboto3
from botocore.config import Config as _BotoConfig
from botocore.exceptions import ClientError

# Fail-fast S3 client config. Without explicit timeouts, a down/restarting
# SeaweedFS (observed on Railway under heavy write load) makes an upload HANG
# indefinitely — which wedges the OneDrive drain coroutine, holds the partition
# lease alive (the LeaseExtender keeps renewing while the process lives), so the
# stale-sweep never reclaims it and the snapshot never finalizes. A bounded
# connect/read timeout turns a seaweed blip into a fast failure → the per-file
# retry queue handles it → the shard completes. max_pool_connections is raised
# well above the default 10 so high file-concurrency doesn't queue/stall waiting
# for a connection.
_SEAWEED_BOTO_CONFIG = _BotoConfig(
    connect_timeout=int(os.getenv("ONPREM_S3_CONNECT_TIMEOUT_S", "10")),
    read_timeout=int(os.getenv("ONPREM_S3_READ_TIMEOUT_S", "120")),
    retries={"max_attempts": int(os.getenv("ONPREM_S3_MAX_ATTEMPTS", "3")),
             "mode": "standard"},
    max_pool_connections=int(os.getenv("ONPREM_S3_MAX_POOL", "256")),
)

from shared.storage.base import BlobInfo, BlobProps
from shared.storage.errors import BackendUnreachableError, ImmutableBlobError

_MODE_TMVAULT_TO_S3 = {"Locked": "COMPLIANCE", "Unlocked": "GOVERNANCE"}


class SeaweedStore:
    kind = "seaweedfs"

    def __init__(
        self,
        backend_id: str,
        name: str,
        endpoint: str,
        access_key: str,
        secret_key: str,
        buckets: list[str],
        region: str = "us-east-1",
        verify_tls: bool = True,
        ca_bundle: Optional[str] = None,
        upload_concurrency: int = 8,
        multipart_threshold_mb: int = 100,
        public_endpoint: Optional[str] = None,
    ):
        self.backend_id = backend_id
        self.name = name
        self._endpoint = endpoint
        # `public_endpoint` is the browser-reachable URL host. Internal
        # S3 calls (uploads, deletes, head) use self._endpoint (the
        # docker-network hostname, e.g. `http://seaweedfs:8333`). Presigned
        # URLs are handed to the browser, which cannot resolve docker-only
        # names — so we rewrite the host in the generated URL to
        # `public_endpoint` when it differs. Falls back to self._endpoint.
        self._public_endpoint = (public_endpoint or endpoint).rstrip("/")
        self._access = access_key
        self._secret = secret_key
        self._buckets = buckets
        self._region = region
        self._verify = ca_bundle if ca_bundle else verify_tls
        self._session = aioboto3.Session()
        self._upload_concurrency = upload_concurrency
        self._multipart_threshold = multipart_threshold_mb * 1024 * 1024
        self._forced_bucket: Optional[str] = None

    @classmethod
    def from_config(cls, backend_id: str, name: str, endpoint: str,
                    secret_ref: str, config: dict) -> "SeaweedStore":
        if secret_ref.startswith("env://"):
            secret = os.getenv(secret_ref[len("env://"):], "")
        else:
            raise ValueError(f"Unsupported secret_ref scheme: {secret_ref}")
        access_env = config.get("access_key_env", "ONPREM_S3_ACCESS_KEY")
        access = os.getenv(access_env, "")
        # public_endpoint resolution order:
        #   1. config["public_endpoint"] (explicit in tm.storage_backends row)
        #   2. env var named by config["public_endpoint_env"]
        #   3. default env var ONPREM_S3_PUBLIC_ENDPOINT
        #   4. fall back to endpoint (internal docker host)
        public_endpoint = (
            config.get("public_endpoint")
            or os.getenv(config.get("public_endpoint_env", "ONPREM_S3_PUBLIC_ENDPOINT"))
            or None
        )
        return cls(
            backend_id=backend_id, name=name, endpoint=endpoint,
            access_key=access, secret_key=secret,
            buckets=list(config.get("buckets", [])),
            region=config.get("region", "us-east-1"),
            verify_tls=config.get("verify_tls", True),
            ca_bundle=config.get("ca_bundle"),
            upload_concurrency=config.get("upload_concurrency", 8),
            multipart_threshold_mb=config.get("multipart_threshold_mb", 100),
            public_endpoint=public_endpoint,
        )

    def shard_for(self, tenant_id: str, resource_id: str) -> "SeaweedStore":
        if not self._buckets:
            raise RuntimeError("no buckets configured")
        h = int(hashlib.md5(f"{tenant_id}:{resource_id}".encode()).hexdigest(), 16)
        chosen = self._buckets[h % len(self._buckets)]
        clone = SeaweedStore(
            backend_id=self.backend_id, name=self.name, endpoint=self._endpoint,
            access_key=self._access, secret_key=self._secret, buckets=[chosen],
            region=self._region,
            verify_tls=self._verify if not isinstance(self._verify, str) else True,
            ca_bundle=self._verify if isinstance(self._verify, str) else None,
            upload_concurrency=self._upload_concurrency,
            multipart_threshold_mb=self._multipart_threshold // (1024 * 1024),
            # Must propagate public_endpoint — the clone is what callers
            # actually use to sign URLs (via _StoreFacade → presigned_url),
            # and SeaweedStore.__init__ defaults _public_endpoint to
            # self._endpoint when public_endpoint is None, which would
            # hand the browser the docker-internal hostname.
            public_endpoint=self._public_endpoint,
        )
        clone._forced_bucket = chosen
        return clone

    def _client_ctx(self):
        return self._session.client(
            "s3",
            endpoint_url=self._endpoint,
            aws_access_key_id=self._access,
            aws_secret_access_key=self._secret,
            region_name=self._region,
            verify=self._verify,
            config=_SEAWEED_BOTO_CONFIG,
        )

    def _bucket(self, container: str) -> str:
        return self._forced_bucket or (self._buckets[0] if self._buckets else container)

    def _key(self, container: str, path: str) -> str:
        if self._forced_bucket and container:
            return f"{container}/{path}"
        return path

    async def upload(self, container, path, content, metadata=None, overwrite=True) -> BlobInfo:
        bucket = self._bucket(container)
        key = self._key(container, path)
        try:
            async with self._client_ctx() as s3:
                await s3.put_object(
                    Bucket=bucket, Key=key, Body=content,
                    Metadata=_clean_metadata(metadata or {}),
                )
                head = await s3.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            raise BackendUnreachableError(str(e)) from e
        return BlobInfo(
            backend_id=self.backend_id, container=container, path=path,
            size=head["ContentLength"], etag=head["ETag"].strip('"'),
            url=f"{self._endpoint}/{bucket}/{key}",
            content_md5=None, last_modified=head["LastModified"],
        )

    async def upload_stream(
        self, container, path,
        byte_stream: "AsyncIterator[bytes]",
        total_size: int,
        metadata=None,
        chunk_size: int = 8 * 1024 * 1024,
        max_parallel_parts: Optional[int] = None,
    ) -> BlobInfo:
        """Stream-upload bytes arriving from an async iterator directly
        into an S3 multipart upload — never touches the worker's /tmp.

        Why this exists:
          * The legacy OneDrive backup path downloaded each file to
            /tmp and then uploaded it. At 5k users doing concurrent
            multi-GB file backups that saturated worker local disk.
          * Holding the entire file in Python memory works for small
            files but OOMs on big ones (1 GB file ≈ 1 GB heap).

        Design notes:
          * Up to max_parallel_parts upload_part calls are in flight at
            once. Throughput ≈ (chunk_size × max_parallel_parts) /
            RTT, so 4 × 8 MB / 50 ms ≈ 640 MB/s per file — limited by
            NIC / SeaweedFS ingest rather than serial RTT cost.
          * On ANY part failure the multipart is aborted so we never
            leave dangling parts consuming bucket quota.
          * Supports streams of unknown length — we read until EOF
            and complete the multipart regardless of total_size.
          * Computes sha256 incrementally as bytes flow past so the
            snapshot_items row can be stamped without a re-read.

        `max_parallel_parts` defaults to None → falls back to
        self._upload_concurrency (set by __init__ from the
        ONPREM_UPLOAD_CONCURRENCY env / config field). Previously
        the signature had a hardcoded 4, which silently ignored the
        env knob and capped every backup-worker at 4 concurrent
        multipart segments per file regardless of configuration.
        """
        if max_parallel_parts is None:
            max_parallel_parts = self._upload_concurrency
        import hashlib as _hl
        bucket = self._bucket(container)
        key = self._key(container, path)
        md = _clean_metadata(metadata or {})
        hasher = _hl.sha256()
        total_written = 0

        async with self._client_ctx() as s3:
            try:
                mp = await s3.create_multipart_upload(
                    Bucket=bucket, Key=key, Metadata=md,
                )
                upload_id = mp["UploadId"]
            except ClientError as e:
                raise BackendUnreachableError(str(e)) from e

            parts: List[Dict[str, Any]] = []
            sem = asyncio.Semaphore(max_parallel_parts)
            inflight: List[asyncio.Task] = []
            part_number = 0
            buf = bytearray()

            async def _send_part(pn: int, data: bytes):
                async with sem:
                    resp = await s3.upload_part(
                        Bucket=bucket, Key=key,
                        PartNumber=pn, UploadId=upload_id,
                        Body=data,
                    )
                    return {"PartNumber": pn, "ETag": resp["ETag"]}

            try:
                async for chunk in byte_stream:
                    if not chunk:
                        continue
                    hasher.update(chunk)
                    total_written += len(chunk)
                    buf.extend(chunk)
                    while len(buf) >= chunk_size:
                        part_number += 1
                        part_data = bytes(buf[:chunk_size])
                        del buf[:chunk_size]
                        inflight.append(asyncio.create_task(
                            _send_part(part_number, part_data),
                        ))

                # Flush remainder. S3 requires min 5 MiB per part
                # EXCEPT the last one — any size is fine there.
                if buf or part_number == 0:
                    part_number += 1
                    inflight.append(asyncio.create_task(
                        _send_part(part_number, bytes(buf)),
                    ))

                # Gather all outstanding part uploads. If any failed,
                # cancel the multipart cleanly before re-raising.
                results = await asyncio.gather(*inflight, return_exceptions=True)
                for r in results:
                    if isinstance(r, Exception):
                        raise r
                    parts.append(r)
                parts.sort(key=lambda p: p["PartNumber"])

                await s3.complete_multipart_upload(
                    Bucket=bucket, Key=key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts},
                )
            except Exception:
                # Defensive: abort the multipart so parts uploaded
                # so far are garbage-collected by SeaweedFS / S3.
                try:
                    await s3.abort_multipart_upload(
                        Bucket=bucket, Key=key, UploadId=upload_id,
                    )
                except Exception:
                    pass
                raise

            head = await s3.head_object(Bucket=bucket, Key=key)

        # Stash the sha256 we computed on the stream onto BlobInfo so
        # callers don't re-hash by reading the object back. BlobInfo's
        # content_md5 field is reused as the "content hash" slot for
        # SeaweedStore since there's no separate sha field on the
        # dataclass and downstream code accesses it as a hex string.
        return BlobInfo(
            backend_id=self.backend_id, container=container, path=path,
            size=head["ContentLength"],
            etag=head["ETag"].strip('"'),
            url=f"{self._endpoint}/{bucket}/{key}",
            content_md5=hasher.hexdigest(),
            last_modified=head["LastModified"],
        )

    async def upload_from_file(self, container, path, file_path, size,
                               metadata=None, overwrite=True) -> BlobInfo:
        bucket = self._bucket(container)
        key = self._key(container, path)
        try:
            async with self._client_ctx() as s3:
                with open(file_path, "rb") as f:
                    await s3.upload_fileobj(
                        f, bucket, key,
                        ExtraArgs={"Metadata": _clean_metadata(metadata or {})},
                    )
                head = await s3.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            raise BackendUnreachableError(str(e)) from e
        return BlobInfo(
            backend_id=self.backend_id, container=container, path=path,
            size=head["ContentLength"], etag=head["ETag"].strip('"'),
            url=f"{self._endpoint}/{bucket}/{key}",
            content_md5=None, last_modified=head["LastModified"],
        )

    async def download(self, container, path) -> Optional[bytes]:
        bucket, key = self._bucket(container), self._key(container, path)
        try:
            async with self._client_ctx() as s3:
                obj = await s3.get_object(Bucket=bucket, Key=key)
                return await obj["Body"].read()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404", "NoSuchBucket"):
                return None
            raise BackendUnreachableError(str(e)) from e

    async def download_stream(self, container, path, chunk_size=4 * 1024 * 1024):
        bucket, key = self._bucket(container), self._key(container, path)
        async with self._client_ctx() as s3:
            try:
                obj = await s3.get_object(Bucket=bucket, Key=key)
            except ClientError as e:
                if e.response["Error"]["Code"] in ("NoSuchKey", "404", "NoSuchBucket"):
                    return
                raise BackendUnreachableError(str(e)) from e
            async for chunk in obj["Body"].iter_chunks(chunk_size):
                yield chunk

    async def stage_block(self, container, path, block_id, data) -> None:
        # SeaweedFS uses S3 multipart (initiate+upload_part+complete), not
        # Azure-style stage_block. Callers must use an explicit multipart
        # helper instead. See plan §5.3 for rationale.
        raise NotImplementedError(
            "SeaweedStore does not support Azure-style stage_block; use an "
            "S3 multipart upload helper instead",
        )

    async def commit_blocks(self, container, path, block_ids, metadata=None) -> None:
        raise NotImplementedError(
            "SeaweedStore does not support Azure-style commit_blocks; use "
            "complete_multipart_upload via an S3 multipart upload helper",
        )

    async def put_block_from_url(self, container, path, block_id, source_url) -> None:
        raise NotImplementedError(
            "put_block_from_url is Azure-specific; use S3 upload_part_copy "
            "inside an explicit multipart session",
        )

    async def server_side_copy(self, source_url, container, path, size,
                               metadata=None) -> BlobInfo:
        bucket, key = self._bucket(container), self._key(container, path)
        if not source_url.startswith(self._endpoint):
            raise NotImplementedError(
                "cross-backend server-side copy not supported — stream via worker",
            )
        try:
            async with self._client_ctx() as s3:
                await s3.copy_object(
                    Bucket=bucket, Key=key, CopySource=source_url,
                    Metadata=_clean_metadata(metadata or {}),
                    MetadataDirective="REPLACE",
                )
                head = await s3.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            raise BackendUnreachableError(str(e)) from e
        return BlobInfo(
            backend_id=self.backend_id, container=container, path=path,
            size=head["ContentLength"], etag=head["ETag"].strip('"'),
            url=f"{self._endpoint}/{bucket}/{key}",
            content_md5=None, last_modified=head["LastModified"],
        )

    async def list_blobs(self, container, prefix: Optional[str] = None):
        bucket = self._bucket(container)
        base = f"{container}/" if self._forced_bucket and container else ""
        full_prefix = (base + prefix) if prefix else base
        async with self._client_ctx() as s3:
            paginator = s3.get_paginator("list_objects_v2")
            async for page in paginator.paginate(Bucket=bucket, Prefix=full_prefix):
                for obj in page.get("Contents", []):
                    yield obj["Key"][len(base):] if base else obj["Key"]

    async def list_with_props(self, container, prefix: Optional[str] = None):
        bucket = self._bucket(container)
        base = f"{container}/" if self._forced_bucket and container else ""
        full_prefix = (base + prefix) if prefix else base
        async with self._client_ctx() as s3:
            paginator = s3.get_paginator("list_objects_v2")
            async for page in paginator.paginate(Bucket=bucket, Prefix=full_prefix):
                for obj in page.get("Contents", []):
                    name = obj["Key"][len(base):] if base else obj["Key"]
                    yield name, BlobProps(
                        size=obj["Size"], content_type=None,
                        last_modified=obj["LastModified"], metadata={},
                        copy_status=None, copy_progress=None,
                        retention_until=None, legal_hold=False,
                    )

    async def get_properties(self, container, path) -> Optional[BlobProps]:
        bucket, key = self._bucket(container), self._key(container, path)
        try:
            async with self._client_ctx() as s3:
                head = await s3.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404", "NoSuchBucket"):
                return None
            raise BackendUnreachableError(str(e)) from e
        return BlobProps(
            size=head["ContentLength"],
            content_type=head.get("ContentType"),
            last_modified=head["LastModified"],
            metadata=head.get("Metadata", {}),
            copy_status=None, copy_progress=None,
            retention_until=head.get("ObjectLockRetainUntilDate"),
            legal_hold=head.get("ObjectLockLegalHoldStatus") == "ON",
        )

    async def delete(self, container, path) -> None:
        bucket, key = self._bucket(container), self._key(container, path)
        try:
            async with self._client_ctx() as s3:
                await s3.delete_object(Bucket=bucket, Key=key)
        except ClientError as e:
            code = e.response["Error"]["Code"]
            msg = (e.response["Error"].get("Message") or "").lower()
            if code in ("AccessDenied", "InvalidRequest") and (
                "worm" in msg or "retention" in msg or "object lock" in msg
            ):
                raise ImmutableBlobError(str(e)) from e
            if code in ("NoSuchKey", "404"):
                return
            raise BackendUnreachableError(str(e)) from e

    async def presigned_url(self, container, path, valid_hours=6) -> str:
        bucket, key = self._bucket(container), self._key(container, path)
        async with self._client_ctx() as s3:
            url = await s3.generate_presigned_url(
                "get_object", Params={"Bucket": bucket, "Key": key},
                ExpiresIn=valid_hours * 3600,
            )
        # boto3 builds the URL with the client's endpoint
        # (self._endpoint — docker-internal hostname). Browsers can't
        # resolve `seaweedfs`, so we swap the signed-URL host for the
        # configured public endpoint. The path-style key + query
        # parameters (Signature, Expires, AWSAccessKeyId) are preserved,
        # which keeps the SigV2 signature valid because SeaweedFS signs
        # over the path + query, not the hostname.
        if self._public_endpoint and self._public_endpoint != self._endpoint.rstrip("/"):
            from urllib.parse import urlsplit, urlunsplit
            pub = urlsplit(self._public_endpoint)
            orig = urlsplit(url)
            url = urlunsplit((
                pub.scheme or orig.scheme,
                pub.netloc or orig.netloc,
                orig.path, orig.query, orig.fragment,
            ))
        return url

    async def apply_immutability(self, container, path, until, mode="Unlocked") -> None:
        bucket, key = self._bucket(container), self._key(container, path)
        s3_mode = _MODE_TMVAULT_TO_S3.get(mode, "GOVERNANCE")
        try:
            async with self._client_ctx() as s3:
                await s3.put_object_retention(
                    Bucket=bucket, Key=key,
                    Retention={"Mode": s3_mode, "RetainUntilDate": until},
                )
        except ClientError as e:
            raise BackendUnreachableError(str(e)) from e

    async def apply_legal_hold(self, container, path, tag="tmvault-legal-hold") -> None:
        bucket, key = self._bucket(container), self._key(container, path)
        async with self._client_ctx() as s3:
            await s3.put_object_legal_hold(
                Bucket=bucket, Key=key, LegalHold={"Status": "ON"},
            )

    async def remove_legal_hold(self, container, path) -> None:
        bucket, key = self._bucket(container), self._key(container, path)
        async with self._client_ctx() as s3:
            await s3.put_object_legal_hold(
                Bucket=bucket, Key=key, LegalHold={"Status": "OFF"},
            )

    async def apply_lifecycle(self, container, hot_days, cool_days, archive_days=None) -> None:
        bucket = self._bucket(container)
        prefix = f"{container}/" if self._forced_bucket and container else ""
        rules = [{
            "ID": f"tier-cool-{hot_days}d",
            "Status": "Enabled",
            "Filter": {"Prefix": prefix},
            "Transitions": [{"Days": hot_days, "StorageClass": "STANDARD_IA"}],
        }]
        if archive_days:
            rules.append({
                "ID": f"expire-{archive_days}d",
                "Status": "Enabled",
                "Filter": {"Prefix": prefix},
                "Expiration": {"Days": hot_days + cool_days + archive_days},
            })
        async with self._client_ctx() as s3:
            await s3.put_bucket_lifecycle_configuration(
                Bucket=bucket, LifecycleConfiguration={"Rules": rules},
            )

    async def ensure_container(self, container) -> None:
        # Bucket must be pre-created with Object Lock enabled; this is a
        # no-op in the default bucket-per-shard layout.
        return

    async def close(self) -> None:
        return


def _clean_metadata(metadata: dict) -> dict:
    clean = {}
    for k, v in metadata.items():
        ks = str(k).encode("ascii", errors="replace").decode("ascii")
        vs = str(v).encode("ascii", errors="replace").decode("ascii")
        clean[ks] = vs
    return clean
