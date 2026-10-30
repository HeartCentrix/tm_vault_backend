"""Explicit-backend catalogue uploads, with durable fenced object ownership."""
from __future__ import annotations

import asyncio
from datetime import timedelta
import inspect
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError

from shared.chat_catalogue import ChatCatalogueConflict, renew_capture
from shared.chat_catalogue_identity import ChatContentDescriptor
from shared.models import ChatBlobObject
from shared.db_retry import run_catalogue_transaction


UPLOAD_TIMEOUT_SECONDS = 300
UPLOAD_QUIESCENCE_SECONDS = 300


class ChatStorageCaptureError(RuntimeError):
    """Required content is not verified durable in its selected backend."""


def catalogue_container(tenant_id):
    return "chat-catalogue-" + str(tenant_id).replace("-", "")[:8]


async def _retry_pause(attempt):
    await asyncio.sleep(2 ** attempt)


async def _stored_encryption(store, container, path):
    if store.kind != "seaweedfs":
        # AzureBlobStore uses backend-native SSE; TMVE is a Seaweed transform.
        return None, 1
    from shared.crypto import envelope, runtime
    async with store._client_ctx() as client:
        response = await client.get_object(Bucket=store._bucket(container),
            Key=store._key(container, path), Range="bytes=0-36")
        body = response["Body"]
        try:
            header = await body.read(37)
        finally:
            closed = body.close()
            if inspect.isawaitable(closed):
                await closed
    if envelope.is_encrypted(header):
        return str(UUID(bytes=envelope.read_key_id(header))), int(header[4])
    if runtime.is_write_enabled():
        raise ChatStorageCaptureError("required encrypted catalogue object was stored without its envelope")
    return None, 1


async def verify_catalogue_content(storage, content):
    """Probe the exact recorded store; ownership must already protect content."""
    if content.object_id is None:
        return
    store = storage.get_store_by_id(str(content.backend_id))
    props = await store.get_properties(content.container, content.path)
    if props is None or (content.encryption_key_id is None and props.size != content.size):
        raise ChatStorageCaptureError("retained catalogue content is unavailable or changed length")


async def upload_capture_content(*, scope, lease, spooled, storage, session_factory,
                                 source_drive_id=None, source_item_id=None,
                                 source_etag=None, source_ctag=None):
    """Each uncertain upload retry gets a fresh unpublished object generation."""
    store = storage.get_store_by_id(str(scope.backend_id))
    if str(store.backend_id) != str(scope.backend_id):
        raise ChatStorageCaptureError("selected catalogue backend differs from capture scope")
    container = catalogue_container(scope.tenant_id)
    await store.ensure_container(container)
    for retry in range(3):
        object_id = uuid4()
        path = f"chat-catalogue/v1/{scope.tenant_id}/{object_id}"
        async def reserve(session):
            await renew_capture(session, lease=lease)
            now = await session.scalar(select(func.clock_timestamp()))
            deadline = now + timedelta(seconds=UPLOAD_TIMEOUT_SECONDS)
            session.add(ChatBlobObject(id=object_id, tenant_id=scope.tenant_id,
                backend_id=scope.backend_id, container=container, path=path,
                sha256=spooled.sha256, size=spooled.size, state="STAGING",
                capture_attempt_id=lease.attempt_id, upload_deadline=deadline,
                quiescent_after=deadline + timedelta(seconds=UPLOAD_QUIESCENCE_SECONDS)))
        await run_catalogue_transaction(session_factory, reserve)
        try:
            metadata = {"tenant_id": str(scope.tenant_id), "sha256": spooled.sha256,
                        "plaintext_size": str(spooled.size), "chat_catalogue_format": "1"}
            kwargs = {"metadata": metadata, "overwrite": False}
            if store.kind == "seaweedfs":
                # This keyword activates the existing TMVE stream transform.
                kwargs["tenant_id"] = str(scope.tenant_id)
            info = await asyncio.wait_for(store.upload_from_file(
                container, path, spooled.path, spooled.size, **kwargs), UPLOAD_TIMEOUT_SECONDS)
            if (str(info.backend_id), info.container, info.path) != (str(scope.backend_id), container, path):
                raise ChatStorageCaptureError("upload receipt differs from the owned object location")
            key_id, frame_version = await _stored_encryption(store, container, path)
            descriptor = ChatContentDescriptor(object_id, scope.backend_id, container,
                path, spooled.sha256, spooled.size, key_id, frame_version, None,
                source_drive_id, source_item_id, source_etag, source_ctag)
            await verify_catalogue_content(storage, descriptor)
            if key_id is not None:
                from shared.crypto import runtime
                if runtime.verify_readback_enabled():
                    import hashlib
                    digest, size = hashlib.sha256(), 0
                    async for chunk in store.download_stream(container, path):
                        digest.update(chunk)
                        size += len(chunk)
                    if size != spooled.size or digest.hexdigest() != spooled.sha256:
                        raise ChatStorageCaptureError("encrypted catalogue readback did not match captured bytes")
            async def make_live(session):
                await renew_capture(session, lease=lease)
                obj = await session.scalar(select(ChatBlobObject).where(ChatBlobObject.id == object_id)
                    .with_for_update().execution_options(populate_existing=True))
                now = await session.scalar(select(func.clock_timestamp()))
                if obj is None or obj.state != "STAGING" or obj.capture_attempt_id != lease.attempt_id or now > obj.upload_deadline:
                    raise ChatCatalogueConflict("catalogue upload generation is no longer publishable")
                obj.encryption_key_id, obj.backup_version = key_id, frame_version
                obj.uploaded_bytes = spooled.size
                obj.state = "LIVE"
            await run_catalogue_transaction(session_factory, make_live)
            return descriptor
        except (asyncio.CancelledError, ChatCatalogueConflict, SQLAlchemyError):
            raise
        except Exception:
            # Retain the owned STAGING generation and quiescence window. An
            # uncertain remote write may still finish after local cancellation.
            if retry == 2:
                raise ChatStorageCaptureError("catalogue upload could not establish durable content") from None
            await _retry_pause(retry)
    raise ChatStorageCaptureError("catalogue upload retries exhausted")
