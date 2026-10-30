"""Resource cutover and bounded coordination for exact chat inventories."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import hashlib
import json
from uuid import UUID

from sqlalchemy import func, or_, select

from shared.chat_catalogue import ChatCatalogueConflict, ChatCatalogueIntegrityError, ChatCatalogueWorkLeaseMoved
from shared.chat_catalogue_identity import CaptureResult, ChatCaptureScope, authentication_context_fingerprint
from shared.db_retry import run_catalogue_transaction
from shared.models import (ChatCaptureAttempt, ChatInventory, Job, JobStatus, Resource, ResourceType, SlaPolicy,
    Snapshot, SnapshotChatInventory, SnapshotItem, SnapshotPartition, SnapshotStatus, SnapshotType)


class ChatCatalogueAdoptionError(RuntimeError):
    """A legacy capture cannot safely enter the immutable writer."""


class ChatCatalogueSuspended(ChatCatalogueAdoptionError):
    retryable = False


class ChatCatalogueProtectionUnsupported(ChatCatalogueAdoptionError):
    retryable = False


async def lock_chat_catalogue_snapshot(session, *, snapshot_id, snapshot_lease_token=None,
        partition_id=None, partition_lease_token=None, job_lease_token=None):
    """Fence settlement with the same original work tokens used for capture."""
    hint = await session.get(Snapshot, snapshot_id)
    if hint is None:
        raise ChatCatalogueWorkLeaseMoved("capture snapshot was removed")
    job = await session.scalar(select(Job).where(Job.id == hint.job_id).with_for_update(read=True)
        .execution_options(populate_existing=True))
    snapshot = await session.scalar(select(Snapshot).where(Snapshot.id == snapshot_id)
        .with_for_update().execution_options(populate_existing=True))
    if snapshot is None or (snapshot.extra_data or {}).get("chat_inventory_format") != 1:
        raise ChatCatalogueWorkLeaseMoved("capture snapshot format or ownership changed")
    if snapshot.status in {SnapshotStatus.COMPLETED, SnapshotStatus.PARTIAL, SnapshotStatus.FAILED}:
        return snapshot
    if snapshot.status != SnapshotStatus.IN_PROGRESS or job is None or job.status not in {JobStatus.RUNNING, JobStatus.RETRYING}:
        raise ChatCatalogueWorkLeaseMoved("capture snapshot or parent job is no longer active")
    if partition_id is not None:
        partition = await session.scalar(select(SnapshotPartition).where(SnapshotPartition.id == partition_id)
            .with_for_update(read=True).execution_options(populate_existing=True))
        if (partition is None or partition.snapshot_id != snapshot_id or partition.resource_id != snapshot.resource_id or
                partition.status != "IN_PROGRESS" or partition_lease_token is None or
                int(partition.lease_token or 0) != partition_lease_token):
            raise ChatCatalogueWorkLeaseMoved("capture partition lease changed before settlement")
    elif ((snapshot_lease_token is None or int(snapshot.lease_token or 0) != snapshot_lease_token) or
            (job_lease_token is None or int(job.lease_token or 0) != job_lease_token)):
        raise ChatCatalogueWorkLeaseMoved("capture parent lease changed before settlement")
    return snapshot


async def adopt_chat_catalogue(session, *, resource_id, snapshot_id, writer_enabled,
                               backend_id=None, partition_id=None, policy_id=None,
                               snapshot_lease_token=None, partition_lease_token=None, job_lease_token=None):
    """Fence every chat entry, including disabled legacy callers, before I/O."""
    hint = await session.get(Snapshot, snapshot_id)
    if hint is None or hint.resource_id != resource_id:
        raise ChatCatalogueAdoptionError("capture snapshot no longer belongs to the resource")
    # Match catalogue lock order: job -> snapshot -> resource. An opposite
    # resource/snapshot order can deadlock with a live capture's shared locks.
    job = await session.scalar(select(Job).where(Job.id == hint.job_id).with_for_update(read=True))
    snapshot = await session.scalar(select(Snapshot).where(Snapshot.id == snapshot_id)
        .with_for_update().execution_options(populate_existing=True))
    resource = await session.scalar(select(Resource).where(Resource.id == resource_id)
        .with_for_update().execution_options(populate_existing=True))
    if resource is None or snapshot is None or resource.type != ResourceType.USER_CHATS:
        raise ChatCatalogueAdoptionError("capture resource is missing or is not USER_CHATS")
    resource_format = (resource.extra_data or {}).get("chat_inventory_format")
    snapshot_format = (snapshot.extra_data or {}).get("chat_inventory_format")
    if snapshot_format == 1 and snapshot.status in {SnapshotStatus.COMPLETED, SnapshotStatus.PARTIAL, SnapshotStatus.FAILED}:
        return True
    if not writer_enabled:
        if resource_format == 1 or snapshot_format == 1:
            raise ChatCatalogueSuspended("catalogue writer is disabled for an adopted chat resource")
        if snapshot.status == SnapshotStatus.IN_PROGRESS:
            # Reserve this attempt before releasing the gate. Otherwise a
            # concurrent flag toggle could relabel this still-empty snapshot
            # while the approved legacy caller is about to write to it.
            snapshot.extra_data = {**(snapshot.extra_data or {}), "chat_legacy_writer_entered": True}
            await session.flush()
        return False
    if (job is None or job.tenant_id != resource.tenant_id or
            job.status not in {JobStatus.RUNNING, JobStatus.RETRYING} or
            snapshot.status != SnapshotStatus.IN_PROGRESS or resource.archived_at is not None or
            resource.deletion_queued_at is not None or str(getattr(resource.status, "value", resource.status)) == "PENDING_DELETION"):
        raise ChatCatalogueAdoptionError("capture snapshot, job or resource is no longer eligible")
    effective_policy = policy_id or resource.sla_policy_id
    policy = await session.scalar(select(SlaPolicy).where(SlaPolicy.id == effective_policy)
        .with_for_update(read=True)) if effective_policy else None
    if policy is not None and (policy.legal_hold_enabled or str(policy.immutability_mode or "None").lower() != "none" or
            str(policy.encryption_mode or "VAULT_MANAGED").upper() == "CUSTOMER_KEY"):
        raise ChatCatalogueProtectionUnsupported("catalogue capture does not yet support native legal hold, immutability or CUSTOMER_KEY policy")
    if snapshot_format != 1:
        other = await session.scalar(select(Snapshot.id).where(Snapshot.resource_id == resource_id,
            Snapshot.id != snapshot_id, Snapshot.status == SnapshotStatus.IN_PROGRESS,
            or_(Snapshot.extra_data["chat_inventory_format"].as_integer().is_(None),
                Snapshot.extra_data["chat_inventory_format"].as_integer() != 1)).limit(1))
        items = await session.scalar(select(SnapshotItem.id).where(SnapshotItem.snapshot_id == snapshot_id).limit(1))
        partitions = await session.scalar(select(SnapshotPartition.id).where(SnapshotPartition.snapshot_id == snapshot_id).limit(1))
        if other is not None or items is not None or partitions is not None or (snapshot.extra_data or {}).get("chat_legacy_writer_entered"):
            raise ChatCatalogueAdoptionError("legacy snapshot items, partitions or another legacy run prevent catalogue adoption")
    if partition_id is not None:
        partition = await session.scalar(select(SnapshotPartition).where(SnapshotPartition.id == partition_id)
            .with_for_update(read=True))
        if (partition is None or partition.snapshot_id != snapshot_id or partition.resource_id != resource_id or
                partition.tenant_id != resource.tenant_id or partition.partition_type != "CHATS" or
                partition.status != "IN_PROGRESS"):
            raise ChatCatalogueAdoptionError("capture partition is not an eligible CHATS partition")
        if partition_lease_token is None or int(partition.lease_token or 0) != partition_lease_token:
            raise ChatCatalogueWorkLeaseMoved("capture partition lease token is unavailable or changed")
    elif ((snapshot_lease_token is not None and int(snapshot.lease_token or 0) != snapshot_lease_token) or
            (job_lease_token is not None and int(job.lease_token or 0) != job_lease_token)):
        raise ChatCatalogueWorkLeaseMoved("capture parent work lease token changed")
    if snapshot.backend_id is None:
        if backend_id is None:
            raise ChatCatalogueAdoptionError("catalogue capture requires a persisted storage backend")
        snapshot.backend_id = UUID(str(backend_id))
    if resource_format != 1:
        resource.extra_data = {**(resource.extra_data or {}), "chat_inventory_format": 1}
    if partition_id is None:
        prior_catalogue = await session.scalar(select(Snapshot.id).where(
            Snapshot.resource_id == resource_id, Snapshot.id != snapshot_id,
            Snapshot.extra_data["chat_inventory_format"].as_integer() == 1,
            Snapshot.status.in_([SnapshotStatus.COMPLETED, SnapshotStatus.PARTIAL])).limit(1))
        if prior_catalogue is None:
            snapshot.type = SnapshotType.FULL
    snapshot.extra_data = {**(snapshot.extra_data or {}), "chat_inventory_format": 1}
    await session.flush()
    return True


async def catalogue_snapshot_result(session, snapshot_id, *, force_recount=False):
    """Count narrow physical links; current versions never need history payloads."""
    snapshot = await session.get(Snapshot, snapshot_id)
    if snapshot is None:
        raise ChatCatalogueConflict("capture snapshot was deleted")
    extra = snapshot.extra_data or {}
    if snapshot.status != SnapshotStatus.IN_PROGRESS and not force_recount:
        # Historical delta is an outcome of that run, not of whichever
        # previous restore points retention still happens to keep today.
        return {"chat_inventory_format": 1, "item_count": int(snapshot.item_count or 0),
            "bytes_total": int(snapshot.bytes_total or 0), "new_item_count": int(snapshot.new_item_count or 0),
            "bytes_added": int(snapshot.bytes_added or 0), "uploaded_bytes": int(extra.get("catalogue_uploaded_bytes") or 0),
            "partitioned": bool(extra.get("partitioned")), "status": snapshot.status.value, "resumed_terminal": True,
            "outcome": snapshot.status.value.lower(), "failure_reason": extra.get("failure_reason") or extra.get("error"),
            "failed": snapshot.status == SnapshotStatus.FAILED}
    primary = SnapshotItem.item_type.notin_(["CHAT_ATTACHMENT", "CHAT_HOSTED_CONTENT"])
    count, logical_bytes = (await session.execute(select(func.count().filter(primary),
        func.coalesce(func.sum(SnapshotItem.content_size), 0)).where(SnapshotItem.snapshot_id == snapshot_id))).one()
    prior = await session.scalar(select(Snapshot.id).where(Snapshot.resource_id == snapshot.resource_id,
        Snapshot.id != snapshot_id, Snapshot.job_id != snapshot.job_id,
        Snapshot.status.in_([SnapshotStatus.COMPLETED, SnapshotStatus.PARTIAL]),
        Snapshot.created_at <= snapshot.created_at).order_by(Snapshot.created_at.desc(), Snapshot.id.desc()).limit(1))
    if prior is None:
        new_count, new_bytes = count, logical_bytes
    else:
        prior_versions = select(SnapshotItem.chat_version_id).where(SnapshotItem.snapshot_id == prior,
            SnapshotItem.chat_version_id.is_not(None))
        new_count, new_bytes = (await session.execute(select(func.count().filter(primary),
            func.coalesce(func.sum(SnapshotItem.content_size), 0)).where(SnapshotItem.snapshot_id == snapshot_id,
                SnapshotItem.chat_version_id.notin_(prior_versions)))).one()
    uploaded = await session.scalar(select(func.coalesce(func.sum(SnapshotChatInventory.uploaded_bytes), 0))
        .where(SnapshotChatInventory.snapshot_id == snapshot_id)) or 0
    return {"chat_inventory_format": 1, "item_count": int(count), "bytes_total": int(logical_bytes),
        "new_item_count": int(new_count), "bytes_added": int(new_bytes), "uploaded_bytes": int(uploaded),
        "partitioned": bool(extra.get("partitioned")), "status": snapshot.status.value,
        "resumed_terminal": snapshot.status != SnapshotStatus.IN_PROGRESS, "outcome": snapshot.status.value.lower(),
        "failure_reason": extra.get("failure_reason") or extra.get("error"),
        "failed": snapshot.status == SnapshotStatus.FAILED}


async def _published_chat(session, scope):
    row = (await session.execute(select(SnapshotChatInventory, ChatInventory, ChatCaptureAttempt)
        .join(ChatInventory, ChatInventory.id == SnapshotChatInventory.inventory_id)
        .join(ChatCaptureAttempt, ChatCaptureAttempt.id == SnapshotChatInventory.capture_attempt_id)
        .where(SnapshotChatInventory.snapshot_id == scope.snapshot_id,
            SnapshotChatInventory.chat_id == scope.chat_id))).one_or_none()
    if row is None:
        return None
    association, inventory, attempt = row
    if inventory.authentication_context_sha256 != authentication_context_fingerprint(scope):
        raise ChatCatalogueConflict("capture configuration changed after a chat inventory was published")
    return CaptureResult("complete" if association.coverage_status == "COMPLETE" else "partial",
        inventory.id, bool(attempt.reused_inventory), inventory.item_count, inventory.logical_bytes,
        association.uploaded_bytes, association.started_at, association.finished_at, association.failure_reason)


def _folder(chat):
    topic = chat.get("topic")
    display = topic.strip() if isinstance(topic, str) and topic.strip() else (
        "Chat " + hashlib.sha256(chat["id"].encode()).hexdigest()[:8])
    return "chats/" + display


async def _chat_list(source):
    chats = {}
    async for chat in source.iter_chats():
        cid = chat.get("id") if isinstance(chat, dict) else None
        if not isinstance(cid, str) or not cid:
            raise ChatCatalogueIntegrityError("chat listing contains an invalid source identity")
        observed = {"id": cid, "topic": chat.get("topic"), "chatType": chat.get("chatType")}
        if cid in chats and chats[cid] != observed:
            raise ChatCatalogueConflict("chat listing changed while it was being enumerated")
        chats[cid] = observed
    return chats


async def run_chat_catalogue_resource(*, resource_id, snapshot_id, writer_enabled, graph_client,
        session_factory, storage, partition_filter=None, partition_id=None, policy_id=None,
        exclude_message=None, scope_filter_hash=None, check_cancelled=None, concurrency=1,
        snapshot_lease_token=None, partition_lease_token=None, job_lease_token=None):
    """Return None only for an eligible disabled legacy path; never downgrade."""
    from shared.chat_capture import capture_chat_inventory
    from shared.chat_capture_graph import pinned_chat_source, effective_capture_scope_hash
    if type(concurrency) is not int or concurrency < 1:
        raise ValueError("chat capture concurrency must be positive")
    if exclude_message is not None and not scope_filter_hash:
        raise ValueError("chat exclusions require a stable configured scope hash")
    async def cancel_check():
        if check_cancelled is not None:
            await check_cancelled()
    await cancel_check()
    async def prepare(session):
        return await adopt_chat_catalogue(session, resource_id=resource_id, snapshot_id=snapshot_id,
            writer_enabled=writer_enabled, backend_id=storage.active_backend_id(),
            partition_id=partition_id, policy_id=policy_id, snapshot_lease_token=snapshot_lease_token,
            partition_lease_token=partition_lease_token, job_lease_token=job_lease_token)
    if not await run_catalogue_transaction(session_factory, prepare):
        return None
    async with session_factory() as session:
        snapshot = await session.get(Snapshot, snapshot_id)
        resource = await session.get(Resource, resource_id)
        if snapshot.status != SnapshotStatus.IN_PROGRESS:
            return await catalogue_snapshot_result(session, snapshot_id)
        principal = (resource.extra_data or {}).get("user_id") or resource.external_id
        tenant_id, backend_id = resource.tenant_id, snapshot.backend_id
        if partition_id is None:
            # Capture omitted parent tokens ONCE before source work; later
            # finalizers must not read a replacement owner's token.
            if snapshot_lease_token is None:
                snapshot_lease_token = int(snapshot.lease_token or 0)
            if job_lease_token is None:
                job_lease_token = int((await session.get(Job, snapshot.job_id)).lease_token or 0)
        published = set((await session.scalars(select(SnapshotChatInventory.chat_id)
            .where(SnapshotChatInventory.snapshot_id == snapshot_id))).all())
        allowed = None
        if partition_filter is not None:
            if not isinstance(partition_filter, dict) or "chatIds" not in partition_filter:
                raise ChatCatalogueAdoptionError("chat partition scope has no chatIds allowlist")
            ids = partition_filter["chatIds"]
            if not isinstance(ids, list) or any(not isinstance(cid, str) or not cid for cid in ids):
                raise ChatCatalogueAdoptionError("chat partition allowlist contains invalid source identities")
            allowed = set(ids)
        if partition_id is not None:
            partition = await session.get(SnapshotPartition, partition_id)
            if allowed != set((partition.payload or {}).get("chat_ids") or []):
                raise ChatCatalogueAdoptionError("chat partition allowlist differs from its claimed scope")
    started = datetime.now(timezone.utc)
    errors, completed, planned, selected = [], 0, 0, set()
    try:
        async with pinned_chat_source(graph_client, principal_id=principal) as source:
            chats = await _chat_list(source)
            selected = set(chats) if allowed is None else set(chats) & allowed
            planned = len(chats) if allowed is None else len(allowed)
            if allowed is not None and allowed - set(chats):
                errors.append("some planned chats are no longer visible to the capture principal")
            if published - selected and allowed is None:
                errors.append("chat membership changed across capture attempts")
            keys = sorted(selected)
            effective_hash = effective_capture_scope_hash(source.permission_scope_hash, scope_filter_hash)
            def chat_scope(cid):
                return ChatCaptureScope(snapshot_id, resource_id, tenant_id, cid, principal,
                    effective_hash, backend_id, partition_id, snapshot_lease_token=snapshot_lease_token,
                    partition_lease_token=partition_lease_token, job_lease_token=job_lease_token)
            async def capture(cid):
                await cancel_check()
                scope = chat_scope(cid)
                async with session_factory() as session:
                    previous = await _published_chat(session, scope)
                if previous is not None:
                    return previous
                return await capture_chat_inventory(scope=scope, graph_client=source, storage=storage,
                    session_factory=session_factory, folder_path=_folder(chats[cid]),
                    exclude_message=exclude_message, scope_filter_hash=scope_filter_hash)
            remaining = iter(keys)
            async def worker():
                nonlocal completed
                for cid in remaining:
                    await cancel_check()
                    try:
                        result = await capture(cid)
                    except (Exception, asyncio.CancelledError) as exc:
                        result = exc
                    await cancel_check()
                    if isinstance(result, asyncio.CancelledError):
                        raise result
                    if isinstance(result, ChatCatalogueWorkLeaseMoved):
                        raise result
                    if isinstance(result, ChatCatalogueConflict):
                        async with session_factory() as session:
                            try:
                                previous = await _published_chat(session, chat_scope(cid))
                            except ChatCatalogueConflict:
                                previous = None
                            if previous is not None:
                                result = previous
                            else:
                                active = await session.scalar(select(ChatCaptureAttempt.id).where(
                                    ChatCaptureAttempt.snapshot_id == snapshot_id, ChatCaptureAttempt.chat_id == cid,
                                    ChatCaptureAttempt.state.in_(["CREATED", "ENUMERATING", "CONTENT_PENDING", "VERIFYING", "READY"]),
                                    ChatCaptureAttempt.expires_at > func.clock_timestamp()).limit(1))
                                if active is not None:
                                    raise ChatCatalogueWorkLeaseMoved("another live capture owns this chat; deferring finalization")
                    if isinstance(result, BaseException):
                        errors.append(f"chat capture did not complete ({type(result).__name__})")
                    elif result.outcome == "complete":
                        completed += 1
                    else:
                        errors.append(result.failure_reason or "chat capture has incomplete coverage")
            workers = [asyncio.create_task(worker()) for _ in range(min(concurrency, len(keys)))]
            try:
                await asyncio.gather(*workers)
            finally:
                for task in workers:
                    task.cancel()
                await asyncio.gather(*workers, return_exceptions=True)
            final_chats = set(await _chat_list(source))
            if (allowed is None and final_chats != set(chats)) or (allowed is not None and allowed - final_chats):
                errors.append("chat visibility changed during resource capture")
    except asyncio.CancelledError:
        raise
    except ChatCatalogueWorkLeaseMoved:
        raise
    except Exception as exc:
        await cancel_check()
        errors.append(f"chat source enumeration did not complete ({type(exc).__name__})")
    await cancel_check()
    finished = datetime.now(timezone.utc)
    async def finish(session):
        snapshot = await lock_chat_catalogue_snapshot(session, snapshot_id=snapshot_id,
            snapshot_lease_token=snapshot_lease_token, partition_id=partition_id,
            partition_lease_token=partition_lease_token, job_lease_token=job_lease_token)
        result = await catalogue_snapshot_result(session, snapshot_id)
        if snapshot.status != SnapshotStatus.IN_PROGRESS:
            return result
        known = await session.scalar(select(func.count()).select_from(SnapshotChatInventory)
            .where(SnapshotChatInventory.snapshot_id == snapshot_id)) or 0
        complete = not errors and completed == planned
        outcome = "complete" if complete else ("partial" if known or result["item_count"] else "failed")
        reason = "; ".join(dict.fromkeys(errors))[:1500] if errors else None
        record = {"complete": complete, "planned_chats": planned, "completed_chats": completed,
            "selection_sha256": hashlib.sha256(json.dumps(sorted(selected), separators=(",", ":")).encode()).hexdigest(),
            "filter_sha256": scope_filter_hash, "started_at": started.isoformat(), "finished_at": finished.isoformat(),
            "failure_reason": reason}
        key = str(partition_id) if partition_id is not None else "resource"
        metadata = dict(snapshot.extra_data or {})
        metadata["chat_catalogue_scopes"] = {**metadata.get("chat_catalogue_scopes", {}), key: record}
        metadata["catalogue_uploaded_bytes"] = result["uploaded_bytes"]
        snapshot.extra_data = metadata
        await session.flush()
        return {**result, "outcome": outcome, "failure_reason": reason, "failed": outcome == "failed",
            "_snapshot_lease_token": snapshot_lease_token, "_job_lease_token": job_lease_token,
            "_partition_lease_token": partition_lease_token}
    return await run_catalogue_transaction(session_factory, finish)
