"""Resource cutover and bounded coordination for exact chat inventories."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
import os
import time
from uuid import UUID

from sqlalchemy import Text, cast, func, or_, select, text
from sqlalchemy.dialects.postgresql import JSONB

from shared.chat_catalogue import ChatCatalogueConflict, ChatCatalogueIntegrityError, ChatCatalogueWorkLeaseMoved
from shared.chat_catalogue_identity import CaptureResult, ChatCaptureScope, authentication_context_fingerprint
from shared.db_retry import run_catalogue_transaction
from shared.models import (ChatCaptureAttempt, ChatInventory, ChatInventoryItem, ChatItemVersion, Job, JobStatus,
    Resource, ResourceType, SlaPolicy, Snapshot, SnapshotChatInventory, SnapshotItem, SnapshotPartition,
    SnapshotStatus, SnapshotType)
from shared.chat_catalogue_identity import SOURCE_UNAVAILABLE_FIELD


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
    job = await session.scalar(select(Job).where(Job.id == hint.job_id).with_for_update(read=True, key_share=True)
        .execution_options(populate_existing=True))
    # NO KEY UPDATE: this transaction writes non-key snapshot columns and must not
    # block KEY SHARE validators of concurrent captures.
    snapshot = await session.scalar(select(Snapshot).where(Snapshot.id == snapshot_id)
        .with_for_update(key_share=True).execution_options(populate_existing=True))
    if snapshot is None or (snapshot.extra_data or {}).get("chat_inventory_format") != 1:
        raise ChatCatalogueWorkLeaseMoved("capture snapshot format or ownership changed")
    if snapshot.status in {SnapshotStatus.COMPLETED, SnapshotStatus.PARTIAL, SnapshotStatus.FAILED}:
        return snapshot
    if snapshot.status != SnapshotStatus.IN_PROGRESS or job is None or job.status not in {JobStatus.RUNNING, JobStatus.RETRYING}:
        raise ChatCatalogueWorkLeaseMoved("capture snapshot or parent job is no longer active")
    if partition_id is not None:
        partition = await session.scalar(select(SnapshotPartition).where(SnapshotPartition.id == partition_id)
            .with_for_update(read=True, key_share=True).execution_options(populate_existing=True))
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
    job = await session.scalar(select(Job).where(Job.id == hint.job_id).with_for_update(read=True, key_share=True))
    snapshot = await session.scalar(select(Snapshot).where(Snapshot.id == snapshot_id)
        .with_for_update(key_share=True).execution_options(populate_existing=True))
    resource = await session.scalar(select(Resource).where(Resource.id == resource_id)
        .with_for_update(key_share=True).execution_options(populate_existing=True))
    if resource is None or snapshot is None or resource.type != ResourceType.USER_CHATS:
        raise ChatCatalogueAdoptionError("capture resource is missing or is not USER_CHATS")
    resource_format = (resource.extra_data or {}).get("chat_inventory_format")
    snapshot_format = (snapshot.extra_data or {}).get("chat_inventory_format")
    if snapshot_format == 1 and snapshot.status in {SnapshotStatus.COMPLETED, SnapshotStatus.PARTIAL, SnapshotStatus.FAILED}:
        return True
    effective_policy = policy_id or resource.sla_policy_id
    policy = await session.scalar(select(SlaPolicy).where(SlaPolicy.id == effective_policy)
        .with_for_update(read=True)) if writer_enabled and effective_policy else None
    protection = policy is not None and next((name for name, on in (("legal_hold", policy.legal_hold_enabled),
        ("immutability", str(policy.immutability_mode or "None").lower() != "none"),
        ("customer_key", str(policy.encryption_mode or "VAULT_MANAGED").upper() == "CUSTOMER_KEY")) if on), None)
    if protection and resource_format != 1 and snapshot_format != 1:
        # The catalogue writes every chat into one deduplicated tenant store
        # (chat-catalogue-<tenant>) that gets no per-policy WORM, legal hold or
        # CMK scope, so keep never-adopted resources on the legacy writer.
        logging.getLogger(__name__).warning("chat_catalogue_legacy_fallback resource=%s snapshot=%s reason=%s",
            resource_id, snapshot_id, protection)
        writer_enabled = False
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
    if protection:
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
            .with_for_update(read=True, key_share=True))
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
    # Through snapshot_items_effective: a backup whose every chat was unchanged points at the full backup
    # it reuses (no rows of its own); an overlay inherits its unchanged chats' rows from its base.
    totals = ("SELECT count(*) FILTER (WHERE e.item_type NOT IN ('CHAT_ATTACHMENT', 'CHAT_HOSTED_CONTENT')), "
              "COALESCE(sum(e.content_size), 0) FROM snapshot_items_effective(CAST(:s AS uuid)) e ")
    count, logical_bytes = (await session.execute(text(totals), {"s": str(snapshot_id)})).one()
    # The prior backup's rows, whatever form it takes, are what this run compares with.
    prior = None if snapshot.reuse_chain_root_id else await session.scalar(select(Snapshot.id).where(
        Snapshot.resource_id == snapshot.resource_id,
        Snapshot.id != snapshot_id, Snapshot.job_id != snapshot.job_id,
        Snapshot.status.in_([SnapshotStatus.COMPLETED, SnapshotStatus.PARTIAL]),
        Snapshot.created_at <= snapshot.created_at).order_by(Snapshot.created_at.desc(), Snapshot.id.desc()).limit(1))
    if snapshot.reuse_chain_root_id:
        new_count, new_bytes = 0, 0
    elif prior is None:
        new_count, new_bytes = count, logical_bytes
    else:
        new_count, new_bytes = (await session.execute(text(totals + "WHERE "
            "e.chat_version_id NOT IN (SELECT p.chat_version_id FROM snapshot_items_effective(CAST(:p AS uuid)) p "
            "WHERE p.chat_version_id IS NOT NULL)"), {"s": str(snapshot_id), "p": str(prior)})).one()
    uploaded = await session.scalar(select(func.coalesce(func.sum(SnapshotChatInventory.uploaded_bytes), 0))
        .where(SnapshotChatInventory.snapshot_id == snapshot_id)) or 0
    return {"chat_inventory_format": 1, "item_count": int(count), "bytes_total": int(logical_bytes),
        "new_item_count": int(new_count), "bytes_added": int(new_bytes), "uploaded_bytes": int(uploaded),
        "partitioned": bool(extra.get("partitioned")), "status": snapshot.status.value,
        "resumed_terminal": snapshot.status != SnapshotStatus.IN_PROGRESS, "outcome": snapshot.status.value.lower(),
        "failure_reason": extra.get("failure_reason") or extra.get("error"),
        "failed": snapshot.status == SnapshotStatus.FAILED}


async def refresh_catalogue_progress(session, scope):
    """Refresh after publication commits, never while holding capture SHARE locks."""
    snapshot = await lock_chat_catalogue_snapshot(session, snapshot_id=scope.snapshot_id,
        snapshot_lease_token=scope.snapshot_lease_token, job_lease_token=scope.job_lease_token,
        partition_id=scope.partition_id, partition_lease_token=scope.partition_lease_token)
    if snapshot.status != SnapshotStatus.IN_PROGRESS:
        return
    primary_count, logical_bytes = (await session.execute(select(
        func.coalesce(func.sum(ChatInventory.evidence["primary_item_count"].as_integer()), 0),
        func.coalesce(func.sum(ChatInventory.logical_bytes), 0),
    ).join(SnapshotChatInventory, SnapshotChatInventory.inventory_id == ChatInventory.id)
        .where(SnapshotChatInventory.snapshot_id == scope.snapshot_id))).one()
    # Legacy inventories without a primary summary contribute a lower bound;
    # final settlement still reconciles their complete physical inventory.
    snapshot.item_count = max(int(snapshot.item_count or 0), int(primary_count))
    snapshot.bytes_total = max(int(snapshot.bytes_total or 0), int(logical_bytes))
    await session.flush()


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


_FOLDER_NAME_MAX = 80
_GRAPH_TIME_FRACTION = __import__("re").compile(r"\.(\d{1,})(?=[+-]|Z|$)")


def _graph_time(value):
    """Parse a Graph ISO timestamp (any fraction length, Z or offset) to aware UTC."""
    if not isinstance(value, str) or not value:
        return None
    text = _GRAPH_TIME_FRACTION.sub(lambda m: "." + (m.group(1) + "000000")[:6], value.replace("Z", "+00:00"))
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def visible_history_start(chat, principal_id):
    """The principal's membership visibleHistoryStartDateTime, or None for full history.

    Teams shares a chat's history from this instant with that member; "0001-01-01"
    means everything. Unknown membership or an unparseable value keeps full history,
    so a backup never silently drops messages it cannot prove are hidden."""
    me = (principal_id or "").lower()
    for member in chat.get("members") or []:
        if isinstance(member, dict) and str(member.get("userId") or "").lower() == me:
            value = member.get("visibleHistoryStartDateTime")
            if not isinstance(value, str) or value.startswith("0001-"):
                return None
            return _graph_time(value)
    return None


def _visibility_filter(chat, principal_id, exclude_message, scope_filter_hash):
    """Per-chat exclusion + stable filter hash combining the configured filter and the
    principal's visible-history boundary (unchanged when history is fully visible)."""
    boundary = visible_history_start(chat, principal_id)
    if boundary is None:
        return exclude_message, scope_filter_hash

    def excluded(raw):
        created = _graph_time(raw.get("createdDateTime"))
        if created is not None and created < boundary:
            return True
        return bool(exclude_message is not None and exclude_message(raw))

    digest = hashlib.sha256(json.dumps(["chat-visible-history-v1", scope_filter_hash, boundary.isoformat()],
                                       separators=(",", ":")).encode()).hexdigest()
    return excluded, digest


def chat_folder(chat, principal_id):
    """Folder label for a chat: topic, else the other members (1:1 → the other person,
    group → "A, B, C" or "A, B +N more"), else a hash. Labels are presentation only."""
    topic = chat.get("topic")
    if isinstance(topic, str) and topic.strip():
        display = topic.strip()
    else:
        me = (principal_id or "").lower()
        others = []
        for member in chat.get("members") or []:
            if not isinstance(member, dict):
                continue
            name = member.get("displayName")
            if isinstance(name, str) and name.strip() and str(member.get("userId") or "").lower() != me:
                others.append(name.strip())
        others.sort(key=lambda n: (n.casefold(), n))  # Graph's member order varies between runs
        if not others:
            display = "Chat " + hashlib.sha256(chat["id"].encode()).hexdigest()[:8]
        elif str(chat.get("chatType") or "").lower() == "oneonone":
            display = others[0]
        else:
            picked = []
            for name in others:
                candidate = ", ".join([*picked, name])
                if picked and len(candidate) > _FOLDER_NAME_MAX:
                    break
                picked.append(name)
            display = ", ".join(picked)
            if len(picked) < len(others):
                display += f" +{len(others) - len(picked)} more"
    return "chats/" + display.replace("/", "-")


_POSTPONED = object()


def _capture_order(chat_ids, principal, first=()):
    """Members of one group chat must not all reach it at the same moment: each would capture
    and upload it in full before any could derive from another's finished capture. Chats
    other members are waiting on this member to capture (``first``) come first."""
    return sorted(chat_ids, key=lambda cid: (cid not in first,
                                             hashlib.sha256((principal + "\x00" + cid).encode()).digest()))


def _chat_members(chat):
    return {str(m.get("userId") or "").lower() for m in chat.get("members") or [] if isinstance(m, dict)}


def _capture_owner(chat, principal, peers):
    """The member whose backup captures a shared chat in this run; the others copy their visible
    part of it. It is the one who sees the most history (earliest visibleHistoryStartDateTime,
    full history first) among this member and the chat's members backed up in this run
    (``peers``, lower-case). Graph lists every member's start, and ties go to a fixed hash,
    so every member's worker picks the same capturer whatever order the workers run in."""
    # Only current members: a donor must be a member to be copied from, and someone who left still
    # lists the chat but is invisible to the members' workers.
    me = (principal or "").lower()
    candidates = _chat_members(chat) & (set(peers) | {me}) or {me}
    def rank(member):
        start = visible_history_start(chat, member)
        return (start is not None, start or datetime.min.replace(tzinfo=timezone.utc),
                hashlib.sha256((str(chat.get("id")) + "\x00" + member).encode()).digest())
    return min(candidates, key=rank)


def _hosted_elsewhere(chat, home_tenant, result) -> bool:
    """A chat hosted in another organization's tenant that Microsoft let us read nothing of
    (it answers app access with 403): listed as hosted elsewhere, not our failure. Graph lists
    some cross-organization chats as chatType unknownFutureValue with no tenantId at all
    (Railway 2026-11-04: every such chat answered 403 InsufficientPrivileges)."""
    return _listed_elsewhere(chat, home_tenant) and result.outcome == "failed" and not result.item_count


def _listed_elsewhere(chat, home_tenant) -> bool:
    chat = chat or {}
    tenant = chat.get("tenantId")
    elsewhere = (tenant.lower() != home_tenant.lower() if isinstance(tenant, str) and tenant
                 else chat.get("chatType") == "unknownFutureValue")
    return isinstance(home_tenant, str) and bool(home_tenant) and elsewhere


async def _refused_elsewhere(source, chat, home_tenant):
    """One request settles a chat listed under another organization that Microsoft refuses (403):
    no capture attempt, so nothing is postponed or waited on (Railway 2026-11-05: Rohit's 92 such
    chats were re-attempted every run, ~7 of a 25-minute incremental). None means capture it."""
    if not _listed_elsewhere(chat, home_tenant):
        return None
    from shared.chat_capture_graph import ChatGraphError
    try:
        await source.latest_message_change(chat["id"])
    except ChatGraphError as exc:
        if exc.status_code == 403:
            now = datetime.now(timezone.utc)
            return CaptureResult("failed", None, False, 0, 0, 0, now, now,
                                 "hosted in another organization's tenant; Microsoft refuses app access")
    return None


def _delta_enabled() -> bool:
    return os.getenv("CHAT_DELTA_BULK_LINK", "true").strip().lower() not in ("0", "false", "no", "off")


async def _merge_resource_metadata(session, resource_id, add: dict, drop=()):
    """Merge keys into a resource's metadata in the database (other writers keep theirs)."""
    await session.execute(text("""UPDATE resources SET metadata = ((COALESCE(metadata::jsonb, '{}'::jsonb)
        - CAST(:drop AS text[])) || CAST(:add AS jsonb))::json WHERE id = :resource"""),
        {"resource": resource_id, "drop": list(drop), "add": json.dumps(add)})


async def _chat_delta_round(session_factory, source, *, resource_id, snapshot_id, principal, since):
    """This snapshot's round of the principal's chat-message delta feed, recorded on the resource so a
    resumed turn reuses it: (ids of chats changed since the snapshot whose run started the saved
    cursor, or None when unknown; that snapshot's id). The new cursor is promoted only when this
    snapshot completes. None when the feed is not used."""
    if not _delta_enabled() or not hasattr(source, "message_delta"):
        return None
    async with session_factory() as session:
        metadata = (await session.get(Resource, resource_id)).extra_data or {}
    pending, saved = metadata.get("chat_delta_pending") or {}, metadata.get("chat_delta_cursor") or {}
    if pending.get("snapshot_id") == str(snapshot_id):
        changed = pending.get("changed")
        return (None if changed is None else set(changed)), pending.get("since_snapshot")
    changed = link = None
    if saved.get("link") and saved.get("principal") == principal and saved.get("scope") == source.permission_scope_hash:
        try:
            changed, link = await source.message_delta(saved["link"])
        except Exception as exc:
            logging.getLogger(__name__).warning("chat_delta_resume_failed resource=%s error=%s %s",
                                                resource_id, type(exc).__name__, str(exc)[:200])
            changed = None
    if link is None:
        try:
            _, link = await source.message_delta(None, since=since - timedelta(minutes=5))
        except Exception as exc:
            logging.getLogger(__name__).warning("chat_delta_start_failed resource=%s error=%s %s",
                                                resource_id, type(exc).__name__, str(exc)[:200])
    since_snapshot = saved.get("snapshot_id") if changed is not None else None
    pending = {"snapshot_id": str(snapshot_id), "link": link, "since_snapshot": since_snapshot,
               "changed": sorted(changed) if changed is not None else None,
               "principal": principal, "scope": source.permission_scope_hash}

    async def record(session):
        await _merge_resource_metadata(session, resource_id, {"chat_delta_pending": pending})
    await run_catalogue_transaction(session_factory, record)
    return changed, since_snapshot


async def _chat_list(source):
    chats = {}
    async for chat in source.iter_chats():
        cid = chat.get("id") if isinstance(chat, dict) else None
        if not isinstance(cid, str) or not cid:
            raise ChatCatalogueIntegrityError("chat listing contains an invalid source identity")
        observed = {"id": cid, "topic": chat.get("topic"), "chatType": chat.get("chatType")}
        if cid in chats and {k: chats[cid][k] for k in observed} != observed:
            raise ChatCatalogueConflict("chat listing changed while it was being enumerated")
        chats[cid] = {**observed, "members": chat.get("members") or [], "tenantId": chat.get("tenantId")}
    return chats


async def run_chat_catalogue_resource(*, resource_id, snapshot_id, writer_enabled, graph_client,
        session_factory, storage, partition_filter=None, partition_id=None, policy_id=None,
        exclude_message=None, scope_filter_hash=None, check_cancelled=None, concurrency=1,
        snapshot_lease_token=None, partition_lease_token=None, job_lease_token=None, full_backup=False):
    """Return None only for an eligible disabled legacy path; never downgrade.
    full_backup: a requested full backup re-reads every chat (no unchanged-chat reuse)."""
    from shared.provider_runtime import (unit_scope, checkpoint_unit, check_provider_turn,
        ProviderTurnDeferred, current_execution, other_provider_work_waiting, yield_provider_turn)
    from shared.chat_capture import capture_chat_inventory, derive_shared_chat, reuse_unchanged_chat, SHARED_CAPTURE_BUSY
    from shared.provider_admission import ProviderDeferred
    from shared.provider_runtime import defer_provider
    from shared.chat_capture_graph import pinned_chat_source, effective_capture_scope_hash
    if type(concurrency) is not int or concurrency < 1:
        raise ValueError("chat capture concurrency must be positive")
    if exclude_message is not None and not scope_filter_hash:
        raise ValueError("chat exclusions require a stable configured scope hash")
    async def cancel_check():
        check_provider_turn()
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
        # Other members' chat backups in this run, planned or started: a shared chat is captured once,
        # by the member who sees the most history; the others copy it. The batch creates its backups
        # one by one (Railway: 16 s apart), so the planned list counts members whose backup is not
        # created yet.
        job_id, peers = snapshot.job_id, {}
        if job_id:
            planned = set((await session.get(Job, job_id)).batch_resource_ids or [])
            planned |= set((await session.scalars(select(Snapshot.resource_id).where(Snapshot.job_id == job_id))).all())
            planned.discard(resource.id)
            if planned:
                peers = {str((extra or {}).get("user_id") or external).lower(): rid
                         for rid, external, extra in (await session.execute(select(Resource.id, Resource.external_id,
                             Resource.extra_data).where(Resource.id.in_(planned),
                             Resource.type == ResourceType.USER_CHATS))).all()}
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
    # Yielding costs a re-claim and a full chat re-list; keep a fresh turn
    # productive for at least this long before fair checkpoints apply.
    turn_started, min_turn_s = time.monotonic(), float(os.getenv("CHAT_TURN_MIN_SECONDS", "60"))
    errors, completed, planned, selected, external = [], 0, 0, set(), []
    try:
        async with pinned_chat_source(graph_client, principal_id=principal) as source:
            chats = await _chat_list(source)
            selected = set(chats) if allowed is None else set(chats) & allowed
            planned = len(chats) if allowed is None else len(allowed)
            if allowed is not None and allowed - set(chats):
                errors.append("some planned chats are no longer visible to the capture principal")
            if published - selected and allowed is None:
                errors.append("chat membership changed across capture attempts")
            shared_capture = os.getenv("CHAT_SHARED_CAPTURE", "true").lower() not in ("0", "false", "no", "off")
            me = principal.lower()
            owners = {cid: _capture_owner(chats[cid], principal, peers) for cid in selected} if shared_capture else {}
            mine_for_others = {cid for cid, owner in owners.items() if owner == me and _chat_members(chats[cid]) & set(peers)}
            keys = _capture_order(selected, principal, mine_for_others) if shared_capture else sorted(selected)
            # Each user's backup holds the history Teams shares with that user.
            filters = {cid: _visibility_filter(chats[cid], principal, exclude_message, scope_filter_hash) for cid in keys}
            def chat_scope(cid):
                return ChatCaptureScope(snapshot_id, resource_id, tenant_id, cid, principal,
                    effective_capture_scope_hash(source.permission_scope_hash, filters[cid][1]),
                    backend_id, partition_id, snapshot_lease_token=snapshot_lease_token,
                    partition_lease_token=partition_lease_token, job_lease_token=job_lease_token)
            def covers_for(cid):
                chat, mine = chats[cid], visible_history_start(chats[cid], principal)
                members = {str(m.get("userId") or "").lower() for m in chat.get("members") or [] if isinstance(m, dict)}
                def covers(donor, recorded_filter):
                    # The donor's capture holds everything this member sees: same configured
                    # filter, and its visible history starts no later than this member's.
                    if (donor or "").lower() not in members:
                        return False
                    if recorded_filter != _visibility_filter(chat, donor, exclude_message, scope_filter_hash)[1]:
                        return False
                    theirs = visible_history_start(chat, donor)
                    return theirs is None or (mine is not None and theirs <= mine)
                return covers
            async def owner_will_capture(cid, final):
                """Another member of this run sees more of the chat and has not captured it yet."""
                owner = owners.get(cid)
                if owner is None or owner == me:
                    return False
                async with session_factory() as session:
                    theirs = (await session.execute(select(Snapshot.id, Snapshot.status).where(
                        Snapshot.job_id == job_id, Snapshot.resource_id == peers[owner])
                        .order_by(Snapshot.created_at.desc()).limit(1))).first()
                    if theirs is None:
                        # Its backup is not created yet: leave the chat for later, never wait on it at the end.
                        return not final
                    if theirs.status != SnapshotStatus.IN_PROGRESS:
                        return False
                    if await session.scalar(select(SnapshotChatInventory.chat_id).where(
                            SnapshotChatInventory.snapshot_id == theirs.id, SnapshotChatInventory.chat_id == cid).limit(1)) is not None:
                        return False
                    # Its own attempt failed (Microsoft 403/423...): it will not publish the chat, and members
                    # waiting on such capturers waited on each other for good (Railway 2026-11-05).
                    return await session.scalar(select(ChatCaptureAttempt.state).where(
                        ChatCaptureAttempt.snapshot_id == theirs.id, ChatCaptureAttempt.chat_id == cid)
                        .order_by(ChatCaptureAttempt.created_at.desc()).limit(1)) != "FAILED"
            async def capture(cid, final=True):
                await cancel_check()
                scope = chat_scope(cid)
                async with session_factory() as session:
                    previous = await _published_chat(session, scope)
                if previous is None:
                    previous = await _refused_elsewhere(source, chats[cid], getattr(source, "home_tenant_id", None))
                busy = False
                if previous is None and shared_capture:
                    previous = await derive_shared_chat(scope=scope, session_factory=session_factory,
                        covers=covers_for(cid), folder_path=chat_folder(chats[cid], principal),
                        exclude_message=filters[cid][0], scope_filter_hash=filters[cid][1])
                    busy = previous is SHARED_CAPTURE_BUSY
                    if busy:
                        previous = None
                if previous is None and not full_backup:
                    # This member's own unchanged capture costs one request and no waiting.
                    previous = await reuse_unchanged_chat(scope=scope, graph_client=source,
                        session_factory=session_factory, folder_path=chat_folder(chats[cid], principal),
                        exclude_message=filters[cid][0], scope_filter_hash=filters[cid][1])
                if previous is None and shared_capture and (busy or await owner_will_capture(cid, final)):
                    # Another member of this run is capturing it (busy) or will: leave it for now.
                    if not final:
                        return _POSTPONED
                    if current_execution() is not None:
                        # Hand the turn back; on resume this chat derives from the finished capture.
                        defer_provider(ProviderDeferred(None, 15 if busy else 60, stage="shared_chat_wait"))
                async with unit_scope('chat:' + hashlib.sha256(cid.encode()).hexdigest(), 'chat'):
                    result = previous
                    if result is None:
                        result = await capture_chat_inventory(scope=scope, graph_client=source, storage=storage,
                            session_factory=session_factory, folder_path=chat_folder(chats[cid], principal),
                            exclude_message=filters[cid][0], scope_filter_hash=filters[cid][1])
                    try:
                        await run_catalogue_transaction(session_factory,
                            lambda session: refresh_catalogue_progress(session, scope))
                    except Exception as exc:
                        # Progress is advisory; a failed refresh cannot undo a
                        # durable publication or replay Graph/storage work.
                        logging.getLogger(__name__).warning("chat live progress refresh failed (%s)", type(exc).__name__)
                    # A chat hosted in another organization is done, not failed (same rule as the snapshot).
                    done = result.outcome == 'complete' or _hosted_elsewhere(
                        chats.get(cid), getattr(source, "home_tenant_id", None), result)
                    await checkpoint_unit(receipt_id=str(result.inventory_id), state='COMPLETED' if done else 'FAILED')
                    return result
            if partition_id is None and allowed is None:
                delta = await _chat_delta_round(session_factory, source, resource_id=resource_id,
                    snapshot_id=snapshot_id, principal=principal, since=started)
                changed, since_snapshot = delta if delta is not None else (None, None)
                if changed is not None and not full_backup:
                    # A resumed turn does not walk chats this snapshot already linked, and a chat hosted in
                    # another tenant at the cursor's snapshot that the feed shows unchanged is listed again
                    # without a request (Railway 2026-11-05: Rohit's 243 linked chats were walked again and
                    # his 92 hosted-elsewhere chats probed again after a fair hand-back).
                    async with session_factory() as session:
                        linked_before = set((await session.scalars(select(ChatCaptureAttempt.chat_id).where(
                            ChatCaptureAttempt.snapshot_id == snapshot_id, ChatCaptureAttempt.state == "PUBLISHED",
                            cast(ChatCaptureAttempt.evidence_history, Text).like("%delta_unchanged_since_snapshot%")))).all()) if published else set()
                        since = await session.get(Snapshot, since_snapshot) if since_snapshot else None
                    known_external = set((((since.extra_data or {}).get("chat_catalogue_scopes") or {}).get("resource") or {})
                                         .get("external_tenant_chats") or []) if since is not None else set()
                    home = getattr(source, "home_tenant_id", None)
                    still_external = {cid for cid in (known_external & set(keys)) - changed if _listed_elsewhere(chats[cid], home)}
                    done_before = (linked_before & set(keys)) | still_external
                    completed += len(done_before)
                    external.extend(sorted(still_external))
                    keys = [cid for cid in keys if cid not in done_before]
                unchanged = [cid for cid in keys if changed is not None and cid not in changed and cid not in published]
                if unchanged and since_snapshot and not full_backup:
                    from shared.chat_capture import link_unchanged_chats
                    # First turn, every chat unchanged: the backup may point at its source (exact match only).
                    linked = await link_unchanged_chats(scopes={cid: chat_scope(cid) for cid in unchanged},
                        folders={cid: chat_folder(chats[cid], principal) for cid in unchanged},
                        filter_hashes={cid: filters[cid][1] for cid in unchanged},
                        session_factory=session_factory, source_snapshot_id=since_snapshot,
                        latest_change=source.latest_message_change,
                        whole_snapshot=not published and len(unchanged) == len(keys), external_chats=set(external))
                    if linked:
                        completed += len(linked)
                        keys = [cid for cid in keys if cid not in linked]
                        try:
                            await run_catalogue_transaction(session_factory,
                                lambda session: refresh_catalogue_progress(session, chat_scope(next(iter(linked)))))
                        except Exception as exc:
                            logging.getLogger(__name__).warning("chat live progress refresh failed (%s)", type(exc).__name__)
            remaining, postponed = iter(keys), []
            yield_requested, processed = False, 0
            # A turn handed back for fairness still waits for captures already running. Free
            # slots keep working until exactly those captures finish (Railway 2026-11-04 22:18:
            # backups idled 15-35 min behind 1-2 huge chats); then no new chat starts.
            running, holders = set(), set()
            async def worker(final):
                nonlocal completed, yield_requested, processed, holders
                for cid in remaining:
                    if yield_requested and not (holders & running):
                        return
                    await cancel_check()
                    token = object()
                    running.add(token)
                    try:
                        result = await capture(cid, final)
                    except (Exception, asyncio.CancelledError) as exc:
                        result = exc
                    finally:
                        running.discard(token)
                    await cancel_check()
                    if result is _POSTPONED:
                        postponed.append(cid)
                        continue
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
                    elif _hosted_elsewhere(chats.get(cid), getattr(source, "home_tenant_id", None), result):
                        completed += 1
                        external.append(cid)
                    else:
                        errors.append(result.failure_reason or "chat capture has incomplete coverage")
                    processed += 1
                    if (not yield_requested and cid not in published and time.monotonic() - turn_started >= min_turn_s
                            and await other_provider_work_waiting()):
                        yield_requested, holders = True, set(running)
            for final in (False, True):
                if final:
                    # Chats another member was capturing come last, by then usually derivable.
                    if not postponed or yield_requested:
                        break
                    remaining = iter(list(postponed))
                workers = [asyncio.create_task(worker(final)) for _ in range(min(concurrency, len(keys)))]
                try:
                    await asyncio.gather(*workers)
                except ProviderTurnDeferred:
                    # Provider pause is cooperative. Ownership loss and explicit
                    # cancellation still interrupt siblings through finally below.
                    await asyncio.gather(*workers, return_exceptions=True)
                    raise
                finally:
                    for task in workers:
                        task.cancel()
                    await asyncio.gather(*workers, return_exceptions=True)
            if yield_requested and processed < len(keys):
                yield_provider_turn()
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
        if partition_id is None and allowed is None and _delta_enabled():
            pending = ((await session.get(Resource, resource_id)).extra_data or {}).get("chat_delta_pending") or {}
            cursor = ({"link": pending["link"], "snapshot_id": str(snapshot_id), "principal": pending["principal"],
                       "scope": pending["scope"]} if complete and pending.get("snapshot_id") == str(snapshot_id)
                      and pending.get("link") else None)
            await _merge_resource_metadata(session, resource_id, {"chat_delta_cursor": cursor} if cursor else {},
                                           drop=("chat_delta_pending", "chat_delta_cursor"))
        outcome = "complete" if complete else ("partial" if known or result["item_count"] else "failed")
        reason = "; ".join(dict.fromkeys(errors))[:1500] if errors else None
        unavailable = await session.scalar(select(func.count()).select_from(SnapshotChatInventory)
            .join(ChatInventoryItem, ChatInventoryItem.inventory_id == SnapshotChatInventory.inventory_id)
            .join(ChatItemVersion, ChatItemVersion.id == ChatInventoryItem.version_id)
            .where(SnapshotChatInventory.snapshot_id == snapshot_id, ChatItemVersion.object_id.is_(None),
                   cast(ChatItemVersion.payload, JSONB)[SOURCE_UNAVAILABLE_FIELD].astext == "true")) or 0
        record = {"complete": complete, "planned_chats": planned, "completed_chats": completed,
            "external_tenant_chats": sorted(external), "source_unavailable_items": unavailable,
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
