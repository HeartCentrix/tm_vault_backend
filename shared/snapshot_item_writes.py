"""Ordered chat snapshot writes under their original source-work ownership."""
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import asyncio
import random
from uuid import UUID, uuid4

from sqlalchemy import inspect, text
from sqlalchemy.orm.attributes import NO_VALUE

CHAT_ITEM_TYPES = frozenset({"TEAMS_CHAT_MESSAGE", "TEAMS_MESSAGE", "TEAMS_MESSAGE_REPLY",
                             "CHAT_MESSAGE", "CHAT_ATTACHMENT", "CHAT_HOSTED_CONTENT"})
_CHAT_WRITE_SCOPE = ContextVar("chat_snapshot_write_scope", default=None)


async def _run_chat_transaction(factory, operation):
    """One combined budget for idempotent pointer writes and connection loss."""
    from shared.chat_catalogue import ChatCatalogueWorkLeaseMoved
    from shared.db_retry import first_db_sqlstate, is_transient_db_error, run_db_transaction, _chain
    for attempt in range(3):
        try:
            # One attempt here; this loop owns the complete recovery budget.
            return await run_db_transaction(factory, operation, attempts=1)
        except Exception as exc:
            if isinstance(exc, (ChatCatalogueWorkLeaseMoved, ValueError)):
                raise
            code = first_db_sqlstate(exc)
            connection = (code is None or code.startswith("08") or code in {"57P01", "57P02", "57P03"})
            timeout = any("statement timeout" in str(link).lower() or "lock timeout" in str(link).lower()
                          for link in _chain(exc))
            retryable = code in {"40P01", "40001"} or (connection and not timeout and is_transient_db_error(exc))
            if attempt == 2 or not retryable:
                raise
            await asyncio.sleep(random.uniform(0.05, min(0.5, 0.1 * 2 ** attempt)))


@dataclass(frozen=True)
class ChatWriteScope:
    snapshot_id: UUID
    job_id: UUID
    resource_id: UUID
    tenant_id: UUID
    backend_id: UUID
    snapshot_token: int
    job_token: int
    snapshot_owner: UUID | None
    job_owner: UUID | None
    partition_id: UUID | None = None
    partition_token: int | None = None
    partition_owner: UUID | None = None


def current_chat_write_scope():
    return _CHAT_WRITE_SCOPE.get()


@contextmanager
def chat_write_scope(scope):
    token = _CHAT_WRITE_SCOPE.set(scope)
    try:
        yield scope
    finally:
        _CHAT_WRITE_SCOPE.reset(token)


async def _locked_parents(session, snapshot_id, job_id, partition_id):
    from shared.models import Job, Snapshot, SnapshotPartition
    from shared.chat_catalogue import ChatCatalogueWorkLeaseMoved
    await session.execute(text("SET LOCAL lock_timeout='2s'"))
    # KEY SHARE: sibling progress/lease UPDATEs flow; cancellation (FOR UPDATE) still waits.
    job = await session.get(Job, job_id, with_for_update={"read": True, "key_share": True}, populate_existing=True)
    snapshot = await session.get(Snapshot, snapshot_id, with_for_update={"read": True}, populate_existing=True)
    partition = await session.get(SnapshotPartition, partition_id, with_for_update={"read": True},
                                  populate_existing=True) if partition_id else None
    if (job is None or snapshot is None or snapshot.job_id != job_id
        or job.status.value not in {"RUNNING", "QUEUED", "PENDING", "RETRYING"}
        or snapshot.status.value != "IN_PROGRESS" or (snapshot.extra_data or {}).get("cancelled_at")
        or (partition_id and (partition is None or partition.snapshot_id != snapshot_id
            or partition.job_id != job_id or partition.status != "IN_PROGRESS"))):
        raise ChatCatalogueWorkLeaseMoved("chat snapshot source-work parent is unavailable")
    return job, snapshot, partition


def _scope_for(job, snapshot, partition):
    return ChatWriteScope(snapshot.id, job.id, snapshot.resource_id, job.tenant_id, snapshot.backend_id,
        snapshot.lease_token, job.lease_token, snapshot.lease_owner_id, job.lease_owner_id,
        partition.id if partition else None, partition.lease_token if partition else None,
        partition.lease_owner_id if partition else None)


async def capture_chat_write_scope(factory, snapshot_id, *, expected_snapshot_token=None,
        expected_job_token=None, partition_id=None, expected_partition_token=None, expected_partition_owner=None):
    """Capture before source I/O, never from inside an already-failed write retry."""
    from shared.models import Snapshot
    from shared.chat_catalogue import ChatCatalogueWorkLeaseMoved
    snapshot_id = UUID(str(snapshot_id))
    partition_id = UUID(str(partition_id)) if partition_id else None
    async def capture(session):
        pointer = await session.get(Snapshot, snapshot_id)
        if pointer is None:
            raise ChatCatalogueWorkLeaseMoved("chat snapshot disappeared before source capture")
        parents = await _locked_parents(session, snapshot_id, pointer.job_id, partition_id)
        scope = _scope_for(*parents)
        for expected, actual in ((expected_snapshot_token, scope.snapshot_token),
                                 (expected_job_token, scope.job_token),
                                 (expected_partition_token, scope.partition_token)):
            if expected is not None and expected != actual:
                raise ChatCatalogueWorkLeaseMoved("chat source-work generation moved before capture")
        if expected_partition_owner is not None and str(scope.partition_owner) != str(expected_partition_owner):
            raise ChatCatalogueWorkLeaseMoved("chat partition owner moved before capture")
        if scope.backend_id is None or scope.tenant_id is None:
            raise ChatCatalogueWorkLeaseMoved("chat source-work storage ownership is missing")
        return scope
    return await _run_chat_transaction(factory, capture)


async def validate_chat_write_scope(session, scope):
    from shared.chat_catalogue import ChatCatalogueWorkLeaseMoved
    current = _scope_for(*await _locked_parents(session, scope.snapshot_id, scope.job_id, scope.partition_id))
    if current != scope:
        raise ChatCatalogueWorkLeaseMoved("chat snapshot source-work ownership moved")


async def persist_chat_snapshot_rows(factory, rows, scope, insert):
    """Retry only pointer persistence; callers already completed Graph/storage I/O."""
    from shared.chat_catalogue import ChatCatalogueWorkLeaseMoved
    if not rows:
        return 0
    if scope is None:
        raise ChatCatalogueWorkLeaseMoved("chat snapshot write has no original source-work ownership")
    prepared = []
    for row in rows:
        if (UUID(str(row["snapshot_id"])) != scope.snapshot_id
            or (row.get("tenant_id") is not None and UUID(str(row["tenant_id"])) != scope.tenant_id)
            or row["item_type"] not in CHAT_ITEM_TYPES):
            raise ChatCatalogueWorkLeaseMoved("chat pointer payload differs from captured target ownership")
        prepared.append({**row, "id": row.get("id") or uuid4(), "snapshot_id": scope.snapshot_id,
            "tenant_id": row.get("tenant_id") or scope.tenant_id,
            "backend_id": row.get("backend_id") or scope.backend_id})
    async def operation(session):
        await validate_chat_write_scope(session, scope)
        return await insert(session, prepared)
    return await _run_chat_transaction(factory, operation)


def ordered_snapshot_item_rows(rows):
    """Stable conflict-key order; matches PostgreSQL UUID and C text ordering."""
    return sorted(rows, key=lambda row: (
        UUID(str(row["snapshot_id"])).bytes,
        row["external_id"].encode("utf-8"), row["item_type"].encode("utf-8")))


def snapshot_item_rows(items):
    """Preserve every assigned mapped field without replacing omitted defaults."""
    rows = []
    for item in items:
        state = inspect(item)
        row = {attribute.key: state.attrs[attribute.key].loaded_value
               for attribute in state.mapper.column_attrs
               if state.attrs[attribute.key].loaded_value is not NO_VALUE}
        if row.get("id") is None:
            item.id = row["id"] = uuid4()
        rows.append(row)
    return rows
