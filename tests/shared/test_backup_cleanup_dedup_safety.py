"""Cancel-cleanup must never delete a blob a retained snapshot still needs.

Content-hash dedup (``shared.blob_dedup.find_existing_blob`` and the
per-page bulk variant in the backup worker) makes a NEW snapshot's
``snapshot_items`` row point at a blob that an OLDER, still-retained
snapshot physically owns — ``build_blob_path`` embeds the ORIGINAL
snapshot_id, and Teams message blobs are content-addressed and shared
across users outright. If the cancel/failed-job cleanup deletes those
bytes, the older retained snapshots silently become unrestorable.

Behavioral tests: the real ``cleanup_cancelled_snapshots`` runs against
a mini in-memory DB (statements interpreted via SQLAlchemy's own
``column_descriptions`` + compiled binds, so the fake answers by row
content rather than canned per-path verdicts) and a recording shard.
The assertion surface is the set of paths handed to ``delete_blob``.
"""
from __future__ import annotations

import asyncio
import uuid
from typing import Any, Dict, List, Optional, Set

import pytest

from shared.models import Snapshot, SnapshotItem, SnapshotStatus


# ─── Stub harness ─────────────────────────────────────────────────────


class _FakeResult:
    def __init__(self, rows: List[Any]):
        self._rows = list(rows)

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def scalar_one_or_none(self):
        return self._rows[0] if self._rows else None


class _FakeRow:
    """Multi-column result row — attribute access, like a SQLAlchemy Row."""

    def __init__(self, **kw):
        self.__dict__.update(kw)


class _FakeDB:
    """Row store + call counters shared by every session the run opens."""

    def __init__(self, snapshots: List[Dict], items: List[SnapshotItem]):
        self.snapshots = snapshots
        self.items = items
        self.ref_queries = 0
        self.ref_query_raises = False
        self.commits = 0
        self.mutations: List[str] = []


class _FakeSession:
    def __init__(self, db: _FakeDB):
        self.db = db

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def expunge(self, obj):  # detach is a no-op for plain objects
        return None

    async def commit(self):
        self.db.commits += 1

    async def execute(self, stmt, params=None):
        descs = getattr(stmt, "column_descriptions", None)
        if not descs:  # DELETE / UPDATE / raw text()
            self.db.mutations.append(str(stmt).split("\n")[0])
            # Reuse-chain probe: no reuse snapshots exist in this world,
            # so no doomed snapshot backs a live chain.
            return _FakeResult([])
        params = dict(stmt.compile().params)
        name = descs[0]["name"]

        if name == "id":  # snapshot-id lookup for the job
            job_id = next(
                (v for v in params.values() if isinstance(v, uuid.UUID)), None
            )
            wanted = {
                getattr(s, "name", str(s))
                for v in params.values()
                if isinstance(v, (list, tuple, set))
                for s in v
            }
            return _FakeResult(sorted(
                (s["id"] for s in self.db.snapshots
                 if s["job_id"] == job_id and s["status"].name in wanted),
                key=str,
            ))

        if name == "SnapshotItem":  # keyset-paginated item batch
            sids: Set[uuid.UUID] = set()
            last: Optional[uuid.UUID] = None
            limit: Optional[int] = None
            for v in params.values():
                if isinstance(v, (list, tuple, set)):
                    sids.update(x for x in v if isinstance(x, uuid.UUID))
                elif isinstance(v, uuid.UUID):
                    last = v
                elif isinstance(v, int):
                    limit = v
            rows = sorted(
                (i for i in self.db.items if i.snapshot_id in sids),
                key=lambda i: i.id,
            )
            if last is not None:
                rows = [r for r in rows if r.id > last]
            if limit:
                rows = rows[:limit]
            return _FakeResult(rows)

        if name == "blob_path":
            # Exact-path reference check. The guard no longer pushes
            # "snapshot_id NOT IN (doomed)" into SQL (it needs the owning
            # snapshot id back so it can also spot reuse-chain roots), so
            # we return (blob_path, snapshot_id) pairs and let it filter.
            self.db.ref_queries += 1
            if self.db.ref_query_raises:
                raise RuntimeError("simulated PG failure on reference check")
            paths: Set[str] = set()
            for v in params.values():
                vals = v if isinstance(v, (list, tuple, set)) else [v]
                for x in vals:
                    if isinstance(x, str):
                        paths.add(x)
            return _FakeResult([
                _FakeRow(blob_path=i.blob_path, snapshot_id=i.snapshot_id)
                for i in sorted(
                    (i for i in self.db.items if i.blob_path in paths),
                    key=lambda i: (i.blob_path, str(i.snapshot_id)),
                )
            ])

        return _FakeResult([])


class _RecordingShard:
    def __init__(self):
        self.deleted: List[str] = []

    async def delete_blob(self, container, path):
        self.deleted.append(path)


def _run(coro):
    # asyncio.run (not get_event_loop) — the cleanup holds no loop-bound
    # state, and a fresh loop per test keeps this file order-independent
    # inside the full suite.
    return asyncio.run(coro)


def _item(snapshot_id, blob_path, *, extra=None, item_type="FILE"):
    return SnapshotItem(
        id=uuid.uuid4(),
        snapshot_id=snapshot_id,
        external_id=str(uuid.uuid4()),
        item_type=item_type,
        name="x",
        blob_path=blob_path,
        extra_data=extra,
    )


class _World:
    """One retained snapshot + one cancelled-job snapshot, wired up."""

    def __init__(self, *, extra_items=(), old_items=()):
        self.tenant_id = uuid.uuid4()
        self.resource_id = uuid.uuid4()
        self.cancelled_job_id = uuid.uuid4()
        self.old_snapshot_id = uuid.uuid4()
        self.new_snapshot_id = uuid.uuid4()
        self.snapshots = [
            {"id": self.old_snapshot_id, "job_id": uuid.uuid4(),
             "status": SnapshotStatus.COMPLETED},
            {"id": self.new_snapshot_id, "job_id": self.cancelled_job_id,
             "status": SnapshotStatus.IN_PROGRESS},
        ]
        self.items: List[SnapshotItem] = list(old_items) + list(extra_items)
        self.db = _FakeDB(self.snapshots, self.items)
        self.shard = _RecordingShard()

    def path_for(self, snapshot_id, item_id=None):
        """``build_blob_path`` shape: tenant/resource/snapshot/ts/item."""
        return (
            f"{self.tenant_id}/{self.resource_id}/{snapshot_id}/"
            f"20260731T101500/{item_id or uuid.uuid4()}"
        )

    def run_cleanup(self):
        from shared.backup_cleanup import (
            cleanup_cancelled_snapshots,
            default_container_resolver,
        )
        return _run(cleanup_cancelled_snapshots(
            job_id=self.cancelled_job_id,
            session_factory=lambda: _FakeSession(self.db),
            shard=self.shard,
            container_resolver=default_container_resolver,
        ))


# ─── The bug ──────────────────────────────────────────────────────────


def test_dedup_inherited_blob_owned_by_retained_snapshot_is_not_deleted():
    """The reported data-loss path: the cancelled snapshot's item is a
    dedup hit whose blob_path belongs to an older RETAINED snapshot."""
    w = _World()
    shared_path = w.path_for(w.old_snapshot_id)
    # Older retained snapshot physically owns the bytes.
    w.items.append(_item(w.old_snapshot_id, shared_path))
    # Cancelled snapshot inherited the same path via dedup.
    w.items.append(_item(w.new_snapshot_id, shared_path))
    # ...plus one blob it really does own.
    own_path = w.path_for(w.new_snapshot_id)
    w.items.append(_item(w.new_snapshot_id, own_path))

    stats = w.run_cleanup()

    assert shared_path not in w.shard.deleted, (
        "deleted a dedup-shared blob still owned by retained snapshot "
        f"{w.old_snapshot_id} — that snapshot is now unrestorable"
    )
    assert own_path in w.shard.deleted
    assert stats["blobs_deleted"] == 1


def test_content_addressed_blob_shared_with_retained_snapshot_is_not_deleted():
    """Teams message blobs are content-addressed (no snapshot_id in the
    path) and shared across users by design — the DB reference check is
    the only thing standing between them and deletion."""
    w = _World()
    ca_path = f"{w.tenant_id}/teams/messages/ab/cd/{'f' * 64}.json"
    w.items.append(_item(w.old_snapshot_id, ca_path,
                         item_type="TEAMS_CHAT_MESSAGE"))
    w.items.append(_item(w.new_snapshot_id, ca_path,
                         item_type="TEAMS_CHAT_MESSAGE"))

    stats = w.run_cleanup()

    assert ca_path not in w.shard.deleted
    assert stats["blobs_deleted"] == 0


def test_dedup_inherited_attachment_path_is_not_deleted():
    """Attachment paths inherit the same ownership semantics."""
    w = _World()
    foreign_att = w.path_for(w.old_snapshot_id)
    own_att = w.path_for(w.new_snapshot_id)
    w.items.append(_item(
        w.new_snapshot_id, None, item_type="EMAIL",
        extra={"attachment_blob_paths": [foreign_att, own_att]},
    ))

    w.run_cleanup()

    assert foreign_att not in w.shard.deleted
    assert own_att in w.shard.deleted


def test_reference_check_failure_retains_rather_than_deletes():
    """A backup product leaks a blob before it guesses at deleting one."""
    w = _World()
    own_path = w.path_for(w.new_snapshot_id)
    w.items.append(_item(w.new_snapshot_id, own_path))
    w.db.ref_query_raises = True

    stats = w.run_cleanup()

    assert w.shard.deleted == [], (
        "reference check failed but blobs were deleted anyway"
    )
    assert stats["blobs_deleted"] == 0


# ─── Cleanup must still do its job ────────────────────────────────────


def test_unreferenced_blobs_of_cancelled_snapshot_are_still_deleted():
    """Guard against 'fix' by neutering: paths nobody else references
    still get reclaimed, whatever their shape."""
    w = _World()
    own_path = w.path_for(w.new_snapshot_id)
    ca_path = f"{w.tenant_id}/teams/messages/00/11/{'a' * 64}.json"
    legacy_path = f"users/u1/chats/c1/messages/m1/hosted/h1"
    w.items.append(_item(w.new_snapshot_id, own_path))
    w.items.append(_item(w.new_snapshot_id, ca_path,
                         item_type="TEAMS_CHAT_MESSAGE"))
    w.items.append(_item(w.new_snapshot_id, legacy_path,
                         item_type="TEAMS_CHAT_MESSAGE"))

    stats = w.run_cleanup()

    assert sorted(w.shard.deleted) == sorted([own_path, ca_path, legacy_path])
    assert stats["blobs_deleted"] == 3
    # 2 commits, not 1: the chat_url_cache pointer clear MUST be durable
    # before its blob is deleted (see invalidate_chat_url_cache_paths), so
    # each batch with deletable paths commits, then the final row-delete /
    # status-flip commits.
    assert w.db.commits == 2


def test_two_cancelled_snapshots_sharing_a_blob_still_reclaim_it():
    """Both referencing snapshots are inside the cleanup set, so the
    blob is genuinely unreferenced afterwards — delete it once."""
    w = _World()
    second_id = uuid.uuid4()
    w.snapshots.append({"id": second_id, "job_id": w.cancelled_job_id,
                        "status": SnapshotStatus.COMPLETED})
    shared_path = w.path_for(w.new_snapshot_id)
    w.items.append(_item(w.new_snapshot_id, shared_path))
    w.items.append(_item(second_id, shared_path))

    w.run_cleanup()

    assert w.shard.deleted.count(shared_path) >= 1


def test_no_snapshots_for_job_is_a_noop():
    w = _World()
    w.snapshots = [w.snapshots[0]]  # drop the cancelled-job snapshot
    w.db.snapshots = w.snapshots

    stats = w.run_cleanup()

    assert stats["snapshots"] == 0
    assert stats["items"] == 0
    assert stats["blobs_deleted"] == 0
    assert stats["blob_errors"] == 0
    assert w.shard.deleted == []
