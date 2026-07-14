"""Unit tests for the snapshot-reuse resolver.

Pure logic tests against a stubbed AsyncSession — no real DB required.
Integration coverage of the validation trigger and the retention
rehydration sequence lives in the per-service integration suite (those
need a Postgres fixture).
"""
from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pytest


@dataclass
class _Row:
    """Tiny SQLAlchemy-Row stand-in: attribute access + indexable."""
    id: uuid.UUID
    reuse_of_snapshot_id: Optional[uuid.UUID]
    reuse_chain_root_id: Optional[uuid.UUID]


@dataclass
class _BulkRow:
    id: uuid.UUID
    target_id: uuid.UUID


class _StubResult:
    def __init__(self, rows: List[Any]):
        self._rows = rows

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return list(self._rows)


class _StubSession:
    """Captures the executed SQL text + binds and returns canned rows.

    Tests stage `rows_by_sql_fragment` keyed by a substring of the
    SQL; the stub finds the first matching entry and returns its rows.
    """
    def __init__(self, rows_by_sql_fragment: Dict[str, List[Any]]):
        self._mapping = rows_by_sql_fragment
        self.executed: List[tuple[str, Dict[str, Any]]] = []

    async def execute(self, statement, binds: Optional[Dict] = None):
        sql = str(statement)
        self.executed.append((sql, dict(binds or {})))
        for fragment, rows in self._mapping.items():
            if fragment in sql:
                return _StubResult(rows)
        return _StubResult([])


def _run(coro):
    # A fresh loop per call — get_event_loop() raises "no current event loop"
    # once any prior pytest-asyncio test in the run closes the default loop
    # (this file's failures were purely that ordering artifact, not logic).
    return asyncio.new_event_loop().run_until_complete(coro)


def test_full_snapshot_resolves_to_itself():
    from shared.snapshot_reuse import resolve_snapshot_items_target
    sid = uuid.uuid4()
    session = _StubSession({
        "FROM snapshots": [
            _Row(id=sid, reuse_of_snapshot_id=None, reuse_chain_root_id=None),
        ],
    })
    out = _run(resolve_snapshot_items_target(session, sid))
    assert out.target_id == sid
    assert out.is_reuse is False
    assert out.chain_depth == 0


def test_reuse_snapshot_resolves_to_chain_root():
    from shared.snapshot_reuse import resolve_snapshot_items_target
    reuse = uuid.uuid4()
    parent = uuid.uuid4()
    root = uuid.uuid4()
    session = _StubSession({
        # First lookup returns the reuse row.
        "FROM snapshots\n         WHERE id = :sid": [
            _Row(id=reuse, reuse_of_snapshot_id=parent, reuse_chain_root_id=root),
        ],
        # Second lookup (depth measurement) returns max depth = 2.
        "WITH RECURSIVE chain": [
            _Row(id=uuid.uuid4(), reuse_of_snapshot_id=None, reuse_chain_root_id=None),
        ],
    })
    # Override the recursive chain-depth result by stub
    class _DepthRow:
        depth = 2
    session._mapping["WITH RECURSIVE chain"] = [_DepthRow()]
    out = _run(resolve_snapshot_items_target(session, reuse))
    assert out.target_id == root
    assert out.is_reuse is True
    assert out.chain_depth == 2


def test_missing_snapshot_raises():
    from shared.snapshot_reuse import (
        resolve_snapshot_items_target,
        SnapshotNotFound,
    )
    sid = uuid.uuid4()
    session = _StubSession({"FROM snapshots": []})
    with pytest.raises(SnapshotNotFound):
        _run(resolve_snapshot_items_target(session, sid))


def test_resolve_many_returns_target_per_input():
    from shared.snapshot_reuse import resolve_many
    full_id = uuid.uuid4()
    reuse_id = uuid.uuid4()
    root_id = uuid.uuid4()
    session = _StubSession({
        "SELECT id,\n               COALESCE(reuse_chain_root_id, id) AS target_id": [
            _BulkRow(id=full_id, target_id=full_id),
            _BulkRow(id=reuse_id, target_id=root_id),
        ],
    })
    out = _run(resolve_many(session, [full_id, reuse_id]))
    assert out[full_id] == full_id
    assert out[reuse_id] == root_id


def test_resolve_many_empty_input_returns_empty():
    from shared.snapshot_reuse import resolve_many
    session = _StubSession({})
    out = _run(resolve_many(session, []))
    assert out == {}


def test_resolve_many_drops_missing_ids():
    """When some input ids no longer exist, the resolver returns only
    the live ones — caller compares against the input set if it cares
    about distinguishing missing from full."""
    from shared.snapshot_reuse import resolve_many
    live_id = uuid.uuid4()
    dead_id = uuid.uuid4()
    session = _StubSession({
        "SELECT id,\n               COALESCE(reuse_chain_root_id, id) AS target_id": [
            _BulkRow(id=live_id, target_id=live_id),
        ],
    })
    out = _run(resolve_many(session, [live_id, dead_id]))
    assert out == {live_id: live_id}


def test_coerce_uuid_accepts_string_and_uuid():
    from shared.snapshot_reuse import _coerce_uuid
    u = uuid.uuid4()
    assert _coerce_uuid(u) is u
    assert _coerce_uuid(str(u)) == u
