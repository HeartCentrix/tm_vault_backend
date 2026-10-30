"""_delete_snapshots loop body — the wrapper around rehydration.

Reuse-chain safety hinges on rehydrating EVERY doomed id into its heir BEFORE the
bulk delete (a descendant of a not-yet-rehydrated root would resolve to a dead
pointer). This proves the ordering + that both the snapshot_items and snapshots
deletes run. _rehydrate_reuse_heir itself is proven against real Postgres in
test_rehydrate_reuse_heir_pg.py; here it's stubbed to a recorder.
"""
from __future__ import annotations

import uuid

import pytest

import shared.retention_cleanup as rc


class _ExecResult:
    def __init__(self, rowcount, rows=()):
        self.rowcount = rowcount
        self.rows = rows

    def fetchall(self):
        return self.rows


class _FakeSession:
    def __init__(self):
        self.events = []   # ordered ("rehydrate", sid) / ("execute", sql-prefix)

    async def execute(self, stmt, params=None):
        if str(stmt).strip().startswith("SELECT"):
            self.events.append(("guard", str(stmt)))
            return _ExecResult(0, [(uuid.UUID(sid),) for sid in params["ids"]])
        self.events.append(("execute", str(stmt)[:32].lower()))
        return _ExecResult(rowcount=2)


@pytest.mark.asyncio
async def test_empty_set_is_noop():
    sess = _FakeSession()
    n = await rc._delete_snapshots(sess, set())
    assert n == 0
    assert sess.events == []


@pytest.mark.asyncio
async def test_rehydrates_every_id_before_deleting(monkeypatch):
    id1, id2 = uuid.uuid4(), uuid.uuid4()
    doomed = {id1, id2}
    calls = []

    async def _fake_rehydrate(session, sid, *, doomed_ids):
        session.events.append(("rehydrate", sid))
        calls.append((sid, set(doomed_ids)))
        return None

    monkeypatch.setattr(rc, "_rehydrate_reuse_heir", _fake_rehydrate)
    sess = _FakeSession()

    n = await rc._delete_snapshots(sess, doomed)

    # rehydrate ran once per id, each seeing the FULL doomed set
    assert {c[0] for c in calls} == doomed
    assert all(c[1] == doomed for c in calls)

    kinds = [e[0] for e in sess.events]
    # every rehydrate happens before the first delete execute
    first_exec = kinds.index("execute")
    assert kinds[:first_exec] == ["guard", "rehydrate", "rehydrate"]
    # two deletes: snapshot_items then snapshots
    assert kinds.count("execute") == 2
    assert "snapshot_items" in sess.events[first_exec][1]
    assert "snapshots" in sess.events[first_exec + 1][1]
    # returns the Snapshot delete's rowcount
    assert n == 2
