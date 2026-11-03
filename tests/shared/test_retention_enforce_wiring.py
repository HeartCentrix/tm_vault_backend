"""Seam 1 — enforce_retention_for_tenant EMISSION + DELETE wiring (P4a end-to-end).

The pure builders are unit-tested elsewhere; this proves the streaming driver
actually (a) adds one AuditEvent per doomed snapshot to the session and (b) calls
_delete_snapshots with exactly those ids WHEN deletes are permitted, and (c) does
NEITHER under the kill-switch dry-run (so the freeze truly deletes nothing and
emits no misleading audit). Fake AsyncSession — no DB; _delete_snapshots stubbed.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

import shared.retention_cleanup as rc


class _Scalars:
    def __init__(self, rows):
        self._rows = list(rows)

    def all(self):
        return list(self._rows)

    def __aiter__(self):
        self._it = iter(self._rows)
        return self

    async def __anext__(self):
        try:
            return next(self._it)
        except StopIteration:
            raise StopAsyncIteration


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def scalars(self):
        return _Scalars(self._rows)


class _FakeSession:
    def __init__(self, policies, resources, snaps):
        self._policies, self._resources, self._snaps = policies, resources, snaps
        self.added = []
        self.commits = 0

    async def execute(self, stmt):
        s = str(stmt).lower()
        if "sla_policies" in s:
            return _Result(self._policies)
        if "from resources" in s:
            return _Result([r.id for r in self._resources] if "limit" in s else self._resources)
        if "snapshots" in s:
            return _Result(self._snaps)
        return _Result([])

    async def stream(self, stmt):
        return _Result(self._resources)

    def add(self, obj):
        self.added.append(obj)

    async def commit(self):
        self.commits += 1


def _snap(days_ago, status="COMPLETED"):
    ts = datetime.utcnow() - timedelta(days=days_ago)
    return SimpleNamespace(
        id=uuid.uuid4(), started_at=ts, created_at=ts, status=SimpleNamespace(value=status),
        type=SimpleNamespace(value="INCREMENTAL"), item_count=1, bytes_total=1,
        reuse_of_snapshot_id=None, reuse_chain_root_id=None,
    )


def _scenario():
    tid = uuid.uuid4()
    pol = SimpleNamespace(
        id=uuid.uuid4(), tenant_id=tid, name="Gold", is_default=True,
        retention_mode="FLAT", retention_days=1, retention_hot_days=0,
        retention_cool_days=0, retention_archive_days=0,
        legal_hold_enabled=False, legal_hold_until=None, immutability_mode=None,
        archived_retention_mode="SAME",
    )
    res = SimpleNamespace(
        id=uuid.uuid4(), tenant_id=tid, org_id=uuid.uuid4(), sla_policy_id=pol.id,
        type=SimpleNamespace(value="USER_MAIL"), display_name="Amit", external_id="a@x",
        status=SimpleNamespace(value="PROTECTED"),
    )
    recent = _snap(0)          # within 1-day window -> kept
    old1 = _snap(10)           # outside -> delete
    old2 = _snap(20)           # oldest baseline anchor -> kept
    return tid, pol, res, recent, old1, old2


@pytest.mark.asyncio
async def test_emits_audit_and_deletes_when_permitted(monkeypatch):
    tid, pol, res, recent, old1, old2 = _scenario()
    sess = _FakeSession([pol], [res], [recent, old1, old2])
    recorded = {}
    async def _fake_delete(session, ids, *, audit_events=None):
        recorded["ids"] = set(ids)
        for sid in ids:
            session.add(audit_events[sid])
        return len(ids)
    monkeypatch.setattr(rc, "_delete_snapshots", _fake_delete)
    monkeypatch.setattr(rc.settings, "RETENTION_DELETE_ENABLED", True)

    await rc.enforce_retention_for_tenant(sess, tid)

    assert recorded["ids"] == {old1.id}                      # deletes the right ones
    assert {e.snapshot_id for e in sess.added} == {old1.id}  # one audit per doomed
    assert all(e.action == "RETENTION_SNAPSHOT_DELETED" for e in sess.added)
    assert recent.id not in recorded["ids"]                          # restorable kept


@pytest.mark.asyncio
async def test_dryrun_emits_no_audit_and_no_delete(monkeypatch):
    tid, pol, res, recent, old1, old2 = _scenario()
    sess = _FakeSession([pol], [res], [recent, old1, old2])
    called = {"n": 0}
    async def _fake_delete(session, ids):
        called["n"] += 1
        return len(ids)
    monkeypatch.setattr(rc, "_delete_snapshots", _fake_delete)
    monkeypatch.setattr(rc.settings, "RETENTION_DELETE_ENABLED", False)

    stats = await rc.enforce_retention_for_tenant(sess, tid)

    assert sess.added == []            # NO audit rows written in dry-run
    assert called["n"] == 0            # _delete_snapshots never called
    assert stats.get("would_delete", 0) == 1   # but it still reports what it WOULD delete
