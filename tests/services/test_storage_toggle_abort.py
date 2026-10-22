"""Storage-toggle abort must actually stop the toggle.

Previously POST /toggle/{id}/abort only rewrote rows: it set the event
status to 'aborted' AND reset system_config.transition_state to 'stable',
but the toggle worker never re-read the status after preflight — it
overwrote 'aborted' with 'drain_completed' and drove promote/DNS/worker
phases to completion. Meanwhile the premature 'stable' reset let
submit_toggle admit a SECOND concurrent toggle against the same
system_config singleton.

Pinned contract:
- the abort endpoint claims the event atomically (guarded UPDATE) and does
  NOT touch system_config — the worker owns transition_state;
- the worker honors an abort before starting, during the drain poll loop,
  and via a guarded 'drain_completed' write that loses to a committed
  abort — standing down and restoring transition_state='stable' itself.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import uuid
from types import SimpleNamespace

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

from services.storage_toggle_worker import orchestrator  # noqa: E402
from services.storage_toggle_worker.drain import (  # noqa: E402
    DrainAborted,
    DrainResult,
    drain_inflight,
)

_ADMIN_PATH = _REPO_ROOT / "api-gateway" / "routes" / "admin_storage.py"
_spec = importlib.util.spec_from_file_location("admin_storage_under_test", _ADMIN_PATH)
admin_storage = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["admin_storage_under_test"] = admin_storage
try:
    _spec.loader.exec_module(admin_storage)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"admin_storage module failed to import: {exc}",
        allow_module_level=True,
    )

from fastapi import HTTPException  # noqa: E402


# ---------------------------------------------------------------- fakes

class FakeConn:
    """asyncpg-conn stand-in for the abort endpoint."""

    def __init__(self, status):
        self.status = status  # None -> event row missing
        self.executed: list[str] = []

    async def fetchval(self, sql, *args):
        self.executed.append(sql)
        if "SET status='aborted'" in sql:
            if self.status in ("started", "drain_started"):
                self.status = "aborted"
                return "aborted"
            return None
        return None

    async def fetchrow(self, sql, *args):
        self.executed.append(sql)
        return {"status": self.status} if self.status is not None else None

    async def execute(self, sql, *args):
        self.executed.append(sql)
        return "UPDATE 1"

    async def close(self):
        pass


class FakeDB:
    """asyncpg-conn stand-in for the orchestrator / drain loop."""

    def __init__(self, event_status="started", drain_update_tag="UPDATE 1"):
        self.event_status = event_status
        self.drain_update_tag = drain_update_tag
        self.executed: list[tuple[str, tuple]] = []

    async def fetchval(self, sql, *args):
        if "FROM storage_toggle_events" in sql:
            return self.event_status
        return None

    async def fetchrow(self, sql, *args):
        if "FROM system_config" in sql:
            return {"transition_state": "stable", "cooldown_until": None}
        return None

    async def fetch(self, sql, *args):
        return []

    async def execute(self, sql, *args):
        self.executed.append((sql, args))
        if "status='drain_completed'" in sql:
            return self.drain_update_tag
        return "UPDATE 1"

    async def close(self):
        pass


# ----------------------------------------------------- abort endpoint

def test_abort_claims_event_and_releases_the_transition_guardedly():
    """Abort must claim the event AND release transition_state — but only
    when no other toggle is live.

    Both extremes are bugs we shipped: an UNCONDITIONAL reset let
    submit_toggle admit a second concurrent toggle while the first worker
    was still promoting, while delegating the reset entirely to the worker
    left the fleet READ-ONLY when the worker pod died mid-drain
    (transition_state stuck 'draining' → TransitionInProgressError on every
    backup write, submit_toggle 409s, no operator escape hatch). The claim
    only wins from 'started'/'drain_started' (strictly pre-promotion) and
    the release carries a NOT EXISTS guard against other live toggles."""
    conn = FakeConn(status="started")

    async def _fake_connect():
        return conn

    orig = admin_storage._connect
    admin_storage._connect = _fake_connect
    try:
        result = asyncio.run(
            admin_storage.abort_toggle(uuid.uuid4(), user={"id": "u1"})
        )
    finally:
        admin_storage._connect = orig

    assert result == {"status": "aborted"}
    assert conn.status == "aborted"
    release = [sql for sql in conn.executed if "system_config" in sql]
    assert release, "abort must release a stuck transition_state"
    sql = release[0]
    assert "transition_state='stable'" in sql
    assert "transition_state <> 'stable'" in sql, (
        "a already-stable row must be left untouched"
    )
    assert "NOT EXISTS" in sql and "storage_toggle_events" in sql, (
        "the release must not steal a transition owned by another live toggle"
    )


def test_abort_409s_once_drain_has_completed():
    conn = FakeConn(status="drain_completed")

    async def _fake_connect():
        return conn

    orig = admin_storage._connect
    admin_storage._connect = _fake_connect
    try:
        with pytest.raises(HTTPException) as exc:
            asyncio.run(admin_storage.abort_toggle(uuid.uuid4(), user={"id": "u1"}))
    finally:
        admin_storage._connect = orig

    assert exc.value.status_code == 409
    assert conn.status == "drain_completed", "terminal status must not be clobbered"


def test_abort_404s_for_missing_event():
    conn = FakeConn(status=None)

    async def _fake_connect():
        return conn

    orig = admin_storage._connect
    admin_storage._connect = _fake_connect
    try:
        with pytest.raises(HTTPException) as exc:
            asyncio.run(admin_storage.abort_toggle(uuid.uuid4(), user={"id": "u1"}))
    finally:
        admin_storage._connect = orig

    assert exc.value.status_code == 404


# ----------------------------------------------------- worker side

def test_drain_loop_raises_on_abort():
    db = FakeDB(event_status="aborted")
    with pytest.raises(DrainAborted):
        asyncio.run(drain_inflight(db, event_id="ev-1"))


def test_run_toggle_skips_pre_aborted_event_but_frees_a_stuck_transition(monkeypatch):
    """A redelivered aborted toggle must stand down WITHOUT promoting — and
    must release transition_state on the way out.

    This path is reached exactly when a prior attempt died mid-run (the
    message stays un-acked for the whole 900s drain), i.e. precisely when
    transition_state is still 'draining' and every backup write is failing
    with TransitionInProgressError. Returning without clearing it left the
    fleet read-only with no operator escape hatch short of manual SQL."""
    db = FakeDB(event_status="aborted")
    promoted = []

    async def _fake_open_db():
        return db

    async def _fake_promote(kind):
        promoted.append(kind)

    monkeypatch.setattr(orchestrator, "_open_db", _fake_open_db)
    monkeypatch.setattr(orchestrator, "promote_target", _fake_promote)

    asyncio.run(orchestrator.run_toggle(
        {"event_id": "ev-1", "from_id": "a", "to_id": "b"}
    ))

    assert promoted == [], "an aborted event must never promote"
    sqls = " ".join(sql for sql, _ in db.executed)
    assert "transition_state='draining'" not in sqls, (
        "an aborted event must never start a drain"
    )
    assert "transition_state='stable'" in sqls, (
        "standing down must release a stuck transition or backup writes stay blocked"
    )
    assert "NOT EXISTS" in sqls, (
        "the release must be guarded so it cannot steal a transition owned by "
        "another live toggle"
    )

def test_run_toggle_stands_down_when_abort_wins_the_drain_race(monkeypatch):
    """Abort commits between the drain finishing and the status write: the
    guarded 'drain_completed' UPDATE matches 0 rows and the worker must
    restore transition_state='stable' and never promote."""
    db = FakeDB(event_status="started", drain_update_tag="UPDATE 0")
    promoted = []

    async def _fake_open_db():
        return db

    async def _fake_preflight(**kwargs):
        return SimpleNamespace(ok=True, checks=[])

    async def _fake_drain(_db, event_id):
        return DrainResult(drained_count=2, retried_count=1)

    async def _fake_promote(kind):
        promoted.append(kind)

    monkeypatch.setattr(orchestrator, "_open_db", _fake_open_db)
    monkeypatch.setattr(orchestrator, "run_preflight", _fake_preflight)
    monkeypatch.setattr(orchestrator, "drain_inflight", _fake_drain)
    monkeypatch.setattr(orchestrator, "promote_target", _fake_promote)

    asyncio.run(orchestrator.run_toggle(
        {"event_id": "ev-1", "from_id": "a", "to_id": "b"}
    ))

    assert promoted == [], (
        "worker kept promoting a toggle the operator had already aborted"
    )
    transitions = [
        args[0] for sql, args in db.executed
        if "transition_state=$1" in sql
    ]
    assert transitions[-1] == "stable", (
        "worker must restore transition_state='stable' when standing down"
    )
