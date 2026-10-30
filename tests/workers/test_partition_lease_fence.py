"""Partition terminal / release writes are fenced on the claimed lease,
and a fenced-out write stands the shard down.

`_claim_partition` bumps ``lease_token`` and stamps ``lease_owner_id``,
but until 2026-10-14 it never returned them, and the four writers —
`_mark_partition_terminal`, its exhaustion ``_release``,
`_requeue_partition_for_retry`'s requeue and ITS ``_release`` — only
carried ``WHERE id = :pid AND status NOT IN ('COMPLETED', 'FAILED')``.
Reconciler STEP A requeues a shard whose lease lapsed (lease cleared,
token bumped; e.g. after a DB outage stalled LeaseExtender renewals) and
another worker re-claims it. The ORIGINAL worker, still draining, could
then flip the row COMPLETED / FAILED / back to QUEUED underneath the new
claimant and finalize the parent snapshot from the wrong counts.

Fake-session pins — no database. The worker binds the claimed lease to
the handler task (every delivery is its own task in `consume_queue`), so
each test runs claim → write → finalize inside ONE coroutine.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import types
import uuid
from typing import Any, Dict, List, Optional, Tuple

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_lease_fence_under_test", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_lease_fence_under_test"] = _bw
assert _spec and _spec.loader
_spec.loader.exec_module(_bw)  # type: ignore[union-attr]


def _run(coro):
    """Run on a live loop, installing one if the suite left none behind.
    run_until_complete wraps the coroutine in a Task with its own copy of
    the context, so a lease bound inside never leaks between tests."""
    try:
        loop = asyncio.get_event_loop_policy().get_event_loop()
        if loop.is_closed():
            raise RuntimeError("closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


# ─── fakes ─────────────────────────────────────────────────────────────


class _Row:
    """SQLAlchemy-Row stand-in: attribute, index and ``_mapping`` access."""

    def __init__(self, **cols: Any):
        self._mapping = dict(cols)
        for k, v in cols.items():
            setattr(self, k, v)

    def __getitem__(self, i: int):
        return list(self._mapping.values())[i]


class _Result:
    def __init__(self, *, rowcount: int = 1, first: Any = None):
        self.rowcount = rowcount
        self._first = first

    def first(self):
        return self._first

    def all(self):
        return []

    def scalar(self):
        return None


class _Session:
    """Records every statement; ``respond(sql, binds)`` decides the result
    (an exception instance is raised)."""

    def __init__(self, respond=None):
        self.executed: List[Tuple[str, Dict[str, Any]]] = []
        self.commits = 0
        self._respond = respond or (lambda sql, binds: _Result())

    async def execute(self, statement, binds: Optional[Dict] = None):
        sql, b = str(statement), dict(binds or {})
        self.executed.append((sql, b))
        out = self._respond(sql, b)
        if isinstance(out, BaseException):
            raise out
        return out

    async def commit(self):
        self.commits += 1

    async def get(self, *_a, **_k):
        return None


class _Factory:
    def __init__(self, session):
        self._session = session

    def __call__(self):
        session = self._session

        class _Ctx:
            async def __aenter__(self):
                return session

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


_OWNER = uuid.UUID("11111111-1111-4111-8111-111111111111")
_OTHER = uuid.UUID("22222222-2222-4222-8222-222222222222")

_FENCE_OWNER = "lease_owner_id = CAST(:__lease_owner AS uuid)"
_FENCE_TOKEN = "lease_token = :__lease_token"
_CLAIM_MARK = "SET status        = 'IN_PROGRESS'"
_REQUEUE_MARK = "lease_token      = lease_token + 1"
_TERMINAL_MARK = "files_uploaded = :nf"


class _Worker:
    """What the methods under test read off ``self``."""
    worker_id = "worker-test"
    _worker_region = "default"
    _heartbeat = types.SimpleNamespace(worker_uuid=_OWNER)


class _Dead(Exception):
    """Stands in for the connection drop that exhausts a retry ladder."""


async def _once(op, **_kw):
    """Single-attempt stand-in for shared.db_retry.run_with_db_retry —
    the real ladder sleeps ~46 s before giving up."""
    return await op()


def _claim_row(sid, *, owner=_OWNER, token=7):
    return _Row(
        snapshot_id=sid, tenant_id=uuid.uuid4(), resource_id=uuid.uuid4(),
        job_id=uuid.uuid4(), drive_id=None, file_ids=None, payload=None,
        partition_index=0, shard_unit_count=3, total_bytes_est=0,
        partition_type="CHATS", retry_count=0,
        lease_owner_id=owner, lease_token=token,
    )


def _bind(pid, sid, *, token=7, owner=_OWNER):
    return _bw._bind_partition_lease(
        pid, {"snapshot_id": sid, "lease_owner_id": owner, "lease_token": token},
    )


def _partition_updates(session):
    return [(s, b) for s, b in session.executed if "UPDATE snapshot_partitions" in s]


def _probes(session):
    return [s for s, _ in session.executed if "SELECT lease_owner_id, lease_token" in s]


async def _claim(session, pid, monkeypatch):
    monkeypatch.setattr(_bw, "async_session_factory", _Factory(session))
    return await _bw.BackupWorker._claim_partition(_Worker(), pid, "worker-test")


# ─── _claim_partition returns + binds the lease ────────────────────────


def test_claim_returns_the_lease_and_binds_it_to_the_handler_task(monkeypatch):
    pid, sid = uuid.uuid4(), uuid.uuid4()
    session = _Session(
        lambda sql, b: _Result(first=_claim_row(sid)) if _CLAIM_MARK in sql else _Result()
    )

    async def body():
        claim = await _claim(session, pid, monkeypatch)
        return claim, _bw._PARTITION_LEASE_CTX.get()

    claim, record = _run(body())
    sql = next(s for s, _ in session.executed if _CLAIM_MARK in s)
    returning = sql.split("RETURNING", 1)[1]
    assert "lease_owner_id" in returning and "lease_token" in returning, (
        "the claim must hand back the fence it just stamped"
    )
    assert claim["lease_token"] == 7 and claim["lease_owner_id"] == _OWNER
    assert record is not None, "the lease must be bound to the handler task"
    assert record["partition_id"] == str(pid)
    assert record["snapshot_id"] == str(sid)
    assert record["lease"].table == "snapshot_partitions"
    assert record["lease"].token == 7
    assert record["lease"].owner_id == str(_OWNER)
    assert record["stolen"] is False


def test_claim_index_fallback_carries_the_lease(monkeypatch):
    pid, sid = uuid.uuid4(), uuid.uuid4()
    # a driver row WITHOUT _mapping → the positional fallback
    row = (sid, uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), None, None, None,
           0, 3, 0, "CHATS", 0, _OWNER, 7)
    session = _Session(lambda sql, b: _Result(first=row))
    claim = _run(_claim(session, pid, monkeypatch))
    assert claim["lease_owner_id"] == _OWNER and claim["lease_token"] == 7


def test_refused_claim_clears_a_stale_binding(monkeypatch):
    pid = uuid.uuid4()
    session = _Session(lambda sql, b: _Result(first=None))

    async def body():
        _bind(pid, uuid.uuid4(), token=3)
        claim = await _claim(session, pid, monkeypatch)
        return claim, _bw._PARTITION_LEASE_CTX.get()

    claim, record = _run(body())
    assert claim is None and record is None


# ─── _mark_partition_terminal ──────────────────────────────────────────


def test_terminal_write_is_fenced_on_owner_and_token(monkeypatch, capsys):
    pid, sid = uuid.uuid4(), uuid.uuid4()
    session = _Session(lambda sql, b: _Result(rowcount=1))
    monkeypatch.setattr(_bw, "async_session_factory", _Factory(session))

    async def body():
        _bind(pid, sid)
        return await _bw.BackupWorker._mark_partition_terminal(
            _Worker(), pid, status="COMPLETED", files_uploaded=3, bytes_uploaded=9,
        )

    assert _run(body()) is True
    (sql, binds), = _partition_updates(session)
    where = sql.split("WHERE", 1)[1]
    assert "AND status NOT IN ('COMPLETED', 'FAILED')" in where, "idempotent guard kept"
    assert _FENCE_OWNER in where and _FENCE_TOKEN in where, where
    assert binds["__lease_owner"] == str(_OWNER) and binds["__lease_token"] == 7
    assert binds["st"] == "COMPLETED" and binds["nf"] == 3 and binds["nb"] == 9
    assert not _probes(session), "a matched write needs no probe"
    assert "[LEASE]" not in capsys.readouterr().out


def test_fenced_out_terminal_write_is_stolen_and_skips_finalize(monkeypatch, capsys):
    pid, sid, jid = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()

    def respond(sql, b):
        if "UPDATE snapshot_partitions" in sql:
            return _Result(rowcount=0)                  # fenced out
        if "SELECT lease_owner_id, lease_token" in sql:  # the lease moved on
            return _Result(first=_Row(lease_owner_id=_OTHER, lease_token=9))
        return _Result(first=None)

    session = _Session(respond)
    monkeypatch.setattr(_bw, "async_session_factory", _Factory(session))

    async def body():
        _bind(pid, sid)
        w = _Worker()
        owned = await _bw.BackupWorker._mark_partition_terminal(
            w, pid, status="COMPLETED", files_uploaded=3, bytes_uploaded=9,
        )
        before = len(session.executed)
        fin = await _bw.BackupWorker._finalize_partitioned_snapshot(w, sid, job_id=jid)
        return owned, fin, len(session.executed) - before, _bw._PARTITION_LEASE_CTX.get()

    owned, fin, finalize_stmts, record = _run(body())
    assert owned is False
    assert record["stolen"] is True
    assert fin is None
    assert finalize_stmts == 0, "finalize must not touch the DB for a stolen shard"
    assert not any("UPDATE snapshots" in s for s, _ in session.executed)
    out = capsys.readouterr().out
    assert (
        f"[LEASE] stolen: partition {pid} token 7 owner {_OWNER} — skipping finalize"
        in out
    ), out
    assert f"finalize of snapshot {sid} skipped" in out


def test_rowcount_zero_under_our_own_lease_is_the_idempotent_replay(monkeypatch, capsys):
    """A replay after a lost commit ack finds the row already terminal
    under OUR token: not a steal — finalize proceeds as it always did."""
    pid, sid, jid = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()

    def respond(sql, b):
        if "UPDATE snapshot_partitions" in sql:
            return _Result(rowcount=0)
        if "SELECT lease_owner_id, lease_token" in sql:
            return _Result(first=_Row(lease_owner_id=_OWNER, lease_token=7))
        return _Result(first=None)

    session = _Session(respond)
    monkeypatch.setattr(_bw, "async_session_factory", _Factory(session))

    async def body():
        _bind(pid, sid)
        w = _Worker()
        owned = await _bw.BackupWorker._mark_partition_terminal(w, pid, status="COMPLETED")
        fin = await _bw.BackupWorker._finalize_partitioned_snapshot(w, sid, job_id=jid)
        return owned, fin

    owned, fin = _run(body())
    assert owned is True
    assert fin is None  # nothing flips on the fake — but the flip was attempted:
    assert any("UPDATE snapshots s SET" in s for s, _ in session.executed)
    assert "[LEASE]" not in capsys.readouterr().out


def test_stolen_flag_is_scoped_to_the_stolen_shards_snapshot(monkeypatch):
    pid, sid_a, sid_b = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()

    def respond(sql, b):
        if "UPDATE snapshot_partitions" in sql:
            return _Result(rowcount=0)
        if "SELECT lease_owner_id, lease_token" in sql:
            return _Result(first=_Row(lease_owner_id=_OTHER, lease_token=9))
        return _Result(first=None)

    session = _Session(respond)
    monkeypatch.setattr(_bw, "async_session_factory", _Factory(session))

    async def body():
        _bind(pid, sid_a)
        w = _Worker()
        await _bw.BackupWorker._mark_partition_terminal(w, pid, status="FAILED")
        await _bw.BackupWorker._finalize_partitioned_snapshot(w, sid_b)

    _run(body())
    assert any("UPDATE snapshots s SET" in s for s, _ in session.executed), (
        "a steal on snapshot A must not silence finalize for snapshot B"
    )


def test_fence_rests_on_the_token_when_the_claim_had_no_owner(monkeypatch):
    """Heartbeat init failed → lease_owner_id NULL (see _claim_partition);
    the fence must not then reject every write."""
    pid, sid = uuid.uuid4(), uuid.uuid4()
    session = _Session(lambda sql, b: _Result(rowcount=1))
    monkeypatch.setattr(_bw, "async_session_factory", _Factory(session))

    async def body():
        _bind(pid, sid, owner=None)
        return await _bw.BackupWorker._mark_partition_terminal(
            _Worker(), pid, status="COMPLETED",
        )

    assert _run(body()) is True
    (sql, binds), = _partition_updates(session)
    assert "CAST(:__lease_owner AS uuid) IS NULL" in sql and _FENCE_TOKEN in sql
    assert binds["__lease_owner"] is None and binds["__lease_token"] == 7


def test_caller_without_a_bound_lease_keeps_the_legacy_statement(monkeypatch, capsys):
    pid = uuid.uuid4()
    session = _Session(lambda sql, b: _Result(rowcount=0))
    monkeypatch.setattr(_bw, "async_session_factory", _Factory(session))
    owned = _run(_bw.BackupWorker._mark_partition_terminal(
        _Worker(), pid, status="COMPLETED",
    ))
    assert owned is True
    (sql, binds), = _partition_updates(session)
    assert "lease_token" not in sql and "__lease_token" not in binds
    assert not _probes(session)
    assert "[LEASE]" not in capsys.readouterr().out


def test_terminal_exhaustion_release_is_fenced(monkeypatch):
    pid, sid = uuid.uuid4(), uuid.uuid4()

    def respond(sql, b):
        if _TERMINAL_MARK in sql:
            return _Dead("connection gone")
        if "UPDATE snapshot_partitions" in sql:  # the release
            return _Result(rowcount=1)
        return _Result(first=None)

    session = _Session(respond)
    monkeypatch.setattr(_bw, "async_session_factory", _Factory(session))
    import shared.db_retry as _dbr
    monkeypatch.setattr(_dbr, "run_with_db_retry", _once)

    async def body():
        _bind(pid, sid)
        with pytest.raises(_Dead):
            await _bw.BackupWorker._mark_partition_terminal(
                _Worker(), pid, status="FAILED", failure_state={"reason": "x"},
            )

    _run(body())
    rel_sql, rel_binds = next(
        (s, b) for s, b in _partition_updates(session)
        if "SET status            = 'QUEUED'" in s
    )
    where = rel_sql.split("WHERE", 1)[1]
    assert "AND status NOT IN ('COMPLETED', 'FAILED')" in where
    assert _FENCE_OWNER in where and _FENCE_TOKEN in where, where
    assert rel_binds["__lease_owner"] == str(_OWNER) and rel_binds["__lease_token"] == 7


# ─── _requeue_partition_for_retry ──────────────────────────────────────


def test_requeue_is_fenced_and_a_no_match_falls_through(monkeypatch):
    pid, sid = uuid.uuid4(), uuid.uuid4()
    session = _Session(lambda sql, b: _Result(first=None))
    monkeypatch.setattr(_bw, "async_session_factory", _Factory(session))
    import shared.db_retry as _dbr
    monkeypatch.setattr(_dbr, "run_with_db_retry", _once)

    async def body():
        _bind(pid, sid)
        return await _bw.BackupWorker._requeue_partition_for_retry(
            _Worker(), pid, reason="transient_db_error_test", message="boom",
        )

    assert _run(body()) is None, "no match → None → caller's terminal path"
    (sql, binds), = _partition_updates(session)
    assert _REQUEUE_MARK in sql
    where = sql.split("WHERE", 1)[1].split("RETURNING", 1)[0]
    assert "AND status = 'IN_PROGRESS'" in where and "AND retry_count < :cap" in where
    assert _FENCE_OWNER in where and _FENCE_TOKEN in where, where
    assert binds["__lease_owner"] == str(_OWNER) and binds["__lease_token"] == 7
    assert "'{}'::json" in sql, "f-string brace doubling must render the literal intact"


def test_requeue_exhaustion_release_is_fenced(monkeypatch):
    pid, sid = uuid.uuid4(), uuid.uuid4()

    def respond(sql, b):
        if _REQUEUE_MARK in sql:
            return _Dead("connection gone")
        if "UPDATE snapshot_partitions" in sql:  # the release
            return _Result(first=_Row(retry_count=1))
        return _Result(first=None)

    session = _Session(respond)
    monkeypatch.setattr(_bw, "async_session_factory", _Factory(session))
    import shared.db_retry as _dbr
    monkeypatch.setattr(_dbr, "run_with_db_retry", _once)

    async def body():
        _bind(pid, sid)
        return await _bw.BackupWorker._requeue_partition_for_retry(
            _Worker(), pid, reason="transient_db_error_test", message="boom",
        )

    assert _run(body()) == 1
    rel_sql, rel_binds = next(
        (s, b) for s, b in _partition_updates(session) if _REQUEUE_MARK not in s
    )
    where = rel_sql.split("WHERE", 1)[1].split("RETURNING", 1)[0]
    assert "AND status NOT IN ('COMPLETED', 'FAILED')" in where
    assert "AND retry_count < :cap" in where
    assert _FENCE_OWNER in where and _FENCE_TOKEN in where, where
    assert rel_binds["__lease_owner"] == str(_OWNER) and rel_binds["__lease_token"] == 7


# ─── the fence only works if the requeue bumps the token ───────────────


def test_reconciler_step_a_requeue_bumps_the_token_the_fence_checks():
    src = (_REPO_ROOT / "shared" / "reconciler.py").read_text()
    start = src.index("STEP A — orphan partitions")
    requeue = src[start:src.index("never-claimed shards", start)]
    assert "lease_token    = sp.lease_token + 1" in requeue
    assert "lease_owner_id = NULL" in requeue
