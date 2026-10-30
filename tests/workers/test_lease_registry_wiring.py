"""Handler-scoped lease renewal is wired at every claim and finalize site.

2026-10-14 DB outage: handlers aborted, their terminal write AND release
failed ("row will strand until this pod exits"), yet the pod's
LeaseExtender kept renewing those rows by owner id for as long as the
process lived — the reconciler needs an expired lease, so 5 RUNNING jobs
/ 9 IN_PROGRESS snapshots were zombies for 12 h.

The worker now registers a row with shared.lease when it takes the lease
(``_claim_partition``, ``create_snapshot``) and unregisters it on the
terminal write, the release, the partition finalize and every exception
exit — in ``finally`` blocks, so a dying handler always unregisters and
its row lapses to the reconciler within LEASE_TTL_S.
"""
from __future__ import annotations

import ast
import asyncio
import importlib.util
import os
import pathlib
import sys
import uuid
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_SRC = _MAIN_PATH.read_text()
_spec = importlib.util.spec_from_file_location("bw_lease_registry", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_lease_registry"] = _bw
assert _spec and _spec.loader
_spec.loader.exec_module(_bw)  # type: ignore[union-attr]

from shared import lease as lease_mod  # noqa: E402
from shared.lease import _reset_registry, register, registered_ids, registry_active  # noqa: E402


def _run(coro):
    try:
        loop = asyncio.get_event_loop_policy().get_event_loop()
        if loop.is_closed():
            raise RuntimeError("closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


@pytest.fixture(autouse=True)
def _clean_registry():
    _reset_registry()
    yield
    _reset_registry()


# ─── fakes ────────────────────────────────────────────────────────────


class _Row:
    def __init__(self, mapping: Dict[str, Any]):
        self._mapping = mapping

    def __getitem__(self, i):
        return list(self._mapping.values())[i]


class _Result:
    def __init__(self, rows):
        self._rows = rows
        self.rowcount = len(rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return list(self._rows)


class _Session:
    def __init__(self, rows: Optional[List[Any]] = None):
        self.executed: List[Tuple[str, Dict]] = []
        self._rows = rows or []

    async def execute(self, statement, binds: Optional[Dict] = None):
        self.executed.append((str(statement), dict(binds or {})))
        return _Result(self._rows)

    async def commit(self):
        pass


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


def _worker():
    w = _bw.BackupWorker.__new__(_bw.BackupWorker)
    w.worker_id = "test-worker"
    w._worker_region = "default"
    return w


async def _retry_that_raises(op, **kw):
    raise ConnectionError("every attempt lost (pool dead)")


# ─── _claim_partition registers ───────────────────────────────────────


def _claim_row(pid: uuid.UUID) -> _Row:
    return _Row({
        "snapshot_id": uuid.uuid4(), "tenant_id": uuid.uuid4(),
        "resource_id": uuid.uuid4(), "job_id": uuid.uuid4(),
        "drive_id": None, "file_ids": None, "payload": {},
        "partition_index": 0, "shard_unit_count": 1, "total_bytes_est": 0,
        "partition_type": "CHATS", "retry_count": 0,
    })


def test_claim_partition_registers_the_row_once_the_claim_is_committed(monkeypatch):
    pid = uuid.uuid4()
    owner = uuid.uuid4()
    me = SimpleNamespace(worker_id="w", _heartbeat=SimpleNamespace(worker_uuid=owner))
    monkeypatch.setattr(_bw, "async_session_factory", _Factory(_Session([_claim_row(pid)])))
    claim = _run(_bw.BackupWorker._claim_partition(me, pid, "w"))
    assert claim is not None
    assert registered_ids("snapshot_partitions") == [str(pid)]
    assert registry_active() is True


def test_claim_partition_refused_registers_nothing(monkeypatch):
    me = SimpleNamespace(worker_id="w", _heartbeat=SimpleNamespace(worker_uuid=uuid.uuid4()))
    monkeypatch.setattr(_bw, "async_session_factory", _Factory(_Session([])))
    assert _run(_bw.BackupWorker._claim_partition(me, uuid.uuid4(), "w")) is None
    assert registered_ids("snapshot_partitions") == []
    assert registry_active() is False


def test_claim_partition_without_a_heartbeat_owner_registers_nothing(monkeypatch):
    # No lease owner stamped (heartbeat init failed) ⇒ nothing to renew.
    me = SimpleNamespace(worker_id="w", _heartbeat=None)
    monkeypatch.setattr(_bw, "async_session_factory", _Factory(_Session([_claim_row(uuid.uuid4())])))
    assert _run(_bw.BackupWorker._claim_partition(me, uuid.uuid4(), "w")) is not None
    assert registry_active() is False


# ─── partition terminal / requeue unregister, including the strand path ─


def test_mark_partition_terminal_unregisters_after_the_terminal_write(monkeypatch):
    pid = uuid.uuid4()
    register("snapshot_partitions", pid)
    session = _Session()
    monkeypatch.setattr(_bw, "async_session_factory", _Factory(session))
    _run(_worker()._mark_partition_terminal(pid, status="COMPLETED"))
    assert any("UPDATE snapshot_partitions" in s for s, _ in session.executed)
    assert registered_ids("snapshot_partitions") == []


def test_mark_partition_terminal_unregisters_when_both_writes_are_lost(monkeypatch, capsys):
    """The 10/14 strand path: terminal write AND release exhausted. The
    row stays IN_PROGRESS in the DB; this process must stop renewing it
    so its lease lapses and the reconciler can sweep it."""
    pid = uuid.uuid4()
    register("snapshot_partitions", pid)
    import shared.db_retry as db_retry
    monkeypatch.setattr(db_retry, "run_with_db_retry", _retry_that_raises)
    with pytest.raises(ConnectionError):
        _run(_worker()._mark_partition_terminal(pid, status="FAILED"))
    assert registered_ids("snapshot_partitions") == []
    out = capsys.readouterr().out
    assert "release also failed" in out and "row will strand" in out


def test_requeue_partition_for_retry_unregisters(monkeypatch):
    pid = uuid.uuid4()
    register("snapshot_partitions", pid)
    monkeypatch.setattr(_bw, "async_session_factory", _Factory(_Session([(1,)])))
    out = _run(_worker()._requeue_partition_for_retry(pid, reason="r", message="m"))
    assert out == 1
    assert registered_ids("snapshot_partitions") == []


def test_requeue_partition_for_retry_unregisters_on_the_strand_path(monkeypatch):
    pid = uuid.uuid4()
    register("snapshot_partitions", pid)
    import shared.db_retry as db_retry
    monkeypatch.setattr(db_retry, "run_with_db_retry", _retry_that_raises)
    with pytest.raises(ConnectionError):
        _run(_worker()._requeue_partition_for_retry(pid, reason="r", message="m"))
    assert registered_ids("snapshot_partitions") == []


# ─── snapshot terminal write unregisters, even when the write is lost ──


def test_write_snapshot_terminal_unregisters_even_when_the_write_is_lost(monkeypatch):
    sid = uuid.uuid4()
    register("snapshots", sid)
    snapshot = _bw.Snapshot(id=sid, status=_bw.SnapshotStatus.FAILED, extra_data={"error": "x"})
    import shared.db_retry as db_retry
    monkeypatch.setattr(db_retry, "run_with_db_retry", _retry_that_raises)
    with pytest.raises(ConnectionError):
        _run(_worker()._write_snapshot_terminal(
            None, snapshot,
            allowed_from=(_bw.SnapshotStatus.IN_PROGRESS, _bw.SnapshotStatus.FAILED),
        ))
    assert registered_ids("snapshots") == []


def test_write_snapshot_terminal_unregisters_after_a_landed_write(monkeypatch):
    sid = uuid.uuid4()
    register("snapshots", sid)
    snapshot = _bw.Snapshot(id=sid, status=_bw.SnapshotStatus.COMPLETED)
    import shared.db_retry as db_retry

    async def landed(op, **kw):
        return True

    monkeypatch.setattr(db_retry, "run_with_db_retry", landed)
    assert _run(_worker()._write_snapshot_terminal(
        None, snapshot, allowed_from=(_bw.SnapshotStatus.IN_PROGRESS,),
    )) is True
    assert registered_ids("snapshots") == []


# ─── structural pins for the sites that need a live DB to exercise ────


def _fn(name: str):
    tree = ast.parse(_SRC)
    for node in ast.walk(tree):
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and node.name == name:
            return node
    raise AssertionError(f"{name} not found")


def _registry_calls(node, fn_name: str) -> List[str]:
    """First-arg table names of every ``fn_name(...)`` call under node."""
    out = []
    for n in ast.walk(node):
        if (isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                and n.func.id == fn_name and n.args
                and isinstance(n.args[0], ast.Constant)):
            out.append(n.args[0].value)
    return out


def _in_finally(node, fn_name: str, table: str) -> bool:
    for n in ast.walk(node):
        if isinstance(n, ast.Try) and table in _registry_calls(
            ast.Module(body=n.finalbody, type_ignores=[]), fn_name,
        ):
            return True
    return False


def test_create_snapshot_registers_both_the_new_row_and_a_reclaimed_prior():
    # New row (lease stamped at creation) + resume path (lease ours).
    assert _registry_calls(_fn("create_snapshot"), "lease_register").count("snapshots") == 2


def test_partition_finalize_unregisters_the_coordinator_snapshot():
    assert "snapshots" in _registry_calls(_fn("_finalize_partitioned_snapshot"), "lease_unregister")


def test_single_backup_fail_path_unregisters_when_the_failed_write_is_lost():
    assert "snapshots" in _registry_calls(_fn("_process_single_backup"), "lease_unregister")


@pytest.mark.parametrize(
    "fn,table",
    [
        ("_mark_partition_terminal", "snapshot_partitions"),
        ("_requeue_partition_for_retry", "snapshot_partitions"),
        ("_write_snapshot_terminal", "snapshots"),
    ],
)
def test_unregister_sits_in_a_finally_so_a_dying_handler_always_unregisters(fn, table):
    assert _in_finally(_fn(fn), "lease_unregister", table), (
        f"{fn}: lease_unregister({table!r}) must run from a finally block — "
        f"an exception exit that keeps the row registered re-creates the "
        f"2026-10-14 zombie (renewed forever, invisible to the reconciler)"
    )


def test_claim_partition_registers_only_after_the_claim_committed():
    node = _fn("_claim_partition")
    src_lines = _SRC.splitlines()
    reg_lines = [
        n.lineno for n in ast.walk(node)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "lease_register"
    ]
    commit_lines = [
        n.lineno for n in ast.walk(node)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "commit"
    ]
    assert reg_lines and commit_lines
    assert min(reg_lines) > max(commit_lines), (
        "registering before the claim commits lets a bump renew a row this "
        "worker never actually took"
    )
    assert "lease_register" in src_lines[min(reg_lines) - 1]
