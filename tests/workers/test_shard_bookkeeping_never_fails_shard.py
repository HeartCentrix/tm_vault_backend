"""Failing to COUNT what was already written must never fail a shard.

2026-08-11 02:20:50 prod. Partition CHATS/4 of snapshot 6807df97 ("Chats —
Carlos Romero") was marked terminally FAILED with files_uploaded=0. Its
drain had already returned and all 19,554 messages across 176 of the
shard's 184 chats were COMMITTED — what died was the POST-drain
bookkeeping query,

    SELECT COUNT(*), SUM(content_size) FROM snapshot_items WHERE snapshot_id=...

with asyncpg ConnectionDoesNotExistError ("connection was closed in the
middle of operation"), collateral from our own pool saturation ("QueuePool
limit of size 40 overflow 40 reached", logged 3x for that snapshot minutes
earlier). It sat INSIDE the try whose `except Exception` marks the
partition FAILED.

And FAILED is the one partition status nothing recovers:
  * `_mark_partition_terminal` treats COMPLETED/FAILED as final,
  * `_claim_partition` claims only 'QUEUED' or a stale 'IN_PROGRESS',
  * reconciler STEP A requeues only ('QUEUED','IN_PROGRESS') and its
    never-claimed / poison-pill arms exclude FAILED too,
  * the backup-scheduler stale-sweep is WHERE status='IN_PROGRESS' in
    both arms.
So one dropped TCP connection permanently cost a shard, with zero data
lost.

Two behaviours are pinned here:
  1. structural — only the DRAIN may live inside the failing try; the
     post-drain counters and the settle live in the `else:` clause, whose
     exceptions the drain's `except` cannot catch. This is the shape of
     the bug, so it is the shape of the test.
  2. functional — a bookkeeping failure degrades to 0/0 plus a marker and
     STILL settles through the normal success path, and a transient DB
     error anywhere in the handler hands the shard back as QUEUED instead
     of burning it.
"""
from __future__ import annotations

import ast
import asyncio
import importlib.util
import os
import pathlib
import sys
import time
import uuid
from typing import Dict, List, Optional, Tuple

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_SRC = _MAIN_PATH.read_text()
_spec = importlib.util.spec_from_file_location("bw_shard_bookkeeping", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_shard_bookkeeping"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )


def _run(coro):
    try:
        loop = asyncio.get_event_loop_policy().get_event_loop()
        if loop.is_closed():
            raise RuntimeError("closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


def _worker():
    w = _bw.BackupWorker.__new__(_bw.BackupWorker)
    w.worker_id = "test-worker"
    w._worker_region = "default"
    return w


class ConnectionDoesNotExistError(Exception):
    """Stand-in for asyncpg's — the classifier matches on class name."""


# ─── 1. structural pin: only the drain may be inside the failing try ─────


def _fn(name: str):
    tree = ast.parse(_SRC)
    for node in ast.walk(tree):
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and node.name == name:
            return node
    return None


def _calls(nodes) -> set:
    out = set()
    for node in nodes if isinstance(nodes, list) else [nodes]:
        for n in ast.walk(node):
            if isinstance(n, ast.Call):
                f = n.func
                if isinstance(f, ast.Attribute):
                    out.add(f.attr)
                elif isinstance(f, ast.Name):
                    out.add(f.id)
    return out


def _drain_try(fn_node, drain_calls: set):
    """The Try whose body performs the actual Graph drain."""
    for n in ast.walk(fn_node):
        if isinstance(n, ast.Try) and (_calls(n.body) & drain_calls):
            return n
    return None


@pytest.mark.parametrize(
    "handler,drain_calls",
    [
        ("_process_chats_partition_message", {"_backup_user_content_parallel"}),
        (
            "_process_mail_partition_message",
            {"_backup_user_content_parallel", "_backup_mailboxes_parallel"},
        ),
    ],
)
def test_post_drain_bookkeeping_is_outside_the_failing_try(handler, drain_calls):
    node = _fn(handler)
    assert node is not None, handler
    t = _drain_try(node, drain_calls)
    assert t is not None, f"{handler}: no try wraps the drain"

    body_calls = _calls(t.body)
    else_calls = _calls(t.orelse)

    assert "_shard_bookkeeping_counters" not in body_calls, (
        f"{handler}: the post-drain COUNT is back inside the try whose "
        f"except marks the partition terminally FAILED — this is exactly "
        f"how CHATS/4 of snapshot 6807df97 was lost on 2026-08-11"
    )
    assert "_settle_partition_outcome" not in body_calls, (
        f"{handler}: the settle is inside the failing try, so a DB blip "
        f"while settling re-enters the FAILED arm"
    )
    assert "_shard_bookkeeping_counters" in else_calls, (
        f"{handler}: the post-drain counters must run in the try's `else:` "
        f"clause — an `except` cannot catch what `else` raises"
    )
    assert "_settle_partition_outcome" in else_calls, (
        f"{handler}: the shard must still settle through the normal "
        f"success path after a bookkeeping failure"
    )
    assert len(t.body) <= 2, (
        f"{handler}: {len(t.body)} statements in the failing try — only the "
        f"drain (plus its tenant-slot `async with`) belongs there"
    )


@pytest.mark.parametrize(
    "handler",
    [
        "_process_chats_partition_message",
        "_process_mail_partition_message",
        "_process_onedrive_partition_message",
        "_process_sharepoint_partition_message",
        "_process_groups_partition_message",
        "_process_entra_partition_message",
    ],
)
def test_every_shard_handler_classifies_before_failing_terminally(handler):
    """A transient DB error must not be able to spend a shard on a status
    no sweep recovers. All six handlers route their escape through the
    classifier."""
    node = _fn(handler)
    assert node is not None, handler
    assert "_partition_retry_or_fail" in _calls(node), handler


@pytest.mark.parametrize(
    "handler",
    ["_process_chats_partition_message", "_process_mail_partition_message"],
)
def test_pre_drain_baseline_is_guarded_too(handler):
    """The pre-drain baseline cannot live in the drain's try, but
    unguarded it escaped the handler entirely — leaving the row
    IN_PROGRESS under a lease this worker keeps renewing (LeaseExtender
    is PROCESS liveness), which no sweep can see until the pod rolls."""
    node = _fn(handler)
    assert node is not None, handler
    body = ast.get_source_segment(_SRC, node) or ""
    assert "run_with_db_retry" in body, (
        f"{handler}: the pre-drain COUNT has no retry"
    )
    assert 'phase="pre-drain counters"' in body, (
        f"{handler}: a DB blip on the pre-drain COUNT must be routed "
        f"through _partition_retry_or_fail, not left to escape"
    )


# ─── 2. functional: bookkeeping failure degrades, never fails ────────────


def test_bookkeeping_failure_degrades_to_zero_with_a_marker():
    w = _worker()
    attempts = {"n": 0}

    async def _counter_fn():
        attempts["n"] += 1
        raise ConnectionDoesNotExistError(
            "connection was closed in the middle of operation"
        )

    items, byts, marker = _run(
        w._shard_bookkeeping_counters(
            _counter_fn, partition_id=uuid.uuid4(), label="chats",
        )
    )
    assert (items, byts) == (0, 0)
    assert marker is not None
    assert marker["bookkeeping_unavailable"] is True
    assert marker["reason"] == "ConnectionDoesNotExistError"
    assert marker["transient_db_error"] is True
    assert attempts["n"] > 1, "a transient counter failure must be retried"


def test_bookkeeping_marker_never_claims_a_lost_file():
    """`_finalize_partitioned_snapshot` and reconciler STEP D both derive
    snapshot PARTIAL from failure_state->'files_failed' > 0. A COUNT that
    could not be read is not a lost file, and must not flip the parent."""
    w = _worker()

    async def _counter_fn():
        raise ConnectionDoesNotExistError("connection was closed")

    _i, _b, marker = _run(
        w._shard_bookkeeping_counters(
            _counter_fn, partition_id=uuid.uuid4(), label="mail",
        )
    )
    assert "files_failed" not in marker
    assert "enumeration_incomplete" not in marker


def test_clean_bookkeeping_returns_no_marker():
    w = _worker()

    async def _counter_fn():
        return (19554, 987654321)

    assert _run(
        w._shard_bookkeeping_counters(
            _counter_fn, partition_id=uuid.uuid4(), label="chats",
        )
    ) == (19554, 987654321, None)


def _settle(bookkeeping_state=None, ledger_entry=None):
    w = _worker()
    pid = uuid.uuid4()
    if ledger_entry is not None:
        w._shard_failure_ledger()[str(pid)] = ledger_entry
    marked: List[Tuple] = []
    audits: List[Tuple] = []

    async def _fake_mark(p, *, status, files_uploaded=0, bytes_uploaded=0,
                         failure_state=None):
        marked.append((status, files_uploaded, bytes_uploaded, failure_state))

    async def _fake_audit(action, outcome, **kw):
        audits.append((action, outcome, kw.get("details") or {}))

    w._mark_partition_terminal = _fake_mark  # type: ignore[assignment]
    w._emit_partition_audit = _fake_audit  # type: ignore[assignment]
    status = _run(
        w._settle_partition_outcome(
            pid,
            tenant_id=str(uuid.uuid4()), resource_id=str(uuid.uuid4()),
            snapshot_id=uuid.uuid4(), job_id=uuid.uuid4(),
            partition_type="CHATS",
            files_uploaded=0, bytes_uploaded=0,
            started_at=time.monotonic(),
            bookkeeping_state=bookkeeping_state,
        )
    )
    return status, marked, audits


def test_shard_with_unreadable_counters_still_settles_completed():
    marker = {"bookkeeping_unavailable": True, "reason": "ConnectionDoesNotExistError"}
    status, marked, audits = _settle(bookkeeping_state=marker)
    assert status == "COMPLETED", (
        "the drain succeeded and its items are committed — the shard must "
        "not be failed because the COUNT could not be read"
    )
    assert marked[0][0] == "COMPLETED"
    assert marked[0][3] == marker, "the marker must land on the row for ops"
    assert audits[0][0] == "PARTITION_COMPLETED"
    assert audits[0][2]["bookkeeping_unavailable"] is True


def test_a_real_shard_failure_still_wins_over_the_marker():
    entry = {"reason": "EnumerationFailed", "message": "USER_CHATS: 8 chats failed"}
    marker = {"bookkeeping_unavailable": True}
    status, marked, _audits = _settle(bookkeeping_state=marker, ledger_entry=entry)
    assert status == "FAILED"
    assert marked[0][3]["reason"] == "EnumerationFailed"
    assert marked[0][3]["bookkeeping_unavailable"] is True


# ─── 3. functional: transient DB error → QUEUED, not terminal FAILED ─────


def _retry_or_fail(exc, requeue_result, ledger_entry=None):
    w = _worker()
    pid = uuid.uuid4()
    if ledger_entry is not None:
        w._shard_failure_ledger()[str(pid)] = ledger_entry
    marked: List[Tuple] = []
    audits: List[Tuple] = []
    requeued: List[Dict] = []

    async def _fake_requeue(p, *, reason, message):
        requeued.append({"reason": reason, "message": message})
        return requeue_result

    async def _fake_mark(p, *, status, files_uploaded=0, bytes_uploaded=0,
                         failure_state=None):
        marked.append((status, failure_state))

    async def _fake_audit(action, outcome, **kw):
        audits.append((action, outcome))

    w._requeue_partition_for_retry = _fake_requeue  # type: ignore[assignment]
    w._mark_partition_terminal = _fake_mark  # type: ignore[assignment]
    w._emit_partition_audit = _fake_audit  # type: ignore[assignment]
    outcome = _run(
        w._partition_retry_or_fail(
            pid,
            tenant_id=None, resource_id=None,
            snapshot_id=uuid.uuid4(), job_id=uuid.uuid4(),
            partition_type="CHATS",
            exc=exc, phase="chats drain",
            started_at=time.monotonic(),
        )
    )
    return outcome, marked, audits, requeued, w, pid


def test_transient_db_error_requeues_instead_of_failing_terminally():
    outcome, marked, audits, requeued, _w, _pid = _retry_or_fail(
        ConnectionDoesNotExistError(
            "connection was closed in the middle of operation"
        ),
        requeue_result=1,
    )
    assert outcome == "QUEUED"
    assert marked == [], (
        "a terminal FAILED here is unrecoverable — no claim, no reconciler "
        "step and no scheduler sweep ever looks at a FAILED partition again"
    )
    assert requeued and requeued[0]["reason"].startswith("transient_db_error")
    assert audits[0][0] == "PARTITION_RETRY_QUEUED"


def test_requeue_clears_this_attempts_stale_failure_ledger_entry():
    """The ledger is in-process and keyed by partition_id, so a failure
    recorded by THIS attempt would settle the RETRY as FAILED even if the
    re-drain is clean."""
    outcome, _m, _a, _rq, w, pid = _retry_or_fail(
        ConnectionDoesNotExistError("connection was closed"),
        requeue_result=1,
        ledger_entry={"reason": "EnumerationFailed", "message": "stale"},
    )
    assert outcome == "QUEUED"
    assert str(pid) not in w._shard_failure_ledger()


def test_transient_db_error_fails_terminally_once_the_budget_is_spent():
    """`_requeue_partition_for_retry` returns None when retry_count has hit
    PARTITION_MAX_RETRIES — a genuinely broken shard must still reach a
    terminal state instead of looping."""
    outcome, marked, audits, _rq, _w, _pid = _retry_or_fail(
        ConnectionDoesNotExistError("connection was closed"),
        requeue_result=None,
    )
    assert outcome == "FAILED"
    assert marked[0][0] == "FAILED"
    assert audits[-1][0] == "PARTITION_FAILED"


def test_a_non_db_failure_keeps_the_old_terminal_behaviour():
    outcome, marked, audits, requeued, _w, _pid = _retry_or_fail(
        RuntimeError("Graph returned 403 for every chat"),
        requeue_result=1,
    )
    assert outcome == "FAILED"
    assert requeued == [], "only a DB transient may spend a retry"
    assert marked[0][0] == "FAILED"
    assert marked[0][1]["reason"] == "RuntimeError"
    assert audits[-1][0] == "PARTITION_FAILED"


# ─── 4. the requeue statement itself ─────────────────────────────────────


class _RecordingResult:
    def first(self):
        return (1,)


class _RecordingSession:
    def __init__(self):
        self.executed: List[Tuple[str, Dict]] = []

    async def execute(self, statement, binds: Optional[Dict] = None):
        self.executed.append((str(statement), dict(binds or {})))
        return _RecordingResult()

    async def commit(self):
        pass


class _RecordingFactory:
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


def test_requeue_statement_hands_the_shard_back_claimably(monkeypatch):
    session = _RecordingSession()
    monkeypatch.setattr(_bw, "async_session_factory", _RecordingFactory(session))
    w = _worker()
    out = _run(
        w._requeue_partition_for_retry(
            uuid.uuid4(), reason="transient_db_error_chats drain",
            message="ConnectionDoesNotExistError: connection was closed",
        )
    )
    assert out == 1
    sql, binds = session.executed[0]
    assert "UPDATE snapshot_partitions" in sql
    assert "status           = 'QUEUED'" in sql, (
        "QUEUED is the only status _claim_partition takes unconditionally, "
        "and the one every partition consumer already reads as 'not done'"
    )
    assert "lease_owner_id   = NULL" in sql and "lease_expires_at = NULL" in sql, (
        "the lease must be dropped: LeaseExtender renews every IN_PROGRESS "
        "row this PROCESS owns, so a retained lease hides the row from "
        "reconciler STEP A until the pod rolls"
    )
    assert "retry_count      = retry_count + 1" in sql
    assert "AND status = 'IN_PROGRESS'" in sql, (
        "only a row this worker still holds may be handed back"
    )
    assert "AND retry_count < :cap" in sql, "the retry must be bounded"
    assert binds["cap"] >= 1
