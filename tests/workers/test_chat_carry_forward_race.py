"""USER_CHATS cross-time carry-forward must survive sibling-shard writes
(2026-10-16 Gold 18:07Z RCA).

``_carry_forward_prior_chat_snapshot_items`` copies the previous chat
snapshot's pointer rows into the new one so an incremental drain (delta
cursor, only NEW messages fetched) still yields a full point-in-time
restore point. It guarded idempotency with ``NOT EXISTS`` only, which is
evaluated at SELECT time while ``uq_snapshot_items_snap_ext_type`` fires at
INSERT time. The coordinator of a partitioned USER_CHATS run issues the copy
while its six shards are still upserting pointer rows into the SAME target
snapshot, so on every large resource one row landed in between, the whole
INSERT..SELECT died with ``UniqueViolationError``, the caller logged
"carry-forward failed" and the snapshot finalized COMPLETED with no carried
history at all. Every chat drained incrementally that run kept only its
delta rows: Riley Stillman "Daily Standup Meeting" 8,710 -> 17 messages,
John Lucas "Financial Services Monday Huddle" 16,949 -> 2, Jason Rue
oneOnOne 7,345 -> 60 (3 users, 7 chats, ~37k messages absent; same
signature on 10/12 and 10/14).

Pins:
  * the copy ends in ``ON CONFLICT (snapshot_id, external_id, item_type)
    DO NOTHING`` (P8's race-free shape) while keeping NOT EXISTS as the
    cheap pre-filter;
  * the predecessor must actually hold chat rows (a 0-row placeholder run
    must not restart the chain empty — the mail helper's guard);
  * the call site retries transient DB faults and, when the copy still
    fails, stamps the snapshot degraded + ``chat_carry_forward_failed`` so
    it settles PARTIAL instead of a clean COMPLETED.
"""
from __future__ import annotations

import contextlib
import importlib.util
import pathlib
import re

import pytest

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_SRC = _MAIN_PATH.read_text()


def _cf_src() -> str:
    start = _SRC.find("async def _carry_forward_prior_chat_snapshot_items")
    assert start > 0, "chat carry-forward helper missing"
    end = _SRC.find("\nasync def ", start + 10)
    return _SRC[start:end]


def _call_site_src() -> str:
    start = _SRC.find("# Cross-time carry-forward for USER_CHATS")
    assert start > 0, "chat carry-forward call site missing"
    end = _SRC.find("# ── Partition gate ──", start)
    assert end > start
    return _SRC[start:end]


def _sql_text(src: str) -> str:
    """Concatenate the adjacent string literals of the INSERT so the
    clauses can be matched regardless of line wrapping."""
    return " ".join(re.findall(r'"((?:[^"\\]|\\.)*)"', src))


# ---------------------------------------------------------------- source pins

def test_chat_carry_forward_insert_is_conflict_tolerant():
    sql = _sql_text(_cf_src())
    assert "INSERT INTO snapshot_items" in sql
    # the race-free shape P8 already uses, and it closes the statement
    # (after the NOT EXISTS pre-filter, before the bind-parameter dict)
    conflict = "ON CONFLICT (snapshot_id, external_id, item_type) DO NOTHING"
    assert conflict in sql
    not_exists = sql.find("AND NOT EXISTS (")
    assert 0 < not_exists < sql.find(conflict)
    # NOT EXISTS stays as the cheap pre-filter so a full predecessor is
    # not re-sent row by row to the conflict handler
    assert "dst.snapshot_id = :tsid" in sql


def test_chat_carry_forward_predecessor_must_hold_chat_rows():
    sql = _sql_text(_cf_src())
    # prior-selection subselect: newest COMPLETED/PARTIAL snapshot created
    # before the target THAT HAS chat rows
    prior = sql[sql.find("AND ps.id = ("):sql.find("AND NOT EXISTS (")]
    assert "SELECT s2.id FROM snapshots s2" in prior
    assert "s2.status IN ('COMPLETED', 'PARTIAL')" in prior
    assert "AND EXISTS (" in prior
    assert "x.snapshot_id = s2.id" in prior
    assert "'TEAMS_CHAT_MESSAGE'" in prior
    assert "ORDER BY s2.created_at DESC" in prior
    assert "LIMIT 1" in prior


def test_chat_carry_forward_still_runs_under_the_giant_slot():
    src = _cf_src()
    assert "carry_forward_slot_session(" in src
    assert 'lane="giant"' in src
    assert 'what="chat carry-forward"' in src


def test_call_site_retries_and_flags_failure():
    src = _call_site_src()
    # retried through the shared transient-fault helper
    assert "run_with_db_retry" in src
    assert "_carry_forward_prior_chat_snapshot_items(" in src
    # a failure is no longer only printed: degrade + flag
    failure = src[src.find("carry-forward failed"):]
    assert "_stamp_snapshot_degraded(" in failure
    assert '"chat_carry_forward_failed": True' in failure


# ---------------------------------------------------------------- behaviour

@pytest.fixture(scope="module")
def bw():
    spec = importlib.util.spec_from_file_location("bw_chat_cf_race", _MAIN_PATH)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    assert spec and spec.loader
    try:
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
    except Exception as exc:  # pragma: no cover - import guard
        pytest.skip(f"backup-worker module failed to import: {exc}")
    return mod


class _Result:
    rowcount = 4803


class _FakeSession:
    def __init__(self):
        self.calls = []
        self.committed = False

    async def execute(self, stmt, params=None):
        self.calls.append((str(stmt), params))
        return _Result()

    async def commit(self):
        self.committed = True


async def test_chat_carry_forward_runs_one_conflict_tolerant_insert(bw, monkeypatch):
    sess = _FakeSession()
    slot_calls = []

    @contextlib.asynccontextmanager
    async def fake_slot(key, **kw):
        slot_calls.append((key, kw))
        yield sess

    monkeypatch.setattr(bw, "carry_forward_slot_session", fake_slot)
    n = await bw._carry_forward_prior_chat_snapshot_items(
        tenant_id="t1", resource_id="r1", target_snapshot_id="s1",
    )
    assert n == 4803
    assert sess.committed
    assert len(sess.calls) == 1
    stmt, params = sess.calls[0]
    assert params == {"tsid": "s1", "rid": "r1"}
    assert "ON CONFLICT (snapshot_id, external_id, item_type) DO NOTHING" in stmt
    assert "NOT EXISTS" in stmt
    assert slot_calls == [("r1", {"lane": "giant", "what": "chat carry-forward"})]


async def test_chat_carry_forward_short_circuits_on_missing_ids(bw, monkeypatch):
    called = []

    @contextlib.asynccontextmanager
    async def fake_slot(key, **kw):  # pragma: no cover - must not run
        called.append(key)
        yield _FakeSession()

    monkeypatch.setattr(bw, "carry_forward_slot_session", fake_slot)
    assert await bw._carry_forward_prior_chat_snapshot_items(
        tenant_id="", resource_id="r1", target_snapshot_id="s1",
    ) == 0
    assert await bw._carry_forward_prior_chat_snapshot_items(
        tenant_id="t1", resource_id="r1", target_snapshot_id="",
    ) == 0
    assert called == []
