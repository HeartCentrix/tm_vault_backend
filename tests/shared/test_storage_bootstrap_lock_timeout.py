"""Regression tests for the storage-bootstrap DDL lock guard.

`ensure_storage_bootstrap` runs from init_db() on EVERY service boot and
issues `DROP TRIGGER IF EXISTS ... ON snapshots / snapshot_items`, which
takes ACCESS EXCLUSIVE even when it's a no-op. With no `lock_timeout`
the boot transaction queues behind live traffic and — per Postgres
lock-queue fairness — parks every subsequent INSERT behind it. Prod
froze ~5 min on 2026-07-30 (~107 blocked sessions, live backup INSERTs
among them) when a service from this lineage restarted.

The bootstrap must therefore:
  * SET LOCAL lock_timeout to a bounded value BEFORE any DDL, and
  * treat a lock-timeout failure as non-fatal (the DDL is idempotent and
    already present on a migrated database).

Pure-Python: a fake AsyncEngine/connection records the SQL text.
"""
from __future__ import annotations

import asyncio
import re
from typing import Any, Dict, List, Optional

import pytest


# ─── Fake engine / connection ─────────────────────────────────────────


class _FakeResult:
    def __init__(self, scalar: Any = None, row: Any = None):
        self._scalar = scalar
        self._row = row

    def scalar(self):
        return self._scalar

    def first(self):
        return self._row


class _InvariantRow:
    active_backend_id = "11111111-1111-1111-1111-111111111111"
    enabled_count = 2


class _FakeConn:
    """Records every executed statement; optionally raises on one of them."""

    def __init__(self, raise_on: Optional[str] = None,
                 exc: Optional[Exception] = None):
        self.statements: List[str] = []
        self._raise_on = raise_on
        self._exc = exc

    async def execute(self, statement, binds: Optional[Dict] = None):
        sql = str(statement)
        self.statements.append(sql)
        if self._raise_on and self._raise_on in sql:
            raise self._exc or RuntimeError("boom")
        if "pg_try_advisory_lock" in sql:
            return _FakeResult(scalar=True)
        if "FROM system_config sc WHERE sc.id = 1" in sql:
            return _FakeResult(row=_InvariantRow())
        return _FakeResult()


class _FakeEngine:
    def __init__(self, conn: _FakeConn):
        self.conn = conn

    def begin(self):
        conn = self.conn

        class _Ctx:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


def _run(coro):
    """Run on a live loop, installing one if the suite left none behind.

    Other modules in this suite use asyncio.run(), which uninstalls the
    current event loop; get_event_loop() then raises depending on test
    order. Leave the loop installed (not closed) so later modules find a
    usable one too.
    """
    try:
        loop = asyncio.get_event_loop_policy().get_event_loop()
        if loop.is_closed():
            raise RuntimeError("closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


def _first_index(statements: List[str], needle: str) -> int:
    for i, s in enumerate(statements):
        if needle in s:
            return i
    return -1


def _lock_timeout_values(statements: List[str]) -> List[str]:
    out = []
    for s in statements:
        m = re.search(r"lock_timeout\s*=\s*'([^']+)'", s, re.IGNORECASE)
        if m:
            out.append(m.group(1))
    return out


# ─── Tests ────────────────────────────────────────────────────────────


def test_bootstrap_bounds_lock_wait_before_access_exclusive_ddl(monkeypatch):
    """A bounded lock_timeout must be SET before the trigger DDL runs."""
    monkeypatch.delenv("STORAGE_BOOTSTRAP_LOCK_TIMEOUT", raising=False)
    from shared.storage_bootstrap import ensure_storage_bootstrap

    conn = _FakeConn()
    _run(ensure_storage_bootstrap(_FakeEngine(conn)))

    values = _lock_timeout_values(conn.statements)
    assert values, (
        "bootstrap issued ACCESS EXCLUSIVE trigger DDL with no "
        "lock_timeout — it can wait unbounded behind live traffic and "
        "block every INSERT queued behind it (prod freeze 2026-07-30). "
        f"statements={conn.statements[:4]}"
    )

    # Bounded: a plain integer of ms, or an integer with a unit — and no
    # more than 60s worth, otherwise the "guard" is not a guard.
    m = re.fullmatch(r"(\d+)(us|ms|s|min|h|d)?", values[0])
    assert m, f"lock_timeout {values[0]!r} is not a valid Postgres duration"
    unit = m.group(2) or "ms"
    ms = int(m.group(1)) * {"us": 0.001, "ms": 1, "s": 1000,
                            "min": 60000, "h": 3600000, "d": 86400000}[unit]
    assert 0 < ms <= 60000, f"lock_timeout {values[0]!r} is not bounded"

    lt_idx = _first_index(conn.statements, "lock_timeout")
    for table in ("ON snapshots", "ON snapshot_items"):
        ddl_idx = _first_index(conn.statements, "DROP TRIGGER IF EXISTS")
        # Per-table index: the generic lookup above finds only the first
        # DROP TRIGGER, which is neither of these two tables.
        tbl_idx = next(
            (i for i, s in enumerate(conn.statements)
             if "DROP TRIGGER IF EXISTS" in s and s.rstrip().endswith(table)),
            -1,
        )
        assert tbl_idx > -1, f"no DROP TRIGGER ... {table} was issued"
        assert ddl_idx > lt_idx and tbl_idx > lt_idx, (
            f"lock_timeout must be set before the DROP TRIGGER {table} "
            f"(lock_timeout at {lt_idx}, first DDL at {ddl_idx}, "
            f"{table} DDL at {tbl_idx})"
        )


def test_bootstrap_lock_timeout_env_override_and_bad_value_fallback(monkeypatch):
    """Operators can tune the guard; a malformed value must not break boot."""
    from shared.storage_bootstrap import ensure_storage_bootstrap

    monkeypatch.setenv("STORAGE_BOOTSTRAP_LOCK_TIMEOUT", "1500ms")
    conn = _FakeConn()
    _run(ensure_storage_bootstrap(_FakeEngine(conn)))
    assert _lock_timeout_values(conn.statements)[:1] == ["1500ms"], (
        "STORAGE_BOOTSTRAP_LOCK_TIMEOUT was not honored: "
        f"{_lock_timeout_values(conn.statements)}"
    )

    # Garbage must fall back to a valid duration, not blow up startup on
    # every boot of every service.
    monkeypatch.setenv("STORAGE_BOOTSTRAP_LOCK_TIMEOUT", "3 seconds")
    conn = _FakeConn()
    _run(ensure_storage_bootstrap(_FakeEngine(conn)))
    values = _lock_timeout_values(conn.statements)
    assert values, "no lock_timeout set at all"
    assert re.fullmatch(r"\d+(us|ms|s|min|h|d)?", values[0]), (
        f"malformed env value leaked into SQL: {values[0]!r}"
    )


def test_bootstrap_survives_lock_timeout_failure_on_trigger_ddl():
    """A lock-timeout on the trigger DDL is non-fatal: the seed still runs.

    The triggers are idempotent and already installed on a migrated
    database, so losing the race must not crash-loop the service.
    """
    from shared.storage_bootstrap import ensure_storage_bootstrap

    conn = _FakeConn(
        raise_on="DROP TRIGGER IF EXISTS trg_snapshot_items_default_backend",
        exc=RuntimeError(
            "canceling statement due to lock timeout (LockNotAvailableError)"
        ),
    )
    _run(ensure_storage_bootstrap(_FakeEngine(conn)))  # must not raise

    assert _lock_timeout_values(conn.statements), (
        "no lock_timeout guard — the DDL would have blocked instead of "
        "timing out"
    )
    assert _first_index(conn.statements, "INSERT INTO storage_backends") > -1, (
        "seed did not run after the trigger DDL failed"
    )
    assert _first_index(conn.statements, "FROM system_config sc") > -1, (
        "invariant check did not run after the trigger DDL failed"
    )


def test_lock_timeout_is_scoped_to_the_ddl_not_the_seed_upserts():
    """`SET LOCAL` is transaction-scoped, so it must be released after the DDL.

    Only the trigger DDL loop is SAVEPOINT-wrapped. The seed UPSERTs on
    storage_backends and the invariant SELECT are bare, so if the bounded
    lock_timeout is still in force a row-lock conflict (concurrent
    bootstrap / storage_toggle_worker holding the azure-primary row)
    aborts the whole transaction and ensure_storage_bootstrap RAISES —
    where before the guard it waited and succeeded. The operator-facing
    endpoint/config upsert is then skipped. Restore the server default
    for the remainder of the transaction once the DDL is done.
    """
    monkeypatch_free_conn = _FakeConn()
    _run(ensure_bootstrap_for_scope_test(monkeypatch_free_conn))

    stmts = monkeypatch_free_conn.statements
    set_idx = _first_index(stmts, "lock_timeout")
    reset_idx = -1
    for i, s in enumerate(stmts):
        if "lock_timeout" in s and re.search(
            r"lock_timeout\s*=\s*DEFAULT", s, re.IGNORECASE,
        ):
            reset_idx = i
            break
    seed_idx = _first_index(stmts, "INSERT INTO storage_backends")
    last_ddl_idx = max(
        i for i, s in enumerate(stmts)
        if "TRIGGER" in s or "CREATE OR REPLACE FUNCTION" in s
    )

    assert reset_idx > -1, (
        "lock_timeout is never released: SET LOCAL lasts the whole "
        "transaction, so the un-SAVEPOINTed seed UPSERTs and invariant "
        f"check inherit it and can abort the bootstrap. statements={stmts}"
    )
    assert set_idx < last_ddl_idx < reset_idx < seed_idx, (
        "the lock_timeout guard must cover the trigger DDL and nothing "
        f"else (set={set_idx} last_ddl={last_ddl_idx} reset={reset_idx} "
        f"seed={seed_idx})"
    )


def ensure_bootstrap_for_scope_test(conn):
    from shared.storage_bootstrap import ensure_storage_bootstrap
    return ensure_storage_bootstrap(_FakeEngine(conn))
