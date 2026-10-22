"""The reconciler must not erase a handler-declared degradation.

Bug (prod, found 2026-08-10): `degraded_reason` is written on the SNAPSHOT
by the worker's honesty gates — `evaluate_drain_honesty` for chats, the
folder gate for mail. It records loss that happened INSIDE a shard, so a
chats snapshot whose 6 partitions each reached COMPLETED can still have
lost 93% of its chats.

Both of the reconciler's snapshot-status derivations (STEP B's orphan
finalize and STEP D's terminal integrity sweep) rebuilt status from
`snapshot_partitions` alone. STEP D therefore re-derived COMPLETED and
overwrote the gate's PARTIAL ~40s later — and, running every 60s, it could
never lose. Measured on 2026-08-10: **31 snapshots** fleet-wide read
COMPLETED while carrying a `degraded_reason` (e.g. `Chats — John Lucas`,
"769 of 806 chat drain(s) failed"), against only 3 that kept an honest
PARTIAL.

That is not merely cosmetic: `retention_cleanup._restore_rank` scores
COMPLETED=3 / PARTIAL=2, so a restored COMPLETED seats itself as the day's
GFS bucket winner and makes genuinely-complete siblings deletion-eligible.

Two invariants are pinned here, both cheap and both able to regress
silently:

  1. every snapshot-status derivation treats a non-empty `degraded_reason`
     as PARTIAL, and does so BEFORE the all-COMPLETED branch;
  2. STEP D's CASE — which appears twice, once in `SET` and once in the
     `s.status != (...)` guard — stays byte-identical. If the SET derives
     PARTIAL while the guard still derives COMPLETED, the row matches on
     every 60s tick forever and `lease_token` climbs without bound.

Row-level semantics need a real Postgres (enum casts, CTEs,
FOR UPDATE SKIP LOCKED) and live in
tests/shared/test_reconciler_unclaimed_partitions.py. This file runs
everywhere.
"""
from __future__ import annotations

import asyncio
import re
from typing import Any, Dict, List, Optional, Tuple

import pytest


class _StubResult:
    def first(self):
        return None

    def all(self):
        return []


class _RecordingSession:
    """Captures (sql, binds) for every statement the sweep issues."""

    def __init__(self):
        self.executed: List[Tuple[str, Dict]] = []
        self.committed = False

    async def execute(self, statement, binds: Optional[Dict] = None):
        self.executed.append((str(statement), dict(binds or {})))
        return _StubResult()

    async def commit(self):
        self.committed = True

    async def rollback(self):
        pass


def _run(coro: Any):
    """Run on a live loop, installing one if the suite left none behind.
    Mirrors tests/shared/test_reconciler_step_a_guard.py."""
    try:
        loop = asyncio.get_event_loop_policy().get_event_loop()
        if loop.is_closed():
            raise RuntimeError("closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


_DEGRADED_TERM = "degraded_reason"


def _sweep_sql() -> List[str]:
    import shared.reconciler as reconciler

    session = _RecordingSession()
    _run(reconciler.sweep_orphans(session))
    assert session.executed, "sweep issued no statements"
    return [s for s, _ in session.executed]


def _integrity_sql() -> str:
    """STEP D — the terminal-snapshot integrity sweep."""
    sqls = [s for s in _sweep_sql() if "part_agg" in s]
    assert len(sqls) == 1, f"expected exactly one STEP D statement, got {len(sqls)}"
    return sqls[0]


def _orphan_snapshot_sql() -> str:
    """STEP B — orphan-snapshot finalize (derives status, sets completed_at)."""
    sqls = [
        s for s in _sweep_sql()
        if "UPDATE snapshots" in s and "completed_at" in s and "part_agg" not in s
    ]
    assert len(sqls) == 1, f"expected exactly one STEP B statement, got {len(sqls)}"
    return sqls[0]


# ─── invariant 1: degradation is honoured, and honoured early enough ──────

def test_step_d_treats_degraded_reason_as_partial():
    sql = _integrity_sql()
    assert _DEGRADED_TERM in sql, (
        "STEP D re-derives snapshot status from partitions only; a snapshot "
        "whose shards all COMPLETED but which carries degraded_reason will be "
        "flipped back to COMPLETED every 60s"
    )


def test_step_b_treats_degraded_reason_as_partial():
    sql = _orphan_snapshot_sql()
    assert _DEGRADED_TERM in sql, (
        "STEP B finalizes orphaned snapshots; a worker that died after its "
        "honesty gate wrote degraded_reason must not settle COMPLETED"
    )


@pytest.mark.parametrize(
    "getter,label",
    [(_integrity_sql, "STEP D"), (_orphan_snapshot_sql, "STEP B")],
)
def test_degraded_branch_precedes_the_all_completed_branch(getter, label):
    """Ordering is the whole fix. A CASE evaluates top-down, so a degraded
    term placed AFTER the all-COMPLETED branch never fires."""
    sql = getter()
    for m in re.finditer(r"\bCASE\b(.*?)\bEND\b", sql, re.S):
        body = m.group(1)
        if _DEGRADED_TERM not in body:
            continue
        i_deg = body.index(_DEGRADED_TERM)
        i_done = body.find("'COMPLETED'::snapshotstatus")
        assert i_done != -1, f"{label}: CASE has no COMPLETED branch to order against"
        assert i_deg < i_done, (
            f"{label}: the degraded_reason branch must come BEFORE the "
            f"all-COMPLETED branch or it can never fire"
        )
        return
    pytest.fail(f"{label}: no CASE expression mentions {_DEGRADED_TERM}")


def test_degraded_predicate_ignores_an_empty_string():
    """`extra_data` defaults to {} and some paths write ''. An empty reason
    must not demote a genuinely complete snapshot to PARTIAL forever."""
    sql = _integrity_sql()
    assert re.search(
        r"coalesce\(\s*s\.extra_data\s*->>\s*'degraded_reason'\s*,\s*''\s*\)\s*<>\s*''",
        sql,
    ), "guard on a non-empty reason, not merely IS NOT NULL"


def test_degraded_predicate_reads_the_real_column_name():
    """`snapshots` stores this as `extra_data` (json). Sibling models map
    their attribute onto a DB column literally named `metadata`, so raw SQL
    that copies from those would silently reference the wrong column."""
    sql = _integrity_sql()
    assert "s.extra_data ->> 'degraded_reason'" in sql
    assert "s.metadata" not in sql


# ─── invariant 2: the duplicated CASE cannot drift ────────────────────────

def test_step_d_set_and_guard_cases_are_identical():
    """STEP D writes the CASE twice: once in SET, once in the
    `s.status != (...)` no-op guard. If they disagree the UPDATE matches on
    every tick forever, rewriting the row and bumping lease_token."""
    sql = _integrity_sql()
    cases = re.findall(
        r"WHEN pa\.all_failed.*?ELSE s\.status\s*\n\s*END", sql, re.S,
    )
    assert len(cases) == 2, (
        f"expected the STEP D CASE twice (SET + guard), found {len(cases)}"
    )
    a, b = cases
    assert a == b, (
        "STEP D's SET and guard CASE expressions have drifted apart — the "
        "row will be rewritten on every 60s tick forever"
    )
    assert _DEGRADED_TERM in a


def test_step_d_remains_idempotent_via_the_status_guard():
    """The no-op guard is what stops the sweep rewriting unchanged rows."""
    sql = _integrity_sql()
    assert "s.status !=" in sql, (
        "STEP D lost its idempotency guard; it would rewrite every "
        "terminal snapshot on every tick"
    )
