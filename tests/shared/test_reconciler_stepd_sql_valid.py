"""STEP D integrity-sweep SQL must be syntactically valid (2026-08-18).

Commit 3ddd0fc appended the counter-restating assignments AFTER the WHERE
clause's guard CASE instead of into the SET list:

    WHERE ... AND s.status != (CASE ... END),
          item_count = GREATEST(...)          -- syntax error at ","

PostgreSQL rejects the statement at PARSE, and because sweep_orphans runs
STEP D on the same session/tick as STEPS A-C, that single error aborted and
ROLLED BACK every orphan-sweep tick fleet-wide (shard requeue, orphan
snapshot/job finalization, batch cascade) from deploy until 2026-08-18 —
silently, one '[ORPHAN_SWEEPER] tick failed' line per minute that log
rotation promptly ate. The repo has no Postgres fixture, so this pins the
statement's SHAPE: every SET assignment must precede the FROM/WHERE tail.
"""
from __future__ import annotations

import pathlib
import re

_SRC = (
    pathlib.Path(__file__).resolve().parents[2] / "shared" / "reconciler.py"
).read_text()


def _step_d() -> str:
    i = _SRC.find("WITH part_agg AS")
    assert i != -1, "STEP D statement not found"
    return _SRC[i: _SRC.find('"""', i)]


def test_counter_assignments_live_in_the_set_list():
    sql = _step_d()
    from_pos = sql.find("FROM part_agg")
    where_pos = sql.find("WHERE s.id")
    for assign in ("item_count = GREATEST", "bytes_total = GREATEST"):
        pos = sql.find(assign)
        assert pos != -1, f"{assign} missing from STEP D"
        assert pos < from_pos < where_pos, (
            f"{assign} sits after the FROM/WHERE tail — that is the 3ddd0fc "
            f"syntax error that killed every sweep tick"
        )


def test_no_assignment_after_the_where_clause():
    sql = _step_d()
    tail = sql[sql.find("WHERE s.id"):]
    # strip SQL comments before scanning: '=' appears in prose freely
    tail_code = re.sub(r"--[^\n]*", "", tail)
    assert not re.search(r",\s*\w+\s*=\s*", tail_code.split("RETURNING")[0].replace("!=", "").replace(">=", "").replace("<=", "").replace(" = ANY", "")), (
        "an assignment-shaped fragment follows the WHERE clause"
    )
