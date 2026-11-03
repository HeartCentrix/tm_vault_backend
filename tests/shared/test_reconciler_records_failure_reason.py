"""The reconciler must say WHY it failed a snapshot.

Live RCA (2026-08-05). Corp_Esplanade's ROOM_MAILBOX backup has never
succeeded, and every post-mortem had to be done by grepping 20 worker pods
because the database recorded nothing:

    snapshot 645d0acf: FAILED  item_count=0  extra_data={}  duration_secs=NULL
                       lease_owner_id=NULL   lease_expires_at=NULL  lease_token=3
    job      bae76f3b: FAILED  error_message=NULL  result={}
    both completed_at = 18:26:48.111543  (identical -> one transaction, PG NOW()
                                          is transaction-scoped)

That is STEP B of the reconciler: the dead-lease orphan path in
``shared/reconciler.py``. It matches the evidence exactly — it NULLs
``lease_owner_id``/``lease_expires_at``, bumps ``lease_token + 1`` (so
token 3 was reconciler settles, NOT three worker claims), sets
``completed_at = NOW()``, and never touches ``extra_data`` or
``duration_secs``.

Marking the orphan FAILED is correct — the comment in that branch is right
that deriving COMPLETED would manufacture a false success. The defect is
that it lands a bare ``{}``, so an operator (or the next investigation)
cannot tell a dead-lease orphan from a handler crash from a Graph failure
without log archaeology across every replica. On a backup product the
reason IS the product.

Deliberately NOT stamped: ``degraded_reason``. That key makes
``terminal_status_for_extra_data`` derive PARTIAL, so writing it here would
silently change status derivation for snapshots this path settles
COMPLETED. Only the operator-facing ``failure_reason`` plus a
``reconciler_settled`` marker go in.
"""
from __future__ import annotations

import inspect
import pathlib
import re

import pytest


_RECON = pathlib.Path(__file__).resolve().parents[2] / "shared" / "reconciler.py"
SRC = _RECON.read_text()


def _step_b_snapshot_update() -> str:
    """The STEP B `UPDATE snapshots` statement (dead-lease orphan settle)."""
    i = SRC.index("UPDATE snapshots s")
    j = SRC.index("RETURNING s.id, s.status::text", i)
    return SRC[i:j]


def _step_b_sql_only() -> str:
    """STEP B with `--` comments stripped — assert on executable SQL, not on
    prose that happens to name a key."""
    return "\n".join(
        re.sub(r"--.*$", "", line) for line in _step_b_snapshot_update().splitlines()
    )


def test_step_b_still_nulls_the_lease_and_bumps_the_token():
    """Premise guard: if this stops being the statement that produced the
    live evidence (lease NULLed, token bumped), the rest of this file is
    pinning the wrong code."""
    blk = _step_b_snapshot_update()
    assert "lease_owner_id   = NULL" in blk
    assert "lease_expires_at = NULL" in blk
    assert "lease_token      = s.lease_token + 1" in blk
    assert "completed_at     = NOW()" in blk


def test_step_b_records_a_failure_reason():
    """The whole point: no more bare {}."""
    blk = _step_b_snapshot_update()
    assert "extra_data" in blk, (
        "STEP B must stamp a reason — a FAILED snapshot with extra_data={} "
        "is undiagnosable without grepping every worker pod"
    )
    assert "failure_reason" in blk


def _step_b_extra_data_write() -> str:
    """Just the `extra_data = jsonb_build_object(...)` expression.

    Scoped deliberately. STEP B must not *stamp* degraded_reason, but since
    2026-08-10 it does *read* it in the status CASE (see
    tests/shared/test_reconciler_preserves_degraded_reason.py) — a snapshot
    whose honesty gate already declared degradation must settle PARTIAL,
    not COMPLETED. Asserting substring-absence across the whole statement
    can no longer tell a write from a read, so match the write itself.
    """
    blk = _step_b_sql_only()
    m = re.search(r"extra_data\s*=\s*jsonb_build_object\(", blk)
    assert m, "STEP B no longer builds extra_data with jsonb_build_object"
    # Walk to the matching close paren.
    depth, i = 0, m.end() - 1
    while i < len(blk):
        if blk[i] == "(":
            depth += 1
        elif blk[i] == ")":
            depth -= 1
            if depth == 0:
                return blk[m.start():i + 1]
        i += 1
    raise AssertionError("unbalanced parens in the extra_data write")


def test_step_b_does_not_stamp_degraded_reason():
    """degraded_reason drives terminal_status_for_extra_data -> PARTIAL.
    Writing it here would change the derived status of snapshots this same
    statement settles COMPLETED.

    Reading it to DERIVE that status is the opposite of the hazard and is
    required — this asserts only that it is never written."""
    write = _step_b_extra_data_write()
    assert "degraded_reason" not in write, (
        "STEP B is stamping degraded_reason into extra_data; it may only "
        "read it when deriving status"
    )


def test_step_b_only_reads_degraded_reason_via_the_json_operator():
    """Belt-and-braces on the write/read distinction: every mention of the
    key in executable SQL must be a `->>` read."""
    blk = _step_b_sql_only()
    for m in re.finditer(r"'degraded_reason'", blk):
        before = blk[max(0, m.start() - 40):m.start()]
        assert "->>" in before, (
            "found a non-read occurrence of 'degraded_reason' in STEP B — "
            "it must never be built into extra_data"
        )


def test_step_b_reason_distinguishes_the_orphan_case():
    """A dead-lease orphan (no partitions, no handler ever ran) is a
    different failure from 'all shards failed'. The reason must say which —
    that distinction is exactly what was missing for Corp_Esplanade."""
    blk = _step_b_snapshot_update()
    assert "orphan" in blk.lower(), (
        "the non-partitioned dead-lease branch needs its own reason text"
    )


def test_step_b_preserves_existing_extra_data():
    """A handler may already have stamped suppressed_reason / partitioned /
    a shard's enumeration_failure. The settle must merge, not clobber."""
    blk = _step_b_snapshot_update()
    assert re.search(r"COALESCE\(\s*s\.extra_data::jsonb", blk), (
        "merge onto the current extra_data (COALESCE for NULL), don't overwrite"
    )
    assert "||" in blk, "use the jsonb concat operator to merge"


def test_step_b_sets_duration_secs():
    """It was NULL on the live row, which is what proved fail_snapshot and
    the inline finalize had never run. Populating it keeps that signal
    meaningful instead of ambiguous."""
    blk = _step_b_snapshot_update()
    assert "duration_secs" in blk


def test_reason_is_only_stamped_when_not_already_present():
    """Idempotence: the reconciler re-runs every 60s. A later pass must not
    overwrite a more specific reason a handler recorded."""
    blk = _step_b_sql_only()
    # Concat order is the whole mechanism: `a || b` lets b overwrite a, so
    # the reconciler's object must sit on the LEFT and the row's existing
    # extra_data on the RIGHT for a handler's more specific key to survive.
    fr = blk.index("failure_reason")
    merge = blk.index("|| COALESCE(s.extra_data::jsonb", fr)
    assert merge > fr, (
        "an existing, more specific failure_reason must not be clobbered — "
        "put the reconciler's object on the LEFT of || so existing keys win"
    )
    # And nothing may re-merge the reconciler object after the row's data.
    assert "COALESCE(s.extra_data::jsonb, '{}'::jsonb) || jsonb_build_object" not in blk
