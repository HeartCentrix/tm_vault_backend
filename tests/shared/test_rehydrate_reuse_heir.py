"""P1 — reuse-chain rehydration correctness (the July-2 data-loss linchpin).

When retention (or a manual cancel-revert) deletes a snapshot that is the reuse
CHAIN ROOT for still-live descendants, its rows MUST first be rehydrated into the
earliest surviving descendant ("heir"), the other descendants repointed at the
heir, and the heir promoted to a full snapshot — otherwise every descendant
resolves to a dead pointer and loses its inventory (exactly the July-2 failure).

Two layers of coverage:
  * THIS file — a characterization test with a fake session that locks the
    ORCHESTRATION invariants (no-op when there's no heir; the copy → promote →
    repoint sequence; and P4b: the promote step carries the doomed row's delta
    cursor forward). Runs everywhere, no DB.
  * test_rehydrate_reuse_heir_pg.py — real Postgres SQL semantics (skipif no DB).
"""
from __future__ import annotations

import uuid

import pytest

from shared.retention_cleanup import _rehydrate_reuse_heir


class _Result:
    def __init__(self, first_val):
        self._first = first_val

    def first(self):
        return self._first


class _FakeSession:
    """Records every execute()'d SQL. The heir-selection query (the one with FOR
    UPDATE) returns ``heir_first``; everything else returns an empty result."""

    def __init__(self, heir_first):
        self.executed = []            # list[str] of SQL text, in order
        self._heir_first = heir_first

    async def execute(self, stmt, params=None):
        sql = getattr(stmt, "text", None) or str(stmt)
        self.executed.append(sql)
        if "FOR UPDATE" in sql:
            return _Result(self._heir_first)
        return _Result(None)


@pytest.mark.asyncio
async def test_no_heir_is_a_noop():
    doomed = uuid.uuid4()
    sess = _FakeSession(heir_first=None)   # no surviving descendant
    heir = await _rehydrate_reuse_heir(sess, doomed, doomed_ids={doomed})

    assert heir is None
    # ONLY the heir-selection query ran — no copy / repoint / promote.
    assert len(sess.executed) == 1
    assert "FOR UPDATE" in sess.executed[0]


@pytest.mark.asyncio
async def test_rehydrates_in_copy_then_promote_then_repoint_order():
    # ORDER MATTERS: the heir must be PROMOTED to a full BEFORE the siblings are
    # repointed at it, or the immediate reuse-validation trigger (which requires a
    # chain root to be a full) aborts the whole retention transaction whenever the
    # doomed chain-root has >=2 surviving descendants.
    doomed = uuid.uuid4()
    heir_id = uuid.uuid4()
    sess = _FakeSession(heir_first=_obj(heir_id))
    result = await _rehydrate_reuse_heir(sess, doomed, doomed_ids={doomed})

    assert result == heir_id
    assert len(sess.executed) == 4
    assert "FOR UPDATE" in sess.executed[0]                                   # select heir
    assert "INSERT INTO snapshot_items" in sess.executed[1]                   # copy items
    # promote (nulls reuse cols + carries the cursor) MUST precede the repoint.
    assert "delta_token = COALESCE(h.delta_token, d.delta_token)" in sess.executed[2]
    assert "reuse_of_snapshot_id = CASE" in sess.executed[3]                  # repoint siblings


@pytest.mark.asyncio
async def test_promote_carries_delta_cursor_forward_p4b():
    doomed = uuid.uuid4()
    heir_id = uuid.uuid4()
    sess = _FakeSession(heir_first=_obj(heir_id))
    await _rehydrate_reuse_heir(sess, doomed, doomed_ids={doomed})

    promote = sess.executed[2]  # promotion is the 3rd statement (after select + copy)
    # P4b: the heir inherits the doomed row's incremental cursor when it has none.
    assert "delta_token = COALESCE(h.delta_token, d.delta_token)" in promote
    assert "delta_tokens_json" in promote


class _obj:
    """Minimal stand-in for the heir SELECT row (only ``.id`` is read)."""

    def __init__(self, id_):
        self.id = id_
