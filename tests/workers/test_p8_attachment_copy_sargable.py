"""P8 attachment copy must be index-sargable (2026-10-15 RCA).

The per-chat ``_copy_chat_attachment_rows_to_snapshot`` INSERT..SELECT used
``COALESCE(src.parent_external_id, split_part(src.external_id, '::', 1)) IN
(subselect)``, which the planner cannot turn into an index condition: every
call walked all ~19.4M CHAT_ATTACHMENT / CHAT_HOSTED_CONTENT rows of the
tenant (Parallel Bitmap Heap Scan over idx_snapshot_items_parent_ext used
only for its item_type predicate, or an ordered Index Scan on
idx_snapshot_items_tenant_type_time) for ~13 min on the IO-bound primary
while holding one of the six cluster-wide carry-forward slots. ~80% of calls
hit CARRY_FORWARD_WAIT_MAX_S and silently lost their attachment pointer rows;
chat shards took 6-17 h and the Gold parents sat RUNNING at 95%.

Only 272 of 19.4M attachment rows carry a NULL parent_external_id, so the
sargable shape is a UNION ALL of
  (a) parent_external_id = ANY(message ids)              -- the 99.999% branch
  (b) parent_external_id IS NULL AND split_part(...) = ANY(message ids)
Both branches are Bitmap Index Scans on idx_snapshot_items_parent_ext
(measured on prod 2026-10-15: 372 ms, 189 shared buffers, vs ~795 s).
"""
from __future__ import annotations

import contextlib
import importlib.util
import pathlib
import re
import sys

import pytest

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_SRC = _MAIN_PATH.read_text()


def _p8_src() -> str:
    start = _SRC.find("async def _copy_chat_attachment_rows_to_snapshot")
    assert start > 0, "P8 helper missing"
    end = _SRC.find("\nasync def ", start + 10)
    return _SRC[start:end]


def _sql_text(src: str) -> str:
    """Concatenate the adjacent string literals of the INSERT so the
    predicate can be matched regardless of line wrapping."""
    return " ".join(re.findall(r'"((?:[^"\\]|\\.)*)"', src))


# ---------------------------------------------------------------- source pins

def test_p8_predicate_no_longer_wraps_parent_in_coalesce_in_subselect():
    sql = _sql_text(_p8_src())
    assert "COALESCE(src.parent_external_id, split_part(src.external_id, '::', 1)) IN" not in sql


def test_p8_predicate_is_union_of_two_index_sargable_branches():
    sql = _sql_text(_p8_src())
    assert "UNION ALL" in sql
    assert "src.parent_external_id = ANY(" in sql
    assert "src.parent_external_id IS NULL" in sql
    assert "split_part(src.external_id, '::', 1) = ANY(" in sql
    # both branches keep the partial-index predicate so
    # idx_snapshot_items_parent_ext stays eligible
    assert sql.count("src.item_type IN ('CHAT_ATTACHMENT', 'CHAT_HOSTED_CONTENT')") >= 2
    # idempotency contract unchanged
    assert "ON CONFLICT (snapshot_id, external_id, item_type) DO NOTHING" in sql
    # still a DISTINCT ON over the union so forwarded attachments collapse
    assert "DISTINCT ON (u.external_id, u.item_type)" in sql


def test_p8_still_runs_under_the_micro_carry_forward_slot():
    src = _p8_src()
    assert 'carry_forward_slot_session(' in src
    assert 'what="P8 attachment copy"' in src


# ---------------------------------------------------------------- behaviour

@pytest.fixture(scope="module")
def bw():
    spec = importlib.util.spec_from_file_location("bw_p8_sargable", _MAIN_PATH)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    assert spec and spec.loader
    try:
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
    except Exception as exc:  # pragma: no cover - import guard
        pytest.skip(f"backup-worker module failed to import: {exc}")
    return mod


class _Result:
    rowcount = 3


class _FakeSession:
    def __init__(self):
        self.calls = []
        self.committed = False

    async def execute(self, stmt, params=None):
        self.calls.append((str(stmt), params))
        return _Result()

    async def commit(self):
        self.committed = True


async def test_p8_copy_runs_union_insert_in_slot_and_returns_rowcount(bw, monkeypatch):
    sess = _FakeSession()
    slot_calls = []

    @contextlib.asynccontextmanager
    async def fake_slot(key, **kw):
        slot_calls.append((key, kw))
        yield sess

    monkeypatch.setattr(bw, "carry_forward_slot_session", fake_slot)
    n = await bw._copy_chat_attachment_rows_to_snapshot(
        tenant_id="t1", target_snapshot_id="s1", chat_id="19:abc@thread.v2",
    )
    assert n == 3
    assert sess.committed
    assert slot_calls and slot_calls[0][0] == "19:abc@thread.v2"
    assert slot_calls[0][1]["what"] == "P8 attachment copy"
    assert len(sess.calls) == 1
    sql, params = sess.calls[0]
    assert sql.lstrip().upper().startswith("INSERT INTO SNAPSHOT_ITEMS")
    assert "UNION ALL" in sql
    assert "src.parent_external_id = ANY(" in sql
    assert "COALESCE(src.parent_external_id, split_part(src.external_id, '::', 1)) IN" not in sql
    assert params == {"tsid": "s1", "tid": "t1", "cid": "19:abc@thread.v2"}


async def test_p8_copy_short_circuits_without_touching_the_slot(bw, monkeypatch):
    entered = []

    @contextlib.asynccontextmanager
    async def fake_slot(key, **kw):
        entered.append(key)
        yield _FakeSession()

    monkeypatch.setattr(bw, "carry_forward_slot_session", fake_slot)
    assert await bw._copy_chat_attachment_rows_to_snapshot(
        tenant_id="", target_snapshot_id="s1", chat_id="c") == 0
    assert await bw._copy_chat_attachment_rows_to_snapshot(
        tenant_id="t", target_snapshot_id="s1", chat_id="") == 0
    assert entered == []


def test_p8_copy_runs_on_skip_by_activity_arm_too():
    """2026-10-16: the claimed-but-nothing-new arm (Graph says no activity
    since the cursor) wrote the chat's message pointer rows but never copied
    its attachment pointer rows, so every chat routed there — including the
    ~1.3k meeting chats the [CHAT-CLAIM SANITY] override re-drains on every
    run — lost its attachment chips from that restore point. Pin that the
    arm now runs the same P8 copy, with the same failure accounting, as the
    skip-claim arm."""
    start = _SRC.find('label=f"chat_pointer_skip:')
    assert start > 0, "skip-by-activity pointer write missing"
    end = _SRC.find("no activity since cursor", start)
    assert end > start
    arm = _SRC[start:end]
    assert "_copy_chat_attachment_rows_to_snapshot(" in arm
    assert '_chat_att_copy["failed"] += 1' in arm
    assert 'f"skip-by-activity failed: "' in arm


def test_p8_copy_still_runs_on_skip_claim_arm():
    start = _SRC.find('label=f"chat_pointer_reuse:')
    assert start > 0, "skip-claim pointer write missing"
    end = _SRC.find("pointer-write on", start)
    arm = _SRC[start:end]
    assert "_copy_chat_attachment_rows_to_snapshot(" in arm
    assert 'f"skip-claim failed: "' in arm
