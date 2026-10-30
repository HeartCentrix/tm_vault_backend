"""A lost chat pointer-write is a FAILED drain, not an ok one.

Prod, 2026-08-10. The two chat "reuse someone else's messages" paths each
wrote this snapshot's pointer rows inside their own DB session, per chat:

  * the cross-user claim-skip path (another backup drained the chat first),
  * the skip-by-activity path (Graph says nothing changed since the cursor).

Both swallowed any exception with a `print` and then incremented
``_chat_outcomes["ok"]``. Two consequences, and the second is the damaging
one:

1. Session churn. One pooled connection per chat, with several CHATS shards
   draining concurrently in a pod, exhausted DB_POOL_SIZE(40) +
   DB_MAX_OVERFLOW(40); every acquire then timed out after 30s. Measured
   1,876 ``QueuePool limit of size 40 overflow 40 reached`` events in the
   10:07 UTC tick and 1,002 in the 18:07 tick.

2. The loss was invisible. Those pointer rows are the snapshot's ONLY
   inventory for the chat — the messages themselves live safely in
   ``chat_thread_messages`` — so losing them drops the chat from the restore
   point. Counting it ``ok`` hid that from ``evaluate_drain_honesty``:
   `Chats — Corp_Esplanade` recorded "3 of 186 chat drain(s) failed" while
   377 chats were actually absent from its inventory, a 125x under-report,
   and the snapshot finalized COMPLETED.

Note the fix bounds concurrency rather than growing the pool. 20 pods x
(40+40) already admits up to 1,600 connections against a server
``max_connections`` of 800 behind a pgbouncer Pooler, so a bigger pool just
moves the failure into the database.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import re
import sys

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_chat_pointer_uut", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_chat_pointer_uut"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )


SRC = _MAIN_PATH.read_text()


def _src_no_comments() -> str:
    return "\n".join(
        re.sub(r"#.*$", "", line) for line in SRC.splitlines()
    )


# ─── the concurrency gate ─────────────────────────────────────────────────

def test_pointer_db_gate_exists_and_is_bounded():
    gate = _bw._chat_pointer_db_gate()
    assert isinstance(gate, asyncio.Semaphore)
    from shared.config import settings
    assert settings.chat_pointer_db_concurrency >= 1


def test_pointer_db_gate_is_a_process_singleton():
    """A fresh semaphore per call would bound nothing."""
    assert _bw._chat_pointer_db_gate() is _bw._chat_pointer_db_gate()


def test_pointer_db_gate_default_is_far_below_the_pool():
    """The gate only helps if it is well under DB_POOL_SIZE — otherwise the
    shards can still drain the pool between them."""
    from shared.config import settings
    assert settings.chat_pointer_db_concurrency <= 10, (
        "a gate near the pool size does not bound anything useful"
    )


def test_gate_actually_serialises_beyond_its_limit():
    """Behavioural check on the primitive: with the configured limit N,
    at most N holders run at once."""
    from shared.config import settings
    limit = settings.chat_pointer_db_concurrency
    gate = _bw._chat_pointer_db_gate()
    peak = {"n": 0, "cur": 0}

    async def worker():
        async with gate:
            peak["cur"] += 1
            peak["n"] = max(peak["n"], peak["cur"])
            await asyncio.sleep(0)
            peak["cur"] -= 1

    async def main():
        await asyncio.gather(*[worker() for _ in range(limit * 4)])

    asyncio.new_event_loop().run_until_complete(main())
    assert peak["n"] <= limit


# ─── the accounting: a failed pointer-write must not count as ok ──────────

def _pointer_failure_blocks() -> list[str]:
    """Each reuse path: from its `_pointer_ok` flag to its `return cid`.

    Anchored on the flag rather than the log text — the claim-skip message is
    split across two f-string literals (`"pointer-write on "` /
    `"skip-claim failed: "`), so matching the sentence finds only one of the
    two paths.
    """
    blocks = []
    for m in re.finditer(r"_pointer_ok = True", SRC):
        end = SRC.find("return cid", m.start())
        assert end != -1, "reuse path has no `return cid` terminator"
        blocks.append(SRC[m.start():end])
    return blocks


def test_both_pointer_write_paths_are_covered():
    """There are exactly two reuse paths — claim-skip and skip-by-activity."""
    blocks = _pointer_failure_blocks()
    assert len(blocks) == 2, (
        f"expected 2 pointer-write paths, found {len(blocks)}"
    )


@pytest.mark.parametrize("idx", [0, 1])
def test_pointer_failure_is_counted_failed_not_ok(idx):
    blk = _pointer_failure_blocks()[idx]
    assert "_pointer_ok = False" in blk, (
        "the except block must mark the drain unsuccessful"
    )
    assert '_chat_outcomes["failed"] += 1' in blk, (
        "a lost pointer-write must count as a FAILED drain so "
        "evaluate_drain_honesty can see it"
    )
    # and the ok increment must be conditional, never unconditional
    assert re.search(r"if _pointer_ok:\s*\n\s*_chat_outcomes\[\"ok\"\] \+= 1", blk), (
        "the ok increment must be gated on _pointer_ok"
    )


@pytest.mark.parametrize("idx", [0, 1])
def test_pointer_write_holds_the_gate(idx):
    blk = _pointer_failure_blocks()[idx]
    assert "_chat_pointer_db_gate()" in blk, (
        "the pointer-write DB work must run under the concurrency gate"
    )


def test_no_unconditional_ok_increment_remains_in_a_reuse_path():
    """Guards the regression shape directly: an `ok` bump sitting straight
    after a swallowed pointer-write."""
    src = _src_no_comments()
    for m in re.finditer(r"pointer-write", src):
        window = src[m.start():m.start() + 1400]
        if '_chat_outcomes["ok"] += 1' not in window:
            continue
        i_ok = window.index('_chat_outcomes["ok"] += 1')
        preceding = window[:i_ok]
        assert "if _pointer_ok:" in preceding, (
            "found an unconditional _chat_outcomes['ok'] increment after a "
            "pointer-write — that is the bug that hid 377 missing chats"
        )


def test_thread_freshness_is_still_marked_on_pointer_failure():
    """Deliberate: thread-level freshness describes the SHARED
    chat_thread_messages store, which a failed pointer-write never touched.
    Regressing this would make other users' backups re-drain needlessly."""
    blk = _pointer_failure_blocks()[1]  # skip-by-activity path
    assert "_mark_chat_thread_drained" in blk
    assert "drain_succeeded=True" in blk


def test_honesty_gate_still_reads_the_outcome_counters():
    """Premise guard — if the gate stops consuming these counters, the fix
    above is inert."""
    assert 'drained_ok=_chat_outcomes["ok"]' in SRC
    assert 'failed=_chat_outcomes["failed"]' in SRC
