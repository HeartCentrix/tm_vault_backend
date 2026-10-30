"""2026-08-18 DB convoy: giant snapshot_items copy statements must take a
cluster-wide advisory slot before running.

N concurrent carry-forward INSERT…SELECTs (100k-700k rows each into the
22M-row/14-index table) convoyed the whole database on LWLock:BufferMapping
— so heavy copies are bounded to CARRY_FORWARD_BUCKETS cluster-wide slots
(transaction-scoped advisory lock: cannot leak).

2026-08-24 amendment (the "pooler disturbance"): the slot must be acquired
via shared.database.carry_forward_slot_session — a pg_TRY_advisory_xact_lock
probe in a fresh short transaction, sleeping OUTSIDE any transaction on a
miss. The original blocking pg_advisory_xact_lock wait sat inside the
caller's transaction and PINNED one pgbouncer server connection per waiter;
one >1h giant copy starved the pool and query_wait_timeout=120s killed every
other TM Vault connection. Source pins below enforce BOTH properties: every
heavy copy/rewrite site takes a slot, and no site (or helper) uses the
blocking lock form.
"""
from __future__ import annotations

import pathlib

_ROOT = pathlib.Path(__file__).resolve().parents[2]
_MAIN = (_ROOT / "workers" / "backup-worker" / "main.py").read_text()
_ARCH = (_ROOT / "workers" / "backup-worker" / "archive_backup.py").read_text()
_DB = (_ROOT / "shared" / "database.py").read_text()


def test_helper_uses_try_lock_xact_scope_and_stable_hash():
    i = _DB.find("async def carry_forward_slot_session")
    assert i != -1, "slot helper must live in shared.database"
    blk = _DB[i - 4000:i + 4000]
    assert "pg_try_advisory_xact_lock" in blk, (
        "must PROBE with the try_ form — the blocking wait pins a pooler "
        "server connection per waiter (2026-08-24 pooler disturbance)"
    )
    assert "hashlib.md5" in _DB[_DB.find("def carry_forward_bucket"):
                                _DB.find("def carry_forward_bucket") + 600], (
        "hash() varies across interpreters (PYTHONHASHSEED unset in prod)"
    )
    # xact scope: the probe is _xact_ locked — released by the caller's
    # commit/rollback; session-scoped advisory locks leak on pgbouncer
    # transaction pooling.
    assert "pg_try_advisory_lock(" not in _DB, (
        "session-scoped advisory locks leak under transaction pooling"
    )


def test_no_blocking_carry_forward_lock_anywhere():
    for name, src in (("main.py", _MAIN), ("archive_backup.py", _ARCH),
                      ("shared/database.py", _DB)):
        # The blocking form pins a pooler slot per waiter. Only the try_
        # probe is allowed (strip try_ occurrences, then look for the rest).
        residue = src.replace("pg_try_advisory_xact_lock", "")
        assert "pg_advisory_xact_lock(420018" not in residue, (
            f"{name}: blocking carry-forward advisory lock reintroduced"
        )
        assert "SELECT pg_advisory_xact_lock(:cls" not in residue, (
            f"{name}: blocking carry-forward advisory lock reintroduced"
        )


def _gated(src: str, func_marker: str, window: int = 4000) -> bool:
    i = src.find(func_marker)
    assert i != -1, f"{func_marker} not found"
    return "carry_forward_slot_session(" in src[i:i + window]


def test_all_heavy_copy_sites_take_a_slot():
    assert _gated(_MAIN, "async def _carry_forward_prior_chat_snapshot_items")
    assert _gated(_MAIN, "async def _carry_forward_prior_file_versions", 2500)
    assert _gated(_ARCH, "async def _default_carry_forward", 3000)
    # both chatTopic normalization passes rewrite metadata across ALL of a
    # resource's items — same herd amplifier
    assert _MAIN.count('what="chatTopic normalize"') == 1
    assert _MAIN.count('what="chatTopic legacy migrate"') == 1
    # P8 per-chat attachment copy — thousands of tenant-wide scans per run
    assert _gated(_MAIN, "async def _copy_chat_attachment_rows_to_snapshot")


def test_archive_slot_wait_counts_as_stall_guard_progress():
    # A run queued for a carry-forward slot is alive, not stalled: the
    # archive call site must thread _mark_progress into the slot wait, or
    # a long queue (real under load) is aborted by the stall guard
    # (ARCHIVE_STALL_ABORT_S — 9000 in prod config since 2026-08-25;
    # code default 5400).
    i = _ARCH.find("carried = await carry_forward(")
    assert i != -1
    assert "on_wait=_mark_progress" in _ARCH[i - 600:i + 600], (
        "archive carry-forward must tick the stall-guard progress stamp "
        "while queued for a slot"
    )
