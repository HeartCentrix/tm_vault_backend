"""Integration invariant tests for mail intra-folder sub-sharding.

Spec: docs/superpowers/specs/2026-06-05-mail-intra-folder-subsharding-design.md
Plan: docs/superpowers/plans/2026-06-05-mail-intra-folder-subsharding.md (Task 7)

These exercise the FULL async drain (`_backup_user_content_parallel` →
`_drain_one_folder`) against a fake Microsoft Graph + a real Postgres test
session, asserting the five correctness invariants. They are marked
`integration` because (per pytest.ini) they require docker services (a test
DB) AND a Graph fake/recording harness that does not yet exist in this repo.

Until that harness lands in CI, the equivalent end-to-end verification is the
**canary deploy** (plan Rollout step 2): back up one large mailbox (Vinay),
confirm the giant folder fans into buckets and finishes faster, then confirm
the NEXT incremental is a cheap delta (proves the id-only finalize captured a
valid per-folder deltaLink). Each test below documents exactly what the canary
must show.

The pure-function core (bucket math + decision gates) is already covered,
runnable, and green in tests/workers/test_mail_intra_folder_subshard.py.
"""
from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration

_NEEDS_HARNESS = "needs docker test-DB + Graph fake harness (not yet in repo); validated via canary"


@pytest.mark.skip(reason=_NEEDS_HARNESS)
def test_full_subshard_then_next_run_is_cheap_delta():
    """Case 3 — full backup of a >MIN_BYTES folder fans into N buckets, all
    persisted, and exactly ONE per-folder deltaLink is stored. A second run
    immediately after downloads ZERO message bodies (resumes the deltaLink,
    delta returns empty). Proves token correctness — THE critical canary check.
    """


@pytest.mark.skip(reason=_NEEDS_HARNESS)
def test_big_jump_incremental_subshards_then_cheap():
    """Case 4 — a folder whose size jumps >= INCREMENTAL_JUMP_BYTES since the
    fingerprint baseline sub-shards the jump window in parallel, finalizes one
    deltaLink, and the following run is a cheap delta.
    """


@pytest.mark.skip(reason=_NEEDS_HARNESS)
def test_crash_mid_bucket_advances_no_token():
    """Case 5 — one bucket task raises mid-run. `_drain_one_folder` returns []
    WITHOUT advancing the folder's mail_folder_delta token. Re-run backfills
    the whole window again (idempotent ON CONFLICT) with zero message loss.
    Invariant #1 + #2.
    """


@pytest.mark.skip(reason=_NEEDS_HARNESS)
def test_one_bucket_permanent_failure_no_token_advance():
    """Case 6 — a bucket fails on every attempt → token never advances; the
    other buckets' persisted rows remain (idempotent) and the missing window is
    refetched next run. No partial-but-marked-complete state.
    """


@pytest.mark.skip(reason=_NEEDS_HARNESS)
def test_boundary_message_in_overlap_persisted_once():
    """Case 8 — a message whose receivedDateTime sits exactly on a bucket
    boundary appears in two overlapping buckets but is written exactly once
    (UNIQUE (snapshot_id, external_id, item_type) + ON CONFLICT DO NOTHING).
    Invariant #3 (overlap is a harmless dedupe; never a gap).
    """


@pytest.mark.skip(reason=_NEEDS_HARNESS)
def test_delta_410_gone_on_finalize_falls_back_safely():
    """Case 9 — the id-only finalize delta walk returns 410 Gone (expired
    token). The folder falls back to a safe full re-enumeration; no message
    loss, token re-established. Invariant #5.
    """


@pytest.mark.skip(reason=_NEEDS_HARNESS)
def test_flag_off_is_byte_identical_to_serial_path():
    """Case 10 — with MAIL_INTRA_FOLDER_SUBSHARD_ENABLED=false the folder takes
    the serial `_drain_pages_persist(url, params)` path and the resulting
    snapshot is identical to pre-feature behavior.
    """
