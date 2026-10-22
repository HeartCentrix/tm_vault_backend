"""The RMQ redelivery guard must require a LIVE worker, not a recent row.

Live incident (2026-08-05, Gold 10:07 UTC tick — the failure the operator
flagged). Corp_Esplanade (ROOM_MAILBOX) produced this and nothing else:

  10:12:33.97  snapshot f72b7bbc created, IN_PROGRESS
  10:12:xx     "RMQ redelivery dropped — another worker is actively running
                this backup for job=0bb6293e resource=1f880075
                (snapshot=f72b7bbc started_at=2026-08-05 10:12:33.966985);
                ACKing without re-running."
  10:30:00.57  job + snapshot both marked FAILED

Across ALL backup-worker replicas in a 10-hour window there is not one
``[MAILBOX]`` line and not one "Calling handler for ROOM_MAILBOX" — the
handler never ran. The snapshot row confirms it: item_count 0,
extra_data {}, duration_secs NULL (so neither fail_snapshot nor the
inline finalize nor the stale-sweep ever touched it).

Mechanism: the fresh-IN_PROGRESS guard drops a delivery whenever an
IN_PROGRESS row for (job_id, resource_id) has ``started_at`` within 25
minutes, on the assumption that "another worker is alive and on it". It
never checks whether a worker actually holds the row. When the first
delivery created the row and then died, the redelivery — the retry that
would have fixed it — was ACKed away as a duplicate. By the time the
25-minute window lapsed the message was long gone.

That is self-perpetuating: it recurs every cycle, which is why this
mailbox has 0 items on every snapshot back to at least 2026-07-25 and
``last_backup_at`` has been NULL since it was discovered on 2026-07-15.

The codebase already has the right liveness signal and the stale-snapshot
sweep already trusts it: a live worker renews ``lease_expires_at`` every
120s via LeaseExtender. The guard must use the lease, which still blocks
the duplicate Graph fetch it was written for (a worker mid-drain holds a
renewed lease) while letting a dead attempt be retried.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys
from datetime import datetime, timedelta, timezone

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_redelivery", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_redelivery"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )

drop = _bw.should_drop_redelivery_for_inprogress
NOW = datetime(2026, 8, 5, 10, 20, 0)


def test_live_lease_still_blocks_the_duplicate_fetch():
    """The case the guard was written for: a worker mid-drain (chats can
    take ~10 min) renews its lease every 120s. Must still drop."""
    assert drop(
        lease_expires_at=NOW + timedelta(minutes=8),
        started_at=NOW - timedelta(minutes=10),
        now=NOW,
    ) is True


def test_lapsed_lease_lets_the_retry_through():
    """The worker died: lease stopped being renewed. This delivery IS the
    retry — processing it is the whole point."""
    assert drop(
        lease_expires_at=NOW - timedelta(minutes=1),
        started_at=NOW - timedelta(minutes=6),
        now=NOW,
    ) is False


def test_no_lease_after_the_init_grace_lets_the_retry_through():
    """THE live bug. f72b7bbc had no lease at all and started_at only ~6
    min earlier, so the old `started_at > now-25min` test dropped the
    delivery and the mailbox was never backed up."""
    assert drop(
        lease_expires_at=None,
        started_at=NOW - timedelta(minutes=6),
        now=NOW,
    ) is False


def test_no_lease_inside_the_init_grace_still_drops():
    """A worker that just created the row may not have initialised its
    lease yet. Keep a short grace window so we don't double-fetch during
    startup — but seconds, not 25 minutes."""
    assert drop(
        lease_expires_at=None,
        started_at=NOW - timedelta(seconds=30),
        now=NOW,
    ) is True


def test_grace_boundary_is_inclusive():
    assert drop(
        lease_expires_at=None,
        started_at=NOW - timedelta(seconds=300),
        now=NOW, lease_init_grace_s=300,
    ) is True
    assert drop(
        lease_expires_at=None,
        started_at=NOW - timedelta(seconds=301),
        now=NOW, lease_init_grace_s=300,
    ) is False


def test_missing_started_at_does_not_strand_the_work():
    """Unknown age must not be read as 'someone is on it' — that is the
    failure mode being fixed."""
    assert drop(
        lease_expires_at=None, started_at=None, now=NOW,
    ) is False


def test_timezone_aware_lease_compares_against_naive_now():
    """lease_expires_at is `timestamp with time zone` while started_at is
    `without` — a mixed-awareness comparison raises TypeError and would
    take down the whole intake path."""
    assert drop(
        lease_expires_at=(NOW + timedelta(minutes=5)).replace(tzinfo=timezone.utc),
        started_at=NOW - timedelta(minutes=10),
        now=NOW,
    ) is True
    assert drop(
        lease_expires_at=(NOW - timedelta(minutes=5)).replace(tzinfo=timezone.utc),
        started_at=NOW - timedelta(minutes=10),
        now=NOW,
    ) is False


def test_aware_now_is_also_accepted():
    assert drop(
        lease_expires_at=(NOW + timedelta(minutes=5)).replace(tzinfo=timezone.utc),
        started_at=(NOW - timedelta(minutes=10)),
        now=NOW.replace(tzinfo=timezone.utc),
    ) is True


# ─── wiring ────────────────────────────────────────────────────────────

def test_intake_guard_selects_the_lease_columns():
    """The decision needs lease_expires_at; selecting only started_at is
    what made the guard blind to liveness."""
    src = _MAIN_PATH.read_text()
    block = src[src.index("fresh_inprogress = (await session.execute("):]
    block = block[:block.index("ACKing without re-running.")]
    assert "lease_expires_at" in block, (
        "the fresh-IN_PROGRESS query must read the lease to judge liveness"
    )
    assert "should_drop_redelivery_for_inprogress" in block, (
        "the drop decision must go through the tested helper"
    )


def test_a_processed_redelivery_is_distinguishable_in_the_log():
    """When we fall THROUGH, the log must say the retry is proceeding and
    why. The old code only ever printed 'another worker is actively
    running this backup', which asserted liveness nobody had verified —
    that wrong line is the only trace Corp_Esplanade ever left."""
    src = _MAIN_PATH.read_text()
    start = src.index("fresh_inprogress = (await session.execute(")
    block = src[start:src.index("if fresh_inprogress is not None:", start)]
    assert "RMQ redelivery PROCEEDING" in block, (
        "falling through must be logged distinctly from dropping"
    )
    assert "no live" in block and "lease" in block, (
        "the log must name the lease as the reason it fell through"
    )
