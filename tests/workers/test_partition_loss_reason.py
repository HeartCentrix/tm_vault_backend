"""A partitioned PARTIAL must say WHY, not just show a status.

Bug (2026-08-13): SHAREPOINT_SITE "IT Projects" finalized PARTIAL twice a
day with ``extra_data == {"partitioned": true, "partition_type":
"SHAREPOINT_DRIVES", ..., "partition_reconciled": true}`` — no
``degraded_reason`` anywhere on the row. The UI reads ``degraded_reason``,
so the operator saw "Partial" with no cause. USER_CHATS rows in the same
feed always carried one ("345 of 445 chat drain(s) failed"), which made the
SharePoint blank look like a UI bug rather than a missing stamp.

Cause: the shard records its loss in ``snapshot_partitions.failure_state``
and still returns COMPLETED (partition status is deliberately a two-value
terminal domain). The finalizer derives PARTIAL from those counters, but
only propagated a reason when a HANDLER had already stamped one — and the
partitioned SharePoint path never does. So the counters knew, and the row
did not.

``file_failure_degraded_reason`` could not be reused: it reads
``snapshot.delta_tokens_json`` on a single-handler run, which a partitioned
run does not populate.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_partition_loss", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_partition_loss"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )


def test_clean_run_gets_no_reason():
    """A run that lost nothing must never be stamped."""
    assert _bw.partition_loss_degraded_reason(
        lost_files=0, shards_lossy=0, shards_failed=0,
        shards_total=4, enumeration_incomplete=False,
    ) is None


def test_lost_files_are_named_with_their_shard_count():
    """The production case: files lost across some shards."""
    r = _bw.partition_loss_degraded_reason(
        lost_files=152, shards_lossy=2, shards_failed=0,
        shards_total=2, enumeration_incomplete=False,
    )
    assert r is not None
    assert "152" in r
    assert "2" in r


def test_failed_shards_are_reported_even_with_no_file_counter():
    """A shard that died outright carries no files_failed number."""
    r = _bw.partition_loss_degraded_reason(
        lost_files=0, shards_lossy=0, shards_failed=3,
        shards_total=6, enumeration_incomplete=False,
    )
    assert r is not None
    assert "3" in r


def test_incomplete_enumeration_is_called_out_separately():
    """Never-listed content is strictly worse than a failed download."""
    r = _bw.partition_loss_degraded_reason(
        lost_files=5, shards_lossy=1, shards_failed=0,
        shards_total=4, enumeration_incomplete=True,
    )
    assert r is not None
    assert "enumeration" in r.lower()


def test_reason_is_a_single_line():
    """It renders in a table cell — no newlines."""
    r = _bw.partition_loss_degraded_reason(
        lost_files=152, shards_lossy=2, shards_failed=1,
        shards_total=4, enumeration_incomplete=True,
    )
    assert r is not None
    assert "\n" not in r
