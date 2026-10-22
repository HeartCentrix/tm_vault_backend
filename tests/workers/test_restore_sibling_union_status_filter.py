"""Regression: restore/export sibling unions must exclude FAILED and
PENDING_DELETION snapshots.

Bug (found 2026-08-04): restore-worker resolves, for each user-picked
snapshot, every sibling snapshot of the same resource with
``created_at <= picked.created_at`` and then dedupes by ``external_id``
with newest-wins ``DISTINCT ON``. That union carried NO status filter at
all, even though its own docstring says it "Mirrors
``_resolve_sibling_snapshot_ids`` in snapshot-service" — and that function
does exclude both states.

Why it corrupts restores rather than merely inflating a count:

  * A FAILED run died mid-upload, so its ``snapshot_items`` rows can point
    at blobs that were never written.
  * A PENDING_DELETION snapshot has been claimed by the retention sweep,
    which is deleting its items and blobs right now.

Either kind is frequently the NEWEST row for a given ``external_id``, so it
WINS the newest-wins dedup and shadows a good older copy. The restore then
writes a missing or empty file for that item and still reports success —
silent data loss at recovery time, which is the worst moment to discover it.

Pins:
  * the excluded set is exactly FAILED + PENDING_DELETION (IN_PROGRESS is
    deliberately allowed: a live sibling's already-written rows are still
    valid newest-wins candidates, matching snapshot-service);
  * EVERY sibling union in the module carries the filter — the counting
    assertion below fails if someone adds a fourth union without it.
"""
from __future__ import annotations

import pathlib
import re

_MAIN = (
    pathlib.Path(__file__).resolve().parents[2]
    / "workers" / "restore-worker" / "main.py"
)
_SRC = _MAIN.read_text()

# The sibling-union bound. Every occurrence marks one union that must be
# status-filtered.
_UNION_ANCHOR = "Snapshot.created_at <= picked.created_at,"
_FILTER = "Snapshot.status.notin_(_SIBLING_EXCLUDED_STATES),"


def test_excluded_states_are_exactly_failed_and_pending_deletion():
    m = re.search(
        r"_SIBLING_EXCLUDED_STATES\s*=\s*\((.*?)\)", _SRC, re.S
    )
    assert m, "_SIBLING_EXCLUDED_STATES tuple not found"
    body = m.group(1)
    assert "SnapshotStatus.FAILED" in body, (
        "FAILED runs' leftover item rows can point at blobs that were "
        "never written — they must not win newest-wins dedup"
    )
    assert "SnapshotStatus.PENDING_DELETION" in body, (
        "PENDING_DELETION snapshots are mid-teardown; their blobs are "
        "being deleted while the restore reads them"
    )
    assert "SnapshotStatus.IN_PROGRESS" not in body, (
        "IN_PROGRESS siblings are deliberately ALLOWED — their "
        "already-written rows are valid candidates, and excluding them "
        "would diverge from snapshot-service's sibling resolution"
    )
    assert "SnapshotStatus.PARTIAL" not in body, (
        "PARTIAL snapshots hold real captured items (the only backup of "
        "the 1.1M-item archive is PARTIAL) — excluding them from a "
        "restore would drop real recoverable data"
    )


def test_every_sibling_union_is_status_filtered():
    """Guards against a future union being added without the filter."""
    unions = _SRC.count(_UNION_ANCHOR)
    filtered = _SRC.count(_FILTER)
    assert unions >= 3, (
        f"expected at least the 3 known sibling unions, found {unions} — "
        "did the anchor text change? update this test deliberately"
    )
    assert filtered == unions, (
        f"{unions} sibling union(s) but only {filtered} status filter(s): "
        "an unfiltered union lets FAILED/PENDING_DELETION rows shadow good "
        "copies during restore"
    )


def test_filter_sits_inside_the_union_query_not_merely_nearby():
    """A filter defined but applied in the wrong place would leave the
    unions unprotected while still satisfying a naive substring count."""
    for match in re.finditer(re.escape(_UNION_ANCHOR), _SRC):
        # The filter must appear within the same select(...) call — i.e.
        # before the where-clause closes a few lines later.
        window = _SRC[match.end():match.end() + 200]
        assert _FILTER in window, (
            "a sibling union's created_at bound is not immediately "
            "followed by the status filter inside the same query; the "
            f"window was: {window[:160]!r}"
        )
