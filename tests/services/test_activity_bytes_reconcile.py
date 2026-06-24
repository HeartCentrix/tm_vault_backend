"""Activity drilldown must attribute bytes so the per-resource grid
reconciles with the row total.

Bug (user-reported): an incremental Activity row shows e.g. "~200 KB", but
clicking it shows no resource accounting for those bytes. Cause: the row
total SUMs snapshots.bytes_added across the whole batch, while the detail
drilldown showed only the *latest* snapshot per resource (DISTINCT ON), whose
bytes_added is 0 on a sparse/no-op incremental. The detail must instead sum
each resource's bytes_added across its batch snapshots so contributing
resources surface their bytes.
"""
from __future__ import annotations

import pathlib

_AUDIT_MAIN = (
    pathlib.Path(__file__).resolve().parents[2]
    / "services" / "audit-service" / "main.py"
)


def test_drilldown_sums_bytes_added_per_resource():
    src = _AUDIT_MAIN.read_text()
    # The per-resource bytes shown in the drilldown must be the SUM of that
    # resource's bytes_added across the batch (window over resource_id), not a
    # single latest snapshot's bytes_added.
    assert "SUM(s.bytes_added) OVER (PARTITION BY s.resource_id)" in src, (
        "activity drilldown must sum bytes_added per resource so the detail "
        "reconciles with the row's SUM(bytes_added) total"
    )
