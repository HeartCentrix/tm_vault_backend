"""Regression: Overview /dashboard/backup/size must report protected-data
size and a real per-day growth chart.

Bug (2026-07-14): the headline summed Resource.storage_bytes — a
bytes-WRITTEN metric (dedup + retention pruning undercount; Overview
said 50 GB while one user's OneDrive alone held 124 GB) — and the chart
bucketed each resource's ENTIRE size into its last_backup_at day, so
with daily incrementals every bar except today read zero.

Source-inspection pins on services/dashboard-service/main.py.
"""
from __future__ import annotations

import pathlib

_MAIN = (
    pathlib.Path(__file__).resolve().parents[2]
    / "services" / "dashboard-service" / "main.py"
).read_text()


def _endpoint_src() -> str:
    start = _MAIN.find("async def get_backup_size")
    assert start != -1
    end = _MAIN.find("\n@app.", start)
    return _MAIN[start:end if end != -1 else len(_MAIN)]


def test_headline_uses_protected_rollup_not_storage_bytes():
    src = _endpoint_src()
    assert "_protected_bytes_for_resource" in src
    assert "func.sum(Resource.storage_bytes)" not in src


def test_rollup_is_sql_side_newest_wins():
    assert "GROUP BY COALESCE(si.external_id, si.id::text)" in _MAIN
    assert "array_agg(si.content_size ORDER BY si.created_at DESC)" in _MAIN
    # FAILED snapshots must never contribute (helper filters statuses)
    helper = _MAIN[_MAIN.find("async def _protected_bytes_for_resource"):
                   _MAIN.find("@app.get(\"/api/v1/dashboard/backup/size\")")]
    assert "SnapshotStatus.COMPLETED" in helper
    assert "FAILED" not in helper


def test_chart_buckets_bytes_added_by_completion_day():
    src = _endpoint_src()
    assert "Snapshot.completed_at" in src
    assert "Snapshot.bytes_added" in src
    # the collapsed-onto-today bucketing must stay dead
    assert "Resource.last_backup_at" not in src


def test_rollup_results_cached_per_latest_snapshot():
    assert "_protected_bytes_cache" in _MAIN


def test_projection_fields_for_storage_page_tiles():
    """Storage page squares (2026-08-20): raw byte fields + linear
    projections off the 30-day average daily write growth."""
    src = _endpoint_src()
    assert '"totalBytes": total' in src
    assert '"dailyGrowthBytes"' in src
    # 1-year / 3-year linear projections, rooted at the protected total
    assert "daily_growth * 365" in src
    assert "daily_growth * 1095" in src
    # rate = mean over the full 30-day window, not the 7-day tail
    assert "/ 30.0" in src
