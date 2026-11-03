"""Regression: OneDrive "Recent" can be expressed as a bounded window.

TMV-016 (QA sweep 2026-08-11): "Recovery › OneDrive › Recent lists the
entire drive (8,295 items) instead of recent files."

Verified against the live deployment first, because half the finding is
already wrong. The ORDERING works today — measured in-cluster against
snapshot a9ce8054 (jrue's USER_ONEDRIVE anchor, 8,295 deduped files):

    ?page=1&size=5                  → "+Connect to New Data Source.odc"  0.08 s
    ?page=1&size=5&sort=created_desc → "Screenshot 2026-08-11 090118.png"
                                        createdDateTime 2026-08-11T16:01:24Z, 0.21 s

…and Recovery.tsx:6368 already sends `sort=created_desc` for that view.

What the backend had NO way to express was the WINDOW. `totalElements`
stayed 8,295 for every sort, so "Recent" was the whole drive in a different
order and scrolling walked every file. `createdSince` / `modifiedSince` are
that missing half; the count and the page share one WHERE, so
`totalElements` now reports the window.

Frontend wiring (the other half of TMV-016):
    GET …/onedrive?sort=created_desc&createdSince=30d&size=50
"""
from __future__ import annotations

import importlib.util
import inspect
import pathlib
import sys
from datetime import datetime, timedelta

import pytest
from fastapi import HTTPException

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "services" / "snapshot-service" / "main.py"
_spec = importlib.util.spec_from_file_location("snap_main_recency", _MAIN_PATH)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["snap_main_recency"] = _mod
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_mod)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import-env guard
    pytest.skip(f"snapshot-service failed to import: {exc}",
                allow_module_level=True)

_parse_since = _mod._parse_since


# ── the window parser ────────────────────────────────────────────────────


def test_no_window_means_no_predicate():
    assert _parse_since(None) is None
    assert _parse_since("") is None
    assert _parse_since("   ") is None


def test_relative_windows():
    for spec, seconds in (("30d", 30 * 86400), ("12h", 12 * 3600), ("90m", 5400)):
        got = datetime.strptime(_parse_since(spec), "%Y-%m-%dT%H:%M:%SZ")
        expected = datetime.utcnow() - timedelta(seconds=seconds)
        assert abs((got - expected).total_seconds()) < 5, spec


def test_absolute_iso_is_normalised_to_graph_zulu():
    assert _parse_since("2026-08-01T00:00:00Z") == "2026-08-01T00:00:00Z"
    assert _parse_since("2026-08-01T00:00:00") == "2026-08-01T00:00:00Z"


def test_offset_aware_input_is_converted_to_utc():
    """A browser sending a local-offset instant must not shift the window."""
    assert _parse_since("2026-08-01T02:00:00+02:00") == "2026-08-01T00:00:00Z"


def test_garbage_is_a_400_not_a_500():
    for bad in ("yesterday", "30x", "2026-13-45", "d"):
        with pytest.raises(HTTPException) as exc:
            _parse_since(bad)
        assert exc.value.status_code == 400


def test_output_format_matches_the_stored_graph_format():
    """The comparison is lexicographic against
    `metadata->'raw'->>'createdDateTime'`, which Graph writes as fixed-width
    Zulu. A different width or an offset suffix would silently mis-compare.
    """
    out = _parse_since("7d")
    assert len(out) == 20 and out.endswith("Z") and out[10] == "T"


# ── the endpoint offers and applies the window ───────────────────────────


def test_onedrive_declares_both_recency_params():
    params = inspect.signature(_mod.list_snapshot_onedrive).parameters
    assert "createdSince" in params, (
        "FastAPI drops undeclared query params silently — an undeclared "
        "window is a window that does nothing (same failure mode as QA-04)"
    )
    assert "modifiedSince" in params
    assert "sort" in params


def test_onedrive_forwards_the_window_into_the_paged_query():
    src = inspect.getsource(_mod.list_snapshot_onedrive)
    assert "extra_where=" in src and "extra_params=" in src
    assert "createdDateTime" in src and "lastModifiedDateTime" in src


def test_the_window_is_applied_inside_the_dedup_so_the_count_matches():
    """`totalElements` must describe the window, not the whole drive — that
    is the actual TMV-016 symptom (8,295 either way)."""
    src = inspect.getsource(_mod._paged_latest_items)
    where_build = src.split("where_sql = ", 1)[0]
    assert "where_parts.append(extra_where)" in where_build, (
        "the recency predicate must join the shared WHERE that BOTH the "
        "COUNT subquery and the page query are built from"
    )


def test_the_window_is_part_of_the_cache_key():
    """Two windows over one sibling set must not share a cached total."""
    src = inspect.getsource(_mod._paged_latest_items)
    assert "extra_where, tuple(sorted((extra_params or {}).items()))" in src, (
        "filter_key must include the recency predicate or a 30d request "
        "will read the 7d request's cached count"
    )


def test_sort_modes_are_still_offered():
    """The ordering half already worked and must keep working — Recent is
    sort + window, not window alone."""
    src = inspect.getsource(_mod.list_snapshot_onedrive)
    assert 'sort == "created_desc"' in src
    assert 'sort == "modified_desc"' in src
