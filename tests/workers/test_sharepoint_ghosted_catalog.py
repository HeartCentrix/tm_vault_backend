"""Possible catalog templates remain diagnostic;404doesnot prove backup coverage."""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys

import httpx
import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_ghosted_catalog", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_ghosted_catalog"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )


_CATALOG_URL = "/sites/projects/_catalogs/wp/FilterActions.dwp"
_NORMAL_URL = "/sites/projects/Shared Documents/budget.xlsx"


def _http_error(status: int, srv_rel: str) -> httpx.HTTPStatusError:
    """A real httpx error, shaped exactly like the one the REST pass raises."""
    url = (
        "https://taylormorrison.sharepoint.com/sites/projects/_api/web/"
        f"GetFileByServerRelativeUrl('{srv_rel}')/$value"
    )
    request = httpx.Request("GET", url)
    response = httpx.Response(status, request=request)
    return httpx.HTTPStatusError(
        f"Client error '{status}' for url '{url}'",
        request=request,
        response=response,
    )


def test_404_under_catalogs_is_a_ghost():
    """The exact production case: 19 Web Part Gallery files, 404 forever."""
    assert _bw.is_ghosted_catalog_file(
        _CATALOG_URL, _http_error(404, _CATALOG_URL),
    ) is True


def test_404_outside_catalogs_is_a_real_failure():
    """A missing document is data loss — it must keep failing the run."""
    assert _bw.is_ghosted_catalog_file(
        _NORMAL_URL, _http_error(404, _NORMAL_URL),
    ) is False


def test_non_404_on_a_catalog_path_is_a_real_failure():
    """403/500 on a catalog file is an error, not a ghost."""
    for status in (403, 500, 503):
        assert _bw.is_ghosted_catalog_file(
            _CATALOG_URL, _http_error(status, _CATALOG_URL),
        ) is False


def test_non_http_exception_is_a_real_failure():
    """A timeout or disk error must never be swallowed as a ghost."""
    assert _bw.is_ghosted_catalog_file(
        _CATALOG_URL, OSError("no space left on device"),
    ) is False


def test_ghosted_template_files_do_not_make_a_site_partial():
    """Template files SharePoint serves from the site definition are Microsoft-side gaps, not our
    failure (owner rule 2026-11-04: PARTIAL only for our app or misconfigured permission). They stay
    counted in rest_ghosted for visibility."""
    stats = {"drive_fail": 0, "rest_fail": 0, "enum_fail": 0, "rest_ghosted": 19}
    assert _bw.sharepoint_total_failures(stats) == 0


def test_template_file_size_mismatch_in_a_catalog_is_a_ghost():
    """Local run 2026-11-04: _catalogs/masterpage .js files served a different length than declared."""
    from shared.provider_read import ProviderReadError
    path = "/sites/projects/_catalogs/masterpage/Display Templates/Search/Item_Default.js"
    assert _bw.is_ghosted_catalog_file(path, ProviderReadError("content_length_mismatch")) is True
    assert _bw.is_ghosted_catalog_file(_NORMAL_URL, ProviderReadError("content_length_mismatch")) is False


def test_real_failures_still_count_alongside_ghosts():
    """Ghost accounting must not mask genuine failures in the same run."""
    stats = {"drive_fail": 1, "rest_fail": 2, "enum_fail": 3, "rest_ghosted": 19}
    assert _bw.sharepoint_total_failures(stats) == 6


# ─── the counting seam ────────────────────────────────────────────────────
# The except block that used to do a bare ``stats["rest_fail"] += 1`` now
# delegates here, so the classification is reachable from a test instead of
# being buried inside a 200-line async method.

def test_recording_a_ghost_increments_only_the_ghost_bucket():
    stats = {"rest_fail": 0}
    bucket = _bw.record_sp_rest_failure(
        stats, _CATALOG_URL, _http_error(404, _CATALOG_URL),
    )
    assert bucket == "ghosted"
    assert stats["rest_ghosted"] == 1
    assert stats["rest_fail"] == 0


def test_recording_a_real_failure_increments_rest_fail():
    stats = {"rest_fail": 0}
    bucket = _bw.record_sp_rest_failure(
        stats, _NORMAL_URL, _http_error(404, _NORMAL_URL),
    )
    assert bucket == "failed"
    assert stats["rest_fail"] == 1
    assert stats.get("rest_ghosted", 0) == 0


def test_recording_accumulates_across_calls():
    """19 ghosts + 2 real failures is the shape of a real IT Projects run."""
    stats: dict = {}
    for _ in range(19):
        _bw.record_sp_rest_failure(
            stats, _CATALOG_URL, _http_error(404, _CATALOG_URL),
        )
    for _ in range(2):
        _bw.record_sp_rest_failure(
            stats, _NORMAL_URL, _http_error(404, _NORMAL_URL),
        )
    assert stats["rest_ghosted"] == 19
    assert stats["rest_fail"] == 2
    # Only the real missing documents make the site PARTIAL; templates stay counted as ghosted.
    assert _bw.sharepoint_total_failures(stats) == 2


def test_catalog_404_is_recorded_as_ghosted_not_as_a_failure():
    stats = {}
    _bw.record_sp_rest_failure(stats, _CATALOG_URL, _http_error(404, _CATALOG_URL))
    assert stats["rest_ghosted"] == 1 and _bw.sharepoint_total_failures(stats) == 0
