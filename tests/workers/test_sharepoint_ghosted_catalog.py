"""A ghosted Web Part Gallery file is not a backup failure.

Bug (2026-08-13): SHAREPOINT_SITE "IT Projects" settled PARTIAL on every
single run — 02:11 and 10:12 on 8/13, and the same before that — with
``snapshot_partitions.failure_state`` reading
``{"files_failed": 152, "enumeration_incomplete": false}`` identically on
all four partitions of both runs.

The 152 resolve to **19 distinct files**, every one of them under
``/sites/projects/_catalogs/wp/`` (the Web Part Gallery), and every failure
is the same::

    [SP_LIST_STREAM FAIL] IT Projects/Web Part Gallery/FilterActions.dwp:
    HTTPStatusError: Client error '404 Not Found' for url
    '.../_api/web/GetFileByServerRelativeUrl(
        '/sites/projects/_catalogs/wp/FilterActions.dwp')/$value'

608 such lines in one day's logs, 100% 404. These are *ghosted* files:
provisioned from the site definition on disk, listed in the gallery's
metadata, but with no content stream in the content database. ``/$value``
404s for them permanently and by design — no amount of retrying, and no
permission grant, will ever produce bytes.

Counting them as failures cost two things: the site was reported PARTIAL
twice a day forever (drowning any real degradation in noise), and
``files_failed`` carried a number that no operator could act on.

The classification is deliberately NARROW — 404 *and* under ``_catalogs/``.
A 404 elsewhere is a genuinely missing file and must still fail the run, and
a 403/500 on a catalog path is a real error, not a ghost.
"""
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


def test_ghosted_files_do_not_count_toward_files_failed():
    """The whole point: a ghost must not drag the snapshot to PARTIAL."""
    stats = {"drive_fail": 0, "rest_fail": 0, "enum_fail": 0, "rest_ghosted": 19}
    assert _bw.sharepoint_total_failures(stats) == 0


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
    # Only the 2 real ones may reach files_failed / PARTIAL.
    assert _bw.sharepoint_total_failures(stats) == 2
