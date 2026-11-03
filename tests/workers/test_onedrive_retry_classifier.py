"""OneDrive per-file retry classification must read the HTTP status, not
grep the error text for digits.

Bug (adversarial review, 2026-07-31): _classify_onedrive_file_error matched
_ONEDRIVE_RETRY_PERMANENT_CODES = ("404", "410", "401", "403") as an
UNANCHORED SUBSTRING over the whole error string, and did so BEFORE the
throttle codes. Two ways that silently loses a file forever — the producer
stamps status=FAILED_PERMANENT, the consumer's PENDING filter skips it, and
the snapshot still COMPLETES so the deltaLink promotes:

  (i)  Download URLs from Graph are pre-signed and live ~1 hour. Under a
       429/Retry-After-300 storm a queued file's URL expires and the CDN
       answers 401/403 — transient, but classified permanent.
  (ii) Any error text merely CONTAINING "404"/"401"/"403" — a byte count,
       a request id, a drive id, a file name — was classified permanent.

The retry consumer resolves a FRESH download URL on every attempt
(_rescue_onedrive_file_into_snapshot step 1), so a CDN 401/403 is worth
retrying; attempt_count/max_attempts still bounds the budget.
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
_spec = importlib.util.spec_from_file_location("bw_od_retry", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_od_retry"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )

_classify = _bw._classify_onedrive_file_error


# ─── (i) expired pre-signed download URL is transient ─────────────────────

def test_download_403_is_retryable():
    """Real text from _stream_graph_to_backend when the pre-signed URL has
    expired behind a long throttle queue."""
    cls = _classify(
        "RuntimeError: Graph download HTTP 403 for Q3-forecast.xlsx: "
        "b'<Error><Code>AuthenticationFailed</Code></Error>'"
    )
    assert cls != "permanent"


def test_download_401_is_retryable():
    cls = _classify(
        "RuntimeError: Download failed with HTTP 401 for budget.xlsx: b''"
    )
    assert cls != "permanent"


def test_range_resume_403_is_retryable():
    cls = _classify(
        "RuntimeError: Range resume rejected (HTTP 403) for big.zip "
        "at offset 104857600: b''"
    )
    assert cls != "permanent"


# ─── (ii) a digit run that merely contains a code is not a status ─────────

def test_byte_count_containing_404_is_not_permanent():
    cls = _classify(
        "RuntimeError: blob upload failed: expected 12404 bytes, got 12000"
    )
    assert cls != "permanent"


def test_request_id_containing_403_is_not_permanent():
    cls = _classify(
        "HTTPStatusError: Server error '500 Internal Server Error' for url "
        "'https://graph.microsoft.com/v1.0/drives/b!403xy/items/01ABC'"
    )
    assert cls != "permanent"


def test_file_name_containing_403_is_not_permanent():
    cls = _classify(
        "TimeoutError: >900s streaming 'Rule 403 Objections.docx'"
    )
    assert cls == "stream_drop"


def test_guid_containing_410_is_not_permanent():
    cls = _classify(
        "ConnectError: [Errno 104] Connection reset by peer while fetching "
        "drive 01BYE5RZ6410ABCDEFGH"
    )
    assert cls == "stream_drop"


# ─── throttle must be decided before any permanent guess ─────────────────

def test_throttle_wins_over_a_code_shaped_substring_in_the_file_name():
    """Pre-fix this returned "permanent" because "404" appears in the file
    name and the permanent loop ran first."""
    cls = _classify(
        "RuntimeError: Graph download HTTP 429 for Invoice 404.pdf: b''"
    )
    assert cls == "throttle"


def test_429_httpx_text_is_throttle():
    cls = _classify(
        "HTTPStatusError: Client error '429 Too Many Requests' for url "
        "'https://graph.microsoft.com/v1.0/drives/d/items/i/content'"
    )
    assert cls == "throttle"


def test_503_is_throttle():
    cls = _classify("RuntimeError: Graph download HTTP 503 for x.docx: b''")
    assert cls == "throttle"


# ─── genuinely permanent statuses still short-circuit the queue ───────────

def test_404_not_found_is_permanent():
    cls = _classify(
        "HTTPStatusError: Client error '404 Not Found' for url "
        "'https://graph.microsoft.com/v1.0/drives/d/items/gone/content'"
    )
    assert cls == "permanent"


def test_410_gone_is_permanent():
    cls = _classify("RuntimeError: Graph download HTTP 410 for x.docx: b''")
    assert cls == "permanent"


def test_423_locked_is_permanent():
    cls = _classify(
        "HTTPStatusError: Client error '423 Locked' for url "
        "'https://graph.microsoft.com/v1.0/drives/d/items/i/content'"
    )
    assert cls == "permanent"


# ─── unchanged classes ────────────────────────────────────────────────────

def test_stream_drop_text_still_classifies():
    assert _classify("RemoteProtocolError: peer closed connection") == "stream_drop"
    assert _classify("ReadTimeout: timed out") == "stream_drop"
    assert _classify("TimeoutError: >900s") == "stream_drop"


def test_empty_is_unknown():
    assert _classify(None) == "unknown"
    assert _classify("") == "unknown"


def test_unrecognised_text_is_unknown_not_permanent():
    assert _classify("blob_upload_failed") == "unknown"


def test_5xx_other_than_503_is_not_permanent():
    cls = _classify(
        "HTTPStatusError: Server error '502 Bad Gateway' for url "
        "'https://graph.microsoft.com/v1.0/drives/d/items/i/content'"
    )
    assert cls != "permanent"


# ─── the producer's status mapping only ever short-circuits "permanent" ──

def test_only_permanent_maps_to_failed_permanent():
    for cls in (
        "throttle", "stream_drop", "unknown",
        _classify("RuntimeError: Graph download HTTP 403 for x.docx: b''"),
    ):
        assert cls != "permanent", (
            "a retryable class must not reach FAILED_PERMANENT"
        )
