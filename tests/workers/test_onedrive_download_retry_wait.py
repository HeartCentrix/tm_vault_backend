"""OneDrive file downloads wait like every other Graph path (shared.graph_ratelimit.retry_wait).

Railway 2026-11-05: a OneDrive download 503 without Retry-After waited 2, 4, 8, 16 s across its
retries (and up to 120 s on a hint), treating a Microsoft server error as throttling. Throttling is
429, or 503 carrying Retry-After (header, or Graph's body retryAfterSeconds); any other 5xx is
retried after a short fixed wait, never above 10 s."""
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
_spec = importlib.util.spec_from_file_location("bw_od_retry_wait", _REPO_ROOT / "workers" / "backup-worker" / "main.py")
_bw = importlib.util.module_from_spec(_spec)
sys.modules["bw_od_retry_wait"] = _bw
try:
    _spec.loader.exec_module(_bw)
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(f"backup-worker module failed to import: {exc}", allow_module_level=True)

wait = _bw._onedrive_download_retry_wait


def test_server_error_without_retry_after_never_waits_over_10s():
    assert all(wait(503, None, b"", attempt) <= 10 for attempt in range(1, 6))


def test_503_with_retry_after_is_throttling_and_honours_microsoft():
    assert wait(503, "30", b"", 1) == 30
    assert wait(503, None, b'{"error": {"code": "serviceNotAvailable", "retryAfterSeconds": 20}}', 1) == 20


def test_429_backs_off_and_stays_bounded():
    assert wait(429, None, b"", 3) == 8
    assert wait(429, "600", b"", 1) == 120
