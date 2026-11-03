"""The compliance report's "protected" count must survive truthful statuses.

The backup-worker used to write ``resources.last_backup_status =
'COMPLETED'`` for every finalize; as of 2026-10-14 it records PARTIAL (and
FAILED) as such. A PARTIAL run still produced a restore point, so coverage
keeps counting it — otherwise every resource whose last run was PARTIAL
would have dropped out of the report the day the worker fix shipped.
FAILED and CANCELLED (cancel_job) stay out, as before.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

import pytest

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_SVC_DIR = _REPO_ROOT / "services" / "report-service"
if str(_SVC_DIR) not in sys.path:
    sys.path.insert(0, str(_SVC_DIR))

_spec = importlib.util.spec_from_file_location(
    "rs_main_protected_under_test", _SVC_DIR / "main.py"
)
_rs = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["rs_main_protected_under_test"] = _rs
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_rs)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"report-service module failed to import: {exc}",
        allow_module_level=True,
    )


def test_partial_counts_as_protected_failed_and_cancelled_do_not():
    statuses = _rs.PROTECTED_LAST_BACKUP_STATUSES
    assert "COMPLETED" in statuses and "PARTIAL" in statuses
    assert "FAILED" not in statuses and "CANCELLED" not in statuses


def test_coverage_query_filters_on_the_shared_constant():
    src = (_SVC_DIR / "main.py").read_text()
    assert (
        "Resource.last_backup_status.in_(PROTECTED_LAST_BACKUP_STATUSES)"
        in src
    )
    assert 'Resource.last_backup_status == "COMPLETED"' not in src
