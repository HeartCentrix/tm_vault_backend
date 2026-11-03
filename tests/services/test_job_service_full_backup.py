"""Regression: an explicit full-backup request must be honored.

Bug (found 2026-07-13): job-service downgraded fullBackup=True to False
whenever the resource had any prior backup (`... and not
has_previous_backup`, two sites). Combined with the scheduler's own
`effective_full_backup = not has_previous_backup`, NOTHING could ever
re-issue a full backup for a resource with history — so a drive whose
baseline walk failed (jrue/jlucas OneDrive) was permanently stuck on
tiny incrementals with no operator recourse from the UI.

Contract pinned here: `resolve_full_backup(requested, has_previous)`
returns exactly what the caller asked for. First-backup defaults are the
caller's business (scheduler already sends full for first backups);
history must never veto an explicit request.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# job-service imports sibling modules (chat_export) relative to its own dir.
_SVC_DIR = _REPO_ROOT / "services" / "job-service"
if str(_SVC_DIR) not in sys.path:
    sys.path.insert(0, str(_SVC_DIR))

_MAIN_PATH = _SVC_DIR / "main.py"
_spec = importlib.util.spec_from_file_location("js_main_under_test", _MAIN_PATH)
_js = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["js_main_under_test"] = _js
assert _spec and _spec.loader
_spec.loader.exec_module(_js)  # type: ignore[union-attr]


def test_explicit_full_backup_honored_despite_history():
    # The bug: this returned False when has_previous_backup was True.
    assert _js.resolve_full_backup(True, has_previous_backup=True) is True


def test_full_backup_passthrough_without_history():
    assert _js.resolve_full_backup(True, has_previous_backup=False) is True


def test_incremental_request_stays_incremental():
    assert _js.resolve_full_backup(False, has_previous_backup=True) is False
    assert _js.resolve_full_backup(False, has_previous_backup=False) is False


def test_none_means_incremental():
    assert _js.resolve_full_backup(None, has_previous_backup=True) is False
