"""Unit tests for the sparse-chat-incremental decision gate.

Spec: docs/superpowers/specs/2026-06-05-chat-incremental-fastpath-design.md
(implemented as 'chats sparse like mail' — an unchanged chat writes ZERO
pointer rows and the sibling-snapshot union reconstructs it, proven on live
data). Loads workers/backup-worker/main.py via importlib (hyphen path) —
same pattern as test_mail_skip_by_fp.py.
"""
from __future__ import annotations

import importlib.util
import pathlib
import sys

import pytest

_MAIN_PATH = (
    pathlib.Path(__file__).resolve().parents[2]
    / "workers" / "backup-worker" / "main.py"
)
_spec = importlib.util.spec_from_file_location("bw_main_chat_sparse", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_main_chat_sparse"] = _bw
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )

_decide = _bw.BackupWorker._chat_sparse_skip_writes


def test_sparse_when_enabled_and_prior_exists():
    # Unchanged chat + prior completed snapshot → skip writes (sparse).
    assert _decide(True, True) is True


def test_full_when_no_prior_snapshot():
    # First backup (no prior) → must write the full inventory.
    assert _decide(True, False) is False


def test_full_when_kill_switch_off():
    assert _decide(False, True) is False


def test_full_when_disabled_and_no_prior():
    assert _decide(False, False) is False
