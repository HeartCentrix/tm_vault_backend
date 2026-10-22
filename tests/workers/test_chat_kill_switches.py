"""Unit tests for the ITO-9652 chat-backup kill-switches:

  - CHAT_USER_LEVEL_SKIP_ENABLED gates `_chat_user_level_skip_active`,
    the decision behind Layer A's chat-list skip-by-activity (reuse
    cached member/displayName metadata for chats unchanged since the
    `chat_skip_baseline_at` baseline instead of re-fetching members).
  - CHAT_INCREMENTAL_FILTER_ENABLED gates `_chat_incremental_filter_active`,
    the decision behind the per-chat drain skip + `$filter=lastModified
    DateTime gt <cursor>` narrowing (the "primary incremental win" that
    avoids re-pulling every message on every incremental run).

Both are pure decision functions extracted so the on/off behavior can
be pinned without DB or Graph access — same pattern as
test_mail_skip_by_fp.py's `_compute_mail_skip_by_fp`.

The local whole-user skip remains opt-in; message filtering stays enabled.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys
from datetime import datetime, timedelta

import pytest

# ── Set required env vars before any shared.config import ─────────────────
os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_main_under_test_kill_switches", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_main_under_test_kill_switches"] = _bw
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )

_skip_active = _bw.BackupWorker._chat_user_level_skip_active
_filter_active = _bw.BackupWorker._chat_incremental_filter_active
_RESCAN_SECS = 3 * 86400  # USER_CHATS_FULL_RESCAN_DAYS default


# ─── shared.config defaults ───────────────────────────────────────────────


def test_config_defaults_preserve_local_safe_gates():
    from shared.config import settings

    assert settings.CHAT_USER_LEVEL_SKIP_ENABLED is False
    assert settings.CHAT_INCREMENTAL_FILTER_ENABLED is True


# ─── _chat_user_level_skip_active (Layer A gate) ──────────────────────────


def test_user_level_skip_active_when_enabled_and_baseline_fresh():
    now = datetime.utcnow()
    assert _skip_active(
        enabled=True,
        baseline_dt=now - timedelta(hours=1),
        now_dt=now,
        rescan_seconds=_RESCAN_SECS,
    ) is True


def test_user_level_skip_inactive_when_flag_disabled_even_with_fresh_baseline():
    """The kill-switch: flag off must force full processing regardless
    of how fresh the baseline is — 'as if no baseline existed'."""
    now = datetime.utcnow()
    assert _skip_active(
        enabled=False,
        baseline_dt=now - timedelta(hours=1),
        now_dt=now,
        rescan_seconds=_RESCAN_SECS,
    ) is False


def test_user_level_skip_inactive_on_first_run_no_baseline():
    now = datetime.utcnow()
    assert _skip_active(
        enabled=True,
        baseline_dt=None,
        now_dt=now,
        rescan_seconds=_RESCAN_SECS,
    ) is False


def test_user_level_skip_inactive_when_baseline_stale_beyond_rescan_window():
    now = datetime.utcnow()
    assert _skip_active(
        enabled=True,
        baseline_dt=now - timedelta(days=5),
        now_dt=now,
        rescan_seconds=_RESCAN_SECS,
    ) is False


# ─── _chat_incremental_filter_active (drain-skip + $filter gate) ─────────


def test_incremental_filter_active_when_enabled_with_iso_cursor():
    assert _filter_active(
        enabled=True,
        saved_cursor="2026-07-01T00:00:00Z",
    ) is True


def test_incremental_filter_inactive_when_flag_disabled_even_with_cursor():
    """The kill-switch: flag off must bypass both the chat-level drain
    skip and the $filter narrowing — full message fetch every run."""
    assert _filter_active(
        enabled=False,
        saved_cursor="2026-07-01T00:00:00Z",
    ) is False


def test_incremental_filter_inactive_on_first_run_no_cursor():
    assert _filter_active(
        enabled=True,
        saved_cursor=None,
    ) is False


def test_incremental_filter_inactive_for_legacy_url_cursor():
    """A full nextLink/deltaLink URL is a mid-drain pagination-resume
    marker, not a last-backup baseline — it is not incremental scoping
    and must not be gated by this flag either way."""
    assert _filter_active(
        enabled=True,
        saved_cursor="https://graph.microsoft.com/v1.0/chats/19:abc/messages?$skiptoken=xyz",
    ) is False
