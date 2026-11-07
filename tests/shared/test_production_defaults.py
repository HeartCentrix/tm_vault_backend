"""Deployments that never set these flags (TM's k8s configmap) must run the validated paths:
the chat catalogue (no P8 history copy / 4-hour bucket wait), provider work, copy work,
batch-row finalization and chat presentation."""

LEGACY_PATH_FLAGS = ("PROVIDER_WORK_ENABLED", "CHAT_CATALOGUE_WRITER_ENABLED", "COPY_WORK_ENABLED",
                     "BATCH_ROW_REDESIGN_ENABLED", "CHAT_PRESENTATION_ENABLED")


def test_validated_paths_are_on_when_unset(monkeypatch):
    for flag in LEGACY_PATH_FLAGS:
        monkeypatch.delenv(flag, raising=False)
    from shared import chat_presentation, copy_workflow, provider_work
    from shared import config
    fresh = config.Settings()
    assert provider_work.enabled() and copy_workflow.copy_work_enabled()
    assert chat_presentation.chat_presentation_enabled()
    assert fresh.CHAT_CATALOGUE_WRITER_ENABLED and fresh.BATCH_ROW_REDESIGN_ENABLED


def test_explicit_false_still_turns_a_path_off(monkeypatch):
    monkeypatch.setenv("PROVIDER_WORK_ENABLED", "false")
    monkeypatch.setenv("CHAT_CATALOGUE_WRITER_ENABLED", "false")
    from shared import provider_work, config
    assert not provider_work.enabled() and not config.Settings().CHAT_CATALOGUE_WRITER_ENABLED


# Railway-validated tuning (10+10 light/heavy, TUNING-LOG 2026-11-05) as code defaults, so a
# deployment that sets none of these (TM's k8s) runs the same numbers.
import re
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_WORKER = "workers/backup-worker/main.py"


def _getenv_defaults(path, name):
    src = (_ROOT / path).read_text()
    return re.findall(r"""getenv\(\s*["']%s["']\s*,\s*["']([^"']*)["']""" % name, src)


@pytest.mark.parametrize("path,name,value", [
    (_WORKER, "COPY_WORK_CONCURRENCY", "4"),
    (_WORKER, "COPY_WORK_BATCH_ROWS", "2000"),
    (_WORKER, "COPY_WORK_LOCK_TIMEOUT_MS", "2000"),
    (_WORKER, "COPY_WORK_RECOVERY_SECONDS", "5"),
    ("shared/chat_capture.py", "CHAT_CAPTURE_CONCURRENCY", "8"),
    (_WORKER, "CHAT_HC_MSG_CONCURRENCY", "24"),
    ("shared/multi_app_manager.py", "GRAPH_APP_429_THRESHOLD", "3"),
    ("shared/multi_app_manager.py", "GRAPH_APP_BAN_LADDER", "5,30,300,1800"),
    ("shared/multi_app_manager.py", "GRAPH_APP_RESET_AFTER_OK", "50"),
    ("shared/multi_app_manager.py", "GRAPH_APP_RATE_RECOVER_S", "30"),
])
def test_railway_tuned_env_defaults(path, name, value):
    assert set(_getenv_defaults(path, name)) == {value}


def test_catalogue_chat_concurrency_default_matches_railway():
    # Catalogue path (the default chat path) reads USER_CHATS_PARALLEL_CHATS with its own default.
    src = (_ROOT / _WORKER).read_text()
    assert 'concurrency=max(1, int(os.getenv("USER_CHATS_PARALLEL_CHATS", "8")))' in src


RAILWAY_SETTINGS = {
    "CHAT_OBSERVATION_HASH_VERSION": 2,
    "CHATS_PARTITION_MIN_CHATS": 5,
    "CHATS_PARTITION_TARGET_CHATS_PER_SHARD": 2,
    "CHATS_PARTITION_MAX_SHARDS": 8,
    "ONEDRIVE_PARTITION_MIN_BYTES": 1024 ** 3,
    "ONEDRIVE_PARTITION_MIN_FILES": 50,
    "ONEDRIVE_PARTITION_MAX_SHARDS": 8,
    "ONEDRIVE_PARTITION_TARGET_BYTES_PER_SHARD": 128 * 1024 ** 2,
    "MAX_CONCURRENT_PARTITIONS_PER_TENANT": 16,
    "ONEDRIVE_BACKUP_CHECKPOINT_EVERY_FILES": 50,
    "ONEDRIVE_BACKUP_CHECKPOINT_EVERY_BYTES": 256 * 1024 ** 2,
    "ONPREM_RETRY_MAX": 5,
    "GRAPH_APP_PACE_REQS_PER_SEC": 60.0,
    "GRAPH_STREAM_PACE_REQS_PER_SEC": 8.0,
    "GRAPH_POST_THROTTLE_BRAKE_MS": 500,
    "GRAPH_THROTTLE_BACKOFF_SECONDS": [60, 120, 240, 480, 600],
}


def test_settings_defaults_match_railway(monkeypatch):
    for name in RAILWAY_SETTINGS:
        monkeypatch.delenv(name, raising=False)
    from shared import config
    fresh = config.Settings()
    assert {n: getattr(fresh, n) for n in RAILWAY_SETTINGS} == RAILWAY_SETTINGS
