"""Config defaults + queue routing for the USER_ARCHIVE (Exchange Online
Archive) Tier-2 workload — Task 2.1.

ARCHIVE_BACKUP_ALLOWED_UPNS optionally restricts discovery to a pilot.
The portable default is empty (all enabled users), preserving local coverage.
"""
from __future__ import annotations

import os

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

from shared.config import Settings, settings
from shared.routing_fence import correct_queue_for


def test_archive_backup_enabled_default_true():
    assert settings.ARCHIVE_BACKUP_ENABLED is True


def test_archive_graph_version_default():
    assert settings.ARCHIVE_GRAPH_VERSION == "beta"


def test_archive_import_timeout_default():
    assert settings.ARCHIVE_IMPORT_TIMEOUT_S == 120


def test_archive_folder_concurrency_default():
    assert settings.ARCHIVE_FOLDER_CONCURRENCY == 8


def test_archive_export_concurrency_default():
    assert settings.ARCHIVE_EXPORT_CONCURRENCY == 8


def test_archive_allowed_upns_default_preserves_local_coverage(monkeypatch):
    monkeypatch.delenv("ARCHIVE_BACKUP_ALLOWED_UPNS", raising=False)
    s = Settings()
    assert s.ARCHIVE_BACKUP_ALLOWED_UPNS == []


def test_archive_allowed_upns_empty_string_means_everyone(monkeypatch):
    monkeypatch.setenv("ARCHIVE_BACKUP_ALLOWED_UPNS", "")
    s = Settings()
    assert s.ARCHIVE_BACKUP_ALLOWED_UPNS == []


def test_archive_allowed_upns_whitespace_only_means_everyone(monkeypatch):
    monkeypatch.setenv("ARCHIVE_BACKUP_ALLOWED_UPNS", "   ")
    s = Settings()
    assert s.ARCHIVE_BACKUP_ALLOWED_UPNS == []


def test_archive_allowed_upns_parses_comma_separated_stripped_lowercased(monkeypatch):
    monkeypatch.setenv("ARCHIVE_BACKUP_ALLOWED_UPNS", "A@X.com , b@y.com")
    s = Settings()
    assert s.ARCHIVE_BACKUP_ALLOWED_UPNS == ["a@x.com", "b@y.com"]


def test_archive_allowed_upns_drops_empties(monkeypatch):
    monkeypatch.setenv("ARCHIVE_BACKUP_ALLOWED_UPNS", "a@x.com,,  ,b@y.com")
    s = Settings()
    assert s.ARCHIVE_BACKUP_ALLOWED_UPNS == ["a@x.com", "b@y.com"]


def test_user_archive_routes_to_heavy_queue():
    assert correct_queue_for("USER_ARCHIVE") == "backup.heavy"
