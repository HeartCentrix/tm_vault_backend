"""Regression test for CNPG Pooler (PgBouncer transaction-mode) compat.

Transaction-mode pooling multiplexes many client connections onto a
small pool of Postgres backend connections, swapping which backend
serves a given client connection between transactions. asyncpg's
client-side prepared-statement cache assumes a stable backend and will
raise DuplicatePreparedStatementError / InvalidSQLStatementNameError
under that swap. The DB_PGBOUNCER flag disables both asyncpg's
statement cache and SQLAlchemy's prepared-statement cache via
connect_args when the engine is pointed at the pooler.
"""
from __future__ import annotations

import os

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

from shared.config import settings
from shared.database import _engine_kwargs


def test_pgbouncer_flag_disables_prepared_statement_cache(monkeypatch):
    monkeypatch.setattr(settings, "DB_PGBOUNCER", True)
    kw = _engine_kwargs()
    assert kw["connect_args"]["prepared_statement_cache_size"] == 0
    assert kw["connect_args"]["statement_cache_size"] == 0


def test_default_keeps_cache(monkeypatch):
    monkeypatch.setattr(settings, "DB_PGBOUNCER", False)
    kw = _engine_kwargs()
    assert "prepared_statement_cache_size" not in kw.get("connect_args", {})
