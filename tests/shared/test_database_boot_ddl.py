"""Boot-time schema sync must stay trivial and non-blocking.

``init_db`` runs on EVERY service boot (alert/auth/report/search/
resource/snapshot services, azure-workload + discovery workers,
backup-scheduler) and runs its whole index batch inside ONE
``engine.begin()`` with ``SET LOCAL lock_timeout = '2000ms'`` /
``statement_timeout = '30000ms'`` and NO per-statement savepoint
(shared/database.py::_execute_batch). So one slow DDL statement does
not just fail itself — it aborts the transaction, silently reverting
every later index, the savepointed ALTERs and ``create_all``, while the
outer handler's ``wait_for_schema_ready`` still reports success because
the required tables/columns already exist.

``snapshot_items`` is PARTITION BY HASH (snapshot_id) with 16
partitions and is sized for hundreds of millions of rows; a
non-concurrent index build there takes SHARE on the parent and every
partition (blocking worker INSERTs) and cannot finish inside 30s. This
repo already ate one production freeze from boot DDL on that table (see
``_prune_existing_add_columns``: ALTER TABLE ADD COLUMN IF NOT EXISTS
took AccessExclusiveLock and ABBA-deadlocked worker INSERTs on every
boot, crash-looping snapshot-service 2026-07-20/21).

Big-table index builds therefore belong in an alembic migration + a DBA
runbook (``CREATE INDEX CONCURRENTLY`` per partition, outside any
transaction), never in the boot path.
"""
from __future__ import annotations

import asyncio
import os
import re

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

import pytest

import shared.database as db_mod


class _FakeResult:
    def __init__(self, value=None, rows=()):
        self._value = value
        self._rows = list(rows)

    def scalar(self):
        return self._value

    def all(self):
        return list(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None


class _FakeConn:
    """Records every statement init_db issues; answers catalog probes
    as 'already there' so the boot path runs to completion."""

    def __init__(self, log):
        self.log = log

    async def execute(self, stmt, params=None):
        sql = " ".join(str(stmt).split())
        self.log.append(sql)
        if "pg_try_advisory_xact_lock" in sql:
            return _FakeResult(value=True)
        if "to_regclass" in sql or "SELECT EXISTS" in sql:
            return _FakeResult(value=True)
        if "information_schema.columns" in sql:
            return _FakeResult(rows=[])
        return _FakeResult()

    async def run_sync(self, fn, *a, **kw):
        # Base.metadata.create_all — must NOT touch a real DB here.
        return None


class _Ctx:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *exc):
        return False


class _FakeEngine:
    def __init__(self):
        self.statements = []

    def begin(self):
        return _Ctx(_FakeConn(self.statements))

    def connect(self):
        return _Ctx(_FakeConn(self.statements))


def _boot_statements(monkeypatch):
    fake = _FakeEngine()
    monkeypatch.setattr(db_mod, "engine", fake)

    async def _noop(*a, **kw):
        return None

    # Tail helpers that would reach a real DB / storage backend.
    monkeypatch.setattr(db_mod, "_seed_preset_policies_for_existing_tenants", _noop)
    import shared.storage_bootstrap as sb
    monkeypatch.setattr(sb, "ensure_storage_bootstrap", _noop)

    asyncio.run(db_mod.init_db())
    return fake.statements


_CREATE_INDEX = re.compile(r"^\s*CREATE\s+(UNIQUE\s+)?INDEX", re.IGNORECASE)


def test_boot_ddl_never_builds_an_index_on_snapshot_items_blob_path(monkeypatch):
    """A blob_path index on the 16-partition snapshot_items table cannot
    be built inside init_db's 2s-lock / 30s-statement transaction. It
    would abort the transaction on every boot, silently reverting the
    rest of schema sync AND queueing a write-blocking lock on the
    hottest table in the product. It belongs to alembic + the DBA
    runbook only."""
    statements = _boot_statements(monkeypatch)

    offenders = [
        s for s in statements
        if _CREATE_INDEX.match(s) and "snapshot_items" in s and "blob_path" in s
    ]
    assert offenders == [], (
        "init_db issues big-table index DDL on snapshot_items(blob_path) at "
        f"boot: {offenders}"
    )

    # Anti-neuter: the pre-existing index batch must still be there, so
    # this can't be satisfied by emptying index_statements.
    assert any(
        "idx_snapshot_items_tenant_checksum" in s for s in statements
    ), "boot schema sync stopped issuing its pre-existing index batch"


def test_boot_ddl_issues_no_concurrent_index_build(monkeypatch):
    """The other wrong fix: CREATE INDEX CONCURRENTLY cannot run inside
    a transaction block at all (and Postgres rejects it outright on a
    partitioned parent), so smuggling it into init_db's batch turns a
    slow boot into a guaranteed failed one."""
    statements = _boot_statements(monkeypatch)

    offenders = [s for s in statements if "CONCURRENTLY" in s.upper()]
    assert offenders == [], (
        f"CONCURRENTLY inside init_db's transaction: {offenders}"
    )
