"""Tests for _stage_rows and _finalize_discovery helpers.

These pin the incremental-staging refactor (Task 2): _persist_discovery_rows
was split into _stage_rows (chunk INSERT) and _finalize_discovery (MERGE +
stale-mark + finalize).  The module has heavy deps (aio_pika, sqlalchemy,
shared.config) so we load it via importlib by file path and set required env
vars before import.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys
import uuid

import pytest

# ── Set required env vars before any shared.config import ─────────────────
os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

# ── Add shared and worker dirs to path ────────────────────────────────────
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "workers" / "discovery-worker"))

_WORKER_PATH = _REPO_ROOT / "workers" / "discovery-worker" / "main.py"

_spec = importlib.util.spec_from_file_location(
    "discovery_worker_main",
    _WORKER_PATH,
)


class FakeResult:
    def __init__(self, scalar=0, rows=None):
        self._s = scalar
        self._rows = rows or []

    def scalar(self):
        return self._s

    def all(self):
        return list(self._rows)


class FakeDB:
    def __init__(self):
        self.calls = []

    async def execute(self, stmt, params=None):
        text = getattr(stmt, "text", str(stmt))
        self.calls.append((text, params))
        return FakeResult(0)

    async def commit(self):
        self.calls.append(("COMMIT", None))


@pytest.mark.asyncio
async def test_stage_rows_chunks_and_inserts():
    mod = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(mod)

    mod.settings.DISCOVERY_STAGE_CHUNK_SIZE = 2
    db = FakeDB()
    run_id = uuid.uuid4()
    tenant_id = uuid.uuid4()
    rows = [
        {
            "tenant_id": str(tenant_id),
            "resource_type": "USER",
            "external_id": str(i),
            "display_name": f"u{i}",
            "email": None,
            "metadata": {},
            "resource_status": "DISCOVERED",
            "resource_hash": "h",
            "azure_subscription_id": None,
            "azure_resource_group": None,
            "azure_region": None,
            "discovered_at": None,
        }
        for i in range(5)
    ]

    staged = await mod._stage_rows(db, run_id, tenant_id, rows)

    assert staged == 5
    inserts = [c for c in db.calls if "INSERT INTO resource_discovery_staging" in c[0]]
    # 5 rows / chunk_size 2 → chunks of [2, 2, 1] = 3 execute calls
    assert len(inserts) == 3
    # Every chunk's params list has run_id prepended to each row
    assert all(p[0]["run_id"] == run_id for _, p in inserts)


@pytest.mark.asyncio
async def test_stage_rows_empty_returns_zero():
    mod = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(mod)

    mod.settings.DISCOVERY_STAGE_CHUNK_SIZE = 100
    db = FakeDB()
    run_id = uuid.uuid4()
    tenant_id = uuid.uuid4()

    staged = await mod._stage_rows(db, run_id, tenant_id, [])

    assert staged == 0
    inserts = [c for c in db.calls if "INSERT INTO resource_discovery_staging" in c[0]]
    assert len(inserts) == 0


@pytest.mark.asyncio
async def test_discover_and_stage_scopes_frees_memory():
    """_discover_and_stage_scopes streams scopes into staging without holding
    the full resource list in memory.  Asserts that fetched_count, staged total,
    successful resource types, and attempted_scope_count are all correct for a
    ['users', 'groups'] run.

    The function now opens per-scope sessions via async_session_factory() so we
    monkeypatch mod.async_session_factory to return an async context manager
    yielding a FakeDB (unit-level, no real DB).
    """
    mod = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(mod)

    # DISCOVERY_SCOPE_DEFINITIONS maps:
    #   "users"  -> method "discover_users"  -> resource_types {ENTRA_USER}
    #   "groups" -> method "discover_groups" -> resource_types {ENTRA_GROUP, M365_GROUP}
    class FakeGraph:
        async def discover_users(self, **kwargs):
            return [{"id": "u0"}, {"id": "u1"}, {"id": "u2"}]

        async def discover_groups(self, **kwargs):
            return [{"id": "g1"}]

    # Track staged row count via a monkeypatched _stage_rows
    staged_total = 0

    async def fake_stage_rows(db, run_id, tenant_id, rows):
        nonlocal staged_total
        staged_total += len(rows)
        db.calls.append(("INSERT INTO resource_discovery_staging", [{"run_id": run_id}] if rows else []))
        return len(rows)

    # _prepare_staging_rows maps resource dicts -> staging row dicts (trivial passthrough for test)
    def fake_prepare(tenant_id, resources):
        return list(resources)

    mod._stage_rows = fake_stage_rows
    mod._prepare_staging_rows = fake_prepare
    mod.settings.DISCOVERY_SCOPE_MAX_CONCURRENCY = 1

    # Monkeypatch async_session_factory so per-scope sessions use FakeDB
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def fake_session_factory():
        yield FakeDB()

    mod.async_session_factory = fake_session_factory

    run_id = uuid.uuid4()
    tenant_id = uuid.uuid4()

    fetched, successful_types, attempted = await mod._discover_and_stage_scopes(
        FakeGraph(), ["users", "groups"], run_id, tenant_id
    )

    assert fetched == 4, f"Expected 4 fetched, got {fetched}"
    assert staged_total == 4, f"Expected 4 staged, got {staged_total}"
    assert attempted == 2, f"Expected 2 attempted scopes, got {attempted}"

    # ENTRA_USER must be in successful types (users scope succeeded)
    assert mod.ResourceType.ENTRA_USER in successful_types, (
        f"ENTRA_USER missing from successful_types: {successful_types}"
    )
    # Both group types should also be present
    assert mod.ResourceType.ENTRA_GROUP in successful_types
    assert mod.ResourceType.M365_GROUP in successful_types


@pytest.mark.asyncio
async def test_all_scopes_failed_returns_no_successful_types():
    """When every scope method raises, _discover_and_stage_scopes should return
    an empty successful_types set and an attempted_scope_count > 0.
    """
    mod = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(mod)

    class FakeGraphAllFailing:
        async def discover_users(self, **kwargs):
            raise RuntimeError("401 Unauthorized")

        async def discover_groups(self, **kwargs):
            raise RuntimeError("401 Unauthorized")

    mod.settings.DISCOVERY_SCOPE_MAX_CONCURRENCY = 1

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def fake_session_factory():
        yield FakeDB()

    mod.async_session_factory = fake_session_factory

    run_id = uuid.uuid4()
    tenant_id = uuid.uuid4()

    fetched, successful_types, attempted = await mod._discover_and_stage_scopes(
        FakeGraphAllFailing(), ["users", "groups"], run_id, tenant_id
    )

    assert fetched == 0, f"Expected 0 fetched, got {fetched}"
    assert not successful_types, f"Expected empty successful_types, got {successful_types}"
    assert attempted == 2, f"Expected 2 attempted scopes, got {attempted}"


@pytest.mark.asyncio
async def test_purge_orphan_staging_keeps_current_run():
    # The purge now resolves run liveness first (see
    # tests/workers/test_purge_orphan_staging_liveness.py) and only deletes
    # rows of dead runs, so with no candidate rows there is nothing to delete.
    mod = importlib.util.module_from_spec(_spec); _spec.loader.exec_module(mod)
    db = FakeDB(); tid = uuid.uuid4(); keep = uuid.uuid4()
    await mod._purge_orphan_staging(db, tid, keep_run_id=keep)
    lookups = [c for c in db.calls if c[0].strip().upper().startswith("SELECT")]
    assert lookups, "expected a liveness lookup on resource_discovery_staging"
    assert lookups[0][1]["tenant_id"] == tid and lookups[0][1]["keep_run_id"] == keep
    dels = [c for c in db.calls if "DELETE FROM resource_discovery_staging" in c[0]]
    assert not dels, "no dead runs ⇒ nothing may be deleted"


@pytest.mark.asyncio
async def test_no_scope_streams_all_canonical_scopes():
    """When discovery_scope is None/empty, _discover_and_stage_scopes must fan
    out through the per-scope streaming path for the 6 full-discovery scopes
    (users, groups, mailboxes, sharepoint, teams, power_platform) and must NOT
    call discover_all().
    """
    mod = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(mod)

    discover_all_called = []

    class FakeGraph:
        async def discover_all(self):
            discover_all_called.append(True)
            return []

        async def discover_users(self, **kwargs):
            return [{"id": "u1", "type": "ENTRA_USER"}]

        async def discover_users_paged(self, **kwargs):
            # paged variant — yield one page so the paged path works too
            yield [{"id": "u1", "type": "ENTRA_USER"}]

        async def discover_groups(self, **kwargs):
            return [{"id": "g1", "type": "ENTRA_GROUP"}, {"id": "g2", "type": "M365_GROUP"}]

        async def discover_mailboxes(self, **kwargs):
            return [{"id": "sm1", "type": "SHARED_MAILBOX"}]

        async def discover_mailboxes_paged(self, **kwargs):
            yield [{"id": "sm1", "type": "SHARED_MAILBOX"}]

        async def discover_sharepoint(self, **kwargs):
            return [{"id": "sp1", "type": "SHAREPOINT_SITE"}]

        async def discover_teams(self, **kwargs):
            return [{"id": "tc1", "type": "TEAMS_CHANNEL"}]

        async def discover_power_platform(self, **kwargs):
            return [{"id": "pb1", "type": "POWER_BI"}]

    staged_batches = []

    async def fake_stage_rows(db, run_id, tenant_id, rows):
        if rows:
            staged_batches.append(len(rows))
        db.calls.append(("INSERT INTO resource_discovery_staging", [{"run_id": run_id}] if rows else []))
        return len(rows)

    def fake_prepare(tenant_id, resources):
        return list(resources)

    mod._stage_rows = fake_stage_rows
    mod._prepare_staging_rows = fake_prepare
    mod.settings.DISCOVERY_SCOPE_MAX_CONCURRENCY = 1

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def fake_session_factory():
        yield FakeDB()

    mod.async_session_factory = fake_session_factory

    run_id = uuid.uuid4()
    tenant_id = uuid.uuid4()

    # Call with None (no scope) — must fan out to all 6 canonical full-discovery scopes
    fetched, successful_types, attempted = await mod._discover_and_stage_scopes(
        FakeGraph(), None, run_id, tenant_id
    )

    # discover_all() must NOT have been called
    assert not discover_all_called, "discover_all() was called but should not be"

    # All 6 scopes should have been attempted
    assert attempted == 6, f"Expected 6 attempted scopes, got {attempted}"

    # Resources were staged in multiple batches (one per scope)
    assert len(staged_batches) == 6, f"Expected 6 staged batches, got {staged_batches}"

    # Total fetched == sum of all scope returns
    assert fetched == sum(staged_batches), (
        f"fetched_count {fetched} != sum of staged batches {sum(staged_batches)}"
    )

    # At minimum ENTRA_USER must be in successful_types (users scope succeeded)
    assert mod.ResourceType.ENTRA_USER in successful_types, (
        f"ENTRA_USER missing from successful_types: {successful_types}"
    )
