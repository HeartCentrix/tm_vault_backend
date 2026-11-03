"""Toggle worker startup must honor the shared runtime schema policy."""
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from services.storage_toggle_worker import main as worker
from shared import database, models, storage_bootstrap


@pytest.fixture
def startup(monkeypatch):
    statements = []
    schema = SimpleNamespace(
        columns={(table.name, column.name)
                 for table in database.Base.metadata.tables.values()
                 for column in table.columns},
        catalogue_ready=True,
    )

    async def execute(statement, params=None):
        sql = " ".join(str(statement).split())
        statements.append(sql)
        return SimpleNamespace(
            all=lambda: list(schema.columns),
            scalar=lambda: schema.catalogue_ready if "pg_constraint" in sql else True,
            first=lambda: SimpleNamespace(active_backend_id="backend-1", enabled_count=2),
        )

    conn = SimpleNamespace(execute=execute, run_sync=AsyncMock())

    @asynccontextmanager
    async def transaction():
        yield conn

    monkeypatch.setattr(database, "engine", SimpleNamespace(
        connect=transaction, begin=transaction,
    ))
    # These reach other external services; keep schema initialization itself real.
    monkeypatch.setattr(database, "_seed_preset_policies_for_existing_tenants", AsyncMock())
    monkeypatch.setattr(storage_bootstrap, "_ensure_seaweed_buckets", AsyncMock())

    lock = SimpleNamespace(close=AsyncMock())
    monkeypatch.setattr(worker, "_acquire_advisory_lock_forever", AsyncMock(return_value=lock))
    monkeypatch.setattr(worker, "_dsn", lambda: "postgresql://unused")
    monkeypatch.setattr(worker, "_rmq_url", lambda: "amqp://unused")
    router = SimpleNamespace(load=AsyncMock(), close=AsyncMock())
    monkeypatch.setattr(worker, "router", router)

    async def messages():
        if False:
            yield

    @asynccontextmanager
    async def iterator():
        yield messages()

    queue = SimpleNamespace(iterator=iterator)
    channel = SimpleNamespace(declare_queue=AsyncMock(return_value=queue))
    broker = SimpleNamespace(channel=AsyncMock(return_value=channel), close=AsyncMock())
    connect_broker = AsyncMock(return_value=broker)
    monkeypatch.setattr(worker.aio_pika, "connect_robust", connect_broker)
    return SimpleNamespace(statements=statements, schema=schema, lock=lock,
                           router=router, broker=broker, connect_broker=connect_broker)


@pytest.mark.parametrize("mode", [None, "verify", "VERIFY"])
async def test_runtime_startup_verifies_schema_without_trigger_ddl(startup, monkeypatch, mode):
    if mode is None:
        monkeypatch.delenv("DB_SCHEMA_MODE", raising=False)
    else:
        monkeypatch.setenv("DB_SCHEMA_MODE", mode)

    await worker.consume()

    assert any("information_schema.columns" in sql for sql in startup.statements)
    assert all(sql.startswith(("SELECT", "SET LOCAL")) for sql in startup.statements), (
        "runtime startup must not rewrite triggers or storage configuration"
    )
    startup.broker.channel.assert_awaited_once()
    startup.lock.close.assert_awaited_once()


@pytest.mark.parametrize("missing", ["storage_column", "catalogue_constraint"])
async def test_missing_schema_stops_consumption_and_releases_leader_lock(startup, monkeypatch, missing):
    monkeypatch.setenv("DB_SCHEMA_MODE", "verify")
    if missing == "storage_column":
        startup.schema.columns.remove(("storage_backends", "id"))
    else:
        startup.schema.catalogue_ready = False

    with pytest.raises(RuntimeError, match="schema is incomplete.*offline bootstrap"):
        await worker.consume()

    startup.connect_broker.assert_not_awaited()
    startup.router.load.assert_not_awaited()
    startup.lock.close.assert_awaited_once()


async def test_explicit_bootstrap_still_installs_storage_triggers_and_seeds(startup, monkeypatch):
    monkeypatch.setenv("DB_SCHEMA_MODE", "bootstrap")

    await worker.consume()

    assert any(sql.startswith("CREATE TRIGGER trg_snapshots_default_backend")
               for sql in startup.statements)
    assert any(sql.startswith("CREATE TRIGGER trg_snapshot_items_default_backend")
               for sql in startup.statements)
    assert any(sql.startswith("INSERT INTO storage_backends") for sql in startup.statements)
    assert any(sql.startswith("INSERT INTO system_config") for sql in startup.statements)
    startup.broker.channel.assert_awaited_once()


async def test_invalid_schema_mode_stops_consumption(startup, monkeypatch):
    monkeypatch.setenv("DB_SCHEMA_MODE", "typo")

    with pytest.raises(ValueError, match="DB_SCHEMA_MODE"):
        await worker.consume()

    assert startup.statements == []
    startup.connect_broker.assert_not_awaited()
    startup.lock.close.assert_awaited_once()
