"""Chat exports must install their read/write crypto before consuming jobs."""
import importlib
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock

import pytest


@pytest.mark.parametrize("enabled", [False, True])
async def test_chat_export_boot_installs_configured_crypto_before_consuming(monkeypatch, enabled):
    package_name = "workers.chat_export_worker"
    if package_name not in sys.modules:
        package = ModuleType(package_name)
        package.__path__ = [str(Path(__file__).resolve().parents[2] / "workers/chat-export-worker")]
        sys.modules[package_name] = package
    worker = importlib.import_module(package_name + ".main")
    from shared.crypto import config_store, runtime
    from shared.storage import startup

    monkeypatch.setenv("BACKUP_ENCRYPTION_KEK_PROVIDER", "env")
    monkeypatch.setenv("BACKUP_ENCRYPTION_MASTER_KEY_HEX", "ab" * 32)
    monkeypatch.setenv("BACKUP_ENCRYPTION_ENABLED", str(enabled).lower())
    monkeypatch.setattr(config_store, "get_config", AsyncMock(return_value=None))
    monkeypatch.setattr(runtime, "_dek_manager", None)
    monkeypatch.setattr(runtime, "_write_enabled", False)
    monkeypatch.setattr(runtime, "_verify_readback", True)
    monkeypatch.setattr(startup, "startup_router", AsyncMock())
    monkeypatch.setattr(worker, "start_http_server", lambda *args: None)
    monkeypatch.setattr(worker, "health_server", AsyncMock())
    monkeypatch.setattr(worker, "_reclaim_orphan_jobs", AsyncMock())
    monkeypatch.setattr(worker, "settings", SimpleNamespace(
        RABBITMQ_URL="amqp://test.invalid", RABBITMQ_CONSUMER_HEARTBEAT_SECONDS=60))

    class BootChecked(Exception):
        pass

    async def connect(*args, **kwargs):
        assert runtime._dek_manager is not None
        assert runtime.is_write_enabled() is enabled
        raise BootChecked

    monkeypatch.setattr(worker.aio_pika, "connect_robust", connect)
    with pytest.raises(BootChecked):
        await worker.main()
