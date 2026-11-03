"""publish() before connect() used to return silently; callers then printed "enqueued"
for work that never left the process. It must say so."""
import os

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

import asyncio

from shared import message_bus as bus_module


def test_publish_without_connection_warns(monkeypatch, capsys):
    monkeypatch.setattr(bus_module.settings, "RABBITMQ_ENABLED", True)
    bus = bus_module.MessageBus()
    assert bus.exchange is None
    asyncio.run(bus.publish("discovery.tier2", {"tenantId": "t"}))
    out = capsys.readouterr().out
    assert "discovery.tier2" in out and "not connected" in out
