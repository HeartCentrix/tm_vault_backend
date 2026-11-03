"""Unavailable RabbitMQ statistics must never imply stopped consumers."""
import os
from unittest.mock import AsyncMock

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")

from shared import health


@pytest.mark.parametrize("metric", [{}, {"consumers": None, "messages": None}])
async def test_unavailable_metrics_are_unknown_not_zero(monkeypatch, metric):
    monkeypatch.setattr(health.settings, "RABBITMQ_ENABLED", True)
    monkeypatch.setattr(health, "_fetch_queues", AsyncMock(return_value=[
        {"name": "backup.normal", **metric},
        {"name": "backup.normal.dlq", **metric},
    ]))
    result = await health.check_rabbitmq_and_queues()
    assert result["rabbitmq"]["status"] == health.HEALTHY
    queues = result["queues"]
    assert queues["status"] == health.DEGRADED
    assert queues["no_consumer"] == []
    assert queues["dlq_nonempty"] == []
    assert "unavailable" in queues["detail"]
    assert "backup.normal" in queues["detail"]
    assert "backup.normal.dlq" in queues["detail"]


@pytest.mark.parametrize("consumers,messages,status", [
    (0, 0, health.UNHEALTHY),
    (2, 3, health.DEGRADED),
    (2, 0, health.HEALTHY),
])
async def test_observed_counts_keep_existing_alarms(monkeypatch, consumers, messages, status):
    monkeypatch.setattr(health.settings, "RABBITMQ_ENABLED", True)
    monkeypatch.setattr(health, "_fetch_queues", AsyncMock(return_value=[
        {"name": "backup.normal", "consumers": consumers},
        {"name": "backup.normal.dlq", "messages": messages},
    ]))
    queues = (await health.check_rabbitmq_and_queues())["queues"]
    assert queues["status"] == status
    assert queues["no_consumer"] == (["backup.normal"] if consumers == 0 else [])
    assert queues["dlq_nonempty"] == (["backup.normal.dlq"] if messages else [])


async def test_unavailable_statistics_do_not_hide_observed_outage(monkeypatch):
    monkeypatch.setattr(health.settings, "RABBITMQ_ENABLED", True)
    monkeypatch.setattr(health, "_fetch_queues", AsyncMock(return_value=[
        {"name": "backup.normal", "consumers": 0},
        {"name": "backup.heavy"},
        {"name": "backup.normal.dlq", "messages": 3},
    ]))
    queues = (await health.check_rabbitmq_and_queues())["queues"]
    assert queues["status"] == health.UNHEALTHY
    assert queues["no_consumer"] == ["backup.normal"]
    assert queues["dlq_nonempty"] == ["backup.normal.dlq"]
    assert "unavailable" in queues["detail"]
