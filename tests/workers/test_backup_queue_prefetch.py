"""Dedicated pools must honor their lane's operator-selected parent budget."""
import ast
import asyncio
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


@pytest.mark.parametrize("queue,heavy,normal,dedicated,expected", [
    ("backup.heavy", None, "1", None, 20),
    ("backup.heavy", "1", "7", None, 1),
    ("backup.heavy", "0", "7", None, 1),
    ("backup.heavy", "-3", "7", None, 1),
    ("backup.normal", "9", "2", "true", 2),
    ("backup.normal", "9", None, "true", 20),
    ("backup.normal", "9", "0", "true", 1),
])
async def test_dedicated_queue_prefetch(monkeypatch, queue, heavy, normal, dedicated, expected):
    for name, value in {
        "BACKUP_HEAVY_PREFETCH": heavy,
        "BACKUP_NORMAL_PREFETCH": normal,
        "BACKUP_WORKER_DEDICATED_ONLY": dedicated,
        "BACKUP_HEAVY_QUEUE": None,
        "ONEDRIVE_RETRY_QUEUE_ENABLED": "false",
    }.items():
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)

    # Execute the actual intake method without importing unrelated storage SDKs.
    source = Path(__file__).resolve().parents[2] / "workers/backup-worker/main.py"
    tree = ast.parse(source.read_text())
    method = next(node for node in ast.walk(tree)
                  if isinstance(node, ast.AsyncFunctionDef) and node.name == "_consume_backup_queues")
    bus = SimpleNamespace(connect=AsyncMock())
    scope = {"asyncio": asyncio, "os": os, "message_bus": bus,
             "settings": SimpleNamespace(ONEDRIVE_PARTITION_ENABLED=False)}
    exec(compile(ast.Module(body=[method], type_ignores=[]), str(source), "exec"), scope)
    subscriptions = []

    async def subscribe(queue_name, prefetch):
        subscriptions.append((queue_name, prefetch))

    worker = SimpleNamespace(_backup_queue_name=queue, worker_id="test", _supervised_consume=subscribe)
    await scope["_consume_backup_queues"](worker)
    assert subscriptions == [(queue, expected)]


async def test_light_pool_consumes_each_sla_lane_independently(monkeypatch):
    """Gold (backup.high) and Silver/Bronze (backup.normal) each get their own
    consumer and prefetch budget, so a full lane can never block the other."""
    for name in ("BACKUP_HIGH_PREFETCH", "BACKUP_NORMAL_PREFETCH", "BACKUP_URGENT_PREFETCH",
                 "BACKUP_WORKER_DEDICATED_ONLY", "BACKUP_HEAVY_QUEUE"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("ONEDRIVE_RETRY_QUEUE_ENABLED", "false")
    source = Path(__file__).resolve().parents[2] / "workers/backup-worker/main.py"
    tree = ast.parse(source.read_text())
    method = next(node for node in ast.walk(tree)
                  if isinstance(node, ast.AsyncFunctionDef) and node.name == "_consume_backup_queues")
    scope = {"asyncio": asyncio, "os": os, "message_bus": SimpleNamespace(connect=AsyncMock()),
             "settings": SimpleNamespace(ONEDRIVE_PARTITION_ENABLED=False)}
    exec(compile(ast.Module(body=[method], type_ignores=[]), str(source), "exec"), scope)
    subscriptions = {}

    async def subscribe(queue_name, prefetch):
        subscriptions[queue_name] = prefetch

    worker = SimpleNamespace(_backup_queue_name="backup.normal", worker_id="t", _supervised_consume=subscribe)
    await scope["_consume_backup_queues"](worker)
    assert subscriptions["backup.high"] > 0 and subscriptions["backup.normal"] > 0
    assert subscriptions["backup.urgent"] > 0
