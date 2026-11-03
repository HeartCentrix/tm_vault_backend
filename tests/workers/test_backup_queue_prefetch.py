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
