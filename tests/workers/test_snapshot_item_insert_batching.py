"""Interleaved row shapes must not split snapshot_items inserts into tiny statements.

Railway 2026-11-05: mail rows alternate email / its attachments (different column sets), and the
bulk insert started a new INSERT at every shape change, so mail wrote ~1.8 rows per statement.
A statement-level trigger appends one protected-size journal event per INSERT statement, so a
10-user run produced 18,635 mail events and the dashboard's backup size lagged the run by over
an hour. Rows are grouped by column set (first-appearance order); chat rows keep their
conflict-key order."""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
from uuid import uuid4

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
_spec = importlib.util.spec_from_file_location("bw_insert_batching", _ROOT / "workers" / "backup-worker" / "main.py")
_bw = importlib.util.module_from_spec(_spec)
sys.modules["bw_insert_batching"] = _bw
try:
    _spec.loader.exec_module(_bw)
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(f"backup-worker module failed to import: {exc}", allow_module_level=True)


class Session:
    def __init__(self):
        self.statements = []

    async def execute(self, stmt):
        self.statements.append([{getattr(k, "key", k): v for k, v in row.items()}["external_id"] for row in stmt._multi_values[0]])


def test_interleaved_email_and_attachment_rows_insert_in_one_statement_per_shape():
    snap, tenant = uuid4(), uuid4()
    rows = []
    for n in range(5):
        rows.append({"snapshot_id": snap, "tenant_id": tenant, "external_id": f"m{n}", "item_type": "EMAIL", "name": f"m{n}"})
        rows.append({"snapshot_id": snap, "tenant_id": tenant, "external_id": f"a{n}", "item_type": "EMAIL_ATTACHMENT",
                     "name": f"a{n}", "parent_external_id": f"m{n}", "content_size": 10})
    session = Session()
    assert asyncio.run(_bw._insert_snapshot_items(session, rows)) == 10
    assert session.statements == [["m0", "m1", "m2", "m3", "m4"], ["a0", "a1", "a2", "a3", "a4"]]
