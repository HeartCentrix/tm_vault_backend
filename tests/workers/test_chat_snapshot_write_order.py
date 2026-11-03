"""Chat writers use one transaction-wide byte order and complete descriptors."""
import importlib.util
import os
from pathlib import Path
import sys
from uuid import UUID, uuid4

import pytest
from sqlalchemy.dialects import postgresql

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-chat-order-secret")
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location("chat_write_order_worker", ROOT / "workers/backup-worker/main.py")
worker = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = worker
spec.loader.exec_module(worker)
from shared.models import SnapshotItem


class Session:
    def __init__(self):
        self.statements = []
        self.commits = 0

    async def execute(self, statement):
        self.statements.append(statement)

    async def commit(self):
        self.commits += 1


def row(key, name="original", *, snapshot_id=None, kind="TEAMS_CHAT_MESSAGE"):
    return {"snapshot_id": snapshot_id or UUID(int=1), "external_id": key,
            "item_type": kind, "name": name, "extra_data": {"name": name}}


async def test_whole_transaction_is_ordered_before_chunking_and_equal_keys_stay_stable(monkeypatch):
    monkeypatch.setattr(worker, "_BULK_INSERT_CHUNK", 1)
    source = [row("z"), row("é"), row("a"), row("A", "first"), row("A", "second")]
    session = Session()
    assert await worker._bulk_upsert_snapshot_items(session, source) == 5
    values = [statement.compile(dialect=postgresql.dialect()).params for statement in session.statements]
    assert [(p["external_id_m0"], p["name_m0"]) for p in values] == [
        ("A", "first"), ("A", "second"), ("a", "original"), ("z", "original"), ("é", "original")]
    assert session.commits == 1
    assert source[0]["external_id"] == "z", "caller-owned payload order was mutated"


async def test_sort_includes_snapshot_uuid_then_external_id_then_type(monkeypatch):
    monkeypatch.setattr(worker, "_BULK_INSERT_CHUNK", 1)
    session = Session()
    await worker._bulk_upsert_snapshot_items(session, [
        row("a", snapshot_id=UUID(int=2)), row("a", kind="TEAMS_MESSAGE"),
        row("a", kind="CHAT_ATTACHMENT"), row("Z")])
    values = [s.compile(dialect=postgresql.dialect()).params for s in session.statements]
    assert [(p["snapshot_id_m0"], p["external_id_m0"], p["item_type_m0"]) for p in values] == [
        (UUID(int=1), "Z", "TEAMS_CHAT_MESSAGE"), (UUID(int=1), "a", "CHAT_ATTACHMENT"),
        (UUID(int=1), "a", "TEAMS_MESSAGE"), (UUID(int=2), "a", "TEAMS_CHAT_MESSAGE")]


def test_attachment_descriptor_keeps_all_assigned_storage_version_and_parent_fields():
    from shared.snapshot_item_writes import snapshot_item_rows
    values = dict(id=uuid4(), snapshot_id=uuid4(), tenant_id=uuid4(), external_id="m::attachment",
        parent_external_id="m", item_type="CHAT_ATTACHMENT", name="report.pdf", folder_path="chats/test",
        content_hash="hash", content_checksum="checksum", content_size=15, blob_path="original/blob",
        backend_id=uuid4(), encryption_key_id="original-dek", backup_version=7, chat_version_id=uuid4(),
        extra_data={"inline_b64": "cGRm"}, is_deleted=True)
    item = SnapshotItem(**values)
    assert snapshot_item_rows([item]) == [values]


def test_unassigned_defaults_stay_omitted_and_generated_item_id_is_stable():
    from shared.snapshot_item_writes import snapshot_item_rows
    item = SnapshotItem(**row("m"))
    first, second = snapshot_item_rows([item])[0], snapshot_item_rows([item])[0]
    assert first == second and first["id"] == item.id
    assert "backup_version" not in first and "created_at" not in first
    assert "is_deleted" not in first and "metadata" not in first
