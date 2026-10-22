"""Archive carry-forward preserves enriched attachments on real PostgreSQL."""
from contextlib import asynccontextmanager
import uuid

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.engine import URL

from tests.shared.test_batch_rollup_sql_semantics import pg_socket_dir  # noqa: F401
from tests.workers.test_archive_backup import ab


@pytest.mark.asyncio
async def test_carry_keeps_attachment_metadata_and_encryption_without_inflating_inventory(
    pg_socket_dir, monkeypatch,
):
    import shared.database as dbmod

    dbname = "archive_" + uuid.uuid4().hex[:12]
    admin = await asyncpg.connect(
        host=pg_socket_dir, port=55433, user="postgres", database="postgres",
    )
    await admin.execute(f'CREATE DATABASE "{dbname}"')
    await admin.close()
    engine = create_async_engine(URL.create(
        "postgresql+asyncpg", username="postgres", database=dbname,
        query={"host": pg_socket_dir, "port": "55433"},
    ))
    factory = async_sessionmaker(engine, expire_on_commit=False)
    prior, target, backend = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    try:
        async with engine.begin() as conn:
            await conn.execute(text("""
                CREATE TABLE snapshot_items (
                    id uuid PRIMARY KEY, snapshot_id uuid, tenant_id uuid,
                    item_type text, external_id text, parent_external_id text,
                    name text, folder_path text, content_hash text,
                    content_checksum text, content_size bigint, blob_path text,
                    encryption_key_id text, backup_version integer, metadata json,
                    is_deleted boolean, indexed_at timestamp, backend_id uuid,
                    created_at timestamp,
                    UNIQUE (snapshot_id, external_id, item_type)
                )
            """))
            for kind, external_id, parent_id in (
                ("ARCHIVE_ITEM", "unchanged", "folder"),
                ("ARCHIVE_ATTACHMENT", "unchanged::att1", "unchanged"),
                ("ARCHIVE_ATTACHMENT", "unchanged::att2", "unchanged"),
                ("ARCHIVE_ITEM", "changed", "folder"),
                ("ARCHIVE_ATTACHMENT", "changed::att1", "changed"),
            ):
                await conn.execute(text("""
                    INSERT INTO snapshot_items (
                        id, snapshot_id, item_type, external_id, parent_external_id,
                        metadata, blob_path, encryption_key_id, backend_id,
                        content_checksum, content_size
                    ) VALUES (
                        :id, :sid, :kind, :ext, :parent,
                        '{"enrich_status":"enriched","change_key":"old-key"}',
                        :blob, 'dek-1', :backend, 'content-sha', 13
                    )
                """), {"id": uuid.uuid4(), "sid": prior, "kind": kind,
                       "ext": external_id, "parent": parent_id,
                       "blob": f"original/{external_id}", "backend": backend})

        @asynccontextmanager
        async def slot(*args, **kwargs):
            async with factory() as session:
                yield session

        monkeypatch.setattr(dbmod, "carry_forward_slot_session", slot)
        count = await ab._default_carry_forward(str(target), str(prior), ["unchanged"])
        assert count == 1
        async with factory() as session:
            rows = (await session.execute(text(
                "SELECT * FROM snapshot_items WHERE snapshot_id=:sid ORDER BY external_id"
            ), {"sid": target})).mappings().all()
        assert [row["external_id"] for row in rows] == [
            "unchanged", "unchanged::att1", "unchanged::att2",
        ]
        for row in rows:
            assert row["metadata"]["enrich_status"] == "enriched"
            assert row["metadata"]["change_key"] == "old-key"
            assert row["blob_path"] == f"original/{row['external_id']}"
            assert row["encryption_key_id"] == "dek-1"
            assert row["backend_id"] == backend
            assert row["content_checksum"] == "content-sha"
        assert await ab._default_carry_forward(str(target), str(prior), ["unchanged"]) == 0
    finally:
        await engine.dispose()
