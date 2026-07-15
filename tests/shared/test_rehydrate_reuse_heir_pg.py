"""Seam 3 — reuse-chain rehydration against REAL Postgres (DB-state, not SQL text).

The characterization test (test_rehydrate_reuse_heir.py) locks the statement order
with a fake session; THIS test drives the ACTUAL _rehydrate_reuse_heir against a
live Postgres to prove the real effects: the immediate reuse-validation trigger is
satisfied for a >=2-descendant chain (promote-before-repoint), items land on the
heir, the heir is promoted to a full, the siblings are repointed, and the doomed
row's delta cursor is carried forward (P4b). Everything runs in ONE transaction
that is ALWAYS rolled back — nothing persists. Reuses an existing resource+backend.

Skipped unless RETENTION_PG_LIVE=1 AND a Postgres is reachable (CI / Railway).
"""
from __future__ import annotations

import os
import uuid

import pytest

pytestmark = pytest.mark.asyncio


def _enabled() -> bool:
    return os.getenv("RETENTION_PG_LIVE") == "1"


@pytest.mark.skipif(not _enabled(), reason="needs live Postgres (set RETENTION_PG_LIVE=1)")
async def test_rehydrate_reuse_heir_live_multi_descendant():
    from sqlalchemy import text
    from shared.database import async_session_factory, init_db
    from shared.retention_cleanup import _rehydrate_reuse_heir

    await init_db()
    async with async_session_factory() as s:
        row = (await s.execute(text(
            "SELECT r.id, r.tenant_id, sn.backend_id FROM resources r "
            "JOIN snapshots sn ON sn.resource_id = r.id "
            "WHERE sn.backend_id IS NOT NULL LIMIT 1"))).first()
        if not row:
            pytest.skip("no resource+backend available to build a fixture")
        rid, tid, backend_id = row.id, row.tenant_id, row.backend_id
        root, d1, d2 = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()

        def _snap(sid, off_h, reuse_of, chain_root, delta):
            return text("""
                INSERT INTO snapshots
                    (id, resource_id, backend_id, type, status, started_at, created_at,
                     item_count, bytes_total, delta_token, reuse_of_snapshot_id,
                     reuse_chain_root_id, hc_drain_status, lease_token, dr_replication_status)
                VALUES (CAST(:id AS UUID), CAST(:rid AS UUID), CAST(:bid AS UUID),
                        'INCREMENTAL', 'COMPLETED', NOW() - (:off || ' hours')::interval, NOW(),
                        0, 0, :delta, CAST(:ro AS UUID), CAST(:cr AS UUID),
                        'NOT_APPLICABLE', 0, 'pending')
            """).bindparams(id=str(sid), rid=str(rid), bid=str(backend_id), off=str(off_h),
                            delta=delta, ro=(str(reuse_of) if reuse_of else None),
                            cr=(str(chain_root) if chain_root else None))

        await s.execute(_snap(root, 48, None, None, "ROOT_CURSOR"))
        await s.execute(_snap(d1, 24, root, root, None))   # earliest survivor -> heir
        await s.execute(_snap(d2, 12, root, root, None))
        for i in range(2):
            await s.execute(text("""
                INSERT INTO snapshot_items
                    (id, snapshot_id, tenant_id, external_id, item_type, name,
                     content_size, is_deleted, backend_id, created_at)
                VALUES (CAST(:id AS UUID), CAST(:sid AS UUID), CAST(:tid AS UUID),
                        :ext, 'EMAIL', :nm, 10, false, CAST(:bid AS UUID), NOW())
            """).bindparams(id=str(uuid.uuid4()), sid=str(root), tid=str(tid),
                            ext=f"itm-{i}", nm=f"item {i}", bid=str(backend_id)))

        # drive the REAL production function
        heir = await _rehydrate_reuse_heir(s, root, doomed_ids={root})
        assert heir == d1

        n_items = (await s.execute(text(
            "SELECT count(*) FROM snapshot_items WHERE snapshot_id = CAST(:h AS UUID)"),
            {"h": str(d1)})).scalar()
        h = (await s.execute(text(
            "SELECT reuse_of_snapshot_id, reuse_chain_root_id, delta_token FROM snapshots "
            "WHERE id = CAST(:h AS UUID)"), {"h": str(d1)})).first()
        d2row = (await s.execute(text(
            "SELECT reuse_of_snapshot_id, reuse_chain_root_id FROM snapshots "
            "WHERE id = CAST(:d AS UUID)"), {"d": str(d2)})).first()

        assert n_items == 2                                   # items copied to heir
        assert h.reuse_of_snapshot_id is None                 # heir promoted to full
        assert h.reuse_chain_root_id is None
        assert h.delta_token == "ROOT_CURSOR"                 # P4b cursor carried forward
        assert str(d2row.reuse_of_snapshot_id) == str(d1)     # sibling repointed
        assert str(d2row.reuse_chain_root_id) == str(d1)

        await s.rollback()   # nothing persists
