"""Planner / To Do incrementals cost O(changes) in blob uploads and snapshot rows.

Each run still walks Graph (Planner has no v1.0 delta). An item whose content hash equals the
prior full snapshot's row reuses that row's blob (blob GC is reference-counted, shared/blob_gc.py);
a run whose whole inventory equals the prior's points at it (shared.snapshot_reuse.point_at) and
writes no rows. A changed run still writes its full inventory: these types are browsed per
snapshot through items_scope, so changed-only rows would hide the unchanged ones.
"""
from __future__ import annotations

import logging

from sqlalchemy import text

# Newest COMPLETED full snapshot of the resource, older than the target, that owns rows.
_PRIOR_SQL = text("""
    SELECT p.id, p.item_count, p.bytes_total FROM snapshots p
     WHERE p.resource_id = :rid AND p.id <> :sid AND p.status = 'COMPLETED'
       AND p.reuse_of_snapshot_id IS NULL AND p.reuse_chain_root_id IS NULL AND p.overlay_base_id IS NULL
       AND p.created_at < (SELECT t.created_at FROM snapshots t WHERE t.id = :sid)
       AND EXISTS (SELECT 1 FROM snapshot_items x WHERE x.snapshot_id = p.id)
     ORDER BY p.created_at DESC LIMIT 1""")


class PriorItems:
    def __init__(self, prior=None, rows=()):
        self.prior = prior
        self.rows = {(r.item_type, r.external_id): (r.content_hash, r.blob_path) for r in rows}

    @classmethod
    async def load(cls, session_factory, resource_id, snapshot_id):
        """The prior full inventory, or an empty one (every item uploads, as before) on any error."""
        try:
            async with session_factory() as session:
                prior = (await session.execute(_PRIOR_SQL, {"rid": str(resource_id), "sid": str(snapshot_id)})).first()
                if prior is None:
                    return cls()
                rows = (await session.execute(text(
                    "SELECT item_type, external_id, content_hash, blob_path FROM snapshot_items "
                    "WHERE snapshot_id = :p AND NOT COALESCE(is_deleted, false)"), {"p": str(prior.id)})).all()
                return cls(prior, rows)
        except Exception as exc:
            logging.getLogger(__name__).warning("item_reuse_prior_failed snapshot=%s: %s", snapshot_id, exc)
            return cls()

    def blob(self, item_type, external_id, content_hash):
        """The prior row's blob path when its content is identical, else None (upload)."""
        prior = self.rows.get((item_type, external_id))
        return prior[1] if prior and prior[1] and prior[0] == content_hash else None

    async def point_if_unchanged(self, session_factory, snapshot_id, items, marker):
        """Point ``snapshot_id`` at the prior when ``items`` equal its inventory; the handler result, or None."""
        if self.prior is None or {(i.item_type, i.external_id, i.content_hash) for i in items} != {
                (t, e, h) for (t, e), (h, _) in self.rows.items()} or len(items) != len(self.rows):
            return None
        from shared.snapshot_reuse import point_at
        async with session_factory() as session:
            if not await point_at(session, target_snapshot_id=snapshot_id, prior_snapshot_id=self.prior.id, marker=marker):
                return None
            await session.commit()
        return {"item_count": self.prior.item_count or 0, "new_item_count": 0,
                "bytes_added": 0, "bytes_total": self.prior.bytes_total or 0}
