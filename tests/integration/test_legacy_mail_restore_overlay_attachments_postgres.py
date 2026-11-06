"""Legacy mail restore (MAIL_RESTORE_V2 off) reads a message's attachments from its snapshot's inventory."""
import pytest

from tests.integration.test_mail_overlay_postgres import db, _load  # noqa: F401
from shared import models

pytestmark = pytest.mark.integration
OK = models.SnapshotStatus.COMPLETED


async def test_legacy_mail_restore_finds_base_attachments_of_an_overlay_owned_message(db):
    """MAIL_RESTORE_V2 off: a message the overlay re-owns (moved/changed) keeps its attachment row in the base."""
    import base64
    from types import SimpleNamespace
    from sqlalchemy import select
    rid = await db.resource()
    base, _ = await db.snapshot(rid, OK, item_count=1)
    await db.item(base, "m", "EMAIL")
    await db.item(base, "m::1", "EMAIL_ATTACHMENT", parent="m", name="a.txt",
                  metadata={"parent_item_id": "m", "inline_b64": base64.b64encode(b"hi").decode()})
    overlay, _ = await db.snapshot(rid, OK, item_count=1, overlay_base_id=base)
    await db.item(overlay, "m", "EMAIL", folder="Archive", metadata={"raw": {"subject": "s"}})
    posted = []

    class Graph:
        async def json_create_non_draft_message(self, *a):
            return "new"

        async def patch_sender_extended_properties(self, *a, **k):
            pass

        async def patch_original_timestamps(self, *a, **k):
            pass

        async def post_small_attachment(self, user, mid, body):
            posted.append(body["name"])

    restore = _load("retention_fold_restore", "workers/restore-worker/main.py")
    worker = restore.RestoreWorker.__new__(restore.RestoreWorker)
    worker.worker_id = "t"
    async with db.factory() as session:
        item = (await session.execute(select(models.SnapshotItem).where(
            models.SnapshotItem.snapshot_id == overlay))).scalar_one()
        await worker._restore_email_to_mailbox(Graph(), SimpleNamespace(external_id="u"), item, session)
    assert posted == ["a.txt"]
