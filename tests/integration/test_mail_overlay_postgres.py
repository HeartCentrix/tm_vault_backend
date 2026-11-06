"""Overlay mail snapshots: a changed mailbox's incremental owns only what changed since a full base
snapshot, and every reader sees base + overlay through snapshot_items_effective().

Rohit, 2026-11-05: 7,085 rows copied forward for one new message (245 s of a 464 s run)."""
from __future__ import annotations

from datetime import datetime, timedelta
import os
from pathlib import Path
import re
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

from shared.database import Base
import shared.models as models

pytestmark = pytest.mark.integration


def _reuse_trigger_ddl():
    source = (Path(__file__).parents[2] / "shared" / "database.py").read_text()
    function = re.search(r'"""(CREATE OR REPLACE FUNCTION validate_snapshot_reuse_target\(\).*?)"""', source, re.S).group(1)
    return [function, "CREATE TRIGGER snapshots_reuse_validate BEFORE INSERT OR UPDATE OF reuse_of_snapshot_id, "
            "reuse_chain_root_id ON snapshots FOR EACH ROW EXECUTE FUNCTION validate_snapshot_reuse_target();"]


@pytest_asyncio.fixture
async def db():
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires explicitly configured disposable PostgreSQL")
    schema = "mail_overlay_test_" + uuid4().hex
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema},
                                                    "statement_cache_size": 0})
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await conn.run_sync(Base.metadata.create_all)
        # production's natural key (bootstrap DDL only, not in the model); the carry's ON CONFLICT needs it
        await conn.execute(text("CREATE UNIQUE INDEX uq_snapshot_items_snap_ext_type "
                                "ON snapshot_items (snapshot_id, external_id, item_type)"))
        for ddl in _reuse_trigger_ddl():
            await conn.execute(text(ddl))
    factory = async_sessionmaker(engine, expire_on_commit=False)
    tenant, backend = uuid4(), uuid4()
    async with factory.begin() as session:
        org = models.Organization(name="Synthetic", slug=uuid4().hex)
        session.add(org)
        await session.flush()
        session.add(models.Tenant(id=tenant, org_id=org.id, display_name="Synthetic"))
        session.add(models.StorageBackend(id=backend, kind="azure", name=uuid4().hex,
                    endpoint="https://storage.invalid", config={}, secret_ref="synthetic-reference"))
    clock = [datetime(2026, 11, 5, 10)]

    async def resource(kind=models.ResourceType.USER_MAIL):
        rid = uuid4()
        async with factory.begin() as session:
            session.add(models.Resource(id=rid, tenant_id=tenant, type=kind, external_id=uuid4().hex,
                                        display_name="Synthetic mailbox"))
        return rid

    async def snapshot(rid, status=models.SnapshotStatus.IN_PROGRESS, **columns):
        job, sid = uuid4(), uuid4()
        clock[0] += timedelta(minutes=10)
        async with factory.begin() as session:
            session.add(models.Job(id=job, type=models.JobType.BACKUP, tenant_id=tenant, resource_id=rid,
                                   status=models.JobStatus.RUNNING))
            await session.flush()
            session.add(models.Snapshot(id=sid, resource_id=rid, job_id=job, backend_id=backend, status=status,
                                        started_at=clock[0], created_at=clock[0], extra_data={}, **columns))
        return sid, job

    async def item(sid, external_id, item_type="EMAIL", *, folder="Inbox", parent=None, size=10, name=None,
                   metadata=None, blob=None):
        async with factory.begin() as session:
            session.add(models.SnapshotItem(
                snapshot_id=sid, tenant_id=tenant, backend_id=backend, item_type=item_type,
                external_id=external_id, parent_external_id=parent, name=name or external_id, folder_path=folder,
                content_size=size, blob_path=blob, extra_data=metadata or {}))

    async def effective(sid):
        async with factory() as session:
            rows = (await session.execute(text(
                "SELECT item_type, external_id, name, folder_path FROM snapshot_items_effective(:s) "
                "ORDER BY item_type, external_id"), {"s": sid})).all()
        return [tuple(r) for r in rows]

    yield type("Db", (), {"factory": factory, "tenant": tenant, "backend": backend,
                          "resource": staticmethod(resource), "snapshot": staticmethod(snapshot),
                          "item": staticmethod(item), "effective": staticmethod(effective)})
    async with engine.begin() as conn:
        await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    await engine.dispose()


async def _base_with_mail(db):
    rid = await db.resource()
    base, _ = await db.snapshot(rid, models.SnapshotStatus.COMPLETED)
    for m in "abcd":
        await db.item(base, m)
    for m in "abc":
        await db.item(base, f"{m}::1", "EMAIL_ATTACHMENT", parent=m)
    return rid, base


async def test_full_snapshot_and_pointer_read_their_own_or_roots_rows(db):
    rid, base = await _base_with_mail(db)
    pointer, _ = await db.snapshot(rid, reuse_of_snapshot_id=base, reuse_chain_root_id=base)
    assert await db.effective(base) == await db.effective(pointer)
    assert len(await db.effective(base)) == 7


async def test_overlay_reads_own_rows_plus_base_minus_overrides_and_removals(db):
    rid, base = await _base_with_mail(db)
    overlay, _ = await db.snapshot(rid, models.SnapshotStatus.COMPLETED, overlay_base_id=base)
    await db.item(overlay, "b", name="b changed", folder="Archive")
    await db.item(overlay, "e")
    await db.item(overlay, "e::1", "EMAIL_ATTACHMENT", parent="e")
    async with db.factory.begin() as session:
        await session.execute(text(
            "INSERT INTO snapshot_overlay_removals (snapshot_id, item_type, external_id) VALUES "
            "(:o, 'EMAIL', 'c'), (:o, 'EMAIL_ATTACHMENT', 'a::1')"), {"o": overlay})
    assert await db.effective(overlay) == [
        ("EMAIL", "a", "a", "Inbox"),
        ("EMAIL", "b", "b changed", "Archive"),
        ("EMAIL", "d", "d", "Inbox"),
        ("EMAIL", "e", "e", "Inbox"),
        # base attachment of a message the overlay re-captured stays (the carry keeps it today)
        ("EMAIL_ATTACHMENT", "b::1", "b::1", "Inbox"),
        ("EMAIL_ATTACHMENT", "e::1", "e::1", "Inbox"),
    ]
    pointer, _ = await db.snapshot(rid, reuse_of_snapshot_id=overlay, reuse_chain_root_id=overlay)
    assert await db.effective(pointer) == await db.effective(overlay)


async def test_overlay_inherits_only_mail_rows_of_its_base(db):
    """The carry copies EMAIL/EMAIL_ATTACHMENT only; any other row type in the base is not part of the
    next run's inventory."""
    rid, base = await _base_with_mail(db)
    await db.item(base, "settings", "MAILBOX_SETTINGS")
    overlay, _ = await db.snapshot(rid, overlay_base_id=base)
    assert ("MAILBOX_SETTINGS", "settings", "settings", "Inbox") not in await db.effective(overlay)


async def test_database_refuses_an_overlay_of_an_overlay_a_pointer_or_another_resource(db):
    from sqlalchemy.exc import DBAPIError
    rid, base = await _base_with_mail(db)
    overlay, _ = await db.snapshot(rid, overlay_base_id=base)
    other = await db.resource()
    foreign, _ = await db.snapshot(other, models.SnapshotStatus.COMPLETED)
    pointer, _ = await db.snapshot(rid, reuse_of_snapshot_id=base, reuse_chain_root_id=base)
    for bad_base in (overlay, foreign, pointer):
        with pytest.raises(DBAPIError):
            await db.snapshot(rid, overlay_base_id=bad_base)
    with pytest.raises(DBAPIError):
        await db.snapshot(rid, overlay_base_id=base, reuse_of_snapshot_id=base, reuse_chain_root_id=base)


async def test_manual_railway_script_applies_twice_as_a_non_superuser_owner():
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires explicitly configured disposable PostgreSQL")
    suffix = uuid4().hex
    role, schema = "overlay_owner_" + suffix, "overlay_script_" + suffix
    admin = create_async_engine(url, connect_args={"statement_cache_size": 0})
    async with admin.begin() as conn:
        await conn.execute(text(f'CREATE ROLE "{role}" NOLOGIN NOSUPERUSER'))
        await conn.execute(text(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{role}"'))
    script = (Path(__file__).parents[2] / "scripts" / "2026_11_06_mail_overlay_snapshots.sql").read_text()
    try:
        async with admin.connect() as conn:
            await conn.execute(text(f'SET ROLE "{role}"'))
            await conn.execute(text(f'SET search_path TO "{schema}"'))
            await conn.run_sync(Base.metadata.create_all)
            for ddl in ("DROP TRIGGER snapshots_overlay_validate ON snapshots",
                        "DROP FUNCTION validate_snapshot_overlay_base()", "DROP FUNCTION snapshot_items_effective(uuid)",
                        "DROP TABLE snapshot_overlay_removals", "ALTER TABLE snapshots DROP COLUMN overlay_base_id"):
                await conn.execute(text(ddl))
            await conn.commit()
            raw = (await conn.get_raw_connection()).driver_connection
            await raw.execute(script)
            await raw.execute(script)
            assert await raw.fetchval("SELECT to_regprocedure('snapshot_items_effective(uuid)') IS NOT NULL")
            assert not await raw.fetchval("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")
            await conn.execute(text("RESET ROLE"))
            await conn.commit()
    finally:
        async with admin.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
            await conn.execute(text(f'DROP ROLE "{role}"'))
        await admin.dispose()


# ── The carry: same mailbox history, overlay on vs today's full copy ────────────────────────────

def _worker_module():
    import importlib.util
    import sys
    name = "bw_mail_overlay"
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, Path(__file__).parents[2] / "workers" / "backup-worker" / "main.py")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return sys.modules[name]


@pytest.fixture
def carry(db, monkeypatch):
    """The worker's real mail carry-forward, its DB slot bound to the test schema."""
    from contextlib import asynccontextmanager
    worker = _worker_module()

    @asynccontextmanager
    async def slot(key, **kwargs):
        async with db.factory() as session:
            yield session
    monkeypatch.setattr(worker, "carry_forward_slot_session", slot)
    monkeypatch.setattr(worker, "async_session_factory", db.factory)
    return worker._carry_forward_prior_mail_snapshot_items


_EFFECTIVE_PAYLOAD = (
    "SELECT item_type, external_id, name, folder_path, blob_path, content_size, parent_external_id, "
    "metadata::text FROM snapshot_items_effective(:s) ORDER BY item_type, external_id")


class Mailbox:
    """Server state of one mailbox and what a run writes, as the drain does: fresh EMAIL (+attachment)
    rows for new / changed / moved messages and for every message of a re-bootstrapped folder,
    Graph's @removed ids as tombstones (a move is a tombstone plus a fresh row), then the carry.
    ``tier1`` mimics shared/room mailboxes (folder whitelist + pathless drop, no excluded paths);
    ``archive`` messages are re-listed in full every run like the tier-1 archive tier."""

    def __init__(self, db, carry, monkeypatch, rid, *, overlay, tier1=False):
        self.db, self.carry, self.monkeypatch, self.rid = db, carry, monkeypatch, rid
        self.overlay, self.tier1 = overlay, tier1
        self.messages = {}
        self.folders = {"Inbox": "fid-inbox", "Archive": "fid-archive", "Projects": "fid-projects"}
        self.snapshots = []

    def _rows(self, mid):
        m = self.messages[mid]
        meta = {"raw": {"parentFolderId": self.folders.get(m["folder"], "fid-gone"), "subject": m["subject"]}}
        if m.get("source"):
            meta["source_mailbox"] = m["source"]
        rows = [dict(item_type="EMAIL", external_id=mid, name=f"{m['subject']} v{m['v']}",
                     folder=m["folder"], blob=f"mail/{mid}/v{m['v']}", metadata=meta, size=100 + m["v"])]
        for n in range(m["atts"]):
            att = {"parent_item_id": mid}
            if n % 2 == 0:
                att["inline_b64"] = "aGk="
            rows.append(dict(item_type="EMAIL_ATTACHMENT", external_id=f"{mid}::{n}", parent=mid,
                             name=f"file{n}.txt", folder=m["folder"], metadata=att, size=7,
                             blob=None if n % 2 == 0 else f"att/{mid}/{n}/v{m['v']}"))
        return rows

    async def run(self, *, add=(), change=(), move=(), delete=(), rebootstrap=(), drop_folders=(),
                  status=models.SnapshotStatus.COMPLETED):
        from shared.mail_snapshot_reuse import reuse_if_unchanged
        fresh, tombs = set(), set()
        for mid, folder, atts, *source in add:
            self.messages[mid] = {"folder": folder, "subject": mid, "v": 1, "atts": atts,
                                  "source": source[0] if source else None}
            fresh.add(mid)
        for mid in change:
            self.messages[mid]["v"] += 1
            fresh.add(mid)
        for mid, folder in move:
            self.messages[mid]["folder"] = folder
            fresh.add(mid)
            tombs.add(mid)
        for mid in delete:
            self.messages.pop(mid)
            fresh.discard(mid)
            tombs.add(mid)
        for folder in drop_folders:
            self.folders.pop(folder)
            for mid in [m for m, v in self.messages.items() if v["folder"] == folder]:
                self.messages.pop(mid)
                fresh.discard(mid)
        fresh |= {m for m, v in self.messages.items() if v["folder"] in rebootstrap or v.get("source")}
        sid, job = await self.db.snapshot(self.rid)
        for mid in sorted(fresh):
            for row in self._rows(mid):
                await self.db.item(sid, row.pop("external_id"), row.pop("item_type"), **row)
        self.monkeypatch.setenv("MAIL_OVERLAY_ENABLED", "true" if self.overlay else "false")
        unchanged = not (fresh or tombs or rebootstrap or drop_folders)
        kwargs = {}
        if self.tier1:
            kwargs = dict(keep_folder_ids=sorted(f for name, f in self.folders.items() if name not in rebootstrap),
                          exclude_pathless=True)
        pointed = await reuse_if_unchanged(self.db.factory, unchanged=unchanged, resource_id=self.rid,
                                           target_snapshot_id=sid, current_job_id=job,
                                           keep_folder_ids=kwargs.get("keep_folder_ids"))
        if not pointed:
            await self.carry(tenant_id=str(self.db.tenant), resource_id=str(self.rid), target_snapshot_id=str(sid),
                             current_job_id=job, tombstoned_ids=sorted(tombs),
                             excluded_paths=None if self.tier1 else sorted(rebootstrap), **kwargs)
        await self.settle(sid, job, status)
        self.snapshots.append(sid)
        return sid

    async def settle(self, sid, job, status):
        """The worker's settle: totals over the snapshot's inventory, delta against the prior."""
        from sqlalchemy import func, select
        from shared.snapshot_reuse import items_scope
        child = ("CHAT_ATTACHMENT", "EMAIL_ATTACHMENT", "CHAT_HOSTED_CONTENT")
        delta = _worker_module().BackupWorker._compute_snapshot_delta_from_prior
        async with self.db.factory.begin() as session:
            items, size = (await session.execute(select(
                func.count(models.SnapshotItem.id).filter(~models.SnapshotItem.item_type.in_(child)),
                func.coalesce(func.sum(models.SnapshotItem.content_size), 0),
            ).where(await items_scope(session, sid)))).one()
            new, added = await delta(session, sid, self.rid, items, size, current_job_id=job)
            await session.execute(text(
                "UPDATE snapshots SET status = :st, completed_at = created_at, item_count = :n, bytes_total = :b, "
                "new_item_count = :new, bytes_added = :added WHERE id = :s"),
                {"s": sid, "st": status.name, "n": items, "b": size, "new": new, "added": added})

    async def counters(self, sid=None):
        async with self.db.factory() as session:
            return tuple((await session.execute(text(
                "SELECT item_count, bytes_total, new_item_count, bytes_added FROM snapshots WHERE id = :s"),
                {"s": sid or self.snapshots[-1]})).one())

    async def inventory(self, sid=None):
        async with self.db.factory() as session:
            return [tuple(r) for r in (await session.execute(text(_EFFECTIVE_PAYLOAD),
                                                             {"s": sid or self.snapshots[-1]})).all()]

    async def shape(self, sid=None):
        """(overlay base, pointer target, own rows, removals) of a snapshot."""
        async with self.db.factory() as session:
            return (await session.execute(text(
                "SELECT s.overlay_base_id, s.reuse_chain_root_id, "
                "(SELECT count(*) FROM snapshot_items i WHERE i.snapshot_id = s.id), "
                "(SELECT count(*) FROM snapshot_overlay_removals r WHERE r.snapshot_id = s.id) "
                "FROM snapshots s WHERE s.id = :s"), {"s": sid or self.snapshots[-1]})).one()


class Twin:
    """The same history applied to an overlay mailbox and a full-copy mailbox."""

    def __init__(self, overlay, full):
        self.overlay, self.full = overlay, full

    async def run(self, **ops):
        await self.overlay.run(**ops)
        await self.full.run(**ops)
        got, want = await self.overlay.inventory(), await self.full.inventory()
        assert got == want, (sorted(set(got) - set(want)), sorted(set(want) - set(got)))
        assert await self.overlay.counters() == await self.full.counters()
        return got


@pytest_asyncio.fixture
async def twin(db, carry, monkeypatch):
    async def make(*, tier1=False):
        pair = []
        for overlay in (True, False):
            rid = await db.resource(models.ResourceType.SHARED_MAILBOX if tier1 else models.ResourceType.USER_MAIL)
            pair.append(Mailbox(db, carry, monkeypatch, rid, overlay=overlay, tier1=tier1))
        t = Twin(*pair)
        await t.run(add=[(f"m{n:03d}", ("Inbox", "Archive", "Projects")[n % 3], n % 3) for n in range(60)])
        return t
    return make


async def test_one_new_message_writes_only_that_message(twin):
    t = await twin()
    base = t.overlay.snapshots[-1]
    await t.run(add=[("new-1", "Inbox", 2)])
    assert tuple(await t.overlay.shape()) == (base, None, 3, 0)
    assert (await t.full.shape())[2] == 60 + 60 + 3
    # 61 messages, one of them new; bytes are every row's size, the added ones are the new rows'
    assert await t.overlay.counters() == (61, 60 * 101 + 60 * 7 + 101 + 14, 1, 101 + 14)


async def test_deleted_message_is_a_removal_and_hides_its_attachments(twin):
    t = await twin()
    inventory = await t.run(delete=["m002"])
    assert not [r for r in inventory if r[1].startswith("m002")]
    assert (await t.overlay.shape())[3] == 1


async def test_moved_message_reads_in_its_new_folder_once(twin):
    t = await twin()
    inventory = await t.run(move=[("m004", "Archive")])
    assert [r[3] for r in inventory if r[1] == "m004"] == ["Archive"]


async def test_rebootstrapped_folder_drops_what_the_server_no_longer_has(twin):
    t = await twin()
    t.overlay.messages.pop("m001")   # deleted server-side while the cursor was lost: no tombstone
    t.full.messages.pop("m001")
    inventory = await t.run(rebootstrap=["Archive"])
    assert not [r for r in inventory if r[1].startswith("m001")]


async def test_attachments_inline_and_blob_follow_their_message(twin):
    t = await twin()
    inventory = await t.run(add=[("att-1", "Projects", 2)], change=["m005"])
    atts = {r[1]: (r[4], r[7]) for r in inventory if r[0] == "EMAIL_ATTACHMENT" and r[1].startswith(("att-1", "m005"))}
    assert atts["att-1::0"][0] is None and "inline_b64" in atts["att-1::0"][1]
    assert atts["att-1::1"][0] == "att/att-1/1/v1"
    assert atts["m005::1"][0] == "att/m005/1/v2"


async def test_consecutive_overlays_share_one_base_and_carry_their_changes(twin):
    t = await twin()
    base = t.overlay.snapshots[-1]
    await t.run(add=[("c1", "Inbox", 1)])
    await t.run(move=[("c1", "Projects")], delete=["m010"])
    await t.run(change=["m011"], add=[("c2", "Archive", 0)])
    await t.run(delete=["c1"], rebootstrap=["Inbox"])
    for sid in t.overlay.snapshots[1:]:
        assert (await t.overlay.shape(sid))[0] == base


async def test_base_row_superseded_by_the_prior_overlay_does_not_resurface(twin, monkeypatch):
    """Run 1 moves m007 (the overlay owns the moved copy); run 2 re-bootstraps that folder and the
    message is gone server-side. Copying only the prior's removal list would bring back the base's
    pre-move copy."""
    monkeypatch.setenv("MAIL_OVERLAY_MAX_RATIO", "1")
    t = await twin()
    await t.run(move=[("m007", "Projects")])
    for box in (t.overlay, t.full):
        box.messages.pop("m007")
    inventory = await t.run(rebootstrap=["Projects"])
    assert (await t.overlay.shape())[0] == t.overlay.snapshots[0]
    assert not [r for r in inventory if r[1].startswith("m007")]


async def test_message_that_reappears_does_not_regain_its_old_base_attachments(twin, monkeypatch):
    monkeypatch.setenv("MAIL_OVERLAY_MAX_RATIO", "1")
    t = await twin()
    await t.run(delete=["m001"])
    inventory = await t.run(add=[("m001", "Inbox", 0)])
    assert (await t.overlay.shape())[0] == t.overlay.snapshots[0]
    assert [r[1] for r in inventory if r[1].startswith("m001")] == ["m001"]


async def test_unchanged_run_after_an_overlay_points_at_the_overlay(twin):
    t = await twin()
    await t.run(add=[("p1", "Inbox", 1)])
    overlay = t.overlay.snapshots[-1]
    await t.run()
    assert tuple(await t.overlay.shape()) == (None, overlay, 0, 0)


async def test_overlay_past_the_size_threshold_compacts_to_a_full_copy(twin, monkeypatch):
    t = await twin()
    monkeypatch.setenv("MAIL_OVERLAY_MAX_RATIO", "0.1")
    await t.run(add=[(f"bulk{n}", "Inbox", 0) for n in range(5)])
    assert (await t.overlay.shape())[0] is not None
    await t.run(add=[(f"bulk{n}", "Inbox", 1) for n in range(5, 25)])
    compacted = t.overlay.snapshots[-1]
    assert (await t.overlay.shape())[0] is None
    await t.run(add=[("after", "Inbox", 0)])
    assert (await t.overlay.shape())[0] == compacted


async def test_overlay_of_an_old_base_compacts_to_a_full_copy(twin, monkeypatch):
    t = await twin()
    monkeypatch.setenv("MAIL_OVERLAY_MAX_AGE_DAYS", "0.02")   # ~29 minutes; one mailbox's runs are 20 minutes apart
    await t.run(add=[("young", "Inbox", 0)])
    assert (await t.overlay.shape())[0] is not None
    await t.run(add=[("old", "Inbox", 0)])
    assert (await t.overlay.shape())[0] is None


async def test_shared_mailbox_whitelist_and_vanished_folder(twin):
    t = await twin(tier1=True)
    await t.run(add=[("s1", "Inbox", 1)])
    assert (await t.overlay.shape())[0] is not None
    await t.run(drop_folders=["Projects"])
    await t.run(rebootstrap=["Archive"], delete=["m003"])


async def test_archive_tier_rows_are_relisted_not_inherited(twin):
    t = await twin()
    await t.run(add=[("arch-1", "Inbox", 1, "archive"), ("arch-2", "Inbox", 0, "archive")])
    t.overlay.messages.pop("arch-1")
    t.full.messages.pop("arch-1")
    inventory = await t.run(add=[("x", "Inbox", 0)])
    assert not [r for r in inventory if r[1].startswith("arch-1")]


async def test_flag_off_writes_exactly_todays_full_copy(db, carry, monkeypatch):
    box = Mailbox(db, carry, monkeypatch, await db.resource(), overlay=False)
    await box.run(add=[("a", "Inbox", 1), ("b", "Inbox", 0)])
    await box.run(add=[("c", "Inbox", 0)])
    assert tuple(await box.shape()) == (None, None, 4, 0)


async def test_random_histories_match_the_full_copy(twin, monkeypatch):
    import random
    monkeypatch.setenv("MAIL_OVERLAY_MAX_RATIO", "0.7")   # mostly overlays of overlays, some compactions
    for seed in range(3):
        rng = random.Random(seed)
        t = await twin(tier1=seed == 2)
        made = 0
        for step in range(25):
            box = t.overlay.messages
            ops, live = {}, sorted(box)
            pick = lambda k: rng.sample(live, min(k, len(live)))  # noqa: E731
            roll = rng.random()
            if roll < 0.15:
                ops = {}
            elif roll < 0.35:
                ops["add"] = [(f"r{seed}-{step}-{n}", rng.choice(["Inbox", "Archive", "Projects"]), rng.randint(0, 2))
                              for n in range(rng.randint(1, 3))]
            elif roll < 0.5:
                ops["delete"] = pick(rng.randint(1, 2))
            elif roll < 0.65:
                ops["move"] = [(m, rng.choice(["Inbox", "Archive", "Projects"])) for m in pick(1)]
            elif roll < 0.8:
                ops["change"] = pick(rng.randint(1, 3))
            else:
                folder = rng.choice(["Inbox", "Archive", "Projects"])
                for m in [m for m, v in box.items() if v["folder"] == folder][:1]:
                    t.overlay.messages.pop(m)   # vanished while the folder's cursor was lost
                    t.full.messages.pop(m)
                ops["rebootstrap"] = [folder]
            await t.run(**ops)
            made += (await t.overlay.shape())[0] is not None
        assert made > 5, f"seed {seed} built only {made} overlays"


async def test_whitelisted_pointer_checks_the_overlays_inherited_rows(db):
    """A shared/room mailbox may point at its prior only if every prior message sits in a kept
    folder; for an overlay prior that includes the base rows it inherits."""
    from shared.mail_snapshot_reuse import reuse_unchanged_mail_snapshot
    rid = await db.resource(models.ResourceType.SHARED_MAILBOX)
    base, _ = await db.snapshot(rid, models.SnapshotStatus.COMPLETED)
    await db.item(base, "stale", metadata={"raw": {"parentFolderId": "fid-gone"}})
    overlay, _ = await db.snapshot(rid, models.SnapshotStatus.COMPLETED, overlay_base_id=base)
    await db.item(overlay, "fresh", metadata={"raw": {"parentFolderId": "fid-inbox"}})
    target, job = await db.snapshot(rid)
    async with db.factory.begin() as session:
        assert await reuse_unchanged_mail_snapshot(session, resource_id=rid, target_snapshot_id=target,
                                                   current_job_id=job, keep_folder_ids=["fid-inbox"]) is None
    async with db.factory.begin() as session:
        assert await reuse_unchanged_mail_snapshot(session, resource_id=rid, target_snapshot_id=target,
                                                   current_job_id=job, keep_folder_ids=["fid-inbox", "fid-gone"]) == overlay


# ── Readers: an overlay reads exactly like the full copy ────────────────────────────────────────

def _load(name, relative):
    import importlib.util
    import sys
    folder = str((Path(__file__).parents[2] / relative).parent)
    if folder not in sys.path:
        sys.path.insert(0, folder)   # worker dirs import their siblings (mail_restore, ...)
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, Path(__file__).parents[2] / relative)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return sys.modules[name]


def _norm(rows, *keys):
    return sorted(tuple(str(r.get(k)) for k in keys) for r in rows)


async def _reader_twin(twin):
    t = await twin()
    await t.run(add=[("new-1", "Inbox", 2)], move=[("m004", "Archive")])
    await t.run(delete=["m002"], change=["m005"])
    return t


async def test_single_snapshot_readers_see_the_overlays_inventory(twin, db):
    api = _load("overlay_snapshot_api", "services/snapshot-service/main.py")
    t = await _reader_twin(twin)
    seen = []
    for box in (t.overlay, t.full):
        sid, rid = str(box.snapshots[-1]), str(box.rid)
        async with db.factory() as session:
            listed = await api.list_snapshot_items(sid, page=1, size=1000, itemType=None, db=session)
            browsed = await api.browse_snapshot_items(rid, sid, page=1, size=1000, itemType="EMAIL", db=session)
            found = await api.search_snapshot_items(rid, query="m00", item_type=None, folder_path=None,
                                                    snapshot_id=sid, page=1, size=1000, db=session)
            from shared.folder_resolver import resolve_selection
            picked = await resolve_selection(session, snapshot_id=sid, item_ids=[], folder_paths=["Inbox"],
                                             excluded_item_ids=[])
        keys = ("itemType", "externalId", "name", "folderPath")
        seen.append((listed.totalElements, _norm([i.model_dump() for i in listed.content], *keys),
                     browsed.totalElements, _norm([i.model_dump() for i in browsed.content], *keys),
                     found.totalElements, _norm([i.model_dump() for i in found.content], *keys),
                     sorted((r.item_type, r.external_id, r.name) for r in picked)))
    assert seen[0] == seen[1]
    assert seen[0][0] == 60 + 60   # 60 + 1 new - 1 deleted messages; 60 + 2 new - 2 of the deleted attachments


async def test_preview_content_and_attachments_of_an_inherited_message(twin, db):
    """The Recovery UI asks for item X 'in snapshot S'; X may be a base row S inherits."""
    api = _load("overlay_snapshot_api", "services/snapshot-service/main.py")
    t = await _reader_twin(twin)
    overlay, base = t.overlay.snapshots[-1], t.overlay.snapshots[0]
    async with db.factory() as session:
        rows = {(r.item_type, r.external_id): r for r in (await session.execute(text(
            "SELECT id, item_type, external_id, snapshot_id FROM snapshot_items_effective(:s)"), {"s": overlay})).all()}
        email, inline = rows[("EMAIL", "m007")], rows[("EMAIL_ATTACHMENT", "m007::0")]
        assert email.snapshot_id == inline.snapshot_id == base
        assert (await api.get_snapshot_item_detail(str(overlay), str(email.id), session)).externalId == "m007"
        content = await api.get_item_content(str(overlay), str(inline.id), download=False, db=session)
        assert content is not None
        names = [a["name"] for a in await api.get_item_attachments(str(overlay), str(email.id), session)]
        assert names == ["file0.txt"]
        own = rows[("EMAIL", "new-1")]
        assert [a["name"] for a in await api.get_item_attachments(str(overlay), str(own.id), session)] == [
            "file0.txt", "file1.txt"]
        hidden = (await session.execute(text(
            "SELECT id FROM snapshot_items WHERE snapshot_id = :b AND external_id = 'm002' AND item_type = 'EMAIL'"),
            {"b": base})).scalar_one()
        from fastapi import HTTPException
        with pytest.raises(HTTPException):
            await api.get_snapshot_item_detail(str(overlay), str(hidden), session)


async def test_sibling_union_readers_and_restore_match_the_full_copy(twin, db, monkeypatch):
    """Mail list, folder tree, restore / PST / ZIP selections union every snapshot up to S, newest
    wins; base + overlays hold the same payloads as the full copies."""
    api = _load("overlay_snapshot_api", "services/snapshot-service/main.py")
    monkeypatch.setattr(api, "_count_session_factory", db.factory)
    restore = _load("overlay_restore_worker", "workers/restore-worker/main.py")
    worker = restore.RestoreWorker.__new__(restore.RestoreWorker)
    t = await _reader_twin(twin)
    seen = []
    for box in (t.overlay, t.full):
        sid = str(box.snapshots[-1])
        async with db.factory() as session:
            mail = await api.list_snapshot_emails(sid, page=1, size=500, folder=None, search=None, source=None,
                                                  category=None, bodies="none", db=session)
            folders = await api.get_snapshot_folders(snapshot_id=sid, item_type="EMAIL", category=None, page=1,
                                                     size=50, db=session)
            fetched = await worker.fetch_snapshot_items(session, [sid], [], folder_paths=["Archive"])
            counted = await worker.count_snapshot_items(session, [sid], [])
        streamed = [i async for batch in worker.stream_snapshot_items_by_group(db.factory, [sid], []) for i in batch]
        seen.append((mail["totalElements"], _norm(mail["content"], "externalId", "subject", "folderPath"),
                     folders["content"], sorted((i.item_type, i.external_id, i.name) for i in fetched), counted,
                     sorted((i.item_type, i.external_id, i.name, i.folder_path) for i in streamed)))
    assert seen[0] == seen[1]


async def test_restore_and_export_size_estimates_cover_the_overlays_inventory(twin, db):
    """job-service routes a restore/export by the selection's bytes; an overlay sized by its own
    rows would send a whole-mailbox restore to the normal queue."""
    from sqlalchemy import func, select
    from shared.snapshot_reuse import items_scope_any
    t = await _reader_twin(twin)
    sizes = []
    for box in (t.overlay, t.full):
        async with db.factory() as session:
            sizes.append(await session.scalar(select(func.coalesce(func.sum(models.SnapshotItem.content_size), 0))
                                              .where(await items_scope_any(session, [str(box.snapshots[-1]), str(uuid4())]))))
    assert sizes[0] == sizes[1] > 0


# ── Retention: retiring bases and middle snapshots keeps every survivor's inventory ─────────────

async def _retire(box, doomed):
    """Delete ``doomed`` through retention; return the survivors' inventories before and after."""
    from shared.retention_cleanup import _delete_snapshots
    survivors = [s for s in box.snapshots if s not in doomed]
    before = [await box.inventory(s) for s in survivors]
    async with box.db.factory.begin() as session:
        deleted = await _delete_snapshots(session, set(doomed))
    assert deleted == len(doomed)
    box.snapshots = survivors
    assert [await box.inventory(s) for s in survivors] == before
    return survivors


async def test_retiring_the_base_materialises_the_earliest_overlay_and_rebases_the_rest(twin):
    t = await twin()
    for ops in ({"add": [("r1", "Inbox", 1)]}, {"move": [("r1", "Archive")], "delete": ["m001"]},
                {"change": ["m005"], "add": [("r2", "Projects", 2)]}):
        await t.run(**ops)
    base, o1, o2, o3 = t.overlay.snapshots
    await _retire(t.overlay, [base])
    assert tuple(await t.overlay.shape(o1))[:2] == (None, None)
    assert (await t.overlay.shape(o2))[0] == (await t.overlay.shape(o3))[0] == o1


async def test_retiring_base_and_a_middle_overlay_together(twin):
    t = await twin()
    for ops in ({"add": [("r1", "Inbox", 1)]}, {"delete": ["m001", "r1"]}, {"move": [("m004", "Projects")]}):
        await t.run(**ops)
    base, o1, o2, o3 = t.overlay.snapshots
    await _retire(t.overlay, [base, o2])
    assert (await t.overlay.shape(o3))[0] == o1


async def test_retiring_a_base_with_pointer_heir_repoints_its_overlays(twin):
    t = await twin()
    await t.run()                                  # unchanged: pointer at the base
    await t.run(add=[("r1", "Inbox", 1)])          # prior is the base (pointers are skipped)
    await t.run(delete=["m003"])
    base, pointer, o1, o2 = t.overlay.snapshots
    assert (await t.overlay.shape(pointer))[1] == base and (await t.overlay.shape(o1))[0] == base
    await _retire(t.overlay, [base])
    assert (await t.overlay.shape(o1))[0] == (await t.overlay.shape(o2))[0] == pointer


async def test_retiring_an_overlay_hands_its_inventory_to_its_pointer(twin):
    t = await twin()
    await t.run(add=[("r1", "Inbox", 1)])
    await t.run()                                  # unchanged: pointer at the overlay
    await t.run(delete=["m003"])
    base, o1, pointer, o2 = t.overlay.snapshots
    await _retire(t.overlay, [o1])
    assert tuple(await t.overlay.shape(pointer))[:2] == (base, None)


async def test_retiring_base_and_overlay_whose_pointer_survives(twin):
    t = await twin()
    await t.run(add=[("r1", "Inbox", 1)])
    await t.run()
    await t.run(delete=["m003"])
    base, o1, pointer, o2 = t.overlay.snapshots
    await _retire(t.overlay, [base, o1])
    assert tuple(await t.overlay.shape(pointer))[:2] == (None, None)
    assert (await t.overlay.shape(o2))[0] == pointer


async def test_blobs_of_a_base_an_overlay_still_reads_are_protected(twin, db):
    from shared.backup_cleanup import resolve_protected_reuse_roots
    t = await twin()
    await t.run(add=[("r1", "Inbox", 1)])
    base = t.overlay.snapshots[0]
    async with db.factory() as session:
        assert await resolve_protected_reuse_roots(session, [base]) == {base}
        assert await resolve_protected_reuse_roots(session, t.overlay.snapshots) == set()


async def test_partitioned_mailbox_union_carry_builds_an_overlay(db, carry, monkeypatch):
    """TM's big mailboxes are partitioned: the winning finalizer unions every shard's tombstones
    and excluded folders and carries once - that carry must become the overlay too."""
    worker_module = _worker_module()
    worker = worker_module.BackupWorker.__new__(worker_module.BackupWorker)
    worker.worker_id = "overlay-test"
    box = Mailbox(db, carry, monkeypatch, await db.resource(), overlay=True)
    await box.run(add=[(f"m{n:03d}", ("Inbox", "Archive")[n % 2], n % 2) for n in range(40)])
    base = box.snapshots[-1]
    sid, job = await db.snapshot(box.rid)
    await db.item(sid, "fresh-1", metadata={"raw": {"parentFolderId": "fid-inbox"}})
    async with db.factory.begin() as session:
        for index, (tombs, paths) in enumerate(((["m002"], []), ([], ["Archive"]))):
            session.add(models.SnapshotPartition(
                snapshot_id=sid, tenant_id=db.tenant, resource_id=box.rid, job_id=job, partition_type="MAIL_FOLDERS",
                partition_index=index, status="COMPLETED", total_files=1,
                payload={"folder_ids": [f"f{index}"], "carry_forward": {
                    "excluded_paths": paths, "tombstoned_ids": tombs, "resumed_any": True,
                    "drained_keys": [f"f{index}"]}}))
    for msg in [m for m, v in box.messages.items() if v["folder"] == "Archive"]:   # re-bootstrapped folder
        for row in box._rows(msg):
            await db.item(sid, row.pop("external_id"), row.pop("item_type"), **row)
    monkeypatch.setenv("MAIL_OVERLAY_ENABLED", "true")
    monkeypatch.setenv("MAIL_OVERLAY_MAX_RATIO", "1")   # half the mailbox re-bootstrapped this run
    await worker._carry_forward_partitioned_user_mail_inner(sid)
    shape = await box.shape(sid)
    assert shape[0] == base
    names = {r[1] for r in await box.inventory(sid) if r[0] == "EMAIL"}
    assert "fresh-1" in names and "m002" not in names and len(names) == 40
