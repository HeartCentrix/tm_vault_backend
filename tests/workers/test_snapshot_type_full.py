"""A snapshot may claim ``type=FULL`` only where the walk really WAS full.

Two defects, one column.

  (1) Write side (the original bug): an explicit force-full request
      (job spec fullBackup:true → resolve_full_backup() → message
      type=FULL / forceFullBackup=true) makes the USER_ONEDRIVE
      enumerator drop the saved deltaLink and re-crawl /root/delta, and
      every enumerated file gets its own row on THAT snapshot (dedup hits
      still stamp blob_path). The run genuinely is a baseline, yet the row
      was stored INCREMENTAL — prod has zero type=FULL rows outside
      Power BI.

  (2) Over-fix (found in review): deriving FULL from the *message* inside
      ``create_snapshot`` types it for all SIX call sites, including
        * the ``skipped_concurrent`` placeholder — created, immediately
          flipped COMPLETED with item_count=0, then RETURNED; the handler
          never runs. For a POWER_BI resource that empty COMPLETED row is
          the newest row in ``assemble_power_bi_items``' chain, and a FULL
          row *is* the chain base — so the newest restore point assembles
          ZERO artifacts. BACKUP_SKIP_CONCURRENT_JOBS defaults true.
        * resource types whose handlers ignore force-full entirely:
          USER_MAIL skips unchanged folders by per-folder fingerprint
          (USER_MAIL_FULL_RESCAN_DAYS=3), SHAREPOINT_SITE and USER_CHATS
          never read the flag. Labelling those a retention baseline is a
          lie in exactly the direction that loses data.

Fix: ``create_snapshot`` stays INCREMENTAL-by-default (mistyping a real
full walk INCREMENTAL is the *safe* direction — the Power BI chain then
keeps MORE history and the age policy forces another full sooner), and the
promotion to FULL happens at the one point that has actually decided to
walk everything: the USER_ONEDRIVE force-full branch, via
``_mark_snapshot_full``.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import types
import uuid
from datetime import datetime, timedelta

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://u:p@localhost/db")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_snaptype_under_test", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_snaptype_under_test"] = _bw
assert _spec and _spec.loader
_spec.loader.exec_module(_bw)  # type: ignore[union-attr]

from shared.models import (  # noqa: E402
    ResourceType, SnapshotStatus, SnapshotType,
)
from shared.power_bi_snapshot import assemble_power_bi_items  # noqa: E402


def _run(coro):
    # Self-contained loop per call (repo convention — see
    # tests/workers/test_archive_backup.py).
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ─────────────────────── create_snapshot fakes ────────────────────────

class FakeResult:
    def __init__(self, row=None):
        self.row = row

    def scalar_one_or_none(self):
        return self.row

    def one(self):
        return (0, 0)


class FakeSession:
    """Stand-in for the short-lived session create_snapshot opens: the
    advisory lock + prior-snapshot SELECT + INSERT, no real DB."""

    def __init__(self, prior=None, live=None):
        self.prior = prior
        self.live = live
        self.added: list = []
        self.commits = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, stmt, params=None):
        return FakeResult(self.prior)

    async def get(self, model, pk):
        return self.live

    def add(self, obj):
        self.added.append(obj)

    async def commit(self):
        self.commits += 1

    async def rollback(self):
        pass


def _create(monkeypatch, message, *, prior=None, **kwargs):
    session = FakeSession(prior)
    monkeypatch.setattr(_bw, "async_session_factory", lambda: session)
    w = object.__new__(_bw.BackupWorker)
    w.worker_id = "test-worker"
    resource = types.SimpleNamespace(id=uuid.uuid4(), display_name="jrue OneDrive")
    snap = _run(w.create_snapshot(resource, message, uuid.uuid4(), **kwargs))
    return snap, session


# ─── (1) the shared constructor must NOT guess FULL from the message ───

def test_force_full_message_alone_does_not_type_the_snapshot_full(monkeypatch):
    # create_snapshot is shared by six call sites; only two resource types
    # honour force-full at all. Typing from the message here mislabels the
    # rest — see the module docstring.
    snap, session = _create(
        monkeypatch, {"type": "FULL", "forceFullBackup": True},
    )
    assert session.commits == 1
    assert snap.type == SnapshotType.INCREMENTAL


def test_incremental_message_creates_incremental_snapshot(monkeypatch):
    snap, _ = _create(monkeypatch, {"type": "INCREMENTAL"})
    assert snap.type == SnapshotType.INCREMENTAL


def test_message_without_any_full_signal_is_incremental(monkeypatch):
    snap, _ = _create(monkeypatch, {})
    assert snap.type == SnapshotType.INCREMENTAL


def test_explicit_snapshot_type_is_honored(monkeypatch):
    # Callers that decide their own type keep it.
    snap, _ = _create(
        monkeypatch, {"type": "FULL"},
        snapshot_type=SnapshotType.PREEMPTIVE,
    )
    assert snap.type == SnapshotType.PREEMPTIVE


def test_resumed_prior_snapshot_is_returned_untouched(monkeypatch):
    prior = types.SimpleNamespace(
        id=uuid.uuid4(), type=SnapshotType.INCREMENTAL,
    )
    snap, session = _create(monkeypatch, {"type": "FULL"}, prior=prior)
    assert snap is prior
    assert session.commits == 0


# ─── the harm the over-fix would have done, at the consumer ───

def _pbi_item(snapshot_id, ext_id, *, created_at):
    return types.SimpleNamespace(
        snapshot_id=snapshot_id, external_id=ext_id, item_type="REPORT",
        name=ext_id, folder_path="", is_deleted=False, created_at=created_at,
    )


def test_skipped_concurrent_placeholder_cannot_truncate_power_bi_chain(monkeypatch):
    """The placeholder path (worker ~3189) calls create_snapshot and then
    immediately marks the row COMPLETED / item_count=0 / extra_data
    .skipped_concurrent — the handler never runs, so whatever type
    create_snapshot chose is final. Power BI's assembler treats the NEWEST
    FULL row as the chain base and discards everything older, so an empty
    FULL placeholder would hide the whole workspace."""
    t0 = datetime(2026, 7, 30, 12, 0, 0)
    real_full = types.SimpleNamespace(
        id=str(uuid.uuid4()), created_at=t0, type=SnapshotType.FULL,
        status=SnapshotStatus.COMPLETED,
    )
    # Same message a bulk force-full trigger carries.
    placeholder, _ = _create(
        monkeypatch, {"type": "FULL", "forceFullBackup": True},
    )
    placeholder_row = types.SimpleNamespace(
        id=str(placeholder.id), created_at=t0 + timedelta(hours=1),
        type=placeholder.type, status=SnapshotStatus.COMPLETED,
    )
    items = [_pbi_item(real_full.id, "report-1", created_at=t0)]

    assembled = assemble_power_bi_items([real_full, placeholder_row], items)
    assert [i.external_id for i in assembled] == ["report-1"], (
        "an empty skipped_concurrent placeholder became the chain base — "
        "the newest Power BI restore point assembles zero artifacts"
    )


# ─── (2) promotion happens where the full walk is decided ───

def test_mark_snapshot_full_updates_row_and_in_memory_attribute(monkeypatch):
    live = types.SimpleNamespace(
        id=uuid.uuid4(), type=SnapshotType.INCREMENTAL,
    )
    session = FakeSession(live=live)
    monkeypatch.setattr(_bw, "async_session_factory", lambda: session)
    w = object.__new__(_bw.BackupWorker)
    w.worker_id = "test-worker"
    snap = types.SimpleNamespace(id=live.id, type=SnapshotType.INCREMENTAL)

    _run(w._mark_snapshot_full(snap))

    assert live.type == SnapshotType.FULL
    assert session.commits == 1
    # complete_snapshot persists via session.merge(snapshot); leaving the
    # in-memory value stale would push INCREMENTAL back over the UPDATE.
    assert snap.type == SnapshotType.FULL


def test_mark_snapshot_full_survives_a_db_failure(monkeypatch):
    class Exploding(FakeSession):
        async def commit(self):
            raise RuntimeError("simulated PG failure")

    live = types.SimpleNamespace(id=uuid.uuid4(), type=SnapshotType.INCREMENTAL)
    monkeypatch.setattr(_bw, "async_session_factory", lambda: Exploding(live=live))
    w = object.__new__(_bw.BackupWorker)
    w.worker_id = "test-worker"
    snap = types.SimpleNamespace(id=live.id, type=SnapshotType.INCREMENTAL)
    # Typing is bookkeeping — it must never fail an otherwise good backup.
    _run(w._mark_snapshot_full(snap))
    assert snap.type == SnapshotType.INCREMENTAL


# ─────────── the real handler branch (Tier-2 fan-out) ───────────

class HandlerSession(FakeSession):
    """Session for the _backup_user_content_parallel path: hands back the
    Snapshot / Resource rows the handler mutates."""

    def __init__(self, snap_row, res_row):
        super().__init__()
        self.snap_row = snap_row
        self.res_row = res_row

    async def get(self, model, pk):
        name = getattr(model, "__name__", "")
        if name == "Snapshot":
            return self.snap_row
        if name == "Resource":
            return self.res_row
        return None

    def add_all(self, objs):
        self.added.extend(objs)


class FakeGraph:
    GRAPH_URL = "https://graph.microsoft.com/v1.0"

    def __init__(self, files):
        self.files = files
        self.urls: list = []

    async def _get(self, url, params=None):
        self.urls.append(url)
        return {
            "value": [
                {
                    "id": f["id"], "name": f["name"], "size": f["size"],
                    "file": {"mimeType": "application/octet-stream"},
                    "parentReference": {"path": "/drives/drive-A/root:"},
                }
                for f in self.files
            ],
            "@odata.deltaLink": f"{self.GRAPH_URL}/drives/drive-A/root/delta?token=NEW",
        }


def _run_tier2(monkeypatch, *, kind, message, resource_extra):
    """Drive the real Tier-2 handler far enough to exercise the
    enumerate-and-decide code, recording every _mark_snapshot_full call."""
    snap_row = types.SimpleNamespace(
        id=uuid.uuid4(), job_id=uuid.uuid4(), type=SnapshotType.INCREMENTAL,
        status=SnapshotStatus.IN_PROGRESS, started_at=datetime.utcnow(),
        item_count=0, new_item_count=0, bytes_total=0, bytes_added=0,
        extra_data={}, delta_tokens_json={}, completed_at=None,
        duration_secs=0,
    )
    resource = types.SimpleNamespace(
        id=uuid.uuid4(), display_name="jrue OneDrive",
        type=getattr(ResourceType, kind), extra_data=dict(resource_extra),
    )
    res_row = types.SimpleNamespace(id=resource.id, extra_data=dict(resource_extra))
    session_holder = {"s": None}

    def _factory():
        session_holder["s"] = HandlerSession(snap_row, res_row)
        return session_holder["s"]

    monkeypatch.setattr(_bw, "async_session_factory", _factory)

    async def _noop_cancel(_job_id):
        return None

    async def _terminal(_snap, proposed):
        return proposed

    async def _contacts(*a, **kw):
        return [], {}

    monkeypatch.setattr(_bw, "_raise_if_job_cancelled", _noop_cancel)
    monkeypatch.setattr(_bw, "_coerce_snapshot_terminal_on_cancel", _terminal)
    monkeypatch.setattr(_bw, "_backup_contacts_for_user", _contacts)
    monkeypatch.setattr(_bw.settings, "BACKUP_CONCURRENCY", 1)
    monkeypatch.setattr(_bw.settings, "ONEDRIVE_PARTITION_ENABLED", False)

    marked: list = []
    w = object.__new__(_bw.BackupWorker)
    w.worker_id = "test-worker"

    async def _create_snap(_res, _msg, _job):
        return snap_row

    async def _mark_full(snapshot):
        marked.append(snapshot)

    async def _sla(_res, _msg):
        return None

    async def _files(*a, **kw):
        return len(a[5]) if len(a) > 5 else 0, 0

    async def _flush(force=False):
        return 0

    async def _delta(*a, **kw):
        return 0, 0

    async def _update_info(*a, **kw):
        return None

    w.create_snapshot = _create_snap
    w._mark_snapshot_full = _mark_full
    w.get_sla_policy = _sla
    w._should_partition_onedrive = lambda _items: False
    w._useronedrive_backup_files = _files
    w.flush_version_rows = _flush
    w._compute_snapshot_delta_from_prior = _delta
    w.update_resource_backup_info = _update_info

    graph = FakeGraph([{"id": "f1", "name": "one.docx", "size": 1024}])
    tenant = types.SimpleNamespace(id=uuid.uuid4())
    _run(w._backup_user_content_parallel(
        [resource], graph, tenant, message, snap_row.job_id,
    ))
    return marked, snap_row, res_row, graph


def test_force_full_onedrive_walk_is_typed_full(monkeypatch):
    # forceFullBackup drops the saved deltaLink and re-crawls the whole
    # drive — this snapshot really is a baseline.
    marked, snap_row, _res_row, graph = _run_tier2(
        monkeypatch, kind="USER_ONEDRIVE",
        message={"type": "FULL", "forceFullBackup": True},
        resource_extra={
            "user_id": "u1", "drive_id": "drive-A",
            "onedrive_delta_link": "https://graph/old-token",
        },
    )
    assert [s is snap_row for s in marked] == [True], (
        "a force-full OneDrive run re-walks /root/delta and writes a row "
        "for every file — it must be recorded as the FULL baseline"
    )
    assert not any("token=" in u for u in graph.urls), (
        "force-full must ignore the saved deltaLink"
    )


def test_incremental_onedrive_walk_stays_incremental(monkeypatch):
    marked, _snap, _res, graph = _run_tier2(
        monkeypatch, kind="USER_ONEDRIVE",
        message={"type": "INCREMENTAL"},
        resource_extra={
            "user_id": "u1", "drive_id": "drive-A",
            "onedrive_delta_link": "https://graph/old-token",
        },
    )
    assert marked == [], (
        "an incremental resumed from the saved deltaLink only sees changed "
        "files — typing it FULL would fake a baseline"
    )
    assert graph.urls == ["https://graph/old-token"]


def test_force_full_on_a_type_that_ignores_it_stays_incremental(monkeypatch):
    # USER_CONTACTS (like USER_MAIL / SHAREPOINT_SITE / USER_CHATS) never
    # reads the force-full flag, so its walk is unchanged by it.
    marked, _snap, _res, _graph = _run_tier2(
        monkeypatch, kind="USER_CONTACTS",
        message={"type": "FULL", "forceFullBackup": True},
        resource_extra={"user_id": "u1"},
    )
    assert marked == []


# ─── the predicate itself (single source of the force-full signal) ───

def test_predicate_matches_only_the_signal_the_enumerator_honors():
    f = _bw.is_force_full_backup_message
    assert f({"forceFullBackup": True}) is True
    assert f({"type": "FULL"}) is True
    assert f({"type": "INCREMENTAL"}) is False
    assert f({}) is False
    assert f(None) is False
    # Aliases the OneDrive enumerator does NOT honour: it keeps the saved
    # deltaLink, so the walk stays incremental.
    assert f({"fullBackup": True}) is False
    assert f({"full_backup": True}) is False
