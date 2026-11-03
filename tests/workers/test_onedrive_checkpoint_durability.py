"""Resume only committed snapshot rows; required persistence cannot fail silently."""
from __future__ import annotations

import asyncio
import importlib.util
import json

import pytest
import os
import pathlib
import sys
import types

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://u:p@localhost/db")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_checkpoint_under_test", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_checkpoint_under_test"] = _bw
assert _spec and _spec.loader
_spec.loader.exec_module(_bw)  # type: ignore[union-attr]

RESOURCE_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
OTHER_RESOURCE_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
SNAPSHOT_ID = "cccccccc-cccc-cccc-cccc-cccccccccccc"
OTHER_SNAPSHOT_ID = "dddddddd-dddd-dddd-dddd-dddddddddddd"
TENANT_ID = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
JOB_ID = "ffffffff-ffff-ffff-ffff-ffffffffffff"
DRIVE_ID = "drive-A"
OTHER_DRIVE_ID = "drive-B"

FILES = [
    {"id": "f1", "name": "one.docx", "size": 1024, "eTag": "v1"},
    {"id": "f2", "name": "two.xlsx", "size": 1024, "eTag": "v1"},
    {"id": "f3", "name": "three.pdf", "size": 1024, "eTag": "v1"},
]


# ─────────────────────────── fakes ────────────────────────────

class FakeDB:
    """Tracks committed blob paths and their downloaded-source receipts.

    `rows` models which external_ids actually have a snapshot_items row on
    this snapshot — the bulk UPDATE only matches those. None = all of them.
    """

    def __init__(self, job_result, rows=None, receipts=()):
        self.persisted = set(receipts)
        self.rows = None if rows is None else set(rows)
        self.receipts = {item["id"]: (item["id"], "saved/blob", item["size"],
                                     {"raw": item, "onedrive_download_source": item})
                         for item in FILES if item["id"] in receipts}
        self.job = types.SimpleNamespace(id=JOB_ID, result=job_result, tenant_id=TENANT_ID,
                                         status=_bw.JobStatus.RUNNING, lease_token=1)
        self.snapshot = types.SimpleNamespace(id=SNAPSHOT_ID, job_id=JOB_ID, resource_id=RESOURCE_ID,
            status=_bw.SnapshotStatus.IN_PROGRESS, extra_data={}, lease_token=1,
            item_count=len(receipts), new_item_count=0, bytes_total=1024 * len(receipts), bytes_added=0)

    def row_exists(self, ext_id) -> bool:
        return self.rows is None or ext_id in self.rows


class FakeScalars:
    def __init__(self, values):
        self._values = values

    def all(self):
        return list(self._values)


class FakeResult:
    def __init__(self, matched=(), rows=()):
        self.matched = list(matched)
        self.rows = list(rows)

    def all(self):
        return self.rows

    def scalar_one_or_none(self):
        return None

    def scalars(self):
        # Mirrors `UPDATE … RETURNING si.external_id`: only rows that
        # actually matched come back.
        return FakeScalars(self.matched)


class FakeSession:
    def __init__(self, db: FakeDB):
        self.db = db
        self.staged: list = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, stmt, params=None):
        # The blob_path bulk UPDATE binds one :eN param per external_id.
        if getattr(stmt, "is_select", False):
            return FakeResult(rows=self.db.receipts.values())
        matched = []
        for key, val in (params or {}).items():
            if key.startswith("e") and key[1:].isdigit():
                if self.db.row_exists(val):
                    index = key[1:]
                    self.staged.append((val, params[f"bp{index}"], params[f"sz{index}"],
                                        json.loads(params[f"ex{index}"])))
                    matched.append(val)
        return FakeResult(matched)

    async def get(self, model, pk, **kwargs):
        name = getattr(model, "__name__", "")
        if name == "Job":
            return self.db.job
        if name == "Snapshot":
            return self.db.snapshot
        return None

    async def commit(self):
        self.db.persisted.update(row[0] for row in self.staged)
        self.db.receipts.update((row[0], row) for row in self.staged)
        self.staged.clear()

    async def rollback(self):
        self.staged.clear()


class FakeGraph:
    def __init__(self):
        self.download_url_calls: list = []

    async def get_download_urls_batch(self, drive_id, ids):
        self.download_url_calls.extend(ids)
        # (url, real_size, quickXorHash) — empty hash keeps the dedup
        # lookup out of the picture.
        return {i: (f"https://dl/{i}", 1024, "") for i in ids}


def _storage():
    return types.SimpleNamespace(
        get_shard_for_resource=lambda rid, tid: types.SimpleNamespace(name="shard0"),
        get_container_name=lambda tid, kind: "files",
        build_blob_path=lambda tid, rid, sid, item_id: f"{tid}/{rid}/{sid}/{item_id}",
    )


def _run(coro):
    # Self-contained loop per call (repo convention — see
    # tests/workers/test_archive_backup.py); never touches the global
    # current-loop pointer, so test ordering can't affect us.
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _make_worker(monkeypatch, *, job_result, checkpoint_every_files=500,
                 rows=None, receipts=()):
    db = FakeDB(job_result, rows=rows, receipts=receipts)
    graph = FakeGraph()

    monkeypatch.setattr(_bw, "azure_storage_manager", _storage())
    monkeypatch.setattr(_bw, "async_session_factory", lambda: FakeSession(db))
    monkeypatch.setattr(_bw.settings, "ONEDRIVE_BACKUP_V2_ENABLED", True)
    monkeypatch.setattr(_bw.settings, "ONEDRIVE_BACKUP_FILE_CONCURRENCY", 1)
    monkeypatch.setattr(_bw.settings, "ONEDRIVE_BACKUP_FILE_TIMEOUT_SECONDS", 0)
    monkeypatch.setattr(
        _bw.settings, "ONEDRIVE_BACKUP_CHECKPOINT_EVERY_FILES",
        checkpoint_every_files,
    )
    monkeypatch.setattr(
        _bw.settings, "ONEDRIVE_BACKUP_CHECKPOINT_EVERY_BYTES", 1 << 40,
    )
    # No mid-run flush: the only flush is the final one, which is what
    # makes the "checkpoint ran ahead of the buffer" window observable.
    monkeypatch.setenv("ONEDRIVE_FLUSH_INTERVAL_S", "60")
    monkeypatch.setenv("ONEDRIVE_FLUSH_BACKLOG_CAP", "1000")
    monkeypatch.setenv("ONEDRIVE_RETRY_QUEUE_ENABLED", "false")

    w = object.__new__(_bw.BackupWorker)
    w.worker_id = "test-worker"
    w._huge_sem = asyncio.Semaphore(2)
    w._huge_sem_capacity = 2

    async def _still_live(_snapshot_id):
        return True

    async def _stream(**kwargs):
        return {"success": True, "content_sha256": f"sha-{kwargs['blob_path']}"}

    async def _retry(**kwargs):
        raise AssertionError(f"unexpected retry enqueue: {kwargs.get('err_text')}")

    w._snapshot_still_live = _still_live
    w._stream_graph_to_backend = _stream
    w._enqueue_onedrive_retry = _retry
    return w, graph, db


async def _run_backup(w, graph, *, drive_id=DRIVE_ID, files=None):
    resource = types.SimpleNamespace(
        id=RESOURCE_ID, display_name="jrue OneDrive", extra_data={},
    )
    snapshot = types.SimpleNamespace(id=SNAPSHOT_ID, job_id=JOB_ID, lease_token=1)
    tenant = types.SimpleNamespace(id=TENANT_ID)
    return await w._useronedrive_backup_files(
        graph, resource, snapshot, tenant, drive_id,
        list(FILES if files is None else files),
    )


def _cp_payload(*, resource_id=RESOURCE_ID, drive_id=DRIVE_ID,
                snapshot_id=SNAPSHOT_ID, done=("f1",), include_snapshot=True):
    payload = {
        "resource_id": resource_id,
        "drive_id": drive_id,
        "files_done_ext_ids": list(done),
        "files_done_count": len(done),
        "bytes_done": 1024 * len(done),
    }
    if include_snapshot:
        payload["snapshot_id"] = snapshot_id
    return payload


# ─── (a) identity: a checkpoint from elsewhere must never skip files ───

def test_checkpoint_from_another_resource_is_not_honored(monkeypatch):
    w, graph, db = _make_worker(
        monkeypatch,
        job_result={"backup_checkpoint": _cp_payload(
            resource_id=OTHER_RESOURCE_ID,
        )},
    )
    _run(_run_backup(w, graph))
    assert db.persisted == {"f1", "f2", "f3"}, (
        "a sibling resource's checkpoint must not mark f1 done — its "
        "blob_path row on THIS snapshot would stay NULL"
    )


def test_checkpoint_from_another_drive_is_not_honored(monkeypatch):
    w, graph, db = _make_worker(
        monkeypatch,
        job_result={"backup_checkpoint": _cp_payload(drive_id=OTHER_DRIVE_ID)},
    )
    _run(_run_backup(w, graph))
    assert db.persisted == {"f1", "f2", "f3"}


def test_checkpoint_from_a_previous_snapshot_is_not_honored(monkeypatch):
    # Same resource + drive, but the ids were uploaded against the FAILED
    # snapshot of an earlier attempt; this run has a fresh snapshot row.
    w, graph, db = _make_worker(
        monkeypatch,
        job_result={"backup_checkpoint": _cp_payload(
            snapshot_id=OTHER_SNAPSHOT_ID,
        )},
    )
    _run(_run_backup(w, graph))
    assert db.persisted == {"f1", "f2", "f3"}


def test_legacy_checkpoint_without_snapshot_id_is_not_honored(monkeypatch):
    # Pre-fix payloads carry no snapshot_id — unverifiable, so re-do the
    # work rather than risk skipping rows that were never committed.
    w, graph, db = _make_worker(
        monkeypatch,
        job_result={"backup_checkpoint": _cp_payload(include_snapshot=False)},
    )
    _run(_run_backup(w, graph))
    assert db.persisted == {"f1", "f2", "f3"}


def test_matching_durable_rows_resume_without_job_checkpoint(monkeypatch):
    w, graph, db = _make_worker(monkeypatch, job_result={}, receipts={"f1"})
    uploaded, _ = _run(_run_backup(w, graph))
    assert uploaded == 2
    assert db.persisted == {"f1", "f2", "f3"}
    assert "f1" not in graph.download_url_calls


def test_matching_legacy_checkpoint_cannot_vouch_for_metadata_only_rows(monkeypatch):
    w, graph, db = _make_worker(monkeypatch, job_result={"backup_checkpoint": _cp_payload()})
    uploaded, _ = _run(_run_backup(w, graph))
    assert uploaded == 3
    assert db.persisted == {"f1", "f2", "f3"}


def test_downloads_do_not_mutate_shared_job_result(monkeypatch):
    result = {"sibling_progress": 42}
    w, graph, db = _make_worker(monkeypatch, job_result=result, checkpoint_every_files=1)
    uploaded, byte_count = _run(_run_backup(w, graph))
    assert (uploaded, byte_count) == (3, 3072)
    assert db.persisted == {"f1", "f2", "f3"}
    assert db.job.result == {"sibling_progress": 42}


def test_missing_snapshot_row_aborts_instead_of_certifying_uploaded_bytes(monkeypatch):
    w, graph, db = _make_worker(monkeypatch, job_result={}, rows={"f1", "f3"})
    with pytest.raises(RuntimeError, match="required blob rows missing"):
        _run(_run_backup(w, graph))
    assert db.persisted == set()


def test_failed_flush_cannot_return_success(monkeypatch):
    w, graph, db = _make_worker(monkeypatch, job_result={})

    class ExplodingSession(FakeSession):
        async def commit(self):
            raise RuntimeError("simulated PG failure during flush")

    monkeypatch.setattr(_bw, "async_session_factory", lambda: ExplodingSession(db))
    with pytest.raises(RuntimeError, match="simulated PG failure"):
        _run(_run_backup(w, graph))
    assert db.persisted == set()
