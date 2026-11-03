"""Regression: the job-scoped OneDrive BackupCheckpoint must never let a
resumed run skip a file whose blob_path was not committed.

Bug (found 2026-07-31, wave-1 audit of workers/backup-worker/main.py):

  (a) identity — the checkpoint is read blindly from
      Job.result.backup_checkpoint and BackupCheckpoint.from_dict never
      checks that the payload belongs to THIS resource / drive / snapshot.
      One Job id covers every resource of a bulk fan-out (all children get
      the parent job id), so sibling resources share the single
      `backup_checkpoint` slot — and a run against a *new* snapshot (the
      prior one FAILED) would honour ids that only exist as rows on the
      OLD snapshot.

  (b) durability — `checkpoint.record_file_done()` fired the moment a blob
      landed, while the matching `snapshot_items.blob_path` UPDATE was
      still buffered in the in-memory `updates` list. A checkpoint commit
      between the two (cadence is every 500 files / 1 GiB) plus an
      OOM-kill before the batch flush left the resumed run skipping those
      files: their rows exist with NO blob_path, the run can still
      finalize COMPLETED and promote the deltaLink. Unrestorable baseline.

Fix: validate checkpoint identity before honouring it, and advance the
checkpoint only AFTER the blob_path UPDATEs commit (redoing a few
downloads is always cheaper than an unrestorable snapshot).
"""
from __future__ import annotations

import asyncio
import importlib.util
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
    {"id": "f1", "name": "one.docx", "size": 1024},
    {"id": "f2", "name": "two.xlsx", "size": 1024},
    {"id": "f3", "name": "three.pdf", "size": 1024},
]


# ─────────────────────────── fakes ────────────────────────────

class FakeDB:
    """Tracks which external_ids have a *committed* blob_path row, plus a
    log of (checkpoint contents, committed ids) at each checkpoint commit.

    `rows` models which external_ids actually have a snapshot_items row on
    this snapshot — the bulk UPDATE only matches those. None = all of them.
    """

    def __init__(self, job_result, rows=None):
        self.persisted: set = set()
        self.rows = None if rows is None else set(rows)
        self.job = types.SimpleNamespace(id=JOB_ID, result=job_result)
        self.checkpoint_commits: list = []   # [(files_done_ext_ids, persisted)]

    def row_exists(self, ext_id) -> bool:
        return self.rows is None or ext_id in self.rows


class FakeScalars:
    def __init__(self, values):
        self._values = values

    def all(self):
        return list(self._values)


class FakeResult:
    def __init__(self, matched=()):
        self.matched = list(matched)

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
        matched = []
        for key, val in (params or {}).items():
            if key.startswith("e") and key[1:].isdigit():
                if self.db.row_exists(val):
                    self.staged.append(val)
                    matched.append(val)
        return FakeResult(matched)

    async def get(self, model, pk):
        name = getattr(model, "__name__", "")
        if name == "Job":
            return self.db.job
        if name == "Snapshot":
            return types.SimpleNamespace(
                item_count=0, new_item_count=0,
                bytes_total=0, bytes_added=0,
            )
        return None

    async def commit(self):
        self.db.persisted.update(self.staged)
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
                 rows=None):
    db = FakeDB(job_result, rows=rows)
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

    async def _commit_cp(job_id, checkpoint):
        db.checkpoint_commits.append(
            (set(checkpoint.files_done_ext_ids), set(db.persisted)),
        )
        checkpoint.mark_committed()

    w._snapshot_still_live = _still_live
    w._stream_graph_to_backend = _stream
    w._enqueue_onedrive_retry = _retry
    w._commit_backup_checkpoint = _commit_cp
    return w, graph, db


async def _run_backup(w, graph, *, drive_id=DRIVE_ID, files=None):
    resource = types.SimpleNamespace(
        id=RESOURCE_ID, display_name="jrue OneDrive", extra_data={},
    )
    snapshot = types.SimpleNamespace(id=SNAPSHOT_ID, job_id=JOB_ID)
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


def test_matching_checkpoint_still_resumes(monkeypatch):
    # Guard against over-fixing: a checkpoint for this exact
    # (resource, drive, snapshot) must still skip the finished file.
    w, graph, db = _make_worker(
        monkeypatch,
        job_result={"backup_checkpoint": _cp_payload(done=("f1",))},
    )
    uploaded, _ = _run(_run_backup(w, graph))
    assert uploaded == 2
    assert db.persisted == {"f2", "f3"}
    assert "f1" not in graph.download_url_calls


# ─── (b) durability: never checkpoint ahead of the committed rows ───

def test_checkpoint_never_runs_ahead_of_committed_blob_paths(monkeypatch):
    # Cadence of 1 file makes every commit boundary observable; the
    # blob_path UPDATEs stay buffered until the final flush.
    w, graph, db = _make_worker(
        monkeypatch, job_result={}, checkpoint_every_files=1,
    )
    _run(_run_backup(w, graph))

    assert db.checkpoint_commits, "expected at least one checkpoint commit"
    for done_ids, persisted in db.checkpoint_commits:
        assert done_ids <= persisted, (
            f"checkpoint recorded {sorted(done_ids - persisted)} as done "
            f"before their blob_path UPDATE was committed — a crash here "
            f"makes the resumed run skip blob-less rows"
        )


def test_final_checkpoint_covers_every_persisted_file(monkeypatch):
    w, graph, db = _make_worker(
        monkeypatch, job_result={}, checkpoint_every_files=1,
    )
    uploaded, byte_count = _run(_run_backup(w, graph))
    assert (uploaded, byte_count) == (3, 3072)
    assert db.persisted == {"f1", "f2", "f3"}
    assert db.checkpoint_commits[-1][0] == {"f1", "f2", "f3"}


def test_file_whose_snapshot_row_is_missing_is_not_recorded_done(monkeypatch):
    # "Committed" is not the same as "matched a row". The bulk UPDATE is
    # keyed on (snapshot_id, external_id); if that row doesn't exist the
    # statement affects nothing, commits happily, and the blob becomes
    # invisible to restore. Recording it done anyway makes the resumed run
    # skip it forever — the same silent-loss shape this file exists to stop.
    w, graph, db = _make_worker(
        monkeypatch, job_result={}, checkpoint_every_files=1,
        rows={"f1", "f3"},          # f2 has no snapshot_items row
    )
    _run(_run_backup(w, graph))

    assert db.persisted == {"f1", "f3"}
    for done_ids, _persisted in db.checkpoint_commits:
        assert "f2" not in done_ids, (
            "f2's blob_path UPDATE matched no row, so nothing on this "
            "snapshot points at its blob — a resume must re-do it"
        )
    assert db.checkpoint_commits[-1][0] == {"f1", "f3"}


def test_failed_flush_leaves_files_out_of_the_checkpoint(monkeypatch):
    # The whole point of the ordering: if the row UPDATE never commits,
    # the file must NOT be recorded done, so the next run re-uploads it.
    w, graph, db = _make_worker(
        monkeypatch, job_result={}, checkpoint_every_files=1,
    )

    class ExplodingSession(FakeSession):
        async def commit(self):
            raise RuntimeError("simulated PG failure during flush")

    monkeypatch.setattr(
        _bw, "async_session_factory", lambda: ExplodingSession(db),
    )
    _run(_run_backup(w, graph))
    assert db.persisted == set()
    for done_ids, _persisted in db.checkpoint_commits:
        assert done_ids == set(), (
            "files whose blob_path UPDATE failed must not be recorded as "
            f"done, got {sorted(done_ids)}"
        )
