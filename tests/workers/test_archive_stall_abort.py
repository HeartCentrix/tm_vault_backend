"""Regression: the archive run must abort itself when it stops making progress.

Bug (2026-08-12, prod): snapshot 0e423f79 ("Archive — Jason Rue") sat
IN_PROGRESS for 15.6 h after its last item write. Nothing noticed, and
nothing could:

  * the worker's ``LEASE_EXTENDER`` bumps ``lease_expires_at`` every 120 s
    while the PROCESS lives;
  * ``_liveness_guard`` re-asserts the snapshot row every 120 s and only
    aborts when the row is GONE or terminal;
  * ``reconciler`` reclaims solely on ``lease_expires_at < NOW()`` or a dead
    heartbeat — both false.

Every liveness signal in the system proves the process is *alive*. Not one
of them proves the work is *advancing*, so a task that awaits something which
never completes stays RUNNING forever and blocks every later run of the same
resource (the scheduler holds it in ``overlap_held_resource_ids``).

This pins a progress-based abort: the guard watches a monotonic progress
stamp written by the folder walk and by each export chunk, and cancels the
run when it goes stale. The existing CancelledError handler converts that
into ``EnumerationFailed`` so the snapshot fails honestly and the job retries.

Threshold note: a single exportItems chunk may legitimately burn
``ARCHIVE_IMPORT_TIMEOUT_S`` (600 s) five times over, plus a capped 120 s
Retry-After each round — worst honest gap ≈ 3,600 s. The default
``ARCHIVE_STALL_ABORT_S`` (5,400 s) sits above that with margin.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import types

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://u:p@localhost/db")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
_MOD_PATH = _REPO_ROOT / "workers" / "backup-worker" / "archive_backup.py"
_spec = importlib.util.spec_from_file_location("archive_backup_stall_test", _MOD_PATH)
ab = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["archive_backup_stall_test"] = ab
assert _spec and _spec.loader
_spec.loader.exec_module(ab)  # type: ignore[union-attr]


# ────────────────────────────── fixtures ──────────────────────────────

def _settings(**over):
    base = dict(
        ARCHIVE_GRAPH_VERSION="beta",
        ARCHIVE_IMPORT_TIMEOUT_S=120,
        ARCHIVE_FOLDER_CONCURRENCY=8,
        ARCHIVE_EXPORT_CONCURRENCY=2,
        ARCHIVE_LIVENESS_PROBE_S=0.05,
        ARCHIVE_LIVENESS_TTL_S=600,
        ARCHIVE_STALL_ABORT_S=0.4,
        ARCHIVE_MIN_COVERAGE_PCT=0,
    )
    base.update(over)
    return types.SimpleNamespace(**base)


def _resource():
    return types.SimpleNamespace(
        id="11111111-1111-1111-1111-111111111111",
        external_id="AAMkArchiveMbx",
        display_name="jrue archive",
        extra_data={"archiveMailboxId": "AAMkArchiveMbx"},
    )


def _snapshot():
    return types.SimpleNamespace(
        id="22222222-2222-2222-2222-222222222222",
        delta_tokens_json=None,
    )


def _tenant():
    return types.SimpleNamespace(id="33333333-3333-3333-3333-333333333333")


class _StubResult:
    rowcount = 1


class _StubSession:
    """Minimal async-session stand-in for the liveness probe UPDATE."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, *_a, **_kw):
        return _StubResult()

    async def commit(self):
        return None


class _HangingGraph:
    """Walk succeeds; the export POST never returns — the prod wedge shape."""

    def __init__(self, items):
        self.items = items
        self.post_calls = 0

    async def _get(self, url, params=None, on_activity=None):
        if url.endswith("/folders"):
            return {"value": [{"id": "F", "displayName": "F", "childFolderCount": 0}]}
        if url.endswith("/items"):
            return {"value": list(self.items)}
        if url.endswith("/childFolders"):
            return {"value": []}
        raise AssertionError(f"unexpected GET {url}")

    async def _post_json(self, url, body, timeout=None):
        self.post_calls += 1
        await asyncio.Event().wait()   # never completes — exactly the wedge
        raise AssertionError("unreachable")


async def _persist(_pairs):
    return (0, 0, 0)


async def _load_prev(_rid, _sid):
    return (None, {})


async def _carry(_a, _b, _c):
    return 0


def _run(coro, timeout=20):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(asyncio.wait_for(coro, timeout))
    finally:
        loop.close()


# ─────────────────────────────── tests ────────────────────────────────

def test_stalled_export_aborts_instead_of_hanging_forever(monkeypatch):
    """The wedge: export never returns ⇒ run must self-abort, not hang."""
    import shared.database as dbmod
    monkeypatch.setattr(dbmod, "async_session_factory", lambda: _StubSession(),
                        raising=False)

    graph = _HangingGraph([{"id": "i1", "changeKey": "ck1"}])

    with pytest.raises(ab.EnumerationFailed) as exc:
        _run(ab.run_archive_backup(
            graph, _resource(), _snapshot(), _tenant(),
            worker_id="wk", settings=_settings(),
            load_prev=_load_prev, persist_batch=_persist, carry_forward=_carry,
        ))

    msg = str(exc.value).lower()
    assert "progress" in msg or "stall" in msg, (
        f"stall abort must say so; got: {exc.value}"
    )
    assert graph.post_calls >= 1


def test_stall_abort_reports_the_stall_not_a_missing_row(monkeypatch):
    """A stall must not be misreported as the liveness 'row is gone' abort."""
    import shared.database as dbmod
    monkeypatch.setattr(dbmod, "async_session_factory", lambda: _StubSession(),
                        raising=False)

    graph = _HangingGraph([{"id": "i1", "changeKey": "ck1"}])

    with pytest.raises(ab.EnumerationFailed) as exc:
        _run(ab.run_archive_backup(
            graph, _resource(), _snapshot(), _tenant(),
            worker_id="wk", settings=_settings(),
            load_prev=_load_prev, persist_batch=_persist, carry_forward=_carry,
        ))

    assert "deleted or flipped terminal" not in str(exc.value)


def test_healthy_run_is_not_aborted_by_the_stall_guard(monkeypatch):
    """Progress keeps resetting the stamp — a working run must finish."""
    import shared.database as dbmod
    monkeypatch.setattr(dbmod, "async_session_factory", lambda: _StubSession(),
                        raising=False)

    class _SlowButWorking(_HangingGraph):
        async def _post_json(self, url, body, timeout=None):
            self.post_calls += 1
            # Each chunk takes longer than the stall threshold on its own,
            # but progress is stamped per chunk so the guard must not fire.
            await asyncio.sleep(0.25)
            return {"value": [{"itemId": i, "changeKey": "ck", "data": "d"}
                              for i in body["itemIds"]]}

    items = [{"id": f"i{n}", "changeKey": f"ck{n}"} for n in range(60)]
    graph = _SlowButWorking(items)

    stored = {"n": 0}

    async def _persist_ok(pairs):
        stored["n"] += len(pairs)
        return (len(pairs), 0, 0)

    result = _run(ab.run_archive_backup(
        graph, _resource(), _snapshot(), _tenant(),
        worker_id="wk", settings=_settings(ARCHIVE_STALL_ABORT_S=0.4),
        load_prev=_load_prev, persist_batch=_persist_ok, carry_forward=_carry,
    ))

    assert result["item_count"] == 60
    assert stored["n"] == 60


def test_stall_guard_disabled_when_threshold_is_zero(monkeypatch):
    """ARCHIVE_STALL_ABORT_S=0 is the documented kill-switch."""
    import shared.database as dbmod
    monkeypatch.setattr(dbmod, "async_session_factory", lambda: _StubSession(),
                        raising=False)

    graph = _HangingGraph([{"id": "i1", "changeKey": "ck1"}])

    with pytest.raises(asyncio.TimeoutError):
        _run(ab.run_archive_backup(
            graph, _resource(), _snapshot(), _tenant(),
            worker_id="wk", settings=_settings(ARCHIVE_STALL_ABORT_S=0),
            load_prev=_load_prev, persist_batch=_persist, carry_forward=_carry,
        ), timeout=2)
