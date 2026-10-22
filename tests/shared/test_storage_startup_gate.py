"""Boot-time storage-router gate (2026-10-13 incident).

Nine backup-worker pods booted while Postgres was failing over, exhausted
startup_router()'s 12 bounded attempts, logged "gave up" and then consumed
backup work for an hour with NO active backend: blob uploads raised
"No storage shards configured" (visible FAILED jobs) and the OneDrive
handler completed snapshots whose ONEDRIVE_FILE rows had blob_path=NULL
(invisible data loss). These tests pin the fix:

  * exhaustion no longer ends the story — a background task keeps
    retrying until the router loads;
  * callers that cannot work without a backend (workers) pass
    required=True / STORAGE_ROUTER_REQUIRED=true and block until loaded;
  * the facade refuses the legacy raw-shard fallback with a message that
    names the real cause when the router never loaded.
"""
from __future__ import annotations

import asyncio

import pytest

from shared.storage import startup as su


class _FakeRouter:
    def __init__(self, fail_times: int):
        self.fail_times = fail_times
        self.calls = 0
        self.closes = 0
        self._active = None

    async def load(self, db_dsn: str) -> None:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise ConnectionRefusedError("[Errno 111] Connect call failed")
        self._active = "cf0dcac3-871c-4ef6-ae35-73d85e6d1552"

    async def close(self) -> None:
        self.closes += 1

    def list_backends(self):
        return [object()] if self._active else []

    def active_backend_id(self):
        return self._active

    def transition_state(self):
        return "stable"


@pytest.fixture
def fast_env(monkeypatch):
    monkeypatch.setenv("STORAGE_ROUTER_MAX_RETRIES", "2")
    monkeypatch.setenv("STORAGE_ROUTER_BACKOFF_BASE_S", "0.01")
    monkeypatch.setenv("STORAGE_ROUTER_BACKOFF_CAP_S", "0.01")
    monkeypatch.setenv("STORAGE_ROUTER_BG_RETRY_S", "0.02")
    monkeypatch.setenv("STORAGE_ROUTER_BG_RETRY_CAP_S", "0.02")
    monkeypatch.delenv("STORAGE_ROUTER_REQUIRED", raising=False)
    # fresh module state per test (event is loop-bound)
    monkeypatch.setattr(su, "_loaded_event", None)
    monkeypatch.setattr(su, "_bg_task", None)
    yield
    task = getattr(su, "_bg_task", None)
    if task is not None and not task.done():
        task.cancel()


async def test_bounded_retry_then_success(fast_env, monkeypatch):
    r = _FakeRouter(fail_times=1)
    monkeypatch.setattr(su, "router", r)
    assert await su.startup_router() is True
    assert r.calls == 2
    assert su.router_loaded() is True
    assert await su.wait_for_router(timeout=0.1) is True


async def test_failed_attempt_closes_half_open_router(fast_env, monkeypatch):
    # load() can fail AFTER asyncpg.connect succeeded; without close() every
    # attempt would leak one server connection.
    r = _FakeRouter(fail_times=1)
    monkeypatch.setattr(su, "router", r)
    await su.startup_router()
    assert r.closes >= 1


async def test_exhausted_not_required_returns_false_but_keeps_retrying(fast_env, monkeypatch):
    r = _FakeRouter(fail_times=4)   # 2 bounded + 2 background failures
    monkeypatch.setattr(su, "router", r)
    assert await su.startup_router() is False
    assert su.router_loaded() is False
    # background retry must eventually bring the router up
    assert await su.wait_for_router(timeout=3.0) is True
    assert r.calls == 5
    assert su.router_loaded() is True


async def test_required_blocks_until_router_loads(fast_env, monkeypatch):
    r = _FakeRouter(fail_times=4)
    monkeypatch.setattr(su, "router", r)
    t = asyncio.create_task(su.startup_router(required=True))
    await asyncio.sleep(0.03)
    assert not t.done(), "required=True must gate startup while the router is down"
    assert await asyncio.wait_for(t, timeout=3.0) is True
    assert su.router_loaded() is True


async def test_required_env_override(fast_env, monkeypatch):
    monkeypatch.setenv("STORAGE_ROUTER_REQUIRED", "true")
    r = _FakeRouter(fail_times=3)
    monkeypatch.setattr(su, "router", r)
    assert await asyncio.wait_for(su.startup_router(), timeout=3.0) is True
    assert r.calls == 4


async def test_wait_for_router_times_out_while_down(fast_env, monkeypatch):
    r = _FakeRouter(fail_times=10_000)
    monkeypatch.setattr(su, "router", r)
    assert await su.startup_router() is False
    assert await su.wait_for_router(timeout=0.05) is False


async def test_shutdown_cancels_background_retry(fast_env, monkeypatch):
    r = _FakeRouter(fail_times=10_000)
    monkeypatch.setattr(su, "router", r)
    await su.startup_router()
    task = su._bg_task
    assert task is not None and not task.done()
    await su.shutdown_router()
    await asyncio.sleep(0)
    assert task.cancelled() or task.done()


def test_facade_refuses_raw_shard_fallback_when_router_never_loaded(monkeypatch):
    from shared import azure_storage as az
    from shared.storage import router as rmod

    monkeypatch.setattr(rmod.router, "_active_backend_id", None)
    mgr = az.AzureStorageManager.__new__(az.AzureStorageManager)
    mgr.shards = []
    with pytest.raises(az.StorageRouterNotReadyError) as ei:
        mgr.get_shard_for_resource("resource", "tenant")
    msg = str(ei.value).lower()
    assert "router" in msg and "not loaded" in msg
    with pytest.raises(az.StorageRouterNotReadyError):
        mgr.get_shard_by_index(0)
    with pytest.raises(az.StorageRouterNotReadyError):
        mgr.get_default_shard()


def test_facade_keeps_plain_error_when_router_loaded_but_no_shards(monkeypatch):
    # Router loaded (active backend set) but get_active_store raising is a
    # different failure; with strict=false the facade falls through to the
    # raw shards and the historical message must be preserved.
    from shared import azure_storage as az
    from shared.storage import router as rmod

    monkeypatch.setattr(rmod.router, "_active_backend_id", "some-backend")
    monkeypatch.setattr(rmod.router, "get_active_store", lambda: (_ for _ in ()).throw(RuntimeError("x")))
    mgr = az.AzureStorageManager.__new__(az.AzureStorageManager)
    mgr.shards = []
    with pytest.raises(RuntimeError) as ei:
        mgr.get_shard_for_resource("resource", "tenant")
    assert not isinstance(ei.value, az.StorageRouterNotReadyError)
    assert "No storage shards configured" in str(ei.value)
