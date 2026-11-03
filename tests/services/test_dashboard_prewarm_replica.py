"""The background host delegates bounded work and never chooses a heavy fallback."""
import asyncio
import contextlib
import importlib.util
from pathlib import Path
import os

os.environ.setdefault('ALLOW_DEV_JWT_SECRETS', 'true')
import pytest


@pytest.fixture
def ds():
    spec = importlib.util.spec_from_file_location('dashboard_refresh_host', Path(__file__).resolve().parents[2] / 'services/dashboard-service/main.py')
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


async def test_host_passes_replica_to_bounded_ledger_without_fallback(ds, monkeypatch):
    source, primary = object(), object()
    calls = []
    async def refresh(factory, **kwargs):
        calls.append((factory, kwargs))
        return {'status': 'replica_lag'}
    monkeypatch.setattr(ds, '_refresh_size_once', refresh)
    monkeypatch.setattr(ds, '_refresh_session_factory', primary, raising=False)
    monkeypatch.setattr(ds, '_ro_session_factory', source)
    assert await ds._protected_prewarm_pass() == {'status': 'replica_lag'}
    assert len(calls) == 1
    assert calls[0][0] is primary
    assert calls[0][1] == {'source_factory': source, 'page_size': 1000, 'statement_timeout_ms': 5000}


def test_refresh_uses_its_own_small_pool(ds):
    assert ds._refresh_engine is not ds.engine
    assert ds._refresh_engine.pool.size() == 1


async def test_primary_only_mode_is_explicit_no_replica_configured(ds, monkeypatch):
    calls = []
    async def refresh(factory, **kwargs):
        calls.append(kwargs['source_factory'])
        return {'status': 'idle'}
    monkeypatch.setattr(ds, '_refresh_size_once', refresh)
    monkeypatch.setattr(ds, '_ro_session_factory', None)
    assert await ds._protected_prewarm_pass() == {'status': 'idle'}
    assert calls == [None]


async def test_loop_starts_once_and_replaces_completed_task(ds, monkeypatch):
    monkeypatch.setattr(ds, '_PREWARM_ENABLED', True)
    first = ds._start_protected_prewarm()
    assert ds._start_protected_prewarm() is first
    first.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await first
    second = ds._start_protected_prewarm()
    try:
        assert second is not first
    finally:
        second.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await second


def test_refresh_kill_switch(ds, monkeypatch):
    monkeypatch.setattr(ds, '_PREWARM_ENABLED', False)
    assert ds._start_protected_prewarm() is None


async def test_replica_failure_is_not_replayed_on_primary(ds, monkeypatch):
    calls = []
    async def refresh(factory, **kwargs):
        calls.append(kwargs)
        raise OSError('replica unavailable')
    monkeypatch.setattr(ds, '_refresh_size_once', refresh)
    monkeypatch.setattr(ds, '_ro_session_factory', object())
    with pytest.raises(OSError, match='replica unavailable'):
        await ds._protected_prewarm_pass()
    assert len(calls) == 1
