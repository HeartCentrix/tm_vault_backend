"""Scoped provider limits and cooldowns share authoritative Redis state."""
import asyncio
from dataclasses import replace
import importlib
import time

import pytest
from test_graph_admission_redis import redis_client

pytestmark = pytest.mark.integration


def scope(module, tenant='tenant', app='app', workload='sharepoint'):
    return module.ProviderRequestContext(tenant, app, workload, resource='resource')


async def test_coordinator_slots_release_and_cooldowns_are_tenant_scoped(redis_client):
    module = importlib.import_module('shared.provider_admission')
    limits = module.AdmissionLimits(tenant_concurrency=1, global_concurrency=2, lease_seconds=1)
    admission = module.ScopedProviderAdmission(redis_client, limits=limits, namespace='test')
    first, other = scope(module), scope(module, tenant='other')
    async with admission.admit(first, wait_budget=0):
        with pytest.raises(module.ProviderDeferred):
            async with admission.admit(first, wait_budget=0):
                pytest.fail('exceeded tenant concurrency')
        async with admission.admit(other, wait_budget=0):
            pass
    await admission.cooldown(first, 2)
    with pytest.raises(module.ProviderDeferred) as error:
        async with admission.admit(first, wait_budget=0):
            pytest.fail('ignored Retry-After')
    assert error.value.retry_after > 1.5
    async with admission.admit(other, wait_budget=0):
        pass


async def test_all_bucket_costs_grant_atomically_and_cancel_has_no_leak(redis_client):
    module = importlib.import_module('shared.provider_admission')
    admission = module.ScopedProviderAdmission(redis_client,
        limits=module.AdmissionLimits(rate=10, burst=1, tenant_concurrency=1, lease_seconds=1), namespace='cancel')
    selected = scope(module)
    entered, release = asyncio.Event(), asyncio.Event()
    async def hold():
        async with admission.admit(selected, wait_budget=0):
            entered.set()
            await release.wait()
    task = asyncio.create_task(hold())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(.12)
    async with admission.admit(selected, wait_budget=0):
        pass


async def test_long_content_renews_slots_and_releases_on_exit(redis_client):
    module = importlib.import_module('shared.provider_admission')
    admission = module.ScopedProviderAdmission(redis_client,
        limits=module.AdmissionLimits(tenant_concurrency=1, lease_seconds=.3), namespace='renew')
    selected = scope(module)
    async with admission.admit(selected, wait_budget=0):
        await asyncio.sleep(.5)
        with pytest.raises(module.ProviderDeferred):
            async with admission.admit(selected, wait_budget=0):
                pytest.fail('lease expired during active content')
    async with admission.admit(selected, wait_budget=0):
        pass


async def test_one_app_cannot_escape_workload_cooldown_by_rotation(redis_client):
    module = importlib.import_module('shared.provider_admission')
    admission = module.ScopedProviderAdmission(redis_client, namespace='rotate')
    first = scope(module)
    await admission.cooldown(first, 600)
    with pytest.raises(module.ProviderDeferred) as error:
        async with admission.admit(replace(first, app_id='different'), wait_budget=0):
            pytest.fail('rotated around tenant workload cooldown')
    assert error.value.retry_after > 590


async def test_denial_reports_safe_scope_and_reason(redis_client):
    module = importlib.import_module('shared.provider_admission')
    admission = module.ScopedProviderAdmission(redis_client,
        limits=module.AdmissionLimits(tenant_concurrency=1, global_concurrency=2), namespace='reason')
    selected = scope(module)
    async with admission.admit(selected, wait_budget=0):
        with pytest.raises(module.ProviderDeferred) as denied:
            async with admission.admit(selected, wait_budget=0):
                pytest.fail('tenant slot limit was bypassed')
        assert (denied.value.reason, denied.value.scope) == ('concurrency', 'tenant')
    await admission.cooldown(selected, 60)
    with pytest.raises(module.ProviderDeferred) as cooling:
        async with admission.admit(selected, wait_budget=0):
            pytest.fail('cooldown was bypassed')
    assert (cooling.value.reason, cooling.value.scope) == ('cooldown', 'workload')


async def test_short_slot_wait_completes_without_restarting_provider_turn(redis_client, monkeypatch):
    from shared.graph_rate_limiter import GraphRateLimiter
    module = importlib.import_module('shared.provider_admission')
    monkeypatch.setenv('PROVIDER_SCOPED_ADMISSION_ENABLED', 'true')
    monkeypatch.setenv('PROVIDER_ADMISSION_WAIT_SECONDS', '2')
    admission = module.ScopedProviderAdmission(redis_client,
        limits=module.AdmissionLimits(tenant_concurrency=1), namespace='short-wait')
    limiter = GraphRateLimiter()
    limiter._scoped = admission
    selected = scope(module)
    entered = asyncio.Event()

    async def holding_request():
        async with admission.admit(selected, wait_budget=0):
            entered.set()
            await asyncio.sleep(1.2)

    holder = asyncio.create_task(holding_request())
    await entered.wait()
    try:
        async with asyncio.timeout(3):
            async with limiter.admit(selected):
                assert holder.done()
    finally:
        await holder


async def test_local_wait_does_not_shorten_server_cooldown(redis_client, monkeypatch):
    from shared.graph_rate_limiter import GraphRateLimiter
    module = importlib.import_module('shared.provider_admission')
    monkeypatch.setenv('PROVIDER_SCOPED_ADMISSION_ENABLED', 'true')
    monkeypatch.setenv('PROVIDER_ADMISSION_WAIT_SECONDS', '2')
    admission = module.ScopedProviderAdmission(redis_client, namespace='long-wait')
    limiter = GraphRateLimiter()
    limiter._scoped = admission
    selected = scope(module)
    await admission.cooldown(selected, 60)
    started = time.monotonic()
    with pytest.raises(module.ProviderDeferred) as deferred:
        async with limiter.admit(selected):
            pytest.fail('ignored server cooldown')
    assert deferred.value.retry_after > 59
    assert time.monotonic() - started < 1


async def test_batch_costs_are_atomic_and_include_all_workloads(redis_client):
    module = importlib.import_module('shared.provider_admission')
    admission = module.ScopedProviderAdmission(redis_client,
        limits=module.AdmissionLimits(rate=1, burst=10), namespace='batch')
    mail = scope(module, workload='exchange')
    files = scope(module, workload='sharepoint')
    await admission.cooldown(files, 600)
    batch = admission.admit_many([(mail, 3), (files, 2)], wait_budget=0)
    with pytest.raises(module.ProviderDeferred):
        async with batch:
            pass
    keys, _, _ = admission._keys(mail)
    assert float(await redis_client.hget(keys[-1], 'tokens')) == 10
    async with admission.admit(mail, cost=10, wait_budget=0):
        pass
