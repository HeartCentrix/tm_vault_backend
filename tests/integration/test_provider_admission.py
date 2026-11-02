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
