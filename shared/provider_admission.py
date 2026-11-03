"""Atomic scoped provider rate, cooldown and network-slot admission.

This module admits one transmission. It never retries a mutation or owns a
backup job; durable callers persist ProviderDeferred in their own lifecycle.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import time
from uuid import uuid4

from shared.provider_read import ProviderReadError


@dataclass(frozen=True)
class ProviderRequestContext:
    tenant_id: str
    app_id: str
    workload: str
    resource: str | None = None
    cloud: str = 'graph.microsoft.com'

    def __post_init__(self):
        for value in (self.tenant_id, self.app_id, self.workload, self.cloud):
            if not isinstance(value, str) or not value or len(value) > 1024:
                raise ValueError('provider request requires a verified identity and workload')
        if self.resource is not None and (not isinstance(self.resource, str) or len(self.resource) > 4096):
            raise ValueError('invalid provider resource scope')


@dataclass(frozen=True)
class AdmissionLimits:
    rate: float = 200
    burst: float = 400
    global_concurrency: int = 16
    tenant_concurrency: int = 8
    resource_concurrency: int = 4
    lease_seconds: float = 30

    def __post_init__(self):
        for value in (self.rate, self.burst, self.lease_seconds):
            if not math.isfinite(value) or value <= 0:
                raise ValueError('provider admission limits must be positive and finite')
        for value in (self.global_concurrency, self.tenant_concurrency, self.resource_concurrency):
            if type(value) is not int or value < 1:
                raise ValueError('provider concurrency must be a positive integer')


class ProviderDeferred(ProviderReadError):
    def __init__(self, context, delay, *, server_time=None, stage='provider_wait'):
        super().__init__(stage, retry_after=max(0.0, delay))
        self.context = context
        self.next_eligible_at = datetime.fromtimestamp(
            (server_time if server_time is not None else time.time()) + self.retry_after,
            timezone.utc)


def _hash(*values):
    return hashlib.sha256(json.dumps(values, separators=(',', ':')).encode()).hexdigest()


class ScopedProviderAdmission:
    # KEYS: rate/cooldown buckets, active-slot sets, cancellation tombstones.
    # A denial consumes no bucket and leaves no future slot reservation.
    _GRANT = """
    local clock=redis.call('TIME')
    local now=tonumber(clock[1])+tonumber(clock[2])/1000000
    local spec=cjson.decode(ARGV[1])
    local slots=cjson.decode(ARGV[2])
    local token=ARGV[3]
    local lease=tonumber(ARGV[4])
    local cancelled=KEYS[#KEYS]
    redis.call('ZREMRANGEBYSCORE',cancelled,'-inf',now)
    if redis.call('ZSCORE',cancelled,token) then return {'-1','0',tostring(now)} end
    local values={}
    local wait=0
    for i,b in ipairs(spec) do
      local last=tonumber(redis.call('HGET',KEYS[i],'last') or now)
      local balance=tonumber(redis.call('HGET',KEYS[i],'tokens') or b[2])
      local until_time=tonumber(redis.call('HGET',KEYS[i],'until') or 0)
      balance=math.min(b[2],balance+math.max(0,now-last)*b[1])
      values[i]=balance
      wait=math.max(wait,until_time-now,math.max(0,b[3]-balance)/b[1])
      redis.call('HSET',KEYS[i],'last',now,'tokens',balance)
      redis.call('EXPIRE',KEYS[i],math.ceil(math.max(600,until_time-now+600)))
    end
    for i,limit in ipairs(slots) do
      local key=KEYS[#spec+i]
      redis.call('ZREMRANGEBYSCORE',key,'-inf',now)
      if redis.call('ZCARD',key)>=limit then
        local first=redis.call('ZRANGE',key,0,0,'WITHSCORES')
        wait=math.max(wait,math.min(0.5,math.max(0.01,tonumber(first[2])-now)))
      end
    end
    if wait>0 then return {'0',tostring(wait),tostring(now)} end
    for i,b in ipairs(spec) do redis.call('HSET',KEYS[i],'tokens',values[i]-b[3]) end
    for i,_ in ipairs(slots) do
      redis.call('ZADD',KEYS[#spec+i],now+lease,token)
      redis.call('EXPIRE',KEYS[#spec+i],math.ceil(lease+60))
    end
    return {'1','0',tostring(now)}
    """
    _RELEASE = """
    local clock=redis.call('TIME')
    local now=tonumber(clock[1])+tonumber(clock[2])/1000000
    redis.call('ZADD',KEYS[#KEYS],now+tonumber(ARGV[2])+60,ARGV[1])
    redis.call('EXPIRE',KEYS[#KEYS],math.ceil(tonumber(ARGV[2])+120))
    for i=1,#KEYS-1 do redis.call('ZREM',KEYS[i],ARGV[1]) end
    return 1
    """
    _RENEW = """
    local clock=redis.call('TIME')
    local now=tonumber(clock[1])+tonumber(clock[2])/1000000
    for _,key in ipairs(KEYS) do
      local expiry=redis.call('ZSCORE',key,ARGV[1])
      if not expiry or tonumber(expiry)<=now then return 0 end
    end
    for _,key in ipairs(KEYS) do
      redis.call('ZADD',key,now+tonumber(ARGV[2]),ARGV[1])
      redis.call('EXPIRE',key,math.ceil(tonumber(ARGV[2])+60))
    end
    return 1
    """
    _COOLDOWN = """
    local clock=redis.call('TIME')
    local now=tonumber(clock[1])+tonumber(clock[2])/1000000
    local until_time=math.max(tonumber(redis.call('HGET',KEYS[1],'until') or 0),now+tonumber(ARGV[1]))
    redis.call('HSET',KEYS[1],'until',until_time)
    redis.call('EXPIRE',KEYS[1],math.ceil(until_time-now+600))
    return tostring(until_time)
    """

    def __init__(self, redis, *, limits=None, namespace='tmvault'):
        self.redis, self.limits = redis, limits or AdmissionLimits()
        self.prefix = 'provider:{' + _hash(namespace)[:16] + '}:'
        self._grant_script = redis.register_script(self._GRANT)
        self._release_script = redis.register_script(self._RELEASE)
        self._renew_script = redis.register_script(self._RENEW)
        self._cooldown_script = redis.register_script(self._COOLDOWN)

    def _keys(self, context):
        tenant = self.prefix + _hash(context.cloud, context.tenant_id)
        workload = tenant + ':' + _hash(context.workload)
        buckets = [self.prefix + 'rate', tenant, workload, workload + ':' + _hash(context.app_id)]
        slots = [self.prefix + 'active', tenant + ':active']
        limits = [self.limits.global_concurrency, self.limits.tenant_concurrency]
        if context.resource:
            resource = workload + ':resource:' + _hash(context.resource)
            buckets.append(resource)
            slots.append(resource + ':active')
            limits.append(self.limits.resource_concurrency)
        return buckets, slots, limits

    async def cooldown(self, context, seconds):
        if not math.isfinite(seconds) or seconds < 0:
            raise ValueError('invalid provider cooldown')
        buckets, _, _ = self._keys(context)
        try:
            return float(await self._cooldown_script(keys=[buckets[2]], args=[seconds]))
        except Exception:
            raise ProviderDeferred(context, max(5, seconds), stage='coordinator_unavailable') from None

    @asynccontextmanager
    async def admit(self, context, *, cost=1, wait_budget=1):
        async with self.admit_many([(context, cost)], wait_budget=wait_budget):
            yield

    @asynccontextmanager
    async def admit_many(self, requirements, *, wait_budget=1):
        if not requirements or len(requirements) > 21:
            raise ValueError('provider batch must contain at most20 subrequests and its outer request')
        context = requirements[0][0]
        if not isinstance(context, ProviderRequestContext):
            raise ValueError('provider admission requires verified request context')
        costs, slot_limits = {}, {}
        for scope, cost in requirements:
            if (not isinstance(scope, ProviderRequestContext) or
                    (scope.tenant_id, scope.app_id, scope.cloud) != (context.tenant_id, context.app_id, context.cloud)):
                raise ValueError('a provider batch cannot cross credential owners')
            if not math.isfinite(cost) or cost <= 0:
                raise ValueError('invalid provider admission cost')
            scoped_buckets, scoped_slots, scoped_limits = self._keys(scope)
            for key in scoped_buckets:
                costs[key] = costs.get(key, 0) + cost
            for key, limit in zip(scoped_slots, scoped_limits):
                slot_limits[key] = min(slot_limits.get(key, limit), limit)
        if max(costs.values()) > self.limits.burst:
            raise ValueError('provider cost exceeds available burst; split request')
        if not math.isfinite(wait_budget) or wait_budget < 0:
            raise ValueError('invalid provider wait budget')
        buckets, slots, limits = list(costs), list(slot_limits), list(slot_limits.values())
        cancelled = self.prefix + 'cancelled'
        token = uuid4().hex
        started = time.monotonic()
        renewer, failure = None, []
        owner = asyncio.current_task()

        async def renew():
            try:
                while True:
                    await asyncio.sleep(self.limits.lease_seconds / 3)
                    if not await self._renew_script(keys=slots, args=[token, self.limits.lease_seconds]):
                        raise RuntimeError('provider network lease lost')
            except asyncio.CancelledError:
                raise
            except Exception:
                failure.append(True)
                owner.cancel()

        try:
            while True:
                try:
                    granted, delay, clock = await self._grant_script(keys=[*buckets, *slots, cancelled], args=[
                        json.dumps([[self.limits.rate, self.limits.burst, costs[key]] for key in buckets]),
                        json.dumps(limits), token, self.limits.lease_seconds])
                except Exception:
                    raise ProviderDeferred(context, 5, stage='coordinator_unavailable') from None
                if int(granted) == -1:
                    raise ProviderDeferred(context, 1, stage='admission_cancelled')
                if int(granted) == 1:
                    break
                delay = float(delay)
                if delay <= 0 or not math.isfinite(delay):
                    raise ProviderDeferred(context, 5, stage='invalid_coordinator_reply')
                if time.monotonic() - started + delay > wait_budget:
                    raise ProviderDeferred(context, delay, server_time=float(clock))
                await asyncio.sleep(delay)
            renewer = asyncio.create_task(renew())
            try:
                yield
            except asyncio.CancelledError:
                if failure:
                    # A transmission may already have started. Do not label it
                    # an unexecuted deferral that could justify replaying a write.
                    raise ProviderReadError('provider_network_lease_lost', retry_after=5) from None
                raise
        finally:
            if renewer is not None:
                renewer.cancel()
                with suppress(asyncio.CancelledError):
                    await renewer
            # Fence a late grant as well as removing any existing token.
            release = asyncio.create_task(self._release_script(keys=[*slots, cancelled], args=[token, self.limits.lease_seconds]))
            try:
                await asyncio.shield(release)
            except asyncio.CancelledError:
                with suppress(Exception):
                    await release
                raise
            except Exception:
                # Redis expiry is the fallback; new grants fail closed during
                # the outage. Never override the original operation outcome.
                pass
