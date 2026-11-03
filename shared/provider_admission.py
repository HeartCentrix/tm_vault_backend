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
import logging
import math
import time
from uuid import uuid4

from shared.provider_read import ProviderReadError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProviderRequestContext:
    tenant_id: str
    app_id: str
    workload: str
    resource: str | None = None
    cloud: str = 'graph.microsoft.com'
    paced_chat: bool = False

    def __post_init__(self):
        for value in (self.tenant_id, self.app_id, self.workload, self.cloud):
            if not isinstance(value, str) or not value or len(value) > 1024:
                raise ValueError('provider request requires a verified identity and workload')
        if self.resource is not None and (not isinstance(self.resource, str) or len(self.resource) > 4096):
            raise ValueError('invalid provider resource scope')
        if type(self.paced_chat) is not bool:
            raise ValueError('chat pacing must be an explicit boolean')
        if self.paced_chat and (self.workload != 'teams' or not self.resource or
                self.resource.partition(':')[0] not in ('chat', 'user_chats') or
                not self.resource.partition(':')[2]):
            raise ValueError('chat pacing requires a Teams chat or user chats resource')


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
    def __init__(self, context, delay, *, server_time=None, stage='provider_wait', reason=None, scope=None):
        super().__init__(stage, retry_after=max(0.0, delay))
        self.context = context
        self.reason = reason
        self.scope = scope
        self.next_eligible_at = datetime.fromtimestamp(
            (server_time if server_time is not None else time.time()) + self.retry_after,
            timezone.utc)


def _hash(*values):
    return hashlib.sha256(json.dumps(values, separators=(',', ':')).encode()).hexdigest()


class ScopedProviderAdmission:
    # KEYS: rate/cooldown buckets, active slots, cancellation and fair queue state.
    # Waiting reserves no active slot or rate token. All keys share one Redis hash tag.
    _GRANT = """
    local clock=redis.call('TIME')
    local now=tonumber(clock[1])+tonumber(clock[2])/1000000
    local spec=cjson.decode(ARGV[1])
    local slots=cjson.decode(ARGV[2])
    local token=ARGV[3]
    local lease=tonumber(ARGV[4])
    local cancelled=KEYS[#KEYS-4]
    local pending=KEYS[#KEYS-3]
    local expiry=KEYS[#KEYS-2]
    local clock_key=KEYS[#KEYS-1]
    local signals=redis.call('XREVRANGE',KEYS[#KEYS],'+','-','COUNT',1)
    local signal=#signals>0 and signals[1][1] or '0-0'
    redis.call('ZREMRANGEBYSCORE',cancelled,'-inf',now)
    if redis.call('ZSCORE',cancelled,token) then return {'-1','0',tostring(now),'cancelled','0',signal} end
    for _,old in ipairs(redis.call('ZRANGEBYSCORE',expiry,'-inf',now)) do
      redis.call('HDEL',pending,old)
    end
    redis.call('ZREMRANGEBYSCORE',expiry,'-inf',now)
    local entry=redis.call('HGET',pending,token)
    if entry then
      entry=cjson.decode(entry)
    else
      if redis.call('HLEN',pending)>=512 then
        return {'0','0.5',tostring(now),'concurrency','-1',signal}
      end
      entry={group=ARGV[5],weight=tonumber(ARGV[6]),born=now,
             seq=redis.call('HINCRBY',clock_key,'seq',1),buckets={},slots={}}
      for i,b in ipairs(spec) do entry.buckets[i]={KEYS[i],b[1],b[2],b[3]} end
      for i,limit in ipairs(slots) do entry.slots[i]={KEYS[#spec+i],limit} end
      redis.call('HSET',pending,token,cjson.encode(entry))
    end
    redis.call('ZADD',expiry,now+30,token)
    redis.call('EXPIRE',pending,60)
    redis.call('EXPIRE',expiry,60)
    redis.call('EXPIRE',clock_key,600)
    local values={}
    local wait=0
    local reason='none'
    local denied=0
    for i,b in ipairs(spec) do
      local last=tonumber(redis.call('HGET',KEYS[i],'last') or now)
      local balance=tonumber(redis.call('HGET',KEYS[i],'tokens') or b[2])
      local until_time=tonumber(redis.call('HGET',KEYS[i],'until') or 0)
      balance=math.min(b[2],balance+math.max(0,now-last)*b[1])
      values[i]=balance
      local cooldown=until_time-now
      local rate_wait=math.max(0,b[3]-balance)/b[1]
      if cooldown>wait then wait=cooldown; reason='cooldown'; denied=i end
      if rate_wait>wait then wait=rate_wait; reason='rate'; denied=i end
      redis.call('HSET',KEYS[i],'last',now,'tokens',balance)
      redis.call('EXPIRE',KEYS[i],math.ceil(math.max(600,until_time-now+600)))
    end
    for i,limit in ipairs(slots) do
      local key=KEYS[#spec+i]
      redis.call('ZREMRANGEBYSCORE',key,'-inf',now)
      if redis.call('ZCARD',key)>=limit then
        local first=redis.call('ZRANGE',key,0,0,'WITHSCORES')
        local slot_wait=math.min(0.5,math.max(0.01,tonumber(first[2])-now))
        if slot_wait>wait then wait=slot_wait; reason='concurrency'; denied=-i end
      end
    end
    if wait>0 then return {'0',tostring(wait),tostring(now),reason,tostring(denied),signal} end
    local function eligible(e)
      for _,b in ipairs(e.buckets) do
        local last=tonumber(redis.call('HGET',b[1],'last') or now)
        local balance=tonumber(redis.call('HGET',b[1],'tokens') or b[3])
        local until_time=tonumber(redis.call('HGET',b[1],'until') or 0)
        if until_time>now or math.min(b[3],balance+math.max(0,now-last)*b[2])<b[4] then return false end
      end
      for _,s in ipairs(e.slots) do
        redis.call('ZREMRANGEBYSCORE',s[1],'-inf',now)
        if redis.call('ZCARD',s[1])>=s[2] then return false end
      end
      return true
    end
    local floor=tonumber(redis.call('HGET',clock_key,'floor') or 0)
    local winner,chosen,score,aged=nil,nil,nil,false
    local entries=redis.call('HGETALL',pending)
    for i=1,#entries,2 do
      local candidate=cjson.decode(entries[i+1])
      if eligible(candidate) then
        local finish=math.max(floor,tonumber(redis.call('GET',candidate.group) or floor))
        local old=now-candidate.born>=5
        if not winner or (old and not aged) or (old==aged and
            ((old and candidate.seq<chosen.seq) or (not old and
             (finish<score or (finish==score and candidate.seq<chosen.seq))))) then
          winner,chosen,score,aged=entries[i],candidate,finish,old
        end
      end
    end
    if winner~=token then return {'0','0.05',tostring(now),'concurrency','-1',signal} end
    redis.call('HSET',clock_key,'floor',score)
    redis.call('SET',entry.group,score+tonumber(ARGV[7])/entry.weight,'EX',600)
    redis.call('HDEL',pending,token)
    redis.call('ZREM',expiry,token)
    for i,b in ipairs(spec) do redis.call('HSET',KEYS[i],'tokens',values[i]-b[3]) end
    for i,_ in ipairs(slots) do
      redis.call('ZADD',KEYS[#spec+i],now+lease,token)
      redis.call('EXPIRE',KEYS[#spec+i],math.ceil(lease+60))
    end
    return {'1','0',tostring(now),'none','0',signal}
    """
    _RELEASE = """
    local clock=redis.call('TIME')
    local now=tonumber(clock[1])+tonumber(clock[2])/1000000
    redis.call('ZADD',KEYS[#KEYS-3],now+tonumber(ARGV[2])+60,ARGV[1])
    redis.call('EXPIRE',KEYS[#KEYS-3],math.ceil(tonumber(ARGV[2])+120))
    local released=0
    for i=1,#KEYS-4 do released=released+redis.call('ZREM',KEYS[i],ARGV[1]) end
    redis.call('HDEL',KEYS[#KEYS-2],ARGV[1])
    redis.call('ZREM',KEYS[#KEYS-1],ARGV[1])
    if released>0 then
      redis.call('XADD',KEYS[#KEYS],'MAXLEN','~',64,'*','released','1')
      redis.call('EXPIRE',KEYS[#KEYS],60)
    end
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
            if context.paced_chat:
                # Shared across app IDs; generic callers must never refill this bucket.
                buckets.append(resource + ':paced_chat')
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
        costs, slot_limits, scopes, paced_buckets = {}, {}, {}, set()
        for scope, cost in requirements:
            if (not isinstance(scope, ProviderRequestContext) or
                    (scope.tenant_id, scope.app_id, scope.cloud) != (context.tenant_id, context.app_id, context.cloud)):
                raise ValueError('a provider batch cannot cross credential owners')
            if not math.isfinite(cost) or cost <= 0:
                raise ValueError('invalid provider admission cost')
            if scope.paced_chat and cost != 1:
                raise ValueError('paced chat admission requires unit cost')
            scoped_buckets, scoped_slots, scoped_limits = self._keys(scope)
            if scope.paced_chat:
                paced_buckets.add(scoped_buckets[-1])
            scopes.update(zip(scoped_buckets, ('global', 'tenant', 'workload', 'app', 'resource', 'resource')))
            scopes.update(zip(scoped_slots, ('global', 'tenant', 'resource')))
            for key in scoped_buckets:
                costs[key] = costs.get(key, 0) + cost
            for key, limit in zip(scoped_slots, scoped_limits):
                slot_limits[key] = min(slot_limits.get(key, limit), limit)
        bucket_specs = [[1, 1, cost] if key in paced_buckets else
            [self.limits.rate, self.limits.burst, cost] for key, cost in costs.items()]
        if any(cost > burst for _, burst, cost in bucket_specs):
            raise ValueError('provider cost exceeds available burst; split request')
        if not math.isfinite(wait_budget) or wait_budget < 0:
            raise ValueError('invalid provider wait budget')
        buckets, slots, limits = list(costs), list(slot_limits), list(slot_limits.values())
        cancelled = self.prefix + 'cancelled'
        pending, expiry, fair_clock, signals = (self.prefix + 'fair:' + key for key in ('pending', 'expiry', 'clock', 'signals'))
        from shared.provider_runtime import current_execution
        execution = current_execution()
        weight = getattr(execution.claim, 'weight', 1) if execution is not None else 1
        if type(weight) is not int or not 1 <= weight <= 8:
            weight = 1
        workload = ','.join(sorted({scope.workload for scope, _ in requirements}))
        group = self.prefix + 'fair:flow:' + _hash(context.cloud, context.tenant_id, workload, weight)
        release_keys = [*slots, cancelled, pending, expiry, signals]
        token = uuid4().hex
        started = time.monotonic()
        renewer, failure = None, []
        polls = 0
        owner = asyncio.current_task()

        async def wait_for_release(delay):
            # Bounded stream wakeups avoid polling a busy slot until its lease expires.
            try:
                await self.redis.xread({signals: signal}, count=1, block=max(1, min(500, int(delay * 1000))))
            except asyncio.CancelledError:
                raise
            except Exception:
                raise ProviderDeferred(context, 5, stage='coordinator_unavailable') from None

        async def withdraw():
            await self._release_script(keys=release_keys, args=[token, self.limits.lease_seconds])

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
                from shared.provider_runtime import (before_provider_admission,
                    provider_admission_granted, wait_provider_concurrency)
                await before_provider_admission(withdraw=withdraw)
                try:
                    polls += 1
                    granted, delay, clock, reason, denied, signal = await self._grant_script(keys=[*buckets, *slots, cancelled, pending, expiry, fair_clock, signals], args=[
                        json.dumps(bucket_specs),
                        json.dumps(limits), token, self.limits.lease_seconds, group, weight, max(costs.values())])
                except Exception:
                    raise ProviderDeferred(context, 5, stage='coordinator_unavailable') from None
                if int(granted) == -1:
                    raise ProviderDeferred(context, 1, stage='admission_cancelled')
                if int(granted) == 1:
                    if not provider_admission_granted():
                        # Do not pin a racing new grant while older transfers need its slot.
                        try:
                            await self._release_script(keys=release_keys, args=[token, self.limits.lease_seconds])
                        except Exception:
                            raise ProviderDeferred(context, 5, stage='coordinator_unavailable') from None
                        await before_provider_admission(withdraw=withdraw)
                    logger.debug('provider_network_granted workload=%s weight=%d wait_s=%.3f polls=%d',
                        workload, weight, time.monotonic()-started, polls)
                    if polls > 1:
                        logger.info('provider_network_wait workload=%s weight=%d wait_s=%.3f polls=%d',
                            workload, weight, time.monotonic()-started, polls)
                    break
                delay = float(delay)
                if delay <= 0 or not math.isfinite(delay):
                    raise ProviderDeferred(context, 5, stage='invalid_coordinator_reply')
                if time.monotonic() - started + delay > wait_budget:
                    index = int(denied)
                    key = buckets[index - 1] if index > 0 else slots[-index - 1]
                    error = ProviderDeferred(context, delay, server_time=float(clock),
                        reason=reason.decode() if isinstance(reason, bytes) else reason, scope=scopes[key])
                    chat_pacing_wait = context.paced_chat and error.reason == 'rate' and key in paced_buckets
                    if (error.reason == 'concurrency' or chat_pacing_wait) and await wait_provider_concurrency(error,
                            wait=asyncio.sleep if chat_pacing_wait else wait_for_release, withdraw=withdraw):
                        continue
                    raise error
                if (reason.decode() if isinstance(reason, bytes) else reason) == 'concurrency':
                    await wait_for_release(delay)
                else:
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
            release = asyncio.create_task(self._release_script(keys=release_keys, args=[token, self.limits.lease_seconds]))
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
