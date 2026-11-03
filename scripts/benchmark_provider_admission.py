"""Synthetic mixed-workload admission benchmark; uses only owned disposable Redis.

Run from the repository root: python scripts/benchmark_provider_admission.py
Results compare local scheduling at several caps, not Microsoft/Railway capacity.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import defaultdict
import json
import os
from pathlib import Path
import resource
import shutil
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault('ALLOW_DEV_JWT_SECRETS', 'true')
from redis.asyncio import Redis
from shared.provider_admission import AdmissionLimits, ProviderRequestContext, ScopedProviderAdmission
from shared import provider_runtime as runtime


async def measure(redis, cap, requests):
    limiter = ScopedProviderAdmission(redis, namespace=uuid4().hex,
        limits=AdmissionLimits(rate=10000, burst=10000, global_concurrency=cap,
                               tenant_concurrency=cap, resource_concurrency=cap))
    waits, served, durations = defaultdict(list), defaultdict(int), {'teams': .01, 'exchange': .03, 'sharepoint': .12}
    active = peak = 0
    start = time.monotonic()

    async def worker(workload, weight):
        nonlocal active, peak
        key = f'{workload}/weight-{weight}'
        token = runtime._EXECUTION.set(runtime.ProviderExecution(None, SimpleNamespace(weight=weight)))
        try:
            async with runtime.provider_live_wait_scope():
                for _ in range(requests):
                    began = time.monotonic()
                    context = ProviderRequestContext('synthetic-tenant', 'synthetic-app', workload)
                    async with limiter.admit(context, wait_budget=0):
                        waits[key].append(time.monotonic()-began)
                        active += 1
                        peak = max(peak, active)
                        try:
                            await asyncio.sleep(durations[workload])
                            served[key] += 1
                        finally:
                            active -= 1
        finally:
            runtime._EXECUTION.reset(token)

    await asyncio.gather(*(worker(w, p) for w in durations for p in [8, 4, 2, 1]))
    elapsed = time.monotonic()-start
    assert peak <= cap and active == 0
    assert len(served) == 12 and all(n == requests for n in served.values())
    assert await redis.hlen(limiter.prefix+'fair:pending') == 0
    assert await redis.zcard(limiter.prefix+'active') == 0
    return {'cap': cap, 'seconds': round(elapsed, 3), 'requests': sum(served.values()),
            'requests_per_second': round(sum(served.values())/elapsed, 2), 'peak_active': peak,
            'workloads': {k: {'completed': served[k], 'max_wait_s': round(max(v), 3),
                              'mean_wait_s': round(sum(v)/len(v), 3)} for k, v in waits.items()}}


async def run(caps, requests):
    binary = shutil.which('redis-server')
    if binary is None:
        raise RuntimeError('Install redis-server to run this local benchmark')
    with tempfile.TemporaryDirectory(prefix='tmv-admission-', dir='/tmp') as directory:
        socket = str(Path(directory)/'redis.sock')
        with open(Path(directory)/'redis.log', 'w') as log:
            process = subprocess.Popen([binary, '--port', '0', '--unixsocket', socket,
                                        '--save', '', '--appendonly', 'no'], stdout=log, stderr=log)
            redis = Redis(unix_socket_path=socket, decode_responses=True, socket_timeout=2)
            try:
                for _ in range(100):
                    try:
                        await redis.ping()
                        break
                    except Exception:
                        if process.poll() is not None:
                            raise RuntimeError('Owned Redis exited during startup') from None
                        await asyncio.sleep(.02)
                else:
                    raise RuntimeError('Owned Redis startup timed out')
                return {'kind': 'synthetic-local-only', 'results': [await measure(redis, cap, requests) for cap in caps],
                        'process_maxrss_platform_units': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}
            finally:
                await redis.aclose()
                process.terminate()
                process.wait(timeout=5)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--caps', nargs='+', type=int, default=[8, 12, 16])
    parser.add_argument('--requests-per-lane', type=int, default=12)
    args = parser.parse_args()
    if any(not 1 <= cap <= 64 for cap in args.caps) or not 1 <= args.requests_per_lane <= 1000:
        parser.error('caps must be1–64 and requests-per-lane1–1000')
    print(json.dumps(asyncio.run(run(args.caps, args.requests_per_lane)), indent=2))
