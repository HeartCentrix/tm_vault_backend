"""Cross-client admission against an owned disposable Redis unix socket."""
import asyncio
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

import pytest
import pytest_asyncio
from redis.asyncio import Redis

from shared.graph_rate_limiter import _LocalTokenBucket, _RedisTokenBucket

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def redis_client():
    binary = shutil.which('redis-server')
    if binary is None:
        pytest.skip('requires local redis-server binary')
    with tempfile.TemporaryDirectory(prefix='tmv-rate-', dir='/tmp') as root:
        socket = str(Path(root) / 'redis.sock')
        with open(Path(root) / 'server.log', 'w') as log:
            process = subprocess.Popen([binary, '--port', '0', '--unixsocket', socket,
                '--save', '', '--appendonly', 'no'], stdout=log, stderr=subprocess.STDOUT)
            client = Redis(unix_socket_path=socket, decode_responses=True, socket_timeout=1)
            try:
                for _ in range(100):
                    try:
                        await client.ping()
                        break
                    except Exception:
                        if process.poll() is not None:
                            pytest.fail('owned Redis process exited during startup')
                        await asyncio.sleep(.02)
                else:
                    pytest.fail('owned Redis startup timed out')
                yield client
            finally:
                await client.aclose()
                process.terminate()
                process.wait(timeout=5)


async def test_concurrent_clients_obtain_separate_real_grants(redis_client):
    buckets = [_RedisTokenBucket(redis_client, 'synthetic-bucket', 10, 1, _LocalTokenBucket(10, 1)) for _ in range(4)]
    await buckets[0].acquire()  # exhaust the initial burst
    started = time.monotonic()
    times = []
    async def take(bucket):
        await bucket.acquire()
        times.append(time.monotonic() - started)
    await asyncio.gather(*(take(buckets[i % 4]) for i in range(8)))
    assert len(times) == 8
    # A timer-as-grant bug releases all eight at~0.1s. Real grants need~0.8s.
    assert max(times) >= .70
    assert all(t >= .085 * (index + 1) for index, t in enumerate(sorted(times)))


async def test_cancelled_wait_does_not_spend_future_capacity(redis_client):
    bucket = _RedisTokenBucket(redis_client, 'cancel-bucket', 10, 1, _LocalTokenBucket(10, 1))
    await bucket.acquire()
    task = asyncio.create_task(bucket.acquire())
    await asyncio.sleep(.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(.10)
    started = time.monotonic()
    await bucket.acquire()
    assert time.monotonic() - started < .08
