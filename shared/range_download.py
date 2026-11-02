"""Bounded, ordered range reads with strong source-version proof."""
from __future__ import annotations
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import re
import shutil
import tempfile

from shared.provider_read import ProviderReadError


class RangeProofUnavailable(ProviderReadError):
    def __init__(self):
        super().__init__('range_version_proof_unavailable')


def strong_etag(response):
    value = response.headers.get('ETag')
    return value if value and not value.startswith('W/') else None


def validate_range(response, start, total, *, end=None, etag=None):
    match = re.fullmatch(r'bytes (\d+)-(\d+)/(\d+)', response.headers.get('Content-Range', ''))
    if not match:
        raise ProviderReadError('invalid_content_range', status=response.status_code)
    lo, hi, size = map(int, match.groups())
    if lo != start or hi < lo or size != total or hi >= total or (end is not None and hi != end):
        raise ProviderReadError('content_range_mismatch', status=response.status_code)
    if etag is not None and strong_etag(response) != etag:
        raise ProviderReadError('download_version_changed')


@asynccontextmanager
async def ordered_ranges(client, url, total, *, segment_size, concurrency, chunk_size=1048576):
    """At most K segment tasks/files, including completed out-of-order segments.

    The first segment establishes an HTTP ETag before publication. Without a
    strong version proof the caller may start one ordinary full-body download.
    """
    if min(total, segment_size, concurrency, chunk_size) <= 0:
        raise ValueError('range sizes and concurrency must be positive')
    count = (total + segment_size - 1) // segment_size
    tasks, version = {}, None
    with tempfile.TemporaryDirectory(prefix='tmvault-ranges-') as directory:
        async def fetch(index):
            nonlocal version
            lo, hi = index * segment_size, min(total, (index + 1) * segment_size) - 1
            headers = {'Range': f'bytes={lo}-{hi}', 'Accept-Encoding': 'identity'}
            if version:
                headers['If-Match'] = version
            path = Path(directory) / str(index)
            async with client.stream('GET', url, headers=headers) as response:
                if index == 0 and (response.status_code == 200 or not strong_etag(response)):
                    if response.status_code in (200, 206):
                        raise RangeProofUnavailable()
                if response.status_code != 206:
                    raise ProviderReadError('range_request_failed', status=response.status_code)
                validate_range(response, lo, total, end=hi, etag=version)
                version = version or strong_etag(response)
                received, space_check = 0, 0
                with path.open('xb') as output:
                    async for block in response.aiter_bytes(chunk_size=65536):
                        if received + len(block) > hi - lo + 1:
                            raise ProviderReadError('range_length_exceeded')
                        if received >= space_check:
                            if shutil.disk_usage(directory).free < len(block) + 256 * 1024 * 1024:
                                raise ProviderReadError('range_spool_capacity', retry_after=30)
                            space_check = received + 4 * 1024 * 1024
                        output.write(block)
                        received += len(block)
                if received != hi - lo + 1:
                    raise ProviderReadError('range_length_mismatch')
            return path

        async def iterate(first):
            tasks[0] = asyncio.get_running_loop().create_future()
            tasks[0].set_result(first)
            following = 1
            while following < min(count, concurrency):
                tasks[following] = asyncio.create_task(fetch(following))
                following += 1
            for index in range(count):
                path = await tasks.pop(index)
                with path.open('rb') as source:
                    while block := source.read(chunk_size):
                        yield block
                path.unlink()
                if following < count:
                    tasks[following] = asyncio.create_task(fetch(following))
                    following += 1

        iterator = None
        try:
            first = await fetch(0)
            iterator = iterate(first)
            yield iterator
        finally:
            if iterator is not None:
                await iterator.aclose()
            for task in tasks.values():
                task.cancel()
            await asyncio.gather(*tasks.values(), return_exceptions=True)
