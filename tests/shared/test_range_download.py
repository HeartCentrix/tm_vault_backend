import asyncio
import httpx
import pytest
from shared.range_download import ordered_ranges, RangeProofUnavailable
from shared.provider_read import ProviderReadError


async def test_slow_reader_cannot_accumulate_all_remaining_segments():
    sent = []
    data = bytes(range(100))
    async def handler(request):
        lo, hi = map(int, request.headers['Range'].replace('bytes=', '').split('-'))
        sent.append(lo)
        if lo:
            assert request.headers['If-Match'] == '"v1"'
        return httpx.Response(206, headers={'ETag': '"v1"', 'Content-Range': f'bytes {lo}-{hi}/100'}, content=data[lo:hi+1])
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        async with ordered_ranges(client, 'https://tenant.sharepoint.com/file', 100, segment_size=10, concurrency=3, chunk_size=2) as stream:
            first = await anext(stream)
            await asyncio.sleep(.02)
            assert len(sent) == 3
            collected = first + b''.join([chunk async for chunk in stream])
    assert collected == data and len(sent) == 10


@pytest.mark.parametrize('problem', ['changed', 'wrong_range', 'too_long'])
async def test_corrupt_range_never_joins_snapshot_content(problem):
    async def handler(request):
        lo, hi = map(int, request.headers['Range'].replace('bytes=', '').split('-'))
        etag = '"v2"' if lo and problem == 'changed' else '"v1"'
        actual_lo = 0 if lo and problem == 'wrong_range' else lo
        body = b'x' * (hi-lo+1 + (1 if lo and problem == 'too_long' else 0))
        return httpx.Response(206, headers={'ETag': etag, 'Content-Range': f'bytes {actual_lo}-{hi}/20'}, content=body)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(ProviderReadError):
            async with ordered_ranges(client, 'https://tenant.sharepoint.com/file', 20, segment_size=10, concurrency=2) as stream:
                _ = [block async for block in stream]


async def test_no_version_proof_returns_before_any_bytes_are_published():
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(206, content=b'x'))) as client:
        with pytest.raises(RangeProofUnavailable):
            async with ordered_ranges(client, 'https://tenant.sharepoint.com/file', 20, segment_size=10, concurrency=2):
                pytest.fail('must use a fresh full-body download instead')
