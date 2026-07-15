"""Backfill: encrypt existing PLAINTEXT backup blobs in place.

Encryption only seals NEW writes; blobs written before it was enabled stay
plaintext-at-rest (they read fine via the legacy passthrough). This turns them
into TMVE envelopes so an "all data encrypted at rest" claim becomes true.

Idempotent: a blob that is already TMVE-framed is skipped, so the job is safe to
re-run and to resume. Each blob is re-uploaded through the normal single-shot
``upload`` path, which does write-then-verify (read back + decrypt + compare)
before it is considered done — we never replace good plaintext with a blob we
can't get back.

Drives the storage facade through a tiny duck-typed interface (head_is_encrypted
/ download_blob / upload_blob / list_blobs) so it is unit-testable without S3.
"""
from __future__ import annotations

import asyncio
from typing import Dict

from . import runtime


async def reencrypt_blob(shard, container: str, path: str, tenant_id: str) -> str:
    """Encrypt one blob in place. Returns 'skipped' (already encrypted),
    'missing' (no such blob), or 'encrypted'."""
    if await shard.head_is_encrypted(container, path):
        return "skipped"
    plaintext = await shard.download_blob(container, path)
    if plaintext is None:
        return "missing"
    res = await shard.upload_blob(container, path, plaintext, tenant_id=tenant_id)
    if not res.get("success"):
        raise RuntimeError(f"reencrypt upload failed for {container}/{path}: {res.get('error')}")
    return "encrypted"


async def reencrypt_container(shard, container: str, tenant_id: str, *, concurrency: int = 8) -> Dict[str, int]:
    """Encrypt every plaintext blob in ``container``. Requires the write hook to
    be enabled (else uploads wouldn't seal and the job would make no progress).
    Returns {'encrypted','skipped','missing','failed'} counts."""
    if not runtime.is_write_enabled():
        raise RuntimeError("reencrypt_container requires encryption writes enabled (is_write_enabled() is False)")

    stats = {"encrypted": 0, "skipped": 0, "missing": 0, "failed": 0}
    sem = asyncio.Semaphore(concurrency)

    async def _one(path: str):
        async with sem:
            try:
                stats[await reencrypt_blob(shard, container, path, tenant_id)] += 1
            except Exception:
                stats["failed"] += 1

    tasks = [asyncio.create_task(_one(p)) async for p in shard.list_blobs(container)]
    if tasks:
        await asyncio.gather(*tasks)
    return stats
