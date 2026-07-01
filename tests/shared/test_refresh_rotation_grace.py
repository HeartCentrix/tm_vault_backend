"""Refresh-token rotation grace window (fixes the 401-logout-after-a-while bug).

Root cause: /auth/refresh rotates the refresh token and burns the old jti with a
strict single-use Redis SET NX. The frontend single-flight is PER-TAB, so two
tabs (each scheduling a proactive refresh) — or a retry after a lost response —
present the same refresh token; one wins the SET NX, the loser gets 401 "already
used" and is logged out.

Fix: when a refresh WINS the rotation it caches the freshly-minted (access,
refresh) pair keyed by the old jti for a short grace window. A concurrent/retried
refresh that LOSES the race retrieves that same pair (idempotent rotation) instead
of a 401. After the grace window a reuse returns nothing -> genuine replay -> 401,
so replay protection is preserved beyond the window.

These tests inject a fake async Redis via the module global (no external Redis).
"""
from __future__ import annotations

import os
import asyncio

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

import shared.security as sec
from shared.security import (
    remember_rotated_tokens,
    get_rotated_tokens,
    resolve_concurrent_refresh,
)


class _FakeRedis:
    def __init__(self):
        self.store = {}

    async def set(self, k, v, nx=False, ex=None):
        if nx and k in self.store:
            return None
        self.store[k] = v
        return True

    async def get(self, k):
        return self.store.get(k)


def _inject():
    fake = _FakeRedis()
    sec._revocation_redis = fake
    return fake


def test_remember_then_get_roundtrip():
    _inject()

    async def go():
        await remember_rotated_tokens("jti1", "acc-tok", "ref-tok", 60)
        return await get_rotated_tokens("jti1")

    assert asyncio.run(go()) == ("acc-tok", "ref-tok")


def test_get_unknown_jti_returns_none():
    _inject()
    assert asyncio.run(get_rotated_tokens("does-not-exist")) is None


def test_concurrent_loser_recovers_winners_pair():
    # winner cached its pair; the loser of the race must get the SAME pair back
    # (idempotent) rather than being forced to 401/logout.
    _inject()

    async def go():
        await remember_rotated_tokens("jtiX", "WIN-acc", "WIN-ref", 60)
        return await resolve_concurrent_refresh("jtiX", poll_attempts=3, poll_interval_s=0.001)

    assert asyncio.run(go()) == ("WIN-acc", "WIN-ref")


def test_no_cache_is_genuine_replay_returns_none():
    # nothing cached for this jti (grace window elapsed / never issued) -> the
    # caller must reject with 401 (real replay protection preserved).
    _inject()
    pair = asyncio.run(
        resolve_concurrent_refresh("stale-jti", poll_attempts=2, poll_interval_s=0.001)
    )
    assert pair is None
