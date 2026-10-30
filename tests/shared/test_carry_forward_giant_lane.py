"""2026-08-27 starvation fix: giant carry-forward copies get a reserved
advisory-lock lane so micro copies can never starve them.

Incident: the 10:07Z Gold archive carry-forward (coarse 5-10s poll) lost
every freed slot to thousands of fast-polling (0.25-0.75s) per-chat/per-file
MICRO copies for 4h straight and died on CARRY_FORWARD_WAIT_MAX_S on both
8/26 (job edd2a576) and 8/27 (job da794fc7); chatTopic normalize / P8
copies on sibling workers hit the same ceiling in the same window.
Try-lock polling has no FIFO, so fairness must come from partitioning:

  * MICRO sites keep buckets [0, CARRY_FORWARD_BUCKETS),
  * GIANT sites (whole-resource INSERT…SELECT / UPDATE statements) use
    reserved buckets [CARRY_FORWARD_BUCKETS,
    CARRY_FORWARD_BUCKETS + GIANT_CARRY_FORWARD_BUCKETS) that micro
    callers can never probe,
  * GIANT_CARRY_FORWARD_BUCKETS defaults to 3 = today's accepted bound on
    concurrent giant statements (the 2026-08-18 LWLock:BufferMapping
    convoy ceiling) — the lane must not RAISE giant concurrency,
  * the probe stays pg_try_advisory_xact_lock in a fresh short
    transaction (the 8/24 pooler-pinning fix must survive this change).
"""
from __future__ import annotations

import hashlib
import os
import pathlib

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

import shared.database as db  # noqa: E402

REPO = pathlib.Path(__file__).resolve().parents[2]


class _FakeResult:
    def __init__(self, value):
        self._value = value

    def scalar(self):
        return self._value


class _FakeSession:
    def __init__(self, grant: bool, tag: int):
        self._grant = grant
        self.statements = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, stmt, params=None):
        self.statements.append((str(stmt), params))
        return _FakeResult(self._grant)

    async def rollback(self):
        pass


def _wire(monkeypatch, grants):
    sessions: list = []

    def factory():
        s = _FakeSession(grants[len(sessions)], len(sessions))
        sessions.append(s)
        return s

    async def fake_sleep(secs):
        pass

    monkeypatch.setattr(db, "async_session_factory", factory)
    monkeypatch.setattr(db.asyncio, "sleep", fake_sleep)
    return sessions


# ── bucket partitioning ──────────────────────────────────────────────


def test_lanes_are_disjoint_bucket_ranges():
    micro = {db.carry_forward_bucket(f"k{i}") for i in range(200)}
    giant = {db.carry_forward_bucket(f"k{i}", lane="giant") for i in range(200)}
    assert micro <= set(range(db.CARRY_FORWARD_BUCKETS))
    assert giant <= set(
        range(
            db.GIANT_CARRY_FORWARD_LANE_BASE,
            db.GIANT_CARRY_FORWARD_LANE_BASE + db.GIANT_CARRY_FORWARD_BUCKETS,
        )
    )
    assert not (micro & giant), (
        "a shared bucket id lets micro pollers starve giants again"
    )


def test_giant_bucket_is_stable_md5():
    key = "f0cfcf02-114a-4d1e-8d2c-4a03a5009d07"
    expected = db.GIANT_CARRY_FORWARD_LANE_BASE + (
        int(hashlib.md5(key.encode()).hexdigest(), 16)
        % max(1, db.GIANT_CARRY_FORWARD_BUCKETS)
    )
    assert db.carry_forward_bucket(key, lane="giant") == expected


def test_giant_range_independent_of_micro_bucket_count(monkeypatch):
    # Env skew: pods can run different CARRY_FORWARD_BUCKETS values. The
    # giant range must not move with it, or one pod's micro range overlaps
    # another pod's giant range and silently re-creates the starvation.
    before = {db.carry_forward_bucket(f"k{i}", lane="giant") for i in range(50)}
    monkeypatch.setattr(db, "CARRY_FORWARD_BUCKETS", 40)
    after = {db.carry_forward_bucket(f"k{i}", lane="giant") for i in range(50)}
    assert before == after
    micro_widened = {db.carry_forward_bucket(f"k{i}") for i in range(200)}
    assert not (micro_widened & after)


def test_giant_default_bound_matches_convoy_ceiling():
    # Default of 3 == the 8/18 convoy bound on concurrent giant copies.
    # Raising it is an env decision, never a silent code change.
    assert db.GIANT_CARRY_FORWARD_BUCKETS == 3


def test_unknown_lane_rejected():
    with pytest.raises(ValueError):
        db.carry_forward_bucket("k", lane="mega")


# ── slot session routing ─────────────────────────────────────────────


async def test_giant_session_probes_reserved_bucket(monkeypatch):
    sessions = _wire(monkeypatch, [True])

    async with db.carry_forward_slot_session("res-1", lane="giant"):
        pass

    stmt, params = sessions[0].statements[0]
    assert "pg_try_advisory_xact_lock" in stmt, (
        "the 8/24 pooler fix (try-lock, never blocking) must survive the lane"
    )
    assert params["cls"] == db.CARRY_FORWARD_LOCK_CLASS
    assert params["b"] == db.carry_forward_bucket("res-1", lane="giant")
    assert params["b"] >= db.GIANT_CARRY_FORWARD_LANE_BASE


async def test_default_lane_is_micro_unchanged(monkeypatch):
    sessions = _wire(monkeypatch, [True])

    async with db.carry_forward_slot_session("res-1"):
        pass

    _, params = sessions[0].statements[0]
    assert params["b"] == db.carry_forward_bucket("res-1")
    assert params["b"] < db.CARRY_FORWARD_BUCKETS


async def test_giant_ceiling_error_names_lane(monkeypatch):
    # The daily 14:24Z failure is diagnosed from this message — it must say
    # which lane starved.
    _wire(monkeypatch, [False] * 50)
    monkeypatch.setattr(db, "CARRY_FORWARD_WAIT_MAX_S", 0.0001)

    with pytest.raises(RuntimeError, match=r"lane=giant"):
        async with db.carry_forward_slot_session("k", lane="giant", what="archive carry-forward"):
            pass


# ── call-site routing (source pins) ──────────────────────────────────

_GIANT_MARKERS = {
    "workers/backup-worker/archive_backup.py": ['what="archive carry-forward"'],
    "workers/backup-worker/main.py": [
        'what="chat carry-forward"',
        'what="chatTopic normalize"',
        'what="chatTopic legacy migrate"',
    ],
}
_MICRO_MARKERS = [
    'what="file-version carry-forward"',
    'what="P8 attachment copy"',
]


def test_giant_sites_use_giant_lane():
    for rel, markers in _GIANT_MARKERS.items():
        src = (REPO / rel).read_text()
        for marker in markers:
            i = src.find(marker)
            assert i != -1, f"{marker} missing from {rel}"
            window = src[max(0, i - 400):i + 400]
            assert 'lane="giant"' in window, (
                f"giant site {marker} in {rel} must take the reserved lane — "
                "micro pollers starved it for 4h on 8/26 and 8/27"
            )


def test_micro_sites_stay_micro():
    src = (REPO / "workers/backup-worker/main.py").read_text()
    for marker in _MICRO_MARKERS:
        i = src.find(marker)
        assert i != -1
        window = src[max(0, i - 400):i + 400]
        assert 'lane="giant"' not in window, (
            f"micro site {marker} must never occupy a giant bucket — that "
            "reintroduces the starvation with roles reversed"
        )
