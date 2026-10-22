"""Regression: OneDrive partition-shard hydration must chunk its id list.

Bug (2026-07-13, jlucas 348GB/199k-file force-full): the shard consumer
hydrated its file slice with one
``SnapshotItem.external_id.in_(file_ids)`` statement. asyncpg caps bind
parameters at 32,767 per statement; Lucas's ~50k-id shards crashed with
InterfaceError on every retry, wedging all 4 partitions at retry_count=3
with zero bytes uploaded. jrue's ~2k-id shards fit, which is why his run
succeeded and this stayed hidden.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_chunk_under_test", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_chunk_under_test"] = _bw
assert _spec and _spec.loader
_spec.loader.exec_module(_bw)  # type: ignore[union-attr]

ASYNCPG_BIND_CAP = 32767


def test_chunks_stay_under_asyncpg_bind_cap():
    ids = [f"id{i}" for i in range(199_000)]  # the jlucas drive scale
    chunks = list(_bw.iter_hydration_chunks(ids))
    assert all(len(c) < ASYNCPG_BIND_CAP - 10 for c in chunks), \
        "every chunk must leave headroom for the non-id bind params"


def test_chunks_preserve_all_ids_in_order():
    ids = [f"id{i}" for i in range(25_000)]
    chunks = list(_bw.iter_hydration_chunks(ids))
    assert [x for c in chunks for x in c] == ids


def test_small_input_is_single_chunk():
    ids = ["a", "b", "c"]
    assert list(_bw.iter_hydration_chunks(ids)) == [ids]


def test_empty_input_yields_nothing():
    assert list(_bw.iter_hydration_chunks([])) == []
