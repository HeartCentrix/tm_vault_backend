"""ITO-10146 — the coverage gate must score against an ABSOLUTE baseline.

Masking factor from the incident: the gate compared each walk only against
the IMMEDIATE prior item-bearing snapshot. Once one truncated 46% PARTIAL
(515,649 of 1,114,978) became "prior", every following truncated walk
enumerated ~the same id set and scored ~100% — four identical 46% captures
in a row each passed as the SLA-current restore point.

Fix: baseline = max(immediate prior, best item-bearing COMPLETED/PARTIAL
snapshot within ARCHIVE_BASELINE_LOOKBACK_DAYS), via the injectable
``load_baseline`` collaborator. Coverage numbers are stamped on EVERY run.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import types

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://u:p@localhost/db")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
_MOD_PATH = _REPO_ROOT / "workers" / "backup-worker" / "archive_backup.py"
_spec = importlib.util.spec_from_file_location("archive_backup_baseline_uut", _MOD_PATH)
ab = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["archive_backup_baseline_uut"] = ab
assert _spec and _spec.loader
_spec.loader.exec_module(ab)  # type: ignore[union-attr]

GRAPH = "https://graph.microsoft.com/beta"
PRI = "MBX:primary@t"


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class _FlatGraph:
    """One location, one folder, N items — enough to drive the gate."""

    def __init__(self, n_items):
        self.n = n_items

    async def _get(self, url, params=None, on_activity=None):
        if url.endswith(f"/{PRI}/folders"):
            return {"value": [{"id": "f1", "displayName": "Inbox",
                               "childFolderCount": 0}]}
        if url.endswith("/items"):
            return {"value": [
                {"id": f"i{k}", "changeKey": f"ck{k}", "size": 1, "type": "m"}
                for k in range(self.n)
            ]}
        raise AssertionError(f"unexpected GET {url}")

    async def _post_json(self, url, body, timeout=None):
        return {"value": [
            {"itemId": i, "changeKey": f"ck-{i}", "data": "d"}
            for i in body["itemIds"]
        ]}


def _settings(**over):
    ns = types.SimpleNamespace(
        ARCHIVE_GRAPH_VERSION="beta",
        ARCHIVE_IMPORT_TIMEOUT_S=120,
        ARCHIVE_FOLDER_CONCURRENCY=4,
        ARCHIVE_EXPORT_CONCURRENCY=4,
        ARCHIVE_STALL_ABORT_S=0,
    )
    for k, v in over.items():
        setattr(ns, k, v)
    return ns


def _resource():
    return types.SimpleNamespace(
        id="11111111-1111-1111-1111-111111111111",
        external_id=PRI, display_name="jrue archive",
        extra_data={"archiveMailboxId": PRI},
    )


def _snapshot():
    return types.SimpleNamespace(
        id="22222222-2222-2222-2222-222222222222", delta_tokens_json=None)


class _Persist:
    async def __call__(self, pairs):
        return len(pairs), len(pairs), 0


async def _no_carry(_t, _p, _i):
    return 0


def _drive(n_items, *, prev_ids, baseline, **settings_over):
    async def load_prev(_r, _c):
        return ("prior-snap", {i: "ck-old" for i in prev_ids}) if prev_ids else (None, {})

    async def load_baseline(_r, _c):
        return baseline

    return _run(ab.run_archive_backup(
        _FlatGraph(n_items), _resource(), _snapshot(),
        types.SimpleNamespace(id="t"),
        settings=_settings(**settings_over),
        load_prev=load_prev,
        load_baseline=load_baseline,
        persist_batch=_Persist(),
        carry_forward=_no_carry,
    ))


def test_truncated_prior_cannot_ratify_a_truncated_walk():
    """46 items enumerated, immediate prior ALSO 46 (the truncated PARTIAL),
    but the 30-day best is 100 → 46% coverage → the run must FAIL."""
    with pytest.raises(ab.EnumerationFailed) as ei:
        _drive(46, prev_ids=[f"i{k}" for k in range(46)], baseline=100)
    msg = str(ei.value)
    assert "baseline 100" in msg and "46.0%" in msg


def test_full_walk_passes_absolute_baseline():
    result = _drive(100, prev_ids=[f"i{k}" for k in range(100)], baseline=100)
    # all unchanged? changeKeys differ (ck-old vs ckN) → re-export, fine —
    # what matters here is the gate passed and the run finalized.
    assert result["item_count"] == 100


def test_gate_disabled_by_zero_floor():
    result = _drive(46, prev_ids=[f"i{k}" for k in range(46)], baseline=100,
                    ARCHIVE_MIN_COVERAGE_PCT=0)
    assert result["item_count"] == 46


def test_baseline_loader_failure_falls_back_to_prior():
    async def load_prev(_r, _c):
        return "prior-snap", {f"i{k}": f"ck{k}" for k in range(46)}

    async def bad_baseline(_r, _c):
        raise RuntimeError("db down")

    async def counting_carry(_t, _p, ids):
        return len(ids)

    result = _run(ab.run_archive_backup(
        _FlatGraph(46), _resource(), _snapshot(),
        types.SimpleNamespace(id="t"),
        settings=_settings(),
        load_prev=load_prev,
        load_baseline=bad_baseline,
        persist_batch=_Persist(),
        carry_forward=counting_carry,
    ))
    # prior=46, enumerated=46 (all unchanged → carried) → 100% of the
    # fallback baseline → passes.
    assert result["item_count"] == 46


def test_coverage_stamped_on_every_run():
    snap = _snapshot()

    async def load_prev(_r, _c):
        return "prior-snap", {f"i{k}": f"ck{k}" for k in range(100)}

    async def load_baseline(_r, _c):
        return 100

    _run(ab.run_archive_backup(
        _FlatGraph(100), _resource(), snap, types.SimpleNamespace(id="t"),
        settings=_settings(),
        load_prev=load_prev,
        load_baseline=load_baseline,
        persist_batch=_Persist(),
        carry_forward=_no_carry,
    ))
    dt = snap.delta_tokens_json or {}
    assert dt.get("items_enumerated") == 100
    assert dt.get("items_baseline") == 100
    assert dt.get("coverage_pct") == 100.0
    assert dt.get("locations_walked") == 1


def test_default_baseline_sql_uses_lookback_and_item_bearing_filter():
    import inspect
    src = inspect.getsource(ab._default_load_baseline)
    assert "MAX(s.item_count)" in src
    assert "make_interval(days => :days)" in src
    assert "item_type = 'ARCHIVE_ITEM'" in src
    assert "'COMPLETED', 'PARTIAL'" in src


def test_default_load_prev_prefers_richest_recent_snapshot():
    """The changeKey baseline must be the item-RICHEST recent snapshot, not
    merely the newest: a truncated PARTIAL as baseline made every recovered
    item look 'new' → ~30k metered exportItems calls per run (ITO-10146)."""
    import inspect
    src = inspect.getsource(ab._default_load_prev)
    assert "make_interval(days => :days)" in src
    assert "s.item_count DESC NULLS LAST" in src
    assert src.find("item_count DESC") < src.find("s.created_at DESC \"")\
        or "created_at DESC " in src.split("item_count DESC", 1)[1], (
        "recency must only tie-break after item richness"
    )
