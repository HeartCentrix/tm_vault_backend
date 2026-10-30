"""Per-resource overlap hold: one slow resource must not starve the policy.

Prod, 2026-08-06: the Gold 02:07Z tick dispatched NOTHING. Audit event::

    BACKUP_CYCLE_SKIPPED_OVERLAP — "Policy 'Gold': scheduled cycle skipped —
    the previous cycle is still running (1 job(s) in flight, 8.0h elapsed)."

The single in-flight job was ``abcd008b``: 2 SharePoint sites (IT OPS NOTES,
a 62 GB partitioned walk) which ran **12.84h** — longer than Gold's own 8h
cadence. Because ``decide_overlap_action`` is cycle-level, that one resource
suppressed the other 49, and the staleness override (3× cadence, 24h floor)
is far too coarse to rescue it. Any resource whose walk exceeds the cadence
therefore guarantees a fully lost tick, every time.

Fix: when the previous cycle is still draining, hold back ONLY the resources
that actually have a backup in flight and dispatch the rest. Data safety is
unchanged — the worker's per-resource ``skipped_concurrent`` guard already
refuses a second concurrent run for the same resource. A cycle is skipped
outright only when EVERY resource is held.
"""
from __future__ import annotations

import importlib.util
import pathlib
import sys
import types
import uuid
from datetime import datetime, timedelta

import pytest

_HELPERS = (
    pathlib.Path(__file__).resolve().parents[2]
    / "services" / "backup-scheduler" / "dispatch_policy.py"
)


def _load():
    spec = importlib.util.spec_from_file_location(
        "backup_scheduler_dispatch_policy_partial", _HELPERS
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


dp = _load()


def _res(rid=None, rtype="USER_MAIL", name=None):
    return types.SimpleNamespace(
        id=rid or uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        last_backup_at=None,
        type=types.SimpleNamespace(value=rtype),
        display_name=name or "res",
    )


def _policy():
    return types.SimpleNamespace(id=uuid.uuid4(), name="Gold",
                                 frequency=types.SimpleNamespace(value="THREE_DAILY"))


# ─── partition ─────────────────────────────────────────────────────────

def test_holds_only_the_inflight_resource():
    slow = _res(rtype="SHAREPOINT_SITE", name="IT OPS NOTES")
    others = [_res() for _ in range(49)]
    go, held = dp.partition_overlap_holds([slow] + others, {slow.id})
    assert [r.id for r in held] == [slow.id]
    assert len(go) == 49, "the other 49 resources must still dispatch"


def test_nothing_in_flight_dispatches_everything():
    rs = [_res() for _ in range(5)]
    go, held = dp.partition_overlap_holds(rs, set())
    assert len(go) == 5 and held == []


def test_all_in_flight_holds_everything():
    """This is the only case that should still skip the whole cycle."""
    rs = [_res() for _ in range(3)]
    go, held = dp.partition_overlap_holds(rs, {r.id for r in rs})
    assert go == [] and len(held) == 3


def test_empty_resource_list():
    go, held = dp.partition_overlap_holds([], {uuid.uuid4()})
    assert go == [] and held == []


def test_inflight_ids_may_be_strings():
    """The id set comes from a SQL union whose UUIDs can arrive as text
    depending on driver/cast — a type mismatch must not silently dispatch a
    resource that is already running."""
    r = _res()
    go, held = dp.partition_overlap_holds([r], {str(r.id)})
    assert [x.id for x in held] == [r.id]
    assert go == []


def test_unrelated_inflight_ids_are_ignored():
    r = _res()
    go, held = dp.partition_overlap_holds([r], {uuid.uuid4(), uuid.uuid4()})
    assert [x.id for x in go] == [r.id] and held == []


def test_order_is_preserved():
    rs = [_res(name=f"r{i}") for i in range(6)]
    go, _held = dp.partition_overlap_holds(rs, {rs[2].id})
    assert [r.display_name for r in go] == ["r0", "r1", "r3", "r4", "r5"]


# ─── audit payload ─────────────────────────────────────────────────────

def test_partial_audit_details_shape():
    slow = _res(rtype="SHAREPOINT_SITE", name="IT OPS NOTES")
    now = datetime(2026, 8, 6, 2, 7)
    d = dp.overlap_partial_audit_details(
        _policy(), [slow],
        inflight_jobs=1,
        oldest_created_at=now - timedelta(hours=8),
        now=now,
        dispatched=49,
    )
    assert d["skip_reason"] == "cycle_overlap"
    assert d["action"] == dp.PARTIAL
    assert d["held_count"] == 1
    assert d["dispatched_count"] == 49
    assert str(slow.id) in d["resource_ids"]
    assert d["inflight_age_hours"] == 8.0
    assert "49" in d["message"] and "1" in d["message"]
    assert d["source"] == "backup_scheduler"
    # the held resource must be identifiable from the event alone
    assert d["resources"][0]["name"] == "IT OPS NOTES"
    assert d["resources"][0]["type"] == "SHAREPOINT_SITE"


def test_partial_audit_details_tolerates_unknown_age():
    d = dp.overlap_partial_audit_details(
        _policy(), [_res()], inflight_jobs=1, oldest_created_at=None,
        now=datetime(2026, 8, 6, 2, 7), dispatched=3,
    )
    assert d["inflight_age_hours"] is None
    assert isinstance(d["message"], str) and d["message"]


def test_partial_action_constant_is_distinct():
    assert dp.PARTIAL not in (dp.DISPATCH, dp.SKIP, dp.OVERRIDE)


# ─── the call site actually uses it ────────────────────────────────────

def test_scheduler_no_longer_returns_immediately_on_skip():
    src = (
        pathlib.Path(__file__).resolve().parents[2]
        / "services" / "backup-scheduler" / "main.py"
    ).read_text()
    assert "partition_overlap_holds(" in src, (
        "dispatch still skips the whole policy on overlap"
    )
    assert "overlap_partial_audit_details(" in src
    assert "BACKUP_CYCLE_PARTIAL_OVERLAP" in src
