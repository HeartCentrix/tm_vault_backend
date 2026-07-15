"""P4a — every retention deletion writes a per-snapshot forensic AuditEvent.

The July-2 data-loss post-mortem had only an aggregate log line ("deleted N
snapshots") — you could not reconstruct WHICH snapshot of WHICH resource vanished,
when it was created, or under which rule. P4a makes retention emit one AuditEvent
per deleted snapshot so a deletion is always forensically reconstructable.

Tests the PURE builder (no DB); emission into the session is exercised by the
retention integration path.
"""
from __future__ import annotations

import uuid
from types import SimpleNamespace

from shared.retention_cleanup import build_retention_audit
from shared.models import AuditEvent


def _fixture():
    snap = SimpleNamespace(
        id=uuid.uuid4(),
        resource_id=uuid.uuid4(),
        started_at=None,
        created_at=None,
        item_count=42,
        bytes_total=1000,
        type=SimpleNamespace(value="INCREMENTAL"),
        reuse_of_snapshot_id=None,
        reuse_chain_root_id=uuid.uuid4(),
        status=SimpleNamespace(value="COMPLETED"),
    )
    res = SimpleNamespace(
        id=snap.resource_id,
        tenant_id=uuid.uuid4(),
        org_id=uuid.uuid4(),
        type=SimpleNamespace(value="USER_MAIL"),
        display_name="Amit Mishra",
        external_id="amit@qfion.com",
    )
    pol = SimpleNamespace(id=uuid.uuid4(), name="Gold", retention_mode="GFS")
    return snap, res, pol


def test_builds_auditevent_with_core_fields():
    snap, res, pol = _fixture()
    ev = build_retention_audit(snap, res, pol, reason="outside GFS window")

    assert isinstance(ev, AuditEvent)
    assert ev.action == "RETENTION_SNAPSHOT_DELETED"
    assert ev.actor_type == "SYSTEM"
    assert ev.outcome == "SUCCESS"
    assert ev.snapshot_id == snap.id
    assert ev.resource_id == res.id
    assert ev.tenant_id == res.tenant_id
    assert ev.org_id == res.org_id
    assert ev.resource_type == "USER_MAIL"
    assert ev.resource_name == "Amit Mishra"


def test_details_capture_forensic_context():
    snap, res, pol = _fixture()
    ev = build_retention_audit(snap, res, pol, reason="outside GFS window")

    d = ev.details
    assert d["reason"] == "outside GFS window"
    assert d["retention_mode"] == "GFS"
    assert d["policy_name"] == "Gold"
    assert d["policy_id"] == str(pol.id)
    assert d["item_count"] == 42
    assert d["bytes_total"] == 1000
    assert d["snapshot_type"] == "INCREMENTAL"
    # reuse-chain provenance matters for reconstructing what a delete affected
    assert d["reuse_chain_root_id"] == str(snap.reuse_chain_root_id)


def test_missing_org_id_is_tolerated():
    snap, res, pol = _fixture()
    del res.org_id  # some resources have no org linkage
    ev = build_retention_audit(snap, res, pol, reason="flat cutoff")
    assert ev.org_id is None
    assert ev.action == "RETENTION_SNAPSHOT_DELETED"


def test_populated_timestamps_reuse_and_name_fallback():
    from datetime import datetime
    snap, res, pol = _fixture()
    snap.started_at = datetime(2026, 7, 1, 8, 30, 0)
    snap.created_at = datetime(2026, 7, 1, 8, 0, 0)
    snap.reuse_of_snapshot_id = uuid.uuid4()
    res.display_name = None                      # force fallback to external_id
    ev = build_retention_audit(snap, res, pol, reason="gfs")
    d = ev.details
    assert d["started_at"] == "2026-07-01T08:30:00"      # isoformat branch
    assert d["created_at"] == "2026-07-01T08:00:00"
    assert d["reuse_of_snapshot_id"] == str(snap.reuse_of_snapshot_id)  # present branch
    assert ev.resource_name == res.external_id           # display_name -> external_id fallback


# ── emission: one event per DELETED snapshot (kept snapshots get none) ─────
from shared.retention_cleanup import _audit_events_for_deletion  # noqa: E402
from shared.models import ResourceStatus  # noqa: E402


def _snaps_and_ctx():
    a, b, c = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    def _s(sid):
        return SimpleNamespace(
            id=sid, resource_id=uuid.uuid4(), started_at=None, created_at=None,
            item_count=1, bytes_total=1, type=SimpleNamespace(value="INCREMENTAL"),
            reuse_of_snapshot_id=None, reuse_chain_root_id=None,
            status=SimpleNamespace(value="COMPLETED"),
        )
    snaps = [_s(a), _s(b), _s(c)]
    res = SimpleNamespace(
        id=uuid.uuid4(), tenant_id=uuid.uuid4(), org_id=uuid.uuid4(),
        type=SimpleNamespace(value="USER_MAIL"), display_name="X", external_id="x",
        status=ResourceStatus.PROTECTED if hasattr(ResourceStatus, "PROTECTED") else "PROTECTED",
    )
    pol = SimpleNamespace(id=uuid.uuid4(), name="Gold", retention_mode="GFS",
                          archived_retention_mode="SAME")
    return snaps, res, pol, {a, c}   # delete a and c, keep b


def test_emits_one_event_per_deleted_only():
    snaps, res, pol, to_delete = _snaps_and_ctx()
    evs = _audit_events_for_deletion(snaps, to_delete, res, pol)
    assert {e.snapshot_id for e in evs} == to_delete           # kept snapshot b excluded
    assert len(evs) == 2
    assert all(e.action == "RETENTION_SNAPSHOT_DELETED" for e in evs)
    assert all("outside GFS retention window" in e.details["reason"] for e in evs)


def test_emits_nothing_when_nothing_deleted():
    snaps, res, pol, _ = _snaps_and_ctx()
    assert _audit_events_for_deletion(snaps, set(), res, pol) == []


def test_archived_resource_reason():
    snaps, res, pol, to_delete = _snaps_and_ctx()
    res.status = ResourceStatus.ARCHIVED
    pol.archived_retention_mode = "KEEP_LAST"
    evs = _audit_events_for_deletion(snaps, to_delete, res, pol)
    assert all("archived-resource retention (KEEP_LAST)" in e.details["reason"] for e in evs)
