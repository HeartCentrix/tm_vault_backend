"""Regressions for three SLA-assignment safety guards in resource-service.

1. Retro-sweep stomp (2026-08 review): _apply_policy_to_matching ran
   `UPDATE resources SET sla_policy_id = :policy` for every group match
   with no IS NULL guard — editing a broad "All Users" group silently
   moved explicitly-pinned resources (e.g. 7-year-retention executive
   mailboxes) onto a 30-day policy, and the next retention_cleanup pass
   deleted their snapshots. The sweep must only bind UNASSIGNED rows.

2. Linked-scope resurrection: expand_linked_policy_scope pulled sibling
   rows with no status filter, and assign paths flip every expanded row
   to ACTIVE — silently undoing a PENDING_DELETION / ARCHIVED teardown
   and re-enrolling the resource in scheduled backups. The exclusion is
   assignment-only: the unassign paths must still reach torn-down
   siblings, otherwise their sla_policy_id is undetachable and the next
   DELETE /policies/{id} dies on the FK with the blocking rows hidden
   from the resource UI.

3. PUT /policies dropped explicit nulls: `val is not None` meant a client
   clearing legalHoldUntil / retentionArchiveDays / GFS counts was
   silently ignored while the save appeared to succeed — retention kept
   its old bound and cleanup kept deleting snapshots the operator had
   just marked keep-forever.
"""
from __future__ import annotations

import importlib.util
import inspect
import os
import pathlib
import sys
from types import SimpleNamespace

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "services" / "resource-service" / "main.py"
_spec = importlib.util.spec_from_file_location(
    "resource_main_sla_guards_under_test", _MAIN_PATH,
)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
assert _spec and _spec.loader
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]


# ─── 1. retro-sweep binds unassigned resources only ─────────────────────

def test_retro_sweep_streams_only_unassigned_resources():
    src = inspect.getsource(_mod._apply_policy_to_matching)
    # Guard must be present on the streamed SELECT and re-checked on both
    # bulk UPDATEs (chunk + tail) so a racing explicit assignment wins.
    assert src.count("Resource.sla_policy_id.is_(None)") >= 3


def test_retro_sweep_docstring_pins_no_override_contract():
    doc = _mod._apply_policy_to_matching.__doc__ or ""
    assert "UNASSIGNED" in doc


# ─── 2. linked-scope expansion excludes torn-down rows ──────────────────

def test_expand_linked_scope_filters_pending_deletion_and_archived():
    # The candidates query (and its torn-down filter) moved into
    # fetch_linked_scope_candidates so callers that expand many chunks can
    # hoist ONE tenant-wide scan per request; expand_linked_policy_scope
    # delegates to it when no candidates are passed. The behavioral tests
    # below still drive expand_linked_policy_scope end-to-end.
    src = inspect.getsource(_mod.fetch_linked_scope_candidates)
    assert "ResourceStatus.PENDING_DELETION" in src
    assert "ResourceStatus.ARCHIVED" in src
    assert ".notin_(" in src


class _CapturingSession:
    """Records the candidate SELECT; expansion needs no rows back."""

    def __init__(self):
        self.statements = []

    async def execute(self, stmt):
        self.statements.append(stmt)
        return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: []))


def _seed():
    return SimpleNamespace(
        id="00000000-0000-0000-0000-0000000000aa",
        tenant_id="00000000-0000-0000-0000-0000000000bb",
        type=_mod.ResourceType.ENTRA_USER,
        external_id="user-1",
        extra_data=None,
    )


async def _candidate_where(**kwargs) -> str:
    db = _CapturingSession()
    await _mod.expand_linked_policy_scope(db, [_seed()], **kwargs)
    assert len(db.statements) == 1
    return str(db.statements[0].whereclause)


def test_assignment_expansion_filters_on_status():
    import asyncio

    where = asyncio.run(_candidate_where())
    assert "resources.status NOT IN" in where


def test_unassign_expansion_reaches_torn_down_siblings():
    import asyncio

    # exclude_torn_down=False must drop the status predicate entirely,
    # otherwise ARCHIVED / PENDING_DELETION rows keep sla_policy_id and
    # the policy can never be deleted.
    where = asyncio.run(_candidate_where(exclude_torn_down=False))
    assert "resources.status" not in where


def test_unassign_call_sites_disable_the_exclusion():
    for fn in (_mod.unassign_policy, _mod.bulk_unassign_policy):
        assert "exclude_torn_down=False" in inspect.getsource(fn)
    # bulk-assign's empty-policyId branch is an unassign in disguise.
    bulk_assign_src = inspect.getsource(_mod.bulk_assign_policy)
    assert "exclude_torn_down=False" in bulk_assign_src
    # ...but its real assign path keeps the exclusion.
    assert bulk_assign_src.count("expand_linked_policy_scope") == 2
    assert bulk_assign_src.count("exclude_torn_down=False") == 1
    assert "exclude_torn_down" not in inspect.getsource(_mod.assign_policy)


# ─── 3. PUT /policies honors explicit nulls on nullable columns ─────────

_FIELD_MAP = {
    "name": "name",
    "frequency": "frequency",
    "retentionArchiveDays": "retention_archive_days",
    "gfsDailyCount": "gfs_daily_count",
    "legalHoldUntil": "legal_hold_until",
    "retentionDays": "retention_days",
}


def _policy_row():
    return SimpleNamespace(
        name="Gold",
        frequency="DAILY",
        retention_archive_days=90,
        gfs_daily_count=7,
        legal_hold_until="2027-01-01T00:00:00",
        retention_days=30,
    )


def test_explicit_null_clears_nullable_retention_fields():
    policy = _policy_row()
    _mod.apply_policy_field_updates(
        policy,
        {
            "name": "Gold",
            "retentionArchiveDays": None,
            "legalHoldUntil": None,
            "gfsDailyCount": None,
        },
        _FIELD_MAP,
    )
    assert policy.retention_archive_days is None
    assert policy.legal_hold_until is None
    assert policy.gfs_daily_count is None
    # Absent keys stay untouched.
    assert policy.retention_days == 30
    assert policy.frequency == "DAILY"


def test_null_on_non_nullable_column_is_ignored():
    policy = _policy_row()
    _mod.apply_policy_field_updates(policy, {"name": None}, _FIELD_MAP)
    assert policy.name == "Gold"


def test_non_null_values_still_apply():
    policy = _policy_row()
    _mod.apply_policy_field_updates(
        policy,
        {"retention_days": 365, "name": "Platinum"},
        _FIELD_MAP,
    )
    assert policy.retention_days == 365
    assert policy.name == "Platinum"


def test_snake_case_null_also_clears():
    policy = _policy_row()
    _mod.apply_policy_field_updates(
        policy, {"retention_archive_days": None}, _FIELD_MAP,
    )
    assert policy.retention_archive_days is None
