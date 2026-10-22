"""POST /api/v1/resources/bulk-assign-policy — filter-driven bulk assignment.

Contract under test (backward-compatible extension of the existing route):

  * exactly ONE of resourceIds / filter must be present → 400 otherwise;
    filter.types must be non-empty → 400; dryRun + resourceIds → 400 (the
    id-list branch has no dry-run — ignoring the flag would turn a
    "preview" into real writes).
  * filter path resolves the match set server-side with the SAME predicates
    GET /api/v1/resources/by-type applies (build_by_type_search_filters +
    type IN types + tenantId + Protection-scope/hidden-type exclusions), so
    the match set == exactly the rows the UI lists across all pages — with
    ONE assign-only narrowing: when filter.status is omitted,
    ARCHIVED/PENDING_DELETION seeds are excluded (assign flips rows to
    ACTIVE; a broad filter must not silently resurrect archived rows).
    Explicit filter.status is honored; unassign never narrows. A
    valid-but-unknown tenantId → 404 (stale-cache parity with /by-type).
  * dryRun=true → resolve + validate + counts, ZERO writes, no notify;
    the unassign form carries unassigned: 0 (same keys as execute).
  * linked-scope expansion candidates (a tenant-wide scan) are fetched
    ONCE per request and shared across chunks, never once per chunk.
  * execute → id-list assignment semantics (expand_linked_policy_scope,
    validate_policy_scope, override existing sla_policy_id) but with
    CHUNKED bulk UPDATEs (stream ids yield_per=1000, UPDATE per 1000-id
    chunk — asyncpg's 32767 bind-param cap / gateway 60s timeout).
  * policyId '' keeps its unassign meaning on the filter path too.
  * notify_scheduler_reschedule fires after commit on every successful
    non-dry mutation, BOTH paths (parity with update_policy — before this
    change bulk assignments waited for the daily cron tick).
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import uuid
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "services" / "resource-service" / "main.py"
_spec = importlib.util.spec_from_file_location(
    "resource_main_bulk_filter_under_test", _MAIN_PATH,
)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
assert _spec and _spec.loader
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]


# ─── fakes ───────────────────────────────────────────────────────────────

class _FakeResult:
    def __init__(self, rows=None, scalar_value=None, rowcount=0):
        self._rows = rows or []
        self._scalar = scalar_value
        self.rowcount = rowcount

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)

    def scalar(self):
        return self._scalar

    def first(self):
        return self._rows[0] if self._rows else None

    def scalar_one_or_none(self):
        return self._rows[0] if self._rows else None


class _FakeStream:
    """Stands in for AsyncSession.stream()'s AsyncResult."""

    def __init__(self, ids):
        self._ids = list(ids)

    def scalars(self):
        async def _gen():
            for i in self._ids:
                yield i
        return _gen()


def _compiled_params(stmt):
    return dict(stmt.compile().params)


def _in_ids(stmt):
    for key, val in _compiled_params(stmt).items():
        if key.startswith("id_") and isinstance(val, (list, tuple)):
            return list(val)
    return []


class _FakeDB:
    """Routes statements by shape; records writes into an event log."""

    def __init__(self, resources=(), policies=(), tenants=(),
                 matched_ids=None, events=None):
        self.resources = {r.id: r for r in resources}
        self.policies = {p.id: p for p in policies}
        self.tenants = {t.id: t for t in tenants}
        self.matched_ids = (
            list(matched_ids) if matched_ids is not None
            else [r.id for r in resources]
        )
        self.events = events if events is not None else []
        self.count_calls = []    # (sql, params)
        self.stream_calls = []   # (sql, params, execution_options)
        self.updates = []        # (sql, ids, values)
        self.commit_count = 0

    async def get(self, model, pk):
        name = getattr(model, "__name__", "")
        if name == "SlaPolicy":
            return self.policies.get(pk)
        if name == "Tenant":
            return self.tenants.get(pk)
        return self.resources.get(pk)

    async def execute(self, stmt, params=None):
        sql = str(stmt)
        upper = sql.strip().upper()
        if upper.startswith("SELECT COUNT"):
            self.count_calls.append((sql, params))
            return _FakeResult(scalar_value=len(self.matched_ids))
        if upper.startswith("UPDATE"):
            ids = _in_ids(stmt)
            values = {
                k: v for k, v in _compiled_params(stmt).items()
                if not k.startswith("id_")
            }
            self.updates.append((sql, ids, values))
            self.events.append(("update", len(ids)))
            return _FakeResult(rowcount=len(ids))
        if upper.startswith("SELECT") and "FROM TENANTS" in upper:
            tid = (params or {}).get("tid")
            return _FakeResult(rows=[1] if tid in self.tenants else [])
        if upper.startswith("SELECT") and "FROM RESOURCES" in upper:
            ids = _in_ids(stmt)
            rows = [self.resources[i] for i in ids if i in self.resources]
            return _FakeResult(rows=rows)
        return _FakeResult()

    async def stream(self, stmt, params=None):
        self.stream_calls.append(
            (str(stmt), params, dict(getattr(stmt, "_execution_options", {}) or {}))
        )
        return _FakeStream(self.matched_ids)

    async def commit(self):
        self.commit_count += 1
        self.events.append(("commit", None))


# ─── fixtures / builders ─────────────────────────────────────────────────

TENANT_ID = uuid.uuid4()
POLICY_ID = uuid.uuid4()


def _resource(type_=None, sla=None):
    return SimpleNamespace(
        id=uuid.uuid4(),
        tenant_id=TENANT_ID,
        type=type_ or _mod.ResourceType.SHAREPOINT_SITE,
        external_id=f"ext-{uuid.uuid4().hex[:8]}",
        extra_data=None,
        status=_mod.ResourceStatus.ACTIVE,
        sla_policy_id=sla,
        email=None,
    )


def _policy(service_type="m365"):
    return SimpleNamespace(id=POLICY_ID, tenant_id=TENANT_ID,
                           service_type=service_type)


def _tenant():
    return SimpleNamespace(id=TENANT_ID, type="M365")


@pytest.fixture
def patched(monkeypatch):
    calls = SimpleNamespace(expand=[], notify=0, tier2=[], events=[],
                            candidate_fetches=[])

    async def fake_expand(db, seeds, *, exclude_torn_down=True,
                          candidates=None):
        calls.expand.append(
            {"seeds": list(seeds), "exclude_torn_down": exclude_torn_down,
             "candidates": candidates}
        )
        return list(seeds)

    async def fake_fetch_candidates(db, tenant_ids, *,
                                    exclude_torn_down=True):
        calls.candidate_fetches.append(
            {"tenant_ids": list(tenant_ids),
             "exclude_torn_down": exclude_torn_down}
        )
        return []

    async def fake_notify():
        calls.notify += 1
        calls.events.append(("notify", None))

    async def fake_tier2(resources):
        calls.tier2.append(list(resources))

    monkeypatch.setattr(_mod, "expand_linked_policy_scope", fake_expand)
    monkeypatch.setattr(_mod, "fetch_linked_scope_candidates",
                        fake_fetch_candidates)
    monkeypatch.setattr(_mod, "notify_scheduler_reschedule", fake_notify)
    monkeypatch.setattr(_mod, "_enqueue_tier2_for_entra_users", fake_tier2)
    return calls


def _req(**kw):
    return _mod.BulkAssignRequest(**kw)


def _filter(**overrides):
    base = {"tenantId": str(TENANT_ID), "types": ["SHAREPOINT_SITE"]}
    base.update(overrides)
    return base


def _run(coro):
    return asyncio.run(coro)


# ─── validation: exactly one of resourceIds / filter ─────────────────────

def test_both_resource_ids_and_filter_is_400(patched):
    db = _FakeDB(policies=[_policy()], tenants=[_tenant()])
    req = _req(resourceIds=[str(uuid.uuid4())], policyId=str(POLICY_ID),
               filter=_filter())
    with pytest.raises(HTTPException) as ei:
        _run(_mod.bulk_assign_policy(req, db))
    assert ei.value.status_code == 400
    assert db.updates == [] and db.commit_count == 0


def test_neither_resource_ids_nor_filter_is_400(patched):
    db = _FakeDB(policies=[_policy()], tenants=[_tenant()])
    req = _req(policyId=str(POLICY_ID))
    with pytest.raises(HTTPException) as ei:
        _run(_mod.bulk_assign_policy(req, db))
    assert ei.value.status_code == 400
    assert db.updates == [] and db.commit_count == 0


def test_empty_filter_types_is_400(patched):
    db = _FakeDB(policies=[_policy()], tenants=[_tenant()])
    req = _req(policyId=str(POLICY_ID), filter=_filter(types=[]))
    with pytest.raises(HTTPException) as ei:
        _run(_mod.bulk_assign_policy(req, db))
    assert ei.value.status_code == 400
    assert db.updates == [] and db.commit_count == 0


def test_id_list_with_dry_run_is_400_and_writes_nothing(patched):
    """dryRun on the id-list branch must refuse loudly, never write.

    The id-list branch has no dry-run implementation; before this guard
    {"resourceIds": [...], "dryRun": true} silently performed REAL
    assignments on what the client believed was a preview call.
    """
    row = _resource()
    db = _FakeDB(resources=[row], policies=[_policy()], tenants=[_tenant()],
                 events=patched.events)
    req = _req(resourceIds=[str(row.id)], policyId=str(POLICY_ID),
               dryRun=True)
    with pytest.raises(HTTPException) as ei:
        _run(_mod.bulk_assign_policy(req, db))
    assert ei.value.status_code == 400
    assert row.sla_policy_id is None, "a 'preview' call must never assign"
    assert db.updates == [] and db.commit_count == 0
    assert patched.notify == 0 and patched.tier2 == []


def test_id_list_unassign_with_dry_run_is_400_and_writes_nothing(patched):
    row = _resource(sla=POLICY_ID)
    db = _FakeDB(resources=[row], policies=[_policy()], tenants=[_tenant()],
                 events=patched.events)
    req = _req(resourceIds=[str(row.id)], policyId="", dryRun=True)
    with pytest.raises(HTTPException) as ei:
        _run(_mod.bulk_assign_policy(req, db))
    assert ei.value.status_code == 400
    assert row.sla_policy_id == POLICY_ID, "a 'preview' call must never unassign"
    assert db.commit_count == 0 and patched.notify == 0


# ─── stale-tenant guard: unknown tenantId → 404, parity with /by-type ─────

def test_filter_unknown_tenant_is_404_on_assign(patched):
    db = _FakeDB(policies=[_policy()], tenants=[_tenant()],
                 events=patched.events)
    req = _req(policyId=str(POLICY_ID),
               filter=_filter(tenantId=str(uuid.uuid4())))
    with pytest.raises(HTTPException) as ei:
        _run(_mod.bulk_assign_policy(req, db))
    assert ei.value.status_code == 404
    assert db.count_calls == [] and db.stream_calls == []
    assert db.updates == [] and patched.notify == 0


def test_filter_unknown_tenant_is_404_on_unassign_not_silent_noop(patched):
    """The unassign form runs no validate_policy_scope, so without an
    explicit check a stale cached tenantId returned a silent 200
    {matched: 0, unassigned: 0} — the exact no-op /by-type's 404 was
    added to eliminate."""
    db = _FakeDB(policies=[], tenants=[_tenant()], events=patched.events)
    req = _req(policyId="", filter=_filter(tenantId=str(uuid.uuid4())))
    with pytest.raises(HTTPException) as ei:
        _run(_mod.bulk_assign_policy(req, db))
    assert ei.value.status_code == 404
    assert db.count_calls == [] and db.stream_calls == []
    assert db.updates == [] and patched.notify == 0


# ─── dry run: resolve + validate, ZERO writes ────────────────────────────

def test_filter_dry_run_returns_matched_with_zero_writes(patched):
    rows = [_resource() for _ in range(3)]
    db = _FakeDB(resources=rows, policies=[_policy()], tenants=[_tenant()],
                 events=patched.events)
    req = _req(policyId=str(POLICY_ID), filter=_filter(), dryRun=True)
    body = _run(_mod.bulk_assign_policy(req, db))
    assert body == {"matched": 3, "assigned": 0, "dryRun": True}
    assert db.updates == []
    assert db.stream_calls == [], "dry run must not open the id stream"
    assert db.commit_count == 0
    assert patched.notify == 0, "dry run must not nudge the scheduler"
    assert patched.candidate_fetches == [], (
        "dry run must not pay the tenant-wide candidates scan"
    )


# ─── execute: assigns, overrides an existing policy ──────────────────────

def test_filter_execute_assigns_and_overrides_existing_assignment(patched):
    other_policy = uuid.uuid4()
    fresh = _resource()
    already_assigned = _resource(sla=other_policy)
    db = _FakeDB(resources=[fresh, already_assigned],
                 policies=[_policy()], tenants=[_tenant()],
                 events=patched.events)
    req = _req(policyId=str(POLICY_ID), filter=_filter())
    body = _run(_mod.bulk_assign_policy(req, db))

    assert body == {"matched": 2, "assigned": 2, "dryRun": False}
    assert len(db.updates) == 1
    sql, ids, values = db.updates[0]
    assert set(ids) == {fresh.id, already_assigned.id}, (
        "an already-assigned resource must be overridden, not skipped"
    )
    assert "sla_policy_id IS NULL" not in sql, (
        "explicit bulk assignment overrides — no IS NULL guard"
    )
    assert values.get("sla_policy_id") == POLICY_ID
    assert values.get("status") == _mod.ResourceStatus.ACTIVE
    assert db.commit_count == 1
    # assignment path keeps torn-down siblings out of the expansion
    assert all(c["exclude_torn_down"] is True for c in patched.expand)


def test_filter_execute_notifies_scheduler_after_commit(patched):
    rows = [_resource()]
    db = _FakeDB(resources=rows, policies=[_policy()], tenants=[_tenant()],
                 events=patched.events)
    req = _req(policyId=str(POLICY_ID), filter=_filter())
    _run(_mod.bulk_assign_policy(req, db))
    assert patched.notify == 1
    kinds = [k for k, _ in patched.events]
    assert kinds.index("commit") < kinds.index("notify"), (
        "reschedule nudge must come after the commit"
    )
    assert all(
        kinds.index("commit") > i
        for i, k in enumerate(kinds) if k == "update"
    ), "all writes belong to the committed transaction"


def test_filter_execute_enqueues_tier2_for_entra_users(patched):
    user = _resource(type_=_mod.ResourceType.ENTRA_USER)
    site = _resource()
    db = _FakeDB(resources=[user, site], policies=[_policy()],
                 tenants=[_tenant()], events=patched.events)
    req = _req(policyId=str(POLICY_ID),
               filter=_filter(types=["ENTRA_USER", "SHAREPOINT_SITE"]))
    _run(_mod.bulk_assign_policy(req, db))
    enqueued = [r for batch in patched.tier2 for r in batch]
    assert [r.id for r in enqueued] == [user.id]


# ─── chunking: >1000 rows → multiple bulk UPDATEs ────────────────────────

def test_filter_chunking_splits_updates_at_1000_ids(patched):
    rows = [_resource() for _ in range(1500)]
    db = _FakeDB(resources=rows, policies=[_policy()], tenants=[_tenant()],
                 events=patched.events)
    req = _req(policyId=str(POLICY_ID), filter=_filter())
    body = _run(_mod.bulk_assign_policy(req, db))

    assert body["matched"] == 1500 and body["assigned"] == 1500
    assert len(db.updates) == 2, "1500 ids must split into 2 bulk UPDATEs"
    sizes = sorted(len(ids) for _, ids, _ in db.updates)
    assert sizes == [500, 1000]
    written = {i for _, ids, _ in db.updates for i in ids}
    assert written == {r.id for r in rows}
    assert db.commit_count == 1, "chunked writes share ONE transaction"
    # ids are streamed server-side, not materialized
    assert len(db.stream_calls) == 1
    _, _, exec_opts = db.stream_calls[0]
    assert exec_opts.get("yield_per") == 1000


def test_filter_expansion_candidates_fetched_once_across_chunks(patched):
    """The expansion-candidates query is a tenant-wide scan of the 11
    linked types. Per-chunk fetching is O(chunks × tenant) ORM hydrations
    — ~200k row reads on the pilot tenant — reintroducing the gateway
    timeout the chunking exists to prevent. One fetch per request."""
    rows = [_resource() for _ in range(1500)]
    db = _FakeDB(resources=rows, policies=[_policy()], tenants=[_tenant()],
                 events=patched.events)
    req = _req(policyId=str(POLICY_ID), filter=_filter())
    _run(_mod.bulk_assign_policy(req, db))

    assert len(patched.expand) == 2, "1500 seeds → 2 expansion flushes"
    assert len(patched.candidate_fetches) == 1, (
        "candidates must be fetched once per request, not once per chunk"
    )
    fetch = patched.candidate_fetches[0]
    assert list(fetch["tenant_ids"]) == [TENANT_ID]
    assert fetch["exclude_torn_down"] is True
    assert all(c["candidates"] is not None for c in patched.expand), (
        "every expansion call must reuse the hoisted candidates"
    )


def test_filter_unassign_candidates_fetch_includes_torn_down(patched):
    rows = [_resource(sla=POLICY_ID)]
    db = _FakeDB(resources=rows, policies=[_policy()], tenants=[_tenant()],
                 events=patched.events)
    req = _req(policyId="", filter=_filter())
    _run(_mod.bulk_assign_policy(req, db))
    assert len(patched.candidate_fetches) == 1
    assert patched.candidate_fetches[0]["exclude_torn_down"] is False


def test_filter_cross_chunk_expansion_dedup_writes_sibling_once(
        patched, monkeypatch):
    """The same linked sibling resolved from seeds in TWO different chunks
    must be written (and tier-2 enqueued) exactly once."""
    seeds = [_resource() for _ in range(1500)]
    sibling = _resource(type_=_mod.ResourceType.ENTRA_USER)
    db = _FakeDB(resources=seeds + [sibling],
                 matched_ids=[r.id for r in seeds],
                 policies=[_policy()], tenants=[_tenant()],
                 events=patched.events)

    async def overlapping_expand(db_, chunk, *, exclude_torn_down=True,
                                 candidates=None):
        return list(chunk) + [sibling]

    monkeypatch.setattr(_mod, "expand_linked_policy_scope",
                        overlapping_expand)
    req = _req(policyId=str(POLICY_ID), filter=_filter())
    body = _run(_mod.bulk_assign_policy(req, db))

    written = [i for _, ids, _ in db.updates for i in ids]
    assert len(written) == 1501, "sibling must be written once, not twice"
    assert len(set(written)) == 1501
    assert sibling.id in set(written)
    # flush 1: 1000 seeds + sibling → sub-chunks 1000 + 1; flush 2: 500
    assert sorted(len(ids) for _, ids, _ in db.updates) == [1, 500, 1000]
    assert body["matched"] == 1500 and body["assigned"] == 1501
    assert db.commit_count == 1
    enqueued = [r.id for batch in patched.tier2 for r in batch]
    assert enqueued == [sibling.id], "tier-2 prep must not enqueue twice"


def test_filter_expansion_inflating_flush_past_1000_splits_subchunks(
        patched, monkeypatch):
    """Expansion can inflate a 1000-seed flush beyond 1000 ids; the write
    loop must sub-chunk so no single UPDATE exceeds 1000 binds."""
    seeds = [_resource() for _ in range(1000)]
    siblings = [_resource() for _ in range(600)]
    db = _FakeDB(resources=seeds, matched_ids=[r.id for r in seeds],
                 policies=[_policy()], tenants=[_tenant()],
                 events=patched.events)

    async def inflating_expand(db_, chunk, *, exclude_torn_down=True,
                               candidates=None):
        return list(chunk) + siblings

    monkeypatch.setattr(_mod, "expand_linked_policy_scope",
                        inflating_expand)
    req = _req(policyId=str(POLICY_ID), filter=_filter())
    body = _run(_mod.bulk_assign_policy(req, db))

    assert [len(ids) for _, ids, _ in db.updates] == [1000, 600]
    assert body["assigned"] == 1600
    assert db.commit_count == 1


# ─── torn-down seeds: assign narrows, explicit status honored ─────────────

def test_filter_assign_without_status_excludes_torn_down_seeds(patched):
    """A broad assign filter must not resurrect archived rows: the write
    flips every matched row to ACTIVE, so ARCHIVED/PENDING_DELETION seeds
    are excluded unless filter.status names one explicitly."""
    rows = [_resource()]
    db = _FakeDB(resources=rows, policies=[_policy()], tenants=[_tenant()],
                 events=patched.events)
    req = _req(policyId=str(POLICY_ID), filter=_filter())
    _run(_mod.bulk_assign_policy(req, db))

    count_sql, _ = db.count_calls[0]
    assert (
        "r.status::text NOT IN ('ARCHIVED', 'PENDING_DELETION')" in count_sql
    )
    # count and stream must still share the exact FROM/WHERE
    stream_sql, _, _ = db.stream_calls[0]
    assert count_sql.split("FROM", 1)[1] == stream_sql.split("FROM", 1)[1]


def test_filter_assign_with_explicit_status_reaches_that_status(patched):
    """filter.status = 'ARCHIVED' is the operator explicitly naming
    torn-down rows — honored, no implicit narrowing on top."""
    rows = [_resource()]
    db = _FakeDB(resources=rows, policies=[_policy()], tenants=[_tenant()],
                 events=patched.events)
    req = _req(policyId=str(POLICY_ID), filter=_filter(status="ARCHIVED"))
    _run(_mod.bulk_assign_policy(req, db))

    count_sql, count_params = db.count_calls[0]
    assert "NOT IN ('ARCHIVED', 'PENDING_DELETION')" not in count_sql
    assert count_params["rstatus"] == "ARCHIVED"


def test_filter_unassign_matches_torn_down_seeds(patched):
    """Unassign never sets status and must keep reaching torn-down rows
    (a torn-down row keeping sla_policy_id blocks policy deletion)."""
    rows = [_resource(sla=POLICY_ID)]
    db = _FakeDB(resources=rows, policies=[_policy()], tenants=[_tenant()],
                 events=patched.events)
    req = _req(policyId="", filter=_filter())
    _run(_mod.bulk_assign_policy(req, db))

    count_sql, _ = db.count_calls[0]
    assert "NOT IN ('ARCHIVED', 'PENDING_DELETION')" not in count_sql


# ─── unassign-by-filter (policyId == '') ─────────────────────────────────

def test_filter_unassign_clears_policy_and_reaches_torn_down(patched):
    rows = [_resource(sla=POLICY_ID) for _ in range(2)]
    db = _FakeDB(resources=rows, policies=[_policy()], tenants=[_tenant()],
                 events=patched.events)
    req = _req(policyId="", filter=_filter())
    body = _run(_mod.bulk_assign_policy(req, db))

    assert body["matched"] == 2
    assert body["assigned"] == 0
    assert body["unassigned"] == 2
    assert body["dryRun"] is False
    assert len(db.updates) == 1
    _, ids, values = db.updates[0]
    assert set(ids) == {r.id for r in rows}
    assert values.get("sla_policy_id") is None
    assert "status" not in values, "unassign must not touch resource status"
    # unassign is the path that must reach ARCHIVED/PENDING_DELETION siblings
    assert all(c["exclude_torn_down"] is False for c in patched.expand)
    assert patched.notify == 1
    assert patched.tier2 == [], "unassign never enqueues tier-2 prep"


def test_filter_unassign_dry_run_reports_matched_for_unassign_all(patched):
    rows = [_resource(sla=POLICY_ID) for _ in range(4)]
    db = _FakeDB(resources=rows, policies=[_policy()], tenants=[_tenant()],
                 events=patched.events)
    req = _req(policyId="", filter=_filter(), dryRun=True)
    body = _run(_mod.bulk_assign_policy(req, db))
    assert body == {"matched": 4, "assigned": 0, "unassigned": 0,
                    "dryRun": True}, (
        "dry-run unassign must carry the same keys as execute unassign — "
        "assigned/unassigned count writes performed (0), matched previews"
    )
    assert db.updates == [] and db.commit_count == 0
    assert patched.notify == 0


# ─── validation: service-type compatibility ──────────────────────────────

def test_filter_service_type_mismatch_is_400_with_zero_writes(patched):
    db = _FakeDB(resources=[], policies=[_policy(service_type="m365")],
                 tenants=[_tenant()], events=patched.events)
    req = _req(policyId=str(POLICY_ID), filter=_filter(types=["AZURE_VM"]))
    with pytest.raises(HTTPException) as ei:
        _run(_mod.bulk_assign_policy(req, db))
    assert ei.value.status_code == 400
    assert db.updates == [] and db.stream_calls == [] and db.commit_count == 0
    assert patched.notify == 0


def test_filter_mixed_service_types_is_400(patched):
    db = _FakeDB(resources=[], policies=[_policy()], tenants=[_tenant()],
                 events=patched.events)
    req = _req(policyId=str(POLICY_ID),
               filter=_filter(types=["AZURE_VM", "SHAREPOINT_SITE"]))
    with pytest.raises(HTTPException) as ei:
        _run(_mod.bulk_assign_policy(req, db))
    assert ei.value.status_code == 400
    assert db.updates == []


# ─── match-set parity with GET /resources/by-type ────────────────────────

def test_filter_match_set_uses_by_type_predicates(patched):
    rows = [_resource()]
    db = _FakeDB(resources=rows, policies=[_policy()], tenants=[_tenant()],
                 events=patched.events)
    req = _req(
        policyId=str(POLICY_ID),
        filter=_filter(query="itops", status="ACTIVE", sla="none"),
    )
    _run(_mod.bulk_assign_policy(req, db))

    count_sql, count_params = db.count_calls[0]
    # search predicates come from the SHARED builder, with its params
    assert "ILIKE :rquery" in count_sql
    assert "r.status::text = :rstatus" in count_sql
    assert "r.sla_policy_id IS NULL" in count_sql
    assert count_params["rquery"] == "%itops%"
    assert count_params["rstatus"] == "ACTIVE"
    # tenant + type-set predicates
    assert "r.tenant_id = :rtenant" in count_sql
    assert "r.type::text = ANY(:rtypes)" in count_sql
    assert count_params["rtenant"] == str(TENANT_ID)
    assert count_params["rtypes"] == ["SHAREPOINT_SITE"]
    # canonical Protection scope (hidden-type exclusion) is applied, so the
    # match set equals what the by-type listing shows
    hidden = sorted(_mod.PROTECTION_SCOPE_EXCLUDED_TYPES)[0]
    assert f"'{hidden}'" in count_sql and "NOT IN" in count_sql

    # the streamed id query must share the exact FROM/WHERE with the count
    stream_sql, stream_params, _ = db.stream_calls[0]
    assert count_sql.split("FROM", 1)[1] == stream_sql.split("FROM", 1)[1]
    assert stream_params == count_params


def test_filter_invalid_status_propagates_400_from_shared_builder(patched):
    db = _FakeDB(resources=[], policies=[_policy()], tenants=[_tenant()])
    req = _req(policyId=str(POLICY_ID), filter=_filter(status="BOGUS"))
    with pytest.raises(HTTPException) as ei:
        _run(_mod.bulk_assign_policy(req, db))
    assert ei.value.status_code == 400


# ─── id-list path: unchanged shape + new scheduler nudge ─────────────────

def test_id_list_assign_response_shape_unchanged_and_notifies(patched):
    row = _resource()
    db = _FakeDB(resources=[row], policies=[_policy()], tenants=[_tenant()],
                 events=patched.events)
    req = _req(resourceIds=[str(row.id)], policyId=str(POLICY_ID))
    body = _run(_mod.bulk_assign_policy(req, db))
    assert body == {"assigned": 1, "not_found": []}
    assert row.sla_policy_id == POLICY_ID
    assert row.status == _mod.ResourceStatus.ACTIVE
    assert patched.notify == 1
    kinds = [k for k, _ in patched.events]
    assert kinds.index("commit") < kinds.index("notify")


def test_id_list_unassign_response_shape_unchanged_and_notifies(patched):
    row = _resource(sla=POLICY_ID)
    db = _FakeDB(resources=[row], policies=[_policy()], tenants=[_tenant()],
                 events=patched.events)
    req = _req(resourceIds=[str(row.id)], policyId="")
    body = _run(_mod.bulk_assign_policy(req, db))
    assert body == {"assigned": 0, "unassigned": 1, "not_found": []}
    assert row.sla_policy_id is None
    assert patched.notify == 1
