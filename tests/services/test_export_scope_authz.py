"""Regression: the export authorization decision must come from server state.

Two round-2 findings, both variations of "the gate reads facts the caller
controls":

1. BYPASS — `export_authz.ensure_job_access` exempted "generic" export jobs
   (`if not _is_chat_export(job): return`), so tenant membership was the whole
   gate for them. `POST /api/v1/jobs/export` is a second door into the same
   pipeline: restore-worker packages Teams message bodies into the generic
   EXPORT_ZIP artifact (workers/restore-worker/main.py:2607), so
   "any logged-in user can download any other user's Teams chats" still
   worked — just via /api/v1/jobs/export instead of /api/v1/exports/chat.

2. FORGERY — the per-job owner check read `spec.kind` / `spec.userId`, and
   `main.trigger_export` had no auth dependency at all and merged the caller's
   raw body into the spec (`spec={..., **request}`). A caller therefore
   controlled the very fields the gate consumed: sending
   `{"kind": "chat_export_thread", "userId": "<my own sub>"}` with someone
   else's snapshot ids manufactured a job that passed the "owner" check.

Contract pinned here — one decision, derived from the database:
  - Every route that CREATES an export/restore artifact requires a verified
    subject, and authorizes every Resource the requested snapshot/item ids
    resolve to (not the ids' claimed provenance).
  - Ids in a request body or in a persisted `Job.spec` are lookup keys only:
    a forged id can add a target to the check, never remove one.
  - `Job.spec` is built from an allowlist with server-derived attribution
    stamped last, so a caller cannot write authorization-relevant fields.
  - Every route that SERVES or DESCRIBES the artifact applies the identical
    rule, with no "generic job" exemption.

Driven through the real FastAPI app with `get_db` /
`get_current_user_from_token` overridden — the status codes below are real
router output, not asserted-on source text.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys
import uuid

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.sql import visitors

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_SVC_DIR = _REPO_ROOT / "services" / "job-service"
if str(_SVC_DIR) not in sys.path:
    sys.path.insert(0, str(_SVC_DIR))

_spec = importlib.util.spec_from_file_location("js_main_scope_under_test", _SVC_DIR / "main.py")
_js = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["js_main_scope_under_test"] = _js
assert _spec and _spec.loader
_spec.loader.exec_module(_js)  # type: ignore[union-attr]

from shared.database import get_db  # noqa: E402
from shared.models import (  # noqa: E402
    Job,
    JobStatus,
    JobType,
    PlatformUser,
    Resource,
    ResourceStatus,
    ResourceType,
    Snapshot,
    SnapshotItem,
    Tenant,
)
from shared.security import get_current_user_from_token  # noqa: E402

ORG_A = uuid.UUID("00000000-0000-0000-0000-0000000000a0")
ORG_B = uuid.UUID("00000000-0000-0000-0000-0000000000b0")
TENANT_A = uuid.UUID("00000000-0000-0000-0000-0000000000a1")
TENANT_B = uuid.UUID("00000000-0000-0000-0000-0000000000b1")

VICTIM = "victim@taylormorrison.com"
ATTACKER = "attacker@taylormorrison.com"

CHAT_KIND = "chat_export_thread"

# A caller who clears the gate on a job that is not COMPLETED gets the
# handler's 409 "Export not ready" — proof authorization passed without
# needing Azure storage configured in the test environment.
_NOT_READY = 409


class _FakeResult:
    def __init__(self, rows):
        self._rows = list(rows)

    def scalar_one_or_none(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        return self._rows[0] if self._rows else None

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)

    def __iter__(self):
        return iter(self._rows)


def _binds(stmt):
    """Flattened bind values, so `col.in_([...])` reads the same as `col ==`."""
    vals = []
    visitors.traverse(stmt, {}, {"bindparam": lambda b: vals.append(b.value)})
    flat = []
    for v in vals:
        if isinstance(v, (list, tuple, set)):
            flat.extend(v)
        else:
            flat.append(v)
    return flat


class _FakeSession:
    def __init__(self, *, resources=(), snapshots=(), items=(), jobs=(), tenants=(), users=()):
        self.resources = {r.id: r for r in resources}
        self.snapshots = {s.id: s for s in snapshots}
        self.items = {i.id: i for i in items}
        self.jobs = {j.id: j for j in jobs}
        self.tenants = list(tenants)
        self.users = {u.id: u for u in users}
        self.added = []

    def add(self, obj):
        self.added.append(obj)

    async def commit(self):
        pass

    async def flush(self):
        pass

    async def get(self, entity, ident):
        return {
            Job: self.jobs,
            Resource: self.resources,
            Snapshot: self.snapshots,
            SnapshotItem: self.items,
        }.get(entity, {}).get(ident)

    async def execute(self, stmt):
        entity = stmt.column_descriptions[0]["entity"]
        binds = _binds(stmt)
        if entity is Job:
            return _FakeResult([self.jobs[b] for b in binds if b in self.jobs])
        if entity is Resource:
            return _FakeResult([self.resources[b] for b in binds if b in self.resources])
        if entity is Snapshot:
            return _FakeResult([self.snapshots[b] for b in binds if b in self.snapshots])
        if entity is SnapshotItem:
            # select(SnapshotItem.snapshot_id).where(SnapshotItem.id.in_(...))
            return _FakeResult(
                [self.items[b].snapshot_id for b in binds if b in self.items]
            )
        if entity is PlatformUser:
            return _FakeResult([self.users[b] for b in binds if b in self.users])
        if entity is Tenant:
            return _FakeResult(
                [
                    t.id
                    for t in self.tenants
                    if t.archived_at is None and (t.id in binds or t.org_id in binds)
                ]
            )
        raise AssertionError(f"unexpected query entity {entity!r}")


@pytest.fixture(autouse=True)
def _no_audit_calls(monkeypatch):
    """The trigger handlers fire a best-effort audit POST inside try/except.
    Make it fail instantly instead of doing DNS for audit-service."""
    import httpx

    class _Boom:
        def __init__(self, *a, **k):
            raise RuntimeError("audit disabled in tests")

    monkeypatch.setattr(httpx, "AsyncClient", _Boom)
    yield


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    _js.app.dependency_overrides.clear()


def _tenant(tid, org) -> Tenant:
    return Tenant(id=tid, org_id=org, display_name=str(tid)[-2:])


def _entra_user(email, tenant_id=TENANT_A) -> Resource:
    return Resource(
        id=uuid.uuid4(),
        tenant_id=tenant_id,
        type=ResourceType.ENTRA_USER,
        external_id=f"entra-{email}",
        display_name=email,
        email=email,
        extra_data={"user_principal_name": email},
        status=ResourceStatus.DISCOVERED,
    )


def _user_chats(parent: Resource) -> Resource:
    return Resource(
        id=uuid.uuid4(),
        tenant_id=parent.tenant_id,
        type=ResourceType.USER_CHATS,
        external_id=f"{parent.external_id}:chats",
        display_name=f"Chats - {parent.display_name}",
        email=parent.email,
        extra_data={"user_id": parent.external_id},
        parent_resource_id=parent.id,
        status=ResourceStatus.DISCOVERED,
    )


def _snapshot(resource: Resource) -> Snapshot:
    return Snapshot(id=uuid.uuid4(), resource_id=resource.id)


def _item(snapshot: Snapshot) -> SnapshotItem:
    return SnapshotItem(
        id=uuid.uuid4(),
        snapshot_id=snapshot.id,
        item_type="TEAMS_CHAT_MESSAGE",
        external_id=str(uuid.uuid4()),
    )


def _token(*, email=ATTACKER, tenant_ids=(TENANT_A,), roles=("USER",), sub=None):
    return {
        "id": str(sub or uuid.uuid4()),
        "email": email,
        "roles": list(roles),
        "org_id": str(ORG_A),
        "tenant_ids": [str(t) for t in tenant_ids],
    }


def _client(sess, user=None) -> TestClient:
    _js.app.dependency_overrides[get_db] = lambda: sess
    if user is not None:
        _js.app.dependency_overrides[get_current_user_from_token] = lambda: user
    return TestClient(_js.app)


def _export_job(*, tenant_id=TENANT_A, resource=None, spec=None, status=JobStatus.RUNNING) -> Job:
    return Job(
        id=uuid.uuid4(),
        tenant_id=tenant_id,
        resource_id=resource.id if resource is not None else None,
        type=JobType.EXPORT,
        status=status,
        spec=spec if spec is not None else {"restore_type": "EXPORT_ZIP", "snapshot_ids": [], "item_ids": []},
        result={},
    )


def _victim_world():
    """Victim's chats + one snapshot + one item, plus tenant A."""
    parent = _entra_user(VICTIM)
    chats = _user_chats(parent)
    snap = _snapshot(chats)
    item = _item(snap)
    return parent, chats, snap, item


# =================================================== create: generic /jobs/export


def test_generic_export_create_requires_a_token():
    """`POST /api/v1/jobs/export` had no auth dependency at all."""
    parent, chats, snap, item = _victim_world()
    sess = _FakeSession(
        resources=[parent, chats], snapshots=[snap], items=[item], tenants=[_tenant(TENANT_A, ORG_A)]
    )
    r = _client(sess).post("/api/v1/jobs/export", json={"snapshotIds": [str(snap.id)]})
    assert r.status_code == 401


def test_generic_export_create_denies_another_users_snapshot():
    """The second door into the chat pipeline: an authenticated tenant member
    queueing an export of a colleague's USER_CHATS snapshot."""
    parent, chats, snap, item = _victim_world()
    sess = _FakeSession(
        resources=[parent, chats], snapshots=[snap], items=[item], tenants=[_tenant(TENANT_A, ORG_A)]
    )
    r = _client(sess, _token()).post("/api/v1/jobs/export", json={"snapshotIds": [str(snap.id)]})
    assert r.status_code == 403
    assert sess.added == []


def test_generic_export_create_denies_items_smuggled_from_another_snapshot():
    """itemIds are authorized too — otherwise a caller lists their own
    snapshot (which sets Job.resource_id) and smuggles the victim's items in
    alongside it."""
    parent, chats, snap, item = _victim_world()
    mine_parent = _entra_user(ATTACKER)
    mine_chats = _user_chats(mine_parent)
    mine_snap = _snapshot(mine_chats)
    sess = _FakeSession(
        resources=[parent, chats, mine_parent, mine_chats],
        snapshots=[snap, mine_snap],
        items=[item],
        tenants=[_tenant(TENANT_A, ORG_A)],
    )
    r = _client(sess, _token()).post(
        "/api/v1/jobs/export",
        json={"snapshotIds": [str(mine_snap.id)], "itemIds": [str(item.id)]},
    )
    assert r.status_code == 403
    assert sess.added == []


def test_generic_export_create_allows_the_resource_owner():
    """Self-service export must keep working."""
    parent = _entra_user(VICTIM)
    chats = _user_chats(parent)
    snap = _snapshot(chats)
    sess = _FakeSession(
        resources=[parent, chats], snapshots=[snap], tenants=[_tenant(TENANT_A, ORG_A)]
    )
    r = _client(sess, _token(email=VICTIM)).post(
        "/api/v1/jobs/export", json={"snapshotIds": [str(snap.id)]}
    )
    assert r.status_code == 200, r.text
    assert len(sess.added) == 1


def test_generic_export_create_allows_an_operator():
    parent, chats, snap, item = _victim_world()
    sess = _FakeSession(
        resources=[parent, chats], snapshots=[snap], tenants=[_tenant(TENANT_A, ORG_A)]
    )
    user = _token(roles=["RESTORE_OPERATOR"])
    r = _client(sess, user).post("/api/v1/jobs/export", json={"snapshotIds": [str(snap.id)]})
    assert r.status_code == 200, r.text


def test_generic_export_spec_is_not_caller_writable():
    """`spec={..., **request}` let the caller write the fields the gate read.
    Attribution must be stamped from the verified subject, and unknown keys
    must not be persisted at all."""
    parent = _entra_user(VICTIM)
    chats = _user_chats(parent)
    snap = _snapshot(chats)
    sess = _FakeSession(
        resources=[parent, chats], snapshots=[snap], tenants=[_tenant(TENANT_A, ORG_A)]
    )
    caller = _token(email=VICTIM)
    r = _client(sess, caller).post(
        "/api/v1/jobs/export",
        json={
            "snapshotIds": [str(snap.id)],
            "kind": CHAT_KIND,
            "userId": str(uuid.uuid4()),
            "userEmail": ATTACKER,
            "tenantId": str(TENANT_B),
            "junkKeyNothingReads": "x",
        },
    )
    assert r.status_code == 200, r.text
    spec = sess.added[0].spec
    assert spec.get("kind") != CHAT_KIND
    assert spec.get("userId") == caller["id"]
    assert spec.get("userEmail") == VICTIM
    assert "junkKeyNothingReads" not in spec
    assert "tenantId" not in spec


# ========================================================= create: other doors


def test_restore_create_requires_a_token():
    parent, chats, snap, item = _victim_world()
    sess = _FakeSession(
        resources=[parent, chats], snapshots=[snap], tenants=[_tenant(TENANT_A, ORG_A)]
    )
    r = _client(sess).post(
        "/api/v1/jobs/restore",
        json={"restoreType": "EXPORT_ZIP", "snapshotIds": [str(snap.id)]},
    )
    assert r.status_code == 401


def test_restore_create_denies_another_users_snapshot():
    """`files_export_or_restore` forwards EXPORT_ZIP here, and
    download_export_zip serves any COMPLETED job with a blob_path regardless
    of Job.type — so this is an export-producing path."""
    parent, chats, snap, item = _victim_world()
    sess = _FakeSession(
        resources=[parent, chats], snapshots=[snap], tenants=[_tenant(TENANT_A, ORG_A)]
    )
    r = _client(sess, _token()).post(
        "/api/v1/jobs/restore",
        json={"restoreType": "EXPORT_ZIP", "snapshotIds": [str(snap.id)]},
    )
    assert r.status_code == 403
    assert sess.added == []


def test_sharepoint_download_requires_a_token():
    parent, chats, snap, item = _victim_world()
    sess = _FakeSession(
        resources=[parent, chats], snapshots=[snap], tenants=[_tenant(TENANT_A, ORG_A)]
    )
    r = _client(sess).post(
        f"/api/v1/sharepoint/{chats.id}/download",
        json={"scope": "site", "snapshotId": str(snap.id)},
    )
    assert r.status_code == 401


def test_export_or_restore_requires_a_token():
    parent, chats, snap, item = _victim_world()
    sess = _FakeSession(
        resources=[parent, chats], snapshots=[snap], tenants=[_tenant(TENANT_A, ORG_A)]
    )
    r = _client(sess).post(
        f"/api/v1/resources/{chats.id}/export-or-restore",
        json={"restoreType": "EXPORT_ZIP", "snapshotId": str(snap.id), "itemIds": [str(uuid.uuid4())]},
    )
    assert r.status_code == 401


def test_restore_status_requires_a_token():
    parent, chats, snap, item = _victim_world()
    job = _export_job(resource=chats)
    sess = _FakeSession(
        resources=[parent, chats], snapshots=[snap], jobs=[job], tenants=[_tenant(TENANT_A, ORG_A)]
    )
    r = _client(sess).get(f"/api/v1/jobs/restore/{job.id}/status")
    assert r.status_code == 401


def test_restore_status_denies_another_users_export_job():
    parent, chats, snap, item = _victim_world()
    job = _export_job(resource=chats)
    sess = _FakeSession(
        resources=[parent, chats], snapshots=[snap], jobs=[job], tenants=[_tenant(TENANT_A, ORG_A)]
    )
    r = _client(sess, _token()).get(f"/api/v1/jobs/restore/{job.id}/status")
    assert r.status_code == 403


# ============================================== serve: no generic-job exemption


def test_generic_export_download_is_not_a_bypass():
    """BLOCKER 1. Generic export job over the victim's chats resource,
    downloaded by an ordinary tenant member: the exemption made this a 409
    (gate cleared)."""
    parent, chats, snap, item = _victim_world()
    job = _export_job(resource=chats, spec={"restore_type": "EXPORT_ZIP", "snapshot_ids": [str(snap.id)]})
    sess = _FakeSession(
        resources=[parent, chats], snapshots=[snap], items=[item], jobs=[job],
        tenants=[_tenant(TENANT_A, ORG_A)],
    )
    r = _client(sess, _token()).get(f"/api/v1/jobs/export/{job.id}/download")
    assert r.status_code == 403


def test_generic_export_status_is_not_a_bypass():
    parent, chats, snap, item = _victim_world()
    job = _export_job(resource=chats, spec={"restore_type": "EXPORT_ZIP", "snapshot_ids": [str(snap.id)]})
    sess = _FakeSession(
        resources=[parent, chats], snapshots=[snap], jobs=[job], tenants=[_tenant(TENANT_A, ORG_A)],
    )
    r = _client(sess, _token()).get(f"/api/v1/jobs/export/{job.id}/status")
    assert r.status_code == 403


def test_forged_chat_export_spec_does_not_authorize_download():
    """BLOCKER 2. `kind` + `userId` in the spec are caller-writable, so they
    cannot be the owner check. Attacker's own sub stamped on a job whose
    resource is the victim's chats."""
    parent, chats, snap, item = _victim_world()
    attacker = _token()
    job = _export_job(
        resource=chats,
        spec={"kind": CHAT_KIND, "userId": attacker["id"], "snapshot_ids": [str(snap.id)]},
    )
    sess = _FakeSession(
        resources=[parent, chats], snapshots=[snap], jobs=[job], tenants=[_tenant(TENANT_A, ORG_A)],
    )
    r = _client(sess, attacker).get(f"/api/v1/jobs/export/{job.id}/download")
    assert r.status_code == 403


def test_download_denied_when_spec_items_belong_to_another_users_snapshot():
    """Job.resource_id points at the caller's own resource, but the spec's
    item ids resolve to the victim's snapshot — the artifact contains the
    victim's messages, so the gate must deny."""
    parent, chats, snap, item = _victim_world()
    mine_parent = _entra_user(ATTACKER)
    mine_chats = _user_chats(mine_parent)
    mine_snap = _snapshot(mine_chats)
    job = _export_job(
        resource=mine_chats,
        spec={"restore_type": "EXPORT_ZIP", "snapshot_ids": [str(mine_snap.id)], "item_ids": [str(item.id)]},
    )
    sess = _FakeSession(
        resources=[parent, chats, mine_parent, mine_chats],
        snapshots=[snap, mine_snap], items=[item], jobs=[job],
        tenants=[_tenant(TENANT_A, ORG_A)],
    )
    r = _client(sess, _token()).get(f"/api/v1/jobs/export/{job.id}/download")
    assert r.status_code == 403


def test_owner_can_still_download_their_generic_export():
    """Recovery/self-service must not be collateral damage."""
    parent = _entra_user(VICTIM)
    chats = _user_chats(parent)
    snap = _snapshot(chats)
    job = _export_job(resource=chats, spec={"restore_type": "EXPORT_ZIP", "snapshot_ids": [str(snap.id)]})
    sess = _FakeSession(
        resources=[parent, chats], snapshots=[snap], jobs=[job], tenants=[_tenant(TENANT_A, ORG_A)],
    )
    r = _client(sess, _token(email=VICTIM)).get(f"/api/v1/jobs/export/{job.id}/download")
    assert r.status_code == _NOT_READY


def test_operator_can_download_a_generic_export_in_tenant():
    """The IT/helpdesk path stays open with the documented role grant."""
    parent, chats, snap, item = _victim_world()
    job = _export_job(resource=chats, spec={"restore_type": "EXPORT_ZIP", "snapshot_ids": [str(snap.id)]})
    sess = _FakeSession(
        resources=[parent, chats], snapshots=[snap], jobs=[job], tenants=[_tenant(TENANT_A, ORG_A)],
    )
    user = _token(roles=["RESTORE_OPERATOR"])
    r = _client(sess, user).get(f"/api/v1/jobs/export/{job.id}/download")
    assert r.status_code == _NOT_READY


def test_operator_still_confined_to_entitled_tenants():
    parent = _entra_user(VICTIM, TENANT_B)
    chats = _user_chats(parent)
    snap = _snapshot(chats)
    job = _export_job(tenant_id=TENANT_B, resource=chats, spec={"snapshot_ids": [str(snap.id)]})
    sess = _FakeSession(
        resources=[parent, chats], snapshots=[snap], jobs=[job],
        tenants=[_tenant(TENANT_A, ORG_A), _tenant(TENANT_B, ORG_B)],
    )
    user = _token(roles=["RESTORE_OPERATOR"], tenant_ids=[TENANT_A])
    r = _client(sess, user).get(f"/api/v1/jobs/export/{job.id}/download")
    assert r.status_code == 403


def test_unattributable_job_is_operator_only():
    """Retention prunes snapshot rows out from under a finished artifact. With
    nothing left to resolve, an ordinary user cannot prove ownership — deny
    rather than fall back to "any tenant member"."""
    job = _export_job(spec={"restore_type": "EXPORT_ZIP", "snapshot_ids": [str(uuid.uuid4())]})
    sess = _FakeSession(jobs=[job], tenants=[_tenant(TENANT_A, ORG_A)])
    r = _client(sess, _token()).get(f"/api/v1/jobs/export/{job.id}/download")
    assert r.status_code == 403
    r2 = _client(sess, _token(roles=["RESTORE_OPERATOR"])).get(
        f"/api/v1/jobs/export/{job.id}/download"
    )
    assert r2.status_code == _NOT_READY
