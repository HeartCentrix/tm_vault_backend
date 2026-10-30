"""Regression: a restore's DESTINATION is authorized, and the check is cheap.

Round-3 blocker. `trigger_restore` grew a source-scope gate and authorized
`targetResourceId` — but restore-worker resolves the destination from
`spec.targetUserId` FIRST (`workers/restore-worker/main.py:1507`
``spec.get("targetUserId") or spec.get("targetResourceId")``) and the OneDrive
engine reads `targetUserId` EXCLUSIVELY
(`_resolve_onedrive_target_user`, :3070). So renaming one JSON key moved the
destination past the guard: an ordinary user could write their own backed-up
files into a colleague's OneDrive — a write-side escalation.

Contract pinned here:
  * EVERY spec key a restore engine consults to decide where bytes land is
    authorized, not just the one the guard happened to know about. Destinations
    that name a `Resource` row (`targetUserId`, `targetResourceId`) must resolve
    to a row the caller may write; destinations that name something outside the
    resources table (`targetEnvironmentId`, `newSite*`, `azureRestoreParams`)
    are operator-only, because there is no row to own.
  * The recognized set is an explicit allowlist and anything destination-shaped
    outside it FAILS CLOSED — a field added to the worker later cannot silently
    reopen this.
  * The legacy `external_id` spelling of a target is rejected here rather than
    resolved by the worker, so the row we authorize is the row the worker writes
    into.
  * The authorization decision is stamped on the Job row at create time, so the
    polled /status + download routes stop re-walking `snapshot_items` (an
    87 GB table) on every tick — and the stamp is an optimization only: it can
    never drop a target the full walk would have found.

Driven through the real FastAPI router with `get_db` /
`get_current_user_from_token` overridden, so the status codes are real router
output and the CALL SITES are what is under test, not just the helpers.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import re
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

_spec_ = importlib.util.spec_from_file_location("js_main_dest_under_test", _SVC_DIR / "main.py")
_js = importlib.util.module_from_spec(_spec_)  # type: ignore[arg-type]
sys.modules["js_main_dest_under_test"] = _js
assert _spec_ and _spec_.loader
_spec_.loader.exec_module(_js)  # type: ignore[union-attr]

import export_authz as _authz  # noqa: E402
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

_NOT_READY = 409  # gate cleared, artifact simply isn't COMPLETED


class _FakeResult:
    def __init__(self, rows):
        self._rows = list(rows)

    def scalar_one_or_none(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        return self._rows[0] if self._rows else None

    def scalar_one(self):
        return self._rows[0] if self._rows else None

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)

    def __iter__(self):
        return iter(self._rows)


def _binds(stmt):
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
    """Stub AsyncSession that answers by selected entity + bind values, and
    records which entities were queried so a test can prove the hot polled
    path no longer touches `snapshot_items`."""

    def __init__(self, *, resources=(), snapshots=(), items=(), jobs=(), tenants=(), users=()):
        self.resources = {r.id: r for r in resources}
        self.snapshots = {s.id: s for s in snapshots}
        self.items = {i.id: i for i in items}
        self.jobs = {j.id: j for j in jobs}
        self.tenants = list(tenants)
        self.users = {u.id: u for u in users}
        self.added = []
        self.queried = []          # entity names, in order
        self.external_id_lookups = 0

    def add(self, obj):
        self.added.append(obj)

    async def commit(self):
        pass

    async def flush(self):
        pass

    async def get(self, entity, ident):
        self.queried.append(getattr(entity, "__name__", str(entity)))
        return {
            Job: self.jobs,
            Resource: self.resources,
            Snapshot: self.snapshots,
            SnapshotItem: self.items,
        }.get(entity, {}).get(ident)

    async def execute(self, stmt):
        entity = stmt.column_descriptions[0]["entity"]
        binds = _binds(stmt)
        self.queried.append(getattr(entity, "__name__", str(entity)))
        if entity is Job:
            return _FakeResult([self.jobs[b] for b in binds if b in self.jobs])
        if entity is Resource:
            hits = [self.resources[b] for b in binds if b in self.resources]
            if not hits:
                # Legacy external_id spelling of a target, as
                # restore_cross_user would resolve it. Counted so a test can
                # prove the gate never gets here.
                ext = {str(r.external_id): r for r in self.resources.values()}
                hits = [ext[str(b)] for b in binds if str(b) in ext]
                if hits:
                    self.external_id_lookups += 1
            return _FakeResult(hits)
        if entity is Snapshot:
            return _FakeResult([self.snapshots[b] for b in binds if b in self.snapshots])
        if entity is SnapshotItem:
            return _FakeResult([self.items[b].snapshot_id for b in binds if b in self.items])
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


def _child(parent: Resource, rtype: ResourceType) -> Resource:
    return Resource(
        id=uuid.uuid4(),
        tenant_id=parent.tenant_id,
        type=rtype,
        external_id=f"{parent.external_id}:{rtype.value.lower()}",
        display_name=f"{rtype.value} - {parent.display_name}",
        email=parent.email,
        extra_data={"user_id": parent.external_id, "drive_id": f"drive-{uuid.uuid4()}"},
        parent_resource_id=parent.id,
        status=ResourceStatus.DISCOVERED,
    )


def _site(tenant_id=TENANT_A) -> Resource:
    """A resource no ordinary user's UPN can match (operator-only)."""
    return Resource(
        id=uuid.uuid4(),
        tenant_id=tenant_id,
        type=ResourceType.SHAREPOINT_SITE,
        external_id=f"site-{uuid.uuid4()}",
        display_name="Finance",
        email=None,
        extra_data={},
        status=ResourceStatus.ACTIVE,
    )


def _snapshot(resource: Resource) -> Snapshot:
    return Snapshot(id=uuid.uuid4(), resource_id=resource.id)


def _item(snapshot: Snapshot) -> SnapshotItem:
    return SnapshotItem(
        id=uuid.uuid4(),
        snapshot_id=snapshot.id,
        item_type="FILE",
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


def _world():
    """Attacker's OneDrive (+snapshot, +item) and the victim's OneDrive, one
    tenant. The PoC shape from the review: own source, foreign destination."""
    mine_parent = _entra_user(ATTACKER)
    mine_drive = _child(mine_parent, ResourceType.ONEDRIVE)
    mine_snap = _snapshot(mine_drive)
    mine_item = _item(mine_snap)
    their_parent = _entra_user(VICTIM)
    their_drive = _child(their_parent, ResourceType.ONEDRIVE)
    return mine_parent, mine_drive, mine_snap, mine_item, their_parent, their_drive


def _sess_for_world(world, *, tenants=None):
    mine_parent, mine_drive, mine_snap, mine_item, their_parent, their_drive = world
    return _FakeSession(
        resources=[mine_parent, mine_drive, their_parent, their_drive],
        snapshots=[mine_snap],
        items=[mine_item],
        tenants=tenants or [_tenant(TENANT_A, ORG_A)],
    )


# ================================================= (a) destinations are authorized


def test_restore_denies_targetuserid_into_another_users_onedrive():
    """THE BLOCKER. Source scope is the caller's own snapshot, so the old gate
    passed; the destination is the victim's OneDrive, which restore-worker
    reads from `targetUserId` and prefers over `targetResourceId`."""
    world = _world()
    _, _, mine_snap, _, _, their_drive = world
    sess = _sess_for_world(world)
    r = _client(sess, _token()).post(
        "/api/v1/jobs/restore",
        json={
            "restoreType": "ONEDRIVE",
            "snapshotIds": [str(mine_snap.id)],
            "targetUserId": str(their_drive.id),
        },
    )
    assert r.status_code == 403, r.text
    assert sess.added == [], "no job may be queued when the destination is denied"


def test_restore_denies_targetresourceid_into_another_users_onedrive():
    """The spelling the previous commit did authorize — pinned so the two
    cannot drift apart again."""
    world = _world()
    _, _, mine_snap, _, _, their_drive = world
    sess = _sess_for_world(world)
    r = _client(sess, _token()).post(
        "/api/v1/jobs/restore",
        json={
            "restoreType": "CROSS_RESOURCE",
            "snapshotIds": [str(mine_snap.id)],
            "targetResourceId": str(their_drive.id),
        },
    )
    assert r.status_code == 403, r.text
    assert sess.added == []


def test_restore_allows_targetuserid_into_own_onedrive():
    """Self-service restore-to-my-own-drive must keep working."""
    world = _world()
    _, mine_drive, mine_snap, _, _, _ = world
    sess = _sess_for_world(world)
    r = _client(sess, _token()).post(
        "/api/v1/jobs/restore",
        json={
            "restoreType": "ONEDRIVE",
            "snapshotIds": [str(mine_snap.id)],
            "targetUserId": str(mine_drive.id),
        },
    )
    assert r.status_code == 200, r.text
    assert len(sess.added) == 1


def test_restore_allows_operator_cross_user_target():
    """The helpdesk path: restore a departed user's files into a colleague's
    drive. Allowed with the documented operator grant, inside the tenant."""
    world = _world()
    _, _, mine_snap, _, _, their_drive = world
    sess = _sess_for_world(world)
    r = _client(sess, _token(roles=["RESTORE_OPERATOR"])).post(
        "/api/v1/jobs/restore",
        json={
            "restoreType": "ONEDRIVE",
            "snapshotIds": [str(mine_snap.id)],
            "targetUserId": str(their_drive.id),
        },
    )
    assert r.status_code == 200, r.text


def test_restore_denies_target_in_unentitled_tenant_even_for_operator():
    world = _world()
    mine_parent, mine_drive, mine_snap, mine_item, _, _ = world
    other_parent = _entra_user(VICTIM, TENANT_B)
    other_drive = _child(other_parent, ResourceType.ONEDRIVE)
    sess = _FakeSession(
        resources=[mine_parent, mine_drive, other_parent, other_drive],
        snapshots=[mine_snap],
        items=[mine_item],
        tenants=[_tenant(TENANT_A, ORG_A), _tenant(TENANT_B, ORG_B)],
    )
    r = _client(sess, _token(roles=["RESTORE_OPERATOR"], tenant_ids=[TENANT_A])).post(
        "/api/v1/jobs/restore",
        json={
            "restoreType": "ONEDRIVE",
            "snapshotIds": [str(mine_snap.id)],
            "targetUserId": str(other_drive.id),
        },
    )
    assert r.status_code == 403, r.text
    assert sess.added == []


def test_restore_rejects_legacy_external_id_target_spelling():
    """`restore_cross_user` also accepts a Graph external_id as the target
    (main.py:1544). The gate resolves Resource.id only, so that spelling must
    be rejected here instead of being resolved by the worker into a row nobody
    authorized."""
    world = _world()
    _, _, mine_snap, _, _, their_drive = world
    sess = _sess_for_world(world)
    r = _client(sess, _token()).post(
        "/api/v1/jobs/restore",
        json={
            "restoreType": "ONEDRIVE",
            "snapshotIds": [str(mine_snap.id)],
            "targetUserId": their_drive.external_id,
        },
    )
    assert r.status_code == 400, r.text
    assert sess.added == []
    assert sess.external_id_lookups == 0, "the gate must not resolve external_ids"


def test_restore_denies_target_that_resolves_to_nothing():
    """A well-formed UUID that is not a Resource row: fail closed rather than
    queue a job whose destination the worker will resolve on its own."""
    world = _world()
    _, _, mine_snap, _, _, _ = world
    sess = _sess_for_world(world)
    r = _client(sess, _token()).post(
        "/api/v1/jobs/restore",
        json={
            "restoreType": "ONEDRIVE",
            "snapshotIds": [str(mine_snap.id)],
            "targetUserId": str(uuid.uuid4()),
        },
    )
    assert r.status_code in (403, 404), r.text
    assert sess.added == []


def test_export_route_is_also_a_write_path_and_gates_the_destination():
    """`POST /api/v1/jobs/export` takes `restoreType` from the body verbatim
    and publishes to restore.normal, which restore-worker dispatches off
    `message.restoreType` — so CROSS_USER + targetUserId reaches the same
    write handler. The destination gate has to be on both doors."""
    world = _world()
    _, _, mine_snap, _, _, their_drive = world
    sess = _sess_for_world(world)
    r = _client(sess, _token()).post(
        "/api/v1/jobs/export",
        json={
            "restoreType": "CROSS_USER",
            "snapshotIds": [str(mine_snap.id)],
            "targetUserId": str(their_drive.id),
        },
    )
    assert r.status_code == 403, r.text
    assert sess.added == []


# =========================== destinations with no Resource row are operator-only


@pytest.mark.parametrize(
    "body_extra",
    [
        {"targetEnvironmentId": "11111111-2222-3333-4444-555555555555"},
        {"newSiteName": "Exfil Site"},
        {"newSiteOwnerEmail": ATTACKER},
        {"azureRestoreParams": {"targetResourceGroup": "rg-attacker"}},
    ],
    ids=["power-env", "new-site", "new-site-owner", "azure-params"],
)
def test_restore_denies_unownable_destination_for_ordinary_user(body_extra):
    """A Power Platform environment, a site that does not exist yet and an
    Azure target RG are all destinations with no `Resource` row to own — so
    they are operator-only rather than unchecked."""
    world = _world()
    _, _, mine_snap, _, _, _ = world
    sess = _sess_for_world(world)
    body = {"restoreType": "IN_PLACE", "snapshotIds": [str(mine_snap.id)]}
    body.update(body_extra)
    r = _client(sess, _token()).post("/api/v1/jobs/restore", json=body)
    assert r.status_code == 403, r.text
    assert sess.added == []


def test_restore_allows_unownable_destination_for_operator():
    world = _world()
    _, _, mine_snap, _, _, _ = world
    sess = _sess_for_world(world)
    r = _client(sess, _token(roles=["RESTORE_OPERATOR"])).post(
        "/api/v1/jobs/restore",
        json={
            "restoreType": "SHAREPOINT",
            "snapshotIds": [str(mine_snap.id)],
            "newSiteName": "Legal Hold 2026",
        },
    )
    assert r.status_code == 200, r.text


def test_empty_destination_values_are_not_treated_as_targets():
    """The SPA sends the whole restore body with unused keys nulled out (and
    `azureRestoreParams` defaults to `{}`), so an unset destination must not
    trip the operator rule."""
    world = _world()
    _, _, mine_snap, _, _, _ = world
    sess = _sess_for_world(world)
    r = _client(sess, _token()).post(
        "/api/v1/jobs/restore",
        json={
            "restoreType": "IN_PLACE",
            "snapshotIds": [str(mine_snap.id)],
            "targetUserId": None,
            "targetResourceId": None,
            "targetEnvironmentId": None,
            "newSiteName": None,
            "newSiteAlias": None,
            "newSiteOwnerEmail": None,
            "azureRestoreParams": {},
            "targetFolder": "Restored by TM",
        },
    )
    assert r.status_code == 200, r.text


# ============================================= (b) unrecognized keys fail closed


def test_unrecognized_destination_key_fails_closed():
    """Structural half of the fix: a destination-shaped spec key that nothing
    has classified must abort the request, so a field added to the worker later
    cannot ride through unchecked."""
    import asyncio

    sess = _FakeSession(tenants=[_tenant(TENANT_A, ORG_A)])
    with pytest.raises(Exception) as exc:
        asyncio.run(
            _authz.authorize_restore_destinations(
                sess, _token(), {"targetMailbox": "victim@taylormorrison.com"}
            )
        )
    detail = getattr(exc.value, "detail", {})
    assert getattr(exc.value, "status_code", None) == 500
    assert detail.get("error") == "DESTINATION_KEY_UNCLASSIFIED"
    assert "targetMailbox" in (detail.get("keys") or [])


def test_spec_allowlist_and_destination_classification_cannot_drift():
    """The allowlist that decides which body keys reach the worker IS the
    classified key set, so adding a key without classifying it as a
    destination (or explicitly not one) is impossible."""
    assert set(_js._EXPORT_SPEC_KEYS) == set(_authz.SPEC_KEYS)
    classified = (
        set(_authz.DESTINATION_RESOURCE_KEYS)
        | set(_authz.DESTINATION_OPERATOR_KEYS)
        | set(_authz.NON_DESTINATION_SPEC_KEYS)
    )
    assert set(_authz.SPEC_KEYS) == classified
    # No key may be classified twice — that would make the decision ambiguous.
    assert len(_authz.DESTINATION_RESOURCE_KEYS) + len(_authz.DESTINATION_OPERATOR_KEYS) + len(
        _authz.NON_DESTINATION_SPEC_KEYS
    ) == len(classified)


def test_every_destination_shaped_key_the_workers_read_is_classified():
    """Drift guard against the exact defect: `targetUserId` was consumed by
    the worker while the gate only knew about `targetResourceId`. Scans the
    real worker sources for top-level `spec.get("...")` reads."""
    pat = re.compile(r"\b_?spec\.get\(\s*[\"']([A-Za-z_][A-Za-z0-9_]*)[\"']")
    keys = set()
    for path in sorted(
        list((_REPO_ROOT / "workers" / "restore-worker").glob("*.py"))
        + list((_REPO_ROOT / "workers" / "azure-workload-worker").glob("*.py"))
    ):
        keys |= set(pat.findall(path.read_text()))
    shaped = {k for k in keys if _authz.is_destination_shaped(k)}
    assert shaped, "scan found no destination-shaped keys — pattern is broken"
    classified = (
        set(_authz.DESTINATION_RESOURCE_KEYS)
        | set(_authz.DESTINATION_OPERATOR_KEYS)
        | set(_authz.NON_DESTINATION_SPEC_KEYS)
    )
    assert shaped <= classified, f"unclassified destination keys in workers: {sorted(shaped - classified)}"
    # The two keys the blocker was about, spelled out so a rename is loud.
    assert "targetUserId" in _authz.DESTINATION_RESOURCE_KEYS
    assert "targetResourceId" in _authz.DESTINATION_RESOURCE_KEYS


# ================================== the decision is stamped, not re-derived per poll


def _stamped_spec(spec, resource_id, resource_ids):
    out = dict(spec)
    out[_authz.AUTHORIZED_SCOPE_KEY] = _authz.authorized_scope_stamp(
        out, resource_id=resource_id, resource_ids=resource_ids
    )
    return out


def test_create_stamps_the_authorized_scope_including_the_destination():
    world = _world()
    _, mine_drive, mine_snap, _, _, _ = world
    sess = _sess_for_world(world)
    r = _client(sess, _token()).post(
        "/api/v1/jobs/restore",
        json={
            "restoreType": "ONEDRIVE",
            "snapshotIds": [str(mine_snap.id)],
            "targetUserId": str(mine_drive.id),
        },
    )
    assert r.status_code == 200, r.text
    stamp = sess.added[0].spec.get(_authz.AUTHORIZED_SCOPE_KEY)
    assert stamp, "create must record the resolved+authorized scope"
    assert str(mine_drive.id) in stamp["resource_ids"]


def test_status_poll_does_not_rewalk_snapshot_items_when_stamped():
    """The DB-load half: `ensure_job_access` re-ran
    `SnapshotItem.id.in_(...)` on every /status poll and every download.
    With the scope stamped at create time the polled path must not touch
    snapshot_items at all — while still making the same decision."""
    parent = _entra_user(VICTIM)
    drive = _child(parent, ResourceType.ONEDRIVE)
    snap = _snapshot(drive)
    item = _item(snap)
    spec = _stamped_spec(
        {"restore_type": "EXPORT_ZIP", "snapshot_ids": [str(snap.id)], "item_ids": [str(item.id)]},
        drive.id,
        [drive.id],
    )
    job = Job(
        id=uuid.uuid4(), tenant_id=TENANT_A, resource_id=drive.id, type=JobType.EXPORT,
        status=JobStatus.RUNNING, spec=spec, result={},
    )
    sess = _FakeSession(
        resources=[parent, drive], snapshots=[snap], items=[item], jobs=[job],
        tenants=[_tenant(TENANT_A, ORG_A)],
    )
    r = _client(sess, _token(email=VICTIM)).get(f"/api/v1/jobs/export/{job.id}/status")
    assert r.status_code == 200, r.text
    assert "SnapshotItem" not in sess.queried
    assert "Snapshot" not in sess.queried


def test_stamp_works_for_the_chat_export_specs_camelcase_ids():
    """`chat_export.trigger` writes the request model's camelCase ids, the
    generic triggers write snake_case. The fingerprint has to read both or the
    SSE route silently keeps re-walking snapshot_items."""
    parent = _entra_user(VICTIM)
    chats = _child(parent, ResourceType.USER_CHATS)
    snap = _snapshot(chats)
    item = _item(snap)
    spec = _stamped_spec(
        {
            "kind": "chat_export_thread",
            "resourceId": str(chats.id),
            "snapshotIds": [str(snap.id)],
            "itemIds": [str(item.id)],
        },
        chats.id,
        [chats.id],
    )
    job = Job(
        id=uuid.uuid4(), tenant_id=TENANT_A, resource_id=chats.id, type=JobType.EXPORT,
        status=JobStatus.RUNNING, spec=spec, result={},
    )
    sess = _FakeSession(
        resources=[parent, chats], snapshots=[snap], items=[item], jobs=[job],
        tenants=[_tenant(TENANT_A, ORG_A)],
    )
    r = _client(sess, _token(email=VICTIM)).get(f"/api/v1/jobs/export/{job.id}/status")
    assert r.status_code == 200, r.text
    assert "SnapshotItem" not in sess.queried


def test_unstamped_job_still_rewalks_and_still_denies():
    """Pre-fix rows carry no stamp: the full walk stays, so nothing that was
    protected becomes readable."""
    parent = _entra_user(VICTIM)
    drive = _child(parent, ResourceType.ONEDRIVE)
    snap = _snapshot(drive)
    item = _item(snap)
    mine = _entra_user(ATTACKER)
    my_drive = _child(mine, ResourceType.ONEDRIVE)
    job = Job(
        id=uuid.uuid4(), tenant_id=TENANT_A, resource_id=my_drive.id, type=JobType.EXPORT,
        status=JobStatus.RUNNING,
        spec={"restore_type": "EXPORT_ZIP", "item_ids": [str(item.id)]},
        result={},
    )
    sess = _FakeSession(
        resources=[parent, drive, mine, my_drive], snapshots=[snap], items=[item], jobs=[job],
        tenants=[_tenant(TENANT_A, ORG_A)],
    )
    r = _client(sess, _token()).get(f"/api/v1/jobs/export/{job.id}/status")
    assert r.status_code == 403, r.text
    assert "SnapshotItem" in sess.queried


def test_stamp_that_does_not_match_the_specs_scope_is_ignored():
    """A stamp is an optimization, never a shrink: if the spec's ids are not
    the ones that were authorized, the full walk runs again and the smuggled
    item is caught."""
    parent = _entra_user(VICTIM)
    drive = _child(parent, ResourceType.ONEDRIVE)
    snap = _snapshot(drive)
    item = _item(snap)
    mine = _entra_user(ATTACKER)
    my_drive = _child(mine, ResourceType.ONEDRIVE)
    my_snap = _snapshot(my_drive)
    # Stamp computed over the caller's own scope, then the victim's item id
    # bolted onto the spec afterwards.
    spec = _stamped_spec(
        {"restore_type": "EXPORT_ZIP", "snapshot_ids": [str(my_snap.id)]}, my_drive.id, [my_drive.id]
    )
    spec["item_ids"] = [str(item.id)]
    job = Job(
        id=uuid.uuid4(), tenant_id=TENANT_A, resource_id=my_drive.id, type=JobType.EXPORT,
        status=JobStatus.RUNNING, spec=spec, result={},
    )
    sess = _FakeSession(
        resources=[parent, drive, mine, my_drive], snapshots=[snap, my_snap], items=[item],
        jobs=[job], tenants=[_tenant(TENANT_A, ORG_A)],
    )
    r = _client(sess, _token()).get(f"/api/v1/jobs/export/{job.id}/download")
    assert r.status_code == 403, r.text
    assert "SnapshotItem" in sess.queried


def test_malformed_stamp_falls_back_to_the_full_walk():
    parent = _entra_user(VICTIM)
    drive = _child(parent, ResourceType.ONEDRIVE)
    snap = _snapshot(drive)
    job = Job(
        id=uuid.uuid4(), tenant_id=TENANT_A, resource_id=None, type=JobType.EXPORT,
        status=JobStatus.RUNNING,
        spec={
            "restore_type": "EXPORT_ZIP",
            "snapshot_ids": [str(snap.id)],
            _authz.AUTHORIZED_SCOPE_KEY: {"v": 1, "resource_ids": ["not-a-uuid"], "scope_sha": "x"},
        },
        result={},
    )
    sess = _FakeSession(
        resources=[parent, drive], snapshots=[snap], jobs=[job], tenants=[_tenant(TENANT_A, ORG_A)]
    )
    r = _client(sess, _token()).get(f"/api/v1/jobs/export/{job.id}/status")
    assert r.status_code == 403, r.text
    assert "Snapshot" in sess.queried


def test_stamped_job_resource_id_is_always_in_the_check():
    """`Job.resource_id` is server-derived, so it is unioned into the stamped
    set — a stamp can never drop it."""
    parent = _entra_user(VICTIM)
    drive = _child(parent, ResourceType.ONEDRIVE)
    snap = _snapshot(drive)
    mine = _entra_user(ATTACKER)
    my_drive = _child(mine, ResourceType.ONEDRIVE)
    spec = _stamped_spec(
        {"restore_type": "EXPORT_ZIP", "snapshot_ids": [str(snap.id)]}, drive.id, [my_drive.id]
    )
    job = Job(
        id=uuid.uuid4(), tenant_id=TENANT_A, resource_id=drive.id, type=JobType.EXPORT,
        status=JobStatus.RUNNING, spec=spec, result={},
    )
    sess = _FakeSession(
        resources=[parent, drive, mine, my_drive], snapshots=[snap], jobs=[job],
        tenants=[_tenant(TENANT_A, ORG_A)],
    )
    r = _client(sess, _token()).get(f"/api/v1/jobs/export/{job.id}/status")
    assert r.status_code == 403, r.text
