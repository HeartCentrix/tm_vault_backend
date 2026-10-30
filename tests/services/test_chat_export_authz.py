"""Regression: chat-export authorization was vacuous (privacy breach).

Bug (found 2026-07-31, code review):
`chat_export._ensure_ownership` only rejected when
`user_tenant is not None and res.tenant_id != user_tenant`, and carried NO
per-resource ownership check at all. Two consequences:

  1. `tenantIds` is empty in every real production token (auth-service
     derived it from `PlatformUser.tenant_id`, a column nothing ever
     writes), so `user_tenant` was always None and the gate rejected
     nothing whatsoever.
  2. Even with a populated claim, membership of the *tenant* was the only
     test — so any logged-in user could export and download any OTHER
     user's Teams chats from the same tenant.

Contract pinned here:
  - Missing subject                                    -> 401.
  - No establishable tenant entitlement                -> 403 (never
    "skip the check").
  - Resource in a tenant the caller is not entitled to  -> 403.
  - Resource owned by a different M365 user             -> 403 for non-admins.
  - Owner (token email matches the resource's / its parent's UPN) -> allowed.
  - TENANT_ADMIN/ORG_ADMIN -> allowed inside their entitled tenants only.
  - SUPER_ADMIN -> cross-tenant break-glass retained.
  - Empty claim + resolvable org membership in the DB -> allowed
    (pre-fix tokens still in flight must not lock the pilot out).

Job-scoped routes (status/SSE, cancel) get the same treatment via
`_ensure_job_access` — the status snapshot hands back the signed download
URL, so a vacuous check there is the actual exfiltration path.

Async entry points are driven with `asyncio.run` (repo convention — the test
venv has no pytest-asyncio plugin despite `asyncio_mode = auto`).
"""
from __future__ import annotations

import asyncio
import importlib.util
import inspect
import os
import pathlib
import sys
import uuid

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

import pytest
from fastapi import HTTPException
from sqlalchemy.sql import visitors

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# job-service imports sibling modules relative to its own dir.
_SVC_DIR = _REPO_ROOT / "services" / "job-service"
if str(_SVC_DIR) not in sys.path:
    sys.path.insert(0, str(_SVC_DIR))

_spec = importlib.util.spec_from_file_location(
    "chat_export_under_test", _SVC_DIR / "chat_export.py"
)
_ce = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["chat_export_under_test"] = _ce
assert _spec and _spec.loader
_spec.loader.exec_module(_ce)  # type: ignore[union-attr]

from shared.models import (  # noqa: E402
    Job,
    JobStatus,
    JobType,
    PlatformUser,
    Resource,
    ResourceStatus,
    ResourceType,
    Tenant,
)

ORG_A = uuid.UUID("00000000-0000-0000-0000-0000000000a0")
ORG_B = uuid.UUID("00000000-0000-0000-0000-0000000000b0")
TENANT_A = uuid.UUID("00000000-0000-0000-0000-0000000000a1")
TENANT_B = uuid.UUID("00000000-0000-0000-0000-0000000000b1")

VICTIM = "victim@taylormorrison.com"
ATTACKER = "attacker@taylormorrison.com"


class _FakeResult:
    def __init__(self, rows):
        self._rows = list(rows)

    def scalar_one_or_none(self):
        return self._rows[0] if self._rows else None

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)

    def __iter__(self):
        return iter(self._rows)


class _FakeSession:
    """Stub AsyncSession that answers by selected entity + bind values.

    Deliberately agnostic about *which* queries the code under test runs, so
    the same fake serves both the pre-fix and post-fix implementations.
    """

    def __init__(self, *, resources=(), tenants=(), users=(), jobs=()):
        self.resources = {r.id: r for r in resources}
        self.tenants = list(tenants)
        self.users = {u.id: u for u in users}
        self.jobs = {j.id: j for j in jobs}

    @staticmethod
    def _binds(stmt):
        """Flattened bind values, so `col.in_([...])` reads like `col ==`.

        The gate resolves a whole scope in one query per entity now, so the
        binds arrive as lists.
        """
        vals = []
        visitors.traverse(stmt, {}, {"bindparam": lambda b: vals.append(b.value)})
        flat = []
        for v in vals:
            if isinstance(v, (list, tuple, set)):
                flat.extend(v)
            else:
                flat.append(v)
        return flat

    async def execute(self, stmt):
        entity = stmt.column_descriptions[0]["entity"]
        binds = self._binds(stmt)
        if entity is Resource:
            return _FakeResult([self.resources[b] for b in binds if b in self.resources])
        if entity is PlatformUser:
            return _FakeResult([self.users[b] for b in binds if b in self.users])
        if entity is Job:
            return _FakeResult([self.jobs[b] for b in binds if b in self.jobs])
        if entity is Tenant:
            # Entitlement lookup: filtered by tenant id or by org id.
            return _FakeResult(
                [
                    t.id
                    for t in self.tenants
                    if t.archived_at is None and (t.id in binds or t.org_id in binds)
                ]
            )
        raise AssertionError(f"unexpected query entity {entity!r}")


def _tenant_a() -> Tenant:
    return Tenant(id=TENANT_A, org_id=ORG_A, display_name="A")


def _tenant_b() -> Tenant:
    return Tenant(id=TENANT_B, org_id=ORG_B, display_name="B")


def _entra_user(email: str, tenant_id: uuid.UUID) -> Resource:
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
    """USER_CHATS Tier-2 child as discovery writes it (email = owner UPN)."""
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


def _token(email: str, *, tenant_ids=(), roles=("USER",), sub=None) -> dict:
    return {
        "id": str(sub or uuid.uuid4()),
        "email": email,
        "roles": list(roles),
        "org_id": str(ORG_A),
        "tenant_ids": [str(t) for t in tenant_ids],
    }


def _own(sess, user, resource_id):
    return asyncio.run(_ce._ensure_ownership(sess, user, resource_id))


def _job_access(sess, user, job):
    return asyncio.run(_ce._ensure_job_access(sess, user, job))


# ---------------------------------------------------------------- resources


def test_other_users_chats_are_not_exportable_same_tenant():
    """THE breach: an attacker holding a correctly-scoped tenant claim could
    export a colleague's Teams chats, because only the tenant was checked."""
    parent = _entra_user(VICTIM, TENANT_A)
    chats = _user_chats(parent)
    sess = _FakeSession(resources=[parent, chats], tenants=[_tenant_a()])
    with pytest.raises(HTTPException) as exc:
        _own(sess, _token(ATTACKER, tenant_ids=[TENANT_A]), str(chats.id))
    assert exc.value.status_code == 403


def test_empty_tenant_claim_does_not_bypass_the_gate():
    """Production reality pre-fix: tenantIds is always []. An entitlement we
    cannot establish must fail closed, not skip the comparison."""
    parent = _entra_user(VICTIM, TENANT_A)
    chats = _user_chats(parent)
    # No PlatformUser row for this subject -> nothing to derive entitlement from.
    sess = _FakeSession(resources=[parent, chats], tenants=[_tenant_a()])
    with pytest.raises(HTTPException) as exc:
        _own(sess, _token(ATTACKER), str(chats.id))
    assert exc.value.status_code == 403


def test_resource_in_unentitled_tenant_rejected():
    parent = _entra_user(VICTIM, TENANT_B)
    chats = _user_chats(parent)
    sess = _FakeSession(resources=[parent, chats], tenants=[_tenant_a(), _tenant_b()])
    with pytest.raises(HTTPException) as exc:
        _own(sess, _token(VICTIM, tenant_ids=[TENANT_A]), str(chats.id))
    assert exc.value.status_code == 403


def test_tenant_admin_cannot_reach_another_orgs_tenant():
    parent = _entra_user(VICTIM, TENANT_B)
    chats = _user_chats(parent)
    sess = _FakeSession(resources=[parent, chats], tenants=[_tenant_a(), _tenant_b()])
    with pytest.raises(HTTPException) as exc:
        _own(
            sess,
            _token(ATTACKER, tenant_ids=[TENANT_A], roles=["TENANT_ADMIN"]),
            str(chats.id),
        )
    assert exc.value.status_code == 403


def test_owner_may_export_own_chats():
    parent = _entra_user(VICTIM, TENANT_A)
    chats = _user_chats(parent)
    sess = _FakeSession(resources=[parent, chats], tenants=[_tenant_a()])
    assert _own(sess, _token(VICTIM, tenant_ids=[TENANT_A]), str(chats.id)).id == chats.id


def test_owner_may_export_via_parent_entra_user_id():
    """Frontend sends either the parent ENTRA_USER id or the USER_CHATS id."""
    parent = _entra_user(VICTIM, TENANT_A)
    sess = _FakeSession(resources=[parent], tenants=[_tenant_a()])
    assert _own(sess, _token(VICTIM, tenant_ids=[TENANT_A]), str(parent.id)).id == parent.id


def test_owner_match_is_case_insensitive():
    parent = _entra_user(VICTIM, TENANT_A)
    chats = _user_chats(parent)
    sess = _FakeSession(resources=[parent, chats], tenants=[_tenant_a()])
    user = _token(VICTIM.upper(), tenant_ids=[TENANT_A])
    assert _own(sess, user, str(chats.id)).id == chats.id


def test_tenant_admin_may_export_inside_entitled_tenant():
    parent = _entra_user(VICTIM, TENANT_A)
    chats = _user_chats(parent)
    sess = _FakeSession(resources=[parent, chats], tenants=[_tenant_a()])
    user = _token(ATTACKER, tenant_ids=[TENANT_A], roles=["TENANT_ADMIN"])
    assert _own(sess, user, str(chats.id)).id == chats.id


def test_super_admin_keeps_cross_tenant_break_glass():
    parent = _entra_user(VICTIM, TENANT_B)
    chats = _user_chats(parent)
    sess = _FakeSession(resources=[parent, chats], tenants=[_tenant_a(), _tenant_b()])
    user = _token(ATTACKER, tenant_ids=[TENANT_A], roles=["SUPER_ADMIN"])
    assert _own(sess, user, str(chats.id)).id == chats.id


def test_empty_claim_falls_back_to_db_org_membership():
    """Pilot safety: access tokens minted before the claim fix carry no
    tenantIds. Entitlement must be re-derived from real data rather than
    locking the owner out of their own export."""
    sub = uuid.uuid4()
    parent = _entra_user(VICTIM, TENANT_A)
    chats = _user_chats(parent)
    sess = _FakeSession(
        resources=[parent, chats],
        tenants=[_tenant_a()],
        users=[PlatformUser(id=sub, email=VICTIM, name="V", org_id=ORG_A)],
    )
    assert _own(sess, _token(VICTIM, sub=sub), str(chats.id)).id == chats.id


def test_missing_subject_is_401():
    parent = _entra_user(VICTIM, TENANT_A)
    sess = _FakeSession(resources=[parent], tenants=[_tenant_a()])
    user = _token(VICTIM, tenant_ids=[TENANT_A])
    user["id"] = ""
    with pytest.raises(HTTPException) as exc:
        _own(sess, user, str(parent.id))
    assert exc.value.status_code == 401


def test_unknown_resource_still_404():
    sess = _FakeSession(tenants=[_tenant_a()])
    with pytest.raises(HTTPException) as exc:
        _own(sess, _token(VICTIM, tenant_ids=[TENANT_A]), str(uuid.uuid4()))
    assert exc.value.status_code == 404


def test_malformed_resource_id_still_400():
    sess = _FakeSession(tenants=[_tenant_a()])
    with pytest.raises(HTTPException) as exc:
        _own(sess, _token(VICTIM, tenant_ids=[TENANT_A]), "not-a-uuid")
    assert exc.value.status_code == 400


# --------------------------------------------------------------------- jobs


def _job(tenant_id: uuid.UUID, owner_sub: str, *, resource: Resource = None) -> Job:
    """`spec.userId` is kept in the fixture deliberately: it is caller-writable
    (`main.trigger_export` merged the request body into the spec), so it must
    not be what authorizes the read. The job's resource is."""
    return Job(
        id=uuid.uuid4(),
        tenant_id=tenant_id,
        resource_id=resource.id if resource is not None else None,
        type=JobType.EXPORT,
        status=JobStatus.COMPLETED,
        spec={"kind": _ce.KIND, "userId": owner_sub, "userEmail": VICTIM},
        result={"signed_url": "https://blob.example/secret.zip"},
    )


def test_other_users_export_job_is_not_readable():
    """The status snapshot returns the signed download URL — this is the
    exfiltration path, so job access needs the same per-user check."""
    job = _job(TENANT_A, str(uuid.uuid4()))
    sess = _FakeSession(jobs=[job], tenants=[_tenant_a()])
    with pytest.raises(HTTPException) as exc:
        _job_access(sess, _token(ATTACKER, tenant_ids=[TENANT_A]), job)
    assert exc.value.status_code == 403


def test_job_owner_may_read_own_export():
    owner_sub = uuid.uuid4()
    parent = _entra_user(VICTIM, TENANT_A)
    chats = _user_chats(parent)
    job = _job(TENANT_A, str(owner_sub), resource=chats)
    sess = _FakeSession(resources=[parent, chats], jobs=[job], tenants=[_tenant_a()])
    _job_access(sess, _token(VICTIM, tenant_ids=[TENANT_A], sub=owner_sub), job)


def test_spec_user_id_does_not_authorize_someone_elses_export():
    """The forgeable field: same sub in spec.userId, but the job's resource
    belongs to the victim."""
    attacker_sub = uuid.uuid4()
    parent = _entra_user(VICTIM, TENANT_A)
    chats = _user_chats(parent)
    job = _job(TENANT_A, str(attacker_sub), resource=chats)
    sess = _FakeSession(resources=[parent, chats], jobs=[job], tenants=[_tenant_a()])
    with pytest.raises(HTTPException) as exc:
        _job_access(sess, _token(ATTACKER, tenant_ids=[TENANT_A], sub=attacker_sub), job)
    assert exc.value.status_code == 403


def test_job_empty_tenant_claim_fails_closed():
    job = _job(TENANT_A, str(uuid.uuid4()))
    sess = _FakeSession(jobs=[job], tenants=[_tenant_a()])
    with pytest.raises(HTTPException) as exc:
        _job_access(sess, _token(ATTACKER), job)
    assert exc.value.status_code == 403


def test_job_in_unentitled_tenant_rejected():
    owner_sub = uuid.uuid4()
    job = _job(TENANT_B, str(owner_sub))
    sess = _FakeSession(jobs=[job], tenants=[_tenant_a(), _tenant_b()])
    with pytest.raises(HTTPException) as exc:
        _job_access(sess, _token(VICTIM, tenant_ids=[TENANT_A], sub=owner_sub), job)
    assert exc.value.status_code == 403


def test_job_tenant_admin_may_read_inside_entitled_tenant():
    job = _job(TENANT_A, str(uuid.uuid4()))
    sess = _FakeSession(jobs=[job], tenants=[_tenant_a()])
    _job_access(sess, _token(ATTACKER, tenant_ids=[TENANT_A], roles=["TENANT_ADMIN"]), job)


def test_job_without_recorded_owner_fails_closed_for_non_admins():
    """Legacy rows whose spec has no userId are not attributable — refuse
    rather than hand the signed URL to whoever asks."""
    job = _job(TENANT_A, str(uuid.uuid4()))
    job.spec = {"kind": _ce.KIND}
    sess = _FakeSession(jobs=[job], tenants=[_tenant_a()])
    with pytest.raises(HTTPException) as exc:
        _job_access(sess, _token(VICTIM, tenant_ids=[TENANT_A]), job)
    assert exc.value.status_code == 403


def test_force_delete_is_tenant_scoped_not_globally_admin():
    """force_delete queues the export blob for reclamation — an admin of
    another org must not be able to destroy it."""
    src = inspect.getsource(_ce.force_delete)
    assert "_ensure_job_access" in src


def test_trigger_records_the_resources_tenant_not_the_first_claim():
    """Once tenantIds carries every org tenant, `tenant_ids[0]` is no longer
    necessarily the resource's tenant — the job row must be stamped with the
    tenant we actually authorized against, or idempotency/concurrency land in
    the wrong bucket and the owner cannot read their own job back."""
    src = inspect.getsource(_ce.trigger)
    assert "_user_tenant_id(user) or res.tenant_id" not in src
    assert "res.tenant_id" in src
