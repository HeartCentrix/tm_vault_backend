"""Regression: the endpoint that actually serves export bytes had NO auth.

Bug (found 2026-07-31, adversarial review of c8fa014):
`GET /api/v1/jobs/export/{job_id}/download` in services/job-service/main.py
took `(job_id, db)` and nothing else — no verified JWT, no tenant check. It
resolved the container from `Job.result.container` / `Job.tenant_id` and
streamed the blob to whoever asked. api-gateway proxies the route with no
dependency of its own and validates no token, so the route was reachable
unauthenticated from the ingress. Worse, chat-export-worker deliberately
publishes that URL as `Job.result.signed_url`
(workers/chat-export-worker/consumers/thread.py:281), and job ids are
enumerable via `GET /api/v1/jobs`. Tightening the *trigger* path in
chat_export.py therefore closed nothing: the ZIP itself was still public.

`GET /api/v1/jobs/export/{job_id}/status` is the same shape (no auth) and
leaks job existence/progress for any id, so it gets the same gate.

Contract pinned here:
  - No bearer token                                   -> 401.
  - Job in a tenant the caller is not entitled to     -> 403.
  - Export job over another user's resource           -> 403.
  - Export job over the caller's own resource         -> passes the gate.
  - Caller holding a documented export-operator role  -> passes the gate for
    someone else's export inside an entitled tenant.
  - A job nothing can be resolved for (tenant_id NULL, or its snapshots
    retention-pruned) -> 403 for ordinary users.

Ownership comes from the job's resource, resolved from the database — NOT from
`spec.userId`, which a caller could write (round-2 finding: `trigger_export`
was anonymous and merged the request body into the spec). And there is no
"generic export job" exemption: the generic EXPORT_ZIP artifact contains Teams
message bodies too. See tests/services/test_export_scope_authz.py for the
forgery and second-door cases.

Authorization must also run BEFORE the readiness check, so probing a
foreign job id cannot tell "not ready" from "not yours".

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

_spec = importlib.util.spec_from_file_location("js_main_authz_under_test", _SVC_DIR / "main.py")
_js = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["js_main_authz_under_test"] = _js
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
    Tenant,
)
from shared.security import get_current_user_from_token  # noqa: E402

ORG_A = uuid.UUID("00000000-0000-0000-0000-0000000000a0")
ORG_B = uuid.UUID("00000000-0000-0000-0000-0000000000b0")
TENANT_A = uuid.UUID("00000000-0000-0000-0000-0000000000a1")
TENANT_B = uuid.UUID("00000000-0000-0000-0000-0000000000b1")

VICTIM = "victim@taylormorrison.com"
CALLER = "caller@taylormorrison.com"

CHAT_KIND = "chat_export_thread"


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
    def __init__(self, *, jobs=(), tenants=(), users=(), resources=(), snapshots=()):
        self.jobs = {j.id: j for j in jobs}
        self.tenants = list(tenants)
        self.users = {u.id: u for u in users}
        self.resources = {r.id: r for r in resources}
        self.snapshots = {s.id: s for s in snapshots}

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

    async def get(self, entity, ident):
        if entity is Job:
            return self.jobs.get(ident)
        return None

    async def execute(self, stmt):
        entity = stmt.column_descriptions[0]["entity"]
        binds = self._binds(stmt)
        if entity is Job:
            return _FakeResult([self.jobs[b] for b in binds if b in self.jobs])
        if entity is Resource:
            return _FakeResult([self.resources[b] for b in binds if b in self.resources])
        if entity is Snapshot:
            return _FakeResult([self.snapshots[b] for b in binds if b in self.snapshots])
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


def _chats_world(email, tenant_id=TENANT_A):
    """ENTRA_USER + its USER_CHATS child + one snapshot of the chats."""
    parent = _entra_user(email, tenant_id)
    chats = _user_chats(parent)
    snap = Snapshot(id=uuid.uuid4(), resource_id=chats.id)
    return parent, chats, snap


# A caller who clears the gate on a job that is not COMPLETED gets the
# handler's 409 "Export not ready" — which proves authorization passed
# without needing Azure storage configured in the test environment.
_NOT_READY = 409


def _chat_job(tenant_id, owner_sub, status=JobStatus.COMPLETED, *, resource=None, snapshot=None) -> Job:
    """A chat export job as `chat_export.trigger` writes it. `spec.userId` is
    kept in the fixture on purpose: it must NOT be what authorizes access."""
    return Job(
        id=uuid.uuid4(),
        tenant_id=tenant_id,
        resource_id=resource.id if resource is not None else None,
        type=JobType.EXPORT,
        status=status,
        spec={
            "kind": CHAT_KIND,
            "userId": str(owner_sub),
            "snapshotIds": [str(snapshot.id)] if snapshot is not None else [],
        },
        result={},
    )


def _generic_export_job(tenant_id, status=JobStatus.RUNNING, *, resource=None, snapshot=None) -> Job:
    """Recovery/mailbox/OneDrive export as `trigger_export` writes it: no
    owner in the spec at all, so the job's resource is the only owner signal."""
    return Job(
        id=uuid.uuid4(),
        tenant_id=tenant_id,
        resource_id=resource.id if resource is not None else None,
        type=JobType.EXPORT,
        status=status,
        spec={
            "restore_type": "EXPORT_ZIP",
            "snapshot_ids": [str(snapshot.id)] if snapshot is not None else [],
            "item_ids": [],
        },
        result={},
    )


def _token(*, tenant_ids=(), roles=("USER",), sub=None, email=CALLER):
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


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    _js.app.dependency_overrides.clear()


# ------------------------------------------------------------------ no token


def test_export_download_requires_a_bearer_token():
    """THE leak: unauthenticated GET streamed the ZIP."""
    job = _chat_job(TENANT_A, uuid.uuid4())
    sess = _FakeSession(jobs=[job], tenants=[_tenant(TENANT_A, ORG_A)])
    r = _client(sess).get(f"/api/v1/jobs/export/{job.id}/download")
    assert r.status_code == 401


def test_export_status_requires_a_bearer_token():
    job = _chat_job(TENANT_A, uuid.uuid4())
    sess = _FakeSession(jobs=[job], tenants=[_tenant(TENANT_A, ORG_A)])
    r = _client(sess).get(f"/api/v1/jobs/export/{job.id}/status")
    assert r.status_code == 401


# ------------------------------------------------------------- cross tenant


def test_export_download_rejects_unentitled_tenant():
    job = _chat_job(TENANT_B, uuid.uuid4())
    sess = _FakeSession(jobs=[job], tenants=[_tenant(TENANT_A, ORG_A), _tenant(TENANT_B, ORG_B)])
    user = _token(tenant_ids=[TENANT_A])
    r = _client(sess, user).get(f"/api/v1/jobs/export/{job.id}/download")
    assert r.status_code == 403


def test_export_status_rejects_unentitled_tenant():
    job = _chat_job(TENANT_B, uuid.uuid4())
    sess = _FakeSession(jobs=[job], tenants=[_tenant(TENANT_A, ORG_A), _tenant(TENANT_B, ORG_B)])
    user = _token(tenant_ids=[TENANT_A])
    r = _client(sess, user).get(f"/api/v1/jobs/export/{job.id}/status")
    assert r.status_code == 403


def test_empty_tenant_claim_fails_closed_on_download():
    """Pre-fix tokens carry no tenantIds and there is no PlatformUser row to
    re-derive from — deny, never "skip the check"."""
    job = _chat_job(TENANT_A, uuid.uuid4())
    sess = _FakeSession(jobs=[job], tenants=[_tenant(TENANT_A, ORG_A)])
    r = _client(sess, _token()).get(f"/api/v1/jobs/export/{job.id}/download")
    assert r.status_code == 403


def test_authorization_runs_before_the_readiness_check():
    """A foreign job that is still RUNNING must 403, not 409 — otherwise the
    response distinguishes "not ready" from "not yours"."""
    job = _chat_job(TENANT_B, uuid.uuid4(), status=JobStatus.RUNNING)
    sess = _FakeSession(jobs=[job], tenants=[_tenant(TENANT_A, ORG_A), _tenant(TENANT_B, ORG_B)])
    user = _token(tenant_ids=[TENANT_A])
    r = _client(sess, user).get(f"/api/v1/jobs/export/{job.id}/download")
    assert r.status_code == 403


# -------------------------------------------------------- chat-export owner


def test_other_users_chat_export_is_not_downloadable_same_tenant():
    """The reported breach, at the endpoint that actually serves the bytes."""
    parent, chats, snap = _chats_world(VICTIM)
    job = _chat_job(TENANT_A, uuid.uuid4(), resource=chats, snapshot=snap)
    sess = _FakeSession(
        jobs=[job], tenants=[_tenant(TENANT_A, ORG_A)],
        resources=[parent, chats], snapshots=[snap],
    )
    user = _token(tenant_ids=[TENANT_A])
    r = _client(sess, user).get(f"/api/v1/jobs/export/{job.id}/download")
    assert r.status_code == 403


def test_chat_export_owner_passes_the_gate():
    """Owner = the token email matches the exported resource's UPN. The
    caller's own sub in `spec.userId` is not what lets them through."""
    sub = uuid.uuid4()
    parent, chats, snap = _chats_world(VICTIM)
    job = _chat_job(TENANT_A, sub, status=JobStatus.RUNNING, resource=chats, snapshot=snap)
    sess = _FakeSession(
        jobs=[job], tenants=[_tenant(TENANT_A, ORG_A)],
        resources=[parent, chats], snapshots=[snap],
    )
    user = _token(tenant_ids=[TENANT_A], sub=sub, email=VICTIM)
    r = _client(sess, user).get(f"/api/v1/jobs/export/{job.id}/download")
    assert r.status_code == _NOT_READY  # past the gate, into the real handler


def test_spec_user_id_alone_does_not_pass_the_gate():
    """Round-2 finding: `spec.userId` is caller-writable, so a job stamped with
    the caller's own sub over someone else's resource must still 403."""
    sub = uuid.uuid4()
    parent, chats, snap = _chats_world(VICTIM)
    job = _chat_job(TENANT_A, sub, status=JobStatus.RUNNING, resource=chats, snapshot=snap)
    sess = _FakeSession(
        jobs=[job], tenants=[_tenant(TENANT_A, ORG_A)],
        resources=[parent, chats], snapshots=[snap],
    )
    user = _token(tenant_ids=[TENANT_A], sub=sub, email=CALLER)
    r = _client(sess, user).get(f"/api/v1/jobs/export/{job.id}/download")
    assert r.status_code == 403


def test_export_operator_role_may_download_another_users_chat_export():
    """The operator path: RESTORE_OPERATOR is the least-privileged existing
    role for "export someone else's backed-up data" and is grantable with one
    INSERT into user_roles (see services/job-service/export_authz.py)."""
    parent, chats, snap = _chats_world(VICTIM)
    job = _chat_job(TENANT_A, uuid.uuid4(), status=JobStatus.RUNNING, resource=chats, snapshot=snap)
    sess = _FakeSession(
        jobs=[job], tenants=[_tenant(TENANT_A, ORG_A)],
        resources=[parent, chats], snapshots=[snap],
    )
    user = _token(tenant_ids=[TENANT_A], roles=["RESTORE_OPERATOR"])
    r = _client(sess, user).get(f"/api/v1/jobs/export/{job.id}/download")
    assert r.status_code == _NOT_READY  # past the gate, into the real handler


def test_operator_role_does_not_grant_cross_tenant_download():
    job = _chat_job(TENANT_B, uuid.uuid4())
    sess = _FakeSession(jobs=[job], tenants=[_tenant(TENANT_A, ORG_A), _tenant(TENANT_B, ORG_B)])
    user = _token(tenant_ids=[TENANT_A], roles=["RESTORE_OPERATOR"])
    r = _client(sess, user).get(f"/api/v1/jobs/export/{job.id}/download")
    assert r.status_code == 403


# ------------------------------------------------------- generic export jobs


def test_generic_export_download_is_not_exempt_from_the_owner_rule():
    """Round-2 BLOCKER: the "generic export job" exemption made tenant
    membership the whole gate, and restore-worker packages Teams message
    bodies into the generic EXPORT_ZIP artifact — a second door into exactly
    the data the owner rule protects."""
    parent, chats, snap = _chats_world(VICTIM)
    job = _generic_export_job(TENANT_A, resource=chats, snapshot=snap)
    sess = _FakeSession(
        jobs=[job], tenants=[_tenant(TENANT_A, ORG_A)],
        resources=[parent, chats], snapshots=[snap],
    )
    user = _token(tenant_ids=[TENANT_A])
    r = _client(sess, user).get(f"/api/v1/jobs/export/{job.id}/download")
    assert r.status_code == 403


def test_generic_export_download_still_works_for_its_owner():
    """Self-service Recovery must not be collateral damage."""
    parent, chats, snap = _chats_world(VICTIM)
    job = _generic_export_job(TENANT_A, resource=chats, snapshot=snap)
    sess = _FakeSession(
        jobs=[job], tenants=[_tenant(TENANT_A, ORG_A)],
        resources=[parent, chats], snapshots=[snap],
    )
    user = _token(tenant_ids=[TENANT_A], email=VICTIM)
    r = _client(sess, user).get(f"/api/v1/jobs/export/{job.id}/download")
    assert r.status_code == _NOT_READY


def test_generic_export_download_still_tenant_scoped():
    job = _generic_export_job(TENANT_B)
    sess = _FakeSession(jobs=[job], tenants=[_tenant(TENANT_A, ORG_A), _tenant(TENANT_B, ORG_B)])
    user = _token(tenant_ids=[TENANT_A])
    r = _client(sess, user).get(f"/api/v1/jobs/export/{job.id}/download")
    assert r.status_code == 403


# ------------------------------------------------------------------- lookups


def test_unknown_job_still_404():
    sess = _FakeSession(tenants=[_tenant(TENANT_A, ORG_A)])
    user = _token(tenant_ids=[TENANT_A])
    r = _client(sess, user).get(f"/api/v1/jobs/export/{uuid.uuid4()}/download")
    assert r.status_code == 404


def test_export_job_with_no_tenant_fails_closed():
    """`trigger_export` writes `tenant_id=None` when it cannot walk
    snapshot -> resource. Such a row is unattributable, so the download must
    deny rather than serve: this is a backup product, failing loudly beats
    handing over bytes we cannot scope. Operators recover the artifact with the
    SUPER_ADMIN break-glass (which logs) or by fixing the job's tenant_id."""
    job = _generic_export_job(TENANT_A)
    job.tenant_id = None
    sess = _FakeSession(jobs=[job], tenants=[_tenant(TENANT_A, ORG_A)])
    user = _token(tenant_ids=[TENANT_A])
    r = _client(sess, user).get(f"/api/v1/jobs/export/{job.id}/download")
    assert r.status_code == 403
