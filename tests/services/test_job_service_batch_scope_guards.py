"""Regressions for the batch trigger path's scope handling.

Two bugs (found 2026-08-04 review), both in
``_create_batch_backup_jobs`` (services/job-service/main.py):

1. The 30s batch debounce matched ONLY (tenant, source='manual_bulk',
   IN_PROGRESS, <30s) — it never compared scope. Every single-user
   "Backup now" also lands here with source='manual_bulk', so clicking
   user A then user B within 30s debounced B against A's batch: via
   trigger-bulk B was never backed up; via /backups/trigger the deduped
   dict has jobId=None so the endpoint fell through to the LEGACY
   single-resource publish, backing up only B's ENTRA_USER profile row
   while mail/OneDrive/calendar/contacts/chats were silently skipped —
   and the API returned 200 QUEUED either way.

2. Every non-Tier-2 resource in scope (SHAREPOINT_SITE, M365_GROUP,
   AZURE_*, ...) was fed to classify_scope, which defers anything
   without child resources. discovery-worker skips non-ENTRA_USER ids
   without writing an outcome, so those batch_pending_users rows sat
   WAITING_DISCOVERY until the watchdog flipped them DISCOVERY_FAILED
   at deadline — pinning the Activity row "In Progress" for an hour
   after all jobs completed.

Contracts pinned here:
  - The debounce query keys on the scope signature (set-equality on
    scope_user_ids), not just the tenant.
  - Only ENTRA_USER resources get batch_pending_users rows / chained
    discovery.tier2 publishes; sites/groups/etc. never do.
  - A debounced (jobId=None) result from the batch path must NOT fall
    through to the legacy single publish in /backups/trigger: the caller
    is pointed at the in-flight batch's job, or gets an explicit 409.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import uuid

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_SVC_DIR = _REPO_ROOT / "services" / "job-service"
if str(_SVC_DIR) not in sys.path:
    sys.path.insert(0, str(_SVC_DIR))

_spec = importlib.util.spec_from_file_location(
    "js_main_scope_under_test", _SVC_DIR / "main.py"
)
_js = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["js_main_scope_under_test"] = _js
assert _spec and _spec.loader
_spec.loader.exec_module(_js)  # type: ignore[union-attr]

from shared.models import Resource, ResourceStatus, ResourceType, SlaPolicy  # noqa: E402
from shared.schemas import TriggerBackupRequest  # noqa: E402

TENANT = uuid.uuid4()


class _Result:
    def __init__(self, rows=()):
        self._rows = list(rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return list(self._rows)

    def scalars(self):
        return self

    def scalar_one_or_none(self):
        return self._rows[0] if self._rows else None


class _Nested:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeSession:
    """Answers the text() SQL _create_batch_backup_jobs issues, recording
    every (sql, params) pair so tests can assert on the behavior."""

    def __init__(self, *, debounce_rows=(), orm_rows=()):
        self.calls = []           # (sql, params)
        self.added = []           # ORM objects
        self.debounce_rows = list(debounce_rows)
        self.orm_rows = list(orm_rows)

    def begin_nested(self):
        return _Nested()

    def add(self, obj):
        self.added.append(obj)

    async def commit(self):
        pass

    async def flush(self):
        pass

    async def rollback(self):
        pass

    async def execute(self, stmt, params=None):
        if not hasattr(stmt, "column_descriptions"):
            sql = str(stmt)
            self.calls.append((sql, params or {}))
            if "INTERVAL '30 seconds'" in sql:
                return _Result(self.debounce_rows)
            if "SELECT DISTINCT parent_resource_id" in sql:
                return _Result([])  # nobody has Tier-2 children yet
            if "FROM snapshots s" in sql:
                return _Result([])  # G1 dedup: nothing in flight
            return _Result([])
        # ORM select — only /backups/trigger's resource fetch lands here.
        return _Result(self.orm_rows)

    def sqls(self, fragment):
        return [(s, p) for s, p in self.calls if fragment in s]


def _resource(rtype, rid=None):
    return Resource(
        id=rid or uuid.uuid4(),
        tenant_id=TENANT,
        type=rtype,
        external_id=f"ext-{uuid.uuid4()}",
        display_name="r",
        status=ResourceStatus.DISCOVERED,
        sla_policy_id=uuid.uuid4(),
        extra_data={},
        last_backup_at=None,
    )


@pytest.fixture()
def publishes(monkeypatch):
    sent = []

    async def _publish(routing_key, msg, priority=0):
        sent.append((routing_key, msg))

    async def _emit(**kwargs):
        pass

    monkeypatch.setattr(_js.message_bus, "publish", _publish)
    monkeypatch.setattr(_js, "emit_backup_triggered", _emit)
    return sent


# ------------------------------------------------- finding 2: deferral scope


def test_only_entra_users_are_deferred(publishes):
    user = _resource(ResourceType.ENTRA_USER)
    site = _resource(ResourceType.SHAREPOINT_SITE)
    sess = _FakeSession(orm_rows=[
        SlaPolicy(id=user.sla_policy_id, backup_entra_id=True),
        SlaPolicy(id=site.sla_policy_id, backup_sharepoint=True),
    ])

    jobs = asyncio.run(
        _js._create_batch_backup_jobs(
            resources_map={str(user.id): user, str(site.id): site},
            db=sess,
            batch_id=str(uuid.uuid4()),
        )
    )

    pending = sess.sqls("INSERT INTO batch_pending_users")
    assert [p["uid"] for _, p in pending] == [str(user.id)], (
        "only the ENTRA_USER may get a WAITING_DISCOVERY row — a deferred "
        "site wedges the batch for DISCOVERY_DEADLINE_MIN"
    )

    tier2_msgs = [m for k, m in publishes if k == "discovery.tier2"]
    assert len(tier2_msgs) == 1
    assert tier2_msgs[0]["userResourceIds"] == [str(user.id)]

    # The site is still backed up directly by the fan-out.
    fanned_out = set()
    for j in sess.added:
        fanned_out.update(str(r) for r in (j.batch_resource_ids or []))
    assert str(site.id) in fanned_out
    assert str(user.id) in fanned_out
    assert jobs, "healthy path must still create jobs"


# ---------------------------------------------- finding 1: debounce signature


def test_debounce_query_carries_the_scope_signature(publishes):
    user = _resource(ResourceType.ENTRA_USER)
    site = _resource(ResourceType.SHAREPOINT_SITE)
    child = _resource(ResourceType.USER_ONEDRIVE)
    archive = _resource(ResourceType.USER_ARCHIVE)
    sess = _FakeSession()

    asyncio.run(
        _js._create_batch_backup_jobs(
            resources_map={
                str(user.id): user,
                str(site.id): site,
                str(child.id): child,
                str(archive.id): archive,
            },
            db=sess,
            batch_id=str(uuid.uuid4()),
        )
    )

    (sql, params), = sess.sqls("INTERVAL '30 seconds'")
    # Set-equality on scope_user_ids: a batch only debounces an identical
    # request, never a different user's click in the same tenant.
    assert "scope_user_ids @>" in sql
    assert "scope_user_ids <@" in sql
    # Tier-2 children are not part of the operator's scope signature.
    assert set(params["scope"]) == {str(user.id), str(site.id)}


def test_identical_scope_within_window_is_debounced(publishes):
    user = _resource(ResourceType.ENTRA_USER)
    existing_bid = str(uuid.uuid4())
    sess = _FakeSession(debounce_rows=[(existing_bid, None)])

    jobs = asyncio.run(
        _js._create_batch_backup_jobs(
            resources_map={str(user.id): user},
            db=sess,
            batch_id=str(uuid.uuid4()),
        )
    )

    assert jobs[0]["deduped"] is True
    assert jobs[0]["duplicateBatchId"] == existing_bid
    assert jobs[0]["jobId"] is None
    assert sess.added == [], "a debounced trigger must not create jobs"
    assert sess.sqls("INSERT INTO backup_batches") == []


# ------------------------------- finding 1: no fallthrough to legacy publish


def _run_trigger(sess, resource, deduped, monkeypatch):
    async def _routed(res, db, **kwargs):
        return [deduped]

    async def _identity(db, res):
        return res

    monkeypatch.setattr(_js, "_route_single_trigger", _routed)
    monkeypatch.setattr(_js, "_redirect_teams_chat_to_export", _identity)
    req = TriggerBackupRequest(resourceId=str(resource.id))
    return asyncio.run(_js.trigger_backup(req, sess))


def test_deduped_single_trigger_points_at_existing_batch(monkeypatch, publishes):
    user = _resource(ResourceType.ENTRA_USER)
    inflight_job = str(uuid.uuid4())
    sess = _FakeSession(orm_rows=[user])
    deduped = {
        "jobId": None,
        "deduped": True,
        "duplicateBatchId": str(uuid.uuid4()),
        "message": "dup",
    }

    # _deduped_pollable_job_id's jobs lookup returns the in-flight job.
    orig_execute = sess.execute

    async def _execute(stmt, params=None):
        if not hasattr(stmt, "column_descriptions") and "SELECT id::text FROM jobs" in str(stmt):
            sess.calls.append((str(stmt), params or {}))
            return _Result([(inflight_job,)])
        return await orig_execute(stmt, params)

    sess.execute = _execute

    resp = _run_trigger(sess, user, deduped, monkeypatch)
    assert resp.id == inflight_job, "caller must poll the existing batch's job"
    assert sess.added == [], (
        "the deduped path must NOT fall through to the legacy publish — "
        "that queues a profile-only backup and skips all Tier-2 content"
    )
    assert publishes == []


def test_deduped_single_trigger_409s_when_batch_has_no_job(monkeypatch, publishes):
    user = _resource(ResourceType.ENTRA_USER)
    sess = _FakeSession(orm_rows=[user])
    deduped = {
        "jobId": None,
        "deduped": True,
        "duplicateBatchId": str(uuid.uuid4()),
        "message": "dup",
    }

    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc:
        _run_trigger(sess, user, deduped, monkeypatch)
    assert exc.value.status_code == 409
    assert sess.added == []
    assert publishes == []
