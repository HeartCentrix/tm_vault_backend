"""_apply_auto_protect_groups must not commit while its stream is open.

The unpolicied-resource sweep iterates an asyncpg server-side cursor
(db.stream, yield_per=1000). A commit ends the transaction that owns the
cursor, so the old mid-flush `await db.commit()` killed the stream after the
first 1000 rows: auto-protect silently truncated, leaving the rest of a new
tenant unprotected — and it prematurely committed the caller's finalize
transaction. The FakeDB below enforces the asyncpg constraint: any commit
while the stream is open kills the cursor, so the pre-fix code fails this
test the same way production did.

Uses asyncio.run inside sync tests (pytest-asyncio is not installed here).
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import uuid
from types import SimpleNamespace

# ── Set required env vars before any shared.config import ─────────────────
os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "workers" / "discovery-worker"))

_WORKER_PATH = _REPO_ROOT / "workers" / "discovery-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("discovery_worker_main_autoprotect", _WORKER_PATH)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)


class _FakeResult:
    def __init__(self, rows=None, scalar_rows=None):
        self._rows = rows or []
        self._scalar_rows = scalar_rows or []

    def all(self):
        return self._rows

    def scalars(self):
        rows = self._scalar_rows

        class _S:
            def all(self):
                return rows

        return _S()


class _FakeStream:
    """Mimics an asyncpg server-side cursor: dies if the owning transaction
    is committed while it is open."""

    def __init__(self, items):
        self._items = items
        self.open = True
        self.dead = False

    def scalars(self):
        async def _gen():
            for item in self._items:
                if self.dead:
                    raise RuntimeError("server-side cursor closed by commit (asyncpg)")
                yield item

        return _gen()

    async def close(self):
        self.open = False


class _FakeDB:
    def __init__(self, resources, group, assignment, policy_id):
        self._resources = resources
        self._group = group
        self._assignment = assignment
        self._policy_id = policy_id
        self.stream_obj = None
        self.commit_count = 0
        self.update_calls = 0

    async def execute(self, stmt, params=None):
        text = str(stmt)
        # NB: the groups select embeds a group_policy_assignments subquery,
        # so match the outermost FROM first.
        if "FROM sla_policies" in text:
            return _FakeResult(rows=[(self._policy_id,)])
        if "FROM resource_groups" in text:
            return _FakeResult(scalar_rows=[self._group])
        if "FROM group_policy_assignments" in text:
            return _FakeResult(scalar_rows=[self._assignment])
        if "UPDATE resources" in text:
            self.update_calls += 1
            return _FakeResult()
        return _FakeResult()

    async def stream(self, stmt):
        self.stream_obj = _FakeStream(self._resources)
        return self.stream_obj

    async def commit(self):
        self.commit_count += 1
        if self.stream_obj is not None and self.stream_obj.open:
            self.stream_obj.dead = True  # the asyncpg failure mode


def test_auto_protect_assigns_past_first_chunk_without_committing(monkeypatch):
    tenant_id = uuid.uuid4()
    group = SimpleNamespace(id=uuid.uuid4(), priority=1)
    assignment = SimpleNamespace(group_id=group.id, policy_id=uuid.uuid4())
    resources = [SimpleNamespace(id=uuid.uuid4()) for _ in range(2500)]
    db = _FakeDB(resources, group, assignment, policy_id=assignment.policy_id)

    import shared.resource_group_matcher as rgm
    monkeypatch.setattr(rgm, "find_matching_groups", lambda resource, groups: list(groups))

    assigned = asyncio.run(_mod._apply_auto_protect_groups(db, tenant_id))

    # Pre-fix: the flush at 1000 committed, the cursor died, and only ~1000
    # of 2500 resources got a policy (silent partial protection).
    assert assigned == 2500, f"every matching resource must be assigned, got {assigned}"
    assert db.commit_count == 0, "the caller's finalize transaction owns the commit"
    assert db.update_calls >= 1
    assert db.stream_obj is not None and db.stream_obj.open is False, (
        "the server-side cursor must be closed when the sweep ends"
    )
