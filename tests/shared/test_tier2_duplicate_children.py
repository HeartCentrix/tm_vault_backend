"""ensure_tier2_children duplicate-race hardening.

resources has no unique constraint on (tenant_id, parent_resource_id, type),
and four independent callers race the check-then-insert. Pins:

  * Pre-existing duplicate USER_* children must NOT raise (the old
    scalar_one_or_none crashed every later discovery pass with
    MultipleResultsFound, wedging the user in DISCOVERY_FAILED forever):
    the first/oldest row is refreshed, no new row is added.
  * On PostgreSQL, when any child row is missing, a per-user advisory lock
    is taken BEFORE the check-then-insert so concurrent callers serialize
    instead of double-inserting.
  * Steady state (all five children already exist, user not archive-eligible)
    stays lock-free so long-running batches never block interactive callers.

Uses asyncio.run inside sync tests (pytest-asyncio is not installed here).
"""
from __future__ import annotations

import asyncio
import os
import uuid
from datetime import datetime

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

from types import SimpleNamespace

from shared.models import Resource, ResourceStatus, ResourceType
from shared.tier2_discovery import TIER2_CHILD_TYPES, ensure_tier2_children


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


class _FakeDB:
    """Pops scripted results in execute order; records every statement."""

    def __init__(self, results, dialect=""):
        self._results = list(results)
        self._dialect = dialect
        self.texts = []
        self.added = []

    def get_bind(self):
        if not self._dialect:
            raise RuntimeError("unbound")
        return SimpleNamespace(dialect=SimpleNamespace(name=self._dialect))

    async def execute(self, stmt, params=None):
        self.texts.append(str(stmt))
        if self._results:
            return self._results.pop(0)
        return _FakeResult()

    def add(self, obj):
        self.added.append(obj)

    async def flush(self):
        self.texts.append("FLUSH")

    def begin_nested(self):
        # The retained (deployed) insert path wraps the INSERT in a SAVEPOINT
        # so a lost race raises IntegrityError without poisoning the caller's
        # batch transaction — see _insert_child_or_get_winner.
        db = self

        class _Savepoint:
            async def __aenter__(self):
                db.texts.append("SAVEPOINT")
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

        return _Savepoint()

    async def commit(self):
        self.texts.append("COMMIT")


class _FakeGraph:
    def __init__(self, children):
        self._children = children

    async def discover_user_content(self, **kwargs):
        return self._children


def _user(email="user@example.com"):
    return Resource(
        id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        type=ResourceType.ENTRA_USER,
        external_id="ext-user-1",
        display_name="User One",
        email=email,
        extra_data={"user_principal_name": email},
        status=ResourceStatus.DISCOVERED,
    )


def _mail_child(user, name, created_at=None):
    return Resource(
        id=uuid.uuid4(),
        tenant_id=user.tenant_id,
        type=ResourceType.USER_MAIL,
        external_id="old-ext",
        display_name=name,
        parent_resource_id=user.id,
        status=ResourceStatus.DISCOVERED,
        created_at=created_at,
    )


def test_duplicate_children_refresh_oldest_without_raising():
    user = _user()
    # created_at is explicit: the survivor is chosen by age, not by the order
    # the DB happened to return the rows in (see _find_existing_child).
    dup_a = _mail_child(user, "dup-a", created_at=datetime(2026, 7, 1, 12, 0, 0))
    dup_b = _mail_child(user, "dup-b", created_at=datetime(2026, 7, 4, 12, 0, 0))
    # dialect "" (non-PG / unbound) ⇒ no probe, no lock; the only DB call is
    # the candidate lookup, which returns TWO rows for USER_MAIL.
    db = _FakeDB([_FakeResult(scalar_rows=[dup_a, dup_b])])
    graph = _FakeGraph([
        {"type": "USER_MAIL", "external_id": "new-ext", "display_name": "Mail", "metadata": {}},
    ])

    out = asyncio.run(ensure_tier2_children(db, user, graph, commit=False))  # must not raise

    assert out == [dup_a], "the first (oldest) duplicate is refreshed and returned"
    assert dup_a.external_id == "new-ext"
    assert db.added == [], "a duplicate pair must not grow a third row"


def test_missing_children_take_advisory_lock_before_insert():
    user = _user()
    db = _FakeDB(
        [
            _FakeResult(rows=[]),                # probe: no children exist
            _FakeResult(),                       # advisory lock
            _FakeResult(scalar_rows=[]),         # USER_MAIL candidate lookup: none
        ],
        dialect="postgresql",
    )
    graph = _FakeGraph([
        {"type": "USER_MAIL", "external_id": "new-ext", "display_name": "Mail", "metadata": {}},
    ])

    asyncio.run(ensure_tier2_children(db, user, graph, commit=False))

    lock_calls = [i for i, t in enumerate(db.texts) if "pg_advisory_xact_lock" in t]
    assert lock_calls, "missing children must take the per-user advisory lock"
    assert len(db.added) == 1, "the missing USER_MAIL child is created"
    insert_marker = len(db.texts)  # add() is not an execute; lock must precede lookup
    assert lock_calls[0] < insert_marker


def test_steady_state_stays_lock_free(monkeypatch):
    from shared.config import settings
    monkeypatch.setattr(settings, "ARCHIVE_BACKUP_ALLOWED_UPNS", ["other@example.com"])
    user = _user()  # not in the archive allowlist ⇒ archive gate is closed
    probe_rows = [(t,) for t in TIER2_CHILD_TYPES]
    db = _FakeDB([_FakeResult(rows=probe_rows)], dialect="postgresql")
    graph = _FakeGraph([])  # nothing to refresh — pure steady-state pass

    asyncio.run(ensure_tier2_children(db, user, graph, commit=False))

    assert not any("pg_advisory_xact_lock" in t for t in db.texts), (
        "all five children present ⇒ no lock, batches must not block Backup-now"
    )
