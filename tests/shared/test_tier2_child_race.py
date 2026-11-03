"""ensure_tier2_children must survive the four-caller race on USER_* children.

There is no unique constraint on resources(tenant_id, type,
parent_resource_id) yet, and FOUR callers can run this code concurrently for
the same user (tenant-service /discover-content, tenant-service backup-now,
the discovery-worker discovery.tier2 consumer, the resource-service SLA
hook). Duplicate children therefore exist in prod, and the existing-child
lookup used `scalar_one_or_none()` — which raises MultipleResultsFound the
moment a duplicate exists, permanently poisoning that user's Tier-2
discovery (today's chats heal path runs exactly this code).

Requirements pinned here:
  * duplicates never raise — a deterministic survivor (oldest) is refreshed
  * duplicates are NEVER deleted (they may own snapshots — backup product)
  * with the unique index applied, the loser of an insert race re-reads the
    winner's row and refreshes it instead of exploding the caller's
    transaction
"""
from __future__ import annotations

import asyncio
import os
import uuid
from datetime import datetime, timedelta

import pytest
from sqlalchemy.exc import IntegrityError, MultipleResultsFound

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

from shared.models import Resource, ResourceStatus, ResourceType
from shared.tier2_discovery import ensure_tier2_children


GRAPH_CHILD = {
    "type": "USER_MAIL",
    "external_id": "mail-ext-1",
    "display_name": "Mail",
    "email": "amit@taylormorrison.com",
    "metadata": {"mailbox_id": "MBX-1"},
}
# Cursor the backup-worker owns on the child row.
WORKER_META = {"mail_delta_token": "mdt-77"}


class _FakeScalars:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return list(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None


class _FakeResult:
    """Mirrors AsyncResult semantics for the shapes this code path uses."""

    def __init__(self, rows):
        self._rows = list(rows)

    def scalars(self):
        return _FakeScalars(self._rows)

    def scalar_one_or_none(self):
        if len(self._rows) > 1:
            raise MultipleResultsFound(
                "Multiple rows were found when one or none was required"
            )
        return self._rows[0] if self._rows else None


class _Savepoint:
    def __init__(self, db):
        self._db = db

    async def __aenter__(self):
        self._db.savepoints += 1
        self._db._snapshot = list(self._db.added)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is not None:
            # Rolling back to the savepoint expunges rows added inside it,
            # exactly like SQLAlchemy does.
            self._db.added = list(self._db._snapshot)
            self._db.rolled_back_savepoints += 1
        return False


class _FakeDB:
    """Stub AsyncSession. `rows` is what the child lookup returns; after a
    failed flush it returns `rows_after_conflict` (the winner's row)."""

    def __init__(self, rows=(), rows_after_conflict=None, flush_raises=False):
        self._rows = list(rows)
        self._rows_after_conflict = rows_after_conflict
        self.added = []
        self.deleted = []
        self.committed = False
        self.flushes = 0
        self.savepoints = 0
        self.rolled_back_savepoints = 0
        self._snapshot = []
        self._flush_raises = flush_raises

    async def execute(self, stmt):
        return _FakeResult(self._rows)

    def add(self, obj):
        self.added.append(obj)

    def delete(self, obj):
        self.deleted.append(obj)

    async def flush(self):
        self.flushes += 1
        if self._flush_raises:
            self._flush_raises = False
            if self._rows_after_conflict is not None:
                self._rows = list(self._rows_after_conflict)
            raise IntegrityError(
                "INSERT INTO resources ...",
                {},
                Exception(
                    'duplicate key value violates unique constraint '
                    '"uq_resource_parent_type"'
                ),
            )

    def begin_nested(self):
        return _Savepoint(self)

    async def commit(self):
        self.committed = True


class _FakeGraph:
    async def discover_user_content(self, **kwargs):
        return [dict(GRAPH_CHILD)]


@pytest.fixture(autouse=True)
def _no_archive_probe(monkeypatch):
    """Keep the archive branch out of the way — it has its own test module."""
    from shared import config
    monkeypatch.setattr(config.settings, "ARCHIVE_BACKUP_ENABLED", False)


def _user() -> Resource:
    return Resource(
        id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        type=ResourceType.ENTRA_USER,
        external_id="entra-user-amit",
        display_name="Amit",
        email="amit@taylormorrison.com",
        extra_data={"user_principal_name": "amit@taylormorrison.com"},
        sla_policy_id=uuid.uuid4(),
        status=ResourceStatus.DISCOVERED,
    )


def _child(user: Resource, *, created_at, external_id="mail-ext-1", extra=None) -> Resource:
    return Resource(
        id=uuid.uuid4(),
        tenant_id=user.tenant_id,
        type=ResourceType.USER_MAIL,
        external_id=external_id,
        display_name="stale",
        email=None,
        extra_data=dict(extra or WORKER_META),
        parent_resource_id=user.id,
        sla_policy_id=user.sla_policy_id,
        status=ResourceStatus.DISCOVERED,
        created_at=created_at,
    )


def test_duplicate_children_do_not_raise_and_oldest_survives():
    user = _user()
    base = datetime(2026, 7, 1, 12, 0, 0)
    oldest = _child(user, created_at=base)
    newer = _child(user, created_at=base + timedelta(days=3), extra={"mail_delta_token": "dup"})
    db = _FakeDB(rows=[oldest, newer])

    out = asyncio.run(ensure_tier2_children(db, user, _FakeGraph(), commit=False))

    mail = [r for r in out if r.type == ResourceType.USER_MAIL]
    assert len(mail) == 1, f"expected one USER_MAIL child back, got {len(mail)}"
    assert mail[0] is oldest, "survivor must be deterministic (oldest row)"
    assert mail[0].display_name == "Mail"
    assert mail[0].extra_data["mail_delta_token"] == "mdt-77", "worker cursor must survive"
    assert mail[0].extra_data["mailbox_id"] == "MBX-1"
    assert db.added == [], "must not add another duplicate"


def test_duplicate_children_are_never_deleted():
    user = _user()
    base = datetime(2026, 7, 1, 12, 0, 0)
    oldest = _child(user, created_at=base)
    newer = _child(user, created_at=base + timedelta(days=3))
    db = _FakeDB(rows=[oldest, newer])

    asyncio.run(ensure_tier2_children(db, user, _FakeGraph(), commit=False))

    assert db.deleted == [], (
        "duplicate child rows may own snapshots — discovery must never delete them"
    )
    assert newer.display_name == "stale", "the non-survivor must be left untouched"


def test_survivor_choice_is_independent_of_db_row_order():
    user = _user()
    base = datetime(2026, 7, 1, 12, 0, 0)
    oldest = _child(user, created_at=base)
    newer = _child(user, created_at=base + timedelta(days=3))
    db = _FakeDB(rows=[newer, oldest])  # reversed

    out = asyncio.run(ensure_tier2_children(db, user, _FakeGraph(), commit=False))

    mail = [r for r in out if r.type == ResourceType.USER_MAIL]
    assert mail[0] is oldest, (
        "two concurrent callers must converge on the same survivor regardless "
        "of the order the DB happens to return rows in"
    )


def test_insert_race_loser_rereads_winner_instead_of_raising():
    """With the unique index applied, a concurrent caller can win the INSERT.
    The loser must adopt the winner's row, not blow up the transaction."""
    user = _user()
    winner = _child(user, created_at=datetime(2026, 7, 30, 9, 0, 0))
    db = _FakeDB(rows=[], rows_after_conflict=[winner], flush_raises=True)

    out = asyncio.run(ensure_tier2_children(db, user, _FakeGraph(), commit=False))

    mail = [r for r in out if r.type == ResourceType.USER_MAIL]
    assert len(mail) == 1
    assert mail[0] is winner, "must return the row the race winner committed"
    assert mail[0].display_name == "Mail", "winner's row must still be refreshed in place"
    assert mail[0].extra_data["mail_delta_token"] == "mdt-77"
    assert db.rolled_back_savepoints == 1, (
        "the failed INSERT must be contained in a SAVEPOINT so the caller's "
        "transaction stays usable"
    )
    assert winner not in db.added


def test_first_discovery_still_inserts_the_child():
    user = _user()
    db = _FakeDB(rows=[])

    out = asyncio.run(ensure_tier2_children(db, user, _FakeGraph(), commit=False))

    mail = [r for r in out if r.type == ResourceType.USER_MAIL]
    assert len(mail) == 1
    assert db.added == mail, "a brand-new child must still be inserted"
    assert mail[0].external_id == "mail-ext-1"
    assert mail[0].extra_data == {"mailbox_id": "MBX-1"}
    assert mail[0].status == ResourceStatus.DISCOVERED
