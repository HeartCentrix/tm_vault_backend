"""Task 1.2: USER_ARCHIVE Tier-2 child creation, allowlist-gated.

Exercises `ensure_tier2_children`'s new archive-probe branch in isolation
from the five fixed USER_MAIL/ONEDRIVE/CONTACTS/CALENDAR/CHATS children
(the FakeGraph's `discover_user_content` always returns [] here, so
raw_children is empty and the only DB SELECT that matters is the
USER_ARCHIVE upsert lookup).

Decisions under test (docs/superpowers/plans/2026-07-20-online-archive-backup.md
Task 1.2 + jrue-only pilot scope):
  - Allowlist check runs BEFORE the Graph probe — a non-allowlisted user
    must not even call get_exchange_settings (zero metered Graph calls).
  - ARCHIVE_BACKUP_ENABLED=false ⇒ no child, no probe.
  - Empty allowlist ⇒ everyone is probed (fleet mode).
  - UPN match is case-insensitive.
  - No archive in the probe payload ⇒ no child.
  - Probe failure (429/5xx/any exception) ⇒ skip quietly this run — no
    child, no exception propagation (create-then-orphan is a prior-defect
    class in this codebase).
"""
from __future__ import annotations

import os
import uuid

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

import pytest

from shared.models import Resource, ResourceStatus, ResourceType
from shared.tier2_discovery import ensure_tier2_children


"""exchangeSettings (Graph beta) is a FLAT object — no nested "archiveMailbox".
primaryMailboxId is always present; inPlaceArchiveMailboxId is present ONLY
when the user has an In-Place Archive. Real example (learn.microsoft.com,
exchangeSettings resource, graph-rest-beta):
    {"@odata.context": "...", "primaryMailboxId": "MBX:e0643f21@a7809c93"}
(archive property absent entirely when there is no archive)."""
ARCHIVE_PAYLOAD = {
    "@odata.context": "https://graph.microsoft.com/beta/$metadata#users('u')/settings/exchange",
    "primaryMailboxId": "MBX:e0643f21@a7809c93",
    "inPlaceArchiveMailboxId": "AAMkArchive123",
}


class _FakeScalars:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return list(self._rows)


class _FakeResult:
    def __init__(self, row):
        self._row = row

    def scalar_one_or_none(self):
        return self._row

    def scalars(self):
        return _FakeScalars([self._row] if self._row is not None else [])


class _NoopSavepoint:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeDB:
    """Stub AsyncSession. `discover_user_content` always returns [] in
    these tests, so the only `execute()` call is the USER_ARCHIVE
    existing-row lookup — always answered with `existing`."""

    def __init__(self, existing=None):
        self._existing = existing
        self.added = []
        self.committed = False

    async def execute(self, stmt):
        return _FakeResult(self._existing)

    def add(self, obj):
        self.added.append(obj)

    def begin_nested(self):
        # New rows are inserted inside a SAVEPOINT so a duplicate-key race
        # can be contained — see tests/shared/test_tier2_child_race.py.
        return _NoopSavepoint()

    async def flush(self):
        pass

    async def commit(self):
        self.committed = True


class _FakeGraph:
    def __init__(self, exchange_settings=None, raise_exc=None):
        self._exchange_settings = exchange_settings
        self._raise_exc = raise_exc
        self.get_exchange_settings_calls = []

    async def discover_user_content(self, **kwargs):
        return []

    async def get_exchange_settings(self, user_id):
        self.get_exchange_settings_calls.append(user_id)
        if self._raise_exc:
            raise self._raise_exc
        return self._exchange_settings


def _user_resource(upn: str = "jrue@taylormorrison.com") -> Resource:
    return Resource(
        id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        type=ResourceType.ENTRA_USER,
        external_id="entra-user-jrue",
        display_name="Jason Rue",
        email=upn,
        extra_data={"user_principal_name": upn},
        sla_policy_id=uuid.uuid4(),
        status=ResourceStatus.DISCOVERED,
    )


def _archive_children(out):
    return [r for r in out if r.type == ResourceType.USER_ARCHIVE]


@pytest.mark.asyncio
async def test_child_created_for_allowlisted_user_with_archive(monkeypatch):
    from shared import config
    monkeypatch.setattr(config.settings, "ARCHIVE_BACKUP_ENABLED", True)
    monkeypatch.setattr(config.settings, "ARCHIVE_BACKUP_ALLOWED_UPNS", ["jrue@taylormorrison.com"])

    user = _user_resource()
    db = _FakeDB(existing=None)
    graph = _FakeGraph(exchange_settings=ARCHIVE_PAYLOAD)

    out = await ensure_tier2_children(db, user, graph, commit=False)

    children = _archive_children(out)
    assert len(children) == 1
    child = children[0]
    assert child.display_name == "Archive — Jason Rue"
    assert child.external_id == "AAMkArchive123"
    assert child.extra_data["archiveMailboxId"] == "AAMkArchive123"
    assert child.extra_data["archive_mailbox_id"] == "AAMkArchive123"
    assert child.extra_data["primary_mailbox_id"] == "MBX:e0643f21@a7809c93"
    assert child.extra_data["user_id"] == user.external_id
    assert graph.get_exchange_settings_calls == ["entra-user-jrue"]
    assert db.added == [child]


@pytest.mark.asyncio
async def test_no_archive_in_payload_means_no_child(monkeypatch):
    from shared import config
    monkeypatch.setattr(config.settings, "ARCHIVE_BACKUP_ENABLED", True)
    monkeypatch.setattr(config.settings, "ARCHIVE_BACKUP_ALLOWED_UPNS", ["jrue@taylormorrison.com"])

    user = _user_resource()
    db = _FakeDB(existing=None)
    # Real no-archive shape: primaryMailboxId present, inPlaceArchiveMailboxId
    # absent entirely. Proves we key off inPlaceArchiveMailboxId specifically
    # and don't mistake primaryMailboxId for an archive id.
    graph = _FakeGraph(exchange_settings={
        "@odata.context": "https://graph.microsoft.com/beta/$metadata#users('u')/settings/exchange",
        "primaryMailboxId": "MBX:e0643f21@a7809c93",
    })

    out = await ensure_tier2_children(db, user, graph, commit=False)

    assert _archive_children(out) == []
    assert graph.get_exchange_settings_calls == ["entra-user-jrue"]
    assert db.added == []


@pytest.mark.asyncio
async def test_archive_backup_disabled_means_no_child_and_no_probe(monkeypatch):
    from shared import config
    monkeypatch.setattr(config.settings, "ARCHIVE_BACKUP_ENABLED", False)
    monkeypatch.setattr(config.settings, "ARCHIVE_BACKUP_ALLOWED_UPNS", ["jrue@taylormorrison.com"])

    user = _user_resource()
    db = _FakeDB(existing=None)
    graph = _FakeGraph(exchange_settings=ARCHIVE_PAYLOAD)

    out = await ensure_tier2_children(db, user, graph, commit=False)

    assert _archive_children(out) == []
    assert graph.get_exchange_settings_calls == []


@pytest.mark.asyncio
async def test_user_not_in_allowlist_means_no_child_and_probe_never_called(monkeypatch):
    from shared import config
    monkeypatch.setattr(config.settings, "ARCHIVE_BACKUP_ENABLED", True)
    monkeypatch.setattr(config.settings, "ARCHIVE_BACKUP_ALLOWED_UPNS", ["jrue@taylormorrison.com"])

    user = _user_resource(upn="someoneelse@taylormorrison.com")
    db = _FakeDB(existing=None)
    graph = _FakeGraph(exchange_settings=ARCHIVE_PAYLOAD)

    out = await ensure_tier2_children(db, user, graph, commit=False)

    assert _archive_children(out) == []
    assert graph.get_exchange_settings_calls == [], (
        "non-allowlisted user must incur ZERO Graph calls (pilot scope)"
    )


@pytest.mark.asyncio
async def test_upn_case_mismatch_still_matches_allowlist(monkeypatch):
    from shared import config
    monkeypatch.setattr(config.settings, "ARCHIVE_BACKUP_ENABLED", True)
    monkeypatch.setattr(config.settings, "ARCHIVE_BACKUP_ALLOWED_UPNS", ["jrue@taylormorrison.com"])

    user = _user_resource(upn="JRue@TaylorMorrison.com")
    db = _FakeDB(existing=None)
    graph = _FakeGraph(exchange_settings=ARCHIVE_PAYLOAD)

    out = await ensure_tier2_children(db, user, graph, commit=False)

    assert len(_archive_children(out)) == 1
    assert graph.get_exchange_settings_calls == ["entra-user-jrue"]


@pytest.mark.asyncio
async def test_empty_allowlist_probes_everyone(monkeypatch):
    from shared import config
    monkeypatch.setattr(config.settings, "ARCHIVE_BACKUP_ENABLED", True)
    monkeypatch.setattr(config.settings, "ARCHIVE_BACKUP_ALLOWED_UPNS", [])

    user = _user_resource(upn="rando@taylormorrison.com")
    db = _FakeDB(existing=None)
    graph = _FakeGraph(exchange_settings=ARCHIVE_PAYLOAD)

    out = await ensure_tier2_children(db, user, graph, commit=False)

    assert len(_archive_children(out)) == 1
    assert graph.get_exchange_settings_calls == ["entra-user-jrue"]


@pytest.mark.asyncio
async def test_probe_failure_skips_quietly_without_raising(monkeypatch):
    from shared import config
    monkeypatch.setattr(config.settings, "ARCHIVE_BACKUP_ENABLED", True)
    monkeypatch.setattr(config.settings, "ARCHIVE_BACKUP_ALLOWED_UPNS", [])

    user = _user_resource()
    db = _FakeDB(existing=None)
    graph = _FakeGraph(raise_exc=RuntimeError("429 Too Many Requests"))

    out = await ensure_tier2_children(db, user, graph, commit=False)  # must not raise

    assert _archive_children(out) == []
    assert db.added == []


@pytest.mark.asyncio
async def test_existing_archive_child_updated_in_place_not_duplicated(monkeypatch):
    from shared import config
    monkeypatch.setattr(config.settings, "ARCHIVE_BACKUP_ENABLED", True)
    monkeypatch.setattr(config.settings, "ARCHIVE_BACKUP_ALLOWED_UPNS", [])

    user = _user_resource()
    existing_child = Resource(
        id=uuid.uuid4(),
        tenant_id=user.tenant_id,
        type=ResourceType.USER_ARCHIVE,
        external_id="old-id",
        display_name="stale",
        extra_data={"archiveMailboxId": "old-id"},
        parent_resource_id=user.id,
        sla_policy_id=user.sla_policy_id,
        status=ResourceStatus.DISCOVERED,
    )
    db = _FakeDB(existing=existing_child)
    graph = _FakeGraph(exchange_settings=ARCHIVE_PAYLOAD)

    out = await ensure_tier2_children(db, user, graph, commit=False)

    children = _archive_children(out)
    assert len(children) == 1
    assert children[0] is existing_child
    assert existing_child.external_id == "AAMkArchive123"
    assert existing_child.display_name == "Archive — Jason Rue"
    assert db.added == [], "must update in place, not insert a duplicate"


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled,allowed,expected_probes", [
    (True, [], 1),
    (True, ["someoneelse@example.com"], 0),
    (False, [], 0),
])
async def test_real_graph_discovery_probes_archive_once_after_gate(monkeypatch, enabled, allowed, expected_probes):
    """Local inline discovery must not bypass the customer gate or double probe."""
    from shared import config, archive_graph
    from shared.graph_client import GraphClient
    monkeypatch.setattr(config.settings, "ARCHIVE_BACKUP_ENABLED", enabled)
    monkeypatch.setattr(config.settings, "ARCHIVE_BACKUP_ALLOWED_UPNS", allowed)
    monkeypatch.setattr(archive_graph, "ARCHIVE_BACKUP_ENABLED", True)
    graph = GraphClient("app", "secret", "tenant")
    probes = []

    async def get(url, params=None):
        if "/settings/exchange" in url:
            probes.append(url)
            return ARCHIVE_PAYLOAD
        if url.endswith("/drive"):
            return {"id": "drive", "quota": {}}
        return {"value": []}

    monkeypatch.setattr(graph, "_get", get)
    user = _user_resource()
    out = await ensure_tier2_children(_FakeDB(), user, graph, commit=False)
    assert len(probes) == expected_probes
    archives = _archive_children(out)
    assert len(archives) == expected_probes
    if archives:
        assert archives[0].extra_data["user_id"] == user.external_id
        assert archives[0].extra_data["archive_mailbox_id"] == "AAMkArchive123"
        assert archives[0].extra_data["archiveMailboxId"] == "AAMkArchive123"
        assert archives[0].extra_data["primary_mailbox_id"] == "MBX:e0643f21@a7809c93"
