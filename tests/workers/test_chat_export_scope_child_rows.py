"""chat-export-worker scope.resolve(): attachment / hosted-content scoping.

resolve() filtered MESSAGES by snapshot + resource, but matched
CHAT_ATTACHMENT / CHAT_HOSTED_CONTENT rows on item_type +
parent_external_id across the ENTIRE snapshot_items table — no snapshot
filter, no tenant filter. Two consequences:

  * duplicates: chat pointer rows are re-captured on every incremental and
    carried forward (backup-worker _carry_forward_prior_chat_snapshot_items),
    so one logical attachment exists once per snapshot that ever held its
    message — the export ZIP got N copies of the same file.
  * bleed: Teams message ids are creation-timestamp strings that recur
    across chats and tenants, so another tenant's / another resource's
    attachment row could be pulled into an export.

The fake session below evaluates the real SQLAlchemy criteria the function
builds against in-memory rows, so these tests exercise the actual queries
and the actual att_map / hosted_map construction.
"""
from __future__ import annotations

import importlib.util
import logging
import os
import pathlib
import sys

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

_SCOPE_PATH = _REPO_ROOT / "workers" / "chat-export-worker" / "scope.py"
_spec = importlib.util.spec_from_file_location("chat_export_scope_under_test", _SCOPE_PATH)
_scope = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["chat_export_scope_under_test"] = _scope
try:
    _spec.loader.exec_module(_spec and _scope)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"chat-export-worker scope module failed to import: {exc}",
        allow_module_level=True,
    )

import asyncio  # noqa: E402

from shared.models import Resource, Snapshot, SnapshotItem  # noqa: E402
from sqlalchemy.sql.elements import BooleanClauseList  # noqa: E402
from sqlalchemy.sql.elements import TextClause  # noqa: E402


# ── in-memory rows ────────────────────────────────────────────────────────


class _Item:
    """SnapshotItem stand-in. `snapshot_resource_id` models the Snapshot
    JOIN target (snapshot_items has no resource_id column), and
    `snapshot_resource_tenant_id` models Resource.tenant_id reached through
    that snapshot — the fallback tenancy for child rows whose own
    tenant_id is NULL (every CHAT_HOSTED_CONTENT row the drain writes)."""

    def __init__(self, **kw):
        self.id = kw.get("id")
        self.snapshot_id = kw.get("snapshot_id")
        self.snapshot_resource_id = kw.get("snapshot_resource_id")
        self.snapshot_resource_tenant_id = kw.get("snapshot_resource_tenant_id")
        self.tenant_id = kw.get("tenant_id")
        self.external_id = kw.get("external_id")
        self.parent_external_id = kw.get("parent_external_id")
        self.item_type = kw.get("item_type")
        self.name = kw.get("name")
        self.folder_path = kw.get("folder_path")
        self.blob_path = kw.get("blob_path")
        self.content_size = kw.get("content_size", 0)
        self.extra_data = kw.get("extra_data") or {}
        self.created_at = kw.get("created_at", 0)


# ── criteria evaluator ────────────────────────────────────────────────────


def _column_value(col, row):
    table = getattr(col, "table", None)
    if getattr(table, "name", "") == "snapshot_items":
        return getattr(row, col.key)
    # aliased(Snapshot) side of the JOIN (anonymous alias name)
    if col.key == "resource_id":
        return row.snapshot_resource_id
    if col.key == "id":
        return row.snapshot_id
    raise AssertionError(f"unhandled snapshot column {col.key}")


def _eval(clause, row) -> bool:
    if clause is None:
        return True
    if isinstance(clause, BooleanClauseList):
        return all(_eval(c, row) for c in clause.clauses)
    op = getattr(clause.operator, "__name__", str(clause.operator))
    left = _column_value(clause.left, row)
    right = clause.right
    if op == "in_op":
        values = right.value if hasattr(right, "value") else right
        return left in list(values)
    if op == "eq":
        return left == (right.value if hasattr(right, "value") else right)
    raise AssertionError(f"unhandled operator {op}")


class _Result:
    def __init__(self, rows):
        self._rows = list(rows)

    def __iter__(self):
        return iter(self._rows)

    def scalars(self):
        return iter(self._rows)

    def all(self):
        return list(self._rows)


def _apply_order_by(stmt, rows):
    """Honour the statement's real ORDER BY. Dedupe now depends on row order
    (newest copy wins), so a harness that returned insertion order would let
    an order_by regression pass unnoticed."""
    for clause in reversed(list(stmt._order_by_clauses)):
        element = getattr(clause, "element", clause)
        descending = getattr(clause.modifier, "__name__", "") == "desc_op"
        key = element.key
        rows = sorted(rows, key=lambda r: getattr(r, key), reverse=descending)
    return rows


def _in_values(clause):
    """Values of the single `col IN (...)` predicate in `clause`."""
    clauses = clause.clauses if isinstance(clause, BooleanClauseList) else [clause]
    for c in clauses:
        if getattr(c.operator, "__name__", "") == "in_op":
            right = c.right
            return list(right.value if hasattr(right, "value") else right)
    raise AssertionError(f"no IN predicate in {clause}")


class _FakeSession:
    """Applies the statement's real WHERE criteria to in-memory rows."""

    def __init__(self, items, child_ids=()):
        self.items = list(items)
        self.child_ids = list(child_ids)
        self.statements: list = []

    async def execute(self, stmt, params=None):
        if isinstance(stmt, TextClause):
            return _Result([])  # no chat_thread_messages hydration rows
        self.statements.append(stmt)
        entity = stmt.column_descriptions[0]["entity"]
        if entity is Resource:
            return _Result([(cid,) for cid in self.child_ids])
        if entity is Snapshot:
            # select(Snapshot.id, Resource.tenant_id) JOIN Resource — the
            # tenancy fallback for child rows with a NULL tenant_id.
            wanted = _in_values(stmt.whereclause)
            out: dict = {}
            for i in self.items:
                if i.snapshot_id in wanted and i.snapshot_id not in out:
                    out[i.snapshot_id] = i.snapshot_resource_tenant_id
            return _Result(list(out.items()))
        assert entity is SnapshotItem, entity
        rows = [i for i in self.items if _eval(stmt.whereclause, i)]
        return _Result(_apply_order_by(stmt, rows))


# ── fixture data ──────────────────────────────────────────────────────────

R1 = "resource-1"
T1 = "tenant-1"
T2 = "tenant-2"
S_NEW = "snap-new"
S_OLD = "snap-old"
THREAD = "Chat with Bob"


def _msg(external_id, snapshot_id=S_NEW):
    return _Item(
        id=f"item-{external_id}-{snapshot_id}",
        snapshot_id=snapshot_id,
        snapshot_resource_id=R1,
        snapshot_resource_tenant_id=T1,
        tenant_id=T1,
        external_id=external_id,
        parent_external_id="chat-1",
        item_type="TEAMS_CHAT_MESSAGE",
        name=external_id,
        folder_path=THREAD,
    )


def _child(item_type, parent, ext, *, snapshot_id=S_NEW, tenant_id=T1,
           resource_id=R1, resource_tenant_id=None, name="report.pdf",
           blob="blob/ok", created_at=0):
    return _Item(
        id=f"child-{ext}-{snapshot_id}-{tenant_id}",
        snapshot_id=snapshot_id,
        snapshot_resource_id=resource_id,
        snapshot_resource_tenant_id=(
            resource_tenant_id if resource_tenant_id is not None
            else (tenant_id if tenant_id is not None else T1)
        ),
        tenant_id=tenant_id,
        external_id=ext,
        parent_external_id=parent,
        item_type=item_type,
        name=name,
        folder_path=THREAD,
        blob_path=blob,
        content_size=10,
        extra_data={"content_type": "application/pdf", "name": name},
        created_at=created_at,
    )


def _resolve(items, snapshot_ids=(S_NEW,)):
    sess = _FakeSession(items)
    return asyncio.run(
        _scope.resolve(
            sess,
            resource_id=R1,
            snapshot_ids=list(snapshot_ids),
            thread_path=THREAD,
            item_ids=[],
        )
    )


def test_attachment_carried_forward_copies_are_deduped():
    """The same attachment kept in an older snapshot must not produce a
    second ZIP entry."""
    items = [
        _msg("m1"),
        _child("CHAT_ATTACHMENT", "m1", "m1::a1", snapshot_id=S_NEW, created_at=2),
        _child("CHAT_ATTACHMENT", "m1", "m1::a1", snapshot_id=S_OLD, created_at=1),
    ]
    scope = _resolve(items)
    assert len(scope.attachment_map["m1"]) == 1, scope.attachment_map["m1"]


def test_attachment_from_another_tenant_is_not_exported():
    """Teams message ids repeat across tenants — a foreign-tenant row must
    never enter the export."""
    items = [
        _msg("m1"),
        _child("CHAT_ATTACHMENT", "m1", "m1::a1", snapshot_id=S_NEW),
        _child(
            "CHAT_ATTACHMENT", "m1", "m1::evil",
            snapshot_id="snap-other-tenant", tenant_id=T2,
            resource_id="resource-9", name="other-tenant-secret.pdf",
            blob="blob/foreign",
        ),
    ]
    scope = _resolve(items)
    names = [a["name"] for a in scope.attachment_map["m1"]]
    assert "other-tenant-secret.pdf" not in names, names
    assert names == ["report.pdf"], names


def test_hosted_content_is_scoped_and_deduped():
    items = [
        _msg("m1"),
        _child("CHAT_HOSTED_CONTENT", "m1", "m1:hc1", snapshot_id=S_NEW, created_at=2),
        _child("CHAT_HOSTED_CONTENT", "m1", "m1:hc1", snapshot_id=S_OLD, created_at=1),
        _child(
            "CHAT_HOSTED_CONTENT", "m1", "m1:hc9",
            snapshot_id="snap-other-tenant", tenant_id=T2,
            resource_id="resource-9", blob="blob/foreign",
        ),
    ]
    scope = _resolve(items)
    hosted = scope.hosted_map["m1"]
    assert [h["hc_id"] for h in hosted] == ["hc1"], hosted


def test_distinct_attachments_on_one_message_all_kept():
    """Dedupe must be per logical attachment, not per message."""
    items = [
        _msg("m1"),
        _child("CHAT_ATTACHMENT", "m1", "m1::a1", name="one.pdf"),
        _child("CHAT_ATTACHMENT", "m1", "m1::a2", name="two.pdf"),
    ]
    scope = _resolve(items)
    assert sorted(a["name"] for a in scope.attachment_map["m1"]) == ["one.pdf", "two.pdf"]


def test_dedupe_keeps_the_copy_that_has_a_blob():
    """When an export spans two snapshots, the surviving copy must be one
    with content — a pointer-only capture would export as .MISSING.txt."""
    items = [
        _msg("m1", snapshot_id=S_OLD),
        _msg("m1", snapshot_id=S_NEW),
        _child("CHAT_ATTACHMENT", "m1", "m1::a1", snapshot_id=S_OLD, blob=None, created_at=1),
        _child("CHAT_ATTACHMENT", "m1", "m1::a1", snapshot_id=S_NEW, blob="blob/ok", created_at=2),
    ]
    scope = _resolve(items, snapshot_ids=(S_OLD, S_NEW))
    atts = scope.attachment_map["m1"]
    assert len(atts) == 1, atts
    assert atts[0]["blob_path"] == "blob/ok", atts


def test_attachments_for_other_messages_untouched():
    """Scoping must not drop a sibling message's own attachment."""
    items = [
        _msg("m1"),
        _msg("m2"),
        _child("CHAT_ATTACHMENT", "m1", "m1::a1", name="one.pdf"),
        _child("CHAT_ATTACHMENT", "m2", "m2::a1", name="two.pdf"),
    ]
    scope = _resolve(items)
    assert [a["name"] for a in scope.attachment_map["m1"]] == ["one.pdf"]
    assert [a["name"] for a in scope.attachment_map["m2"]] == ["two.pdf"]


# ── deployed data shapes the scoping must tolerate ────────────────────────


def test_hosted_content_with_null_tenant_id_is_kept():
    """`_userchats_backup_hosted_contents` (backup-worker/main.py:10120)
    never passes tenant_id, carry-forward copies src.tenant_id, and P8's
    back-fill filters on `src.tenant_id = :tid` — so EVERY
    CHAT_HOSTED_CONTENT row in the deployed DB has tenant_id NULL. A plain
    `tenant_id IN (...)` scope drops every inline image from every export,
    silently (the packager emits no entry and no .MISSING.txt for a hosted
    id it never sees)."""
    items = [
        _msg("m1"),
        _child("CHAT_HOSTED_CONTENT", "m1", "m1:hc1", tenant_id=None),
    ]
    scope = _resolve(items)
    assert [h["hc_id"] for h in scope.hosted_map.get("m1", [])] == ["hc1"], scope.hosted_map


def test_attachment_in_first_drainers_snapshot_is_kept():
    """Shared chats persist CHAT_ATTACHMENT rows in the FIRST drainer's
    snapshot only — claim-skipped users get the message pointer row but the
    attachment row lives under another user's snapshot/resource
    (backup-worker/main.py:1230-1237; snapshot-service/main.py:3158-3170
    scopes by tenant for exactly this reason). Scoping children to the
    exporting resource's snapshots loses attachments that were exported
    before the fix."""
    items = [
        _msg("m1", snapshot_id=S_NEW),
        _child(
            "CHAT_ATTACHMENT", "m1", "m1::a1",
            snapshot_id="snap-first-drainer", resource_id="resource-amit",
            tenant_id=T1, name="shared.pdf",
        ),
    ]
    scope = _resolve(items)
    assert [a["name"] for a in scope.attachment_map.get("m1", [])] == ["shared.pdf"], \
        scope.attachment_map


def test_null_tenant_child_from_another_tenant_is_still_excluded():
    """Tolerating NULL tenant_id must not become "allow anything": a
    hosted-content row with no tenant_id whose snapshot belongs to another
    tenant's resource is resolved through snapshot -> resource -> tenant
    and excluded."""
    items = [
        _msg("m1"),
        _child("CHAT_HOSTED_CONTENT", "m1", "m1:hc1", tenant_id=None),
        _child(
            "CHAT_HOSTED_CONTENT", "m1", "m1:evil",
            snapshot_id="snap-other-tenant", resource_id="resource-9",
            tenant_id=None, resource_tenant_id=T2, blob="blob/foreign",
        ),
    ]
    scope = _resolve(items)
    assert [h["hc_id"] for h in scope.hosted_map["m1"]] == ["hc1"], scope.hosted_map


def test_excluded_child_rows_are_logged_not_silently_dropped(caplog):
    """A child row that matches a message id but is scoped out must leave a
    trace. Silence is the failure mode that hid the hosted-content loss."""
    items = [
        _msg("m1"),
        _child("CHAT_ATTACHMENT", "m1", "m1::a1"),
        _child(
            "CHAT_ATTACHMENT", "m1", "m1::evil",
            snapshot_id="snap-other-tenant", tenant_id=T2,
            resource_id="resource-9", name="other-tenant-secret.pdf",
        ),
    ]
    with caplog.at_level(logging.WARNING, logger="chat-export.scope"):
        scope = _resolve(items)
    assert [a["name"] for a in scope.attachment_map["m1"]] == ["report.pdf"]
    assert any(
        "excluded" in rec.getMessage() and "CHAT_ATTACHMENT" in rec.getMessage()
        for rec in caplog.records
    ), [r.getMessage() for r in caplog.records]


def test_dedupe_prefers_the_newest_copy():
    """Retention / blob reap walks the OLDEST snapshots first, so when
    several copies all have a blob the newest one is the safest bet."""
    items = [
        _msg("m1"),
        _child("CHAT_ATTACHMENT", "m1", "m1::a1", snapshot_id=S_OLD,
               blob="blob/old", created_at=1),
        _child("CHAT_ATTACHMENT", "m1", "m1::a1", snapshot_id=S_NEW,
               blob="blob/new", created_at=2),
    ]
    scope = _resolve(items)
    atts = scope.attachment_map["m1"]
    assert len(atts) == 1, atts
    assert atts[0]["blob_path"] == "blob/new", atts
