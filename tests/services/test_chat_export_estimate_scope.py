"""job-service /exports/chat/estimate must agree with what the worker writes.

`_resolve_counts` feeds `attachmentBytes` / `estimatedZipBytes` and the
soft/hard cap gate (settings.chat_export_size_hard_cap_bytes) that can
outright refuse an export. It summed CHAT_ATTACHMENT / CHAT_HOSTED_CONTENT
bytes on item_type + parent_external_id alone, while the worker
(workers/chat-export-worker/scope.py) tenant-scopes and de-duplicates the
same rows. That divergence lets an inflated estimate hard-block an export
whose ZIP would have been well under cap:

  * carry-forward keeps one logical attachment once per snapshot that ever
    held its message, so N snapshots inflated the estimate N-fold;
  * a foreign tenant's row on a colliding Teams message id inflated it too.

The estimate must use the worker's scoping and dedupe, including tolerating
the NULL tenant_id every CHAT_HOSTED_CONTENT row carries and keeping
attachments that live only in the first drainer's snapshot.

The fake session evaluates the real SQLAlchemy criteria against in-memory
rows, so these tests exercise the actual queries.
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
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

_CE_PATH = _REPO_ROOT / "services" / "job-service" / "chat_export.py"
_spec = importlib.util.spec_from_file_location("chat_export_under_test", _CE_PATH)
_ce = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["chat_export_under_test"] = _ce
try:
    _spec.loader.exec_module(_ce)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"job-service chat_export failed to import: {exc}",
        allow_module_level=True,
    )

from shared.models import Resource, Snapshot, SnapshotItem  # noqa: E402
from sqlalchemy.sql.elements import BooleanClauseList  # noqa: E402


# ── in-memory rows ────────────────────────────────────────────────────────


class _Item:
    """SnapshotItem stand-in. `snapshot_resource_id` /
    `snapshot_resource_tenant_id` model the Snapshot (and Snapshot ->
    Resource) JOIN targets."""

    def __init__(self, **kw):
        self.id = kw.get("id") or uuid.uuid4()
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
        self.created_at = kw.get("created_at", 0)


# ── criteria evaluator ────────────────────────────────────────────────────


def _column_value(col, row):
    table = getattr(col, "table", None)
    if getattr(table, "name", "") == "snapshot_items":
        return getattr(row, col.key)
    element = getattr(table, "element", None)
    base = getattr(element, "name", getattr(table, "name", ""))
    if base == "snapshots":
        if col.key == "resource_id":
            return row.snapshot_resource_id
        if col.key == "id":
            return row.snapshot_id
    if base == "resources" and col.key == "tenant_id":
        return row.snapshot_resource_tenant_id
    raise AssertionError(f"unhandled column {base}.{col.key}")


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


def _apply_order_by(stmt, rows):
    for clause in reversed(list(stmt._order_by_clauses)):
        element = getattr(clause, "element", clause)
        descending = getattr(clause.modifier, "__name__", "") == "desc_op"
        key = element.key
        rows = sorted(rows, key=lambda r: getattr(r, key), reverse=descending)
    return rows


def _in_values(clause):
    clauses = clause.clauses if isinstance(clause, BooleanClauseList) else [clause]
    for c in clauses:
        if getattr(c.operator, "__name__", "") == "in_op":
            right = c.right
            return list(right.value if hasattr(right, "value") else right)
    raise AssertionError(f"no IN predicate in {clause}")


class _Result:
    def __init__(self, rows):
        self._rows = list(rows)

    def __iter__(self):
        return iter(self._rows)

    def scalars(self):
        return iter(self._rows)

    def all(self):
        return list(self._rows)

    def one(self):
        return self._rows[0]

    def scalar(self):
        if not self._rows:
            return None
        first = self._rows[0]
        return first[0] if isinstance(first, tuple) else first


class _FakeSession:
    def __init__(self, items, child_ids=()):
        self.items = list(items)
        self.child_ids = list(child_ids)

    async def execute(self, stmt, params=None):
        descs = stmt.column_descriptions
        entity = descs[0]["entity"]
        if entity is Resource:
            return _Result([(cid,) for cid in self.child_ids])
        if entity is Snapshot:
            wanted = _in_values(stmt.whereclause)
            out: dict = {}
            for i in self.items:
                if i.snapshot_id in wanted and i.snapshot_id not in out:
                    out[i.snapshot_id] = i.snapshot_resource_tenant_id
            return _Result(list(out.items()))
        rows = _apply_order_by(
            stmt, [i for i in self.items if _eval(stmt.whereclause, i)]
        )
        names = [d["name"] for d in descs]
        # count() / coalesce(sum(content_size), 0) collapse the row set;
        # everything else is a plain SnapshotItem column.
        if any(n in ("count", "coalesce", "sum") for n in names):
            agg = []
            for n in names:
                if n == "count":
                    agg.append(len(rows))
                else:
                    agg.append(sum(r.content_size or 0 for r in rows))
            return _Result([tuple(agg)])
        assert entity is SnapshotItem, entity
        tuples = [tuple(getattr(r, n) for n in names) for r in rows]
        if stmt._distinct:
            seen, uniq = set(), []
            for t in tuples:
                if t not in seen:
                    seen.add(t)
                    uniq.append(t)
            tuples = uniq
        return _Result(tuples)


# ── fixture data ──────────────────────────────────────────────────────────

R1 = uuid.uuid4()
T1 = uuid.uuid4()
T2 = uuid.uuid4()
S_NEW = uuid.uuid4()
S_OLD = uuid.uuid4()
S_FIRST_DRAINER = uuid.uuid4()
S_OTHER_TENANT = uuid.uuid4()
THREAD = "Chat with Bob"


def _msg(external_id, snapshot_id=S_NEW, size=100):
    return _Item(
        snapshot_id=snapshot_id,
        snapshot_resource_id=R1,
        snapshot_resource_tenant_id=T1,
        tenant_id=T1,
        external_id=external_id,
        parent_external_id="chat-1",
        item_type="TEAMS_CHAT_MESSAGE",
        name=external_id,
        folder_path=THREAD,
        content_size=size,
    )


def _child(item_type, parent, ext, *, snapshot_id=S_NEW, tenant_id=T1,
           resource_id=R1, resource_tenant_id=None, size=10,
           blob="blob/ok", created_at=0):
    return _Item(
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
        name=ext,
        folder_path=THREAD,
        blob_path=blob,
        content_size=size,
        created_at=created_at,
    )


def _counts(items, snapshot_ids=(S_NEW,)):
    sess = _FakeSession(items)
    return asyncio.run(
        _ce._resolve_counts(
            sess,
            resource_id=str(R1),
            snapshot_ids=[str(s) for s in snapshot_ids],
            thread_path=THREAD,
            item_ids=[],
        )
    )


# ── tests ─────────────────────────────────────────────────────────────────


def test_estimate_dedupes_carried_forward_attachment_copies():
    """Carry-forward keeps the same attachment in every later snapshot. The
    worker writes ONE ZIP entry, so the estimate must count it once —
    otherwise an N-snapshot chat inflates N-fold straight into the hard-cap
    gate."""
    items = [
        _msg("m1"),
        _child("CHAT_ATTACHMENT", "m1", "m1::a1", snapshot_id=S_NEW, created_at=2),
        _child("CHAT_ATTACHMENT", "m1", "m1::a1", snapshot_id=S_OLD, created_at=1),
    ]
    count, total, layout, _path = _counts(items)
    assert count == 1
    assert total == 100 + 10, total


def test_estimate_excludes_other_tenants_attachment_rows():
    """A colliding Teams message id in another tenant must not inflate the
    estimate — the worker never puts that row in the ZIP."""
    items = [
        _msg("m1"),
        _child("CHAT_ATTACHMENT", "m1", "m1::a1"),
        _child(
            "CHAT_ATTACHMENT", "m1", "m1::evil", snapshot_id=S_OTHER_TENANT,
            tenant_id=T2, resource_id=uuid.uuid4(), size=5_000_000_000,
        ),
    ]
    _count, total, _layout, _path = _counts(items)
    assert total == 100 + 10, total


def test_estimate_counts_null_tenant_hosted_content():
    """Every CHAT_HOSTED_CONTENT row has tenant_id NULL
    (backup-worker/main.py:10120 never sets it). Those bytes ARE written to
    the ZIP, so the estimate must include them — a tenant filter that drops
    them under-reports instead."""
    items = [
        _msg("m1"),
        _child("CHAT_HOSTED_CONTENT", "m1", "m1:hc1", tenant_id=None, size=42),
    ]
    _count, total, _layout, _path = _counts(items)
    assert total == 100 + 42, total


def test_estimate_counts_first_drainers_attachment():
    """Shared-chat attachments live in the FIRST drainer's snapshot only. The
    worker exports them, so the estimate must count them."""
    items = [
        _msg("m1"),
        _child(
            "CHAT_ATTACHMENT", "m1", "m1::a1",
            snapshot_id=S_FIRST_DRAINER, resource_id=uuid.uuid4(), tenant_id=T1,
            size=77,
        ),
    ]
    _count, total, _layout, _path = _counts(items)
    assert total == 100 + 77, total


def test_estimate_ignores_messages_outside_the_requested_snapshots():
    """The message ids that anchor the attachment sum must come from the
    snapshots being exported, exactly like scope.resolve()'s message query.
    A message that only exists in an unselected snapshot must not drag its
    attachment bytes into the estimate."""
    items = [
        _msg("m1", snapshot_id=S_NEW),
        _msg("m2", snapshot_id=S_OLD),
        _child("CHAT_ATTACHMENT", "m1", "m1::a1", snapshot_id=S_NEW, size=10),
        _child("CHAT_ATTACHMENT", "m2", "m2::a1", snapshot_id=S_OLD, size=999),
    ]
    count, total, _layout, _path = _counts(items, snapshot_ids=(S_NEW,))
    assert count == 1
    assert total == 100 + 10, total
