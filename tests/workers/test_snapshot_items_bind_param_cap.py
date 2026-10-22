"""snapshot_items bulk-INSERT bind-parameter ceiling + the metadata/extra_data trap.

Bug 1 (prod, 2026-08-05 → 2026-08-10): ``_backup_message_attachments``
built one unbounded multi-row ``INSERT INTO snapshot_items`` from
``all_rows`` — the union of every attachment across every flagged message
in the mailbox. The Postgres wire format carries the bound-parameter
count as a signed int16, so the statement crossed 32,767 parameters at
~2.1k rows and asyncpg raised::

    InterfaceError: the number of query arguments cannot exceed 32767

That failed the whole handler. ROOM_MAILBOX had exactly one ACTIVE
resource (Corp_Esplanade), so every scheduled room-mailbox run failed
from 2026-08-05 02:07 UTC on. ``_backup_event_attachments`` carried the
identical unchunked pattern, latent only because no calendar had yet
accumulated that many attachments in one snapshot.

Bug 2 (caught reviewing the fix for bug 1, never shipped): routing those
rows through ``_bulk_upsert_snapshot_items`` is only correct if they are
re-keyed. The helper compiles the ORM **entity**, while both call sites
keyed rows ``"metadata"`` — the DB column name. ``SnapshotItem`` maps
``extra_data -> Column("metadata")``, so on the entity ``"metadata"``
resolves to ``Base.metadata`` and raises ``AttributeError: 'MetaData'
object has no attribute '_bulk_update_tuples'``. That would have traded
one guaranteed failure for another.

These tests pin:
  1. no compiled statement can exceed the ceiling, measured on the real
     SnapshotItem row shape via actual compilation (not key counting —
     SQLAlchemy binds column defaults the row omits, so an 11-key row
     compiles to 14 params);
  2. the chunk divisor is the table's column count, so the bound holds
     for every row shape a caller might pass;
  3. neither attachment path hand-rolls an unbounded INSERT again;
  4. rows fed to the helper key ``extra_data``, never ``metadata``.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys
import uuid

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_bind_param_cap", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_bind_param_cap"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )

from sqlalchemy.dialects import postgresql  # noqa: E402

from shared.models import SnapshotItem  # noqa: E402


class _StubSession:
    """Records each executed statement. Mirrors the stub-AsyncSession
    convention in tests/workers/test_archive_backup.py."""

    def __init__(self):
        self.statements = []
        self.committed = 0

    async def execute(self, statement, binds=None):
        self.statements.append(statement)
        return None

    async def commit(self):
        self.committed += 1

    async def rollback(self):  # pragma: no cover - not exercised here
        pass


def _run(coro):
    import asyncio

    return asyncio.new_event_loop().run_until_complete(coro)


def _attachment_rows(n: int, *, key: str = "extra_data"):
    """The real EMAIL_ATTACHMENT row shape built at main.py:22779."""
    snap = uuid.uuid4()
    tenant = uuid.uuid4()
    return [
        {
            "snapshot_id": snap,
            "tenant_id": tenant,
            "external_id": f"msg{i}::att{i}",
            "item_type": "EMAIL_ATTACHMENT",
            "name": f"file{i}.pdf",
            "folder_path": "/Inbox",
            "content_hash": "h" * 64,
            "content_size": 1234,
            "blob_path": f"blob/{i}",
            "content_checksum": "h" * 64,
            key: {"parent_item_id": f"msg{i}", "attachment_kind": "file"},
        }
        for i in range(n)
    ]


def _compiled_param_counts(stub):
    """Bound params per recorded statement, from the real compiled SQL."""
    return [
        len(st.compile(dialect=postgresql.dialect()).params)
        for st in stub.statements
    ]


# ─── the ceiling ──────────────────────────────────────────────────────────

def test_protocol_ceiling_constant_is_the_int16_limit():
    """32,767 is a protocol limit, not a tunable."""
    assert _bw._PG_MAX_BIND_PARAMS == 32767


def test_compiled_row_is_wider_than_the_dict():
    """Why the divisor cannot be len(row_dict): SQLAlchemy also binds every
    column with a Python-side default the row omits. If this ever equals
    the key count, the comment in the helper needs revisiting."""
    row = _attachment_rows(1)[0]
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    stmt = pg_insert(SnapshotItem).values([row])
    n_params = len(stmt.compile(dialect=postgresql.dialect()).params)
    assert n_params > len(row), (
        f"{len(row)} keys compiled to {n_params} params — a key-count "
        f"divisor would under-estimate the real width"
    )


def test_no_compiled_statement_exceeds_the_ceiling():
    """The regression. 20k attachment rows — the old code emitted ONE
    statement (~280k params) and asyncpg refused it outright."""
    stub = _StubSession()
    rows = _attachment_rows(20000)

    total = _run(_bw._bulk_upsert_snapshot_items(stub, rows))

    assert total == 20000
    counts = _compiled_param_counts(stub)
    assert counts, "expected at least one INSERT"
    assert max(counts) <= _bw._PG_MAX_BIND_PARAMS, (
        f"a statement bound {max(counts)} params, over the "
        f"{_bw._PG_MAX_BIND_PARAMS} ceiling"
    )
    assert stub.committed == 1


def test_chunk_divisor_is_the_table_column_count():
    """Bound on the widest possible row, so no caller's row shape can
    breach the ceiling. 32767 // 19 columns = 1724 rows."""
    stub = _StubSession()
    _run(_bw._bulk_upsert_snapshot_items(stub, _attachment_rows(5000)))

    expected = _bw._PG_MAX_BIND_PARAMS // len(SnapshotItem.__table__.columns)
    (first,) = stub.statements[0]._multi_values
    assert len(first) == min(_bw._BULK_INSERT_CHUNK, expected)
    # Even a fully-populated row (every column bound) stays under.
    assert expected * len(SnapshotItem.__table__.columns) <= _bw._PG_MAX_BIND_PARAMS


def test_all_rows_are_inserted_exactly_once():
    """Chunking must not drop or duplicate a row."""
    stub = _StubSession()
    rows = _attachment_rows(4000)

    _run(_bw._bulk_upsert_snapshot_items(stub, rows))

    # Compiling the ORM entity re-keys each row by Column object, whose
    # .name is the DB column name — so extra_data reads back as "metadata".
    def _by_name(r):
        return {getattr(k, "name", k): v for k, v in r.items()}

    seen = [
        _by_name(r)["external_id"]
        for st in stub.statements
        for r in st._multi_values[0]
    ]
    assert len(seen) == 4000 and len(set(seen)) == 4000
    assert set(seen) == {r["external_id"] for r in rows}


def test_empty_rows_is_a_noop():
    stub = _StubSession()
    assert _run(_bw._bulk_upsert_snapshot_items(stub, [])) == 0
    assert stub.statements == [] and stub.committed == 0


# ─── the metadata / extra_data trap ───────────────────────────────────────

def test_metadata_keyed_rows_are_rejected_by_the_helper():
    """Documents the trap that nearly shipped: the helper compiles the ORM
    entity, where "metadata" is Base.metadata, not the mapped column. Any
    caller keying the raw DB column name fails — loudly, but only at run
    time, which is why the source guard below exists."""
    stub = _StubSession()
    with pytest.raises(AttributeError, match="_bulk_update_tuples"):
        _run(_bw._bulk_upsert_snapshot_items(stub, _attachment_rows(2, key="metadata")))


def test_extra_data_is_the_mapped_attribute_for_the_metadata_column():
    """The mapping that makes the trap possible. If this changes, the
    comments at both attachment sites are wrong."""
    assert SnapshotItem.extra_data.property.columns[0].name == "metadata"


# ─── source guards: the shapes that fail must not come back ───────────────

def test_no_unbounded_multi_row_insert_over_all_rows():
    """``_pg_insert(...).values(all_rows)`` is the exact shape that failed."""
    src = _MAIN_PATH.read_text()
    assert "values(all_rows)" not in src, (
        "an unbounded multi-row INSERT over all_rows is back; route it "
        "through _bulk_upsert_snapshot_items instead"
    )


def test_helper_callers_do_not_key_the_raw_db_column_name():
    """Only the two _pg_insert(SnapshotItem.__table__) sites may key
    "metadata". Every _bulk_upsert_snapshot_items caller must key
    "extra_data" or it dies with AttributeError at run time."""
    src = _MAIN_PATH.read_text().splitlines()
    metadata_lines = [i + 1 for i, l in enumerate(src) if '"metadata":' in l]
    # Each remaining site must have a __table__-form insert after it and
    # before the next 120 lines — i.e. it is not a helper caller.
    for ln in metadata_lines:
        window = "\n".join(src[ln:ln + 120])
        assert "_bulk_upsert_snapshot_items" not in window, (
            f'row dict at main.py:{ln} keys "metadata" but feeds '
            f"_bulk_upsert_snapshot_items — must be \"extra_data\""
        )
