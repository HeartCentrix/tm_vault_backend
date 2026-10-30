"""SHAREPOINT_LIST row persist must be an ON CONFLICT DO NOTHING upsert.

Prod, 2026-08-11, backup-worker-5bf49d96b-f8h5f, snapshot
6fcb1336-3bc0-4c14-b086-5f1456aadc00 (tenant 163c487b, resource fe762a38)::

    list-folder persist failed for IT PO & Invoice/Announcement Tiles:
    (asyncpg.exceptions.UniqueViolationError) duplicate key value violates
    unique constraint "uq_snapshot_items_snap_ext_type"
    DETAIL:  Key (snapshot_id, external_id, item_type)=(6fcb1336-…,
             dff378de-5cca-4900-8f61-01d30e0b9e48, SHAREPOINT_LIST)
             already exists.

Measured in that one pod's log buffer: 357 occurrences, 357/357
UniqueViolation, all on the SAME snapshot, across 57 distinct list GUIDs —
10 lists collided once, 29 collided 7×, 18 collided 8×.

``list_producer`` persisted the list-folder row with a bare
``sess.add(list_row)``. Two independent producers offer the same
(snapshot_id, external_id, 'SHAREPOINT_LIST') key:

 1. Cross-replica partition shards. snapshot 6fcb1336 carries
    ``extra_data.partitioned=true, partition_count=2``; both
    snapshot_partitions rows are COMPLETED against the SAME snapshot_id.
    ``partitionFilter.driveIds`` narrows only the DRIVE walk —
    ``list_producer`` is created for every site target in every shard, so
    each list is offered once per shard.

 2. Subsite echo. ``get_sharepoint_site_lists``'s SP-REST merge pass
    truncates a list webUrl to ``/sites/<first-segment>``, so for a SUBSITE
    it queries the PARENT web and merges the parent's ``_catalogs/*`` lists
    in as though they belonged to the subsite. Evidence: list
    ``001aceca-fdcd-4e40-a6e2-c395cde9a0f0`` was offered under 7 different
    site labels (ACR & Plot Plan, ATL Intrgration, IT Projects, Nintex
    Forms, Nintex Workflow, Sales Assimilation Plan, TM360) with a
    byte-identical ``web_url`` of
    ``https://taylormorrison.sharepoint.com/sites/projects/Lists/Badges``
    every time. That one belongs in shared/graph_client.py; the persist
    site must be idempotent regardless.

Blast radius when it raised: the row was lost for that producer only — the
persist ran in its own short-lived session holding just this row, and the
``except Exception`` swallowed it as a warning. It did NOT abort a batch,
did NOT roll back sibling items, and did NOT feed ``files_failed`` (the
snapshot's 152 failures are drive_fail + rest_fail + enum_fail; the list
persist increments none of them). The cost was 357 log lines per run and a
``list_folders`` undercount.

These tests pin:
  1. the statement the file actually builds is an INSERT … ON CONFLICT
     DO NOTHING keyed on the real 3-column unique index, compiled against
     the real SnapshotItem table (so the extra_data/metadata trap and an
     index_elements typo both fail here, not in prod);
  2. it RETURNs, and ``list_folders`` is incremented only when a row came
     back — DO NOTHING makes the statement succeed while inserting
     nothing, and an unconditional bump would inflate total_items → the
     shard's item_count → snapshot_partitions.files_uploaded;
  3. the bare ``sess.add(list_row)`` shape does not come back.
"""
from __future__ import annotations

import ast
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
_SRC = _MAIN_PATH.read_text()
_TREE = ast.parse(_SRC)

from sqlalchemy.dialects import postgresql  # noqa: E402
from sqlalchemy.dialects.postgresql import insert as _pg_insert  # noqa: E402

from shared.models import SnapshotItem  # noqa: E402

# The unique index the violation named. Kept as a literal so a schema
# change has to come through this test.
_UQ_COLS = ["snapshot_id", "external_id", "item_type"]


# ─── locating the real code ───────────────────────────────────────────────

def _list_producer() -> ast.AsyncFunctionDef:
    """The nested `async def list_producer` inside backup_sharepoint."""
    found = [
        n for n in ast.walk(_TREE)
        if isinstance(n, ast.AsyncFunctionDef) and n.name == "list_producer"
    ]
    assert len(found) == 1, (
        f"expected exactly one list_producer in main.py, found {len(found)}"
    )
    return found[0]


def _assign_in(fn: ast.AST, target: str) -> ast.expr:
    for n in ast.walk(fn):
        if isinstance(n, ast.Assign):
            for t in n.targets:
                if isinstance(t, ast.Name) and t.id == target:
                    return n.value
    raise AssertionError(
        f"no `{target} = …` assignment inside list_producer — the "
        f"SHAREPOINT_LIST persist site has changed shape"
    )


class _Any:
    """Stand-in for every local the persist block closes over (snapshot,
    tenant, lst, display, …). Absorbs attribute access, subscripting,
    slicing and calls so the REAL expression from main.py can be
    evaluated without reconstructing backup_sharepoint's whole scope."""

    def __getattr__(self, _name):
        # Dunders must raise: SQLAlchemy's coercion layer does
        # `while hasattr(element, "__clause_element__")`, so answering
        # every dunder truthfully spins forever (caught by pytest-timeout
        # at 120s the first time this was written).
        if _name.startswith("__") and _name.endswith("__"):
            raise AttributeError(_name)
        return _Any()

    def __getitem__(self, _key):
        return _Any()

    def __call__(self, *_a, **_k):
        return _Any()

    def __bool__(self):
        return False

    def __str__(self):
        return "stub"

    def __iter__(self):
        # Without this, `x in stub` falls back to the __getitem__ iteration
        # protocol, which never raises StopIteration here and hangs.
        raise TypeError("_Any is not iterable")


class _NS(dict):
    def __missing__(self, _key):
        return _Any()


def _eval(node: ast.expr, **names):
    """Evaluate an expression lifted verbatim out of main.py."""
    expr = ast.unparse(node)
    ns = _NS(names)
    ns.setdefault("uuid", uuid)
    return eval(compile(ast.parse(expr, mode="eval"), "<main.py>", "eval"), {}, ns)


# ─── 1. the compiled statement ────────────────────────────────────────────

def _list_row_dict() -> dict:
    row = _eval(_assign_in(_list_producer(), "list_row"))
    assert isinstance(row, dict), (
        "list_row must be a plain dict of DB column names for "
        "_pg_insert(SnapshotItem.__table__); a SnapshotItem ORM instance "
        "means the bare session.add() INSERT is back"
    )
    return row


def test_list_row_persist_is_on_conflict_do_nothing():
    """The exact statement main.py builds, compiled against the real
    SnapshotItem table."""
    fn = _list_producer()
    row = _list_row_dict()
    stmt = _eval(
        _assign_in(fn, "stmt"),
        _pg_insert=_pg_insert,
        SnapshotItem=SnapshotItem,
        list_row=row,
    )
    sql = str(stmt.compile(dialect=postgresql.dialect()))
    assert "INSERT INTO snapshot_items" in sql
    assert "ON CONFLICT" in sql and "DO NOTHING" in sql, (
        f"SHAREPOINT_LIST persist is not conflict-safe:\n{sql}"
    )
    # Keyed on the index the prod violation named, not a 2-col subset
    # (which raises InvalidColumnReferenceError at run time).
    for col in _UQ_COLS:
        assert col in sql.split("ON CONFLICT", 1)[1], (
            f"ON CONFLICT target is missing {col}; "
            f"uq_snapshot_items_snap_ext_type is {tuple(_UQ_COLS)}"
        )


def test_list_row_keys_the_db_column_name_not_the_orm_attr():
    """SnapshotItem.extra_data maps to the column literally named
    "metadata". The __table__ form needs the column name; "extra_data"
    raises CompileError "Unconsumed column names" and every list dies."""
    row = _list_row_dict()
    assert "metadata" in row and "extra_data" not in row
    assert SnapshotItem.extra_data.property.columns[0].name == "metadata"


# ─── 2. the counter stays honest ──────────────────────────────────────────

def test_list_folders_counted_only_when_a_row_was_actually_inserted():
    """DO NOTHING SUCCEEDS while inserting nothing. Bumping list_folders
    unconditionally would inflate total_items → the shard's item_count →
    snapshot_partitions.files_uploaded (already 14,013 + 14,029 against
    1,856 real rows on snapshot 6fcb1336)."""
    fn = _list_producer()
    stmt_src = ast.unparse(_assign_in(fn, "stmt"))
    assert "returning" in stmt_src, (
        "the upsert must RETURN so the caller can tell an insert from a "
        "skipped conflict"
    )
    bumps = [
        n for n in ast.walk(fn)
        if isinstance(n, ast.AugAssign)
        and isinstance(n.target, ast.Subscript)
        and ast.unparse(n.target.slice) == "'list_folders'"
    ]
    assert bumps, "stats['list_folders'] is no longer incremented at all"
    guarded = [
        n for n in ast.walk(fn)
        if isinstance(n, ast.If) and any(b in ast.walk(n) for b in bumps)
    ]
    assert guarded, (
        "stats['list_folders'] += 1 is unconditional after an ON CONFLICT "
        "DO NOTHING — it now counts rows this shard did not write"
    )


# ─── 3. the shape that failed must not come back ──────────────────────────

def test_no_bare_add_of_the_list_row():
    assert "sess.add(list_row)" not in _SRC and "session.add(list_row)" not in _SRC, (
        "the bare INSERT for SHAREPOINT_LIST is back; it raised "
        "UniqueViolation 357× in one pod on 2026-08-11"
    )


def test_sibling_sp_rest_row_persist_is_still_conflict_safe():
    """The prior art this fix mirrors. If it regresses, the two SharePoint
    persist sites have diverged again."""
    fn = [
        n for n in ast.walk(_TREE)
        if isinstance(n, ast.AsyncFunctionDef) and n.name == "_backup_sp_rest_row"
    ]
    assert len(fn) == 1
    assert "on_conflict_do_nothing" in ast.unparse(fn[0])
