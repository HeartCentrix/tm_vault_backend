"""Regression: the USER_CHATS chatTopic rename passes must be scoped to the
snapshot being written and stay index-compatible.

Measured on prod 2026-10-14 (postgres-ha, PostgreSQL 17.10). The previous
shape -- ``JOIN snapshots s ON s.id = si.snapshot_id AND s.resource_id =
:rid`` with no snapshot_id predicate, matching on
``si.metadata::jsonb->>'chatId'`` -- planned::

    Update on snapshot_items si  (cost=338..35,770,395)
      -> Hash Join
           -> Seq Scan on snapshot_items si  (rows=443,431,845)

for EVERY USER_CHATS shard (two statements per shard), holding the giant
carry-forward slot 9-76 min per execution while updating 0 rows. With
``si.snapshot_id = :sid`` and the json ``->>`` operator the same statement
plans::

    Update on snapshot_items si  (cost=3,329..1,003,799)
      -> Nested Loop
           -> Index Scan using snapshots_pkey on snapshots s (id = $4)
           -> Hash Join
                -> Bitmap Heap Scan on snapshot_items si
                     -> Bitmap Index Scan on idx_snapshot_items_snapshot_type
                          Index Cond: (snapshot_id = $4)   est. 268,909 rows

(legacy-migrate pass: BitmapAnd of the same index with
idx_snapshot_items_folder_type, est. 28,717 rows, cost 741,659). No Seq
Scan in either.

The ``::jsonb`` cast form can never use idx_snapshot_items_metadata_chat_id,
whose indexed expression is the json operator ``(metadata ->> 'chatId')``.

The SQL lives in ``_chat_topic_rename_sql`` so the statement text can be
asserted without a database; the call-site checks are source assertions,
same convention as tests/workers/test_chat_drain_no_orderby.py.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys

import pytest
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_MAIN = _MAIN_PATH.read_text()

_spec = importlib.util.spec_from_file_location("bw_chat_topic_scoping", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_chat_topic_scoping"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )

_NORMALIZE = _bw._chat_topic_rename_sql(legacy_patterns_only=False)
_MIGRATE = _bw._chat_topic_rename_sql(legacy_patterns_only=True)

_LEGACY_PATTERNS = (
    "si.folder_path LIKE 'chats/Untitled chat #%'",
    "si.folder_path LIKE 'chats/Untitled group #%'",
    "si.folder_path LIKE 'chats/1-on-1 Chat (19:%'",
    "si.folder_path LIKE 'chats/Group Chat (19:%'",
    "si.folder_path LIKE 'chats/Chat %'",
)


# ─── statement text ───────────────────────────────────────────────────────

@pytest.mark.parametrize("sql", [_NORMALIZE, _MIGRATE], ids=["normalize", "migrate"])
def test_update_is_scoped_to_the_snapshot_being_written(sql):
    """The predicate must be on the UPDATE target itself (``si``) so the
    planner can drive the heap access from idx_snapshot_items_snapshot_type;
    a predicate only on the joined ``snapshots`` row does not."""
    assert "AND si.snapshot_id = :sid" in sql


@pytest.mark.parametrize("sql", [_NORMALIZE, _MIGRATE], ids=["normalize", "migrate"])
def test_chat_id_match_uses_the_indexed_json_expression(sql):
    assert "(si.metadata->>'chatId') = nm.cid" in sql
    assert "::jsonb->>" not in sql, (
        "the ::jsonb cast form does not match idx_snapshot_items_metadata_chat_id"
    )


@pytest.mark.parametrize("sql", [_NORMALIZE, _MIGRATE], ids=["normalize", "migrate"])
def test_resource_guard_is_kept(sql):
    """Scoping by snapshot must not drop the resource guard: a wrong sid
    can still never rename another resource's rows."""
    assert "s.id = si.snapshot_id" in sql
    assert "AND s.resource_id = :rid" in sql


@pytest.mark.parametrize("sql", [_NORMALIZE, _MIGRATE], ids=["normalize", "migrate"])
def test_bind_names_match_what_the_call_sites_supply(sql):
    compiled = text(sql).compile(dialect=postgresql.dialect())
    assert set(compiled.params) == {"cids", "names", "rid", "sid"}


def test_only_the_migrate_pass_restricts_to_legacy_fallback_folders():
    for pat in _LEGACY_PATTERNS:
        assert pat in _MIGRATE
    assert "LIKE" not in _NORMALIZE
    # Identical apart from the appended legacy-folder block.
    assert _MIGRATE.startswith(_NORMALIZE.rstrip())


def test_set_clause_unchanged():
    for sql in (_NORMALIZE, _MIGRATE):
        assert "SET folder_path = 'chats/' || nm.dn," in sql
        assert "jsonb_build_object('chatTopic', nm.dn))::json" in sql
        assert "AND si.folder_path != 'chats/' || nm.dn" in sql


# ─── call sites (source assertions) ───────────────────────────────────────

def test_no_unscoped_jsonb_cast_form_left_in_the_worker():
    offenders = [
        ln.strip()
        for ln in _MAIN.splitlines()
        if "::jsonb->>'chatId'" in ln and not ln.strip().startswith("#")
    ]
    assert offenders == [], offenders


def test_both_slot_sessions_execute_the_scoped_builder_and_bind_sid():
    assert 'what="chatTopic normalize"' in _MAIN
    assert 'what="chatTopic legacy migrate"' in _MAIN
    assert _MAIN.count("text(_chat_topic_rename_sql(legacy_patterns_only=False))") == 1
    assert _MAIN.count("text(_chat_topic_rename_sql(legacy_patterns_only=True))") == 1
    assert _MAIN.count('"sid": _sid}') == 2


def test_passes_are_skipped_rather_than_unscoped_without_a_snapshot():
    assert "_sid = str(snapshot.id) if snapshot is not None else None" in _MAIN
    assert "if real_cids and _sid:" in _MAIN
    assert "if migrate_cids and _sid:" in _MAIN
