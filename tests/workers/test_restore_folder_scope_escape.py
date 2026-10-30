"""Regression: restore-worker folder scoping must declare LIKE ESCAPE.

``restore_in_place``/export do NOT go through
``shared.folder_resolver.resolve_selection`` — they import
``_prefix_and_exact_for`` and build the LIKE themselves. An escaped
pattern with no ``ESCAPE`` clause is still wrong, so the escape has to
reach every one of those call sites or `/HR_2026` keeps sweeping in
`/HRX2026`.

Pinned here for ``count_snapshot_items`` (the pre-flight count the PST
path gates on) — the same clause construction as
``fetch_snapshot_items`` and ``stream_snapshot_items_by_group``.
"""
from __future__ import annotations

import asyncio
import importlib.util
import pathlib
import sys
import uuid

from sqlalchemy.dialects import postgresql

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from shared.folder_resolver import LIKE_ESCAPE  # noqa: E402

_WORKER_DIR = pathlib.Path(__file__).resolve().parents[2] / "workers" / "restore-worker"
if str(_WORKER_DIR) not in sys.path:
    sys.path.insert(0, str(_WORKER_DIR))


def _load_main():
    name = "restore_worker_main_for_tests"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, _WORKER_DIR / "main.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class _CaptureSession:
    """Returns no snapshot rows (so the sibling set collapses to the
    picked ids) and records every statement executed."""

    def __init__(self):
        self.stmts = []

    async def execute(self, stmt):
        self.stmts.append(stmt)
        outer = self

        class _R:
            def scalars(self):
                return self

            def all(self):
                return []

            def scalar_one(self):
                return 0

        return _R()


def test_count_snapshot_items_emits_escape_clause():
    main_mod = _load_main()
    worker = main_mod.RestoreWorker()
    sess = _CaptureSession()

    total = asyncio.run(worker.count_snapshot_items(
        sess,
        snapshot_ids=[str(uuid.uuid4())],
        item_ids=[],
        folder_paths=["/HR_2026"],
    ))
    assert total == 0

    count_stmt = sess.stmts[-1]
    compiled = count_stmt.compile(dialect=postgresql.dialect())
    # The psycopg paramstyle doubles backslashes in the rendered SQL;
    # collapse them before comparing to the module's escape char.
    sql = str(compiled).replace(LIKE_ESCAPE * 2, LIKE_ESCAPE)
    # Escaped pattern AND the ESCAPE declaration must travel together.
    assert f"ESCAPE '{LIKE_ESCAPE}'" in sql
    assert "/HR\\_2026/%" in compiled.params.values()
