"""Regression: LIKE metacharacters in folder names must match literally.

Bug (found 2026-08-04 review): ``_prefix_and_exact_for`` built descendant
patterns as ``stripped + "/%"`` with no escaping, and every consumer
matched them via ``SnapshotItem.folder_path.like(...)``. ``_`` matches
any single character in LIKE and is extremely common in real folder
names, so restoring ``/HR_2026`` also selected every item under a
sibling ``/HRX2026`` — the wrong folder's files silently shipped in the
restore/export (and an IN_PLACE OVERWRITE restore wrote them into the
target). The escape first landed on the services lineage only, so the
restore-worker's direct ``_prefix_and_exact_for`` call sites still built
unescaped patterns until this integration; every consumer now passes
``escape=LIKE_ESCAPE`` (restore-worker arms covered by
tests/workers/test_restore_folder_scope_escape.py).

Contract pinned here:
  - ``_``, ``%`` and ``\\`` in the ticked folder path are escaped in the
    descendant prefix pattern — and ONLY there: the exact-match arm uses
    equality, so it must carry the path verbatim.
  - The trailing ``/%`` descendant wildcard itself stays live.
  - ``resolve_selection`` wires the matching ESCAPE character into the
    LIKE clause it emits.
"""
from __future__ import annotations

import asyncio
import os
import pathlib
import sys
import uuid

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from shared.folder_resolver import (  # noqa: E402
    LIKE_ESCAPE,
    _prefix_and_exact_for,
    resolve_selection,
)


def test_underscore_is_escaped_in_prefix_but_not_exact():
    prefixes, exacts = _prefix_and_exact_for(["/HR_2026"])
    assert prefixes == ["/HR\\_2026/%"]
    assert exacts == ["/HR_2026"]


def test_percent_and_backslash_are_escaped():
    prefixes, exacts = _prefix_and_exact_for(["/100% done", "/back\\slash"])
    assert prefixes == ["/100\\% done/%", "/back\\\\slash/%"]
    assert exacts == ["/100% done", "/back\\slash"]


def test_descendant_wildcard_survives_escaping():
    (prefix,), _ = _prefix_and_exact_for(["/HR_2026/"])
    assert prefix.endswith("/%")
    assert not prefix.endswith("\\/%"), "the live wildcard must not be escaped"


def test_root_folder_case_unchanged():
    prefixes, exacts = _prefix_and_exact_for(["/"])
    assert prefixes == ["/%"]
    assert exacts == ["/"]


class _CaptureSession:
    """Records the statement resolve_selection executes."""

    def __init__(self):
        self.stmt = None

    async def execute(self, stmt):
        self.stmt = stmt

        class _R:
            def scalars(self):
                return self

            def all(self):
                return []

        return _R()


def test_resolve_selection_emits_escape_clause():
    sess = _CaptureSession()
    asyncio.run(
        resolve_selection(
            sess,
            snapshot_id=str(uuid.uuid4()),
            item_ids=[],
            folder_paths=["/HR_2026"],
            excluded_item_ids=[],
        )
    )
    assert sess.stmt is not None
    compiled = sess.stmt.compile()
    sql = str(compiled)
    # The LIKE must declare the same escape character the pattern uses —
    # without it, engines with no default escape treat '\_' as two chars.
    assert f"ESCAPE '{LIKE_ESCAPE}'" in sql
    assert "/HR\\_2026/%" in compiled.params.values()
