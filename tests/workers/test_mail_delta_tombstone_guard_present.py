"""Pin the mail page loop's tombstone guard IN main.py itself.

Why a source-level test: tests/workers/test_mail_delta_tombstones_not_items.py
protects this behaviour by RE-IMPLEMENTING the page loop inside the test file
(`_run_page_loop`, whose docstring says "Mirrors the page loop at
main.py:...") . A mirror cannot notice when the original loses the guard, so
all of those tests stay green while production regresses. That is not
hypothetical:

On 2026-08-11 the mail 410/resyncRequired re-walk fix was authored on a base
predating e33bedc — the commit that ADDED this guard — and wrapped the page
loop in a bounded `while True`. The resulting 3-way conflict was confined to
the loop header and looked like pure re-indentation, so the natural
resolution ("take the incoming side") would have silently deleted

    if is_delta_tombstone(m):
        continue

from the live walk. Consequences had it landed: tombstones re-enter
`local_out`, `_mail_items_persisted = max(_mail_persisted_total, len(out))`
is inflated by the tombstone count, and `evaluate_drain_honesty` can settle
non-FAILED on a run that captured zero real messages — an honest failure
reading as success. It would also have re-opened the 2026-08-07 incident
where 13,249 "(no subject)" removal rows buried 435k dated messages in
Recovery, since `_stream_persist_folder_items`' second filter is only a
belt-and-braces backstop for the streaming accumulator.

So: assert on the real source, not on a mirror of it.
"""
from __future__ import annotations

import ast
import pathlib

_WORKER_MAIN = (
    pathlib.Path(__file__).resolve().parents[2]
    / "workers"
    / "backup-worker"
    / "main.py"
)
_SRC = _WORKER_MAIN.read_text()


def test_guard_helper_still_exists():
    assert "def is_delta_tombstone(" in _SRC


def test_every_known_guard_site_survives():
    """Five call sites as of 2026-08-11: the helper def, the mail streaming
    pre-filter, the mail page loop, the OneDrive walk, and the shared-mailbox
    walk. If you legitimately add or remove one, update this count and say
    why in the commit message."""
    # 2026-09-04: sixth site = the Tier-1 per-folder delta drain
    # (_drain_tier1_folder_delta, tests/workers/test_tier1_mail_delta.py),
    # which must drop @removed entries exactly like the USER_MAIL loop.
    # Seventh site: full-subshard baseline id census excludes removals too.
    assert _SRC.count("is_delta_tombstone") == 7, (
        "tombstone guard call-site count changed — a merge may have dropped "
        "one; see this test's docstring"
    )


def test_mail_page_loop_skips_tombstones_before_building_a_path():
    """The load-bearing assertion: inside the mail delta page loop, the guard
    must come BEFORE the row is shaped, so neither accumulator sees it."""
    tree = ast.parse(_SRC)
    accept = next(n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef)
                  and n.name == "_accept_mail_page")
    body = ast.get_source_segment(_SRC, accept)
    assert body.index("if is_delta_tombstone(m):") < body.index("path = (")
    assert "continue" in body.split("path = (", 1)[0]


def test_guard_is_inside_the_bounded_rewalk_loop():
    """Every 410 retry and full baseline must route through the guarded page
    helper, including the new parallel subshard path."""
    tree = ast.parse(_SRC)
    folder = next(n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef)
                  and n.name == "_drain_one_folder")
    pager = next(n for n in ast.walk(folder) if isinstance(n, ast.AsyncFunctionDef)
                 and n.name == "_drain_pages_persist")
    assert "_accept_mail_page" in ast.unparse(pager)
    rewalk = next(n for n in ast.walk(folder) if isinstance(n, ast.While))
    assert "_drain_pages_persist(url, params)" in ast.unparse(rewalk)
    assert "_drain_subsharded_baseline()" in ast.unparse(rewalk)
