"""USER_MAIL: a Graph-expired (410 resyncRequired) deltaLink must be
re-walked ONCE and EVICTED — never resumed again.

Bug (2026-08-11): `_drain_one_folder` resumes from the saved deltaLink
verbatim and its `except Exception` arm had no 410 / resyncRequired case.
Graph answers 410 when it expires a delta token; the generic arm marked the
folder "drain failed", which correctly HELD BACK the cursor but never
DROPPED it. So the next run resumed the same dead token, failed on page 1
again, and the folder was frozen out of every future snapshot — while the
mailbox's other folders kept succeeding, so the mailbox looked healthy.

Live case: resource "Mail — Orlando Garcia", folder /Inbox/Tickets/Halo
(fingerprint totalItemCount 32,105 / 13,378 unread) captured ZERO items
from 2026-08-10 10:12 UTC onward. Per-run history: 999 (8/3 full), 12, 82,
20, 20, 28, 51, 56, 68, 28 (8/10 18:11), then 0, 0. His other 47 folders
backed up 27-38 items/run throughout.

The fix mirrors the existing arms — USER_ONEDRIVE (410 on a saved link →
drop it, re-walk /root/delta in the SAME run), USER_CALENDAR and
USER_CONTACTS (evict the key so the next run bootstraps) — and routes the
eviction through the ONE cursor gate (`_persist_mail_delta_state`) so both
writers (the `mail_folder_delta` row AND the legacy extra_data mirror) drop
the dead token. Evicting only one of the two is no eviction at all: the
read side is `{**legacy, **new_table}`.

The drain closure lives inside a 4,000-line handler body that cannot be
imported, so its half is source-inspected (the house pattern — see
test_delta_tombstones_and_expiry.py); the persist half drives the real
method with a fake session factory.
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
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_SRC = _MAIN_PATH.read_text()

_spec = importlib.util.spec_from_file_location("bw_mail_resync", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_mail_resync"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )


HALO = "AAMkAGI2-inbox-tickets-halo"
SENT = "AAMkAGI2-sent-items"
DEAD_CURSOR = "https://graph/mailFolders/halo/messages/delta?$deltatoken=DEAD"
FRESH_CURSOR = "https://graph/mailFolders/halo/messages/delta?$deltatoken=NEW"


def _drain_region(size: int | None = None) -> str:
    """Source of the ``_drain_one_folder`` closure.

    Bounded by the closure's own indentation rather than a fixed character
    count. The original 12,000-char window stopped covering the
    resyncRequired arm the moment any comment was added earlier in the body:
    on 2026-08-11, re-inserting e33bedc's tombstone guard during the 410
    re-walk merge pushed `local_out = []` ~400 chars past the cutoff and
    test_drain_resets_the_partial_page_set_before_rewalking failed against
    code that was correct. A source-slicing test must not depend on how
    verbose the code above it is.
    """
    i = _SRC.find("async def _drain_one_folder")
    assert i != -1, "anchor moved: _drain_one_folder"
    if size is not None:
        return _SRC[i:i + size]
    lines = _SRC[i:].splitlines(keepends=True)
    base_indent = len(lines[0]) - len(lines[0].lstrip())
    out = [lines[0]]
    for line in lines[1:]:
        if line.strip():
            indent = len(line) - len(line.lstrip())
            if indent <= base_indent:
                break  # dedented out of the closure
        out.append(line)
    return "".join(out)


# ─── the drain closure: detect, re-walk once, evict ───────────────────────

def test_drain_detects_410_resyncrequired():
    r = _drain_region()
    assert "resyncrequired" in r.lower(), (
        "the folder-drain except arm must recognise Graph's "
        "410 resyncRequired — without it a poisoned cursor is just a "
        "'drain failed' and is resumed again next run, forever"
    )
    assert '"410" in _emsg' in r and "status_code" in r, (
        "match both the stringified error and an httpx status_code, since "
        "_iter_pages surfaces the failure either way"
    )


def test_drain_rewalks_from_the_bootstrap_url_in_the_same_run():
    r = _drain_region()
    assert "_bootstrap_url" in r and "_bootstrap_params" in r, (
        "the bare (no-cursor) delta target must be derived up front so the "
        "410 arm can fall back to it"
    )
    assert "url = _bootstrap_url" in r and "continue" in r, (
        "a 410 on a RESUMED walk must re-walk this folder from the bootstrap "
        "delta URL inside the same run (the USER_ONEDRIVE pattern)"
    )
    assert "_resync_rewalked" in r, "the re-walk must be bounded to one attempt"


def test_drain_rewalk_is_conditional_on_having_resumed_from_a_saved_token():
    r = _drain_region()
    assert "if _is_resync and saved and not _resync_rewalked:" in r, (
        "only a walk that RESUMED from a saved token can be fixed by "
        "dropping it; a 410 on the bare bootstrap URL is a different fault "
        "and must not loop"
    )


def test_drain_evicts_the_poisoned_token_even_if_the_rewalk_fails():
    r = _drain_region()
    assert "_mail_folders_token_evicted.add(_tok_key)" in r, (
        "eviction must be recorded BEFORE the re-walk — if the re-walk also "
        "dies, the dead token must still be gone by end of run or the next "
        "cycle inherits the same permanent failure"
    )
    # And the eviction set must reach the single cursor gate.
    assert "evicted_token_keys=_mail_folders_token_evicted," in _SRC, (
        "the eviction set must be handed to _persist_mail_delta_state, the "
        "one place allowed to write cursors"
    )


def test_drain_resets_the_partial_page_set_before_rewalking():
    r = _drain_region()
    i = r.find("_mail_folders_token_evicted.add(_tok_key)")
    assert i != -1
    arm = r[i:i + 1600]
    assert "local_out = []" in arm and "delta_out = None" in arm, (
        "pages captured before the 410 are re-walked from scratch; keeping "
        "them would duplicate rows in `out` and could pair a stale "
        "deltaLink with a re-bootstrapped walk"
    )


def test_mail_reason_names_the_failed_folders():
    """A counter alone ("1 of 1 folder drain(s) failed") identifies nothing
    on a 48-folder mailbox — that is why the Halo incident went unnoticed."""
    i = _SRC.find("_missing_paths = sorted(")
    assert i != -1, "the degraded reason must name the folders it lost"
    seg = _SRC[i:i + 900]
    assert "folder_tree.get(_f)" in seg, "paths come from folder_tree"
    assert "sorted(" in seg, "stable across runs — set order is not"
    assert "more" in seg, "cap the list but keep the count"


def test_enumeration_failed_is_not_double_prefixed():
    """_fetch_for's outer arm re-wraps as EnumerationFailed(f"{kind}: {e}"),
    so a handler-side "USER_MAIL: " prefix produced the doubled operator
    string seen in prod: "enumeration failed: USER_MAIL: USER_MAIL: …"."""
    assert 'raise EnumerationFailed(f"USER_MAIL: {_mail_reason}")' not in _SRC, (
        "the handler must not prefix a kind the outer wrapper already adds"
    )
    assert "raise EnumerationFailed(_mail_reason)" in _SRC


# ─── the persist gate: eviction goes through BOTH writers ─────────────────


class _FakeResourceRow:
    def __init__(self, extra_data):
        self.extra_data = dict(extra_data or {})


class _FakeSession:
    def __init__(self, row):
        self._row = row
        self.committed = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, _model, _pk):
        return self._row

    async def commit(self):
        self.committed += 1


def _run_persist(*, legacy_extra_data, new_tokens, ok_token_keys,
                 evicted_token_keys=()):
    """Drive the real writer path; return (row, upserts, deletes)."""
    row = _FakeResourceRow(legacy_extra_data)
    session = _FakeSession(row)
    upserts = []
    deletes = []
    fp_deletes = []
    bulk_calls = []

    w = _bw.BackupWorker.__new__(_bw.BackupWorker)
    w.worker_id = "test-worker"

    async def _fake_upsert(resource_id, folder_id, token):
        upserts.append((str(folder_id), token))

    async def _fake_delete(resource_id, folder_id):
        deletes.append(str(folder_id))

    async def _fake_fp_upsert(resource_id, folder_id, t, u, s):
        return None

    async def _fake_fp_delete(resource_id, folder_id):
        fp_deletes.append(str(folder_id))
        return True

    async def _fake_bulk(resource_id, folder_ids):
        bulk_calls.append([str(f) for f in folder_ids])
        return True

    w._upsert_mail_folder_delta = _fake_upsert  # type: ignore[assignment]
    w._delete_mail_folder_delta = _fake_delete  # type: ignore[assignment]
    w._upsert_mail_folder_fingerprint = _fake_fp_upsert  # type: ignore[assignment]
    w._delete_mail_folder_fingerprint = _fake_fp_delete  # type: ignore[assignment]
    w._delete_mail_folder_state_bulk = _fake_bulk  # type: ignore[assignment]
    _run_persist.last_bulk_calls = bulk_calls  # type: ignore[attr-defined]

    orig_factory = _bw.async_session_factory
    _bw.async_session_factory = lambda: session  # type: ignore[assignment]
    try:
        asyncio.run(
            w._persist_mail_delta_state(
                uuid.uuid4(),
                new_tokens=dict(new_tokens),
                ok_token_keys=set(ok_token_keys),
                drained_fids=set(),
                fingerprints_now={},
                evicted_token_keys=set(evicted_token_keys),
            )
        )
    finally:
        _bw.async_session_factory = orig_factory  # type: ignore[assignment]
    return row, upserts, deletes, fp_deletes


def test_failed_rewalk_evicts_the_dead_token_from_both_writers():
    """THE regression. Folder 410s, the one re-walk also fails → no fresh
    cursor. The dead token must be deleted from the authoritative table AND
    popped from the legacy mirror, or the dual-read hands it straight back
    to the next run."""
    row, upserts, deletes, fp_deletes = _run_persist(
        legacy_extra_data={
            "mail_delta_tokens_by_folder": {HALO: DEAD_CURSOR},
        },
        new_tokens={},          # re-walk failed → no token captured
        ok_token_keys=set(),    # folder not drained OK
        evicted_token_keys={HALO},
    )
    assert deletes == [HALO], "the mail_folder_delta row must be deleted"
    assert fp_deletes == [HALO], "the fingerprint row must go with the cursor"
    legacy = row.extra_data.get("mail_delta_tokens_by_folder") or {}
    assert HALO not in legacy, (
        "the legacy mirror still holds the dead cursor — the dual-read "
        "({**legacy, **new_table}) makes the mirror WIN when the table row "
        "is gone, so the next run resumes the poisoned token again"
    )
    assert upserts == []


def test_successful_rewalk_keeps_its_fresh_cursor_and_is_not_evicted():
    """The re-walk succeeded: the folder is drained OK and earned a new
    token. Eviction must not wipe what the same run just rescued."""
    row, upserts, deletes, fp_deletes = _run_persist(
        legacy_extra_data={
            "mail_delta_tokens_by_folder": {HALO: DEAD_CURSOR},
        },
        new_tokens={HALO: FRESH_CURSOR},
        ok_token_keys={HALO},
        evicted_token_keys={HALO},
    )
    assert deletes == [], "a rescued folder must not have its row deleted"
    assert fp_deletes == []
    assert upserts == [(HALO, FRESH_CURSOR)]
    legacy = row.extra_data.get("mail_delta_tokens_by_folder") or {}
    assert legacy.get(HALO) == FRESH_CURSOR


def test_eviction_never_touches_sibling_folders():
    row, _upserts, deletes, fp_deletes = _run_persist(
        legacy_extra_data={
            "mail_delta_tokens_by_folder": {
                HALO: DEAD_CURSOR, SENT: "keep-me",
            },
        },
        new_tokens={},
        ok_token_keys=set(),
        evicted_token_keys={HALO},
    )
    assert deletes == [HALO]
    assert fp_deletes == [HALO]
    legacy = row.extra_data.get("mail_delta_tokens_by_folder") or {}
    assert legacy.get(SENT) == "keep-me"
    assert HALO not in legacy


def test_no_eviction_argument_is_a_no_op():
    """Existing callers (and the no-410 run) must behave exactly as before."""
    row, upserts, deletes, fp_deletes = _run_persist(
        legacy_extra_data={
            "mail_delta_tokens_by_folder": {SENT: "old"},
        },
        new_tokens={SENT: "new"},
        ok_token_keys={SENT},
    )
    assert deletes == []
    assert fp_deletes == []
    assert upserts == [(SENT, "new")]
    assert (row.extra_data.get("mail_delta_tokens_by_folder") or {})[SENT] == "new"


def test_multi_folder_eviction_routes_through_the_bulk_writer():
    """A carry rewind / force-full evicts many folders: one session, two
    bulk DELETEs — not 2N single-row sessions."""
    other = "AAMkAGI2-other-folder"
    row, upserts, deletes, fp_deletes = _run_persist(
        legacy_extra_data={"mail_delta_tokens_by_folder": {HALO: "old", other: "old2"}},
        new_tokens={},
        ok_token_keys=set(),
        evicted_token_keys=[HALO, other],
    )
    assert deletes == [] and fp_deletes == []
    assert _run_persist.last_bulk_calls == [sorted([HALO, other])]  # type: ignore[attr-defined]
    assert HALO not in row.extra_data["mail_delta_tokens_by_folder"]
    assert other not in row.extra_data["mail_delta_tokens_by_folder"]


def _persist_with(session_cls, *, evicted, get_returns_row=True):
    """Drive the real gated writer with fully-faked table helpers (all
    succeed) and a custom session class; return evict_result."""
    row = _FakeResourceRow({"mail_delta_tokens_by_folder": {HALO: "old"},
                            "mail_folder_fingerprints": {HALO: "1|1|1"}})
    session = session_cls(row if get_returns_row else None)
    w = _bw.BackupWorker.__new__(_bw.BackupWorker)
    w.worker_id = "test-worker"

    async def _ok(*a, **k):
        return True

    w._upsert_mail_folder_delta = _ok  # type: ignore[assignment]
    w._delete_mail_folder_delta = _ok  # type: ignore[assignment]
    w._delete_mail_folder_fingerprint = _ok  # type: ignore[assignment]
    w._delete_mail_folder_state_bulk = _ok  # type: ignore[assignment]
    w._upsert_mail_folder_fingerprint = _ok  # type: ignore[assignment]
    er = {}
    orig = _bw.async_session_factory
    _bw.async_session_factory = lambda: session  # type: ignore[assignment]
    try:
        asyncio.run(w._persist_mail_delta_state(
            uuid.uuid4(), new_tokens={}, ok_token_keys=set(), drained_fids=set(),
            fingerprints_now={}, evicted_token_keys=set(evicted), evict_result=er,
        ))
    finally:
        _bw.async_session_factory = orig  # type: ignore[assignment]
    return er


class _FailingCommitSession(_FakeSession):
    async def commit(self):
        raise RuntimeError("connection dropped")


def test_rewind_verdict_is_true_only_when_the_legacy_mirror_also_landed():
    assert _persist_with(_FakeSession, evicted=[HALO])["ok"] is True
    # the dual-read gives the mirror's value to any folder without a table
    # row, so a swallowed mirror failure resurrects the evicted cursor
    assert _persist_with(_FailingCommitSession, evicted=[HALO])["ok"] is False
    assert _persist_with(_FakeSession, evicted=[HALO], get_returns_row=False)["ok"] is False
