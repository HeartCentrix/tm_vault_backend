"""USER_MAIL cursor advance: ONE gate, BOTH writers (call-site wiring).

Bug (adversarial review, 2026-07-31 — the half-applied fix): the
per-folder checkpoint and the end-of-run `mail_folder_delta` UPSERT both
honoured "drained AND persisted", but the legacy
`extra_data.mail_delta_tokens_by_folder` mirror 40 lines below merged
EVERY captured deltaLink unconditionally. The read side is
``{**legacy, **new_table}``, so the legacy value WINS for any folder with
no `mail_folder_delta` row yet — precisely the failed-first-drain case the
gate exists to protect. Inbox pages 1-3 persist, page 4's persist raises →
the folder is held back everywhere except the mirror → next run resumes
from the advanced cursor and page 4's messages are gone permanently.

These tests drive the REAL writer path (`_persist_mail_delta_state`) with
a fake session factory, so a gate applied to only one of the two writers
fails here. The pure-function tests in test_mail_fingerprint_gating.py
could not see it — that is the whole point of this file.
"""
from __future__ import annotations

import asyncio
import ast
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
_spec = importlib.util.spec_from_file_location("bw_mail_dualwrite", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_mail_dualwrite"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )


INBOX = "inbox-folder-id-aaaaaaaa"
SENT = "sent-folder-id-bbbbbbbb"
OLD_INBOX_CURSOR = "https://graph/delta?$deltatoken=INBOX_RUN_1"
NEW_INBOX_CURSOR = "https://graph/delta?$deltatoken=INBOX_RUN_2"
NEW_SENT_CURSOR = "https://graph/delta?$deltatoken=SENT_RUN_2"


class _FakeResourceRow:
    def __init__(self, extra_data):
        self.extra_data = dict(extra_data or {})


class _FakeSession:
    """Minimal AsyncSession stand-in for the legacy mirror's RMW block."""

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


def _worker():
    w = _bw.BackupWorker.__new__(_bw.BackupWorker)
    w.worker_id = "test-worker"
    return w


def _run_persist(*, legacy_extra_data, new_tokens, ok_token_keys,
                 drained_fids=(), fingerprints_now=None, monkeypatch=None):
    """Drive the real writer path; return (resource_row, delta_upserts, fp_upserts)."""
    row = _FakeResourceRow(legacy_extra_data)
    session = _FakeSession(row)
    delta_upserts = []
    fp_upserts = []

    w = _worker()

    async def _fake_delta_upsert(resource_id, folder_id, token):
        delta_upserts.append((str(folder_id), token))

    async def _fake_fp_upsert(resource_id, folder_id, t, u, s):
        fp_upserts.append((str(folder_id), t, u, s))

    w._upsert_mail_folder_delta = _fake_delta_upsert  # type: ignore[assignment]
    w._upsert_mail_folder_fingerprint = _fake_fp_upsert  # type: ignore[assignment]

    orig_factory = _bw.async_session_factory
    _bw.async_session_factory = lambda: session  # type: ignore[assignment]
    try:
        asyncio.run(
            w._persist_mail_delta_state(
                uuid.uuid4(),
                new_tokens=dict(new_tokens),
                ok_token_keys=set(ok_token_keys),
                drained_fids=set(drained_fids),
                fingerprints_now=dict(fingerprints_now or {}),
            )
        )
    finally:
        _bw.async_session_factory = orig_factory  # type: ignore[assignment]
    return row, delta_upserts, fp_upserts


# ─── the blocker: both writers must honour the same gate ──────────────────

def test_held_back_folder_is_not_advanced_by_the_legacy_mirror():
    """Inbox drained but a page-persist failed → NEITHER writer may advance.

    The legacy mirror is the one that used to advance it anyway, and for a
    folder with no mail_folder_delta row the mirror's value is what the
    next run reads.
    """
    row, delta_upserts, _fp = _run_persist(
        # Prior run left a cursor in the legacy dict only (no table row) —
        # the dual-read case where legacy WINS.
        legacy_extra_data={
            "mail_delta_tokens_by_folder": {INBOX: OLD_INBOX_CURSOR},
        },
        new_tokens={INBOX: NEW_INBOX_CURSOR},
        ok_token_keys=set(),  # drain/persist incomplete for Inbox
    )
    assert delta_upserts == [], "held-back folder must not reach mail_folder_delta"
    legacy = row.extra_data.get("mail_delta_tokens_by_folder") or {}
    assert legacy.get(INBOX) == OLD_INBOX_CURSOR, (
        "legacy mirror advanced the cursor of a folder whose pages were "
        "never persisted — next run resumes past unstored mail"
    )


def test_newly_discovered_held_back_folder_gets_no_legacy_cursor_at_all():
    """No prior row anywhere: the mirror must not CREATE the cursor either."""
    row, delta_upserts, _fp = _run_persist(
        legacy_extra_data={},
        new_tokens={INBOX: NEW_INBOX_CURSOR},
        ok_token_keys=set(),
    )
    assert delta_upserts == []
    legacy = row.extra_data.get("mail_delta_tokens_by_folder") or {}
    assert INBOX not in legacy, (
        "a first-drain-failed folder must have NO cursor, so the next run "
        "re-enumerates the folder in full"
    )


def test_mixed_run_advances_only_the_clean_folder_in_both_writers():
    row, delta_upserts, _fp = _run_persist(
        legacy_extra_data={
            "mail_delta_tokens_by_folder": {INBOX: OLD_INBOX_CURSOR},
        },
        new_tokens={INBOX: NEW_INBOX_CURSOR, SENT: NEW_SENT_CURSOR},
        ok_token_keys={SENT},
    )
    assert delta_upserts == [(SENT, NEW_SENT_CURSOR)]
    legacy = row.extra_data.get("mail_delta_tokens_by_folder") or {}
    assert legacy.get(SENT) == NEW_SENT_CURSOR
    assert legacy.get(INBOX) == OLD_INBOX_CURSOR


def test_clean_run_advances_both_writers():
    row, delta_upserts, fp_upserts = _run_persist(
        legacy_extra_data={
            "mail_delta_tokens_by_folder": {INBOX: OLD_INBOX_CURSOR},
        },
        new_tokens={INBOX: NEW_INBOX_CURSOR},
        ok_token_keys={INBOX},
        drained_fids={INBOX},
        fingerprints_now={INBOX: "10|2|4096"},
    )
    assert delta_upserts == [(INBOX, NEW_INBOX_CURSOR)]
    assert fp_upserts == [(INBOX, 10, 2, 4096)]
    legacy = row.extra_data.get("mail_delta_tokens_by_folder") or {}
    assert legacy.get(INBOX) == NEW_INBOX_CURSOR
    assert (row.extra_data.get("mail_folder_fingerprints") or {}).get(INBOX) == (
        "10|2|4096"
    )
    assert row.extra_data.get("mail_folder_baseline_at")


def test_root_scan_placeholder_uses_the_all_key_on_both_writers():
    """Empty folder tree → the run carries one cursor under "__all__"."""
    row, delta_upserts, _fp = _run_persist(
        legacy_extra_data={},
        new_tokens={"__all__": NEW_INBOX_CURSOR},
        ok_token_keys={"__all__"},
    )
    assert delta_upserts == [("__all__", NEW_INBOX_CURSOR)]
    legacy = row.extra_data.get("mail_delta_tokens_by_folder") or {}
    assert legacy.get("__all__") == NEW_INBOX_CURSOR


def test_sibling_folders_in_the_legacy_dict_are_preserved():
    """The mirror merges forward — a shard must not wipe sibling cursors."""
    other = "other-shard-folder-cccccccc"
    row, _d, _fp = _run_persist(
        legacy_extra_data={
            "mail_delta_tokens_by_folder": {other: "keep-me"},
        },
        new_tokens={INBOX: NEW_INBOX_CURSOR},
        ok_token_keys={INBOX},
    )
    legacy = row.extra_data.get("mail_delta_tokens_by_folder") or {}
    assert legacy.get(other) == "keep-me"
    assert legacy.get(INBOX) == NEW_INBOX_CURSOR


def test_fingerprint_mirror_is_independent_of_the_cursor_gate():
    """A folder held back for cursors also gets no fingerprint advance."""
    row, _d, fp_upserts = _run_persist(
        legacy_extra_data={},
        new_tokens={INBOX: NEW_INBOX_CURSOR},
        ok_token_keys=set(),
        drained_fids=set(),
        fingerprints_now={INBOX: "10|2|4096"},
    )
    assert fp_upserts == []
    assert INBOX not in (row.extra_data.get("mail_folder_fingerprints") or {})


# ─── the gate itself ──────────────────────────────────────────────────────

def test_gate_mail_delta_tokens_splits_allowed_from_held_back():
    allowed, held = _bw.gate_mail_delta_tokens(
        {INBOX: "a", SENT: "b"}, {SENT},
    )
    assert allowed == {SENT: "b"}
    assert held == [INBOX]


def test_gate_mail_delta_tokens_empty_inputs():
    assert _bw.gate_mail_delta_tokens(None, None) == ({}, [])
    assert _bw.gate_mail_delta_tokens({}, {INBOX}) == ({}, [])
    allowed, held = _bw.gate_mail_delta_tokens({INBOX: "a"}, None)
    assert allowed == {} and held == [INBOX]


# ─── single-source-of-truth pin: no second writer in the handler ──────────

def _usermail_region() -> str:
    src = _MAIN_PATH.read_text()
    start = src.find("mail_new_tokens: Dict[str, str] = {}")
    assert start != -1
    end = src.find("# ── Drain-failure honesty gate ──", start)
    assert end != -1
    return src[start:end]


def test_handler_has_no_second_ungated_cursor_writer():
    region = _usermail_region()
    assert "merged.update(mail_new_tokens)" not in region, (
        "the end-of-run legacy mirror must not merge raw captured tokens"
    )
    assert "_persist_mail_delta_state(" in region, (
        "the handler must route cursor persistence through the single gate"
    )


def test_mail_new_tokens_is_only_consumed_by_the_gated_writer():
    """Every read of the raw token dict must be the gate's own input."""
    src = _MAIN_PATH.read_text()
    tree = ast.parse(src)
    reads = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id == "mail_new_tokens":
            reads.append(node.lineno)
    # 1 init, 1 write inside the page loop, 1 keyword arg on the gated
    # writer call. Anything else is a second consumer that can bypass the
    # gate.
    assert len(reads) == 3, (
        f"unexpected mail_new_tokens references at lines {reads} — a new "
        f"consumer must go through gate_mail_delta_tokens"
    )
    region_src = _usermail_region()
    assert "new_tokens=mail_new_tokens," in region_src


def test_persist_helper_feeds_both_writers_from_the_gate():
    """AST pin: inside _persist_mail_delta_state, the legacy mirror merges
    the GATED map, never the raw argument."""
    src = _MAIN_PATH.read_text()
    tree = ast.parse(src)
    fn = None
    for node in ast.walk(tree):
        if (isinstance(node, ast.AsyncFunctionDef)
                and node.name == "_persist_mail_delta_state"):
            fn = node
            break
    assert fn is not None, "_persist_mail_delta_state must exist"
    body = ast.dump(fn)
    assert "gate_mail_delta_tokens" in body
    # `new_tokens` may appear ONLY as the gate's argument.
    raw_reads = [
        n.lineno for n in ast.walk(fn)
        if isinstance(n, ast.Name) and n.id == "new_tokens"
    ]
    assert len(raw_reads) == 1, (
        f"raw new_tokens read {len(raw_reads)}× (lines {raw_reads}); only "
        f"the gate call may read it"
    )
