"""Three silent-data-loss swallows in the backup worker (2026-10-14).

1. A null subject sank a 50-message mailbox batch. Graph returns
   ``"subject": null`` for subject-less mail; ``msg.get("subject", msg_id)``
   keeps the None because the key IS present, and ``snapshot_items.name``
   is NOT NULL — so one such message rejected the whole multi-row INSERT it
   rode in. Proven in prod: 9 batches of Corp_Esplanade "Sent Items", 0 rows
   persisted for every message in them, snapshot still COMPLETED.

2. ``_backup_contacts_for_user`` wrapped the ``/contactFolders`` listing in
   ``except Exception: pass``. With no folder list the per-folder delta walks
   never run, so every contact filed under a custom folder is absent from
   the run — and nothing said so; the snapshot settled COMPLETED.

3. The USER_CHATS claim-skip path copies CHAT_ATTACHMENT /
   CHAT_HOSTED_CONTENT pointer rows in a step whose failure was only
   printed. ``_pointer_ok`` stayed True, the chat counted ``ok`` and the
   snapshot finalized COMPLETED with the attachment rows missing from this
   restore point.

Each fix routes through what the honesty gates already use —
``SNAPSHOT_DEGRADED_REASON_KEY`` via ``_stamp_snapshot_degraded`` — so
``terminal_status_for_extra_data`` settles PARTIAL at both finalize sites.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import re
import sys

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_null_subject_swallows", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_null_subject_swallows"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )

_SRC = _MAIN_PATH.read_text()


# ═══ (1) null subject ═════════════════════════════════════════════════════

class TestEmailItemName:
    def test_null_subject_falls_back_to_the_message_id(self):
        """THE defect: Graph sends the key with a null value."""
        assert _bw._email_item_name({"id": "AAMk1", "subject": None}, "AAMk1") == "AAMk1"

    def test_empty_subject_is_treated_as_missing(self):
        assert _bw._email_item_name({"subject": ""}, "AAMk2") == "AAMk2"

    def test_missing_subject_falls_back_to_the_message_id(self):
        assert _bw._email_item_name({"id": "AAMk3"}, "AAMk3") == "AAMk3"

    def test_present_subject_is_used(self):
        assert _bw._email_item_name({"subject": "Re: closing docs"}, "x") == "Re: closing docs"

    def test_a_batch_with_one_null_subject_has_no_null_name(self):
        """The failure shape: 50 rows in one INSERT, one of them NULL."""
        batch = [{"id": f"m{i}", "subject": f"s{i}"} for i in range(49)]
        batch.append({"id": "m49", "subject": None})
        names = [_bw._email_item_name(m, m["id"]) for m in batch]
        assert all(isinstance(n, str) and n for n in names)
        assert names[-1] == "m49"


def test_both_email_row_builders_use_the_helper():
    """Tier-1 ``backup_batch`` dict rows and the ``backup_message_batch``
    ORM rows — the two EMAIL writers that took name straight from Graph."""
    assert _SRC.count("_email_item_name(msg, msg_id)") == 2
    assert 'name=msg.get("subject", msg_id)' not in _SRC
    assert '"name": msg.get("subject", msg_id)' not in _SRC


# ═══ (2) contacts folder enumeration ═════════════════════════════════════

class TestContactsEnumerationReason:
    def test_no_errors_means_no_reason(self):
        assert _bw.contacts_enumeration_degraded_reason(None) is None
        assert _bw.contacts_enumeration_degraded_reason([]) is None
        assert _bw.contacts_enumeration_degraded_reason(["", None]) is None

    def test_reason_names_the_failure(self):
        r = _bw.contacts_enumeration_degraded_reason(
            ["/contactFolders: HTTPStatusError: 503 Service Unavailable"],
        )
        assert r is not None
        assert "contact folder enumeration failed" in r
        assert "503 Service Unavailable" in r

    def test_reason_is_capped_at_the_stamp_truncation(self):
        r = _bw.contacts_enumeration_degraded_reason(["x" * 900])
        assert len(r) <= 300

    def test_reason_settles_partial_at_the_finalize(self):
        r = _bw.contacts_enumeration_degraded_reason(["/contactFolders: boom"])
        status, reason = _bw.terminal_status_for_extra_data(
            {_bw.SNAPSHOT_DEGRADED_REASON_KEY: r},
        )
        assert status is _bw.SnapshotStatus.PARTIAL
        assert reason == r


class _FakeGraph:
    """Minimal GraphClient stand-in for ``_backup_contacts_for_user``."""
    GRAPH_URL = "https://graph.test/v1.0"

    def __init__(self, *, folders_fail: bool):
        self.folders_fail = folders_fail
        self.calls: list = []

    async def _get(self, url, params=None):
        self.calls.append(url)
        if url.endswith("/contactFolders"):
            if self.folders_fail:
                raise RuntimeError("503 Service Unavailable")
            return {"value": [{"id": "f1", "displayName": "Vendors"}]}
        if "/contacts/delta" in url:
            return {
                "value": [{"id": "c1", "displayName": "Ada", "parentFolderId": "root"}],
                "@odata.deltaLink": url.split("?")[0] + "?$deltatoken=t1",
            }
        # Deleted / Recoverable Items walks — not under test.
        raise RuntimeError("not in this test")


def test_folder_enumeration_failure_is_logged_recorded_and_default_walk_kept(capsys):
    """The swallow, made loud: the error reaches the caller's sink, the
    operator log names the user, and the default-folder walk still runs."""
    g = _FakeGraph(folders_fail=True)
    sink: list = []
    rows, deltas, expired = asyncio.run(
        _bw._backup_contacts_for_user(g, "u1", enumeration_errors=sink)
    )
    assert sink == ["/contactFolders: RuntimeError: 503 Service Unavailable"]
    assert "[CONTACTS] folder enumeration failed for u1: RuntimeError: 503 Service Unavailable" in capsys.readouterr().out
    # default-folder walk kept
    assert any(u.endswith("/users/u1/contacts/delta") for u in g.calls)
    assert [(r[0], r[1], r[2], r[4]) for r in rows] == [("USER_CONTACT", "Ada", "c1", "Contacts")]
    assert "__root__" in deltas
    assert expired == set()
    # and no per-folder walk could have run
    assert not any("/contactFolders/" in u for u in g.calls)


def test_folder_enumeration_success_records_nothing(capsys):
    """Negative control: a clean listing leaves the sink empty and walks
    the custom folder."""
    g = _FakeGraph(folders_fail=False)
    sink: list = []
    rows, _deltas, _expired = asyncio.run(
        _bw._backup_contacts_for_user(g, "u1", enumeration_errors=sink)
    )
    assert sink == []
    assert "[CONTACTS] folder enumeration failed" not in capsys.readouterr().out
    assert any(u.endswith("/contactFolders/f1/contacts/delta") for u in g.calls)
    assert _bw.contacts_enumeration_degraded_reason(sink) is None


def test_contacts_helper_still_works_without_a_sink():
    """The kwarg is optional — existing callers/fakes keep their shape."""
    g = _FakeGraph(folders_fail=True)
    out = asyncio.run(_bw._backup_contacts_for_user(g, "u1"))
    assert len(out) == 3


def test_user_contacts_handler_plumbs_the_sink_into_the_degraded_stamp():
    i = _SRC.find('if kind == "USER_CONTACTS":')
    j = _SRC.find('if kind == "USER_CALENDAR":', i)
    assert i != -1 and j != -1
    blk = _SRC[i:j]
    assert "enumeration_errors=_contact_enum_errors" in blk
    assert "contacts_enumeration_degraded_reason(" in blk
    assert "_stamp_snapshot_degraded(" in blk, (
        "the reason must reach the row both finalize sites re-read"
    )
    assert "except Exception:\n        pass" not in _SRC[
        _SRC.find("async def _backup_contacts_for_user("):
        _SRC.find("async def _fetch_contacts(url):")
    ], "the /contactFolders swallow is back"


# ═══ (3) chat attachment-copy ════════════════════════════════════════════

class TestFoldChatAttachmentCopyFailures:
    def test_zero_failures_leave_a_clean_verdict_alone(self):
        assert _bw.fold_chat_attachment_copy_failures(None, None, 0) == (None, None)

    def test_failures_turn_a_clean_run_partial(self):
        status, reason = _bw.fold_chat_attachment_copy_failures(None, None, 3)
        assert status is _bw.SnapshotStatus.PARTIAL
        assert reason == "3 chat attachment copies failed"

    def test_singular(self):
        _s, reason = _bw.fold_chat_attachment_copy_failures(None, None, 1)
        assert reason == "1 chat attachment copy failed"

    def test_failures_append_to_an_existing_partial_reason(self):
        status, reason = _bw.fold_chat_attachment_copy_failures(
            _bw.SnapshotStatus.PARTIAL, "2 of 40 chat drain(s) failed", 2,
        )
        assert status is _bw.SnapshotStatus.PARTIAL
        assert reason == "2 of 40 chat drain(s) failed; 2 chat attachment copies failed"

    def test_failed_verdict_is_untouched(self):
        got = _bw.fold_chat_attachment_copy_failures(
            _bw.SnapshotStatus.FAILED, "nothing persisted", 5,
        )
        assert got == (_bw.SnapshotStatus.FAILED, "nothing persisted")

    def test_garbage_count_is_ignored(self):
        assert _bw.fold_chat_attachment_copy_failures(None, None, "x") == (None, None)
        assert _bw.fold_chat_attachment_copy_failures(None, None, None) == (None, None)

    def test_reason_settles_partial_at_the_finalize(self):
        _s, reason = _bw.fold_chat_attachment_copy_failures(None, None, 4)
        status, settled = _bw.terminal_status_for_extra_data(
            {_bw.SNAPSHOT_DEGRADED_REASON_KEY: reason},
        )
        assert status is _bw.SnapshotStatus.PARTIAL
        assert settled == reason


def test_att_copy_failure_is_counted_where_it_was_swallowed():
    """The except that only printed now bumps the counter; the retry/claim
    logic around it is untouched (``_pointer_ok`` stays True — the chat's
    messages ARE pointed at)."""
    assert '_chat_att_copy = {"failed": 0}' in _SRC
    call = _SRC.find("_att_copied = await _copy_chat_attachment_rows_to_snapshot(")
    assert call != -1
    blk = _SRC[call: call + 1200]
    m = re.search(r"except Exception as _ae:\s*\n(.*?)print\(", blk, re.S)
    assert m, "att-copy except block missing"
    assert '_chat_att_copy["failed"] += 1' in m.group(1)
    assert "_pointer_ok = False" not in blk[: blk.find("except Exception as _se:")], (
        "an attachment-copy failure is not a failed drain"
    )


def test_att_copy_failures_feed_the_chat_honesty_gate():
    i = _SRC.find("_chat_honesty_status, _chat_honesty_reason = (")
    j = _SRC.find("_chat_honesty_status is SnapshotStatus.FAILED", i)
    assert i != -1 and j != -1
    blk = _SRC[i:j]
    assert "fold_chat_attachment_copy_failures(" in blk
    assert '_chat_att_copy["failed"]' in blk
    # the fold sits AFTER evaluate_drain_honesty so it can only add to the verdict
    assert blk.find("evaluate_drain_honesty(") < blk.find("fold_chat_attachment_copy_failures(")
