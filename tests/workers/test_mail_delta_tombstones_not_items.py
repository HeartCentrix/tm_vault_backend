"""Graph delta deletion markers must not be persisted as live emails.

Live report (2026-08-07, Jason Rue's Recovery view). Every visible row in
the Mail tab read "(no subject)", dated "Aug 7, 2026". The mail was NOT
lost — 449,316 rows carried real subjects. What filled the list were
13,249 Graph delta TOMBSTONES stored as ordinary EMAIL rows:

    {"raw": {"@odata.type": "#microsoft.graph.message",
             "id": "AAMk...",
             "@removed": {"reason": "deleted"}}}

A removal carries no `subject`, so `m.get("subject") or "(no subject)"`
(main.py:5321 / :5408) stored the literal string, and nothing set
`is_deleted`, so it defaulted to false.

They monopolised the list because they carry no `receivedDateTime` AND no
`sentDateTime` (verified: 0 of 13,249 had either). The /mail sort is
COALESCE(received, sent, created_at) DESC, so every tombstone fell
through to `created_at` — the backup run's own timestamp — and outranked
all 435,229 properly-dated real emails.

The OneDrive walk already got this right (main.py:5835): it calls
`is_delta_tombstone` and `continue`s, with the comment "we must not
re-download or emit them as live files". The mail walk never adopted it.
Same rule now applies to mail.

Both accumulators in the page loop must skip, not just one: `local_out`
feeds the legacy tuple persist and `page_raw_msgs` feeds the streaming
persist, so filtering only one still leaks tombstones by the other route.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_mail_tombstones", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_mail_tombstones"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )

is_delta_tombstone = _bw.is_delta_tombstone

# The exact row shape observed in prod for resource 8dbfc7c8 (Mail — Jason Rue).
PROD_TOMBSTONE = {
    "@odata.type": "#microsoft.graph.message",
    "id": "AAMkAGQ4OTc2YjdkLWY0NTItNDU4NC05NmZiLTk4Nzk3MGIzNDRhYQBGAAA=",
    "@removed": {"reason": "deleted"},
}

REAL_MESSAGE = {
    "@odata.type": "#microsoft.graph.message",
    "id": "AAMkREAL=",
    "subject": "[JIRA] (ITO-10028) DRBACK14 — replace failed HDD in bay",
    "receivedDateTime": "2026-08-06T18:22:11Z",
    "sentDateTime": "2026-08-06T18:22:09Z",
    "parentFolderId": "AAMkFOLDER=",
}


class TestTombstoneDetection:
    def test_the_exact_prod_tombstone_is_detected(self):
        assert is_delta_tombstone(PROD_TOMBSTONE) is True

    def test_deleted_facet_form_is_also_detected(self):
        """Graph expresses removals two ways — main.py:1568."""
        assert is_delta_tombstone({"id": "x", "deleted": {"state": "deleted"}}) is True

    def test_a_real_message_is_not_a_tombstone(self):
        assert is_delta_tombstone(REAL_MESSAGE) is False

    def test_a_subjectless_but_real_message_is_not_a_tombstone(self):
        """145 of jrue's rows are genuinely subject-less real mail — those
        must still be backed up."""
        real_no_subject = {
            "id": "AAMkNOSUBJ=",
            "receivedDateTime": "2026-08-01T10:00:00Z",
            "parentFolderId": "AAMkFOLDER=",
        }
        assert is_delta_tombstone(real_no_subject) is False


class TestMailPageFilteringSkipsBothAccumulators:
    """Mirrors the page loop at main.py:5397-5414, which appends to
    local_out (legacy tuple persist) and page_raw_msgs (streaming persist).
    Both must skip tombstones or they leak by the unfiltered route."""

    @staticmethod
    def _run_page_loop(values):
        local_out = []
        page_raw_msgs = []
        for m in values:
            if is_delta_tombstone(m):
                continue
            local_out.append(("EMAIL", m.get("subject") or "(no subject)", m.get("id")))
            if isinstance(m, dict):
                page_raw_msgs.append(m)
        return local_out, page_raw_msgs

    def test_tombstones_reach_neither_accumulator(self):
        local_out, page_raw = self._run_page_loop([PROD_TOMBSTONE, REAL_MESSAGE])

        assert len(local_out) == 1
        assert len(page_raw) == 1
        assert local_out[0][2] == "AAMkREAL="
        assert page_raw[0]["id"] == "AAMkREAL="

    def test_no_row_is_ever_named_no_subject_by_a_removal(self):
        local_out, _ = self._run_page_loop([PROD_TOMBSTONE] * 13249)
        assert local_out == []

    def test_an_all_tombstone_page_persists_nothing(self):
        local_out, page_raw = self._run_page_loop([PROD_TOMBSTONE] * 50)
        assert (local_out, page_raw) == ([], [])

    def test_real_mail_survives_a_mixed_page(self):
        page = [PROD_TOMBSTONE, REAL_MESSAGE, PROD_TOMBSTONE, REAL_MESSAGE]
        local_out, page_raw = self._run_page_loop(page)
        assert len(local_out) == 2
        assert all(r[1].startswith("[JIRA]") for r in local_out)
