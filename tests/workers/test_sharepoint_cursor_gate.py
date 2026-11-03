"""The SharePoint drive-delta cursor gate must not be self-perpetuating.

Bug (found 2026-08-11): the gate added 2026-08-04 withheld the drive delta
cursor when ANY per-item failure occurred, summing drive_fail + rest_fail +
enum_fail. Because SHAREPOINT_SITE "IT Projects" fails the same 152 files on
every run, the cursor was withheld on every run, so every run re-enumerated
~11,500 list rows from the 2026-08-04 baseline — for 7 days and ~21
consecutive runs, each settling PARTIAL with a different arbitrary subset
(1,856 / 2,400 / 2,262 / 3,190 items). The condition that withheld the cursor
was one that re-walking could never clear.

Those 152 are 19 out-of-the-box Web Part Gallery stubs
(AuthoredListFilter.webpart, BusinessDataFilter.dwp, …) across 8 subsites:
list rows whose Length is non-zero but whose bytes were never in the content
DB, so /$value 404s permanently.

The fix is narrow ON PURPOSE. rest_fail cannot leave a hole in the DRIVE
delta — the two are disjoint object spaces fed by different enumerations, and
the REST walk carries no token and re-enumerates in full every run. So REST
failures no longer gate. drive_fail and enum_fail still do, because advancing
past an undownloaded DRIVE item makes it permanently absent and SharePoint has
neither a retry ledger nor forceFullBackup support to recover it.

An earlier attempt at this fix was rejected in review for classifying
404/410/423 as "gone" and releasing the cursor on the DRIVE side as well. 423
Locked means the file EXISTS (checked out), and every failure actually
observed in 48h of fleet logs was REST-side — so it added hole-capable
behaviour to a path with no demonstrated problem. These tests pin the
narrowness, not just the unblocking.
"""
from __future__ import annotations

import pathlib
import re

_WORKER_MAIN = (
    pathlib.Path(__file__).resolve().parents[2]
    / "workers"
    / "backup-worker"
    / "main.py"
)
_SRC = _WORKER_MAIN.read_text()


def _load(name: str):
    """Lift a self-contained pure helper out of the 25k-line worker module.

    Importing the module pulls in settings/clients; these two functions are
    deliberately dependency-free so the decision can be tested for real
    instead of by grepping source text.
    """
    start = _SRC.index(f"def {name}(")
    end = _SRC.index("\ndef ", start + 1)
    ns: dict = {}
    exec("from typing import Any, Dict\n" + _SRC[start:end], ns)
    return ns[name]


blocking = _load("sharepoint_cursor_blocking_failures")
total = _load("sharepoint_total_failures")


# ── the regression itself ────────────────────────────────────────────────

def test_rest_failures_alone_do_not_block_the_cursor():
    """The exact prod shape of the 2026-08-11 18:11 IT Projects run."""
    stats = {"drive_fail": 0, "rest_fail": 152, "enum_fail": 0}
    assert blocking(stats) == 0, (
        "152 permanently-404 REST list files froze this site's drive cursor "
        "for 7 days; REST failures must not gate the drive delta"
    )


def test_the_run_still_reports_every_failure_honestly():
    """Unblocking the cursor must NOT quietly improve the failure count —
    the snapshot still has to settle PARTIAL with the true number."""
    stats = {"drive_fail": 0, "rest_fail": 152, "enum_fail": 0}
    assert total(stats) == 152


# ── the narrowness, which is the safety-critical half ────────────────────

def test_drive_failures_still_block_the_cursor():
    """Advancing past an undownloaded DRIVE item makes it permanently absent:
    an unchanged file never reappears in a later delta stream, and SharePoint
    has no retry ledger and does not honour forceFullBackup."""
    assert blocking({"drive_fail": 1, "rest_fail": 0, "enum_fail": 0}) == 1


def test_enumeration_failures_still_block_the_cursor():
    """A library that was never listed is exactly what re-enumeration fixes."""
    assert blocking({"drive_fail": 0, "rest_fail": 0, "enum_fail": 1}) == 1


def test_a_drive_failure_blocks_even_when_rest_failures_dominate():
    """The common real-world mix must not let 152 benign REST 404s mask one
    genuine drive failure into an advance."""
    assert blocking({"drive_fail": 1, "rest_fail": 152, "enum_fail": 0}) == 1


def test_clean_run_advances():
    assert blocking({"drive_fail": 0, "rest_fail": 0, "enum_fail": 0}) == 0
    assert total({}) == 0


def test_missing_and_none_counters_are_tolerated():
    """stats is assembled incrementally; absent keys must read as 0, not raise
    — an exception here would abort the whole SharePoint finalize."""
    assert blocking({}) == 0
    assert blocking({"drive_fail": None, "rest_fail": None, "enum_fail": None}) == 0
    assert total({"drive_fail": None, "rest_fail": 3}) == 3


# ── both cursor writes must share ONE predicate ──────────────────────────

def test_both_cursor_writes_gate_on_the_same_predicate():
    """The authoritative sharepoint_drive_delta upsert and the legacy
    extra_data dual-write must use the SAME gate, or a rollback to the legacy
    reader resumes from a cursor the authoritative path refused to advance.
    The original code called this out explicitly; keep it true."""
    gates = re.findall(r"and not (_sp_cursor_blocking|_sp_failed)\b", _SRC)
    assert gates, "the cursor-advance gates disappeared entirely"
    assert set(gates) == {"_sp_cursor_blocking"}, (
        f"cursor gates disagree or regressed to the total: {gates}"
    )


def test_total_is_still_what_gets_reported():
    """files_failed must keep using the TOTAL, not the narrowed count."""
    assert '"files_failed": _sp_failed' in _SRC


def test_no_gone_classification_was_reintroduced_on_the_drive_side():
    """Guards against re-landing the rejected approach: releasing the drive
    cursor for 404/410/423 'gone' files. 423 Locked means the file exists."""
    assert "_sp_error_is_gone" not in _SRC
    assert "rest_fail_gone" not in _SRC
    assert "drive_fail_gone" not in _SRC
