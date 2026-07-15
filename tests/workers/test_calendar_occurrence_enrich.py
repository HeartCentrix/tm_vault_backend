"""Recurring-event occurrences must inherit their subject from the series master.

Microsoft Graph's /calendarView/delta returns recurring occurrences as bare
stubs — only id/start/end/type/seriesMasterId, no subject (which lives on the
seriesMaster). Confirmed live: 510 of Amit's 512 calendar items stored as
"(no subject)". `_inherit_master_fields` merges the master's descriptive fields
onto each occurrence so it stores a real subject.

Loads the pure helper by AST (not import) to avoid the worker module's heavy
import side effects — same approach as test_pst_archive_export.py.
"""
from __future__ import annotations

import ast
import pathlib

_MAIN = (
    pathlib.Path(__file__).resolve().parents[2]
    / "workers" / "backup-worker" / "main.py"
)


def _load_symbols():
    tree = ast.parse(_MAIN.read_text())
    ns: dict = {}
    for node in tree.body:
        take = (
            isinstance(node, ast.Assign)
            and any(getattr(t, "id", None) == "CAL_INHERIT_FIELDS" for t in node.targets)
        ) or (
            isinstance(node, ast.FunctionDef) and node.name == "_inherit_master_fields"
        )
        if take:
            mod = ast.Module(body=[node], type_ignores=[])
            exec(compile(mod, str(_MAIN), "exec"), ns)
    return ns["_inherit_master_fields"]


_inherit_master_fields = _load_symbols()


def _occurrence(series_master="M1", name="(no subject)", subject=None):
    raw = {
        "id": "occ1",
        "type": "occurrence",
        "start": {"dateTime": "2026-07-08T13:45:00", "timeZone": "UTC"},
        "end": {"dateTime": "2026-07-08T14:00:00", "timeZone": "UTC"},
        "seriesMasterId": series_master,
    }
    if subject is not None:
        raw["subject"] = subject
    md = {"raw": raw, "seriesMasterId": series_master, "type": "occurrence"}
    return ("CALENDAR_EVENT", subject or name, "ext-occ1", md, "Calendar/Cal")


def test_inherits_subject_and_fields_from_master():
    master = {
        "subject": "Fasal Standup",
        "location": {"displayName": "Room 4"},
        "organizer": {"emailAddress": {"name": "Boss"}},
        "isOnlineMeeting": True,
    }
    out, n = _inherit_master_fields([_occurrence()], {"M1": master})
    assert n == 1
    _it, name, _ext, md, _folder = out[0]
    assert name == "Fasal Standup"
    assert md["raw"]["subject"] == "Fasal Standup"
    assert md["raw"]["location"] == {"displayName": "Room 4"}
    assert md["raw"]["isOnlineMeeting"] is True
    assert md["inheritedFromMaster"] is True
    # occurrence-specific fields must survive untouched
    assert md["raw"]["id"] == "occ1"
    assert md["raw"]["start"]["dateTime"] == "2026-07-08T13:45:00"


def test_no_master_in_cache_leaves_row_untouched():
    out, n = _inherit_master_fields([_occurrence()], {})
    assert n == 0
    assert out[0][1] == "(no subject)"
    assert "subject" not in out[0][3]["raw"]


def test_existing_subject_never_overwritten():
    out, n = _inherit_master_fields(
        [_occurrence(subject="Real One")], {"M1": {"subject": "MASTER"}}
    )
    assert n == 0
    assert out[0][1] == "Real One"
    assert out[0][3]["raw"]["subject"] == "Real One"


def test_non_calendar_rows_ignored():
    row = ("EMAIL", "hi", "e1", {"raw": {}, "seriesMasterId": "M1"}, "Inbox")
    out, n = _inherit_master_fields([row], {"M1": {"subject": "X"}})
    assert n == 0
    assert out[0][1] == "hi"


if __name__ == "__main__":
    test_inherits_subject_and_fields_from_master()
    test_no_master_in_cache_leaves_row_untouched()
    test_existing_subject_never_overwritten()
    test_non_calendar_rows_ignored()
    print("ok")
