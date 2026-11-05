"""Calendar backup keeps every series master and every event outside the calendarView window;
restore rebuilds a series from its master instead of posting each occurrence too.

Railway 2026-11-05 (full 10-user run vs direct Graph): stored occurrences referenced 159 recurring
series but only 60 series masters were stored, so 99 series could not be restored or exported as
series; and events older than 2 years or more than 1 year ahead (the calendarView window) were not
backed up at all (holiday calendars: 70 stored vs 94-154 in Graph).

Helpers are loaded by AST (not import) to avoid the workers' heavy import side effects."""
from __future__ import annotations

import ast
import asyncio
import pathlib
from types import SimpleNamespace

_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _load(path, name):
    tree = ast.parse((_ROOT / path).read_text())
    ns: dict = {}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), ns)
    return ns[name]


rows_for = _load("workers/backup-worker/main.py", "_calendar_series_and_single_rows")
children_to_skip = _load("workers/restore-worker/main.py", "_series_children_restored_by_master")


class Graph:
    GRAPH_URL = "https://graph.microsoft.com/v1.0"

    def __init__(self, events):
        self.events, self.urls = events, []

    async def _iter_pages(self, url, params=None):
        self.urls.append(url)
        yield {"value": self.events}


def test_backup_adds_series_masters_and_events_outside_the_window_once():
    graph = Graph([
        {"id": "M1", "type": "seriesMaster", "subject": "Daily standup", "recurrence": {"pattern": {}}},
        {"id": "OLD", "type": "singleInstance", "subject": "Kickoff 2021"},
        {"id": "IN", "type": "singleInstance", "subject": "Already in the view"},
    ])
    seen = {"IN", "occ-1"}
    rows = asyncio.run(rows_for(graph, "user-1", "cal-1", "Calendar", seen))
    assert [r[2] for r in rows] == ["M1", "OLD"]
    assert rows[0][0] == "CALENDAR_EVENT" and rows[0][4] == "Calendar/Calendar"
    assert rows[0][3]["type"] == "seriesMaster" and rows[0][3]["raw"]["recurrence"]
    # No date window: the plain events list returns every master and single event.
    assert graph.urls == ["https://graph.microsoft.com/v1.0/users/user-1/calendars/cal-1/events?$top=500"]
    assert {"M1", "OLD"} <= seen


def test_restore_skips_children_whose_master_is_restored_with_them():
    def item(i, ext, raw):
        return SimpleNamespace(id=i, external_id=ext, item_type="CALENDAR_EVENT", raw=raw)
    items = [
        item(1, "M1", {"type": "seriesMaster"}),
        item(2, "occ-1", {"type": "occurrence", "seriesMasterId": "M1"}),
        item(3, "exc-1", {"type": "exception", "seriesMasterId": "M1"}),
        item(4, "occ-9", {"type": "occurrence", "seriesMasterId": "M9"}),   # master not backed up
        item(5, "single", {"type": "singleInstance"}),
    ]
    assert children_to_skip(items, lambda it: it.raw) == {2, 3}
