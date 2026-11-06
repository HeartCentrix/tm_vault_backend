"""One persistently failing drive held back EVERY drive cursor of the site, so each
later run re-downloaded a growing change set for all libraries. A drive's deltaLink is
only registered after that drive's walk completes, and a download failure leaves a hole
only in its own drive's delta stream — so only that drive's cursor is held back.
Enumeration failures, and failures that cannot be attributed to a drive, still hold all."""
from __future__ import annotations

import pathlib
import re

_SRC = (pathlib.Path(__file__).resolve().parents[2] / "workers" / "backup-worker" / "main.py").read_text()


def _load(name):
    start = _SRC.index(f"def {name}(")
    end = _SRC.index("\ndef ", start + 1)
    ns: dict = {}
    exec("from typing import Any, Dict\n" + _SRC[start:end], ns)
    return ns[name]


advance = _load("sharepoint_advanceable_drive_tokens")
TOKENS = {"site": {"d1": "t1", "d2": "t2"}, "sub": {"d3": "t3"}}


def test_a_failed_drive_holds_back_only_its_own_cursor():
    stats = {"drive_fail": 4, "failed_drive_ids": {"d1"}, "new_drive_tokens": TOKENS}
    assert advance(stats) == {"site": {"d2": "t2"}, "sub": {"d3": "t3"}}


def test_clean_run_advances_every_drive():
    assert advance({"new_drive_tokens": TOKENS}) == TOKENS


def test_enumeration_failure_holds_back_every_drive():
    assert advance({"enum_fail": 1, "new_drive_tokens": TOKENS}) == {}


def test_failure_without_a_drive_id_holds_back_every_drive():
    assert advance({"drive_fail": 1, "new_drive_tokens": TOKENS}) == {}
    assert advance({"drive_fail": 1, "failed_drive_ids": {None}, "new_drive_tokens": TOKENS}) == {}


def test_consumer_records_the_failing_drive():
    body = _SRC[_SRC.index("async def drive_consumer():"):_SRC.index("async def rest_consumer():")]
    assert body.count('stats["drive_fail"] += 1') == body.count('failed_drive_ids", set()).add(item.get("_drive_id"))') == 2


def test_both_cursor_writes_use_the_advanceable_tokens():
    assert len(re.findall(r"in _sp_advance_tokens\.items\(\)", _SRC)) == 2
