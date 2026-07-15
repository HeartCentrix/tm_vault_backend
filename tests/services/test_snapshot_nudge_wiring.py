"""Seam 1 (browse side) — snapshot-service browse actually delegates its on-demand
archive-enrichment nudge to the shared, tested nudge_enrichment.

Mirror of the export-side guard (test_archive_enrich_nudge.py::nudge_unenriched_
export). _maybe_trigger_archive_enrichment is a thin delegate; this proves it (a)
forwards (db, items, message_bus.publish) to nudge_enrichment and (b) no-ops when
there is no broker. The module is loaded once via importlib (hyphenated dir).
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import types

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "x" * 64)

_ROOT = pathlib.Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "snapsvc_main", str(_ROOT / "services" / "snapshot-service" / "main.py"))
snapsvc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(snapsvc)


@pytest.mark.asyncio
async def test_browse_delegates_to_shared_nudge(monkeypatch):
    import shared.archive_enrichment as eng
    captured = {}

    async def _fake_nudge(db, items, publish):
        captured["args"] = (db, items, publish)
        return ["rX"]

    monkeypatch.setattr(eng, "nudge_enrichment", _fake_nudge)
    fake_bus = types.SimpleNamespace(publish=lambda *a, **k: None)
    monkeypatch.setattr(snapsvc, "message_bus", fake_bus)

    db, items = object(), [object()]
    await snapsvc._maybe_trigger_archive_enrichment(db, items)

    assert captured["args"][0] is db                 # forwards the session
    assert captured["args"][1] is items              # forwards the browsed items
    assert captured["args"][2] == fake_bus.publish   # forwards the publisher


@pytest.mark.asyncio
async def test_no_broker_is_noop(monkeypatch):
    import shared.archive_enrichment as eng
    called = {"n": 0}

    async def _fake_nudge(*a, **k):
        called["n"] += 1

    monkeypatch.setattr(eng, "nudge_enrichment", _fake_nudge)
    monkeypatch.setattr(snapsvc, "message_bus", None)   # read API must still work

    await snapsvc._maybe_trigger_archive_enrichment(object(), [object()])
    assert called["n"] == 0
