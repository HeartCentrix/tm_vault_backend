"""Microsoft occasionally returns a corrupt gzip body for a message's attachment list
(TM: 12x in 30 days, httpx.DecodingError). Amit, 2026-11-06: retry once; if it fails the same
way again, log the reason and hold the folder's cursor (next run re-reads it) without calling
the backup failed or partial. Any other error keeps today's handling."""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import re
import sys
from types import SimpleNamespace

import httpx
import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_SRC = _MAIN_PATH.read_text()
_spec = importlib.util.spec_from_file_location("bw_att_decoding", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)
sys.modules["bw_att_decoding"] = _bw
try:
    _spec.loader.exec_module(_bw)
except Exception as exc:  # pragma: no cover
    pytest.skip(f"backup-worker module failed to import: {exc}", allow_module_level=True)


def _run(errors):
    calls = []
    async def list_message_attachments(mailbox, msg_id):
        calls.append(msg_id)
        if errors:
            raise errors.pop(0)
        return []
    graph = SimpleNamespace(list_message_attachments=list_message_attachments)
    failed, held = set(), set()
    worker = SimpleNamespace(worker_id="t")
    asyncio.run(_bw.BackupWorker._backup_message_attachments(
        worker, graph, SimpleNamespace(external_id="mbx"), SimpleNamespace(id="s"), SimpleNamespace(id="t"),
        "container", None, [{"id": "m1"}], failed_out=failed, held_out=held))
    return calls, failed, held


def test_corrupt_response_is_retried_once_and_then_captured():
    calls, failed, held = _run([httpx.DecodingError("Error -3 while decompressing data")])
    assert calls == ["m1", "m1"] and not failed and not held


def test_corrupt_response_twice_holds_the_cursor_without_failing():
    calls, failed, held = _run([httpx.DecodingError("bad gzip"), httpx.DecodingError("bad gzip")])
    assert calls == ["m1", "m1"] and not failed and held == {"m1"}


def test_other_errors_still_fail_the_message():
    calls, failed, held = _run([RuntimeError("boom")])
    assert calls == ["m1"] and failed == {"m1"} and not held


def test_held_folders_keep_their_cursor_but_do_not_degrade_the_snapshot():
    flat = re.sub(r"\s+", " ", _SRC)
    i = flat.index("fresh_count, fresh_bytes, persist_failed_fids = await self._persist_tier1_mail_items(")
    window = flat[i:i + 1500]
    assert "held_out=held_fids" in window and "ok_keys -= held_fids" in window
