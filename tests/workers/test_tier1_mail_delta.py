"""Tier-1 mailbox handler (MAILBOX / SHARED_MAILBOX / ROOM_MAILBOX →
`backup_mailbox`) must stop re-downloading the whole mailbox every run.

Why (fleet, 2026-09-04): `GraphClient.get_messages_delta` is a plain
``/users/{id}/messages?$top=999`` walk — Graph has no change tracking on the
all-folders collection — so it never yields a deltaLink and the handler
re-enumerated, re-serialised and re-uploaded every message AND re-listed /
re-downloaded every attachment on every tick: Corp_Esplanade (Gold, 8h)
25k items / 2.28 GB three times a day; 61 of 63 ROOM_MAILBOX snapshots in 7
days had new_item_count == item_count, ~50 GB re-uploaded.

Fix shape (pilot-gated): per-folder ``/mailFolders/{fid}/messages/delta``
with the USER_MAIL cursor machinery (mail_folder_delta rows keyed by the
Tier-1 resource id; no ``$top`` — it caps the whole round, see
test_mail_delta_no_top.py — page size via ``Prefer: odata.maxpagesize``),
the SAME Tier-1 row format (blob_path + inline raw + source_mailbox) so
Recovery/restore are unchanged, and carry-forward of the prior snapshot's
EMAIL / EMAIL_ATTACHMENT pointer rows so every snapshot stays a full
inventory (GFS-safe). Attachments are fetched only for messages the delta
actually returned.

Rollout safety: default OFF. ``TIER1_MAIL_DELTA_ENABLED=true`` turns it on
fleet-wide; ``TIER1_MAIL_DELTA_RESOURCE_IDS`` (comma list) pilots single
rooms. Everything else keeps the verbatim legacy full walk
(`_backup_mailbox_fullwalk`).
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
_SRC = _MAIN_PATH.read_text()

_spec = importlib.util.spec_from_file_location("bw_tier1_mail_delta", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_tier1_mail_delta"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )

RID = "1f880075-0000-4000-8000-000000000001"


# ── pure helpers ──────────────────────────────────────────────────────────

def test_tier1_delta_default_off():
    assert _bw.tier1_mail_delta_enabled(RID, env={}) is False


def test_tier1_delta_fleet_switch():
    assert _bw.tier1_mail_delta_enabled(RID, env={"TIER1_MAIL_DELTA_ENABLED": "true"}) is True
    assert _bw.tier1_mail_delta_enabled(RID, env={"TIER1_MAIL_DELTA_ENABLED": "false"}) is False


def test_tier1_delta_pilot_allowlist():
    env = {"TIER1_MAIL_DELTA_RESOURCE_IDS": f" other-id , {RID.upper()} "}
    assert _bw.tier1_mail_delta_enabled(RID, env=env) is True
    assert _bw.tier1_mail_delta_enabled("not-listed", env=env) is False


def test_snapshot_counters_legacy_result_unchanged():
    assert _bw.snapshot_counters_from_result({"item_count": 10, "bytes_added": 5}) == (10, 10, 5, 5)


def test_snapshot_counters_split_when_provided():
    r = {"item_count": 22908, "new_item_count": 12, "bytes_added": 900, "bytes_total": 2_277_000_000}
    assert _bw.snapshot_counters_from_result(r) == (22908, 12, 900, 2_277_000_000)


def test_snapshot_counters_missing_keys_are_zero():
    assert _bw.snapshot_counters_from_result({}) == (0, 0, 0, 0)


def test_mailbox_parallel_folders_clamp():
    assert _bw.mailbox_parallel_folders(None) == 4
    assert _bw.mailbox_parallel_folders("0") == 1
    assert _bw.mailbox_parallel_folders("99") == 16
    assert _bw.mailbox_parallel_folders("abc") == 4


# ── behaviour: the per-folder drain against a fake Graph client ──────────

class _FakeGraph:
    GRAPH_URL = "https://graph.microsoft.com/v1.0"

    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []

    async def _iter_pages(self, url, params=None, headers=None):
        self.calls.append({"url": url, "params": params, "headers": headers})
        for p in self.pages:
            yield p


def _worker():
    w = _bw.BackupWorker.__new__(_bw.BackupWorker)
    w.worker_id = "test-worker"
    return w


def _run(coro):
    return asyncio.run(coro)


def test_drain_bootstrap_has_no_top_and_prefer_header_on_the_delta_url():
    fake = _FakeGraph([
        {"value": [{"id": "m1", "subject": "a"}, {"id": "gone", "@removed": {"reason": "deleted"}}],
         "@odata.nextLink": "https://graph.microsoft.com/v1.0/x?$skiptoken=1"},
        {"value": [{"id": "m2", "subject": "b"}],
         "@odata.deltaLink": "https://graph.microsoft.com/v1.0/x?$deltatoken=D1"},
    ])
    out = _run(_worker()._drain_tier1_folder_delta(fake, "user-1", "fA", None))

    assert fake.calls[0]["url"].endswith("/users/user-1/mailFolders/fA/messages/delta")
    assert "$top" not in (fake.calls[0]["params"] or {})
    assert fake.calls[0]["headers"] == {"Prefer": "odata.maxpagesize=999"}
    assert [m["id"] for m in out["messages"]] == ["m1", "m2"]
    assert out["tombstoned"] == {"gone"}
    assert out["delta_token"].endswith("$deltatoken=D1")
    assert out["ok"] is True and out["evicted"] is False


def test_drain_resumes_saved_token_verbatim():
    saved = "https://graph.microsoft.com/v1.0/users/user-1/mailFolders/fA/messages/delta?$deltatoken=OLD"
    fake = _FakeGraph([{"value": [], "@odata.deltaLink": saved + "2"}])
    out = _run(_worker()._drain_tier1_folder_delta(fake, "user-1", "fA", saved))

    assert fake.calls[0]["url"] == saved
    assert fake.calls[0]["params"] is None
    assert fake.calls[0]["headers"] == {"Prefer": "odata.maxpagesize=999"}
    assert out["messages"] == [] and out["ok"] is True


def test_drain_failure_holds_back_the_cursor():
    class _Boom(_FakeGraph):
        async def _iter_pages(self, url, params=None, headers=None):
            self.calls.append({"url": url})
            raise RuntimeError("graph down")
            yield  # pragma: no cover

    out = _run(_worker()._drain_tier1_folder_delta(_Boom([]), "user-1", "fA", None))
    assert out["ok"] is False
    assert out["delta_token"] is None
    assert out["messages"] == []


# ── source pins on the live handler wiring ───────────────────────────────

def _method_src(name: str) -> str:
    start = _SRC.index(f"    async def {name}(")
    end = _SRC.find("\n    async def ", start + 10)
    end2 = _SRC.find("\n    def ", start + 10)
    stop = min(x for x in (end, end2, len(_SRC)) if x > 0)
    return _SRC[start:stop]


def test_handler_table_still_routes_tier1_types_to_backup_mailbox():
    tbl_start = _SRC.index("_HANDLER_TABLE: Dict[str, str] = {")
    tbl = _SRC[tbl_start: _SRC.index("}", tbl_start)]
    for t in ("MAILBOX", "SHARED_MAILBOX", "ROOM_MAILBOX"):
        assert re.search(rf'"{t}":\s*"backup_mailbox"', tbl), t


def test_backup_mailbox_dispatches_on_the_pilot_gate():
    body = _method_src("backup_mailbox")
    assert "tier1_mail_delta_enabled(" in body
    assert "_backup_mailbox_fullwalk(" in body
    assert "_backup_mailbox_delta(" in body


def test_legacy_fullwalk_is_the_only_caller_of_get_messages_delta():
    assert "get_messages_delta(" in _method_src("_backup_mailbox_fullwalk")
    assert "get_messages_delta(" not in _method_src("_backup_mailbox_delta")


def test_delta_path_uses_cursor_machinery_carry_forward_and_honesty():
    body = _method_src("_backup_mailbox_delta")
    for needle in (
        "is_force_full_backup_message(message)",
        "_load_mail_folder_deltas(",
        "_drain_tier1_folder_delta(",
        "_persist_mail_delta_state(",
        "_carry_forward_prior_mail_snapshot_items(",
        "_mark_snapshot_full(",
        "_stamp_snapshot_degraded(",
        "mailbox_parallel_folders(",
    ):
        assert needle in body, needle
    assert '"$top"' not in body


def test_delta_path_fetches_attachments_only_for_fresh_messages():
    body = _method_src("_backup_mailbox_delta")
    # the shared persist helper receives only messages returned by the delta
    assert "_persist_tier1_mail_items(" in body
    assert "_persist_tier1_mail_items(" in _method_src("_backup_mailbox_fullwalk")


def test_complete_snapshot_splits_counters():
    body = _method_src("complete_snapshot")
    assert "snapshot_counters_from_result(result)" in body
    assert 'snapshot.new_item_count = result.get("item_count", 0)' not in body


def test_carry_forward_skips_secondary_tier_rows():
    start = _SRC.index("async def _carry_forward_prior_mail_snapshot_items(")
    body = _SRC[start: _SRC.find("\nclass ", start)]
    flat = re.sub(r"\s+", " ", body)
    assert "COALESCE(src.metadata->>'source_mailbox', 'primary') = 'primary'" in flat, (
        "archive/recoverable rows are re-listed in full every run and must not be carried"
    )


# ── review round 1 (2026-09-04) pins ──────────────────────────────────────

def test_tier1_hard_off_beats_allowlist():
    env = {"TIER1_MAIL_DELTA_ENABLED": "false", "TIER1_MAIL_DELTA_RESOURCE_IDS": RID}
    assert _bw.tier1_mail_delta_enabled(RID, env=env) is False


def test_delta_failures_become_partial_on_the_single_backup_path():
    body = _method_src("_backup_mailbox_delta")
    # in-memory stamps: complete_snapshot only reads the object it was handed
    assert '"files_failed"' in body
    assert "SNAPSHOT_DEGRADED_REASON_KEY" in body
    cs = _method_src("complete_snapshot")
    assert "terminal_status_for_extra_data(" in cs


def test_persist_failures_hold_back_cursors():
    body = _method_src("_backup_mailbox_delta")
    assert "_source_folder_id" in body, "fresh messages must be tagged with their folder"
    assert re.search(r"ok_keys\s*(-=|\.difference_update\()", body)
    ph = _method_src("_persist_tier1_mail_items")
    assert "_bulk_upsert_snapshot_items(" in ph, "add_all+commit lets one duplicate sink a 50-row batch"
    assert "session.add_all(" not in ph
    assert "failed_folder_ids" in ph


def test_full_type_only_when_nothing_failed():
    assert "if not saved and not failed:" in _method_src("_backup_mailbox_delta")


def test_force_full_evicts_every_stale_cursor():
    body = _method_src("_backup_mailbox_delta")
    assert "saved_before" in body


def test_cursors_advance_only_after_carry_and_counters():
    body = _method_src("_backup_mailbox_delta")
    assert body.index("_carry_forward_prior_mail_snapshot_items(") < body.index("_persist_mail_delta_state(")
    assert "final counter query failed" in body


def test_tier1_carry_uses_folder_whitelist_and_drops_gone_cursors():
    body = _method_src("_backup_mailbox_delta")
    assert "keep_folder_ids=" in body
    assert "gone_fids" in body and "_delete_mail_folder_delta(" in body


def test_tier1_does_not_write_the_legacy_cursor_mirror():
    assert "write_legacy_mirror=False" in _method_src("_backup_mailbox_delta")
    assert "write_legacy_mirror" in _method_src("_persist_mail_delta_state")


def test_terminal_write_preserves_a_row_only_degraded_stamp():
    tw = _method_src("_write_snapshot_terminal")
    assert "SNAPSHOT_DEGRADED_REASON_KEY" in tw


# ── review round 2 (2026-09-04) pins ──────────────────────────────────────

def test_secondary_tier_tree_failure_is_recorded_not_swallowed():
    body = _method_src("_backup_mailbox_delta")
    assert 'failed.append(f"__{source}' in body


def test_degraded_reason_names_folders_and_labels_carry_failures():
    body = _method_src("_backup_mailbox_delta")
    assert "folder_failures" in body
    assert "__carry_forward__" in body
    # the sentinel must never be formatted as a folder
    assert 'primary_tree.get(first) or first' not in body
    cs = _method_src("complete_snapshot")
    assert "None if _deg_reason else file_failure_degraded_reason(" in cs


def test_bytes_total_counts_only_blob_backed_rows():
    body = _method_src("_backup_mailbox_delta")
    assert "FILTER (WHERE blob_path IS NOT NULL)" in body


def test_delta_path_checks_cancel_before_persist_and_cursors():
    body = _method_src("_backup_mailbox_delta")
    assert body.count("_raise_if_job_cancelled(") >= 2
    assert body.index("_raise_if_job_cancelled(") < body.index("_persist_tier1_mail_items(")


def test_legacy_parallel_mailbox_drain_has_no_top_either():
    start = _SRC.index("    async def _backup_mailboxes_parallel(")
    end = _SRC.index("\n    async def ", start + 10)
    body = _SRC[start:end]
    assert '"$top": "999", "$select": mbx_select' not in body, (
        "the fanout-off/partition path shares mail_folder_delta rows with the pilot; "
        "a $top-minted deltaLink would cap a whole round at 999 on resume"
    )
    assert "mail_delta_initial_params(mbx_select)" in body
    assert "headers=mail_delta_request_headers()" in body


# ── review round 3 (2026-09-04) pins ──────────────────────────────────────

def test_secondary_tier_messages_are_tagged_and_list_failures_recorded():
    body = _method_src("_backup_mailbox_delta")
    assert 'm["_source_folder_id"] = sfid' in body
    assert 'failed.append(f"__{source}_folder__' in body


def test_non_delta_fallback_keeps_a_real_page_size():
    # $top caps a DELTA round; on the plain /messages listing it is a page
    # size (Graph default 10) and no deltaLink is ever minted from it.
    start = _SRC.index("    async def _backup_mailboxes_parallel(")
    end = _SRC.index("\n    async def ", start + 10)
    body = _SRC[start:end]
    assert '"$top": "999"' in body, "non-delta /messages fallback lost its page size"
    um = _SRC[_SRC.index("_bootstrap_params = mail_delta_initial_params(mail_select)") - 900:
              _SRC.index("_bootstrap_params = mail_delta_initial_params(mail_select)") + 400]
    assert '"$top": "999"' in um


def test_parallel_path_routes_pilot_rooms_to_the_delta_handler():
    start = _SRC.index("    async def _backup_mailboxes_parallel(")
    end = _SRC.index("\n    async def ", start + 10)
    body = _SRC[start:end]
    # the legacy path never carries: every snapshot it writes is flagged so
    # prior selection skips it, and a pilot room arriving here is logged loudly
    assert "tier1_mail_delta_enabled(resource.id)" in body
    assert '{"mail_delta_only": True}' in body


def test_union_carry_not_run_when_preempted_all_failed():
    start = _SRC.index("    async def _finalize_partitioned_snapshot(")
    end = _SRC.index("\n    async def ", start + 10)
    body = _SRC[start:end]
    i = body.index("_carry_forward_partitioned_user_mail(snapshot_id)")
    assert "preempted" in body[i - 600:i]


def test_tier1_reason_splits_primary_and_secondary_persist_failures():
    body = _method_src("_backup_mailbox_delta")
    assert "primary_persist_failed" in body and "secondary_persist_failed" in body
    assert "secondary-tier folder(s)" in body


def test_tier1_force_full_eviction_is_labeled():
    body = _method_src("_backup_mailbox_delta")
    assert 'evict_reason=("force_full rebuild"' in body
