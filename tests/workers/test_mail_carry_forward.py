"""Every USER_MAIL snapshot must be a full point-in-time inventory, not a
bare delta — carry the prior snapshot's EMAIL / EMAIL_ATTACHMENT pointer rows
forward (the USER_CHATS model, `_carry_forward_prior_chat_snapshot_items`).

Why (2026-09-04, proven on jrue's own mailbox): the mail drain is
incremental — each run persists only the folder deltas — while GFS retention
keeps ONE snapshot per calendar day and hard-deletes the rest together with
their snapshot_items. Gold runs three times a day, so two of three daily
mail deltas were destroyed every day since June:

    window (jrue USER_MAIL)                                  Graph  missing from vault
    9/2 18:15Z → 9/3 10:10Z  (02:12 + 10:12 deltas pruned)    213        211
    9/3 10:15Z → 9/3 18:10Z  (18:12 delta kept)               128          0
    8/26 18:15Z → 8/27 10:10Z (pruned)                        132        131

Rooms/shared mailboxes were safe only because their Tier-1 handler re-walks
the whole mailbox every run. With carry-forward, any single kept snapshot
holds everything the resource ever captured (minus messages Graph reported
deleted), so retention can prune predecessors without losing data — and
`_compute_snapshot_delta_from_prior` still reports honest new_item_count /
bytes_added because it diffs against the prior full inventory.

Rules pinned here:
  * rows come from the immediately-prior COMPLETED/PARTIAL snapshot of the
    same resource from a DIFFERENT job (same-job siblings are not "prior");
  * NOT EXISTS on (snapshot_id, external_id, item_type) — fresh rows win;
  * messages the delta reported as @removed this run are NOT carried
    (their ids are collected in the page loop right where the tombstone
    guard already drops them), and neither are their attachments;
  * folders that were (re-)bootstrapped this run — no saved token, or the
    token was 410-evicted — are NOT carried: a bootstrap re-enumerates the
    whole folder, so anything absent from it was deleted server-side;
  * the all-folders fallback (folder tree empty → fid None) is a full
    non-delta scan and needs no carry at all;
  * EMAIL_ATTACHMENT rows are carried only when their parent EMAIL row is
    present in the target snapshot (fresh or carried);
  * the copy runs under carry_forward_slot_session (tests/workers/
    test_carry_forward_herd_bound.py) — never an unbounded INSERT…SELECT.
"""
from __future__ import annotations

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

_spec = importlib.util.spec_from_file_location("bw_mail_carry_forward", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_mail_carry_forward"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )

TREE = {"fA": "/Inbox", "fB": "/Sent Items", "fC": "/Inbox/Alerts"}


# ── pure helper: which folder paths must NOT be carried this run ──────────

def test_excluded_paths_empty_when_every_folder_resumed_a_token():
    out = _bw.mail_carry_forward_excluded_paths(
        planned_fids=["fA", "fB", "fC"],
        existing_tokens={"fA": "t", "fB": "t", "fC": "t"},
        evicted_keys=set(),
        folder_tree=TREE,
    )
    assert out == []


def test_excluded_paths_include_bootstrapped_folders():
    out = _bw.mail_carry_forward_excluded_paths(
        planned_fids=["fA", "fB", "fC"],
        existing_tokens={"fA": "t", "fC": "t"},  # fB has no saved cursor
        evicted_keys=set(),
        folder_tree=TREE,
    )
    assert out == ["/Sent Items"]


def test_excluded_paths_include_evicted_folders():
    out = _bw.mail_carry_forward_excluded_paths(
        planned_fids=["fA", "fB", "fC"],
        existing_tokens={"fA": "t", "fB": "t", "fC": "t"},
        evicted_keys={"fC"},
        folder_tree=TREE,
    )
    assert out == ["/Inbox/Alerts"]


def test_excluded_paths_are_sorted_and_deduped():
    out = _bw.mail_carry_forward_excluded_paths(
        planned_fids=["fB", "fA", "fB"],
        existing_tokens={"fC": "t"},  # something resumed → carry is on
        evicted_keys={"fA"},
        folder_tree=TREE,
    )
    assert out == ["/Inbox", "/Sent Items"]


def test_excluded_paths_is_none_when_nothing_resumed_a_cursor():
    # No saved cursor anywhere → every folder was enumerated from scratch →
    # the snapshot already IS the full inventory; carrying would only add
    # ghosts (deleted/renamed folders, pathless legacy rows).
    out = _bw.mail_carry_forward_excluded_paths(
        planned_fids=["fA", "fB"],
        existing_tokens={},
        evicted_keys=set(),
        folder_tree=TREE,
    )
    assert out is None


def test_bootstrapped_folder_that_failed_keeps_its_prior_rows():
    # fB had no cursor (bootstrap) but its drain FAILED — nothing fresh was
    # captured, so its prior rows are better than nothing: do not exclude.
    out = _bw.mail_carry_forward_excluded_paths(
        planned_fids=["fA", "fB"],
        existing_tokens={"fA": "t"},
        evicted_keys=set(),
        folder_tree=TREE,
        drained_ok_fids={"fA"},
    )
    assert out == []


def test_carry_forward_kill_switch_defaults_on():
    assert _bw.mail_carry_forward_enabled(env={}) is True
    assert _bw.mail_carry_forward_enabled(env={"MAIL_CARRY_FORWARD_ENABLED": "false"}) is False
    assert _bw.mail_carry_forward_enabled(env={"MAIL_CARRY_FORWARD_ENABLED": "0"}) is False


def test_all_folders_fallback_disables_carry_forward():
    # fid None = folder enumeration failed → non-delta scan of everything;
    # the snapshot is already a full inventory, carrying would resurrect
    # server-side deletions.
    out = _bw.mail_carry_forward_excluded_paths(
        planned_fids=[None],
        existing_tokens={},
        evicted_keys=set(),
        folder_tree={},
    )
    assert out is None


# ── source pins on the live worker code ───────────────────────────────────

def _fn_src(name: str) -> str:
    start = _SRC.index(f"async def {name}(")
    end = _SRC.find("\nasync def ", start + 10)
    end2 = _SRC.find("\nclass ", start + 10)
    stop = min(x for x in (end, end2, len(_SRC)) if x > 0)
    return _SRC[start:stop]


def test_mail_carry_forward_helper_exists_and_takes_a_slot():
    body = _fn_src("_carry_forward_prior_mail_snapshot_items")
    assert "carry_forward_slot_session(" in body
    assert "pg_advisory_xact_lock(" not in body, "blocking lock form is banned (herd bound)"


def test_mail_carry_forward_sql_shape():
    body = _fn_src("_carry_forward_prior_mail_snapshot_items")
    flat = re.sub(r"\s+", " ", body)
    assert "INSERT INTO snapshot_items" in flat
    assert "NOT EXISTS" in flat
    assert "status IN ('COMPLETED', 'PARTIAL')" in flat
    assert re.search(r"job_id IS DISTINCT FROM (CAST\()?:jid", flat), (
        "prior snapshot must come from a different job than the current run"
    )
    assert re.search(r"folder_path[^)]*\) = ANY\(CAST\(:excl_paths AS TEXT\[\]\)\)", flat), (
        "bootstrapped/evicted folders must be excluded by path"
    )
    assert "external_id = ANY(" in flat, "tombstoned message ids must be excluded"
    assert "'EMAIL_ATTACHMENT'" in flat and "'EMAIL'" in flat
    # attachments follow their parent: only carried when the parent EMAIL
    # row is present in the target snapshot
    assert "split_part(" in flat or "parent_external_id" in flat


def test_drain_collects_tombstoned_ids_where_it_drops_them():
    # the guard itself is pinned by test_mail_delta_tombstone_guard_present;
    # here: the id must be recorded before the `continue`.
    m = re.search(
        r"if is_delta_tombstone\(m\):\s*\n\s*mail_tombstoned_ids\.add\(",
        _SRC,
    )
    assert m, "tombstoned message ids are not collected in the mail page loop"


def test_user_mail_finalize_runs_carry_before_cursor_persist():
    # Cursors are committed mid-drain per folder; the run's terminal cursor
    # write must come AFTER the carry so a carry failure can REWIND them —
    # otherwise the failed run's fresh delta is orphaned from the chain.
    block_start = _SRC.index("mail_carry_forward_excluded_paths(\n                        planned_fids=mail_folder_ids,")
    persist = _SRC.index("await self._persist_mail_delta_state(\n                        resource.id,")
    assert block_start < persist
    honesty = _SRC.index("_mail_status, _mail_reason = evaluate_drain_honesty(")
    between = _SRC[block_start:honesty]
    assert "tombstoned_ids=" in between and "excluded_paths=" in between


def test_user_mail_carry_failure_rewinds_cursors():
    block = _user_mail_carry_block()
    assert "_mail_carry_failed = True" in block
    # rewind through the SAME gate: evict every cursor drained this run and
    # advance no fingerprint (the raw token map has exactly one consumer —
    # pinned by test_mail_cursor_dual_write_gating)
    assert "_mail_folders_token_evicted |=" in block
    assert "_mail_ok_token_keys = set()" in block
    assert "_drained_fids = set()" in block
    assert block.index("_mail_carry_failed = True") < block.index("await self._persist_mail_delta_state(")



# ── review round 1 (2026-09-04) pins ──────────────────────────────────────

def _carry_flat() -> str:
    body = _fn_src("_carry_forward_prior_mail_snapshot_items")
    return re.sub(r"\s+", " ", body)


def test_prior_snapshot_must_hold_rows_and_not_be_a_reuse_or_failed_carry():
    flat = _carry_flat()
    assert "EXISTS (SELECT 1 FROM snapshot_items" in flat, "a 0-row prior restarts the chain empty"
    assert "reuse_of_snapshot_id IS NULL" in flat
    assert "mail_carry_forward_failed" in flat, "a snapshot whose own carry failed must not be a prior"


def test_inserts_are_idempotent_under_concurrent_fresh_writes():
    flat = _carry_flat()
    # one definition, appended to BOTH statements (EMAIL and EMAIL_ATTACHMENT)
    assert "ON CONFLICT (snapshot_id, external_id, item_type) DO NOTHING" in flat
    assert flat.count("+ on_conflict_sql") == 2


def test_carry_supports_folder_id_whitelist_for_tier1():
    flat = _carry_flat()
    assert "keep_folder_ids" in flat
    assert "->'raw'->>'parentFolderId') = ANY(CAST(:keep_fids AS TEXT[]))" in flat


def _user_mail_carry_block() -> str:
    start = _SRC.index("mail_carry_forward_excluded_paths(\n                        planned_fids=mail_folder_ids,")
    honesty = _SRC.index("_mail_status, _mail_reason = evaluate_drain_honesty(")
    return _SRC[start:honesty]


def test_user_mail_shards_do_not_run_the_inline_carry():
    # each partition shard sees only ITS folders' tombstones/exclusions; a
    # shard-local carry against the shared parent snapshot resurrects mail
    # deleted in sibling shards and runs N giant copies instead of one.
    block = _user_mail_carry_block()
    # shards record their inputs for the finalizer; only whole-mailbox runs
    # take the inline carry branch
    assert block.index("if _running_as_mail_shard:") < block.index("_record_shard_carry_inputs(")
    assert block.index("_record_shard_carry_inputs(") < block.index("elif mail_carry_forward_enabled()")
    assert block.index("elif mail_carry_forward_enabled()") < block.index("_carry_forward_prior_mail_snapshot_items(")


def test_user_mail_carry_respects_kill_switch_and_drained_ok():
    block = _user_mail_carry_block()
    assert "mail_carry_forward_enabled(" in block
    assert "drained_ok_fids=_mail_folders_drained_ok" in block


def test_user_mail_carry_failure_is_recorded_not_swallowed():
    block = _user_mail_carry_block()
    assert "_stamp_snapshot_degraded(" in block
    assert "mail_carry_forward_failed" in block


# ── review round 2 (2026-09-04) pins ──────────────────────────────────────

def test_user_mail_cancel_is_checked_before_cursor_persist():
    persist = _SRC.index("await self._persist_mail_delta_state(\n                        resource.id,")
    window = _SRC[persist-3000:persist]
    assert "_is_job_cancelled(job_id)" in window, (
        "an operator cancel must not let this run's cursors advance — the "
        "cancelled-sweep hard-deletes the snapshot and its blobs"
    )


def test_partition_shards_persist_carry_inputs_for_the_finalizer():
    # each USER_MAIL shard records its exclusions + tombstones on its
    # snapshot_partitions row so the winning finalize can carry ONCE with
    # the union (5 of 8 Gold mailboxes run partitioned on every tick).
    block_start = _SRC.index("mail_carry_forward_excluded_paths(\n                        planned_fids=mail_folder_ids,")
    honesty = _SRC.index("_mail_status, _mail_reason = evaluate_drain_honesty(")
    between = _SRC[block_start:honesty]
    assert "_record_shard_carry_inputs(" in between
    body = _fn_src("_record_shard_carry_inputs") if "async def _record_shard_carry_inputs(" in _SRC else _SRC[_SRC.index("    async def _record_shard_carry_inputs("):]
    assert "snapshot_partitions" in body and "carry_forward" in body


def test_partition_finalizer_carries_union_for_user_mail():
    start = _SRC.index("    async def _finalize_partitioned_snapshot(")
    end = _SRC.index("\n    async def ", start + 10)
    body = _SRC[start:end]
    assert "_carry_forward_partitioned_user_mail(" in body
    helper = _finalizer_helper()  # inner body + settling wrapper
    assert "_carry_forward_prior_mail_snapshot_items(" in helper
    assert "carry_forward" in helper and "USER_MAIL" in helper and "MAIL_FOLDERS" in helper
    # a shard without recorded inputs → no carry + degraded/flag, never a partial union
    assert "mail_carry_forward_failed" in helper and "resumed_any" in helper


def test_carry_helper_warns_on_huge_copies():
    assert "MAIL_CARRY_FORWARD_WARN_ROWS" in _fn_src("_carry_forward_prior_mail_snapshot_items")


# ── review round 3 (2026-09-04) pins ──────────────────────────────────────

def _finalizer_helper() -> str:
    hs = _SRC.index("    async def _carry_forward_partitioned_user_mail(")
    he = _SRC.index("\n    async def ", hs + 10)
    body = _SRC[hs:he]
    if "_carry_forward_partitioned_user_mail_inner(" in body:
        hi = _SRC.index("    async def _carry_forward_partitioned_user_mail_inner(")
        body = _SRC[hi:_SRC.index("\n    async def ", hi + 10)] + body
    return body


def test_union_carry_decides_resumed_before_refusing():
    body = _finalizer_helper()
    # "nothing resumed" (excluded_paths None, resumed_any False) is a legitimate
    # full bootstrap — it must return 0 WITHOUT stamping/flagging; only an
    # explicit all-folders fallback refuses.
    assert body.index('bool(cf.get("resumed_any"))') < body.index("_stamp_snapshot_degraded(")
    assert "all_folders_fallback" in body


def test_union_carry_missing_shard_inputs_contribute_nothing():
    body = _finalizer_helper()
    # a shard that died before recording had no fresh data → its folders'
    # prior rows are carried (same rule as a failed folder inline)
    assert "shard inputs missing" not in body


def test_union_carry_has_a_one_shot_claim_and_rewinds_on_failure():
    body = _finalizer_helper()
    assert "mail_union_carry_claimed" in body
    assert "_rewind_partitioned_mail_state(" in body
    rw = _SRC[_SRC.index("    async def _rewind_partitioned_mail_state("):]
    rw = rw[: rw.index("\n    async def ", 10)]
    assert "_persist_mail_delta_state(" in rw and "ok_token_keys=set()" in rw
    # eviction outcome is threaded back so a failed rewind is never reported as done
    assert "evict_result" in rw and "fingerprint" in rw


def test_shard_records_drained_keys_and_caps_tombstones():
    body = _SRC[_SRC.index("    async def _record_shard_carry_inputs("):]
    body = body[: body.index("\n    async def ", 10)]
    assert "drained_keys" in body and "all_folders_fallback" in body
    assert "MAIL_CARRY_TOMBSTONE_CAP" in body


def test_user_mail_cancel_rewinds_then_raises():
    block = _user_mail_carry_block()
    assert "_is_job_cancelled(job_id)" in block
    # the rewind + gated persist must precede the raise
    assert block.index("_persist_mail_delta_state(") < block.index("raise JobCancelledMidFlight(")


def test_fetch_for_does_not_relabel_cancel_as_enumeration_failure():
    i = _SRC.index('raise EnumerationFailed(f"{kind}: {e}") from e')
    window = _SRC[i - 1500:i]
    assert "except JobCancelledMidFlight:" in window


def test_prior_selection_skips_delta_only_parallel_snapshots():
    flat = _carry_flat()
    assert "mail_delta_only" in flat


# ── round-4 review pins (cancel propagation, unrecorded shards, markers) ──

def _parallel_gather_tail() -> str:
    start = _SRC.index(
        "results = await asyncio.gather(*[_backup_one(r) for r in resources], return_exceptions=True)"
    )
    end = _SRC.index('return {"item_count": item_count, "bytes_added": bytes_added}', start)
    return _SRC[start:end]


def test_gather_propagates_a_mid_flight_cancel_instead_of_swallowing_it():
    # gather(return_exceptions=True) turned the USER_MAIL branch's
    # JobCancelledMidFlight into a result element: the shard settled COMPLETED
    # and the union carry ran on a cancelled snapshot.
    tail = _parallel_gather_tail()
    assert re.search(r"isinstance\(_r,\s*(?:\([^)]*\bJobCancelledMidFlight\b[^)]*\)|JobCancelledMidFlight)\s*\)", tail)
    assert tail.index("raise _r") < tail.index("item_count = sum(")


def test_union_carry_refuses_when_any_shard_left_no_carry_inputs():
    body = _finalizer_helper()
    # a dead shard contributes no exclusions/tombstones AND its mid-drain
    # checkpoints may have advanced past fresh rows — never infer "nothing
    # resumed" from the recorded shards alone.
    assert "unrecorded_fids" in body
    assert "len(recorded) != len(mail_parts)" in body or "len(recorded) < len(mail_parts)" in body
    # the unrecorded shards' folders are part of every rewind set
    assert "union_drained | unrecorded_fids" in body


def test_union_carry_claims_first_and_clears_pending_on_every_early_exit():
    # claim-first: the pending marker must be durable BEFORE any statement
    # that can fail, so a preamble error never leaves an unflagged
    # delta-only snapshot eligible as a prior (round-5 #4/#15).
    body = _finalizer_helper()
    assert body.index("mail_union_carry_claimed") < body.index("SELECT r.tenant_id")
    # not-USER_MAIL / no shards / all-bootstrapped / success clear it; refuse /
    # failure settle it by rewind outcome; cancel + preamble errors delegate to
    # the flagging rewind helper
    assert body.count('"mail_carry_forward_pending": False') >= 3
    assert body.count('"mail_carry_forward_pending": not _rw_ok') >= 2
    assert "_rewind_failed_partitioned_user_mail(" in body


def test_union_carry_marks_pending_until_the_copy_lands():
    body = _finalizer_helper()
    # claim sets the marker; the snapshot must not be a prior while half-copied
    assert 'mail_carry_forward_pending\\": true' in body
    # cleared on the success path AND on the refuse/failure paths
    assert body.count('"mail_carry_forward_pending": False') >= 2


def test_union_carry_skips_a_cancelled_job():
    body = _finalizer_helper()
    assert "_is_job_cancelled(" in body
    # cancelled → rewind + flagged, never a copy
    i = body.index("_is_job_cancelled(")
    assert "_rewind_failed_partitioned_user_mail(" in body[i:i + 900]
    hs = _SRC.index("    async def _rewind_failed_partitioned_user_mail(")
    helper = _SRC[hs:_SRC.index("\n    async def ", hs + 10)]
    assert '"mail_carry_forward_failed": True' in helper  # the rewind helper flags never-a-prior


def test_prior_selection_skips_pending_and_cancelled_snapshots():
    flat = _carry_flat()
    assert "mail_carry_forward_pending" in flat
    assert "s2.extra_data->>'cancelled_at' IS NULL" in flat


def test_terminal_write_preserves_delta_only_and_claim_markers():
    start = _SRC.index("    async def _write_snapshot_terminal(")
    body = _SRC[start:_SRC.index("\n    async def ", start + 10)]
    i = body.index('"mail_carry_forward_failed",')
    window = body[i - 400:i + 400]
    for marker in ("mail_delta_only", "mail_union_carry_claimed", "mail_carry_forward_pending",
                   "mail_rewind_keys"):
        assert f'"{marker}"' in window, marker


def test_rewind_pops_the_legacy_fingerprint_mirror_for_evicted_keys():
    start = _SRC.index("    async def _persist_mail_delta_state(")
    body = _SRC[start:_SRC.index("\n    async def ", start + 10)]
    mirror = body[body.index("if write_legacy_mirror:"):]
    # evicted keys leave BOTH mirrors, or skip-by-fingerprint hides the gap
    assert re.search(r"for _dead_key in _evicted:[\s\S]{0,300}mail_folder_fingerprints", mirror)
    assert mirror.count("mail_folder_fingerprints") >= 4


def test_eviction_log_names_the_resource():
    start = _SRC.index("    async def _persist_mail_delta_state(")
    body = _SRC[start:_SRC.index("\n    async def ", start + 10)]
    i = body.index("cursor EVICTED")
    assert "{resource_id}" in body[i - 200:i]


def test_shard_record_failure_flags_the_snapshot():
    block = _user_mail_carry_block()
    i = block.index("except Exception as _rec_err:")
    window = block[i:i + 1500]
    assert "_stamp_snapshot_degraded(" in window
    assert '"mail_carry_forward_failed": True' in window


def test_failed_partitioned_snapshot_rewinds_every_shard():
    start = _SRC.index("    async def _finalize_partitioned_snapshot(")
    body = _SRC[start:_SRC.index("\n    async def ", start + 10)]
    assert "_rewind_failed_partitioned_user_mail(" in body
    hs = _SRC.index("    async def _rewind_failed_partitioned_user_mail(")
    helper = _SRC[hs:_SRC.index("\n    async def ", hs + 10)]
    assert '"folder_ids"' in helper and '"drained_keys"' in helper
    assert "_rewind_partitioned_mail_state(" in helper


def test_union_carry_uses_the_module_level_cancel_checker():
    body = _finalizer_helper()
    assert "self._is_job_cancelled(" not in body, "_is_job_cancelled is module-level; self. raises AttributeError and the call-site swallows it"
    assert "await _is_job_cancelled(" in body


def test_cursor_eviction_also_drops_the_table_fingerprint():
    # skip-by-fingerprint does not require a live cursor: an evicted folder
    # whose fingerprint row survives is skipped next run and never re-walked.
    start = _SRC.index("    async def _persist_mail_delta_state(")
    body = _SRC[start:_SRC.index("\n    async def ", start + 10)]
    i = body.index("for _fid in sorted(_evicted):")
    loop = body[i:i + 600]
    assert "_delete_mail_folder_delta(resource_id, _fid)" in loop
    assert "_delete_mail_folder_fingerprint(resource_id, _fid)" in loop
    assert "    async def _delete_mail_folder_fingerprint(" in _SRC


# ── round-5 review pins ─────────────────────────────────────────────────────

def _method_src(name: str) -> str:
    start = _SRC.index(f"    async def {name}(")
    end = _SRC.find("\n    async def ", start + 10)
    end2 = _SRC.find("\n    def ", start + 10)
    stop = min(x for x in (end, end2, len(_SRC)) if x > 0)
    return _SRC[start:stop]


def test_single_backup_except_arm_does_not_fail_or_requeue_a_cancelled_job():
    body = _method_src("_process_single_backup")
    i = body.index("except Exception as e:")
    arm = body[i:body.index("# ── Step 3b", i)]
    assert "_is_cancel" in arm
    assert arm.index("_is_cancel = ") < arm.index("update_job_status(")
    # the FAILED write is skipped for a cancel...
    assert "elif not _is_cancel:" in arm
    # ...and the message is acked, not re-raised into a requeue
    assert re.search(r"if _is_cancel:\s*\n\s*return", arm)
    assert arm.index("if _is_cancel:") < arm.rindex("raise")


def test_update_job_status_never_overwrites_a_cancelled_job():
    body = _method_src("update_job_status")
    assert '"CANCELLING"' in body and '"CANCELLED"' in body
    assert body.index("return") < body.index("job.status = status")


def test_success_path_checks_the_pending_clear():
    body = _finalizer_helper()
    i = body.index("_carry_forward_prior_mail_snapshot_items(")
    tail = body[i:]
    assert "if not _cleared:" in tail
    assert tail.index("if not _cleared:") < tail.index("return copied")


def test_union_carry_is_wrapped_so_a_preamble_error_flags_and_rewinds():
    body = _finalizer_helper()
    assert "_carry_forward_partitioned_user_mail_inner(" in body
    i = body.index("except Exception as _pre_err:")
    window = body[i:i + 1200]
    assert '"mail_carry_forward_failed": True' in window
    assert "_rewind_failed_partitioned_user_mail(" in window


def test_finalizer_settles_a_carry_helper_exception():
    start = _SRC.index("    async def _finalize_partitioned_snapshot(")
    body = _SRC[start:_SRC.index("\n    async def ", start + 10)]
    i = body.index("except Exception as _pcf_err:")
    window = body[i:i + 1800]
    assert '"mail_carry_forward_failed": True' in window
    assert "_rewind_failed_partitioned_user_mail(" in window
    # late FAILED / cancel after the reconcile commit also rewinds
    assert body.count("_rewind_failed_partitioned_user_mail(") >= 3
    j = body.index("_coerce_snapshot_terminal_on_cancel(")
    assert "_rewind_failed_partitioned_user_mail(" in body[j:j + 2500]


def test_failed_rewind_helper_flags_the_snapshot():
    hs = _SRC.index("    async def _rewind_failed_partitioned_user_mail(")
    helper = _SRC[hs:_SRC.index("\n    async def ", hs + 10)]
    assert '"mail_carry_forward_failed": True' in helper


def test_user_mail_run_reconciles_stale_carry_state_before_loading_cursors():
    fetch = _SRC[_SRC.index("        async def _fetch_for("):]
    fetch = fetch[:fetch.index("_load_mail_folder_deltas(") + 40]
    assert "_reconcile_stale_mail_carry_state(" in fetch
    helper = _method_src("_reconcile_stale_mail_carry_state")
    assert "mail_carry_forward_pending" in helper
    assert "mail_union_carry_claimed" in helper
    assert "MAIL_FOLDERS" in helper
    assert "_carry_forward_partitioned_user_mail(" in helper
    assert "_rewind_failed_partitioned_user_mail(" in helper
    assert "LIMIT" in helper


def test_eviction_is_batched_when_more_than_one_folder():
    start = _SRC.index("    async def _persist_mail_delta_state(")
    body = _SRC[start:_SRC.index("\n    async def ", start + 10)]
    assert "_delete_mail_folder_state_bulk(" in body
    bulk = _method_src("_delete_mail_folder_state_bulk")
    assert bulk.count("ANY(CAST(:fids AS TEXT[]))") == 2
    assert "mail_folder_delta" in bulk and "mail_folder_fingerprint" in bulk


def test_legacy_group_path_reports_a_cancel_instead_of_dropping_totals():
    i = _SRC.index("        async def process_group(")
    body = _SRC[i:i + 2500]
    assert "except JobCancelledMidFlight" in body
    assert '"error": "cancelled"' in body


def test_mail_shard_handler_settles_a_cancel_without_requeue():
    body = _method_src("_process_mail_partition_message")
    i = body.index("mail shard failed:")
    window = body[i - 1500:i]
    assert "except JobCancelledMidFlight" in window
    assert '"job_cancelled"' in window


# ── round-6 review pins ─────────────────────────────────────────────────────

def test_prior_must_be_older_than_the_target_snapshot():
    # the late (catch-up) carry runs for an OLD snapshot while newer ones
    # exist; ORDER BY created_at DESC alone would copy a NEWER snapshot's
    # rows into it.
    flat = _carry_flat()
    assert "s2.created_at < (SELECT t.created_at FROM snapshots t WHERE t.id = :tsid)" in flat
    chat = re.sub(r"\s+", " ", _fn_src("_carry_forward_prior_chat_snapshot_items"))
    assert "s2.created_at < (SELECT t.created_at FROM snapshots t WHERE t.id = :tsid)" in chat


def test_prior_selection_refuses_an_unsettled_new_era_partitioned_snapshot():
    # a partitioned snapshot whose shards recorded carry inputs must have
    # been settled by the union carry (claim) before it can be a prior —
    # sweeper-settled snapshots hold only their own deltas.
    flat = _carry_flat()
    assert "mail_union_carry_claimed" in flat
    assert "(sp.payload::jsonb) ? 'carry_forward'" in flat


def _fetch_for_head() -> str:
    fetch = _SRC[_SRC.index("        async def _fetch_for("):]
    return fetch[:fetch.index("_load_mail_folder_deltas(") + 40]


def test_stale_carry_reconcile_runs_only_outside_shards():
    head = _fetch_for_head()
    i = head.index("_reconcile_stale_mail_carry_state(")
    assert 'if not (message or {}).get("partitionFilter")' in head[i - 700:i]


def test_legacy_mirror_is_reread_after_the_reconcile_hook():
    # the hook evicts through the DB; the ORM row loaded before it would
    # resurrect the evicted cursor / fingerprint via the dual-read
    fetch = _SRC[_SRC.index("        async def _fetch_for("):]
    fetch = fetch[:fetch.index("_compute_mail_skip_by_fp(")]
    i = fetch.index("_reconcile_stale_mail_carry_state(")
    tail = fetch[i:]
    assert "_ed_fresh" in tail
    assert '_ed_fresh.get("mail_delta_tokens_by_folder"' in tail or "_ed_fresh.get(\n" in tail
    assert '_ed_fresh.get("mail_folder_fingerprints")' in tail
    # nothing after the hook may read the pre-hook ORM copy
    assert "resource.extra_data" not in tail


def test_claim_is_mail_only_and_timestamped():
    body = _finalizer_helper()
    i = body.index("mail_union_carry_claimed\\\": true")
    claim = body[i - 400:i + 900]
    assert "mail_union_carry_claimed_at" in claim
    assert "r.type::text = 'USER_MAIL'" in claim


def test_stale_reconcile_respects_claim_age_and_rewinds_before_claiming():
    helper = _method_src("_reconcile_stale_mail_carry_state")
    assert "mail_union_carry_claimed_at" in helper
    assert "INTERVAL '5 hours'" in helper
    assert "ORDER BY COALESCE(s.completed_at, s.created_at) ASC" in helper
    # rewind first; the one-shot claim is stamped only after a verified rewind
    assert helper.index("_rewind_failed_partitioned_user_mail(") < helper.index('"mail_union_carry_claimed": True')
    assert "if _ok:" in helper or "if _rewound:" in helper


def test_rewinds_report_success_and_a_failed_rewind_leaves_pending():
    assert "    ) -> bool:" in _method_src("_rewind_partitioned_mail_state")
    assert "    ) -> bool:" in _method_src("_rewind_failed_partitioned_user_mail")
    for name in ("_delete_mail_folder_delta", "_delete_mail_folder_fingerprint", "_delete_mail_folder_state_bulk"):
        assert "-> bool:" in _method_src(name), name
    persist = _method_src("_persist_mail_delta_state")
    assert "evict_result" in persist
    body = _finalizer_helper()
    # refuse / failure / preamble paths: pending stays True when the rewind did not land
    assert body.count('"mail_carry_forward_pending": not _rw_ok') >= 2


def test_cancel_path_skips_error_message_and_labels_the_audit():
    body = _method_src("_process_single_backup")
    i = body.index("except Exception as e:")
    arm = body[i:body.index("# ── Step 3b", i)]
    assert arm.index("if not _is_cancel:") < arm.index("UPDATE jobs SET error_message")
    assert '"reason": ("job_cancelled" if _is_cancel else "handler_error")' in arm


def test_update_job_status_logs_a_refused_late_write():
    body = _method_src("update_job_status")
    assert "late" in body and "ignored" in body


# ── round-7 review pins ─────────────────────────────────────────────────────

def test_per_folder_eviction_helpers_are_retried():
    for name in ("_delete_mail_folder_delta", "_delete_mail_folder_fingerprint"):
        assert "run_with_db_retry" in _method_src(name), name


def test_kill_switch_off_stamps_delta_only_when_cursors_resumed():
    block = _user_mail_carry_block()
    assert "elif _cf_excluded is not None and not _running_as_mail_shard:" in block
    i = block.index("elif _cf_excluded is not None and not _running_as_mail_shard:")
    assert '"mail_delta_only": True' in block[i:i + 700]
    t1 = _method_src("_backup_mailbox_delta")
    assert "elif saved and keep_fids:" in t1
    j = t1.index("elif saved and keep_fids:")
    assert '"mail_delta_only": True' in t1[j:j + 600]


def test_inline_rewind_is_verified_and_marks_retry_keys():
    i = _SRC.index("evicted_token_keys=_mail_folders_token_evicted,")
    window = _SRC[i - 800:i + 1800]
    assert "evict_result=_inline_er" in window
    assert '"mail_rewind_keys"' in window
    assert '"mail_carry_forward_pending": True' in window


def test_stale_reconcile_covers_inline_rewind_keys_and_failed_rows():
    helper = _method_src("_reconcile_stale_mail_carry_state")
    assert "LIMIT 25" in helper
    assert "? 'mail_rewind_keys'" in helper
    # a flagged-failed row is never an in-flight carry: no 5h wait
    assert "mail_carry_forward_failed', 'false') = 'true'" in helper
    rf = _method_src("_rewind_failed_partitioned_user_mail")
    assert "mail_rewind_keys" in rf


def test_late_prior_superseded_by_a_newer_claimed_sibling_rewinds():
    body = _finalizer_helper()
    assert body.count("_rewind_if_superseded_by_newer_prior(") >= 2
    helper = _method_src("_rewind_if_superseded_by_newer_prior")
    assert "created_at >" in helper and "mail_union_carry_claimed" in helper
    assert "_rewind_partitioned_mail_state(" in helper


def test_rewind_logs_do_not_claim_success_up_front():
    body = _finalizer_helper()
    assert "cursors rewound\"" not in body.replace("f\"", "\"")
    assert "shard cursors rewound" not in body


# ── round-8 review pins ─────────────────────────────────────────────────────

def test_stale_reconcile_routes_failed_and_rewind_key_rows_to_the_rewind_arm():
    helper = _method_src("_reconcile_stale_mail_carry_state")
    assert "s.extra_data->>'mail_carry_forward_failed'" in helper
    assert 'str(_failed) == "true" or _rewind_keys' in helper
    # the rewind arms run even while the carry kill switch is off; only the
    # late union carry is gated
    assert helper.index("SELECT s.id") < helper.index("mail_carry_forward_enabled()")
    # resource-level fallback survives a hard-deleted cancelled snapshot
    assert "mail_pending_evictions" in helper


def test_inner_rewinds_a_whole_resource_retry_row_instead_of_clearing_pending():
    body = _finalizer_helper()
    i = body.index("if not mail_parts:")
    assert "if head[6]:" in body[i:i + 500]
    assert "_rewind_failed_partitioned_user_mail(" in body[i:i + 500]


def test_superseded_check_uses_prior_eligibility_and_marks_a_failed_rewind():
    helper = _method_src("_rewind_if_superseded_by_newer_prior")
    assert "? 'carry_forward'" in helper
    assert "mail_delta_only" in helper and "cancelled_at" in helper
    assert '"mail_rewind_keys"' in helper


def test_inline_rewind_failure_also_records_a_resource_level_retry():
    i = _SRC.index("evicted_token_keys=_mail_folders_token_evicted,")
    window = _SRC[i:i + 2600]
    assert "_append_resource_pending_evictions(" in window
    helper = _method_src("_append_resource_pending_evictions")
    assert "mail_pending_evictions" in helper


def test_wrapper_log_does_not_claim_a_rewind_up_front():
    body = _finalizer_helper()
    assert "flagged + rewound" not in body


# ── round-9 review pins ─────────────────────────────────────────────────────

def test_union_carry_head_query_reads_tenant_from_resources():
    # snapshots has NO tenant_id column (prod schema); the join partner does
    body = _finalizer_helper()
    assert "s.tenant_id" not in body
    assert "SELECT r.tenant_id, s.resource_id" in body


def test_parked_evictions_use_atomic_sql_not_orm_read_modify_write():
    helper = _method_src("_append_resource_pending_evictions")
    assert "jsonb_agg" in helper and "mail_pending_evictions" in helper
    assert "r.extra_data = ed" not in helper
    rec = _method_src("_reconcile_stale_mail_carry_state")
    assert "- 'mail_pending_evictions'" in rec
    assert "_ed.pop(" not in rec


def test_failed_partitioned_rewind_also_parks_keys_on_the_resource():
    rf = _method_src("_rewind_failed_partitioned_user_mail")
    assert "_append_resource_pending_evictions(" in rf
