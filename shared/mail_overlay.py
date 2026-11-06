"""A changed mailbox's incremental becomes an OVERLAY of a full base snapshot instead of copying
the whole prior inventory forward (docs/mail-overlay-snapshots.md).

The overlay owns this run's fresh rows plus every row changed since the base (the prior overlay's
own rows, filtered exactly as the carry filters them), and records as removals the base rows the
full copy would not have carried. snapshot_items_effective() then reads base + overlay. Rohit,
2026-11-05: one new message copied 7,085 rows forward (245 s of a 464 s run).
"""
from __future__ import annotations

import os
from typing import Optional

from sqlalchemy import text

from shared.mail_snapshot_reuse import MAIL_PRIOR_SNAPSHOT_SQL


def mail_overlay_enabled(env=None) -> bool:
    """``MAIL_OVERLAY_ENABLED`` = 0/false/no/off copies every changed mailbox forward as before."""
    flag = str((os.environ if env is None else env).get("MAIL_OVERLAY_ENABLED", "") or "").strip().lower()
    return flag not in ("0", "false", "no", "off")


def _env_float(name, default):
    try:
        return float(os.environ.get(name) or default)
    except ValueError:
        return default


# A row the carry would NOT copy forward this run (alias {a}). Mirrors the EMAIL filter of
# workers/backup-worker _carry_forward_prior_mail_snapshot_items and copy_executor MAIL_CARRY.
_DROPPED = """(COALESCE({a}.metadata->>'source_mailbox', 'primary') <> 'primary'
    OR COALESCE({a}.folder_path, '') = ANY(CAST(:excl_paths AS TEXT[]))
    OR {a}.external_id = ANY(CAST(:excl_ids AS TEXT[]))
    OR (:whitelist AND NOT COALESCE(({a}.metadata->'raw'->>'parentFolderId') = ANY(CAST(:keep_fids AS TEXT[])), false))
    OR (:exclude_pathless AND COALESCE({a}.folder_path, '') = ''))"""

_OWNED = """EXISTS (SELECT 1 FROM snapshot_items own WHERE own.snapshot_id = :tsid
    AND own.item_type = {a}.item_type AND own.external_id = {a}.external_id)"""

_COPY_COLUMNS = ("tenant_id, item_type, external_id, parent_external_id, name, folder_path, content_hash, "
                 "content_checksum, content_size, blob_path, encryption_key_id, backup_version, metadata, "
                 "is_deleted, indexed_at, backend_id, chat_version_id")
_COPY_INTO_TARGET = (
    f"INSERT INTO snapshot_items (id, snapshot_id, {_COPY_COLUMNS}, created_at) "
    f"SELECT gen_random_uuid(), :tsid, {', '.join('src.' + c for c in _COPY_COLUMNS.split(', '))}, NOW() "
    "FROM snapshot_items src WHERE src.snapshot_id = :prior ")


async def build_mail_overlay(session, *, resource_id, target_snapshot_id, current_job_id, excluded_paths=(),
                             tombstoned_ids=(), keep_folder_ids=None, exclude_pathless=False):
    """Make ``target_snapshot_id`` an overlay. Returns ``(result, reason)``: ``result`` is a dict
    (carried rows, own rows, removals, base rows, base id) when the overlay was written, else None
    and ``reason`` says why the caller must do the full copy instead. Never commits: the caller
    commits a result and rolls back a None (the threshold check runs after the writes)."""
    p = {"tsid": str(target_snapshot_id), "rid": str(resource_id),
         "jid": str(current_job_id) if current_job_id else None,
         "excl_paths": sorted({x for x in (excluded_paths or []) if x}),
         "excl_ids": sorted({str(x) for x in (tombstoned_ids or []) if x}),
         "whitelist": keep_folder_ids is not None, "keep_fids": sorted({str(x) for x in (keep_folder_ids or []) if x}),
         "exclude_pathless": bool(exclude_pathless)}
    target = (await session.execute(text(
        "SELECT created_at FROM snapshots WHERE id = CAST(:tsid AS uuid) AND status = 'IN_PROGRESS' "
        "AND reuse_of_snapshot_id IS NULL AND overlay_base_id IS NULL AND extra_data->>'cancelled_at' IS NULL "
        "FOR NO KEY UPDATE"), p)).first()
    if target is None:
        return None, "target is not an in-progress full snapshot"
    prior = (await session.execute(text(
        f"SELECT p.id, p.overlay_base_id FROM snapshots p WHERE p.id = {MAIL_PRIOR_SNAPSHOT_SQL}"), p)).first()
    if prior is None:
        return None, "no prior snapshot to overlay"
    p["prior"], p["base"] = str(prior.id), str(prior.overlay_base_id or prior.id)
    # KEY SHARE: retention's DELETE of the base waits for this transaction, then sees the dependent.
    base = (await session.execute(text(
        "SELECT created_at FROM snapshots WHERE id = CAST(:base AS uuid) AND status IN ('COMPLETED', 'PARTIAL') "
        "AND extra_data->>'cancelled_at' IS NULL FOR KEY SHARE"), p)).first()
    if base is None:
        return None, f"base {p['base']} is not a restorable snapshot"
    max_age = _env_float("MAIL_OVERLAY_MAX_AGE_DAYS", 7.0)
    if (target.created_at - base.created_at).total_seconds() > max_age * 86400:
        return None, f"base {p['base']} is older than {max_age:g} days (compaction)"

    await session.execute(text("UPDATE snapshots SET overlay_base_id = CAST(:base AS uuid) "
                               "WHERE id = CAST(:tsid AS uuid)"), p)
    from_overlay = prior.overlay_base_id is not None
    carried = 0
    if from_overlay:
        # The prior overlay's own messages, filtered as the full copy filters the prior inventory.
        carried += (await session.execute(text(
            _COPY_INTO_TARGET + "AND src.item_type = 'EMAIL' AND NOT " + _DROPPED.format(a="src")
            + " AND NOT " + _OWNED.format(a="src")
            + " ON CONFLICT (snapshot_id, external_id, item_type) DO NOTHING"), p)).rowcount or 0
    # Hide every base row the full copy would not carry: not in the prior's inventory (removed or
    # superseded by the prior overlay's own row), or an email this run's filters drop. A key the
    # target owns needs no removal.
    await session.execute(text(
        "INSERT INTO snapshot_overlay_removals (snapshot_id, item_type, external_id) "
        "SELECT CAST(:tsid AS uuid), b.item_type, b.external_id FROM snapshot_items b "
        "WHERE b.snapshot_id = CAST(:base AS uuid) AND b.item_type IN ('EMAIL', 'EMAIL_ATTACHMENT') "
        "AND NOT " + _OWNED.format(a="b") + " AND ("
        + ("b.id NOT IN (SELECT e.id FROM snapshot_items_effective(CAST(:prior AS uuid)) e "
           "WHERE e.snapshot_id = CAST(:base AS uuid)) OR " if from_overlay else "")
        + "(b.item_type = 'EMAIL' AND " + _DROPPED.format(a="b") + ")) ON CONFLICT DO NOTHING"), p)
    if from_overlay:
        # The prior overlay's own attachments whose message this snapshot still holds.
        carried += (await session.execute(text(
            _COPY_INTO_TARGET + "AND src.item_type = 'EMAIL_ATTACHMENT' AND NOT " + _OWNED.format(a="src")
            + " AND COALESCE(src.parent_external_id, split_part(src.external_id, '::', 1)) IN ("
            "SELECT e.external_id FROM snapshot_items_effective(CAST(:tsid AS uuid)) e WHERE e.item_type = 'EMAIL')"
            " ON CONFLICT (snapshot_id, external_id, item_type) DO NOTHING"), p)).rowcount or 0

    sizes = (await session.execute(text(
        "SELECT (SELECT count(*) FROM snapshot_items WHERE snapshot_id = CAST(:tsid AS uuid) "
        "        AND item_type IN ('EMAIL', 'EMAIL_ATTACHMENT')) AS own, "
        "       (SELECT count(*) FROM snapshot_overlay_removals WHERE snapshot_id = CAST(:tsid AS uuid)) AS removed, "
        "       (SELECT count(*) FROM snapshot_items WHERE snapshot_id = CAST(:base AS uuid) "
        "        AND item_type IN ('EMAIL', 'EMAIL_ATTACHMENT')) AS base_rows"), p)).one()
    ratio = _env_float("MAIL_OVERLAY_MAX_RATIO", 0.25)
    if sizes.own + sizes.removed > ratio * sizes.base_rows:
        return None, (f"{sizes.own} own + {sizes.removed} removed rows exceed {ratio:g} x "
                      f"{sizes.base_rows} base rows (compaction)")
    await session.execute(text(
        "UPDATE snapshots SET extra_data = (COALESCE(extra_data::jsonb, '{}'::jsonb) || jsonb_build_object("
        "'mail_overlay_base', CAST(:base AS text), 'mail_overlay_prior', CAST(:prior AS text)))::json "
        "WHERE id = CAST(:tsid AS uuid)"), p)
    return {"carried": carried, "own": sizes.own, "removed": sizes.removed, "base_rows": sizes.base_rows,
            "base": p["base"]}, "overlay"


async def try_mail_overlay(session_scope, **kwargs) -> Optional[dict]:
    """Build the overlay in one transaction from ``session_scope()`` (an async context manager
    yielding a session). Returns the build result, or None after logging why the full copy runs;
    an error never fails the backup, it only costs the full copy."""
    try:
        async with session_scope() as session:
            result, reason = await build_mail_overlay(session, **kwargs)
            if result is None:
                await session.rollback()
            else:
                await session.commit()
    except Exception as exc:
        result, reason = None, f"overlay build failed: {type(exc).__name__}: {exc}"
    target, resource = kwargs.get("target_snapshot_id"), kwargs.get("resource_id")
    if result is None:
        print(f"[MAIL-OVERLAY] resource {resource}: full copy into snapshot {target} — {reason}")
    else:
        print(f"[MAIL-OVERLAY] resource {resource}: snapshot {target} overlays base {result['base']} — "
              f"{result['own']} own rows ({result['carried']} carried), {result['removed']} removed, "
              f"{result['base_rows']} base rows inherited")
    return result
