"""A SharePoint site incremental becomes an OVERLAY of a full base snapshot instead of copying every
unchanged list row forward (same scheme as shared.mail_overlay, docs/mail-overlay-snapshots.md).

During the run a carried list (backup_sharepoint _copy_forward_sp_list_rows) copies only the prior
overlay's own rows of that list; every other row it would have copied stays in the base. At the end
build_sp_overlay hides each base row the full copy would not have held, drops own rows identical to
their base row, and checks the compaction caps; any failure copies the carried lists in full instead.
Railway 2026-11-06: a no-change incremental of 19 sites wrote 5,763 identical list rows.

Library files (item_type FILE) stay sparse: they are not overlay types.
"""
from __future__ import annotations

import os
from typing import Optional

from sqlalchemy import text

SP_OVERLAY_TYPES = ("SHAREPOINT_LIST", "SHAREPOINT_FILE", "SHAREPOINT_FOLDER", "SHAREPOINT_LIST_ITEM")
_TYPES = "('" + "', '".join(SP_OVERLAY_TYPES) + "')"

# The full snapshot an overlay of :prior would sit on: the prior's own base, or the prior itself.
BASE_OF_PRIOR = ("(SELECT COALESCE(t.overlay_base_id, t.id) FROM snapshots s "
                 "JOIN snapshots t ON t.id = COALESCE(s.reuse_chain_root_id, s.id) WHERE s.id = CAST(:prior AS uuid))")

_OWNED = """EXISTS (SELECT 1 FROM snapshot_items own WHERE own.snapshot_id = CAST(:tsid AS uuid)
    AND own.item_type = {a}.item_type AND own.external_id = {a}.external_id)"""

# Columns that make an own row the same captured item as its base row.
_SAME = ("parent_external_id", "name", "folder_path", "content_hash", "content_checksum", "content_size",
         "blob_path", "encryption_key_id", "is_deleted", "backend_id")


def sp_overlay_enabled(env=None) -> bool:
    """``SHAREPOINT_OVERLAY_ENABLED`` = 0/false/no/off copies every carried list forward as before."""
    flag = str((os.environ if env is None else env).get("SHAREPOINT_OVERLAY_ENABLED", "") or "").strip().lower()
    return flag not in ("0", "false", "no", "off")


def sp_overlay_plan() -> Optional[dict]:
    """Per-run state the carry fills (prior, base, carried lists, rows read again); None = full copy."""
    return {"inherited": set(), "skip": set()} if sp_overlay_enabled() else None


def _env_float(name, default):
    try:
        return float(os.environ.get(name) or default)
    except ValueError:
        return default


async def build_sp_overlay(session, *, target_snapshot_id, prior, base, inherited, skip):
    """Make ``target_snapshot_id`` an overlay of ``base``. ``inherited`` = {"site list"} carried from
    ``prior`` except the ``skip`` row ids. Returns ``(result, reason)``; never commits (the caller
    commits a result and rolls back a None)."""
    p = {"tsid": str(target_snapshot_id), "prior": str(prior), "base": str(base),
         "inherited": sorted(inherited), "skip": sorted(skip)}
    target = (await session.execute(text(
        "SELECT created_at FROM snapshots WHERE id = CAST(:tsid AS uuid) AND status = 'IN_PROGRESS' "
        "AND reuse_of_snapshot_id IS NULL AND (overlay_base_id IS NULL OR overlay_base_id = CAST(:base AS uuid)) "
        "AND extra_data->>'cancelled_at' IS NULL FOR NO KEY UPDATE"), p)).first()
    if target is None:
        return None, "target is not an in-progress snapshot of this base"
    if str(await session.scalar(text(f"SELECT {BASE_OF_PRIOR}"), p)) != p["base"]:
        return None, f"prior {prior} no longer sits on base {base}"
    # KEY SHARE: retention's DELETE of the base waits for this transaction, then sees the dependent.
    base_row = (await session.execute(text(
        "SELECT created_at FROM snapshots WHERE id = CAST(:base AS uuid) AND status IN ('COMPLETED', 'PARTIAL') "
        "AND extra_data->>'cancelled_at' IS NULL FOR KEY SHARE"), p)).first()
    if base_row is None:
        return None, f"base {base} is not a restorable snapshot"
    max_age = _env_float("SHAREPOINT_OVERLAY_MAX_AGE_DAYS", 7.0)
    if (target.created_at - base_row.created_at).total_seconds() > max_age * 86400:
        return None, f"base {base} is older than {max_age:g} days (compaction)"

    await session.execute(text("UPDATE snapshots SET overlay_base_id = CAST(:base AS uuid) "
                               "WHERE id = CAST(:tsid AS uuid)"), p)
    # A retried run of the same snapshot recomputes what it hides.
    await session.execute(text("DELETE FROM snapshot_overlay_removals WHERE snapshot_id = CAST(:tsid AS uuid)"), p)
    # Hide every base row the full copy would not hold: it holds this run's own rows plus, of a carried
    # list, the prior's rows not read again (_copy_forward_sp_list_rows).
    await session.execute(text(
        "INSERT INTO snapshot_overlay_removals (snapshot_id, item_type, external_id) "
        "SELECT CAST(:tsid AS uuid), b.item_type, b.external_id FROM snapshot_items b "
        f"WHERE b.snapshot_id = CAST(:base AS uuid) AND b.item_type IN {_TYPES} AND NOT " + _OWNED.format(a="b") +
        " AND NOT (b.metadata->>'source' = 'sp_rest'"
        "          AND (COALESCE(b.metadata->>'site_id', '') || ' ' || COALESCE(b.metadata->>'list_id', ''))"
        "              = ANY(CAST(:inherited AS text[]))"
        "          AND NOT b.external_id = ANY(CAST(:skip AS text[]))"
        "          AND b.id IN (SELECT e.id FROM snapshot_items_effective(CAST(:prior AS uuid)) e"
        "                        WHERE e.snapshot_id = CAST(:base AS uuid)))"
        " ON CONFLICT DO NOTHING"), p)
    # An own row identical to its base row adds nothing: the base row shows through (no removal above).
    same = " AND ".join(f"o.{c} IS NOT DISTINCT FROM b.{c}" for c in _SAME)
    await session.execute(text(
        "DELETE FROM snapshot_items o USING snapshot_items b "
        f"WHERE o.snapshot_id = CAST(:tsid AS uuid) AND o.item_type IN {_TYPES} "
        "AND b.snapshot_id = CAST(:base AS uuid) AND b.item_type = o.item_type AND b.external_id = o.external_id "
        f"AND {same} AND o.metadata::jsonb IS NOT DISTINCT FROM b.metadata::jsonb"), p)

    sizes = (await session.execute(text(
        f"SELECT (SELECT count(*) FROM snapshot_items WHERE snapshot_id = CAST(:tsid AS uuid) AND item_type IN {_TYPES}) AS own, "
        "       (SELECT count(*) FROM snapshot_overlay_removals WHERE snapshot_id = CAST(:tsid AS uuid)) AS removed, "
        f"      (SELECT count(*) FROM snapshot_items WHERE snapshot_id = CAST(:base AS uuid) AND item_type IN {_TYPES}) AS base_rows"),
        p)).one()
    ratio = _env_float("SHAREPOINT_OVERLAY_MAX_RATIO", 0.25)
    if sizes.own + sizes.removed > ratio * sizes.base_rows:
        return None, (f"{sizes.own} own + {sizes.removed} removed rows exceed {ratio:g} x "
                      f"{sizes.base_rows} base rows (compaction)")
    await session.execute(text(
        "UPDATE snapshots SET extra_data = (COALESCE(extra_data::jsonb, '{}'::jsonb) || jsonb_build_object("
        "'sp_overlay_base', CAST(:base AS text), 'overlay_prior', CAST(:prior AS text)))::json "
        "WHERE id = CAST(:tsid AS uuid)"), p)
    return {"own": sizes.own, "removed": sizes.removed, "base_rows": sizes.base_rows, "base": p["base"]}, "overlay"


async def try_sp_overlay(session_scope, target_snapshot_id, plan) -> Optional[dict]:
    """Build the overlay in one transaction; None (after logging why) means the caller copies the
    carried lists in full. An error never fails the backup, it only costs the full copy."""
    try:
        async with session_scope() as session:
            result, reason = await build_sp_overlay(
                session, target_snapshot_id=target_snapshot_id, prior=plan["prior"], base=plan["base"],
                inherited=plan["inherited"], skip=plan["skip"])
            if result is None:
                await session.rollback()
            else:
                await session.commit()
    except Exception as exc:
        result, reason = None, f"overlay build failed: {type(exc).__name__}: {exc}"
    if result is None:
        print(f"[SP-OVERLAY] full copy into snapshot {target_snapshot_id} — {reason}")
    else:
        print(f"[SP-OVERLAY] snapshot {target_snapshot_id} overlays base {result['base']} — {result['own']} own rows, "
              f"{result['removed']} removed, {result['base_rows']} base rows inherited")
    return result
