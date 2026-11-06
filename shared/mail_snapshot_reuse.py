"""An unchanged USER_MAIL incremental reuses its prior full snapshot instead of copying it forward.

The reuse snapshot owns zero ``snapshot_items`` rows; ``reuse_of_snapshot_id`` and
``reuse_chain_root_id`` both name the prior full snapshot, and readers resolve through
``shared.snapshot_reuse``. Railway 2026-11-05 (incremental #9): all 10 mailboxes were unchanged,
yet each copied its whole mailbox (Rohit 7,085 rows: 280 s wall for 31 s of copying).
"""
from __future__ import annotations

from sqlalchemy import text

# Prior = newest COMPLETED/PARTIAL snapshot of a DIFFERENT job that actually holds mail rows (a
# 0-row run - fingerprint-skip, empty delta - must not restart the chain empty; an overlay holds
# its base's rows even when it owns none), is not a reuse-chain pointer, and did not itself fail
# its carry-forward (its inventory is known-incomplete).
# Shared by the carry-forward (its copy source) and the reuse (its pointer target), so both always
# name the same snapshot. Binds :rid, :tsid, :jid.
MAIL_PRIOR_SNAPSHOT_SQL = (
    "(SELECT s2.id FROM snapshots s2 "
    "  WHERE s2.resource_id = :rid "
    "    AND s2.id <> :tsid "
    "    AND s2.status IN ('COMPLETED', 'PARTIAL') "
    "    AND s2.job_id IS DISTINCT FROM CAST(:jid AS UUID) "
    "    AND s2.reuse_of_snapshot_id IS NULL "
    "    AND s2.reuse_chain_root_id IS NULL "
    "    AND COALESCE(s2.extra_data->>'mail_carry_forward_failed', 'false') <> 'true' "
    "    AND COALESCE(s2.extra_data->>'mail_delta_only', 'false') <> 'true' "
    "    AND COALESCE(s2.extra_data->>'mail_carry_forward_pending', 'false') <> 'true' "
    "    AND s2.extra_data->>'cancelled_at' IS NULL "
    "    AND s2.created_at < (SELECT t.created_at FROM snapshots t WHERE t.id = :tsid) "
    "    AND (COALESCE(s2.extra_data->>'mail_union_carry_claimed', 'false') = 'true' "
    "         OR NOT EXISTS (SELECT 1 FROM snapshot_partitions sp "
    "                         WHERE sp.snapshot_id = s2.id "
    "                           AND sp.partition_type = 'MAIL_FOLDERS' "
    "                           AND (sp.payload::jsonb) ? 'carry_forward')) "
    "    AND (s2.overlay_base_id IS NOT NULL "
    "         OR EXISTS (SELECT 1 FROM snapshot_items x "
    "                     WHERE x.snapshot_id = s2.id "
    "                       AND x.item_type IN ('EMAIL', 'EMAIL_ATTACHMENT'))) "
    "  ORDER BY s2.created_at DESC "
    "  LIMIT 1)"
)


async def reuse_unchanged_mail_snapshot(session, *, resource_id, target_snapshot_id, current_job_id,
                                        keep_folder_ids=None):
    """Point ``target_snapshot_id`` (which wrote no rows) at the carry-forward's prior snapshot and copy
    its totals (nothing new, nothing added). Returns the prior id, or None with nothing written when
    there is no prior, the prior is PARTIAL (a pointer may only name a COMPLETED snapshot; an older
    COMPLETED one would miss the PARTIAL run's mail), or the target already owns rows. Pointers
    always name a full snapshot, so a chain is never more than one hop. ``keep_folder_ids`` (the
    shared/room mailbox carry's folder whitelist): every prior message must sit in one of them, as
    that copy drops the rest and pathless rows."""
    params = {"rid": str(resource_id), "tsid": str(target_snapshot_id),
              "jid": str(current_job_id) if current_job_id else None}
    prior = (await session.execute(text(
        "SELECT p.id, p.status::text AS status, p.item_count, p.bytes_total FROM snapshots p "
        f"WHERE p.id = {MAIL_PRIOR_SNAPSHOT_SQL}"), params)).first()
    if prior is None or prior.status != "COMPLETED":
        return None
    if keep_folder_ids is not None and await session.scalar(text(
            "SELECT EXISTS (SELECT 1 FROM snapshot_items x WHERE x.snapshot_id = CAST(:prior AS UUID) "
            "AND x.item_type = 'EMAIL' AND COALESCE(x.metadata::jsonb->'raw'->>'parentFolderId', '') "
            "<> ALL(CAST(:keep AS TEXT[])))"), {"prior": str(prior.id), "keep": list(keep_folder_ids)}):
        return None
    from shared.snapshot_reuse import point_at
    pointed = await point_at(session, target_snapshot_id=target_snapshot_id, prior_snapshot_id=prior.id,
                             marker="mail_reused_prior")
    return prior.id if pointed else None


def mail_unchanged_reuse_enabled(env=None) -> bool:
    """Kill switch: ``MAIL_UNCHANGED_REUSE_ENABLED`` = 0/false/no/off copies unchanged mailboxes forward
    as before. Default on."""
    import os
    flag = str((os.environ if env is None else env).get("MAIL_UNCHANGED_REUSE_ENABLED", "") or "").strip().lower()
    return flag not in ("0", "false", "no", "off")


async def reuse_if_unchanged(session_factory, *, unchanged, resource_id, target_snapshot_id, current_job_id,
                             keep_folder_ids=None):
    """The worker's entry point: the prior id when this unchanged run now points at it, else None and the
    caller carries forward as before (a failure here never fails the backup)."""
    if not unchanged or not mail_unchanged_reuse_enabled():
        return None
    import logging
    try:
        async with session_factory() as session:
            prior = await reuse_unchanged_mail_snapshot(session, resource_id=resource_id,
                target_snapshot_id=target_snapshot_id, current_job_id=current_job_id, keep_folder_ids=keep_folder_ids)
            await session.commit()
    except Exception as exc:
        logging.getLogger(__name__).warning("mail_unchanged_reuse_failed snapshot=%s error=%s: %s",
                                            target_snapshot_id, type(exc).__name__, exc)
        return None
    return prior
