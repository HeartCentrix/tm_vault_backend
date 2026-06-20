"""Activity-row shaping for BACKUP operations.

Single source of truth for Activity rows backed by `backup_batches`.
The legacy `_group_batch_jobs` reconstruction in audit-service/main.py
is no longer used for BACKUP — it produced a different user count
AND different progress number than `list_backup_batches` for the
same operator click, which surfaced in the 2026-05-15 incident
(card said 100%, detail said 78%; "9 users" displayed instead of
the operator's 54-user click count).

See docs/superpowers/specs/2026-05-15-backup-batch-race-fix-design.md.
"""
from __future__ import annotations

from typing import Any, Dict


_STATUS_LABEL = {
    "IN_PROGRESS": "In Progress",
    "COMPLETED": "Done",
    "PARTIAL": "Partial",
    "FAILED": "Failed",
    "CANCELLED": "Canceled",
}


def _fmt_bytes(n: int) -> str:
    """Minimal SI bytes formatter for the activity row. Matches the
    output style of audit-service's existing _fmt_bytes (binary units,
    two significant digits below 100, one digit at or above)."""
    if n is None:
        return "0 B"
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    f = float(n)
    for u in units:
        if abs(f) < 1024.0 or u == units[-1]:
            if f >= 100:
                return f"{f:.1f} {u}".replace(".0 ", " ")
            return f"{f:.2f} {u}"
        f /= 1024.0
    return f"{f:.2f} PiB"


def shape_activity_row(row) -> Dict[str, Any]:
    """Build one Activity-feed dict from a backup_batches row.

    `row` exposes (attribute access, like a SQLAlchemy Row):
      - batch_id (str), created_at (datetime), completed_at (datetime|None)
      - status (str — backup_batches.status raw value)
      - source (str), actor_email (str|None)
      - scope_user_ids (list[uuid|str])
      - bytes_expected (int|None), bytes_done (int)
      - job_ids (list[uuid|str])
      - waiting_discovery_count (int)
      - total_scope_count (int)
    """
    scope_count = int(row.total_scope_count or 0)
    user_label = (
        f"{scope_count} user" if scope_count == 1 else f"{scope_count} users"
    )

    bytes_done = int(row.bytes_done or 0)
    bytes_expected = int(row.bytes_expected) if row.bytes_expected else None
    progress_pct = None
    if bytes_expected and bytes_expected > 0:
        progress_pct = min(100, int(100 * bytes_done / bytes_expected))

    status_label = _STATUS_LABEL.get(row.status, row.status)

    if row.status == "COMPLETED":
        details = (
            f"{_fmt_bytes(bytes_done)} backed up" if bytes_done else "Completed"
        )
    elif row.status == "FAILED":
        details = "Failed"
    elif row.status == "CANCELLED":
        details = "Cancelled"
    elif row.status == "PARTIAL":
        details = f"Partial — {_fmt_bytes(bytes_done)} backed up"
    else:
        # IN_PROGRESS — bytes-only progress hint, no percent. The UI
        # already renders the percent in the dedicated progress bar +
        # detail-panel header (driven by progressPct). Carrying a
        # second percent here produced visible mismatches across
        # polls (row text 78 % vs bar 83 % vs detail 84 %); strip it
        # so the dedicated bar is the single source of truth.
        bits = []
        if bytes_done and bytes_expected:
            bits.append(
                f"{_fmt_bytes(bytes_done)} of {_fmt_bytes(bytes_expected)}"
            )
        elif bytes_done:
            bits.append(f"{_fmt_bytes(bytes_done)} so far")
        else:
            bits.append("In progress")
        waiting = int(row.waiting_discovery_count or 0)
        if waiting > 0:
            bits.append(f"— discovering {waiting} of {scope_count}")
        details = " ".join(bits)

    return {
        "id": str(row.batch_id),
        "batchId": str(row.batch_id),
        "start_time": row.created_at.isoformat() if row.created_at else None,
        "finish_time": (
            row.completed_at.isoformat() if row.completed_at else None
        ),
        "status": status_label,
        "operation": "BACKUP",
        "object": user_label,
        "details": details,
        "batchSource": row.source,
        "jobIds": [str(j) for j in (row.job_ids or [])],
        "progressPct": progress_pct,
        "bytesDone": bytes_done,
        "bytesExpected": bytes_expected,
    }


def merge_backup_batch_rows(
    legacy_items: list[Dict[str, Any]],
    v2_rows: list[Dict[str, Any]],
) -> list[Dict[str, Any]]:
    """Overlay backup_batches rows onto legacy rollup rows.

    ``backup_batches`` owns operator intent (the clicked scope: "11 users",
    one user name, source, etc.). The legacy rollup owns live progress derived
    from Jobs/Snapshots/Partitions. Merge the two by batchId so v2 rows keep
    the correct scope label without losing progress/details while
    ``bytes_expected`` is unknown on first-ever backups.
    """
    legacy_by_batch = {
        str(row.get("batchId")): row
        for row in legacy_items
        if row.get("operation") == "BACKUP" and row.get("batchId")
    }

    merged: list[Dict[str, Any]] = []
    v2_batch_ids: set[str] = set()
    for source in v2_rows:
        row = dict(source)
        batch_id = row.get("batchId")
        if batch_id:
            v2_batch_ids.add(str(batch_id))
        fallback = legacy_by_batch.get(str(batch_id)) if batch_id else None

        if fallback:
            legacy_progress = fallback.get("progress_pct")
            if legacy_progress is None:
                legacy_progress = fallback.get("progressPct")
            if row.get("progressPct") is None and legacy_progress is not None:
                row["progressPct"] = legacy_progress
            if row.get("progress_pct") is None and legacy_progress is not None:
                row["progress_pct"] = legacy_progress

            if not row.get("details") and fallback.get("details"):
                row["details"] = fallback["details"]
            if not row.get("finish_time") and fallback.get("finish_time"):
                row["finish_time"] = fallback["finish_time"]

            for key in (
                "phase",
                "counts",
                "warnings",
                "cancellable",
                "data_backed_up",
                "total_data",
            ):
                if row.get(key) is None and fallback.get(key) is not None:
                    row[key] = fallback[key]

        merged.append(row)

    for row in legacy_items:
        if row.get("operation") == "BACKUP" and str(row.get("batchId")) in v2_batch_ids:
            continue
        merged.append(row)

    return merged


_TERMINAL_BATCH_STATUSES = {"COMPLETED", "PARTIAL", "FAILED", "CANCELLED"}


def prune_unrun_terminal_children(
    resources: list[Dict[str, Any]],
    batch_status: str | None,
) -> list[Dict[str, Any]]:
    """Hide child workloads that did not participate in a terminal batch.

    The drilldown endpoint expands an ENTRA_USER to every discovered Tier-2
    child. That is useful while a batch is live because not-yet-created
    snapshots should appear as pending work. Once the batch is terminal,
    no-snapshot children mean "not part of this run" (for example a Mail-only
    SLA), not "still backing up".
    """
    if (batch_status or "").upper() not in _TERMINAL_BATCH_STATUSES:
        return resources

    pruned: list[Dict[str, Any]] = []
    for parent in resources:
        entry = dict(parent)
        children = entry.get("children")
        if isinstance(children, list):
            entry["children"] = [
                dict(child)
                for child in children
                if child.get("snapshotId")
            ]
        pruned.append(entry)

    return pruned
