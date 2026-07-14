"""Pure, testable helpers for the Exchange in-place ARCHIVE backup handler.

The heavy orchestration (snapshot lifecycle, blob upload, Graph fan-out) lives
on the worker in ``main.py``; the incremental-correctness CORE lives here so it
can be unit-tested without the 9k-line worker. Pairs with
``shared/archive_graph.py`` (the Graph Import/Export adapter).

Model: each item's OPAQUE MAPI FastTransfer blob is stored 1:1 (never parsed).
Incremental is driven by a per-resource ``{item_id: changeKey}`` STATE BLOB
(the ``mailboxItem:delta`` endpoint is broken on ``/beta``) — held in object
storage, O(1) load+save, no DB row churn. Each snapshot stays SPARSE (only that
run's changed items), matching every other incremental handler; the full
catalog is reconstructed from the snapshot chain at restore.
"""
from __future__ import annotations

import uuid
from typing import Any, Dict, Optional

# Object-storage workload string for opaque archive blobs. Deliberately NOT
# "email": the readable Graph-JSON mail blobs and the opaque MAPI archive blobs
# must never share a container — different restore semantics + GC lifecycles.
ARCHIVE_WORKLOAD = "archive"

ARCHIVE_ITEM_TYPE = "ARCHIVE_ITEM"


# Error signals meaning "this archive is inaccessible to us" — the app lacks
# consent in the tenant, or the mailbox is unlicensed / gone. These must SKIP
# gracefully (benign 0-item snapshot), never mark the backup FAILED. Everything
# else (5xx, timeouts, bugs) is a real/transient failure that should fail+retry.
_SKIP_SIGNALS = (
    "MailboxNotEnabledForRESTAPI", "ErrorAccessDenied", "Authorization_RequestDenied",
    "ErrorMailboxNotFound", "does not have access", "Access is denied", "Forbidden",
    "Unauthorized",
)


def is_skippable_error(error_text: Optional[str]) -> bool:
    """True if the error means the archive is simply inaccessible (skip), not a
    transient/real failure (fail + retry)."""
    s = error_text or ""
    sl = s.lower()
    if any(sig.lower() in sl for sig in _SKIP_SIGNALS):
        return True
    # bare HTTP auth/consent status codes (GraphClient stringifies errors)
    return any(code in s for code in ("403", "401"))


def item_kind(mapi_type: Optional[str]) -> str:
    """Map a mailboxItem's MAPI message class (the only descriptive field the
    Import/Export items endpoint exposes — live-verified that $select of
    subject/sender/from returns HTTP 400 "no such property") to a human kind for
    the Recovery browse. Subjects are unavailable without staging, so this is the
    most a catalog row can say about "what it is"."""
    t = (mapi_type or "").strip()
    tl = t.lower()
    if tl.startswith("ipm.note"):
        return "Email"
    if tl.startswith("ipm.contact") or tl.startswith("ipm.distlist"):
        return "Contact"
    if tl.startswith("ipm.appointment") or tl.startswith("ipm.schedule"):
        return "Calendar event"
    if tl.startswith("ipm.task"):
        return "Task"
    if tl.startswith("ipm.stickynote"):
        return "Note"
    if tl.startswith("ipm.activity"):
        return "Journal"
    if tl.startswith("ipm.post"):
        return "Post"
    if tl.startswith("report."):
        return "Delivery report"
    return t or "Item"


def state_blob_path(tenant_id: str, resource_id: str) -> str:
    """Deterministic, OVERWRITABLE path for a resource's incremental changeKey
    state blob. Fixed (no snapshot id / timestamp) so every run reads and
    overwrites the same object — this is the live cursor, not per-run history."""
    return f"{tenant_id}/{resource_id}/_archive_state/changekeys.json"


def advance_state(
    prev: Optional[Dict[str, str]],
    current_ck: Dict[str, str],
    stored_ck: Dict[str, str],
) -> Dict[str, str]:
    """Next incremental state (``item_id -> changeKey``) after a run.

    * ``current_ck`` — every item id present in the archive right now.
    * ``stored_ck`` — items we successfully EXPORTED + STORED this run (new key).
    * ``prev``       — last run's state (``None`` on the first run).

    Per item id in ``current_ck``:
      - stored this run           -> take the newly-stored key (advance)
      - else known in ``prev``    -> keep the prev key. Two cases collapse here:
          an UNCHANGED item (key already matched, nothing to do), and a CHANGED
          item whose export FAILED — keeping the OLD key makes
          ``select_changed`` re-flag it next run, i.e. automatic per-item retry.
      - else (brand-new + failed) -> omit, so it's retried next run.

    Items absent from ``current_ck`` (deleted from the archive) are DROPPED —
    their backed-up blobs stay in prior snapshots (never deleted); the driver
    just stops tracking them, and a re-created item re-exports from scratch.
    """
    prev = prev or {}
    out: Dict[str, str] = {}
    for iid, _ck in current_ck.items():
        if iid in stored_ck:
            out[iid] = stored_ck[iid]
        elif iid in prev:
            out[iid] = prev[iid]
        # else: brand-new item we failed to store -> omit (retry next run)
    return out


def build_archive_item_row(
    *, snapshot_id: Any, tenant_id: Any, folder: Dict[str, Any], item: Dict[str, Any],
    blob_path: str, content_hash: str, blob_len: int, mailbox_id: str,
    graph_version: str,
) -> Dict[str, Any]:
    """One SnapshotItem dict for ``_bulk_upsert_snapshot_items`` (mapped-class
    insert -> the JSON-envelope key MUST be ``extra_data``, NOT the column name
    ``metadata``, or it silently vanishes). The item's ``changeKey`` rides in
    ``extra_data`` so the catalog self-describes for restore; the blob itself is
    the opaque MAPI FastTransfer stream, stored verbatim."""
    kind = item_kind(item.get("type"))
    return {
        "id": uuid.uuid4(),
        "snapshot_id": snapshot_id,
        "tenant_id": tenant_id,
        "external_id": item["id"],
        "parent_external_id": folder.get("id"),
        "item_type": ARCHIVE_ITEM_TYPE,
        # No subject is available from the opaque endpoint — the friendly kind
        # (Email/Contact/Calendar…) is the most a browse row can show.
        "name": kind,
        "folder_path": folder.get("displayName") or folder.get("id"),
        "content_size": blob_len,
        "content_hash": content_hash,
        "blob_path": blob_path,
        "content_checksum": content_hash,
        "extra_data": {
            "change_key": item.get("changeKey"),
            "opaque": True,
            "kind": kind,
            "mapi_type": item.get("type"),
            "mailbox_id": mailbox_id,
            "graph_version": graph_version,
            "size": item.get("size"),
            "last_modified": item.get("lastModifiedDateTime"),
        },
    }
