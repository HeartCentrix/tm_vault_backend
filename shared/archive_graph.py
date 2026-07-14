"""Exchange Online IN-PLACE ARCHIVE access via the Graph Mailbox Import/Export API.

The standard Graph mail API (``/users/{id}/mailFolders``) cannot reach a user's
archive mailbox (it 404s / 503s cross-server). This module uses the Mailbox
Import/Export API (``/admin/exchange/mailboxes/{mailboxId}``) instead, and
reuses a live ``GraphClient``'s ``_get``/``_post`` so it inherits the whole
token + multi-app rotation + pacing + 429/503 backoff machinery for free — the
only way a TB-scale, 3k-user archive sweep stays throttle-free.

Design (see the archive-backup design decision):
  * Backup unit = the opaque ``exportItems`` blob, stored 1:1 (restore-grade).
    The blob is Microsoft-opaque MAPI FastTransfer — **do not parse it**
    (HTML bodies + attachments live in separate MAPI props, not the embedded
    MIME; Exchange-origin items have no MIME at all). Restore replays the blob
    verbatim via ``createImportSession`` — the only lossless round-trip.
  * Export to EML/MBOX/PST is a separate restore-to-staging step, never a parse
    of this blob.

Archive support is ``/beta`` today; ``ARCHIVE_GRAPH_VERSION`` flips to ``v1.0``
at GA with no code change. The whole workload is gated by ``ARCHIVE_BACKUP_ENABLED``
(default off) so the code can ship + be tested on beta without touching prod.
"""
from __future__ import annotations

import base64
import os
from typing import Any, AsyncIterator, Dict, Iterator, List, Optional, Tuple

# API version segment. "beta" today (archive support is beta-only); set to
# "v1.0" via env the day archive support GAs — one flip, no code change.
ARCHIVE_GRAPH_VERSION = os.environ.get("ARCHIVE_GRAPH_VERSION", "beta")

# Master feature flag. Default OFF: ship + test on beta without exposing prod.
ARCHIVE_BACKUP_ENABLED = os.environ.get("ARCHIVE_BACKUP_ENABLED", "false").lower() in (
    "1", "true", "yes",
)

# Graph HARD cap: exportItems accepts at most 20 itemIds per POST.
EXPORT_BATCH_MAX = 20


def _base(version: Optional[str] = None) -> str:
    return f"https://graph.microsoft.com/{version or ARCHIVE_GRAPH_VERSION}"


def _batches(ids: List[str], n: int = EXPORT_BATCH_MAX) -> Iterator[List[str]]:
    for i in range(0, len(ids), n):
        yield ids[i:i + n]


async def get_archive_mailbox_id(gc, user_id: str, *, version: Optional[str] = None) -> Optional[str]:
    """Return the user's in-place archive mailbox id, or ``None`` if no archive
    is provisioned. Cheap single call; reuses the existing bulk-discovery era
    of ``/settings/exchange`` which also carries ``primaryMailboxId``."""
    j = await gc._get(f"{_base(version)}/users/{user_id}/settings/exchange")
    return (j or {}).get("inPlaceArchiveMailboxId") or None


async def list_archive_folders(gc, mailbox_id: str, *, version: Optional[str] = None) -> List[Dict[str, Any]]:
    """All folders in the archive mailbox (paged via @odata.nextLink)."""
    url = f"{_base(version)}/admin/exchange/mailboxes/{mailbox_id}/folders?$top=100"
    out: List[Dict[str, Any]] = []
    while url:
        j = await gc._get(url)
        out.extend(j.get("value", []) or [])
        url = j.get("@odata.nextLink")
    return out


# Lightweight per-item fields for enumeration + change detection. We deliberately
# do NOT $select body/content here — content comes only from exportItems (the
# opaque blob). changeKey drives incremental; size/type/dates drive the catalog.
_ITEM_SELECT = "id,changeKey,lastModifiedDateTime,size,type"


async def list_archive_items(
    gc, mailbox_id: str, folder_id: str, *, version: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """All items in a folder (paged via @odata.nextLink), lightweight fields only.

    Uses ``/items`` — NOT ``mailboxItem:delta``: the delta endpoint returns a
    truncated body (``…"value":[`` and nothing more, HTTP 200) on ``/beta`` today,
    so it is unusable. Incremental backup is therefore done by CHANGEKEY DIFF
    (see :func:`select_changed`) against the previous run's stored id->changeKey
    map, not a server-side delta cursor. Flip to delta once it works at GA
    (ponytail: server-side delta, changeKey-diff until the beta endpoint is fixed).
    """
    url = (
        f"{_base(version)}/admin/exchange/mailboxes/{mailbox_id}"
        f"/folders/{folder_id}/items?$top=200&$select={_ITEM_SELECT}"
    )
    out: List[Dict[str, Any]] = []
    while url:
        j = await gc._get(url)
        out.extend(j.get("value", []) or [])
        url = j.get("@odata.nextLink")
    return out


def select_changed(items: List[Dict[str, Any]], prev_changekeys: Optional[Dict[str, str]]) -> List[str]:
    """Item ids to (re)export this run: new items + items whose ``changeKey``
    differs from the previous backup. ``prev_changekeys`` = ``{item_id: changeKey}``
    persisted last run. Unchanged items are skipped — this is the incremental
    engine (a 22 GB archive is paid once, then near-zero)."""
    prev = prev_changekeys or {}
    return [it["id"] for it in items if it.get("id") and prev.get(it["id"]) != it.get("changeKey")]


async def export_items(
    gc, mailbox_id: str, item_ids: List[str], *, version: Optional[str] = None,
) -> AsyncIterator[Tuple[str, bytes, Optional[dict]]]:
    """Export items in <=20-id batches; yield ``(item_id, blob_bytes, error)``
    as each batch returns. Streaming (not list-returning) so a TB-scale mailbox
    never holds more than one batch of blobs in memory. The blob is the opaque
    full-fidelity stream to store 1:1; on a per-item error the blob is empty and
    ``error`` carries the Graph error dict (e.g. ``ErrorArchiveFolderMovedPermanently``
    on auto-expanded archives — caller reissues per the redirect).
    """
    for batch in _batches(list(item_ids)):
        j = await gc._post(
            f"{_base(version)}/admin/exchange/mailboxes/{mailbox_id}/exportItems",
            {"itemIds": batch},
        )
        for e in j.get("value", []) or []:
            err = e.get("error")
            data = e.get("data")
            blob = base64.b64decode(data) if (data and not err) else b""
            yield e.get("itemId"), blob, err
