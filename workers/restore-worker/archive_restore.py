"""In-place restore of Exchange in-place ARCHIVE items via the Graph
Import/Export API.

Archive items are backed up as OPAQUE MAPI FastTransfer blobs (never parsed —
HTML bodies + attachments live in separate MAPI props). Restore replays each
blob VERBATIM into the mailbox through a preauthenticated import session — the
only lossless round-trip. Pairs with ``shared/archive_graph.py`` (the adapter
that owns ``create_import_session`` / ``import_item``).

The import POST goes to a PREAUTHENTICATED ``outlook.office365.com`` URL whose
token is embedded — it must carry NO ``Authorization`` header. ``GraphClient._post``
always attaches auth and targets ``graph.microsoft.com``, so it is the wrong
callable; :func:`post_json_no_auth` is the bare client the import path needs.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from shared import archive_graph
# Canonical home for the unauth import POST is shared/archive_export.py (both the
# restore + enrichment paths stage through it). Re-exported here so restore-path
# callers and test_archive_restore's `ar.post_json_no_auth` keep resolving.
from shared.archive_export import post_json_no_auth  # noqa: F401


def resolve_target_mailbox_id(items: List[Any]) -> Optional[str]:
    """The archive mailbox id every item was exported from (stashed in
    ``extra_data.mailbox_id`` at backup). All items under one USER_ARCHIVE share
    it; return the first non-empty one."""
    for it in items:
        mid = (getattr(it, "extra_data", None) or {}).get("mailbox_id")
        if mid:
            return mid
    return None


def _parse_expiry(iso: Optional[str]) -> Optional[datetime]:
    if not iso:
        return None
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except Exception:
        return None


class ArchiveRestoreEngine:
    """Batched in-place restore: open ONE import session per mailbox, replay
    each opaque blob into its origin folder, refresh the session before it
    expires. Failures are per-item (logged + counted), never fatal."""

    def __init__(
        self,
        graph_client,
        *,
        download_blob: Callable[[Any], Any],
        post_json=post_json_no_auth,
        worker_id: str = "restore",
        refresh_margin_s: int = 120,
    ):
        self.gc = graph_client
        self.download_blob = download_blob
        self.post_json = post_json
        self.worker_id = worker_id
        self.refresh_margin_s = refresh_margin_s

    def _near_expiry(self, exp_dt: Optional[datetime]) -> bool:
        if exp_dt is None:
            return False
        return (exp_dt - datetime.now(timezone.utc)).total_seconds() <= self.refresh_margin_s

    async def _prepare_primary_folder(
        self, user_id: str, name: str,
    ) -> Optional[Tuple[str, str]]:
        """Create a folder ``name`` in the user's PRIMARY (active) mailbox and
        resolve its Import/Export FolderId → ``(primary_mailbox_id, folder_ie_id)``
        or ``None`` if it can't be prepared. This is where "recover to a separate
        folder" lands an archived item — the in-place archive has no folder-create
        API, and a recovered item is most useful in the active mailbox. Mirrors
        the enrichment stager's proven create→resolve pattern (the mail-create API
        and the I/E list API are separate subsystems, so the new folder can lag a
        few seconds before it appears — poll with backoff)."""
        primary_id = await archive_graph.get_primary_mailbox_id(self.gc, user_id)
        if not primary_id:
            return None
        graph_url = getattr(self.gc, "GRAPH_URL", "https://graph.microsoft.com/v1.0")
        try:
            await self.gc._post(
                f"{graph_url}/users/{user_id}/mailFolders",
                {"displayName": name, "isHidden": False},
            )
        except Exception as e:
            # A 409 (folder already exists) is fine — we resolve by name next.
            print(f"[{self.worker_id}] [ARCHIVE_RESTORE] create folder {name!r}: "
                  f"{type(e).__name__} (resolving existing)")
        attempts = int(os.getenv("ARCHIVE_STAGE_FOLDER_RESOLVE_ATTEMPTS", "8"))
        delay = float(os.getenv("ARCHIVE_STAGE_FOLDER_RESOLVE_DELAY_S", "2.0"))
        for i in range(max(1, attempts)):
            for f in await archive_graph.list_archive_folders(self.gc, primary_id):
                if (f.get("displayName") or "") == name:
                    return primary_id, f.get("id")
            if i < attempts - 1:
                await asyncio.sleep(delay)
        return None

    async def run(
        self, items: List[Any], *, mailbox_id: Optional[str] = None, mode: str = "create",
        target_folder: Optional[str] = None, user_id: Optional[str] = None,
    ) -> Dict[str, int]:
        # "Recover to a separate folder" → create the named folder in the ACTIVE
        # mailbox and import there. Otherwise (in-place) → back to each item's
        # origin archive folder (parent_external_id) in its archive mailbox.
        name = (target_folder or "").strip("/").split("/")[-1].strip() if target_folder else ""
        folder_override: Optional[str] = None
        if name and user_id:
            prep = await self._prepare_primary_folder(user_id, name)
            if not prep:
                print(f"[{self.worker_id}] [ARCHIVE_RESTORE] target folder {name!r} could not "
                      f"be prepared in the primary mailbox — {len(items)} item(s) NOT restored")
                return {"created": 0, "failed": len(items), "reason": "target_folder_unavailable"}
            mailbox_id, folder_override = prep
        else:
            mailbox_id = mailbox_id or resolve_target_mailbox_id(items)
            if not mailbox_id:
                print(f"[{self.worker_id}] [ARCHIVE_RESTORE] no target mailbox id — cannot restore {len(items)} item(s)")
                return {"created": 0, "failed": len(items), "reason": "no_mailbox_id"}

        import_url, expiry = await archive_graph.create_import_session(self.gc, mailbox_id)
        exp_dt = _parse_expiry(expiry)
        created = failed = 0

        for it in items:
            # Refresh the preauth session before it lapses mid-restore.
            if self._near_expiry(exp_dt):
                import_url, expiry = await archive_graph.create_import_session(self.gc, mailbox_id)
                exp_dt = _parse_expiry(expiry)

            # Named-folder recovery imports every item into the one target folder;
            # in-place uses each item's captured origin folder.
            folder_id = folder_override or getattr(it, "parent_external_id", None)
            if not folder_id:
                print(f"[{self.worker_id}] [ARCHIVE_RESTORE] item {getattr(it,'external_id',None)} has no folder — skipped")
                failed += 1
                continue
            try:
                blob = await self.download_blob(it)
                if not blob:
                    failed += 1
                    continue
                await archive_graph.import_item(self.post_json, import_url, folder_id, blob, mode=mode)
                created += 1
            except Exception as e:
                print(
                    f"[{self.worker_id}] [ARCHIVE_RESTORE] import failed "
                    f"item={getattr(it, 'external_id', None)}: {type(e).__name__}: {e}"
                )
                failed += 1

        dest = f"folder={name!r} (primary mbx {mailbox_id})" if folder_override else f"mailbox={mailbox_id}"
        print(f"[{self.worker_id}] [ARCHIVE_RESTORE] {dest} created={created} failed={failed}")
        return {"created": created, "failed": failed}
