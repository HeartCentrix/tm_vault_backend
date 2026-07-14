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

import os
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

import httpx

from shared import archive_graph


async def post_json_no_auth(
    url: str, body: Dict[str, Any], *, _client_transport: Optional[Any] = None,
) -> Dict[str, Any]:
    """POST JSON to a preauthenticated import URL with NO Authorization header.

    ``_client_transport`` is a test seam (an ``httpx.MockTransport``); production
    callers omit it and a real network client is used."""
    timeout = float(os.getenv("ARCHIVE_IMPORT_TIMEOUT_S", "120"))
    kwargs: Dict[str, Any] = {"timeout": timeout}
    if _client_transport is not None:
        kwargs["transport"] = _client_transport
    async with httpx.AsyncClient(**kwargs) as client:
        resp = await client.post(url, json=body)
        resp.raise_for_status()
        return resp.json() if resp.content else {}


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

    async def run(
        self, items: List[Any], *, mailbox_id: Optional[str] = None, mode: str = "create",
    ) -> Dict[str, int]:
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

            folder_id = getattr(it, "parent_external_id", None)
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

        print(f"[{self.worker_id}] [ARCHIVE_RESTORE] mailbox={mailbox_id} created={created} failed={failed}")
        return {"created": created, "failed": failed}
