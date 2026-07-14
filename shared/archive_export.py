"""Archive → EML/MBOX/PST EXPORT + PREVIEW-ENRICHMENT via restore-to-staging.

The opaque MAPI blob can't be parsed, so to read an archive item's content we
stage it: import blobs into a TEMP folder of the user's OWN PRIMARY mailbox (the
only mailbox the standard Graph mail API can read), read them back as normal
Graph JSON, then either hand that JSON to the EXISTING exporters (build_eml /
MboxWriter / pst_convert) unchanged, or map it back onto the source
``ARCHIVE_ITEM`` row for preview-enrichment. Footprint is bounded by batching
(import → read → empty → repeat), and the temp folder is ALWAYS deleted at the
end — staging must never leave archive data sitting in the user's live mailbox.

Split into a TESTABLE engine (:class:`ArchiveExportStager` — the batching loop +
the cleanup guarantee) and the LIVE Graph ops (:class:`LiveStagingOps`), injected
so the engine is unit-tested with fakes. The live ops are exercised end-to-end by
the Phase-6 round-trip proof.

Lives in ``shared/`` because BOTH the restore-worker (export-to-file) and the
enrichment-worker (preview-enrichment) stage through it.
"""
from __future__ import annotations

import os
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Tuple

import httpx

from shared import archive_graph


async def post_json_no_auth(
    url: str, body: Dict[str, Any], *, _client_transport: Optional[Any] = None,
) -> Dict[str, Any]:
    """POST JSON to a preauthenticated import URL with NO Authorization header.

    The Import/Export API returns a preauthenticated ``outlook.office365.com``
    ``importUrl`` whose token is embedded — it must carry NO ``Authorization``
    header (``GraphClient._post`` always attaches auth + targets
    ``graph.microsoft.com``, so it is the wrong callable). Canonical home for
    the import path; ``archive_restore`` re-exports it for the restore side.

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


def _chunks(seq: List[Any], n: int):
    n = max(1, int(n))
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def message_to_eml(msg: Dict[str, Any]) -> bytes:
    """One staged Graph JSON message (read back with ``$expand=attachments``) →
    EML bytes, reusing the SHARED mail exporters so archive EML/MBOX get exactly
    the same fidelity as primary mail — inline file attachments AND mail-in-mail
    (``itemAttachment`` nested emails become ``message/rfc822`` parts)."""
    from shared.mime_builder import (
        build_eml,
        build_nested_message_attachment,
        _inline_attachment_refs,
    )
    atts = msg.get("attachments") or []
    refs = _inline_attachment_refs(atts)  # fileAttachment (+ inline images)
    for a in atts:
        if isinstance(a, dict) and "itemattachment" in (a.get("@odata.type") or "").lower():
            nested = a.get("item")
            if isinstance(nested, dict):
                refs.append(build_nested_message_attachment(nested))
    return build_eml(msg, refs)


class ArchiveExportStager:
    """Stage opaque archive blobs through a temp folder and yield readable Graph
    JSON. ``ops`` supplies the live operations; ``download_blob`` fetches an
    item's opaque bytes. The temp folder is created once and GUARANTEED-deleted
    in ``finally`` even if any batch fails midway."""

    def __init__(self, *, ops, download_blob: Callable[[Any], Any],
                 batch_size: int = 200, worker_id: str = "restore"):
        self.ops = ops
        self.download_blob = download_blob
        self.batch_size = int(batch_size)
        self.worker_id = worker_id

    async def stage_and_read(self, archive_items: List[Any]) -> AsyncIterator[Dict[str, Any]]:
        temp_std_id = await self.ops.create_temp_folder()
        try:
            temp_ie_id = await self.ops.resolve_ie_folder_id(temp_std_id)
            for batch in _chunks(list(archive_items), self.batch_size):
                import_url, _exp = await self.ops.create_import_session()
                imported = 0
                for it in batch:
                    blob = await self.download_blob(it)
                    if not blob:
                        continue
                    await self.ops.import_blob(import_url, temp_ie_id, blob)
                    imported += 1
                if not imported:
                    continue  # nothing landed -> no read/empty for this batch
                async for msg in self.ops.read_messages(temp_std_id):
                    yield msg
                # Bound the live-mailbox footprint: clear the batch before the next.
                await self.ops.empty_folder(temp_std_id)
        finally:
            # Never leave imported archive data in the user's live mailbox.
            await self.ops.delete_folder(temp_std_id)

    async def stage_and_map(self, items: List[Any]) -> AsyncIterator[tuple]:
        """Like :meth:`stage_and_read`, but yields ``(source_item, graph_json)``
        pairs — the ENRICHMENT path needs to write each staged message's JSON
        back onto its ORIGINAL ``ARCHIVE_ITEM`` row. Matching is by the scratch
        item id returned from ``import_blob``: import assigns a new id, the temp
        folder read returns messages carrying those ids, so ``{new_id: source}``
        maps a staged message back to its source. Same guaranteed cleanup."""
        temp_std_id = await self.ops.create_temp_folder()
        try:
            temp_ie_id = await self.ops.resolve_ie_folder_id(temp_std_id)
            for batch in _chunks(list(items), self.batch_size):
                import_url, _exp = await self.ops.create_import_session()
                new_to_src: Dict[str, Any] = {}
                for it in batch:
                    blob = await self.download_blob(it)
                    if not blob:
                        continue
                    new_id = await self.ops.import_blob(import_url, temp_ie_id, blob)
                    if new_id:
                        new_to_src[new_id] = it
                if not new_to_src:
                    continue
                async for msg in self.ops.read_messages(temp_std_id):
                    src = new_to_src.get(msg.get("id"))
                    if src is not None:  # drop any stray/pre-existing scratch msg
                        yield src, msg
                await self.ops.empty_folder(temp_std_id)
        finally:
            await self.ops.delete_folder(temp_std_id)


class LiveStagingOps:
    """Live Graph implementation of the staging ops. Standard mail API (v1.0) for
    the temp folder + message reads on the PRIMARY mailbox; the Import/Export API
    (via ``archive_graph``, beta) for the opaque-blob import. NOTE: exercised by
    the Phase-6 live round-trip proof — the batching/cleanup logic it plugs into
    is unit-tested in :class:`ArchiveExportStager`."""

    def __init__(self, gc, *, user_id: str, primary_mailbox_id: str, folder_name: str,
                 post_json, read_page_size: int = 50):
        self.gc = gc
        self.user_id = user_id
        self.primary_mailbox_id = primary_mailbox_id
        self.folder_name = folder_name
        self.post_json = post_json
        self.read_page_size = read_page_size

    def _graph(self) -> str:
        return getattr(self.gc, "GRAPH_URL", "https://graph.microsoft.com/v1.0")

    async def create_temp_folder(self) -> str:
        j = await self.gc._post(
            f"{self._graph()}/users/{self.user_id}/mailFolders",
            {"displayName": self.folder_name, "isHidden": True},
        )
        return j["id"]

    async def resolve_ie_folder_id(self, _std_id: str) -> str:
        # The Import/Export API folder-id namespace differs from standard Graph,
        # so match our just-created temp folder by displayName in the I/E listing.
        folders = await archive_graph.list_archive_folders(self.gc, self.primary_mailbox_id)
        for f in folders:
            if f.get("displayName") == self.folder_name:
                return f["id"]
        raise RuntimeError(f"temp folder {self.folder_name!r} not found in Import/Export folder listing")

    async def create_import_session(self) -> Tuple[Optional[str], Optional[str]]:
        return await archive_graph.create_import_session(self.gc, self.primary_mailbox_id)

    async def import_blob(self, import_url: str, folder_ie_id: str, blob: bytes) -> Optional[str]:
        # Returns the scratch-mailbox item id so enrichment can map the staged
        # message back to its source ARCHIVE_ITEM row.
        res = await archive_graph.import_item(self.post_json, import_url, folder_ie_id, blob, mode="create")
        return (res or {}).get("itemId") if isinstance(res, dict) else None

    async def read_messages(self, std_id: str) -> AsyncIterator[Dict[str, Any]]:
        url = (
            f"{self._graph()}/users/{self.user_id}/mailFolders/{std_id}"
            f"/messages?$expand=attachments&$top={self.read_page_size}"
        )
        while url:
            j = await self.gc._get(url)
            for m in j.get("value", []) or []:
                yield m
            url = j.get("@odata.nextLink")

    async def empty_folder(self, std_id: str) -> None:
        # Delete this batch's messages so the next batch's import stays bounded.
        # ponytail: per-message DELETE; switch to Graph $batch (20/req) if a
        # huge single-folder batch makes this the bottleneck.
        url = f"{self._graph()}/users/{self.user_id}/mailFolders/{std_id}/messages?$select=id&$top=100"
        while url:
            j = await self.gc._get(url)
            for m in j.get("value", []) or []:
                await self.gc._delete(f"{self._graph()}/users/{self.user_id}/messages/{m['id']}")
            url = j.get("@odata.nextLink")

    async def delete_folder(self, std_id: str) -> None:
        await self.gc._delete(f"{self._graph()}/users/{self.user_id}/mailFolders/{std_id}")
