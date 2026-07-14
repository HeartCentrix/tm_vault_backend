"""MailPstWriter: EMAIL items → Graph message JSON (with inlined
attachments) → pstwriter ``pst_convert mail``.

Per-item flow:
1. ``mail_fetch.fetch_message`` — load the Graph message JSON (inline or
   from blob storage).
2. Read sibling ``EMAIL_ATTACHMENT`` SnapshotItems off
   ``item._email_attachment_items`` (stamped by ``PstExportOrchestrator``
   from a pre-fetched index keyed by ``parent_item_id``).
3. Download each attachment blob, base64-encode, and inject as a
   ``#microsoft.graph.fileAttachment`` entry on the message JSON. The
   pstwriter library reads ``contentBytes`` directly out of these
   entries, so no EML round-trip is needed.

Returns the augmented message dict; the base class accumulates a list
of these dicts and ships them in one ``pst_convert`` call per chunk.
"""
from __future__ import annotations

import base64
import logging
import os
import sys

# Make the sibling restore-worker directory importable so `from mail_fetch
# import ...` resolves both in production (where the worker dir is on
# PYTHONPATH) and in tests.
_HERE = os.path.dirname(__file__)
_WORKER = os.path.abspath(os.path.join(_HERE, ".."))
if _WORKER not in sys.path:
    sys.path.insert(0, _WORKER)

from .base import PstWriterBase, MAX_ATTACHMENT_BYTES  # noqa: E402

logger = logging.getLogger(__name__)

# Non-reserved root folder that Online Archive items are nested under in a PST,
# so Outlook's import can't merge them into the special "Archive" folder. Keeps
# the source archive hierarchy underneath. Overridable if a client prefers a
# different label.
ARCHIVE_PST_ROOT = os.getenv("ARCHIVE_PST_ROOT_FOLDER", "Online Archive (Recovered)")


class MailPstWriter(PstWriterBase):
    item_type = "EMAIL"
    cli_kind = "mail"
    # Block budget defaults from the base class (3 blocks/msg for PC +
    # Recipient TC + body subnode; attachment data adds blocks based on
    # per-item byte size). No item-count cap — the chunker bin-packs by
    # estimated block consumption against ``_MAX_BLOCKS_PER_CHUNK``.
    # Retained so callers / tests that introspect ``standard_folder_type``
    # (the Aspose-era field) keep working. pstwriter's CLI hard-codes the
    # folder name to "Inbox" for mail; this string is informational only.
    standard_folder_type = "Inbox"

    # Bytes the PC header + recipients + small Graph metadata occupy in the
    # heap-on-node before anything spills to a subnode. Tuned conservatively
    # — actual mail with no body still fits in one block. Used only when we
    # have a Graph dict to inspect; the base-class fallback (content_size
    # of the raw Graph JSON) over-counts because it includes envelope JSON
    # bytes that land inline in the PC and consume zero subnode blocks.
    _PC_ENVELOPE_BYTES = 4 * 1024

    def _estimate_item_size(self, item, graph_obj) -> int:
        """Bytes the pstwriter will materialise per mail.

        Only body text/HTML + attachment payloads actually drive subnode
        block consumption — headers, recipients, ConversationIndex,
        importance flags etc. all live inline in the PC. Counting the
        whole Graph JSON envelope (the base-class default) over-estimates
        by a factor of 5-10× for HTML-heavy mail and produces unnecessary
        chunk splits (e.g. a 39-mail Deleted Items folder fanning out to
        5 PSTs when 1-2 would suffice).
        """
        if not isinstance(graph_obj, dict):
            # No Graph payload to inspect — fall back to the base estimate
            # so unusual code paths (raw bytes only) still get a sensible
            # number.
            return super()._estimate_item_size(item, graph_obj)

        body = graph_obj.get("body") or {}
        body_content = body.get("content") if isinstance(body, dict) else None
        body_bytes = len(body_content.encode("utf-8")) \
            if isinstance(body_content, str) else 0

        att_bytes = 0
        for a in graph_obj.get("attachments") or []:
            if isinstance(a, dict):
                try:
                    att_bytes += int(a.get("size") or 0)
                except (TypeError, ValueError):
                    pass

        return self._PC_ENVELOPE_BYTES + body_bytes + att_bytes

    async def _nested_rfc822_att_dict(self, att, ed, blob_path, source_container):
        """Rebuild an ``itemAttachment``'s nested email into a
        ``message/rfc822`` fileAttachment dict (pst_convert reads
        ``contentBytes``). Bytes come from ``blob_path`` (downloaded via the
        same machinery as file attachments) or ``extra_data.inline_b64`` when
        the backup inlined a small nested message. Returns ``None`` on any
        failure so one bad nested email never fails the whole message."""
        import json
        from shared.mime_builder import build_nested_message_attachment

        _name = getattr(att, "name", "?")
        try:
            raw = None
            inline_b64 = ed.get("inline_b64")
            if blob_path:
                from shared.azure_storage import azure_storage_manager
                from mail_fetch import _normalize_containers, _download_first_hit
                att_shard = azure_storage_manager.get_shard_for_item(att)
                item_extra = list(
                    getattr(att, "_source_container_candidates", None) or ()
                )
                legacy_fb = getattr(att, "_mailbox_fallback_container", None)
                if legacy_fb:
                    item_extra.append(legacy_fb)
                containers = _normalize_containers(source_container, item_extra)
                raw, _hit = await _download_first_hit(att_shard, containers, blob_path)
            elif inline_b64:
                raw = base64.b64decode(inline_b64)
            if not raw:
                print(f"[mail.collect] DROP itemattachment no_bytes att={_name}", flush=True)
                return None
            if len(raw) > MAX_ATTACHMENT_BYTES:
                print(
                    f"[mail.collect] DROP itemattachment oversize att={_name} "
                    f"bytes={len(raw)} cap={MAX_ATTACHMENT_BYTES}",
                    flush=True,
                )
                return None
            nested = json.loads(raw.decode("utf-8"))
            ref = build_nested_message_attachment(nested)
            return {
                "@odata.type": "#microsoft.graph.fileAttachment",
                "name": ref.name,
                "contentType": "message/rfc822",
                "contentBytes": base64.b64encode(ref.data_bytes).decode("ascii"),
                "size": len(ref.data_bytes),
                "isInline": False,
            }
        except Exception as exc:
            print(
                f"[mail.collect] itemAttachment rebuild failed att={_name}: "
                f"{type(exc).__name__}: {exc}",
                flush=True,
            )
            return None

    async def _collect_graph_item(self, item, shard, source_container: str):
        """Return the Graph message JSON for this item with attachment
        bytes inlined as base64. ``None`` when the body cannot be located."""
        from mail_fetch import fetch_message

        msg_json = await fetch_message(item, shard, source_container)
        if msg_json is None:
            logger.warning(
                "mail_fetch returned None for item %s",
                getattr(item, "external_id", "?"),
            )
            return None

        # Defensive copy — fetch_message returns the SnapshotItem's own
        # extra_data dict in the inline path, and we don't want to mutate
        # caller-visible state by appending attachments to it.
        msg_json = dict(msg_json)

        # Stamp the SnapshotItem's folder_path onto the message JSON so
        # pst_convert can group messages by source folder ("/Inbox",
        # "/Sent Items/Project X", etc.) and rebuild the folder
        # hierarchy in the PST. Without this every message lands in a
        # single hard-coded "Inbox" and Outlook's import wizard sees
        # only one folder. We also fall back to the raw Graph
        # parentFolderName so older snapshots without folder_path stay
        # functional. The pstwriter side reads ``_folderPath`` (under
        # this leading-underscore name to make it obvious the value was
        # injected by the exporter rather than coming from Graph itself).
        item_folder_path = getattr(item, "folder_path", None) or ""
        if not item_folder_path:
            item_folder_path = (
                msg_json.get("parentFolderName")
                or msg_json.get("_full_folder_path")
                or ""
            )
        # Online Archive items: nest under a NON-reserved root. The source
        # archive's own folders are often reserved names (e.g. "Archive",
        # "Deleted Items") — importing a PST with a top-level "Archive" folder
        # merges it into the mailbox's special Archive / online-archive folder,
        # so the message vanishes from view even though the PST is valid. A
        # neutral root keeps the archive's folder hierarchy intact while landing
        # it in a plainly visible new folder on import.
        if getattr(item, "item_type", None) == "ARCHIVE_ITEM":
            item_folder_path = f"{ARCHIVE_PST_ROOT}/{item_folder_path}".rstrip("/")
        if item_folder_path:
            msg_json["_folderPath"] = str(item_folder_path)

        # Sibling EMAIL_ATTACHMENT items are pre-fetched by the orchestrator
        # and stamped on each EMAIL item as ``_email_attachment_items``. They
        # carry ``blob_path`` (bytes location), ``name``, and an extra_data
        # dict with ``attachment_kind``, ``content_type``, ``is_inline``,
        # ``content_id``. We download the bytes lazily here so a chunk's
        # peak memory is one attachment payload, not the whole mailbox.
        email_atts = getattr(item, "_email_attachment_items", None) or []
        print(
            f"[mail.collect] ext_id={getattr(item, 'external_id', '?')[:40]}... "
            f"email_atts={len(email_atts)}",
            flush=True,
        )
        if email_atts:
            inline_atts: list[dict] = []
            for att in email_atts:
                blob_path = getattr(att, "blob_path", None)
                _att_name = getattr(att, "name", "?")
                ed = (getattr(att, "extra_data", None) or {})
                kind = (ed.get("attachment_kind") or "").lower()
                # Mail-in-mail: a nested email (itemAttachment) is rebuilt into
                # a message/rfc822 fileAttachment so it embeds natively in the
                # PST. Bytes come from blob_path or, for small inlined nested
                # messages, extra_data.inline_b64 (blob_path is None then, so
                # this must run before the no_blob_path drop below).
                if "itemattachment" in kind:
                    item_att = await self._nested_rfc822_att_dict(
                        att, ed, blob_path, source_container,
                    )
                    if item_att is not None:
                        inline_atts.append(item_att)
                    continue
                if not blob_path:
                    print(f"[mail.collect] DROP no_blob_path att={_att_name}", flush=True)
                    continue
                # referenceAttachment can't roundtrip via contentBytes —
                # pstwriter's mail CLI only consumes fileAttachment. The
                # live-restore path handles it via Graph POST; for PST export
                # it stays skipped (matches the legacy MBOX/EML behaviour).
                if kind and "fileattachment" not in kind:
                    print(f"[mail.collect] DROP non_file_kind={kind} att={_att_name}", flush=True)
                    continue
                # Route by the attachment's own backend_id rather than the
                # orchestrator-passed `shard`. The orchestrator passes the
                # currently-active default shard, but a snapshot taken on
                # SeaweedFS (then later toggled to Azure, or vice versa)
                # still has its bytes on the original backend; the
                # SnapshotItem.backend_id column is the source of truth.
                # azure_storage_manager.get_shard_for_item resolves to the
                # correct backend's shard via shared.storage.router.
                from shared.azure_storage import azure_storage_manager
                from mail_fetch import _normalize_containers, _download_first_hit
                att_shard = azure_storage_manager.get_shard_for_item(att)
                # Build candidate list: orchestrator's source_container
                # (which may itself be a sequence) + any per-item override
                # the dedup-relocation pass stamped on the row. Walking the
                # list is required because cross-snapshot dedup can hand us
                # a blob_path anchored in a sibling resource's workload
                # container (e.g. MAILBOX-era "backup-mailbox-<tenant>")
                # while the active container is "backup-email-<tenant>".
                item_extra = list(getattr(att, "_source_container_candidates", None) or ())
                legacy_fb = getattr(att, "_mailbox_fallback_container", None)
                if legacy_fb:
                    item_extra.append(legacy_fb)
                container_candidates = _normalize_containers(source_container, item_extra)
                print(
                    f"[mail.collect] att={_att_name} "
                    f"backend_id={getattr(att,'backend_id','?')} "
                    f"shard={type(att_shard).__name__} "
                    f"containers={list(container_candidates)} "
                    f"path_len={len(blob_path)}",
                    flush=True,
                )
                try:
                    data, hit_container = await _download_first_hit(
                        att_shard, container_candidates, blob_path,
                    )
                except Exception as exc:
                    print(f"[mail.collect] DOWNLOAD_EXC att={_att_name}: {type(exc).__name__}: {exc}", flush=True)
                    continue
                print(
                    f"[mail.collect] att={_att_name} "
                    f"download_returned={'None' if data is None else f'{len(data)} bytes from {hit_container}'}",
                    flush=True,
                )
                if not data:
                    continue
                if len(data) > MAX_ATTACHMENT_BYTES:
                    print(f"[mail.collect] DROP oversize att={_att_name} bytes={len(data)} cap={MAX_ATTACHMENT_BYTES}", flush=True)
                    continue
                att_dict = {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": getattr(att, "name", None) or "attachment",
                    "contentType": ed.get("content_type")
                                   or "application/octet-stream",
                    "contentBytes": base64.b64encode(data).decode("ascii"),
                    "size": len(data),
                    "isInline": bool(ed.get("is_inline")),
                }
                # Preserve original Content-ID so inline images in the
                # restored HTML body still resolve via cid:xxx references.
                cid = ed.get("content_id") or ed.get("contentId")
                if cid:
                    att_dict["contentId"] = str(cid).strip("<>")
                inline_atts.append(att_dict)

            print(
                f"[mail.collect] inline_atts_built={len(inline_atts)} "
                f"for ext_id={getattr(item, 'external_id', '?')[:40]}...",
                flush=True,
            )
            if inline_atts:
                # Replace any metadata-only attachments array on the source
                # JSON with our resolved-bytes list. Graph delivers
                # attachments via a separate /attachments endpoint, so the
                # message body usually doesn't carry them at all; but if it
                # does, those entries lack contentBytes and can't be written.
                msg_json["attachments"] = inline_atts
                msg_json["hasAttachments"] = True

        return msg_json
