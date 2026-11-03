"""Mail export (MBOX + EML) — streaming, blob-to-blob pipeline.

See docs/superpowers/specs/2026-04-19-mbox-mail-export-design.md for design.
Entry point is MailExportOrchestrator.run(); FolderExportTask implements the
per-folder streaming writer.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, List, Optional

# Import shared utilities. The restore-worker runtime mounts /app/shared, so
# `from shared.X import ...` works in prod. Tests import via shim that preloads
# the module; we also allow repo-root imports when PYTHONPATH=.
_HERE = os.path.dirname(__file__)
_SHARED = os.path.abspath(os.path.join(_HERE, "..", "..", "shared"))
if os.path.isdir(_SHARED) and _SHARED not in sys.path:
    sys.path.insert(0, os.path.dirname(_SHARED))  # parent so `import shared.X` works

from shared.mime_builder import AttachmentRef, build_eml, build_eml_streaming  # noqa: E402
from shared.mbox_writer import MboxWriter  # noqa: E402
from shared.file_path_sanitize import sanitize_arcname  # noqa: E402

# Reserved chars forbidden in Windows path components, reused for arcname confinement.
_ARCNAME_RESERVED = '<>:"/\\|?*'


def _confine_arcname(raw: str) -> str:
    """Confine an attacker-influenced ZIP arcname: drop ``..``/``.``/empty
    segments and reserved chars so no traversal or absolute path survives."""
    return sanitize_arcname(raw, max_len=4096, replace_chars=_ARCNAME_RESERVED)


@dataclass
class FolderExportResult:
    folder_name: str
    # MBOX path: per-folder MBOX blobs already uploaded to the destination
    # container. ZIP assembly pulls these via server-side copy / download.
    produced_blobs: List[str] = field(default_factory=list)
    # EML path: inline (arcname, bytes) pairs. For scale, EML format skips the
    # intermediate per-message blob upload and feeds bytes straight into the
    # ZIP streamer. Memory is the currently-being-zipped member only.
    produced_inline_members: List[tuple] = field(default_factory=list)
    exported_count: int = 0
    failed_items: List[dict] = field(default_factory=list)


class FolderExportTask:
    """Streams one folder's worth of messages into per-folder MBOX blobs (or
    loose EML blobs) on the given Azure shard. Parallel fetch of message JSON
    and attachment bytes; serial writer path so mbox chunks stay ordered."""

    def __init__(
        self,
        *,
        folder_name: str,
        items: List[Any],
        shard,
        source_container: str,
        dest_container: str,
        dest_blob_prefix: str,
        split_bytes: int,
        block_size: int,
        fetch_batch_size: int,
        queue_maxsize: int,
        format: str,
        include_attachments: bool,
        manifest,
        mbox_inline_limit_bytes: int = 100 * 1024 * 1024,
    ):
        self.folder_name = folder_name
        self.items = items
        self.shard = shard
        self.source_container = source_container
        self.dest_container = dest_container
        self.dest_blob_prefix = dest_blob_prefix
        self.split_bytes = split_bytes
        self.block_size = block_size
        self.fetch_batch_size = fetch_batch_size
        self.queue_maxsize = queue_maxsize
        self.format = format.upper()
        self.include_attachments = include_attachments
        self.manifest = manifest
        # MBOX tiering — folders under this byte size accumulate in memory and
        # go inline into the final ZIP; larger folders fall back to staging an
        # intermediate folder-MBOX blob so memory stays bounded.
        self.mbox_inline_limit_bytes = mbox_inline_limit_bytes

    async def run(self) -> FolderExportResult:
        result = FolderExportResult(folder_name=self.folder_name)
        if self.format == "MBOX":
            await self._run_mbox(result)
        elif self.format == "EML":
            await self._run_eml(result)
        else:
            raise ValueError(f"Unknown format: {self.format}")
        return result

    async def _fetch_message(self, item) -> Optional[dict]:
        from mail_fetch import fetch_message
        return await fetch_message(item, self.shard, self.source_container)

    async def _gather_attachments(self, att_paths) -> List[AttachmentRef]:
        from mail_fetch import gather_attachments
        return await gather_attachments(att_paths, self.shard, self.source_container, self.include_attachments)

    async def _gather_item_attachments(self, item) -> List[AttachmentRef]:
        """message/rfc822 refs for any itemAttachment (mail-in-mail) siblings
        stamped on the EMAIL item. Additive — fileAttachment / inline-image
        handling above is untouched."""
        from mail_fetch import gather_item_attachments
        rows = getattr(item, "_email_attachment_items", None) or []
        if not rows:
            return []
        return await gather_item_attachments(
            rows, self.shard, self.source_container, self.include_attachments
        )

    async def _build_eml_for_item(self, item):
        print(f"[FolderExportTask/{self.folder_name}] _build_eml_for_item START ext_id={item.external_id}", flush=True)
        msg_json = await self._fetch_message(item)
        if msg_json is None:
            raise FileNotFoundError(f"blob missing: {item.blob_path}")
        attachments = await self._gather_attachments(
            getattr(item, "attachment_blob_paths", []) or []
        )
        # Mail-in-mail: nested emails (itemAttachment) embed as message/rfc822
        # parts. Separate channel from file attachments so the latter is
        # unchanged; refs simply append to the same attachment list.
        item_atts = await self._gather_item_attachments(item)
        if item_atts:
            attachments = list(attachments) + item_atts
        print(f"[FolderExportTask/{self.folder_name}] building EML ext_id={item.external_id} atts={len(attachments)}", flush=True)
        if attachments:
            pieces = []
            async for chunk in build_eml_streaming(msg_json, attachments):
                pieces.append(chunk)
            eml_bytes = b"".join(pieces)
        else:
            eml_bytes = build_eml(msg_json, attachments=[])
        print(f"[FolderExportTask/{self.folder_name}] EML built size={len(eml_bytes)} ext_id={item.external_id}", flush=True)
        return msg_json, eml_bytes

    async def _run_mbox(self, result: FolderExportResult):
        """Build a folder's MBOX bytes with a two-tier storage strategy:

        Phase A — inline accumulation (default).
            All bytes go into an in-memory bytearray. No Azure I/O. At folder
            close, emits as an inline ZIP member. Avoids the double I/O cost
            (write intermediate blob + read during ZIP assembly).

        Phase B — blob spillover (only if folder exceeds mbox_inline_limit_bytes).
            When the in-memory buffer crosses the threshold, flushes everything
            accumulated so far to an Azure intermediate blob via stage_block,
            and every subsequent message streams to the same blob. At close,
            commits the blob and emits as a blob-path ZIP member. Memory stays
            bounded regardless of folder size.

        Size-splits (split_bytes) still fire inside the MboxWriter; each part
        is evaluated independently under the same inline/blob rule.
        """
        print(f"[FolderExportTask/{self.folder_name}] _run_mbox START items={len(self.items)} prefix={self.dest_blob_prefix} inline_limit={self.mbox_inline_limit_bytes}", flush=True)

        part_index = 1
        current_path = f"{self.dest_blob_prefix}.01.mbox"

        # Per-part state. Reset at each size-split rollover.
        state = {
            "buf": bytearray(),          # accumulating bytes (Phase A and pre-flush Phase B)
            "mode": "inline",            # "inline" or "blob"
            "block_ids": [],             # blob mode: staged block ids so far
            "total_bytes": 0,            # running count for this part (both modes)
        }

        async def _spillover_to_blob():
            """Transition current part from inline → blob: stage all buffered
            bytes to Azure, switch mode."""
            print(f"[FolderExportTask/{self.folder_name}] part={part_index} spill to blob at {state['total_bytes']} bytes", flush=True)
            # Drain buf in block-sized chunks.
            while state["buf"]:
                take = min(self.block_size, len(state["buf"]))
                chunk = bytes(state["buf"][:take])
                del state["buf"][:take]
                bid = f"blk-{len(state['block_ids']):05d}"
                await self.shard.stage_block(self.dest_container, current_path, bid, chunk)
                state["block_ids"].append(bid)
            state["mode"] = "blob"

        async def flush_block(final: bool = False):
            """In blob mode: drain buf into stage_block calls."""
            if state["mode"] != "blob":
                return
            buf = state["buf"]
            while len(buf) >= self.block_size or (final and buf):
                take = min(self.block_size, len(buf))
                chunk = bytes(buf[:take])
                del buf[:take]
                bid = f"blk-{len(state['block_ids']):05d}"
                await self.shard.stage_block(self.dest_container, current_path, bid, chunk)
                state["block_ids"].append(bid)

        def emit(data: bytes):
            state["buf"].extend(data)
            state["total_bytes"] += len(data)

        def _on_rollover(idx: int):
            # MboxWriter fires this AFTER append_message crosses split_bytes.
            # The folder-level loop below notices part_index changed and
            # finalizes current before continuing.
            nonlocal part_index
            part_index = idx + 1

        writer = MboxWriter(emit=emit, split_bytes=self.split_bytes, on_rollover=_on_rollover)

        async def finalize_current():
            """Close out the current part: emit as inline or commit blob."""
            nonlocal current_path
            if state["mode"] == "inline":
                if state["buf"]:
                    arc = f"{self.folder_name}.{part_index:02d}.mbox" if part_index > 1 else f"{self.folder_name}.mbox"
                    result.produced_inline_members.append((arc, bytes(state["buf"])))
                    print(f"[FolderExportTask/{self.folder_name}] part={part_index} finalized INLINE size={state['total_bytes']}", flush=True)
            else:  # blob mode
                await flush_block(final=True)
                if state["block_ids"]:
                    await self.shard.commit_block_list_manual(
                        self.dest_container, current_path, state["block_ids"],
                        metadata={"folder": _sanitize_metadata(self.folder_name), "part": str(part_index)},
                    )
                    result.produced_blobs.append(current_path)
                    print(f"[FolderExportTask/{self.folder_name}] part={part_index} finalized BLOB path={current_path} size={state['total_bytes']}", flush=True)
            # Reset for next part.
            state["buf"] = bytearray()
            state["mode"] = "inline"
            state["block_ids"] = []
            state["total_bytes"] = 0

        idx = 0
        while idx < len(self.items):
            batch = self.items[idx : idx + self.fetch_batch_size]
            idx += len(batch)

            async def _prep(it):
                try:
                    msg_json, eml_bytes = await self._build_eml_for_item(it)
                    return it, (msg_json, eml_bytes), None
                except Exception as exc:
                    return it, None, f"{type(exc).__name__}: {exc}"

            prepared = await asyncio.gather(*(_prep(i) for i in batch))

            for it, data, err in prepared:
                if err:
                    print(f"[FolderExportTask/{self.folder_name}] ITEM FAILED ext_id={it.external_id} error={err}", flush=True)
                    result.failed_items.append({"id": it.external_id, "name": getattr(it, "name", ""), "error": err})
                    if self.manifest:
                        self.manifest.record_failure(
                            item_id=it.external_id, name=getattr(it, "name", ""),
                            folder=self.folder_name, error=err,
                        )
                    continue
                msg_json, eml_bytes = data
                sender = ((msg_json.get("from") or {}).get("emailAddress") or {}).get("address", "unknown@unknown")
                sent_iso = msg_json.get("sentDateTime") or msg_json.get("receivedDateTime")
                try:
                    from email.utils import parsedate_to_datetime
                    sent_epoch = int(parsedate_to_datetime(sent_iso).timestamp()) if sent_iso else int(time.time())
                except Exception:
                    sent_epoch = int(time.time())

                prior_part = part_index
                writer.append_message(eml_bytes, sender_addr=sender, sent_at_epoch=sent_epoch)
                result.exported_count += 1
                if self.manifest:
                    self.manifest.record_success(
                        item_id=it.external_id, name=getattr(it, "name", ""),
                        folder=self.folder_name, size_bytes=len(eml_bytes),
                    )

                if part_index != prior_part:
                    await finalize_current()
                    current_path = f"{self.dest_blob_prefix}.{part_index:02d}.mbox"

                # Tier transition: inline → blob when folder grows past limit.
                if state["mode"] == "inline" and state["total_bytes"] > self.mbox_inline_limit_bytes:
                    await _spillover_to_blob()

                # In blob mode, flush whenever buf exceeds block_size.
                if state["mode"] == "blob" and len(state["buf"]) >= self.block_size:
                    await flush_block()

        writer.close()
        await finalize_current()
        print(f"[FolderExportTask/{self.folder_name}] _run_mbox DONE produced_blobs={len(result.produced_blobs)} produced_inline={len(result.produced_inline_members)} failed={len(result.failed_items)}", flush=True)

    async def _run_eml(self, result: FolderExportResult):
        """Build EMLs in-memory and stage them for ZIP assembly. No per-message
        Azure upload — that was an I/O bottleneck that scaled linearly with
        mailbox size and blew past the 60s frontend-poll budget for full
        mailboxes.

        Messages are fetched + EMLs built in parallel batches so a thousand-
        message folder completes in ~seconds instead of ~minutes. Inline
        (arcname, bytes) pairs land on result.produced_inline_members; the
        orchestrator feeds them directly into the streaming ZIP writer.
        """
        print(f"[FolderExportTask/{self.folder_name}] _run_eml START items={len(self.items)} prefix={self.dest_blob_prefix}", flush=True)

        idx = 0
        while idx < len(self.items):
            batch = self.items[idx : idx + self.fetch_batch_size]
            idx += len(batch)

            async def _prep(it):
                try:
                    _, eml_bytes = await self._build_eml_for_item(it)
                    return it, eml_bytes, None
                except Exception as exc:
                    return it, None, f"{type(exc).__name__}: {exc}"

            prepared = await asyncio.gather(*(_prep(i) for i in batch))

            for it, eml_bytes, err in prepared:
                if err:
                    print(f"[FolderExportTask/{self.folder_name}] ITEM FAILED (EML path) ext_id={it.external_id} error={err}", flush=True)
                    result.failed_items.append({
                        "id": it.external_id, "name": getattr(it, "name", ""),
                        "error": err,
                    })
                    if self.manifest:
                        self.manifest.record_failure(
                            item_id=it.external_id, name=getattr(it, "name", ""),
                            folder=self.folder_name, error=err,
                        )
                    continue
                safe_name = "".join(c if c.isalnum() or c in "-._" else "_" for c in (it.name or it.external_id))
                arcname = _confine_arcname(f"{self.folder_name}/{safe_name}-{it.external_id}.eml")
                assert arcname and ".." not in arcname.split("/") and not arcname.startswith("/"), (
                    f"unsafe arcname: {arcname!r}"
                )
                result.produced_inline_members.append((arcname, eml_bytes))
                result.exported_count += 1
                if self.manifest:
                    self.manifest.record_success(
                        item_id=it.external_id, name=getattr(it, "name", ""),
                        folder=self.folder_name, size_bytes=len(eml_bytes),
                    )

        print(f"[FolderExportTask/{self.folder_name}] _run_eml DONE produced_inline={len(result.produced_inline_members)} failed={len(result.failed_items)}", flush=True)


def _sanitize_metadata(val: str) -> str:
    """Azure blob metadata values must be ASCII — folder names can be UTF-8."""
    return val.encode("ascii", errors="replace").decode("ascii")


try:
    from shared.export_manifest import ExportManifestBuilder  # type: ignore
except ImportError:  # pragma: no cover
    ExportManifestBuilder = None  # type: ignore


async def stream_zip_to_block_blob(
    *,
    dest_shard,
    dest_container: str,
    dest_blob_path: str,
    members,
    member_source_shard,
    manifest_bytes: bytes,
    block_size: int,
) -> None:
    """Build a ZIP64 archive chunk-by-chunk and push it to object storage.

    Two sinks, auto-selected by destination-shard capability:

    * **Azure Blob (native stage_block path)** — memory stays at ~block_size
      regardless of archive size. Each `block_size`-aligned slice of the ZIP
      stream is staged as a block via ``dest_shard.stage_block`` and
      committed in one go at the end via ``commit_block_list_manual``.
      The original mail-v2 behaviour; unchanged on Azure deployments.

    * **SeaweedFS / S3-compat fallback** — Seaweed deliberately refuses
      the Azure stage_block shape (``NotImplementedError``) and steers
      callers to S3 multipart. Rather than drag a second streaming
      assembler in, we buffer the ZIP to a bounded-size ``bytearray``
      and single-PUT via ``upload_blob`` at the end. Memory ceiling =
      archive size (SharePoint list / small-file exports are usually
      MB-scale, not GB). For very large on-prem exports a future
      upgrade swaps this branch for ``upload_stream`` with a live
      byte-iterator so the ceiling drops back to block_size; keeping
      it simple for now unblocks the file-family + metadata-mix
      SharePoint/Teams/Group download paths on Seaweed.

    `members` is a list of tuples in one of three shapes:
      (arcname, container, blob_path)  — pull bytes from member_source_shard
      (arcname, bytes_or_bytearray)    — inline bytes (EML / JSON members)
      (arcname, "inline", bytes)       — tagged inline form
    """
    import zipstream  # package: zipstream-ng

    # Probe: pick the sink by destination-shard "kind". Only azure_blob
    # supports the stage_block + commit_block_list_manual flow. Every
    # non-Azure backend (SeaweedFS, any future S3-compat) has a
    # stage_block method on the wrapped store but raises
    # NotImplementedError from it, so we must NOT call it and instead
    # take the buffered-single-PUT branch.
    kind = (getattr(dest_shard, "kind", None)
            or getattr(getattr(dest_shard, "_store", None), "kind", None)
            or "unknown")
    azure_mode = kind == "azure_blob"

    print(
        f"[stream_zip] START dest={dest_blob_path} members={len(members)} "
        f"block_size={block_size} mode={'azure-stage_block' if azure_mode else 'buffered-single-put'}",
        flush=True,
    )
    zs = zipstream.ZipStream(compress_type=zipstream.ZIP_STORED)

    block_ids: list = []
    staged = bytearray()
    block_index = 0

    async def _flush_azure(final: bool = False):
        nonlocal block_index
        while len(staged) >= block_size or (final and staged):
            take = min(block_size, len(staged))
            chunk = bytes(staged[:take])
            del staged[:take]
            bid = f"blk-{block_index:05d}"
            await dest_shard.stage_block(dest_container, dest_blob_path, bid, chunk)
            block_ids.append(bid)
            block_index += 1

    # For the buffered sink we just append to `staged` and flush once at end.
    async def _flush_buffered(final: bool = False):
        # no-op during the member loop; content accumulates in `staged`.
        return None

    _flush_to_block = _flush_azure if azure_mode else _flush_buffered

    print(f"[stream_zip] adding {len(members)} members to zipstream", flush=True)
    for m in members:
        if len(m) == 2:
            arcname, content = m
            chunks = [bytes(content)]
        elif len(m) == 3 and m[1] == "inline":
            arcname, _tag, content = m
            chunks = [bytes(content)]
        else:
            arcname, container, path = m
            chunks = []
            async for chunk in member_source_shard.download_blob_stream(
                container, path, chunk_size=block_size
            ):
                chunks.append(chunk)
        arcname = _confine_arcname(arcname)
        assert arcname and ".." not in arcname.split("/") and not arcname.startswith("/"), (
            f"unsafe arcname: {arcname!r}"
        )
        zs.add(iter(chunks), arcname=arcname)
        del chunks

        for zchunk in zs.file():
            staged.extend(zchunk)
            await _flush_to_block()

    zs.add(iter([manifest_bytes]), arcname="_MANIFEST.json")

    # Finalize: emits remaining member(s) + central directory + EOCD.
    for final_chunk in zs.finalize():
        staged.extend(final_chunk)
        await _flush_to_block()

    if azure_mode:
        await _flush_azure(final=True)
        print(f"[stream_zip] committing {len(block_ids)} blocks to {dest_blob_path}", flush=True)
        await dest_shard.commit_block_list_manual(
            dest_container, dest_blob_path, block_ids,
            metadata={"streamed": "true"},
        )
    else:
        # Single-PUT the whole ZIP buffer. For large archives this will
        # delegate to S3 multipart on the Seaweed side automatically.
        print(
            f"[stream_zip] buffered single-PUT to {dest_blob_path} "
            f"bytes={len(staged)}",
            flush=True,
        )
        await dest_shard.upload_blob(
            dest_container, dest_blob_path, bytes(staged),
            metadata={"streamed": "true"},
        )
    print(f"[stream_zip] DONE dest={dest_blob_path}", flush=True)


class MailExportOrchestrator:
    """Entry point for the mail-export pipeline. Partitions items by folder,
    spawns one FolderExportTask per folder under asyncio.Semaphore(parallelism),
    assembles the final ZIP + manifest.

    When shard_manager is provided (Task 27+), items are grouped by
    (folder, shard_index) and each FolderExportTask runs on the shard that
    holds its source data. The final ZIP lands on the shard with the biggest
    byte contribution. The legacy shard= kwarg is preserved for single-shard
    callers.

    Final ZIP is built in memory for this task; Task 28 upgrades to streaming
    into stage_block calls via zipstream-ng."""

    def __init__(
        self,
        *,
        job_id: str,
        snapshot_ids: List[str],
        items: List[Any],
        shard=None,                     # legacy — single shard
        shard_manager=None,             # NEW — preferred, per-item resolution
        source_container: str,
        dest_container: str,
        parallelism: int,
        split_bytes: int,
        block_size: int,
        fetch_batch_size: int,
        queue_maxsize: int,
        format: str,
        include_attachments: bool,
        manifest: Optional["ExportManifestBuilder"] = None,
        checkpoint: Optional[dict] = None,
        persist_checkpoint=None,  # async callable(dict) — e.g. update Job.result in DB
        mbox_inline_limit_bytes: int = 100 * 1024 * 1024,
    ):
        if shard_manager is None and shard is None:
            raise ValueError("pass either shard_manager (preferred) or shard (legacy)")
        self.shard_manager = shard_manager
        self.shard = shard
        self.job_id = job_id
        self.snapshot_ids = snapshot_ids
        self.items = items
        self.source_container = source_container
        self.dest_container = dest_container
        self.parallelism = max(1, parallelism)
        self.split_bytes = split_bytes
        self.block_size = block_size
        self.fetch_batch_size = fetch_batch_size
        self.queue_maxsize = queue_maxsize
        self.format = format.upper()
        self.include_attachments = include_attachments
        self.mbox_inline_limit_bytes = mbox_inline_limit_bytes
        self.manifest = manifest or (
            ExportManifestBuilder(job_id=job_id, snapshot_ids=snapshot_ids)
            if ExportManifestBuilder
            else None
        )
        self.checkpoint = dict(checkpoint or {"completed_folders": [], "produced_blobs": {}})
        self.persist_checkpoint = persist_checkpoint

    async def _safe_persist_checkpoint(self, cp_dict: dict, folder_name: str) -> None:
        """Best-effort background checkpoint write. Errors logged, never raised."""
        try:
            await self.persist_checkpoint(cp_dict)
            print(f"[MailExportOrchestrator] checkpoint persisted folder={folder_name}", flush=True)
        except Exception as exc:
            print(f"[MailExportOrchestrator] checkpoint persist failed folder={folder_name} (non-fatal): {type(exc).__name__}: {exc}", flush=True)

    def preflight(self) -> List[str]:
        """M5 — warn if total export size is pathologically large."""
        warnings: List[str] = []
        total = sum(getattr(it, "content_size", 0) or 0 for it in self.items)
        threshold = 100 * 1024 * 1024 * 1024  # 100 GB
        if self.include_attachments and total > threshold:
            warnings.append(
                f"large export: {total // (1024 ** 3)} GB with attachments on "
                f"({len(self.items)} items) — may take 15+ min and peak memory ~800 MB"
            )
        return warnings

    async def run(self) -> dict:
        print(f"[MailExportOrchestrator] ENTER job={self.job_id} items={len(self.items)} fmt={self.format}", flush=True)
        # M5 preflight
        for w in self.preflight():
            print(f"[MailExportOrchestrator] {w}", flush=True)

        # M6 memory monitor — soft-kill before Docker OOM mid-commit.
        from shared.memory_monitor import MemoryMonitor
        from shared.config import settings as _s

        breached = asyncio.Event()

        async def _on_breach():
            breached.set()

        # 4 GB Docker limit from compose M4. soft_limit_pct and grace from config.
        monitor = MemoryMonitor(
            limit_bytes=4 * 1024 * 1024 * 1024,
            soft_limit_pct=_s.EXPORT_MEMORY_SOFT_LIMIT_PCT,
            grace_seconds=_s.EXPORT_MEMORY_KILL_GRACE_SECONDS,
            poll_interval_seconds=5.0,
            on_breach=_on_breach,
        )
        await monitor.start()
        print(f"[MailExportOrchestrator] monitor started job={self.job_id}", flush=True)

        try:
            if self.shard_manager:
                groups = self._partition_by_folder_and_shard()
            else:
                groups = {(fn, 0): items for fn, items in self._partition_by_folder().items()}
            print(f"[MailExportOrchestrator] partitioned into {len(groups)} (folder,shard) groups: {list(groups.keys())}", flush=True)

            completed = set(self.checkpoint.get("completed_folders", []))
            sem = asyncio.Semaphore(self.parallelism)

            async def _run_and_persist(folder_name, shard_index, folder_items):
                print(f"[MailExportOrchestrator] _run_and_persist START folder={folder_name} shard={shard_index} items={len(folder_items)}", flush=True)
                shard = self.shard_manager.get_shard_by_index(shard_index) if self.shard_manager else self.shard
                async with sem:
                    print(f"[MailExportOrchestrator] folder sem acquired folder={folder_name}", flush=True)
                    task = FolderExportTask(
                        folder_name=folder_name,
                        items=folder_items,
                        shard=shard,
                        source_container=self.source_container,
                        dest_container=self.dest_container,
                        dest_blob_prefix=(
                            f"{self.job_id}/shard-{shard_index}/{folder_name}"
                            if self.shard_manager else f"{self.job_id}/{folder_name}"
                        ),
                        split_bytes=self.split_bytes,
                        block_size=self.block_size,
                        fetch_batch_size=self.fetch_batch_size,
                        queue_maxsize=self.queue_maxsize,
                        format=self.format,
                        include_attachments=self.include_attachments,
                        mbox_inline_limit_bytes=self.mbox_inline_limit_bytes,
                        manifest=self.manifest,
                    )
                    result = await task.run()
                    print(f"[MailExportOrchestrator] folder={folder_name} produced={len(result.produced_blobs)} failures={len(result.failed_items)}", flush=True)

                cp_folders = list(self.checkpoint.get("completed_folders", []))
                cp_blobs = dict(self.checkpoint.get("produced_blobs", {}))
                if folder_name not in cp_folders:
                    cp_folders.append(folder_name)
                cp_blobs[folder_name] = list(result.produced_blobs)
                self.checkpoint["completed_folders"] = cp_folders
                self.checkpoint["produced_blobs"] = cp_blobs
                # Mid-run checkpoint persist disabled — it opened a second DB
                # session that raced with the main session's final commit and
                # clobbered result.blob_path. Checkpoint is carried in the
                # final returned dict (see run() below) and persisted as part
                # of the main job update, so resumability still works across
                # redeliveries.
                print(f"[MailExportOrchestrator] _run_and_persist RETURN folder={folder_name}", flush=True)
                return shard_index, shard, result

            remaining_groups = {
                k: v for k, v in groups.items() if k[0] not in completed
            }
            print(f"[MailExportOrchestrator] gathering {len(remaining_groups)} folder tasks", flush=True)
            per_group = await asyncio.gather(
                *(_run_and_persist(fn, si, items)
                  for (fn, si), items in remaining_groups.items())
            )
            print(f"[MailExportOrchestrator] all folder tasks done ({len(per_group)} results)", flush=True)

            # For legacy single-shard path: also recover prior checkpoint blobs
            # into folder_results so ZIP assembly includes them.
            if not self.shard_manager:
                done_folder_names = {fr.folder_name for (_, _, fr) in per_group}
                extra = []
                for prior_folder, prior_blobs in self.checkpoint.get("produced_blobs", {}).items():
                    if prior_folder not in done_folder_names:
                        rec = FolderExportResult(folder_name=prior_folder)
                        rec.produced_blobs = list(prior_blobs)
                        extra.append((0, self.shard, rec))
                per_group = list(per_group) + extra

            # Pick destination shard: the one with the largest contribution.
            bytes_per_shard: dict = {}
            for shard_index, shard, fr in per_group:
                total = 0
                for p in fr.produced_blobs:
                    try:
                        props = await shard.get_blob_properties(self.dest_container, p)
                        total += (props or {}).get("size", 0) or 0
                    except Exception:
                        pass
                bytes_per_shard[shard_index] = bytes_per_shard.get(shard_index, 0) + total

            dest_shard_index = max(bytes_per_shard, key=bytes_per_shard.get) if bytes_per_shard else 0
            dest_shard = (
                self.shard_manager.get_shard_by_index(dest_shard_index)
                if self.shard_manager else self.shard
            )

            zip_blob_path = f"{self.job_id}/export_{int(time.time())}.zip"
            print(f"[MailExportOrchestrator] assembling ZIP dest_shard={dest_shard_index} path={zip_blob_path}", flush=True)
            await self._assemble_zip_multi_shard(
                dest_shard=dest_shard,
                zip_blob_path=zip_blob_path,
                per_group=per_group,
                dest_shard_index=dest_shard_index,
            )

            status = "COMPLETED"
            if breached.is_set():
                status = "COMPLETED_WITH_ERRORS"
                print("[MailExportOrchestrator] M6 breach — export finalized under pressure")

            return {
                "blob_path": zip_blob_path,
                "dest_shard_index": dest_shard_index,
                "exported_count": self.manifest.exported_count if self.manifest else sum(fr.exported_count for (_, _, fr) in per_group),
                "failed_count": self.manifest.failed_count if self.manifest else sum(len(fr.failed_items) for (_, _, fr) in per_group),
                "folder_count": len(groups),
                "status": status,
                "manifest": json.loads(self.manifest.to_json().decode("utf-8")) if self.manifest else None,
                "checkpoint": dict(self.checkpoint),
            }
        finally:
            await monitor.stop()

    def _partition_by_folder(self) -> dict:
        from collections import defaultdict

        out: dict = defaultdict(list)
        for it in self.items:
            folder = _confine_arcname((getattr(it, "folder_path", None) or "Inbox").strip("/")) or "Inbox"
            out[folder].append(it)
        return dict(out)

    def _partition_by_folder_and_shard(self) -> dict:
        """Group items by (folder_name, shard_index). Used when shard_manager
        is set so each FolderExportTask runs on the shard that owns its data."""
        from collections import defaultdict

        out: dict = defaultdict(list)
        for it in self.items:
            folder = _confine_arcname((getattr(it, "folder_path", None) or "Inbox").strip("/")) or "Inbox"
            shard_index = getattr(it, "shard_index", 0) if self.shard_manager else 0
            out[(folder, shard_index)].append(it)
        return dict(out)

    async def _assemble_zip_multi_shard(self, *, dest_shard, zip_blob_path, per_group, dest_shard_index):
        """Stream the final ZIP into block staging on dest_shard.
        Three member sources:
          - same-shard blob (MBOX folder files on same shard as dest) — direct
          - cross-shard blob (MBOX from another shard) — server-side-copy first
          - inline bytes (EML messages built in memory) — no Azure round-trip
        """
        inline_members = []           # (arcname, bytes) — EML path
        same_shard_members = []       # (arcname, container, blob_path) — same-shard MBOX
        cross_shard_tmp_copies = []   # (arcname, container, tmp_path) — cross-shard MBOX copied locally

        for shard_index, shard, fr in per_group:
            # EML inline members: no Azure round-trip needed. Arcname already
            # set by FolderExportTask._run_eml (e.g. "Inbox/subject-id.eml").
            for arc, eml_bytes in getattr(fr, "produced_inline_members", []) or []:
                inline_members.append((arc, "inline", eml_bytes))

            # MBOX per-folder blobs (legacy + MBOX format).
            for blob_path in fr.produced_blobs:
                arc = blob_path.split(f"{self.job_id}/shard-{shard_index}/", 1)[-1]
                if "/shard-" not in blob_path:
                    arc = blob_path.split(f"{self.job_id}/", 1)[-1]
                if shard_index == dest_shard_index:
                    same_shard_members.append((arc, self.dest_container, blob_path))
                else:
                    # Server-side S3-to-S3 copy (put_block_from_url) — the SAS is
                    # consumed by the backend, never by a browser, and the copy
                    # preserves any TMVE envelope byte-for-byte, so signing an
                    # encrypted blob here is safe (allow_encrypted=True).
                    try:
                        sas_src = await shard.get_blob_sas_url(self.dest_container, blob_path, allow_encrypted=True)
                    except Exception:
                        sas_src = await shard.get_blob_url(self.dest_container, blob_path, allow_encrypted=True)
                    tmp_path = f"{self.job_id}/_crosshard/shard-{shard_index}/{arc}"
                    try:
                        await dest_shard.put_block_from_url(
                            self.dest_container, tmp_path, "b01", sas_src
                        )
                        await dest_shard.commit_block_list_manual(
                            self.dest_container, tmp_path, ["b01"]
                        )
                        cross_shard_tmp_copies.append((arc, self.dest_container, tmp_path))
                    except Exception as exc:
                        print(f"[MailExportOrchestrator] cross-shard copy failed for {blob_path}: {exc}")

        all_members = inline_members + same_shard_members + cross_shard_tmp_copies
        manifest_bytes = self.manifest.to_json() if self.manifest else b"{}"

        print(f"[MailExportOrchestrator] ZIP member breakdown: inline={len(inline_members)} same_shard={len(same_shard_members)} cross_shard={len(cross_shard_tmp_copies)}", flush=True)

        await stream_zip_to_block_blob(
            dest_shard=dest_shard,
            dest_container=self.dest_container,
            dest_blob_path=zip_blob_path,
            members=all_members,
            member_source_shard=dest_shard,
            manifest_bytes=manifest_bytes,
            block_size=self.block_size,
        )

        # Cleanup intermediate per-folder MBOX blobs + cross-shard temp copies.
        # Inline members have no Azure footprint to clean up. Failures here
        # are non-fatal — the final ZIP already committed.
        for _, container, path in same_shard_members + cross_shard_tmp_copies:
            try:
                await dest_shard.delete_blob(container, path)
            except Exception:
                pass

    async def _run_one_folder(
        self,
        sem: asyncio.Semaphore,
        folder_name: str,
        folder_items: List[Any],
    ) -> FolderExportResult:
        async with sem:
            task = FolderExportTask(
                folder_name=folder_name,
                items=folder_items,
                shard=self.shard,
                source_container=self.source_container,
                dest_container=self.dest_container,
                dest_blob_prefix=f"{self.job_id}/{folder_name}",
                split_bytes=self.split_bytes,
                block_size=self.block_size,
                fetch_batch_size=self.fetch_batch_size,
                queue_maxsize=self.queue_maxsize,
                format=self.format,
                include_attachments=self.include_attachments,
                manifest=self.manifest,
            )
            return await task.run()

    async def _assemble_zip(
        self, zip_blob_path: str, folder_results: List[FolderExportResult]
    ):
        """Build the final ZIP. Task 28 replaces this with a streaming impl
        backed by zipstream-ng + stage_block."""
        import io as _io
        import zipfile as _zipfile

        zip_buf = _io.BytesIO()
        with _zipfile.ZipFile(zip_buf, "w", _zipfile.ZIP_STORED, allowZip64=True) as zf:
            for fr in folder_results:
                for blob_path in fr.produced_blobs:
                    content = await self.shard.download_blob(
                        self.dest_container, blob_path
                    )
                    arcname = _confine_arcname(blob_path.split(f"{self.job_id}/", 1)[-1])
                    assert arcname and ".." not in arcname.split("/") and not arcname.startswith("/"), (
                        f"unsafe arcname: {arcname!r}"
                    )
                    zf.writestr(arcname, content or b"")
            if self.manifest:
                zf.writestr("_MANIFEST.json", self.manifest.to_json())

        await self.shard.upload_blob(
            self.dest_container, zip_blob_path, zip_buf.getvalue()
        )
