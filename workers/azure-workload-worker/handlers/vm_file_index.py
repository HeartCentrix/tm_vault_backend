"""File-level index builder for Azure VM backups.

After the VM backup has copied every disk snapshot into our blob
storage we walk each disk's filesystem(s) — without the live VM
having to be running, and without downloading the full VHD — and
persist one row per file or directory into `vm_file_index`.

Design rationale
----------------
* pytsk3 (The Sleuth Kit binding) can open a disk image given a
  file-like object, so we wrap the Azure page blob in a seekable
  reader that turns Python `.read(offset, length)` calls into
  HTTP range-gets via `azure-storage-blob`. That avoids downloading
  the whole VHD (127+ GB is not unusual).
* The walker runs BFS from each filesystem root, logs every
  directory visit + a running count, and flushes index rows in
  batches of 500 so we don't hold a giant transaction open for
  the whole walk.
* File contents are NOT stored in the index — only the metadata
  needed to re-open the same VHD later and extract the bytes
  (fs_inode, fs_type, partition_offset, blob_path).
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterator, Optional

from shared.database import async_session_factory
from shared.models import VmFileIndex


logger = logging.getLogger("vm_file_index")


# ──────────────────────────────────────────────────────────────────────
# Streaming VHD reader — pytsk3 expects a subclass of Img_Info whose
# read() returns bytes for a given offset/length. We translate that
# into Azure blob range-gets so the full VHD never touches local disk.
# ──────────────────────────────────────────────────────────────────────

def _make_img_info(blob_client, blob_size: int):
    """Return a `pytsk3.Img_Info` subclass that range-reads from the
    given async-only `blob_client`. pytsk3 is synchronous so we
    call a sync-wrapped version under `run_in_executor`."""
    import pytsk3  # deferred import — only needed in the worker container

    class AzureBlobImg(pytsk3.Img_Info):
        def __init__(self):
            super().__init__(url="")  # URL unused; we override reads.
            self._blob = blob_client
            self._size = blob_size

        def get_size(self) -> int:
            return self._size

        def read(self, offset: int, size: int) -> bytes:
            # pytsk3 calls this synchronously. blob_client here is the
            # SYNC azure-storage-blob BlobClient (the worker instantiates
            # the sync flavor specifically for TSK).
            end = min(offset + size, self._size)
            if end <= offset:
                return b""
            stream = self._blob.download_blob(offset=offset, length=end - offset)
            return stream.readall()

        def close(self):
            # BlobClient close is optional for the sync SDK; TSK calls
            # close when done but we let GC handle the blob_client.
            pass

    return AzureBlobImg()


# ──────────────────────────────────────────────────────────────────────
# Walker
# ──────────────────────────────────────────────────────────────────────

@dataclass
class WalkStats:
    started_at: float = field(default_factory=time.monotonic)
    directories: int = 0
    files: int = 0
    skipped: int = 0
    total_bytes: int = 0

    def elapsed(self) -> float:
        return time.monotonic() - self.started_at


_FS_TYPE_NAMES = {
    0x01: "ntfs", 0x02: "fat12", 0x04: "fat16", 0x05: "ext2", 0x06: "ufs1",
    0x07: "ext3", 0x08: "swap", 0x09: "raw", 0x0A: "iso9660", 0x0B: "hfs",
    0x0C: "ext4", 0x0D: "yaffs2", 0x0E: "exfat", 0x0F: "btrfs", 0x10: "fat32",
}


def _fs_name(fs_info) -> str:
    """Best-effort lookup of the FS kind for logging + the index column."""
    try:
        ftype = int(fs_info.info.ftype)
    except Exception:
        return "unknown"
    return _FS_TYPE_NAMES.get(ftype, f"fs_{ftype}")


def _walk_filesystem(
    fs, partition_offset: int, fs_type: str,
    volume_item_id: str, snapshot_id: str, blob_path: str,
) -> Iterator[dict]:
    """BFS walk emitting one dict per entry in pytsk3 -> DB shape."""
    import pytsk3
    root_inum = int(getattr(fs.info.root_inum, "value", fs.info.root_inum) or 0) \
        if hasattr(fs.info, "root_inum") else None

    # Start at the filesystem root directory. fs.open_dir(path=...) uses
    # the FS's own notion of path, which for NTFS is "/" and for ext4
    # is also "/". The results get normalised to forward-slash paths.
    queue: list[tuple[str, any]] = [("/", fs.open_dir(path="/"))]

    while queue:
        parent_path, directory = queue.pop(0)
        try:
            entries = list(directory)
        except Exception as e:
            logger.warning("[vm-index] list failed for %s: %s", parent_path, e)
            continue

        for entry in entries:
            try:
                if not entry.info or not entry.info.name:
                    continue
                raw_name = entry.info.name.name
                if raw_name is None:
                    continue
                name = raw_name.decode("utf-8", errors="replace")
                if name in (".", ".."):
                    continue
                meta = entry.info.meta
                if meta is None:
                    # Entry has no metadata — common for deleted items we
                    # don't want to surface in the backup index.
                    continue

                is_dir = int(meta.type) == int(pytsk3.TSK_FS_META_TYPE_DIR)
                size = int(getattr(meta, "size", 0) or 0)
                inode = int(getattr(meta, "addr", 0) or 0)
                mtime_epoch = int(getattr(meta, "mtime", 0) or 0)
                mtime = (datetime.fromtimestamp(mtime_epoch, tz=timezone.utc)
                         .replace(tzinfo=None)) if mtime_epoch > 0 else None

                yield {
                    "snapshot_id": snapshot_id,
                    "volume_item_id": volume_item_id,
                    "parent_path": parent_path,
                    "name": name,
                    "is_directory": is_dir,
                    "size_bytes": size,
                    "modified_at": mtime,
                    "fs_inode": inode,
                    "fs_type": fs_type,
                    "partition_offset": partition_offset,
                    "blob_path": blob_path,
                }

                if is_dir:
                    child_path = (parent_path.rstrip("/") + "/" + name)
                    try:
                        child = fs.open_dir(inode=inode)
                        queue.append((child_path, child))
                    except Exception as e:
                        logger.debug("[vm-index] open_dir failed for %s: %s", child_path, e)
            except Exception as e:
                logger.debug("[vm-index] entry skipped (%s): %s", parent_path, e)


def walk_and_index_blob(
    sync_blob_client, blob_path: str, blob_size: int,
    snapshot_id: str, volume_item_id: str,
    max_entries: Optional[int] = None,
    loop: Optional[asyncio.AbstractEventLoop] = None,
) -> WalkStats:
    """Open the VHD at `blob_path`, walk every filesystem we find, and
    insert batched `vm_file_index` rows via the shared async session
    factory. Returns a stats object for the caller to log.

    `sync_blob_client` must be a SYNC azure-storage-blob BlobClient
    because TSK is blocking; the worker runs this whole function
    inside asyncio.to_thread so the event loop isn't starved.

    `loop` must be the event loop that owns the shared async DB engine
    (capture asyncio.get_running_loop() BEFORE handing off to the
    thread). asyncpg connections are loop-bound, so the inserts have
    to hop back to that loop.

    `max_entries` is primarily a safety cap during first-time rollout
    — a 200k-file VM will take noticeable time, so set a limit while
    you trust-but-verify. Pass None in production."""
    import pytsk3  # noqa: F401 (imported for side effects + version check)
    import uuid as _uuid

    if loop is None:
        raise ValueError(
            "walk_and_index_blob requires the engine-owning event loop "
            "(pass loop=asyncio.get_running_loop() before to_thread)"
        )

    stats = WalkStats()
    logger.info("[vm-index] start blob=%s size=%sMB", blob_path, blob_size // (1024 * 1024))

    img = _make_img_info(sync_blob_client, blob_size)

    # Enumerate partitions. Most Windows VHDs have a 100MB system
    # reserved + the C: partition. Linux disks often have /boot + /.
    try:
        volume = pytsk3.Volume_Info(img)
    except Exception as e:
        # No partition table — could be a whole-disk filesystem.
        logger.info("[vm-index] no partition table on %s (%s); trying FS at offset 0", blob_path, e)
        volume = None

    rows_buf: list[dict] = []

    def _flush():
        """Bulk insert on the engine-owning loop. We're inside a worker
        thread and asyncpg connections are bound to the loop they were
        created on — running the insert on a throwaway new_event_loop()
        seeded the shared pool with dead-loop connections, and the NEXT
        checkout on the main loop (job/snapshot finalize) crashed with
        'Event loop is closed', failing an otherwise-good backup."""
        if not rows_buf:
            return
        asyncio.run_coroutine_threadsafe(_insert_rows(list(rows_buf)), loop).result()
        rows_buf.clear()

    async def _insert_rows(rows: list[dict]) -> None:
        async with async_session_factory() as sess:
            objs = [VmFileIndex(id=_uuid.uuid4(), **r) for r in rows]
            sess.add_all(objs)
            await sess.commit()

    # Walk every partition we recognise.
    fs_targets: list[tuple[int, any]] = []
    if volume is not None:
        for part in volume:
            flags = int(getattr(part, "flags", 0) or 0)
            # Skip unallocated / metadata partitions.
            if flags & pytsk3.TSK_VS_PART_FLAG_META:
                continue
            if flags & pytsk3.TSK_VS_PART_FLAG_UNALLOC:
                continue
            offset = int(part.start) * int(volume.info.block_size)
            length = int(part.len) * int(volume.info.block_size)
            logger.info(
                "[vm-index]   partition start=%s len_bytes=%s desc=%s",
                offset, length,
                getattr(part, "desc", b"").decode("utf-8", errors="replace") if getattr(part, "desc", None) else "",
            )
            try:
                fs = pytsk3.FS_Info(img, offset=offset)
                fs_targets.append((offset, fs))
            except Exception as e:
                logger.info("[vm-index]   no recognised FS at offset %s: %s", offset, e)
    else:
        try:
            fs = pytsk3.FS_Info(img, offset=0)
            fs_targets.append((0, fs))
        except Exception as e:
            logger.warning("[vm-index] no FS at offset 0: %s", e)

    if not fs_targets:
        logger.warning("[vm-index] no recognised filesystems in %s — index empty", blob_path)
        return stats

    for offset, fs in fs_targets:
        fs_type = _fs_name(fs)
        logger.info("[vm-index]   walking %s @ offset=%s", fs_type, offset)
        for row in _walk_filesystem(
            fs, partition_offset=offset, fs_type=fs_type,
            volume_item_id=volume_item_id, snapshot_id=snapshot_id,
            blob_path=blob_path,
        ):
            rows_buf.append(row)
            if row["is_directory"]:
                stats.directories += 1
            else:
                stats.files += 1
                stats.total_bytes += row["size_bytes"]
            if max_entries is not None and (stats.directories + stats.files) >= max_entries:
                logger.info(
                    "[vm-index]   hit max_entries=%s — truncating", max_entries,
                )
                break
            if len(rows_buf) >= 500:
                _flush()
                logger.info(
                    "[vm-index]   progress dirs=%s files=%s elapsed=%.1fs",
                    stats.directories, stats.files, stats.elapsed(),
                )
        _flush()

    logger.info(
        "[vm-index] DONE blob=%s dirs=%s files=%s total_bytes=%s elapsed=%.1fs",
        blob_path, stats.directories, stats.files, stats.total_bytes, stats.elapsed(),
    )
    return stats
