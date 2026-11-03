"""Windows-safe ZIP arcname helpers.

OneDrive file + folder names include characters forbidden on Windows (`< > : " / \\ | ? *`),
paths that exceed the 260-char default filesystem cap, and sometimes collide after
sanitization (two files in one folder both renamed "_______.docx"). These helpers
make arcnames safe to extract on every target platform without losing uniqueness.
"""
from __future__ import annotations

import hashlib
import os


def sanitize_arcname(
    raw_path: str,
    *,
    max_len: int,
    replace_chars: str,
) -> str:
    """Return a Windows-safe ZIP arcname derived from `raw_path`.

    Replaces reserved chars with `_`, normalises backslashes to forward slashes,
    collapses empty segments, and truncates oversize paths with a SHA-1 suffix
    so two long names can't alias to the same arcname.
    """
    if not raw_path:
        return ""

    path = raw_path.replace("\\", "/")

    _BAD = {"", ".", ".."}
    sanitized_segments = []
    for seg in path.split("/"):
        if seg.strip() in _BAD:
            continue
        cleaned = "".join("_" if c in replace_chars else c for c in seg)
        cleaned = cleaned.replace("/", "_").rstrip(". ")
        if cleaned:
            sanitized_segments.append(cleaned)

    arcname = "/".join(sanitized_segments)

    if len(arcname) <= max_len:
        return arcname

    _root, ext = os.path.splitext(arcname)
    digest = hashlib.sha1(raw_path.encode("utf-8")).hexdigest()[:10]
    suffix = f"~{digest}{ext}"
    head_budget = max_len - len(suffix)
    if head_budget <= 0:
        return arcname[: max_len - 11] + "~" + digest
    return arcname[:head_budget] + suffix


def resolve_arcname_collision(
    arcname: str,
    *,
    external_id: str,
    used: set,
) -> str:
    """If `arcname` already exists in `used`, append `~<external_id[:8]>` before
    the extension. Mutates `used` to record the returned name."""
    if arcname not in used:
        used.add(arcname)
        return arcname
    head, ext = os.path.splitext(arcname)
    suffix = f"~{external_id[:8]}"
    collision_free = f"{head}{suffix}{ext}"
    used.add(collision_free)
    return collision_free
