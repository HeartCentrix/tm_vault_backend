"""Regression: PST export must probe the ARCHIVE container for attachment bytes.

Online Archive fileAttachments are captured as ARCHIVE_ATTACHMENT sibling rows
whose bytes live in ``backup-archive-<tenant>`` (shared/archive_enrichment.py).
The PST export downloads attachment bytes by trying a list of candidate source
containers in order. That list was ("email", "mailbox", "group-mailbox") — it
never included "archive", so archive attachment blobs were never found: the
download returned None, mail.py's ``if not data: continue`` skipped it, and the
metadata-only attachment (name/size, no bytes) got written to the PST — a
broken ~270-byte attachment the user couldn't open.

This pins "archive" into the candidate workload list so those bytes resolve.
"""
from __future__ import annotations

import ast
import pathlib

_PST_EXPORT = (
    pathlib.Path(__file__).resolve().parents[2]
    / "workers" / "restore-worker" / "pst_export.py"
)


def _load_const(name: str):
    src = _PST_EXPORT.read_text()
    tree = ast.parse(src)
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            getattr(t, "id", None) == name for t in node.targets
        ):
            return ast.literal_eval(node.value)
    return None


def test_pst_source_workloads_include_archive():
    workloads = _load_const("PST_SOURCE_WORKLOADS")
    assert workloads is not None, "PST_SOURCE_WORKLOADS constant missing"
    assert "archive" in workloads  # archive fileAttachment bytes must be searchable
    # mail-bearing containers still covered (no regression)
    assert "email" in workloads
    assert "mailbox" in workloads
