"""Ref-counted blob garbage-collection primitives.

Blobs in TMvault are shared: the sparse / carry-forward capture model means one
physical blob_path is referenced by the snapshot that first wrote it AND by
every later snapshot that carried the unchanged item forward (content-addressed
reuse). Therefore a blob may be physically deleted ONLY when no surviving
snapshot_item references it — a plain "delete this snapshot's blobs" purge
deletes bytes that live snapshots still need.

These are pure, side-effect-free helpers so they can be unit-tested and reused
by every deletion path (the cancelled-snapshot sweep today, retention GC once
it is re-enabled). The actual DELETE is left to the caller; these functions only
decide WHAT is safe to delete and WHICH container(s) to target.
"""
from __future__ import annotations

from typing import Iterable, List, Optional, Set, Tuple


def blobs_safe_to_delete(
    candidate_blob_paths: Optional[Iterable[str]],
    still_referenced_blob_paths: Optional[Iterable[str]],
) -> Set[str]:
    """Ref-counted GC core.

    Return the subset of ``candidate_blob_paths`` that NO surviving
    snapshot_item references (``still_referenced_blob_paths``) — i.e. the blobs
    that are safe to physically delete. Falsy paths are never emitted. Safe on
    ``None`` inputs so callers don't have to pre-normalise query results.
    """
    referenced = set(still_referenced_blob_paths or ())
    return {p for p in (candidate_blob_paths or ()) if p and p not in referenced}


def container_candidates(
    resource_type: str,
    tenant_id: str,
    workloads: Optional[Iterable[str]] = None,
) -> List[str]:
    """Container names a blob for this resource may physically live in.

    Reproduces the WRITE-side derivation: blobs are written under the workload
    suffix (``AzureStorageManager.get_container_name(tenant, workload)``), not
    the raw resource_type — e.g. ONEDRIVE bytes land in ``backup-files-<t>``,
    never ``backup-onedrive-<t>``. Purge/GC must target these, newest-convention
    first, with the legacy raw-resource_type container offered LAST as a
    best-effort fallback for very old rows.

    ``workloads`` can be injected (keeps this hermetic / unit-testable); when
    omitted it is looked up lazily from the shared mapping so importing this
    module never drags in the heavy storage stack.
    """
    tenant_short = (str(tenant_id) if tenant_id else "").replace("-", "")[:8]

    if workloads is None:
        try:
            from shared.azure_storage import workload_candidates_for_resource_type
            workloads = workload_candidates_for_resource_type(resource_type)
        except Exception:
            workloads = ()

    names: List[str] = []
    for w in workloads or ():
        safe = str(w).lower().replace("_", "-")
        name = f"backup-{safe}-{tenant_short}"
        if name not in names:
            names.append(name)

    # Legacy fallback: some very old rows used the raw resource_type as the
    # container suffix. Kept LAST so the correct workload bucket always wins.
    legacy = f"backup-{str(resource_type or 'generic').lower().replace('_', '-')}-{tenant_short}"
    if legacy not in names:
        names.append(legacy)
    return names


def partition_blob_paths_for_deletion(
    doomed_blob_paths: Optional[Iterable[str]],
    surviving_blob_paths: Optional[Iterable[str]],
) -> Tuple[Set[str], Set[str]]:
    """Split a doomed snapshot's blob_paths into (safe_to_delete, still_shared).

    Convenience wrapper around :func:`blobs_safe_to_delete` that also returns the
    retained set, so a caller can log exactly how many blobs were protected by
    ref-counting (observability for a destructive operation).
    """
    doomed = {p for p in (doomed_blob_paths or ()) if p}
    safe = blobs_safe_to_delete(doomed, surviving_blob_paths)
    shared = doomed - safe
    return safe, shared
