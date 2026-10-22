"""One canonical, server-side description of a storage backend row.

Why this module exists (2026-08-11, QA-11)
------------------------------------------
System > Storage and System > Health described the SAME backend row two
different ways and looked like a disagreement about where the bytes go:

    System > Storage : "Cloud Storage - WASABI - US-WEST-2 - STABLE"
    System > Health  : "active=seaweedfs/seaweedfs-local, state=stable"

Both surfaces read `system_config.active_backend_id` -> `storage_backends`;
neither was stale. They rendered different COLUMNS of the same row:

    id        cf0dcac3-871c-4ef6-ae35-73d85e6d1552
    kind      seaweedfs                     <- health printed this
    name      seaweedfs-local               <- ...and this
    endpoint  https://s3.us-west-2.wasabisys.com   <- storage page labelled this

The `seaweedfs` kind is the S3-compatible SLOT, and in this deployment
deploy/taylor-morrison/k8s/10-configmap.yaml points that slot at Wasabi
(ONPREM_S3_ENDPOINT), keeping the seeded name "seaweedfs-local" only
because shared/storage_bootstrap.py seeds that literal and
STORAGE_DEFAULT_BACKEND must match it. So "seaweedfs-local" is a slot
name, not a host — printing it alone names a SeaweedFS box that receives
nothing.

Ground truth measured 2026-08-11: all 3,579,233 snapshot_items written in
the preceding 24h carry backend_id=cf0dcac3 (that row), storage_toggle_events
is empty (0 toggles ever), and there is no active-backend override in
app_settings. The bytes are on Wasabi us-west-2, bucket tm-vault.

The label logic below deliberately mirrors the frontend's
src/services/adminStorage.ts backendLabel()/backendDetail() so the two can
no longer drift; the intent is that the frontend switches to the
server-provided `label`/`detail` fields and deletes its local copy.
"""
from __future__ import annotations

import re
from typing import Any, Dict, Optional

_WASABI_REGION_RE = re.compile(r"s3\.([a-z0-9-]+)\.wasabisys\.com")

_KIND_LABELS = {
    "azure_blob": "Azure Blob",
    "seaweedfs": "SeaweedFS",
}


def backend_provider(kind: Optional[str], endpoint: Optional[str]) -> str:
    """Provider the endpoint actually resolves to.

    Endpoint wins over kind: the `seaweedfs` kind is an S3-compatible slot
    that can be (and here is) pointed at a hosted S3 provider. Falls back to
    the kind when the endpoint says nothing recognisable.
    """
    ep = (endpoint or "").lower()
    if "wasabisys.com" in ep:
        return "wasabi"
    if "blob.core.windows.net" in ep:
        return "azure_blob"
    if "amazonaws.com" in ep:
        return "s3"
    return (kind or "unknown")


def backend_label(kind: Optional[str], name: Optional[str],
                  endpoint: Optional[str]) -> str:
    """Human-facing name — the same string the Storage page shows."""
    provider = backend_provider(kind, endpoint)
    if provider == "wasabi":
        return "Wasabi (S3)"
    if provider == "s3":
        return "Amazon S3"
    if provider == "azure_blob" or kind == "azure_blob":
        return "Azure Blob"
    if kind in _KIND_LABELS:
        return _KIND_LABELS[kind]
    return name or (kind or "unknown")


def backend_detail(kind: Optional[str], endpoint: Optional[str]) -> str:
    """Short sub-label (provider + region / host) shown beside the name."""
    ep = (endpoint or "").lower()
    m = _WASABI_REGION_RE.search(ep)
    if m:
        return f"wasabi · {m.group(1)}"
    return kind or "unknown"


def describe_backend(kind: Optional[str], name: Optional[str],
                     endpoint: Optional[str]) -> Dict[str, Any]:
    """Full display bundle for one storage_backends row."""
    return {
        "label": backend_label(kind, name, endpoint),
        "detail": backend_detail(kind, endpoint),
        "provider": backend_provider(kind, endpoint),
    }


def backend_summary(kind: Optional[str], name: Optional[str],
                    endpoint: Optional[str]) -> str:
    """One-line identification used in log/health strings.

    Carries the friendly label AND the raw slot identifiers AND the
    endpoint, because each answers a different operator question and QA-11
    happened when a surface printed only one of the three.
    """
    return (
        f"{backend_label(kind, name, endpoint)} "
        f"[{kind or '?'}/{name or '?'}] "
        f"endpoint={endpoint or 'unset'}"
    )
