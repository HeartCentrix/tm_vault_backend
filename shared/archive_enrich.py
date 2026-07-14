"""Archive PREVIEW ENRICHMENT — pure core.

Turns a staged Graph JSON message (the scratch-mailbox read of an archive item's
opaque blob) into the update applied to the source ``ARCHIVE_ITEM`` row, so the
item becomes previewable/exportable exactly like normal mail. The heavy async
orchestration (stage → map → persist, paced + incremental) lives in
``shared/archive_enrichment.py`` + the enrichment-worker; this module is the
pure, testable transform. Pairs with archive_export.py
(:meth:`ArchiveExportStager.stage_and_map`).

Backup/restore/export never depend on enrichment — it only unlocks in-browser
preview + fast export. State lives on the row's ``extra_data.enrich_status``.
"""
from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict, List

# enrich_status lifecycle on ARCHIVE_ITEM.extra_data
PENDING = "pending"
PROCESSING = "processing"
ENRICHED = "enriched"
FAILED = "failed"


def _addr(entry: Any) -> str:
    """Graph emailAddress shape → 'Name <addr>' (or just addr, or '')."""
    a = (entry or {}).get("emailAddress", {}) if isinstance(entry, dict) else {}
    name, addr = (a.get("name") or "").strip(), (a.get("address") or "").strip()
    if name and addr:
        return f"{name} <{addr}>"
    return addr or ""


def needs_enrichment(extra_data: Dict[str, Any]) -> bool:
    """True if this row should be (re)enriched: never touched, or a prior attempt
    FAILED (retry). Skip when already ENRICHED or currently PROCESSING."""
    st = (extra_data or {}).get("enrich_status")
    return st not in (ENRICHED, PROCESSING)


def enrichment_from_message(graph_json: Dict[str, Any]) -> Dict[str, Any]:
    """Build the row update from a staged Graph message. Returns
    ``{"name": <subject>, "extra_data_patch": {...}}`` — the worker merges the
    patch into the row's ``extra_data`` and sets ``name`` so the browse list
    shows the real subject and click-to-preview renders off ``raw``."""
    g = graph_json or {}
    subject = (g.get("subject") or "").strip() or "(no subject)"
    return {
        "name": subject[:255],
        "extra_data_patch": {
            "enrich_status": ENRICHED,
            "raw": g,  # full message JSON — preview renders from this, like USER_MAIL
            "subject": subject,
            "from": _addr(g.get("from") or g.get("sender")),
            "to": [_addr(r) for r in (g.get("toRecipients") or [])],
            "received": g.get("receivedDateTime") or g.get("sentDateTime"),
            "has_attachments": bool(g.get("hasAttachments") or g.get("attachments")),
        },
    }


async def enrich_items(
    stager: Any,
    items: List[Any],
    apply_update: Callable[[Any, str, Dict[str, Any]], Awaitable[None]],
) -> int:
    """Stage ``items`` (ARCHIVE_ITEM rows), map each staged Graph message back to
    its source row, and persist the enrichment. ``apply_update(row, name, patch)``
    writes ONE row (the worker supplies the DB write). Per-item failures are
    logged + isolated — one bad row never aborts the batch. Returns the count of
    successfully enriched rows.

    ``stager`` is an :class:`archive_export.ArchiveExportStager` whose
    ``download_blob`` fetches each row's opaque blob; it owns the temp-folder
    lifecycle + guaranteed cleanup."""
    enriched = 0
    async for src, msg in stager.stage_and_map(items):
        try:
            e = enrichment_from_message(msg)
            await apply_update(src, e["name"], e["extra_data_patch"])
            enriched += 1
        except Exception as ex:  # noqa: BLE001 - isolate per-row failures
            print(f"[archive_enrich] enrich failed for "
                  f"{getattr(src, 'external_id', None)}: {type(ex).__name__}: {ex}")
    return enriched
