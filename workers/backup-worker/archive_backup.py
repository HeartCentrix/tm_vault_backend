"""USER_ARCHIVE backup — Exchange Online in-place archive via the Graph
Mailbox Import/Export APIs (Task 2.2, ITO-9777).

Wire shapes are implemented against the official MS beta docs
(learn.microsoft.com "Handle archive mailbox redirects", fetched 2026-07-24),
not assumptions:

  1. Folder/item enumeration — ``GET .../folders`` and ``.../folders/{id}/items``
     ($select=id,changeKey,size,type; pages via @odata.nextLink). Content that
     lives in an auxiliary (autoexpanded) archive mailbox is served via an
     ``HTTP 308 Permanent Redirect`` with a ``Location`` header and empty body.
     308 preserves method + body, and httpx re-sends the Authorization header
     on SAME-HOST redirects — the aux archive is the same graph.microsoft.com
     host — so the shared client (follow_redirects=True, see
     GraphClient._get_shared_http) auto-follows these transparently. We do NOT
     hand-parse a 4xx-with-JSON body for enumeration redirects: that is not the
     documented shape for GET.
  2. changeKey incremental — per-item ``changeKey`` diff vs the last
     COMPLETED/PARTIAL snapshot's ARCHIVE_ITEM rows. Unchanged items are NOT
     re-exported; their prior rows are carried forward (INSERT…SELECT, blob
     reused), mirroring main.py's OneDrive FILE_VERSION carry-forward.
  3. exportItems — ``POST .../exportItems`` body ``{"itemIds":[...]}`` (HARD CAP
     20). Returns **200 OK**; each ``value[]`` entry is EITHER a success
     (``{"itemId","changeKey","data"}``) OR a per-item error
     (``{"itemId","Error":{"code","message"}}``, capital-E ``Error``). For
     ``ErrorArchiveFolderMovedPermanently`` the ``Error.message`` is the full
     reissue URL: we group such items by that URL and re-POST (still ≤20),
     capped at 2 redirect hops per item. The ``data`` is an opaque FastTransfer
     stream — base64-decoded for storage, never parsed. Incremental selection
     uses ``changeKey``; byte hashes preserve the local storage format.

Honesty rules:
  * Any Graph *enumeration/export transport* failure (auth, persistent 5xx,
    throttle-exhaustion, network) raises ``EnumerationFailed`` so the caller
    marks the snapshot FAILED — never COMPLETED-empty after a real error
    (commit 525da41 pattern). On the first such failure sibling export tasks
    are cancelled so no more rows commit under a doomed run.
  * Per-item export failures (redirect past the hop cap, any other per-item
    ``Error`` code) and failed blob uploads are counted and surfaced as
    ``files_failed`` on ``snapshot.delta_tokens_json`` — the convention
    ``complete_snapshot`` (main.py) reads to finalize PARTIAL (>0 failures)
    vs COMPLETED (0). An archive with 0 items and 0 errors is a legitimate
    COMPLETED-empty.

Decoupling: Graph/DB/storage collaborators are injectable and the
DB/storage/config imports are lazy (inside the default collaborators) so unit
tests drive the control flow with a stub session + stub storage — the repo has
no real-DB fixture (Postgres-only models). ``backup_archive`` in main.py is a
thin delegator.
"""
from __future__ import annotations

import asyncio
import re
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import httpx

from urllib.parse import urlsplit

# SSRF guard (Check-17): URLs rebuilt from Graph response bodies
# (@odata.context origins, error.message redirects) must never re-anchor a
# credentialed request off-host. Reuse the single allow-list the
# credential-attaching helpers enforce in shared.graph_client.
from shared.graph_client import _assert_graph_target as _assert_graph_target



import uuid
from typing import Any, Dict, List, Optional

# Object-storage workload string for opaque archive blobs. Deliberately NOT
# "email": the readable Graph-JSON mail blobs and the opaque MAPI archive blobs
# must never share a container — different restore semantics + GC lifecycles.
ARCHIVE_WORKLOAD = "archive"

ARCHIVE_ITEM_TYPE = "ARCHIVE_ITEM"


# Error signals meaning "this archive is inaccessible to us" — the app lacks
# consent in the tenant, or the mailbox is unlicensed / gone. These must SKIP
# gracefully (benign 0-item snapshot), never mark the backup FAILED. Everything
# else (5xx, timeouts, bugs) is a real/transient failure that should fail+retry.
_SKIP_SIGNALS = (
    "MailboxNotEnabledForRESTAPI", "ErrorAccessDenied", "Authorization_RequestDenied",
    "ErrorMailboxNotFound", "does not have access", "Access is denied", "Forbidden",
    "Unauthorized",
)


def is_skippable_error(error_text: Optional[str]) -> bool:
    """True if the error means the archive is simply inaccessible (skip), not a
    transient/real failure (fail + retry)."""
    s = error_text or ""
    sl = s.lower()
    if any(sig.lower() in sl for sig in _SKIP_SIGNALS):
        return True
    # bare HTTP auth/consent status codes (GraphClient stringifies errors)
    return any(code in s for code in ("403", "401"))


def item_kind(mapi_type: Optional[str]) -> str:
    """Map a mailboxItem's MAPI message class (the only descriptive field the
    Import/Export items endpoint exposes — live-verified that $select of
    subject/sender/from returns HTTP 400 "no such property") to a human kind for
    the Recovery browse. Subjects are unavailable without staging, so this is the
    most a catalog row can say about "what it is"."""
    t = (mapi_type or "").strip()
    tl = t.lower()
    if tl.startswith("ipm.note"):
        return "Email"
    if tl.startswith("ipm.contact") or tl.startswith("ipm.distlist"):
        return "Contact"
    if tl.startswith("ipm.appointment") or tl.startswith("ipm.schedule"):
        return "Calendar event"
    if tl.startswith("ipm.task"):
        return "Task"
    if tl.startswith("ipm.stickynote"):
        return "Note"
    if tl.startswith("ipm.activity"):
        return "Journal"
    if tl.startswith("ipm.post"):
        return "Post"
    if tl.startswith("report."):
        return "Delivery report"
    return t or "Item"


def state_blob_path(tenant_id: str, resource_id: str) -> str:
    """Deterministic, OVERWRITABLE path for a resource's incremental changeKey
    state blob. Fixed (no snapshot id / timestamp) so every run reads and
    overwrites the same object — this is the live cursor, not per-run history."""
    return f"{tenant_id}/{resource_id}/_archive_state/changekeys.json"


def advance_state(
    prev: Optional[Dict[str, str]],
    current_ck: Dict[str, str],
    stored_ck: Dict[str, str],
) -> Dict[str, str]:
    """Next incremental state (``item_id -> changeKey``) after a run.

    * ``current_ck`` — every item id present in the archive right now.
    * ``stored_ck`` — items we successfully EXPORTED + STORED this run (new key).
    * ``prev``       — last run's state (``None`` on the first run).

    Per item id in ``current_ck``:
      - stored this run           -> take the newly-stored key (advance)
      - else known in ``prev``    -> keep the prev key. Two cases collapse here:
          an UNCHANGED item (key already matched, nothing to do), and a CHANGED
          item whose export FAILED — keeping the OLD key makes
          ``select_changed`` re-flag it next run, i.e. automatic per-item retry.
      - else (brand-new + failed) -> omit, so it's retried next run.

    Items absent from ``current_ck`` (deleted from the archive) are DROPPED —
    their backed-up blobs stay in prior snapshots (never deleted); the driver
    just stops tracking them, and a re-created item re-exports from scratch.
    """
    prev = prev or {}
    out: Dict[str, str] = {}
    for iid, _ck in current_ck.items():
        if iid in stored_ck:
            out[iid] = stored_ck[iid]
        elif iid in prev:
            out[iid] = prev[iid]
        # else: brand-new item we failed to store -> omit (retry next run)
    return out


def build_folder_path_map(folders: List[Dict[str, Any]]) -> Dict[str, str]:
    """Map each folder id -> its full ``/``-joined path from the mailbox root,
    walking ``parentFolderId``. The Import/Export folder object's ``displayName``
    is only the LEAF name (proven live: 'Archive', 'Deleted Items'), and the
    mailbox root itself is NOT in the list — so a top-level folder maps to just
    its own name and a nested folder to 'Parent/Child/Leaf'. Cycle/orphan-safe:
    the walk is bounded by the folder count, so a malformed parent loop
    terminates instead of hanging."""
    by_id = {f["id"]: f for f in folders if f.get("id")}
    out: Dict[str, str] = {}
    for fid, f in by_id.items():
        parts: List[str] = []
        cur: Optional[Dict[str, Any]] = f
        seen: set = set()
        while cur is not None and cur.get("id") not in seen and len(parts) <= len(by_id):
            seen.add(cur.get("id"))
            parts.append(cur.get("displayName") or cur.get("id"))
            cur = by_id.get(cur.get("parentFolderId"))  # None at the (unlisted) root
        out[fid] = "/".join(reversed(parts))
    return out


def build_archive_item_row(
    *, snapshot_id: Any, tenant_id: Any, folder: Dict[str, Any], item: Dict[str, Any],
    blob_path: str, content_hash: str, blob_len: int, mailbox_id: str,
    graph_version: str, folder_path: Optional[str] = None,
) -> Dict[str, Any]:
    """One SnapshotItem dict for ``_bulk_upsert_snapshot_items`` (mapped-class
    insert -> the JSON-envelope key MUST be ``extra_data``, NOT the column name
    ``metadata``, or it silently vanishes). The item's ``changeKey`` rides in
    ``extra_data`` so the catalog self-describes for restore; the blob itself is
    the opaque MAPI FastTransfer stream, stored verbatim."""
    kind = item_kind(item.get("type"))
    return {
        "id": uuid.uuid4(),
        "snapshot_id": snapshot_id,
        "tenant_id": tenant_id,
        "external_id": item["id"],
        "parent_external_id": folder.get("id"),
        "item_type": ARCHIVE_ITEM_TYPE,
        # No subject is available from the opaque endpoint — the friendly kind
        # (Email/Contact/Calendar…) is the most a browse row can show.
        "name": kind,
        # Full nested path ('Archive/Projects/2024') when the caller reconstructs
        # it from the parentFolderId chain; else the folder's own leaf name.
        "folder_path": folder_path or folder.get("displayName") or folder.get("id"),
        "content_size": blob_len,
        "content_hash": content_hash,
        "blob_path": blob_path,
        "content_checksum": content_hash,
        "extra_data": {
            "change_key": item.get("changeKey"),
            "opaque": True,
            "kind": kind,
            "mapi_type": item.get("type"),
            "mailbox_id": mailbox_id,
            "graph_version": graph_version,
            "size": item.get("size"),
            "last_modified": item.get("lastModifiedDateTime"),
        },
    }


def _allowlisted_https(url: Any) -> Optional[str]:
    """Return ``url`` when it is an https request to an allow-listed Graph/
    SharePoint host, else None. Wraps _assert_graph_target so callers that
    treat a bad value as 'no redirect' don't have to catch."""
    if not isinstance(url, str):
        return None
    try:
        _assert_graph_target(url)
    except ValueError:
        return None
    return url


# exportItems accepts at most 20 itemIds per call (Graph hard cap, doc §3).
BATCH = 20

# Per-item reissue cap for the autoexpanded-archive exportItems redirect:
# an item may be re-POSTed to a Location URL at most twice; still redirecting
# after that counts as a failed item.
MAX_EXPORT_HOPS = 2

_URL_RE = re.compile(r"https?://[^\s\"'<>]+")


class EnumerationFailed(Exception):
    """A Graph folder/item enumeration or exportItems *transport* call died
    (auth failure, persistent 5xx, throttle-exhaustion, network). Raised so the
    caller marks the snapshot FAILED instead of finalizing an empty COMPLETED
    snapshot after a real error — the same honesty rule the mailbox/OneDrive
    walks enforce. (Per-item export failures are NOT this — they finalize
    PARTIAL, see run_archive_backup.)"""


# ─────────────────────────── pure helpers ────────────────────────────────

def _chunks(seq: List[Any], n: int):
    """Yield successive ``n``-sized slices of ``seq`` (used for the 20-id cap)."""
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _select_items_to_export(
    items: List[Dict[str, Any]], prev_changekeys: Dict[str, Optional[str]],
) -> List[Dict[str, Any]]:
    """Incremental filter: keep an item when it is new (id absent from the
    prior snapshot) OR its ``changeKey`` differs from the prior snapshot's.
    Unchanged items are dropped here and carried forward instead."""
    return [
        it for it in items
        if prev_changekeys.get(it.get("id")) != it.get("changeKey")
    ]


def _base_from_odata_context(ctx: Any, fallback: str) -> str:
    """Return the ``…/folders`` request base for the mailbox that ACTUALLY
    served a response, read back off its ``@odata.context``.

    When Exchange auto-expands an archive it relocates folders into an
    auxiliary archive mailbox. Graph answers a request for a relocated folder
    with ``308`` + ``ErrorArchiveFolderMovedPermanently`` whose ``Location``
    points at the AUX mailbox, and the shared httpx client follows that
    transparently (see ``_walk_folders``). So the response body is correct but
    the caller never learns it came from a different mailbox — the only signal
    is ``@odata.context``::

      https://graph.microsoft.com/beta/$metadata#admin/Exchange/Mailboxes/
      MBX:aux@tenant/folders('AQMk…')/childFolders

    Every folder id inside such a response is scoped to the AUX mailbox and
    answers ``404 ErrorItemNotFound`` against the primary base — the 2026-08-06
    incident. Returns ``fallback`` when the context is absent or an
    unrecognised shape, so a Graph response-shape change degrades to today's
    behaviour rather than crashing the walk.
    """
    if not isinstance(ctx, str) or "$metadata#" not in ctx:
        return fallback
    _root, _, frag = ctx.partition("$metadata#")
    if "/folders" not in frag:
        return fallback
    path = frag.split("/folders", 1)[0].strip("/")
    if not path:
        return fallback
    # SSRF guard (Check-17): the context's own origin (``_root`` =
    # scheme+host+API-version, e.g. https://graph.microsoft.com/beta) is
    # Graph response data. Trust it ONLY when it is an https request to an
    # allow-listed Graph host — then use it verbatim, which preserves both
    # the /beta version segment AND the auxiliary-mailbox path that the
    # ITO-10146 union walk depends on. A hostile origin (link-local metadata,
    # a ClusterIP, localhost) fails the allow-list and degrades to the
    # known-good ``fallback`` base rather than re-pointing a bearer request.
    if _allowlisted_https(_root.strip()):
        return f"{_root.strip().rstrip('/')}/{path}/folders"
    return fallback


def _export_redirect_url(entry_error: Any) -> Optional[str]:
    """If a per-item exportItems ``Error`` object is the autoexpanded-archive
    redirect (``ErrorArchiveFolderMovedPermanently``), return the reissue URL —
    the ``message`` IS the full URL per the docs; regex-fallback if it is only
    embedded. Any other error code (or shape) returns None → treated as a
    failed item by the caller."""
    if not isinstance(entry_error, dict):
        return None
    if entry_error.get("code") != "ErrorArchiveFolderMovedPermanently":
        return None
    # SSRF guard (Check-17): the reissue URL is re-POSTed via a helper that
    # attaches the app-only bearer, so require https AND an allow-listed
    # Graph/SharePoint host before trusting anything extracted from the
    # (attacker-influenceable) error body. A value that fails the check is
    # treated as 'no redirect' → the caller marks the item failed.
    msg = entry_error.get("message")
    if isinstance(msg, str):
        if msg.startswith("https://"):
            ok = _allowlisted_https(msg)
            if ok:
                return ok
        else:
            m = _URL_RE.search(msg)
            if m:
                ok = _allowlisted_https(m.group(0))
                if ok:
                    return ok
    for key in ("target", "url", "location"):
        v = entry_error.get(key)
        if isinstance(v, str) and v.startswith("https://"):
            ok = _allowlisted_https(v)
            if ok:
                return ok
    return None


# ───────────────────────── folder enumeration ────────────────────────────

def _graph_error_code(exc: httpx.HTTPStatusError) -> Optional[str]:
    """``error.code`` out of a Graph error body, best-effort."""
    resp = getattr(exc, "response", None)
    if resp is None:
        return None
    try:
        body = resp.json()
    except Exception:
        return None
    if isinstance(body, dict):
        err = body.get("error")
        if isinstance(err, dict):
            code = err.get("code")
            if isinstance(code, str):
                return code
    return None


# Auth-class statuses are NEVER a "bad folder" — they mean the whole walk is
# doomed, and tolerating them would let an expired credential produce a
# near-empty archive that finalizes PARTIAL and is offered as a restore point.
_WALK_FATAL_STATUSES = frozenset({401, 403})

_MBX_BASE_RE = re.compile(
    r"(https?://[^\s\"'<>]+?/admin/exchange/mailboxes/[^/\s\"'<>]+)",
    re.IGNORECASE,
)


class _ArchiveWalker:
    """Multi-location archive enumeration (ITO-10146).

    An autoexpanded archive shards its content across the primary archive
    mailbox plus auxiliary mailboxes. Since ~2026-08-06 Graph's folder
    listings return only the children RESIDENT in the location that serves
    the call — ``childFolders`` declared 23, returned 2, HTTP 200, no
    nextLink, ``$top``-independent (reproduced 3/3 live 2026-08-18) — so a
    single-root walk silently loses every subtree resident elsewhere
    (~600k items across the terminated-employee archives). Folder ids are
    LOCATION-SCOPED (an aux-minted id 404s against the primary base), so the
    same logical folder must be enumerated once per location and the trees
    unioned by PATH; items live in exactly one location, so deduping items
    by id collapses any facet overlap.

    Liveness: every Graph round-trip fires ``on_progress`` (threaded to
    ``_get(on_activity=…)``), because one folder's item listing paginates
    for hours (332k items at the endpoint's 10-per-page default) inside a
    single ``_get`` — stamping only on folder completion is what let the
    stall guard deterministically kill every run at 562 progress events.

    Failure accounting is derived at the UNION level: a facet that lists
    no children while another location serves them is NOT a loss, so
    per-facet shortfalls/failures are logged live but only counted after
    all locations are walked (`derive_failures`).
    """

    def __init__(self, graph_client, graph_base: str, mbx: str, *,
                 concurrency: int, worker_id: str,
                 max_folder_failures: int = 100,
                 max_locations: int = 8,
                 folder_page_size: int = 200,
                 item_page_size: int = 1000,
                 on_progress: Optional[Callable[[], None]] = None):
        from typing import Set  # local: keeps the module import block stable
        self.g = graph_client
        self.graph_base = graph_base
        self.mbx = mbx
        self.sem = asyncio.Semaphore(max(1, concurrency))
        self.worker_id = worker_id
        self.max_folder_failures = max_folder_failures
        self.max_locations = max(1, max_locations)
        self.folder_page_size = folder_page_size
        self.item_page_size = item_page_size
        self.on_progress = on_progress
        self.lock = asyncio.Lock()
        self.primary_base = f"{graph_base}/admin/exchange/mailboxes/{mbx}/folders"
        self.collected: List[Dict[str, Any]] = []
        # Raw per-facet failure records — diagnostic + the in-flight fatal cap.
        self.raw_failures: List[Dict[str, Any]] = []
        # Union bookkeeping, all keyed on logical folder PATH (names mirror
        # across locations; ids do not).
        # path -> max item count any facet served. A bare 200 is NOT
        # proof of service: items live in exactly one location, so every
        # non-resident facet answers 200-EMPTY by design — healing an items
        # failure off an empty 200 silently drops the resident folder's
        # entire content (review finding, 2026-08-18).
        self.items_served: Dict[str, int] = {}
        self.items_failed: Dict[str, Dict[str, Any]] = {}
        self.children_ok: Set[str] = set()
        self.children_failed: Dict[str, Dict[str, Any]] = {}
        self.declared: Dict[str, int] = {}
        self.children_seen: Dict[str, Set[str]] = {}
        self.paths_seen: Set[str] = set()
        self.failed_top_ids: Set[str] = set()
        self.top_count: int = 0
        self.walked_bases: Set[str] = set()
        self.pending_bases: List[str] = []
        # DISTINCT bases skipped over the cap (a set — the same over-cap
        # location is sighted once per response it serves).
        self.skipped_bases: Set[str] = set()
        # Root-listing failures per location — surfaced by derive_failures:
        # a location whose root won't list loses ALL its residents, which
        # must finalize PARTIAL, never a clean COMPLETED.
        self.location_root_failures: List[Dict[str, Any]] = []

    # ── location discovery ────────────────────────────────────────────
    def note_base(self, folders_base: str) -> None:
        """Queue a newly-seen mailbox location for a root walk of its own.
        No Graph API lists auxiliary mailboxes — they are only discoverable
        from ``@odata.context`` re-basing and exportItems redirect URLs."""
        norm = folders_base.rstrip("/").lower()
        if not norm.endswith("/folders"):
            return
        if norm in self.walked_bases:
            return
        if any(p.rstrip("/").lower() == norm for p in self.pending_bases):
            return
        if len(self.walked_bases) + len(self.pending_bases) >= self.max_locations:
            self.skipped_bases.add(norm)
            return
        self.pending_bases.append(folders_base)

    def _params_folders(self) -> Dict[str, str]:
        # Explicit $top: the endpoint's default page is 10 and its
        # pagination is the broken part — $top=100 returned 5 top-level
        # folders the default page never surfaced (live 2026-08-18).
        return {"$top": str(self.folder_page_size)}

    def _params_items(self) -> Dict[str, str]:
        return {"$select": "id,changeKey,size,type",
                "$top": str(self.item_page_size)}

    # ── failure recording (per facet — logged now, counted at union) ──
    async def _record_failure(self, exc: httpx.HTTPStatusError, *, path: str,
                              fid: Any, is_top: bool, kind: str) -> None:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        if status in _WALK_FATAL_STATUSES:
            raise exc
        code = _graph_error_code(exc)
        rec = {
            "path": path, "folder_id": str(fid),
            "status": status, "code": code,
            "message": str(exc)[:300],
        }
        async with self.lock:
            self.raw_failures.append(rec)
            if kind == "items":
                self.items_failed.setdefault(path, rec)
            else:
                self.children_failed.setdefault(path, rec)
            if is_top:
                self.failed_top_ids.add(str(fid))
            count = len(self.raw_failures)
        print(f"[{self.worker_id}]   [ARCHIVE] folder {path!r} {kind} "
              f"unreadable at one location ({status} {code}) — recorded; "
              f"loss counted only if no other location serves it")
        if count > self.max_folder_failures:
            raise EnumerationFailed(
                f"archive walk aborted for {self.mbx}: {count} facet "
                f"failure(s), over the {self.max_folder_failures} cap "
                f"(last: {path} {status} {code})"
            )

    def _rebase(self, resp: Dict[str, Any], current: str, label: str) -> str:
        """Base for ids inside ``resp`` (see _base_from_odata_context). A
        case-only difference is NOT a relocation. Any genuinely different
        serving mailbox is also queued for a root walk of its own — the
        listings it serves inline carry only ITS residents."""
        derived = _base_from_odata_context(resp.get("@odata.context"), current)
        if derived.lower() == current.lower():
            return current
        print(f"[{self.worker_id}]   [ARCHIVE] {label} lives in an auxiliary "
              f"archive mailbox — re-basing its children onto "
              f"{derived.split('/folders')[0].rsplit('/', 1)[-1]}")
        self.note_base(derived)
        return derived

    # ── the walk ──────────────────────────────────────────────────────
    async def _process_folder(self, folder: Dict[str, Any], parent_path: str,
                              folders_base: str, *, is_top: bool = False):
        fid = folder.get("id")
        name = folder.get("displayName") or folder.get("name") or fid or ""
        path = f"{parent_path}/{name}" if parent_path else f"/{name}"
        async with self.lock:
            self.paths_seen.add(path)
            declared = folder.get("childFolderCount")
            if isinstance(declared, int):
                self.declared[path] = max(self.declared.get(path, 0), declared)
            self.children_seen.setdefault(path, set())

        try:
            async with self.sem:
                items_resp = await self.g._get(
                    f"{folders_base}/{fid}/items",
                    self._params_items(),
                    on_activity=self.on_progress,
                )
        except httpx.HTTPStatusError as exc:
            # Record the loss, then KEEP WALKING — the subtree stays
            # reachable via the parent's listing (2026-08-07→08-10: three
            # 500s silently dropped 343,154 items when this returned early).
            await self._record_failure(exc, path=path, fid=fid,
                                       is_top=is_top, kind="items")
            items_resp = {}
        else:
            async with self.lock:
                self.items_served[path] = max(
                    self.items_served.get(path, 0),
                    len(items_resp.get("value", []) or []),
                )
        page_items = items_resp.get("value", []) or []
        for it in page_items:
            it["_folder_path"] = path
            it["_folder_id"] = fid
            it["_mailbox_id"] = folders_base.split("/mailboxes/", 1)[-1].split("/", 1)[0]
        if page_items:
            async with self.lock:
                self.collected.extend(page_items)
        # Folder completion is still a progress stamp (cheap, honest), on
        # top of the per-page stamps _get fires via on_activity.
        if self.on_progress is not None:
            self.on_progress()

        # Recurse into child folders. Prefer the explicit count, but fall
        # back to always probing when the field is absent so we never miss
        # a subtree on a shape that omits childFolderCount.
        has_children = (
            folder.get("childFolderCount")
            or folder.get("childFolders")
            or "childFolderCount" not in folder
        )
        if not has_children:
            return
        try:
            async with self.sem:
                children_resp = await self.g._get(
                    f"{folders_base}/{fid}/childFolders",
                    self._params_folders(),
                    on_activity=self.on_progress,
                )
        except httpx.HTTPStatusError as exc:
            await self._record_failure(exc, path=path, fid=fid,
                                       is_top=False, kind="childFolders")
            return
        children = children_resp.get("value", []) or []
        async with self.lock:
            self.children_ok.add(path)
            self.children_seen.setdefault(path, set()).update(
                (c.get("displayName") or c.get("name") or str(c.get("id")))
                for c in children
            )
        declared = folder.get("childFolderCount")
        if isinstance(declared, int) and declared > len(children):
            # Per-facet shortfall: expected when the missing children are
            # resident in another location — log it, count it only if the
            # union still misses them (derive_failures).
            print(f"[{self.worker_id}]   [ARCHIVE] folder {path!r} "
                  f"childFolders short at this location — declared "
                  f"{declared}, got {len(children)}; union pending")
        child_base = self._rebase(children_resp, folders_base, f"folder {name!r}")
        if children:
            await asyncio.gather(
                *[self._process_folder(c, path, child_base) for c in children]
            )

    async def walk_base(self, folders_base: str, *, is_primary: bool = False) -> None:
        """Enumerate one mailbox location from its root."""
        norm = folders_base.rstrip("/").lower()
        if norm in self.walked_bases:
            return
        self.walked_bases.add(norm)
        loc = folders_base.split("/folders")[0].rsplit("/", 1)[-1]
        try:
            top_resp = await self.g._get(
                folders_base, self._params_folders(),
                on_activity=self.on_progress,
            )
        except httpx.HTTPStatusError as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if is_primary or status in _WALK_FATAL_STATUSES:
                raise
            # An aux root that won't list is coverage loss, not a walk-killer;
            # the union accounting + coverage gate surface what it cost.
            async with self.lock:
                rec = {
                    "kind": "location_root_unreadable",
                    "path": f"[{loc}]", "folder_id": "",
                    "status": status, "code": _graph_error_code(exc),
                    "message": str(exc)[:300],
                }
                self.raw_failures.append(rec)
                self.location_root_failures.append(rec)
            print(f"[{self.worker_id}]   [ARCHIVE] aux location {loc} root "
                  f"listing failed ({status}) — its residents are lost this run")
            return
        top = top_resp.get("value", []) or []
        top_base = self._rebase(top_resp, folders_base,
                                "the archive root" if is_primary else f"aux root {loc}")
        if is_primary:
            self.top_count = len(top)
        if top:
            await asyncio.gather(*[
                self._process_folder(f, "", top_base, is_top=is_primary)
                for f in top
            ])
        if is_primary and top and len(self.failed_top_ids) == len(top) and not self.collected:
            # Nothing captured anywhere is a failed run, never a PARTIAL — a
            # PARTIAL is offered as a restore point.
            raise EnumerationFailed(
                f"archive walk captured nothing for {self.mbx}: all "
                f"{len(top)} top-level folder(s) were unreadable"
            )

    async def drain(self) -> None:
        """Walk every queued location (discovered via re-basing or the
        exportItems location probe), including ones queued mid-drain."""
        while self.pending_bases:
            base = self.pending_bases.pop(0)
            await self.walk_base(base)

    async def run(self) -> None:
        await self.walk_base(self.primary_base, is_primary=True)
        await self.drain()

    # ── results ───────────────────────────────────────────────────────
    def items(self) -> List[Dict[str, Any]]:
        """Deduped enumeration (last occurrence wins — drift re-reads and
        facet overlap both trail the original, so later = freshest)."""
        deduped = {it.get("id"): it for it in self.collected}
        return list(deduped.values())

    def derive_failures(self) -> List[Dict[str, Any]]:
        """Union-level folder failures: one record per logical path that no
        walked location could serve."""
        failures: List[Dict[str, Any]] = []
        # A location whose ROOT would not list loses all its residents —
        # always a recorded coverage loss (finalizes PARTIAL).
        failures.extend(dict(rec) for rec in self.location_root_failures)
        for path, rec in sorted(self.items_failed.items()):
            if self.items_served.get(path, 0) > 0:
                continue  # another location actually served items here —
                          # a bare 200-EMPTY facet is NOT proof of service
            failures.append({**rec, "kind": "items_unreadable"})
        for path, rec in sorted(self.children_failed.items()):
            if path in self.children_ok:
                continue
            failures.append({**rec, "path": f"{path}/*",
                             "kind": "children_unreadable"})
        for path in sorted(self.declared):
            declared = self.declared[path]
            seen = len(self.children_seen.get(path) or ())
            if declared <= seen:
                continue
            if path in self.children_failed and path not in self.children_ok:
                continue  # already recorded as children_unreadable
            failures.append({
                "kind": "subtree_truncated",
                "path": f"{path}/*", "folder_id": "",
                "status": None, "code": "ChildFolderListTruncated",
                "declared_child_folders": declared,
                "returned_child_folders": seen,
                "message": (
                    f"childFolders truncated: Graph declared {declared} "
                    f"child folder(s), union across "
                    f"{len(self.walked_bases)} location(s) returned {seen}, "
                    f"no continuation"
                ),
            })
            print(f"[{self.worker_id}]   [ARCHIVE] folder {path!r} "
                  f"childFolders TRUNCATED after union — declared {declared}, "
                  f"got {seen} across {len(self.walked_bases)} location(s)")
        if self.skipped_bases:
            failures.append({
                "kind": "locations_over_cap",
                "path": "[locations]", "folder_id": "",
                "status": None, "code": "LocationCapExceeded",
                "message": (
                    f"{len(self.skipped_bases)} discovered mailbox "
                    f"location(s) not walked (ARCHIVE_MAX_LOCATIONS cap)"
                ),
            })
        return failures


async def _walk_folders(
    graph_client, base: str, mbx: str, *, concurrency: int, worker_id: str,
    max_folder_failures: Optional[int] = None,
    on_progress: Optional[Callable[[], None]] = None,
    folder_page_size: int = 200,
    item_page_size: int = 1000,
    max_locations: int = 8,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Compatibility wrapper over ``_ArchiveWalker`` — enumerate every
    location of the archive and return ``(deduped items, union failures)``."""
    if max_folder_failures is None:
        max_folder_failures = getattr(
            _walk_folders, "_default_max_folder_failures", 100,
        )
    walker = _ArchiveWalker(
        graph_client, base, mbx,
        concurrency=concurrency, worker_id=worker_id,
        max_folder_failures=max_folder_failures,
        max_locations=max_locations,
        folder_page_size=folder_page_size,
        item_page_size=item_page_size,
        on_progress=on_progress,
    )
    await walker.run()
    failures = walker.derive_failures()
    # The failure cap now applies to UNION-level losses (a facet failure
    # healed by another location is not a loss), so it is enforced here
    # rather than mid-walk for shortfalls.
    if len(failures) > max_folder_failures:
        raise EnumerationFailed(
            f"archive walk aborted for {mbx}: {len(failures)} folder(s) "
            f"unreadable/truncated after union, over the "
            f"{max_folder_failures} cap"
        )
    return walker.items(), failures


# ─────────────────── default DB/storage collaborators ────────────────────
#
# These mirror backup_mailbox's plumbing exactly (shard/container resolution,
# build_blob_path, upload_blob_with_retry, SnapshotItem construction) and the
# OneDrive FILE_VERSION carry-forward SQL. Imports are lazy so unit tests that
# inject fakes never load the DB engine / storage SDK / main.py.

def _make_default_persist(resource, snapshot, tenant, worker_id: str):
    import base64
    import binascii
    import hashlib
    from shared.config import settings

    meta = resource.extra_data or {}
    mailbox_id = meta.get("archiveMailboxId") or meta.get("archive_mailbox_id") or resource.external_id

    async def _persist(success_pairs: List[Tuple[Dict[str, Any], Dict[str, Any]]]) -> Tuple[int, int, int]:
        """Persist exportItems successes. ``success_pairs`` = list of
        (item_dict, entry_dict) where entry_dict carries the opaque ``data``.
        Returns (rows_stored, bytes_added, failed_uploads)."""
        from shared.azure_storage import (
            azure_storage_manager, upload_blob_with_dedup,
        )
        from shared.database import async_session_factory
        from shared.models import SnapshotItem

        shard = azure_storage_manager.get_shard_for_resource(
            str(resource.id), str(tenant.id),
        )
        # Dedicated "archive" workload container, namespaced further by
        # tenant/resource/snapshot via build_blob_path — same shape as the
        # mailbox path's per-item blob layout.
        container = azure_storage_manager.get_container_name(
            str(tenant.id), "archive",
        )

        upload_tasks = []
        metas = []
        failed = 0
        for it, entry in success_pairs:
            iid = it.get("id")
            data = entry.get("data")
            if data is None:
                failed += 1
                continue
            # Graph wraps the opaque stream in base64. Existing restore and
            # enrichment paths expect its decoded bytes and encode on import.
            try:
                blob_bytes = base64.b64decode(data, validate=True)
            except (ValueError, TypeError, binascii.Error):
                failed += 1
                print(f"[{worker_id}]   [ARCHIVE] Invalid export data for item {iid}")
                continue
            if not blob_bytes:
                failed += 1
                continue
            change_key = entry.get("changeKey") or it.get("changeKey")
            blob_path = azure_storage_manager.build_blob_path(
                str(tenant.id), str(resource.id), str(snapshot.id), iid,
            )
            content_hash = hashlib.sha256(blob_bytes).hexdigest()
            upload_tasks.append(upload_blob_with_dedup(
                tenant.id, content_hash, container, blob_path, blob_bytes, shard,
                max_retries=3,
            ))
            row = build_archive_item_row(
                snapshot_id=snapshot.id, tenant_id=tenant.id,
                folder={"id": it.get("_folder_id")},
                item={**it, "changeKey": change_key},
                blob_path=blob_path,
                content_hash=content_hash,
                blob_len=len(blob_bytes),
                mailbox_id=it.get("_mailbox_id") or mailbox_id,
                graph_version=settings.ARCHIVE_GRAPH_VERSION,
                folder_path=it.get("_folder_path"),
            )
            # Table insert uses the DB column name; retain both metadata keys
            # so existing local and customer snapshots share the same reader.
            row["metadata"] = row.pop("extra_data")
            row["metadata"].update({
                "changeKey": change_key, "graph_type": it.get("type"),
                "source": "archive",
            })
            row["backend_id"] = getattr(snapshot, "backend_id", None) or getattr(shard, "backend_id", None)
            metas.append(row)

        upload_results = await asyncio.gather(*upload_tasks, return_exceptions=True)

        rows = []
        bytes_added = 0
        for row, res in zip(metas, upload_results):
            if isinstance(res, dict) and res.get("success"):
                row["blob_path"] = res.get("blob_path") or row["blob_path"]
                rows.append(row)
                bytes_added += row["content_size"]
            else:
                # A failed blob upload must NOT be silently dropped — it counts
                # toward files_failed so the snapshot finalizes PARTIAL.
                failed += 1
                err = res.get("error", "unknown") if isinstance(res, dict) else res
                print(f"[{worker_id}]   [ARCHIVE] Upload FAILED for item {row['external_id']}: {err}")

        stored = len(rows)
        if rows:
            # ON CONFLICT DO NOTHING backstop: session.add_all() raises
            # UniqueViolation on uq_snapshot_items_snap_ext_type and rolls
            # back the WHOLE batch (killed run #6 2026-07-29; same class as
            # the 2026-05-13 attachment and 2026-07-21 sp_rest_row fixes).
            from sqlalchemy.dialects.postgresql import insert as _pg_insert
            stmt = _pg_insert(SnapshotItem.__table__).values(rows).on_conflict_do_nothing(
                index_elements=["snapshot_id", "external_id", "item_type"],
            )
            async with async_session_factory() as session:
                res = await session.execute(stmt)
                await session.commit()
                if getattr(res, "rowcount", None) is not None and res.rowcount >= 0:
                    stored = res.rowcount
        return stored, bytes_added, failed

    return _persist


async def _default_load_prev(
    resource_id: str, current_snapshot_id: str, *, lookback_days: int = 30,
) -> Tuple[Optional[str], Dict[str, Optional[str]]]:
    """Return (prior_snapshot_id, {external_id: changeKey}) from the BEST
    recent COMPLETED/PARTIAL snapshot's ARCHIVE_ITEM rows.

    "Best" = the item-richest snapshot inside ``lookback_days`` (falling
    back to the newest item-bearing one ever, for young resources). Using
    merely the NEWEST item-bearing snapshot made a truncated PARTIAL the
    changeKey baseline (ITO-10146): the ~600k items it was missing all
    looked "new" to the diff, forcing ~30k metered exportItems calls and
    ~15h of re-export per run, when a richer 13-day-old baseline could
    carry them forward for free. A superset baseline is always safe: a
    modified item's changeKey differs (→ export), a deleted item is not
    enumerated (→ neither carried nor exported)."""
    from sqlalchemy import text
    from shared.database import async_session_factory

    async with async_session_factory() as s:
        prior = (await s.execute(
            text(
                "SELECT id FROM snapshots s "
                " WHERE s.resource_id = :rid AND s.id <> :csid "
                "   AND s.status IN ('COMPLETED', 'PARTIAL') "
                # Gold dispatch mints 13ms COMPLETED "skip snapshots"
                # (0 items) for a resource whose run is already in
                # flight. Picking one as the baseline makes prev={} and
                # forces a FULL re-export of the whole archive on the
                # next run. The baseline must be a snapshot that
                # actually carries archive rows.
                "   AND EXISTS (SELECT 1 FROM snapshot_items si "
                "                WHERE si.snapshot_id = s.id "
                "                  AND si.item_type = 'ARCHIVE_ITEM') "
                " ORDER BY "
                "   (s.created_at > NOW() - make_interval(days => :days)) DESC, "
                "   s.item_count DESC NULLS LAST, "
                "   s.created_at DESC "
                " LIMIT 1"
            ),
            {"rid": resource_id, "csid": current_snapshot_id,
             "days": int(lookback_days)},
        )).first()
        if prior is None:
            return None, {}
        prior_snap_id = str(prior[0])
        rows = (await s.execute(
            text(
                "SELECT external_id, COALESCE(metadata->>'changeKey', "
                "metadata->>'change_key') AS change_key "
                "  FROM snapshot_items "
                " WHERE snapshot_id = :sid AND item_type = 'ARCHIVE_ITEM'"
            ),
            {"sid": prior_snap_id},
        )).all()
    return prior_snap_id, {r[0]: r[1] for r in rows if r[1] is not None}


async def _default_load_prior_paths(
    prior_snapshot_id: str,
) -> Dict[str, List[str]]:
    """{folder_path: [up to 3 sample external_ids]} from the prior snapshot's
    ARCHIVE_ITEM rows — feeds the location probe: for prior folders that no
    walked location lists any more, exportItems on a few known item ids makes
    Graph name the owning mailbox in the per-item redirect error."""
    from sqlalchemy import text
    from shared.database import async_session_factory

    async with async_session_factory() as s:
        rows = (await s.execute(
            text(
                "SELECT folder_path, (array_agg(external_id))[1:3] AS ids "
                "  FROM snapshot_items "
                " WHERE snapshot_id = :sid AND item_type = 'ARCHIVE_ITEM' "
                "   AND folder_path IS NOT NULL "
                " GROUP BY folder_path"
            ),
            {"sid": prior_snapshot_id},
        )).all()
    return {r[0]: list(r[1] or []) for r in rows}


async def _default_load_baseline(
    resource_id: str, current_snapshot_id: str, *, lookback_days: int = 30,
) -> int:
    """Best (max) item_count of any item-bearing COMPLETED/PARTIAL snapshot in
    the lookback window — the ABSOLUTE coverage baseline. Comparing only
    against the immediate prior let a truncated 46% PARTIAL become the
    baseline that scored the next truncated walk ~100% (ITO-10146)."""
    from sqlalchemy import text
    from shared.database import async_session_factory

    async with async_session_factory() as s:
        row = (await s.execute(
            text(
                "SELECT COALESCE(MAX(s.item_count), 0) FROM snapshots s "
                " WHERE s.resource_id = :rid AND s.id <> :csid "
                "   AND s.status IN ('COMPLETED', 'PARTIAL') "
                "   AND s.created_at > NOW() - make_interval(days => :days) "
                "   AND EXISTS (SELECT 1 FROM snapshot_items si "
                "                WHERE si.snapshot_id = s.id "
                "                  AND si.item_type = 'ARCHIVE_ITEM')"
            ),
            {"rid": resource_id, "csid": current_snapshot_id,
             "days": int(lookback_days)},
        )).first()
    return int(row[0] or 0) if row else 0


async def _probe_missing_locations(
    graph_client, export_url: str, id_batches: List[List[str]], *,
    timeout: float, worker_id: str,
    on_activity: Optional[Callable[[], None]] = None,
) -> "set[str]":
    """Best-effort auxiliary-location discovery (ITO-10146): POST a few
    prior-snapshot item ids to exportItems and harvest the owning mailbox
    from each per-item ``ErrorArchiveFolderMovedPermanently`` reissue URL.
    Returns folder-base URLs. Never raises — the probe failing must not cost
    the run; it only costs recovered coverage.

    ``on_activity`` fires after every batch (returned OR failed): each POST
    may honestly burn its full retry budget, and several silent batches
    back-to-back would otherwise eat into the stall guard's window."""
    bases: "set[str]" = set()
    for ids in id_batches:
        try:
            resp = await graph_client._post_json(
                export_url, {"itemIds": ids}, timeout=timeout,
            )
        except Exception as e:
            print(f"[{worker_id}]   [ARCHIVE] location probe call failed "
                  f"({type(e).__name__}: {e}) — continuing")
            if on_activity is not None:
                on_activity()
            continue
        if on_activity is not None:
            on_activity()
        value = resp.get("value", []) if isinstance(resp, dict) else []
        for entry in value:
            err = entry.get("Error") or entry.get("error")
            if not err:
                continue
            url = _export_redirect_url(err)
            if not url:
                continue
            m = _MBX_BASE_RE.search(url)
            if m:
                bases.add(f"{m.group(1)}/folders")
    return bases


async def _default_carry_forward(
    target_snapshot_id: str, prior_snapshot_id: str, external_ids: List[str],
    on_wait: Optional[Callable[[], None]] = None,
) -> int:
    """Clone unchanged ARCHIVE_ITEM rows from the prior snapshot into this one
    with their enriched attachment rows (blob reused — no Graph, no upload).
    The return count excludes attachments so archive inventory stays comparable.
    Idempotent via NOT EXISTS on
    (snapshot_id, external_id, item_type). Mirrors
    ``_carry_forward_prior_file_versions`` (main.py).

    ``on_wait`` is ticked once per missed slot attempt so the archive stall
    guard counts queueing for a carry-forward slot as liveness, not a stall
    (pre-2026-08-24 the blocking slot wait emitted no progress AND pinned a
    pgbouncer server connection — see shared.database.carry_forward_slot_session)."""
    if not (target_snapshot_id and prior_snapshot_id and external_ids):
        return 0
    from sqlalchemy import text
    from shared.database import carry_forward_slot_session

    async with carry_forward_slot_session(
        str(target_snapshot_id), lane="giant", on_wait=on_wait, what="archive carry-forward",
    ) as s:
        result = await s.execute(
            text(
                "WITH carried AS (INSERT INTO snapshot_items ("
                "  id, snapshot_id, tenant_id, item_type, external_id, "
                "  parent_external_id, name, folder_path, content_hash, "
                "  content_checksum, content_size, blob_path, "
                "  encryption_key_id, backup_version, metadata, is_deleted, "
                "  indexed_at, backend_id, created_at"
                ") "
                "SELECT gen_random_uuid(), :tsid, src.tenant_id, src.item_type, "
                "       src.external_id, src.parent_external_id, src.name, "
                "       src.folder_path, src.content_hash, src.content_checksum, "
                "       src.content_size, src.blob_path, src.encryption_key_id, "
                "       COALESCE(src.backup_version, 1), src.metadata, "
                "       COALESCE(src.is_deleted, false), src.indexed_at, "
                "       src.backend_id, NOW() "
                "  FROM snapshot_items src "
                " WHERE src.snapshot_id = :psid "
                "   AND ((src.item_type = 'ARCHIVE_ITEM' "
                "         AND src.external_id = ANY(:ids)) "
                "     OR (src.item_type = 'ARCHIVE_ATTACHMENT' "
                "         AND src.parent_external_id = ANY(:ids))) "
                "   AND NOT EXISTS ("
                "         SELECT 1 FROM snapshot_items dst "
                "          WHERE dst.snapshot_id = :tsid "
                "            AND dst.external_id = src.external_id "
                "            AND dst.item_type = src.item_type"
                "       ) RETURNING item_type) "
                "SELECT COUNT(*) FROM carried WHERE item_type = 'ARCHIVE_ITEM'"
            ),
            {"tsid": target_snapshot_id, "psid": prior_snapshot_id, "ids": external_ids},
        )
        await s.commit()
        return int(result.scalar() or 0)


# ─────────────────────────── orchestrator ────────────────────────────────

async def run_archive_backup(
    graph_client,
    resource,
    snapshot,
    tenant,
    *,
    worker_id: str = "archive",
    force_full: bool = False,
    settings=None,
    load_prev: Optional[Callable] = None,
    persist_batch: Optional[Callable] = None,
    carry_forward: Optional[Callable] = None,
    load_prior_paths: Optional[Callable] = None,
    load_baseline: Optional[Callable] = None,
) -> Dict[str, Any]:
    """Back up one USER_ARCHIVE resource. Returns the standard handler result
    dict (``item_count`` / ``bytes_added`` / ``new_delta_token``) consumed by
    ``complete_snapshot``. On any Graph transport error raises
    ``EnumerationFailed`` (caller → FAILED). On per-item export/upload failures
    it stamps ``snapshot.delta_tokens_json['files_failed']`` so
    ``complete_snapshot`` finalizes PARTIAL rather than COMPLETED."""
    if settings is None:
        from shared.config import settings as settings  # noqa: PLW0127
    _default_db_collabs = load_prev is None
    if load_prev is None:
        async def load_prev(rid: str, csid: str):  # noqa: PLW0127
            return await _default_load_prev(
                rid, csid,
                lookback_days=getattr(
                    settings, "ARCHIVE_BASELINE_LOOKBACK_DAYS", 30,
                ),
            )
    if carry_forward is None:
        carry_forward = _default_carry_forward
    if persist_batch is None:
        persist_batch = _make_default_persist(resource, snapshot, tenant, worker_id)
    # The DB-backed defaults for the two ITO-10146 collaborators are wired
    # only when load_prev is ALSO the DB default: a test (or caller) that
    # injects a stub load_prev has no DB, and silently reaching for one here
    # would crash it. Prod always takes the default path.
    if load_prior_paths is None and _default_db_collabs:
        load_prior_paths = _default_load_prior_paths
    if load_baseline is None and _default_db_collabs:
        async def load_baseline(rid: str, csid: str) -> int:  # noqa: PLW0127
            return await _default_load_baseline(
                rid, csid,
                lookback_days=getattr(
                    settings, "ARCHIVE_BASELINE_LOOKBACK_DAYS", 30,
                ),
            )

    # {base} is version-driven — never hardcode "beta" (GA is a config flip).
    base = f"https://graph.microsoft.com/{settings.ARCHIVE_GRAPH_VERSION}"
    mbx = ((resource.extra_data or {}).get("archiveMailboxId")
           or (resource.extra_data or {}).get("archive_mailbox_id")
           or resource.external_id)
    export_url = f"{base}/admin/exchange/mailboxes/{mbx}/exportItems"

    print(f"[{worker_id}] [ARCHIVE START] {getattr(resource, 'display_name', mbx)} (mbx={mbx})")

    sem = asyncio.Semaphore(max(1, settings.ARCHIVE_EXPORT_CONCURRENCY))

    # ── Progress stamp ──────────────────────────────────────────────
    # Monotonic "the work moved" marker, read by the stall guard below.
    # Deliberately NOT a DB write: the whole point is to detect the case
    # where DB writes have stopped.
    _progress = {"at": time.monotonic(), "n": 0}

    def _mark_progress() -> None:
        _progress["n"] += 1
        _progress["at"] = time.monotonic()

    async def _export_chunk(
        chunk: List[Dict[str, Any]],
    ) -> Tuple[List[Tuple[Dict[str, Any], Dict[str, Any]]], int]:
        """Export one ≤20-id chunk, following per-item exportItems redirects up
        to ``MAX_EXPORT_HOPS``. Returns (success_pairs, failed_count).
        ``success_pairs`` = list of (item, success_entry)."""
        success_pairs: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
        failed = 0
        # frontier maps a POST url → the items to (re)issue against it.
        frontier: Dict[str, List[Dict[str, Any]]] = {export_url: list(chunk)}
        hop = 0
        while frontier:
            next_frontier: Dict[str, List[Dict[str, Any]]] = {}
            for url, items in frontier.items():
                async with sem:
                    resp = await graph_client._post_json(
                        url,
                        {"itemIds": [it.get("id") for it in items]},
                        timeout=settings.ARCHIVE_IMPORT_TIMEOUT_S,
                    )
                # Stamp on every returned POST, not just on chunk completion:
                # a chunk that is retrying (and therefore genuinely talking to
                # Graph) must not look like a stall.
                _mark_progress()
                value = resp.get("value", []) if isinstance(resp, dict) else []
                by_id = {e.get("itemId"): e for e in value}
                for it in items:
                    entry = by_id.get(it.get("id"))
                    if entry is None:
                        failed += 1
                        continue
                    # 200-OK response: entry is EITHER a success (has data) OR
                    # a per-item error (capital-E Error).
                    err = entry.get("Error") or entry.get("error")
                    if err:
                        reissue = _export_redirect_url(err)
                        if reissue and hop < MAX_EXPORT_HOPS:
                            next_frontier.setdefault(reissue, []).append(it)
                        else:
                            # Redirect past the hop cap, or any other per-item
                            # error code → failed item (PARTIAL, not dropped).
                            failed += 1
                    elif entry.get("data") is not None:
                        success_pairs.append((it, entry))
                    else:
                        failed += 1
            frontier = next_frontier
            hop += 1
        return success_pairs, failed

    async def _run_chunk(chunk: List[Dict[str, Any]]) -> Tuple[int, int, int]:
        try:
            success_pairs, chunk_failed = await _export_chunk(chunk)
        except (httpx.HTTPError, asyncio.TimeoutError, ValueError) as e:
            # One chunk exhausting _post_json's retries must cost only its
            # ≤20 items (files_failed → PARTIAL) — not the whole multi-hour
            # walk (run #7 2026-07-30 died on a single retry-exhausted
            # exportItems 504). The failed items have no row in this
            # snapshot, so the next run's changeKey diff re-selects them.
            #
            # ValueError covers json.JSONDecodeError (its subclass): a
            # truncated/corrupt exportItems body — run #9 (2026-08-04)
            # died at 81% (898k/1.11M items, ~18h in) parsing a ~31MB
            # response for one giant item. That kill is DETERMINISTIC
            # per item, so without this catch every retry re-pays the
            # full walk and dies at the same spot; with it the ≤20-item
            # chunk goes to files_failed and the run finalizes PARTIAL.
            print(f"[{worker_id}]   [ARCHIVE] export chunk FAILED "
                  f"({len(chunk)} item(s)): {type(e).__name__}: {e}")
            return 0, 0, len(chunk)
        # Persist OUTSIDE the semaphore so blob upload / DB write doesn't hold
        # a Graph-export slot.
        stored, added, upload_failed = await persist_batch(success_pairs)
        _mark_progress()
        return stored, added, chunk_failed + upload_failed

    # ── Snapshot liveness guard ─────────────────────────────────────
    # The walk runs for hours with ZERO snapshot-row writes, so the
    # worker cannot notice when its snapshot row is deleted or flipped
    # terminal underneath it (run #8, 2026-07-31: the cancelled-sweep
    # deleted the row ≤30s in; the worker walked ~1.1M items blind for
    # 7h and died on the first item INSERT's FK). This probe re-asserts
    # the row every ~2 min — the UPDATE doubles as a lease heartbeat
    # keyed on snapshot ID (owner-independent, so a resumed run is
    # covered too) — and aborts the run fast when the row is gone.
    _liveness_lost = asyncio.Event()
    _stalled = asyncio.Event()
    _victim = asyncio.current_task()

    async def _liveness_guard() -> None:
        from sqlalchemy import text as _lt
        from shared.database import async_session_factory as _lsf
        probe_s = float(getattr(settings, "ARCHIVE_LIVENESS_PROBE_S", 120))
        ttl_s = int(getattr(settings, "ARCHIVE_LIVENESS_TTL_S", 600))
        # ── Stall abort ─────────────────────────────────────────────
        # The probe above proves the ROW is alive; the worker's
        # LEASE_EXTENDER proves the PROCESS is alive. Neither proves the
        # WORK is alive, and on 2026-08-12 that gap cost 15.6 h: snapshot
        # 0e423f79 stopped writing at 02:16:36 UTC with 188,380 of 340,258
        # items done, and stayed RUNNING because both liveness signals kept
        # reporting healthy. The reconciler only reclaims on an EXPIRED
        # lease, so nothing on the fleet could ever have noticed.
        #
        # Threshold: one chunk may honestly burn ARCHIVE_IMPORT_TIMEOUT_S
        # (600 s) on each of max_retries (5) attempts, plus a clamped 120 s
        # Retry-After per round ⇒ worst honest gap ≈ 3,600 s. 5,400 s sits
        # above that with margin. Set to 0 to disable.
        stall_s = float(getattr(settings, "ARCHIVE_STALL_ABORT_S", 5400))
        while True:
            await asyncio.sleep(probe_s)
            if stall_s > 0:
                _idle = time.monotonic() - _progress["at"]
                if _idle > stall_s:
                    print(
                        f"[{worker_id}]   [ARCHIVE] STALL: no progress for "
                        f"{_idle:.0f}s (limit {stall_s:.0f}s, "
                        f"{_progress['n']} progress event(s) total) — "
                        f"aborting snapshot {snapshot.id} so it fails "
                        f"honestly instead of holding a live lease forever"
                    )
                    _stalled.set()
                    if _victim is not None:
                        _victim.cancel()
                    return
            try:
                async with _lsf() as _ls:
                    _res = await _ls.execute(
                        _lt(
                            "UPDATE snapshots "
                            "   SET lease_expires_at = GREATEST("
                            "         COALESCE(lease_expires_at, NOW()), "
                            "         NOW() + make_interval(secs => :ttl)) "
                            " WHERE id = :sid AND status = 'IN_PROGRESS'"
                        ),
                        {"sid": str(snapshot.id), "ttl": ttl_s},
                    )
                    await _ls.commit()
                if not _res.rowcount:
                    print(
                        f"[{worker_id}]   [ARCHIVE] liveness probe: snapshot "
                        f"{snapshot.id} row gone or no longer IN_PROGRESS — "
                        f"aborting the run instead of walking blind"
                    )
                    _liveness_lost.set()
                    if _victim is not None:
                        _victim.cancel()
                    return
            except Exception as _lx:
                # Transient DB blips must not kill a healthy run; the
                # normal lease extender still covers the row.
                print(f"[{worker_id}]   [ARCHIVE] liveness probe error "
                      f"(transient): {type(_lx).__name__}: {_lx}")

    _guard = asyncio.create_task(_liveness_guard())

    try:
        walker = _ArchiveWalker(
            graph_client, base, mbx,
            concurrency=settings.ARCHIVE_FOLDER_CONCURRENCY,
            worker_id=worker_id,
            max_folder_failures=getattr(
                settings, "ARCHIVE_MAX_FOLDER_FAILURES", 100,
            ),
            max_locations=getattr(settings, "ARCHIVE_MAX_LOCATIONS", 8),
            folder_page_size=getattr(settings, "ARCHIVE_FOLDER_PAGE_SIZE", 200),
            item_page_size=getattr(settings, "ARCHIVE_ITEM_PAGE_SIZE", 1000),
            on_progress=_mark_progress,
        )
        await walker.run()
        items = walker.items()

        prior_snap_id, prev = await load_prev(str(resource.id), str(snapshot.id))

        # ── Location probe ───────────────────────────────────────────────
        # Aux mailboxes are only discoverable organically. For prior-snapshot
        # folders that NO walked location lists any more, spend a few
        # exportItems calls on known item ids: the per-item
        # ErrorArchiveFolderMovedPermanently reissue URL names the owning
        # mailbox, whose root is then walked and unioned like any other.
        _probe_calls = int(getattr(settings, "ARCHIVE_LOCATION_PROBE_CALLS", 3))
        if prior_snap_id and _probe_calls > 0 and load_prior_paths is not None:
            try:
                _prior_paths = await load_prior_paths(prior_snap_id)
            except Exception as _pe:
                print(f"[{worker_id}]   [ARCHIVE] prior-path load failed "
                      f"({type(_pe).__name__}: {_pe}) — probe skipped")
                _prior_paths = {}
            _enumerated_ids = {it.get("id") for it in items}
            # A path is a probe candidate when NO walked location lists it,
            # OR it is listed but none of its sampled prior items were
            # enumerated — a facet answering 200-EMPTY while the resident
            # location is undiscovered looks "seen" by path alone (review
            # finding: listed-but-emptied folders never probed).
            _missing = {
                p: ids for p, ids in _prior_paths.items()
                if p not in walker.paths_seen
                or not any(i in _enumerated_ids for i in ids if i)
            }
            if _missing:
                _sample: List[str] = []
                # Shallow paths first: one id from a top-level subtree
                # locates the same mailbox as fifty from its leaves.
                for _p in sorted(_missing, key=lambda x: (x.count("/"), x)):
                    for _iid in _missing[_p]:
                        if _iid and _iid not in _enumerated_ids:
                            _sample.append(_iid)
                        if len(_sample) >= _probe_calls * BATCH:
                            break
                    if len(_sample) >= _probe_calls * BATCH:
                        break
                if _sample:
                    _batches = list(_chunks(_sample, BATCH))[:_probe_calls]
                    print(f"[{worker_id}]   [ARCHIVE] {len(_missing)} prior "
                          f"folder path(s) unlisted by all "
                          f"{len(walker.walked_bases)} walked location(s) — "
                          f"probing {len(_batches)} exportItems call(s) for "
                          f"their owning mailbox")
                    _new_bases = await _probe_missing_locations(
                        graph_client, export_url, _batches,
                        timeout=settings.ARCHIVE_IMPORT_TIMEOUT_S,
                        worker_id=worker_id,
                        on_activity=_mark_progress,
                    )
                    _mark_progress()
                    _fresh = [b for b in _new_bases
                              if b.rstrip("/").lower() not in walker.walked_bases]
                    if _fresh:
                        print(f"[{worker_id}]   [ARCHIVE] probe discovered "
                              f"{len(_fresh)} new location(s) — walking them")
                        for _b in _fresh:
                            walker.note_base(_b)
                        await walker.drain()
                        items = walker.items()

        folder_failures = walker.derive_failures()
        _max_ff = getattr(settings, "ARCHIVE_MAX_FOLDER_FAILURES", 100)
        if len(folder_failures) > _max_ff:
            raise EnumerationFailed(
                f"archive walk aborted for {mbx}: {len(folder_failures)} "
                f"folder(s) unreadable/truncated after union, over the "
                f"{_max_ff} cap"
            )
        print(f"[{worker_id}]   [ARCHIVE] Enumerated {len(items)} item(s) "
              f"across {len(walker.walked_bases)} mailbox location(s)")
        if folder_failures:
            print(f"[{worker_id}]   [ARCHIVE] "
                  f"{len(folder_failures)} folder(s) unreadable/truncated "
                  f"after union — snapshot will finalize PARTIAL: "
                  + ", ".join(
                      f"{f['path']} ({f.get('status')} {f.get('code')})"
                      for f in folder_failures[:10]
                  ))

        # ── Coverage gate (ABSOLUTE baseline — ITO-10146) ────────────────
        # Refuse to build a restore point from a walk that enumerated
        # materially less than the best recent capture. The archive walk has
        # no delta cursor — every run re-enumerates the whole tree — so a
        # big shortfall means the TREE got truncated, not that mail was
        # deleted. The baseline is max(immediate prior, best item-bearing
        # snapshot in ARCHIVE_BASELINE_LOOKBACK_DAYS): comparing only against
        # the immediate prior let a truncated 46% PARTIAL become the
        # baseline that scored the NEXT truncated walk ~100%, so four
        # identical 515k/1.11M captures each passed as the SLA-current
        # restore point (2026-08-07→08-11).
        #
        # Failing beats a plausible-looking PARTIAL: the prior good snapshot
        # stays the newest usable restore point (retention pins it), and a
        # FAILED run is visible. Set ARCHIVE_MIN_COVERAGE_PCT=0 to disable
        # if a mailbox is ever legitimately bulk-purged.
        _baseline_n = len(prev)
        if load_baseline is not None:
            try:
                _baseline_n = max(
                    _baseline_n,
                    int(await load_baseline(
                        str(resource.id), str(snapshot.id),
                    ) or 0),
                )
            except Exception as _be:
                print(f"[{worker_id}]   [ARCHIVE] baseline load failed "
                      f"({type(_be).__name__}: {_be}) — gate falls back to "
                      f"the immediate prior ({len(prev)} items)")
        _min_cov = getattr(settings, "ARCHIVE_MIN_COVERAGE_PCT", 90)
        _cov_pct = (100.0 * len(items) / _baseline_n) if _baseline_n else None
        if _baseline_n and _min_cov > 0:
            if _cov_pct < _min_cov:
                # Deterministic per tree state: an immediate retry walks the
                # same folders and fails the same gate. Marked non-retryable
                # so the AUTO_RETRY sweep doesn't burn walks on it
                # (fail_snapshot stamps extra_data['retryable']=false).
                _gate_exc = EnumerationFailed(
                    f"archive walk for {mbx} enumerated {len(items)} item(s) "
                    f"vs baseline {_baseline_n} "
                    f"(best of prior snapshot {prior_snap_id} = {len(prev)} "
                    f"and the {getattr(settings, 'ARCHIVE_BASELINE_LOOKBACK_DAYS', 30)}-day max) "
                    f"({_cov_pct:.1f}% coverage, floor {_min_cov}%) — "
                    f"refusing to finalize a partial restore point. "
                    f"{len(folder_failures)} folder failure(s) recorded: "
                    f"{[f.get('path') for f in folder_failures[:5]]}"
                )
                _gate_exc.retryable = False
                raise _gate_exc

        todo = list(items) if force_full else _select_items_to_export(items, prev)
        todo_ids = {it.get("id") for it in todo}
        # Carry forward only unchanged items that actually have a prior row.
        unchanged_ids = [
            it.get("id") for it in items
            if it.get("id") not in todo_ids and it.get("id") in prev
        ]
        print(f"[{worker_id}]   [ARCHIVE] {len(todo)} to export, "
              f"{len(unchanged_ids)} unchanged (carry-forward), "
              f"prior_snapshot={prior_snap_id}")

        succeeded = 0
        bytes_added = 0
        failed_items = 0
        total_to_export = len(todo)

        if todo:
            # Real bounded concurrency: one task per 20-id chunk, gated by the
            # semaphore inside _export_chunk. On the FIRST transport failure,
            # cancel the siblings so no more rows commit under a doomed run.
            tasks = [
                asyncio.ensure_future(_run_chunk(chunk))
                for chunk in _chunks(todo, BATCH)
            ]
            try:
                results = await asyncio.gather(*tasks)
            except BaseException:
                for t in tasks:
                    if not t.done():
                        t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise
            for stored, added, chunk_failed in results:
                succeeded += stored
                bytes_added += added
                failed_items += chunk_failed

        carried = 0
        if prior_snap_id and unchanged_ids:
            if carry_forward is _default_carry_forward:
                # Queueing for a carry-forward slot is liveness, not a
                # stall — tick the progress stamp per missed attempt so
                # the stall guard doesn't abort a healthily-queued run.
                # (Injected test doubles keep the plain 3-arg call.)
                carried = await carry_forward(
                    str(snapshot.id), prior_snap_id, unchanged_ids,
                    on_wait=_mark_progress,
                )
            else:
                carried = await carry_forward(
                    str(snapshot.id), prior_snap_id, unchanged_ids,
                )

    except EnumerationFailed:
        raise
    except asyncio.CancelledError:
        # Distinguish the guard's deliberate aborts from a real task
        # cancellation (shutdown): the guard sets the matching event first.
        if _stalled.is_set():
            raise EnumerationFailed(
                f"archive run aborted: no progress for over "
                f"{float(getattr(settings, 'ARCHIVE_STALL_ABORT_S', 5400)):.0f}s "
                f"(stall guard) — snapshot {snapshot.id} failed instead of "
                f"holding a live lease indefinitely"
            ) from None
        if _liveness_lost.is_set():
            raise EnumerationFailed(
                f"archive run aborted: snapshot {snapshot.id} row was "
                f"deleted or flipped terminal mid-run (liveness probe)"
            ) from None
        raise
    except (httpx.HTTPError, asyncio.TimeoutError) as e:
        raise EnumerationFailed(
            f"archive enumeration/export failed for {mbx}: {type(e).__name__}: {e}"
        ) from e
    except Exception as e:
        # Graph throttle-exhaustion and similar client errors surface with
        # provider-specific types; treat them as enumeration failures too so
        # the snapshot fails honestly rather than completing empty.
        if type(e).__name__ in ("GraphRetryExhaustedError", "GraphError"):
            raise EnumerationFailed(
                f"archive enumeration/export failed for {mbx}: {type(e).__name__}: {e}"
            ) from e
        raise
    finally:
        _guard.cancel()

    item_count = succeeded + carried

    # PARTIAL-finalize convention (complete_snapshot reads delta_tokens_json):
    # any failed export item or failed blob upload ⇒ files_failed>0 ⇒ PARTIAL.
    # Zero failures ⇒ files_failed absent/0 ⇒ COMPLETED (incl. legit empty).
    # An unreadable FOLDER is coverage loss too, so it must also force PARTIAL:
    # complete_snapshot keys the PARTIAL flip solely off `files_failed`, so the
    # folder count is added there (and reported separately in `folders_failed`
    # so the two units stay distinguishable for diagnosis).
    # Coverage numbers are stamped on EVERY run (ITO-10146): a clean-looking
    # run with a shrunken enumeration is exactly the case the operator needs
    # numbers for, and stamping only on failure hid four identical 46%
    # captures in a row.
    existing = dict(getattr(snapshot, "delta_tokens_json", None) or {})
    existing["items_enumerated"] = len(items)
    existing["locations_walked"] = len(walker.walked_bases)
    if _baseline_n:
        existing["items_baseline"] = _baseline_n
        existing["coverage_pct"] = round(_cov_pct, 1)
    if failed_items or folder_failures:
        existing.update({
            "files_total": total_to_export,
            "files_succeeded": succeeded,
            "files_failed": failed_items + len(folder_failures),
        })
        if folder_failures:
            existing["folders_failed"] = len(folder_failures)
            existing["folder_failures"] = folder_failures[:20]
            # Coverage, not just failure counts. `folders_failed` counts
            # ENTRY POINTS, so it read 3 while ~48 folders had produced
            # nothing and 599,475 items were missing — a 75x under-report
            # that let the regression run 4 times unnoticed. These make the
            # shortfall visible without an item-id set diff.
            _truncated = [f for f in folder_failures
                          if f.get("kind") == "subtree_truncated"]
            if _truncated:
                existing["subtrees_truncated"] = len(_truncated)
                existing["child_folders_unreachable"] = sum(
                    (f.get("declared_child_folders") or 0)
                    - (f.get("returned_child_folders") or 0)
                    for f in _truncated
                )
        print(f"[{worker_id}]   [ARCHIVE] {failed_items} item(s) and "
              f"{len(folder_failures)} folder(s) failed — "
              f"snapshot will finalize PARTIAL")
    snapshot.delta_tokens_json = existing

    print(f"[{worker_id}] [ARCHIVE COMPLETE] {getattr(resource, 'display_name', mbx)} — "
          f"{item_count} item(s) ({carried} carried forward, {failed_items} failed), "
          f"{bytes_added} bytes")
    return {
        "item_count": item_count,
        "bytes_added": bytes_added,
        "new_delta_token": None,
        "files_total": total_to_export,
        "files_succeeded": succeeded,
        "files_failed": failed_items,
        "archive_carried_forward": carried,
    }
