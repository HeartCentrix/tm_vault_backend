"""Cleanup of partial-backup artifacts when a job is cancelled.

When a user / admin cancels a running backup job, the worker stops
pulling new items from Graph but the items it had already written to
blob storage remain. Without this module those blobs would orphan: the
DB-side ``DROP SCHEMA`` (or a manual cleanup) eventually wipes the
``snapshot_items`` rows, but the underlying object-store bytes linger
and consume disk indefinitely.

``cleanup_cancelled_snapshots`` deletes the bytes + DB rows in one
pass:

  1. Find every snapshot for the job whose status is IN_PROGRESS or
     COMPLETED (the worker may have flushed a few entries before the
     cancel signal landed).
  2. Stream-delete every snapshot_item's blob via the storage abstraction
     (works for Azure Blob and SeaweedFS uniformly), skipping any blob
     another snapshot still references — see "Reference safety" below.
  3. Delete attachment blobs referenced by ``extra_data.attachment_blob_paths``.
  4. Delete the snapshot_items rows.
  5. Mark the snapshot as CANCELLED.

Steps 2-5 are all skipped for a snapshot that is still the reuse-chain
root of a LIVE descendant; instead it is HELD
(``hold_protected_reuse_roots``) so the unguarded backup-scheduler
sweep can't finish the job 30 seconds later — see step 3's note in
``cleanup_cancelled_snapshots``.

Idempotent: re-running on an already-cleaned job is a no-op.  Safe to
call from the worker mid-shutdown OR from a separate cancel endpoint.

Soft failure model: per-blob deletion errors are logged + counted.
We do NOT abort cleanup on a single missing/permission-denied blob —
the goal is to free as many bytes as we can. Hard storage errors
(connection refused) abort the run so the caller can retry.

Failure model for the GUARD is the opposite: fail closed. If the
reuse-chain protection set cannot be computed, nothing is deleted at
all — not the blobs, not the ``snapshot_items`` rows, not the status
flip — and ``BlobCleanupGuardUnavailable`` is raised. A per-batch
sharer-probe failure retains that batch's bytes (the row reap for
non-protected doomed snapshots stays safe either way).

Reference safety
----------------
A snapshot_item's ``blob_path`` is NOT proof that this snapshot owns
the bytes. Content-hash dedup (``shared.blob_dedup``) points a new
item at a blob an older, still-retained snapshot uploaded, and Teams
message blobs are content-addressed and shared across users outright.
Deleting those bytes leaves the retained snapshot unrestorable — data
loss discovered only at restore time. So every candidate path goes
through ``select_deletable_blob_paths`` before deletion and anything
uncertain is RETAINED. A leaked orphan blob costs storage; a deleted
shared blob costs customer data.

That guard is deliberately NOT private to this module. This function
is only one of the teardown paths: ``cancel_job`` (services/job-service)
flips in-flight snapshots to FAILED + ``extra_data.cancelled_at`` and
the reaping is owned by ``_sweep_cancelled_snapshots`` in
services/backup-scheduler (every 30s), which reads
``snapshot_items`` directly. See ``select_deletable_blob_paths`` for
the contract both call sites share.

That sweep is the path most cancelled bytes actually die on, so it now
runs the guard too, via ``teardown_cancelled_sweep_blobs`` — the same
adoption sequence, packaged, and aggregated so its cost does not scale
with the number of snapshots in a pass:

  1. ``roots = await resolve_protected_reuse_roots(session, doomed)``
     ONCE per sweep pass, with the whole candidate batch's ids.
  2. ``await hold_protected_reuse_roots(session, roots)`` — then skip
     those snapshots ENTIRELY (no blob deletes, no
     ``DELETE FROM snapshot_items``, no ``DELETE FROM snapshots``).
     Without the hold the sweep re-selects them every 30s forever.
  3. ``stream_sweep_blob_candidates`` ONCE for the whole batch, through a
     server-side cursor in ``_SWEEP_ROW_BATCH`` chunks. Per chunk:
     ``select_deletable_blob_paths(..., protected_roots=roots)`` →
     ``invalidate_chat_url_cache_paths`` (BEFORE the deletes) → delete
     only ``plan.deletable``.
  4. ONE aggregated ``select_deletable_blob_paths`` at the end of the
     pass for the checksum-less residue chunk 3 held back, so the whole
     pass buys AT MOST ONE unindexed ``blob_path`` scan.

Steps 3 and 4 are split because ``unkeyed_probe_budget`` bounds PATHS,
not SCANS. The first version of this helper looped per snapshot, so a
200-snapshot pass issued up to 200 separate ``blob_path IN (...)``
statements — 200 full sequential scans of the 87GB heap — while still
"respecting" a 500-path budget. Streaming also caps peak memory: on a
cancelled 200k-item snapshot the per-snapshot shape peaked at ~282MiB of
candidates+plan inside a 512Mi pod (measured), against ~0.6MiB streamed
(~3MiB when every row is checksum-less).

Passes do not overlap. The sweep interval is 30 SECONDS and a pass does
a blob delete per snapshot_item, so ``teardown_cancelled_sweep_blobs``
refuses to start while another pass is in flight and returns
``SweepBlobTeardown(skipped=True)``; backup-scheduler carries the same
brace one level up plus ``max_instances=1, coalesce=True``.

The sweep keeps ownership of its own row DELETEs and only applies them
to ``SweepBlobTeardown.reapable`` — never to a held reuse root (its
descendant resolves through those rows) and never to a snapshot whose
guard raised (deleting the rows would orphan the retained bytes with
nothing left to find them, and no later pass could retry).

Nothing in this module makes an un-adopted caller worse either: the
snapshots it protects are moved out of every sweep population, and the
snapshots it reaps are gone before the sweep sees them.

Known residual: the in-flight dedup race
----------------------------------------
The guard proves "no live referrer" as of the instant it runs. A
backup for a DIFFERENT resource in the same tenant can resolve a
dedup hit against a doomed snapshot's row (``find_existing_blob`` is
tenant-scoped, not job-scoped) and flush its ``snapshot_items`` row
AFTER the probe and BEFORE/AFTER the delete — that row then points at
bytes we removed. The window is the probe→delete gap, and the pre-fix
behaviour lost those bytes 100% of the time, so this narrows the hole
rather than closing it. Closing it needs a tombstone protocol
(mark doomed rows PENDING_DELETION, have blob_dedup exclude them, and
delete only after a quiescence interval) — a design change, not a
patch, and deliberately NOT attempted here. ``_evict_dedup_cache``
handles only the same-process half of the analogous LRU staleness;
another worker pod's 5-minute LRU can still serve a deleted path.
"""
from __future__ import annotations

import contextlib
import logging
import os
from dataclasses import dataclass, field
from typing import (
    Any, AsyncIterator, Awaitable, Callable, Dict, FrozenSet, Iterable, List,
    Mapping, Optional, Sequence, Set,
)
from uuid import UUID

from sqlalchemy import delete as sa_delete, select, text, update as sa_update
from sqlalchemy.ext.asyncio import AsyncSession

from shared.models import ChatUrlCache, Snapshot, SnapshotItem, SnapshotStatus, SnapshotChatInventory
from shared.blob_gc import is_catalogue_blob_path

logger = logging.getLogger(__name__)


# Chunk size for every IN-list this module builds. Matches
# shared/blob_dedup.py::_BULK_CHUNK — Postgres allows 65k binds but
# real planners choke on IN-lists past ~1K.
_PROBE_CHUNK = 500

# Per-RUN ceiling on unkeyed (no content_checksum) candidate paths a
# teardown may check with the exact blob_path probe.
#
# Sized against the REAL prod schema, 2026-07-31: snapshot_items is a
# PLAIN ~87GB heap (relkind='r', zero partitions) whose only indexes are
# the ones shared/database.py creates — none of them on blob_path. So
# each chunk of _PROBE_CHUNK paths costs ONE full 87GB sequential scan,
# not 1/16th of one. An earlier value of 2000 was justified in a comment
# as "~4 unindexed scans of every snapshot_items PARTITION", i.e. it was
# budgeted against a 16-way hash-partitioned table that does not exist:
# on the real heap it bought 4 whole-table scans per teardown, on the
# database the live Gold backups share, fired from a detached background
# task on every cancel and re-fired by a 30s sweep.
#
# One chunk per run is the ceiling now. Paths past it are RETAINED
# (never deleted unchecked), which trades reclaimed storage — visible in
# ``retained_reasons[RETAIN_UNVERIFIABLE]`` and reconcilable against
# orphan GC — for a bounded database. Re-arming a bigger budget wants
# ix_snapshot_items_blob_path to exist first.
#
# NOTE the unit: PATHS, not scans. A caller that loops must therefore
# also aggregate — see ``teardown_cancelled_sweep_blobs``, which collects
# the whole pass's checksum-less candidates into ONE call so a pass costs
# one scan total instead of one per snapshot.
_UNKEYED_RUN_BUDGET = _PROBE_CHUNK

# Rows the cancelled-snapshot sweep pulls off its streaming cursor before
# planning + deleting them, and therefore the ceiling on how many
# ``BlobCandidate`` objects (and how big a ``by_path`` plan) exist at
# once. backup-scheduler's container limit is 512Mi and a cancelled
# 200k-item snapshot is a real shape, so the pass must never materialize
# a whole snapshot: at 500 the peak is ~1MB of candidates + plan instead
# of the ~350MB a 200k-row read plus its whole-snapshot plan costs.
#
# It doubles as the keyed-probe chunk size: one
# ``(tenant_id, content_checksum) IN (...)`` round trip per batch of
# rows, which is where the per-snapshot fan-out used to come from.
_SWEEP_ROW_BATCH = 500

# Distinct past-budget unkeyed paths the sweep tracks for the retained
# histogram before it degrades to counting occurrences. Bounds the
# accounting structure itself on a 200k-item snapshot (all of whose paths
# are past the budget); the numbers are operator telemetry, not a
# correctness input.
_UNKEYED_OVERFLOW_TRACK = 10_000


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        logger.warning(
            "[backup_cleanup] %s=%r is not an integer; using %d",
            name, raw, default,
        )
        return default


# Wall-clock ceiling for ONE unkeyed probe chunk, applied as a
# transaction-local statement_timeout (SET LOCAL, reset afterwards —
# safe under the pooler's transaction pooling, same pattern as
# shared/storage_bootstrap.py's lock_timeout guard).
#
# Nothing else bounds this statement: shared/database.py sets
# statement_timeout only via SET LOCAL on DDL paths, and both teardown
# entry points run detached from any HTTP request, so before this a
# single scan could run for minutes. 30s matches the cancelled-snapshot
# sweep's own interval, so one probe can never straddle two passes.
#
# A timeout RETAINS the paths it could not verify (never deletes them),
# so the failure mode is leaked storage, not lost data. Set
# BLOB_GUARD_UNKEYED_PROBE_TIMEOUT_MS=0 to disable the unkeyed fallback
# entirely — every checksum-less path is then retained as unverifiable.
_UNKEYED_PROBE_TIMEOUT_MS = _env_int(
    "BLOB_GUARD_UNKEYED_PROBE_TIMEOUT_MS", 30_000,
)

# Process-wide single-flight flag for the unkeyed probe. asyncio is
# single-threaded, so a plain bool is race-free and — unlike a lock —
# cannot wedge the scheduler: a teardown that finds a scan already in
# flight RETAINS its unkeyed paths instead of queueing behind it. Both
# entry points fire detached tasks (job-service's post-cancel task, the
# backup-scheduler sweep every 30s), so without this a burst of cancels
# fans out one 87GB sequential scan per cancel, concurrently.
_unkeyed_probe_in_flight = False


@contextlib.contextmanager
def _unkeyed_probe_slot():
    global _unkeyed_probe_in_flight
    _unkeyed_probe_in_flight = True
    try:
        yield
    finally:
        _unkeyed_probe_in_flight = False


# Process-wide single-flight flag for a WHOLE cancelled-sweep pass. Same
# reasoning and same asyncio-single-threaded race-freedom as the probe
# flag above, one level up: the sweep fires every 30 SECONDS and a pass
# over its ``LIMIT 200`` candidate batch does a blob delete per
# snapshot_item, so a pass routinely outlives its own interval. Stacking
# passes means two teardowns planning and deleting the same batch's bytes
# concurrently, doubling the DB load that this module exists to bound.
# A pass that finds one already running returns
# ``SweepBlobTeardown(skipped=True)`` — nothing deleted, nothing reapable,
# so the caller deletes no rows either.
_sweep_pass_in_flight = False


@contextlib.contextmanager
def _sweep_pass_slot():
    global _sweep_pass_in_flight
    _sweep_pass_in_flight = True
    try:
        yield
    finally:
        _sweep_pass_in_flight = False


class _NoSavepoint:
    """Fallback when the session has no savepoint support."""

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *exc) -> bool:
        return False


def _savepoint(session: Any) -> Any:
    """SAVEPOINT around the unkeyed probe.

    A statement_timeout abort poisons the whole transaction in Postgres
    ("current transaction is aborted"), which would take out the
    caller's session — including the chat_url_cache invalidation that
    has to run before any delete. Rolling back to a savepoint restores
    the transaction, so a timeout costs only the paths it was probing.
    """
    begin_nested = getattr(session, "begin_nested", None)
    if begin_nested is None:
        return _NoSavepoint()
    try:
        return begin_nested()
    except Exception:  # pragma: no cover - defensive
        return _NoSavepoint()


async def _probe_unkeyed_paths_bounded(
    session: AsyncSession,
    paths: Sequence[str],
) -> List[Any]:
    """One time-bounded, savepoint-isolated exact-path probe chunk.

    Raises on timeout / DB error; the caller RETAINS the chunk's paths.
    """
    stmt = (
        select(SnapshotItem.blob_path, SnapshotItem.snapshot_id)
        .where(SnapshotItem.blob_path.in_(list(paths)))
        .distinct()
    )
    async with _savepoint(session):
        try:
            await session.execute(text(
                f"SET LOCAL statement_timeout = "
                f"{int(_UNKEYED_PROBE_TIMEOUT_MS)}"
            ))
            rows = (await session.execute(stmt)).all()
        finally:
            # Do not leave the rest of the transaction (notably the
            # chat_url_cache UPDATE) running under the probe's timeout.
            # On the abort path the savepoint rollback also undoes the
            # SET LOCAL, so this is best-effort by design.
            with contextlib.suppress(Exception):
                await session.execute(
                    text("SET LOCAL statement_timeout = DEFAULT")
                )
    return rows

# Reason codes recorded in ``BlobDeletionPlan.retained``. Stable
# strings: they land in log lines and stats histograms that operators
# reconcile against orphan GC.
RETAIN_FOREIGN_OWNER = "foreign_owner"
RETAIN_SHARED_REFERENCE = "shared_reference"
RETAIN_REUSE_DESCENDANT = "reuse_chain_descendant"
RETAIN_UNVERIFIABLE = "unverifiable"


class BlobCleanupGuardUnavailable(RuntimeError):
    """The reference guard could not be established, so no teardown ran.

    Raised instead of deleting on an unverified guess: without the
    reuse-chain protection set we cannot tell whether a target snapshot
    is the chain root a LIVE descendant reads its inventory out of, and
    that makes the ``snapshot_items`` delete + counter zeroing exactly
    as destructive as the blob delete. Callers should log and retry;
    every teardown here is idempotent.
    """


@dataclass(frozen=True)
class BlobCandidate:
    """One blob path a teardown wants to delete, plus the keys needed
    to prove nobody else references it.

    ``content_checksum`` is the dedup join key — every mechanism that
    hands one snapshot another snapshot's ``blob_path`` copies it
    along with the path (see ``select_deletable_blob_paths``). Often
    absent, and legitimately so: the azure-workload handlers set it to
    None on every row they write. Those paths are still checked, by the
    unindexed exact-path probe, under a per-run budget.

    ``backend_id`` is carried through untouched so callers that route
    deletes per storage backend (the backup-scheduler sweep) can use
    the same objects for the probe and the delete.
    """
    blob_path: str
    snapshot_id: Optional[UUID] = None
    tenant_id: Optional[UUID] = None
    content_checksum: Optional[str] = None
    backend_id: Optional[UUID] = None


@dataclass(frozen=True)
class BlobDeletionPlan:
    """Verdict for a set of ``BlobCandidate`` paths.

    ``deletable`` is the only set a caller may hand to storage.
    ``retained`` maps every other candidate path to its reason code,
    so the deliberate leaks stay visible. ``protected_roots`` names the
    doomed snapshots that still have live reuse-chain descendants —
    a caller that would rather reclaim those bytes must rehydrate the
    heir first (``shared.retention_cleanup._rehydrate_reuse_heir``).

    ``unkeyed_probed`` is how many paths this call spent on the
    unindexed exact-path fallback. A caller that loops (one call per
    batch) MUST subtract it from the ``unkeyed_probe_budget`` it passes
    next time, or the cap resets every batch and the run is unbounded.
    """
    deletable: FrozenSet[str] = frozenset()
    retained: Mapping[str, str] = field(default_factory=dict)
    protected_roots: FrozenSet[UUID] = frozenset()
    unkeyed_probed: int = 0

    @property
    def retained_reasons(self) -> Dict[str, int]:
        """Histogram of reason code → distinct retained path count."""
        out: Dict[str, int] = {}
        for reason in self.retained.values():
            out[reason] = out.get(reason, 0) + 1
        return out


def _evict_dedup_cache(
    tenant_id: Optional[UUID],
    content_checksum: Optional[str],
) -> None:
    """Drop this process's blob_dedup LRU entry for a just-deleted blob.

    Best-effort: the cache is advisory, and failing to evict must never
    abort a cleanup. Imported lazily so ``backup_cleanup`` stays cheap
    to import from a request handler.
    """
    if tenant_id is None or not content_checksum:
        return
    try:
        from shared.blob_dedup import invalidate_dedup_cache_entry
        invalidate_dedup_cache_entry(tenant_id, content_checksum)
    except Exception as exc:  # pragma: no cover - advisory cache only
        logger.debug("[backup_cleanup] dedup LRU evict skipped: %s", exc)


def _owning_snapshot_id(blob_path: str) -> Optional[UUID]:
    """The snapshot that physically uploaded ``blob_path``, if knowable.

    ``azure_storage_manager.build_blob_path`` emits
    ``{tenant}/{resource}/{snapshot}/{ts}/{item}`` — segment 2 names the
    snapshot that wrote the bytes, so a dedup-inherited path still
    carries its original owner. Content-addressed layouts (Teams
    messages, chat hosted content) have no such segment; those return
    None and are governed by the DB reference check alone.
    """
    parts = (blob_path or "").split("/")
    if len(parts) < 4:
        return None
    try:
        return UUID(parts[2])
    except (AttributeError, TypeError, ValueError):
        return None


def blob_candidates_from_items(items: Iterable[Any]) -> List[BlobCandidate]:
    """Every blob a teardown of ``items`` would delete, keyed for the guard.

    Accepts ORM ``SnapshotItem`` objects or any row exposing the same
    attributes. Emits the primary ``blob_path`` plus the legacy
    ``extra_data.attachment_blob_paths`` fan-out. Attachment paths get
    no ``content_checksum`` — the parent row carries only its own — so
    they fall into the guard's bounded unindexed fallback. (No current
    writer emits ``attachment_blob_paths``; this stays for pre-deploy
    rows.)
    """
    out: List[BlobCandidate] = []
    for it in items:
        sid = getattr(it, "snapshot_id", None)
        tid = getattr(it, "tenant_id", None)
        backend_id = getattr(it, "backend_id", None)
        bp = getattr(it, "blob_path", None)
        if bp:
            out.append(BlobCandidate(
                blob_path=bp, snapshot_id=sid, tenant_id=tid,
                content_checksum=getattr(it, "content_checksum", None),
                backend_id=backend_id,
            ))
        extra = getattr(it, "extra_data", None) or {}
        att = (
            extra.get("attachment_blob_paths") if isinstance(extra, dict) else None
        ) or []
        for ap in att:
            if ap:
                out.append(BlobCandidate(
                    blob_path=ap, snapshot_id=sid, tenant_id=tid,
                    content_checksum=None, backend_id=backend_id,
                ))
    return out


async def fetch_blob_candidates(
    session: AsyncSession,
    *,
    snapshot_id: UUID,
) -> List[BlobCandidate]:
    """Read one snapshot's deletable-blob candidates, keys included.

    For callers that really do handle ONE snapshot and know its inventory
    is small. The cancelled-snapshot sweep uses
    ``stream_sweep_blob_candidates`` instead: a whole-snapshot read of a
    cancelled 200k-item snapshot is ~282MiB of candidates+plan inside
    backup-scheduler's 512Mi limit, and one call per snapshot is what
    fanned the guard's probes out per snapshot.

    Same WHERE clause the backup-scheduler sweep already uses for its
    blob walk, plus the two columns the guard needs
    (``tenant_id``, ``content_checksum``). Equality on ``snapshot_id``
    is index-qualified on the real (plain, unpartitioned) prod table —
    ``idx_snapshot_items_snapshot_type`` and
    ``idx_snapshot_items_snap_type_ext`` both lead on snapshot_id — so
    this read is cheap regardless of the heap's 87GB.
    """
    rows = (await session.execute(
        text(
            "SELECT blob_path, snapshot_id, tenant_id, content_checksum, "
            "       backend_id "
            "  FROM snapshot_items "
            " WHERE snapshot_id = CAST(:sid AS UUID) "
            "   AND blob_path IS NOT NULL "
            "   AND backend_id IS NOT NULL"
        ),
        {"sid": str(snapshot_id)},
    )).all()
    return [
        BlobCandidate(
            blob_path=r.blob_path, snapshot_id=r.snapshot_id,
            tenant_id=r.tenant_id, content_checksum=r.content_checksum,
            backend_id=r.backend_id,
        )
        for r in rows if r.blob_path
    ]


async def stream_sweep_blob_candidates(
    session: AsyncSession,
    *,
    snapshot_ids: Sequence[UUID],
    batch_rows: int = _SWEEP_ROW_BATCH,
) -> AsyncIterator[List[BlobCandidate]]:
    """Yield the whole sweep batch's blob candidates in bounded chunks.

    ONE statement for the entire pass — the per-snapshot
    ``fetch_blob_candidates`` loop is what forced a per-snapshot plan and
    therefore a per-snapshot sharer probe. ``snapshot_id = ANY(...)`` is
    still index-qualified on the real (plain, unpartitioned) prod table:
    ``idx_snapshot_items_snapshot_type`` and
    ``idx_snapshot_items_snap_type_ext`` both lead on snapshot_id, so this
    is a bitmap index scan regardless of the heap's 87GB.

    Read through a server-side cursor (``AsyncSession.stream`` +
    ``yield_per``, the same idiom ``shared/retention_cleanup.py`` and
    services/resource-service already use) so peak memory is
    ``batch_rows`` candidates, not one snapshot's whole inventory. No
    ``ORDER BY``: sorting a cancelled 200k-item snapshot would spill to
    disk for no benefit, and nothing downstream depends on row order.

    The cursor's transaction is READ ONLY and takes no row locks; the
    caller does its writes (chat_url_cache) and its blob deletes in
    separate short sessions.
    """
    stmt = text(
        "SELECT blob_path, snapshot_id, tenant_id, content_checksum, "
        "       backend_id "
        "  FROM snapshot_items "
        " WHERE snapshot_id = ANY(CAST(:sids AS UUID[])) "
        "   AND blob_path IS NOT NULL "
        "   AND backend_id IS NOT NULL"
    ).execution_options(yield_per=int(batch_rows))
    result = await session.stream(
        stmt, {"sids": [str(s) for s in snapshot_ids]},
    )
    async for rows in result.partitions(int(batch_rows)):
        yield [
            BlobCandidate(
                blob_path=r.blob_path, snapshot_id=r.snapshot_id,
                tenant_id=r.tenant_id, content_checksum=r.content_checksum,
                backend_id=r.backend_id,
            )
            for r in rows if r.blob_path
        ]


async def resolve_protected_reuse_roots(
    session: AsyncSession,
    doomed_snapshot_ids: Sequence[UUID],
) -> Set[UUID]:
    """Doomed snapshots that a LIVE reuse-chain descendant still needs.

    A reuse snapshot owns zero ``snapshot_items`` rows — it resolves
    through ``reuse_chain_root_id`` to the root's rows
    (``shared.snapshot_reuse.resolve_snapshot_items_target``). So if the
    root is inside the teardown set and a descendant is not, NO
    snapshot_items probe can see the dependency: guard 1 finds no rows
    outside the set and guard 2 resolves the owner to a doomed
    snapshot. Every blob the root's rows point at is load-bearing for
    that descendant.

    One query for the whole run — both columns have partial indexes
    (``shared/database.py``: ix on reuse_of_snapshot_id /
    reuse_chain_root_id WHERE NOT NULL).
    """
    ids = [str(x) for x in doomed_snapshot_ids]
    if not ids:
        return set()
    rows = (await session.execute(
        text(
            "SELECT DISTINCT anc.ancestor_id FROM ( "
            "  SELECT reuse_chain_root_id AS ancestor_id "
            "    FROM snapshots "
            "   WHERE reuse_chain_root_id = ANY(CAST(:doomed AS UUID[])) "
            "     AND id <> ALL(CAST(:doomed AS UUID[])) "
            "  UNION ALL "
            "  SELECT reuse_of_snapshot_id AS ancestor_id "
            "    FROM snapshots "
            "   WHERE reuse_of_snapshot_id = ANY(CAST(:doomed AS UUID[])) "
            "     AND id <> ALL(CAST(:doomed AS UUID[])) "
            ") anc WHERE anc.ancestor_id IS NOT NULL"
        ),
        {"doomed": ids},
    )).all()
    return {
        r.ancestor_id if isinstance(r.ancestor_id, UUID)
        else UUID(str(r.ancestor_id))
        for r in rows
    }


async def hold_protected_reuse_roots(
    session: AsyncSession,
    snapshot_ids: Sequence[UUID],
) -> int:
    """Park a protected reuse root where no unguarded reaper will find it.

    Refusing to touch a protected root is not enough on its own. A
    cancelled snapshot left ``IN_PROGRESS``, or left carrying
    ``extra_data.cancelled_at``, is exactly what
    ``_sweep_cancelled_snapshots`` (services/backup-scheduler, every 30s)
    selects — Population B is ``status='IN_PROGRESS' AND j.status IN
    ('CANCELLED','FAILED')`` and Populations A/C are ``cancelled_at IS
    NOT NULL`` — and that sweep deletes every ``blob_path`` it finds, the
    ``snapshot_items`` rows and the ``snapshots`` row, with no reference
    guard at all. So "skip it" would hand the root over inside 30
    seconds, which is worse than not having a guard.

    This makes the protection durable without editing the sweep:

      * ``IN_PROGRESS`` → ``FAILED``. Terminal (the cancel really did
        happen) and out of Population B. A ``COMPLETED``/``PARTIAL``
        root keeps its status — job-service deliberately doesn't
        downgrade those, and they are not in Population B anyway.
      * ``extra_data.cancelled_at`` is MOVED to
        ``reuse_protected_cancelled_at`` (not dropped), which takes it
        out of Populations A/C while keeping the cancel history. Both
        job-service branches that stamp ``cancelled_at`` are also
        status-filtered on IN_PROGRESS / COMPLETED+no-marker, so after
        this flip neither re-stamps it.
      * ``item_count`` / ``bytes_total`` / ``snapshot_items`` are NOT
        touched. They ARE the live descendant's inventory
        (``shared.snapshot_reuse.resolve_snapshot_items_target``, which
        does not look at status).
      * ``reuse_protected`` = true is the marker an adopting sweep
        should skip on, and the flag an operator greps for before
        running ``_rehydrate_reuse_heir`` to reclaim the bytes.

    Idempotent, and safe to call again after a partial run. Returns the
    number of rows held. Commits — the hold must be durable BEFORE any
    byte is deleted.
    """
    ids = [str(x) for x in dict.fromkeys(snapshot_ids)]
    if not ids:
        return 0
    result = await session.execute(
        text(
            "UPDATE snapshots SET "
            "  status = CASE WHEN status = 'IN_PROGRESS' "
            "                THEN 'FAILED'::snapshotstatus ELSE status END, "
            "  extra_data = ( "
            "    (COALESCE(extra_data::jsonb, '{}'::jsonb) - 'cancelled_at') "
            "    || jsonb_build_object( "
            "         'reuse_protected', true, "
            "         'reuse_protected_at', NOW(), "
            "         'reuse_protected_cancelled_at', "
            "           COALESCE(extra_data::jsonb ->> 'cancelled_at', "
            "                    extra_data::jsonb ->> "
            "                      'reuse_protected_cancelled_at'), "
            "         'cancel_phase', 'reuse_protected_hold') "
            "  )::json "
            " WHERE id = ANY(CAST(:ids AS UUID[]))"
        ),
        {"ids": ids},
    )
    await session.commit()
    return getattr(result, "rowcount", 0) or 0


async def select_deletable_blob_paths(
    session: AsyncSession,
    *,
    doomed_snapshot_ids: Sequence[UUID],
    candidates: Iterable[BlobCandidate],
    protected_roots: Optional[Set[UUID]] = None,
    unkeyed_probe_budget: int = _PROBE_CHUNK,
) -> BlobDeletionPlan:
    """Given the snapshots being torn down, return only the blob paths
    that are safe to physically delete.

    **Intended call sites.** Every teardown that deletes bytes:

      * ``cleanup_cancelled_snapshots`` below (worker intake drop +
        job-service's post-cancel background task).
      * ``_sweep_cancelled_snapshots`` in services/backup-scheduler —
        the path that actually reaps cancel-flipped snapshots (job-
        service flips IN_PROGRESS → FAILED + ``cancelled_at``, which is
        outside this module's status filter, so the sweep is where most
        cancelled bytes die). Pair with
        ``stream_sweep_blob_candidates``, and pass the WHOLE pass's
        snapshot ids as ``doomed_snapshot_ids`` even though each call
        gets only a slice of the candidates — otherwise sibling doomed
        snapshots look like live referrers and nothing is ever
        reclaimed. A snapshot in ``plan.protected_roots`` must be
        skipped WHOLESALE (blobs, rows AND the snapshots row) and passed
        to ``hold_protected_reuse_roots`` so the sweep stops
        re-selecting it; the module docstring has the full call order.

        A looping caller must ALSO aggregate its checksum-less
        candidates into one final call rather than passing them slice by
        slice: ``unkeyed_probe_budget`` bounds probed PATHS, and every
        call that has even one unkeyed path left after the owner/reuse
        gates issues its own 87GB sequential scan.

    A path is deletable only when ALL of these hold:

      1. **Owner gate.** If the layout reveals the snapshot that
         uploaded the bytes (``build_blob_path`` puts it in segment 2),
         that snapshot is itself doomed. Free — no query.
      2. **Reuse gate.** Its source snapshot is not a chain root with a
         live descendant (see ``resolve_protected_reuse_roots``).
      3. **Sharer gate.** No ``snapshot_items`` row outside the doomed
         set references the same path.

    Gate 3 takes one of TWO paths per candidate, decided purely by
    whether that candidate carries a ``content_checksum``. Both do the
    full reference check; they differ only in cost.

      * **Keyed** (``tenant_id`` AND ``content_checksum`` present) —
        driven off ``idx_snapshot_items_tenant_checksum``. The checksum
        is a sound join key because every mechanism that points one
        snapshot at ANOTHER snapshot's blob_path copies content_checksum
        along with the path: ``blob_dedup.find_existing_blob`` /
        ``bulk_find_existing_blobs`` and ``upload_blob_with_dedup``
        match ON that column; the Teams per-page cross-user dedup
        matches on it and re-stamps it (workers/backup-worker/main.py
        ~18068/~18129, where it also equals the sha in the
        content-addressed path); the chat_url_cache hit path stamps the
        cached ``content_hash`` into it; ``_rehydrate_reuse_heir`` and
        every raw ``INSERT INTO snapshot_items ... SELECT`` copy both
        columns; both ``UPDATE ... SET blob_path`` sites set
        content_checksum in the same statement. This covers everything
        the backup worker writes — all 31 of its ``SnapshotItem(...)``
        constructions that set ``blob_path`` also set a computed sha256.
      * **Unkeyed** (no checksum, or no tenant) — falls back to the
        exact ``blob_path IN (...)`` probe, which uses no index. This is
        routine, not exotic. Re-audited 2026-07-31: **18 of the 49**
        ``SnapshotItem`` constructions that set ``blob_path`` set
        ``content_checksum=None`` outright — every azure-workload-worker
        row (postgres_handler 934/959/982/1007/1083/1106/1146/1168,
        vm_handler 986/1034/1057/1081/1105/1133, sql_handler
        1001/1021/1051/1074: VM config, disks, volumes, NICs, public
        IPs, Azure DB config/schema/table artifacts) — plus every
        ``extra_data.attachment_blob_paths`` fan-out. An earlier version
        of this docstring claimed 0 of 49; that was WRONG. These
        candidates are neither skipped nor deleted unverified: they get
        the probe, bounded by ``unkeyed_probe_budget`` paths (a caller
        that loops must decrement it by ``plan.unkeyed_probed``), and
        anything past the budget is RETAINED with reason
        ``unverifiable``. Their blast radius is small in practice —
        Azure workload snapshots run tens-to-thousands of items, not the
        200k of a mail walk.

    Cost, against the real prod schema (PG 17.10, 2026-07-31):
    ``snapshot_items`` is a PLAIN ~87GB heap — relkind='r', ZERO
    partitions — so the keyed probe is an index scan on
    ``idx_snapshot_items_tenant_checksum`` while the unkeyed one has no
    usable index at all and reads all 87GB. That is why the unkeyed
    residue is triple-bounded: at most ``_UNKEYED_RUN_BUDGET`` paths
    (one chunk = one scan) per run, each chunk under a
    ``statement_timeout``, and single-flight process-wide so a burst of
    cancels cannot fan out concurrent scans. Earlier revisions of this
    docstring described the table as ``PARTITION BY HASH (snapshot_id)``
    with 16 partitions and sized the budget against 1/16th-of-the-table
    scans; the migration that would have partitioned it never applied to
    prod, so those numbers were 16x optimistic and are corrected here.

    Never raises for a "maybe": anything unproven is retained. DB
    errors DO propagate so the caller can retain the whole batch
    instead of deleting on an unverified guess.
    """
    doomed = list(dict.fromkeys(doomed_snapshot_ids))
    doomed_set = set(doomed)

    # Collapse to one entry per path; a path can arrive from several
    # rows (two items dedup'd onto it) and any of their checksums is a
    # valid probe key.
    by_path: Dict[str, Dict[str, Any]] = {}
    for c in candidates:
        if not c.blob_path:
            continue
        slot = by_path.setdefault(
            c.blob_path, {"tenants": set(), "checksums": set(), "sources": set()},
        )
        if c.tenant_id is not None:
            slot["tenants"].add(c.tenant_id)
        if c.content_checksum:
            slot["checksums"].add(c.content_checksum)
        if c.snapshot_id is not None:
            slot["sources"].add(c.snapshot_id)
    if not by_path:
        return BlobDeletionPlan()

    if protected_roots is None:
        protected_roots = await resolve_protected_reuse_roots(session, doomed)

    retained: Dict[str, str] = {}
    survivors: Set[str] = set()
    for path, slot in by_path.items():
        if is_catalogue_blob_path(path):
            retained[path] = "catalogue_owned"
            continue
        owner = _owning_snapshot_id(path)
        if owner is not None and owner not in doomed_set:
            retained[path] = RETAIN_FOREIGN_OWNER
            continue
        if (slot["sources"] & protected_roots) or (
            owner is not None and owner in protected_roots
        ):
            retained[path] = RETAIN_REUSE_DESCENDANT
            continue
        survivors.add(path)

    # --- Gate 3a: indexed (tenant_id, content_checksum) sharer probe --
    keyed: Dict[UUID, Set[str]] = {}
    unkeyed: List[str] = []
    for path in survivors:
        slot = by_path[path]
        if slot["tenants"] and slot["checksums"]:
            for tid in slot["tenants"]:
                keyed.setdefault(tid, set()).update(slot["checksums"])
        else:
            unkeyed.append(path)

    for tenant_id, checksums in keyed.items():
        ordered = sorted(checksums)
        for start in range(0, len(ordered), _PROBE_CHUNK):
            stmt = (
                select(
                    SnapshotItem.snapshot_id,
                    SnapshotItem.blob_path,
                    SnapshotItem.content_checksum,
                )
                .where(SnapshotItem.tenant_id == tenant_id)
                .where(SnapshotItem.content_checksum.in_(
                    ordered[start:start + _PROBE_CHUNK]
                ))
            )
            for row in (await session.execute(stmt)).all():
                path = row.blob_path
                if not path or path not in survivors:
                    continue
                sid = row.snapshot_id
                if sid not in doomed_set or sid in protected_roots:
                    survivors.discard(path)
                    retained[path] = RETAIN_SHARED_REFERENCE

    # --- Gate 3b: bounded exact-path fallback for unkeyed candidates --
    # Unkeyed is a normal, expected shape, not an anomaly: every
    # azure-workload-worker row sets content_checksum=None, and so does
    # every attachment_blob_paths fan-out. They still get a real
    # reference check; the budget only decides how many get it in one
    # run, and the ones that don't are KEPT.
    unkeyed.sort()
    budget = max(0, int(unkeyed_probe_budget))
    if len(unkeyed) > budget:
        for path in unkeyed[budget:]:
            survivors.discard(path)
            retained[path] = RETAIN_UNVERIFIABLE
        logger.warning(
            "[backup_cleanup] unkeyed probe budget spent: %d of %d "
            "checksum-less path(s) RETAINED unverified this call (they "
            "need an unindexed snapshot_items scan; reconcile against "
            "orphan GC, or build ix_snapshot_items_blob_path)",
            len(unkeyed) - budget, len(unkeyed),
        )
        unkeyed = unkeyed[:budget]

    def _retain_unprobed(paths: Sequence[str], why: str) -> None:
        if not paths:
            return
        for path in paths:
            survivors.discard(path)
            retained[path] = RETAIN_UNVERIFIABLE
        logger.warning(
            "[backup_cleanup] %d checksum-less path(s) RETAINED unverified "
            "(%s) — a full sequential scan of the 87GB snapshot_items heap "
            "was not affordable here; reconcile against orphan GC, or build "
            "ix_snapshot_items_blob_path so this probe stops being a scan",
            len(paths), why,
        )

    probed = 0
    if unkeyed and _UNKEYED_PROBE_TIMEOUT_MS <= 0:
        _retain_unprobed(unkeyed, "unkeyed fallback disabled by env")
    elif unkeyed and _unkeyed_probe_in_flight:
        # Single-flight: another teardown is already scanning the heap.
        # Piling a second scan on top is how the live Gold backups get
        # starved, and both entry points fire detached tasks.
        _retain_unprobed(unkeyed, "another unkeyed probe already in flight")
    elif unkeyed:
        with _unkeyed_probe_slot():
            for start in range(0, len(unkeyed), _PROBE_CHUNK):
                chunk = unkeyed[start:start + _PROBE_CHUNK]
                try:
                    rows = await _probe_unkeyed_paths_bounded(session, chunk)
                except Exception as exc:
                    # Timeout or DB error. Keep the bytes, keep the keyed
                    # verdicts, and do NOT charge the run's budget for a
                    # scan that produced no answer.
                    _retain_unprobed(
                        unkeyed[start:], f"probe failed: {exc}",
                    )
                    break
                probed += len(chunk)
                for row in rows:
                    path = row.blob_path
                    if not path or path not in survivors:
                        continue
                    sid = row.snapshot_id
                    if sid not in doomed_set or sid in protected_roots:
                        survivors.discard(path)
                        retained[path] = RETAIN_SHARED_REFERENCE

    return BlobDeletionPlan(
        deletable=frozenset(survivors),
        retained=retained,
        protected_roots=frozenset(protected_roots),
        unkeyed_probed=probed,
    )


async def invalidate_chat_url_cache_paths(
    session: AsyncSession,
    *,
    blob_paths: Iterable[str],
    tenant_ids: Iterable[UUID] = (),
) -> int:
    """Clear ``chat_url_cache`` rows that point at ``blob_paths``.

    ``chat_url_cache`` is a tenant-scoped, CROSS-SNAPSHOT
    (url_sha256 → blob_path) map (shared/models.py::ChatUrlCache) and
    the worker's bulk hit path reuses ``blob_path`` with no existence
    probe (workers/backup-worker/main.py ~9855). Chat attachment blobs
    are written under the WRITING snapshot's id, so the owner gate
    calls them owned and no other snapshot_items row references them
    yet — the blob goes, the cache row survives, and the NEXT backup
    stamps a fresh snapshot_items row onto a dead path. Nulling
    ``blob_path`` is enough: both read paths hit only on
    ``content_hash AND (blob_path OR inline_b64)``, so the row becomes a
    miss and the URL is re-resolved — or, if it also carries
    ``inline_b64``, stays a hit served from the inline bytes. Nothing
    else on the row is touched (in particular not ``content_size``,
    which that surviving-hit case still reports).

    MUST be awaited BEFORE the blobs are deleted: a crash between the
    two then leaks bytes (recoverable) instead of leaving a live
    pointer to bytes that no longer exist (not recoverable). Callers
    must retain the batch if this raises.

    Returns rows cleared. ``tenant_ids`` scopes the UPDATE onto the
    table's PK prefix; pass it whenever known.
    """
    paths = sorted({p for p in blob_paths if p})
    if not paths:
        return 0
    tenants = list(dict.fromkeys(t for t in tenant_ids if t is not None))
    if not tenants:
        logger.warning(
            "[backup_cleanup] chat_url_cache invalidation running "
            "un-scoped (%d path(s)) — no tenant id on the candidate rows",
            len(paths),
        )
    cleared = 0
    for start in range(0, len(paths), _PROBE_CHUNK):
        stmt = sa_update(ChatUrlCache).where(
            ChatUrlCache.blob_path.in_(paths[start:start + _PROBE_CHUNK])
        )
        if tenants:
            stmt = stmt.where(ChatUrlCache.tenant_id.in_(tenants))
        # blob_path ONLY. A row can legitimately carry blob_path AND
        # inline_b64 (_write_tenant_url_cache's COALESCE merge), and both
        # read paths hit on content_hash AND (blob_path OR inline_b64) —
        # so such a row stays a hit, and nulling content_size would make
        # it report `content_size or 0` = 0 bytes forever.
        result = await session.execute(stmt.values(blob_path=None))
        cleared += getattr(result, "rowcount", 0) or 0
    await session.commit()
    return cleared


@dataclass(frozen=True)
class SweepBlobTeardown:
    """Verdict for one pass of the cancelled-snapshot sweep.

    The three id sets partition the candidate batch and tell the caller
    exactly which rows it may now DELETE:

      * ``held`` — protected reuse roots. Their blobs, their
        ``snapshot_items`` rows and their counters are all load-bearing
        for a LIVE descendant, so the caller must skip them WHOLESALE.
        They have been flipped out of the sweep's own candidate
        populations (``hold_protected_reuse_roots``) so the next pass
        stops re-selecting them; an operator reclaims them by
        rehydrating the heir.
      * ``reapable`` — the guard answered, deletable bytes are gone, and
        the ``snapshot_items`` / ``snapshots`` rows may be deleted. Some
        of their paths may still have been RETAINED (shared reference,
        foreign owner, unverifiable): that is expected and safe, because
        any live referrer of a retained path owns its own
        ``snapshot_items`` row.
      * ``blocked`` — the guard raised for that snapshot. Its bytes were
        kept, so its rows MUST be kept too: deleting them would orphan
        the bytes with nothing left pointing at them and no way for a
        later pass to retry. The next 30s pass re-selects it.

    ``skipped`` is set when this pass declined to run at all because
    another pass was still in flight (see ``_sweep_pass_slot``). Every
    other field is then zero/empty, which is exactly right for the
    caller: nothing was deleted, so nothing is reapable.
    """
    held: FrozenSet[UUID] = frozenset()
    reapable: FrozenSet[UUID] = frozenset()
    blocked: FrozenSet[UUID] = frozenset()
    blobs_deleted: int = 0
    blob_errors: int = 0
    blobs_retained: int = 0
    blob_paths_retained: int = 0
    retained_reasons: Mapping[str, int] = field(default_factory=dict)
    cache_rows_invalidated: int = 0
    unkeyed_probed: int = 0
    candidate_rows: int = 0
    skipped: bool = False


async def teardown_cancelled_sweep_blobs(
    session_factory: Callable[[], AsyncSession],
    *,
    snapshot_ids: Sequence[UUID],
    container_for: Callable[[UUID], str],
    store_for: Callable[[Any], Any],
    unkeyed_probe_budget: int = _UNKEYED_RUN_BUDGET,
) -> SweepBlobTeardown:
    """Reference-safe blob teardown for ONE cancelled-snapshot sweep pass.

    This is the adoption surface the module docstring describes, packaged
    so ``_sweep_cancelled_snapshots`` (services/backup-scheduler, every
    30s) gets the guard without hand-rolling the call order — and it is
    the path most cancelled bytes actually die on, because job-service's
    cancel endpoint flips IN_PROGRESS → FAILED + ``cancelled_at``, which
    is outside ``cleanup_cancelled_snapshots``' status filter.

    Runs the documented sequence, in order:

      1. ``resolve_protected_reuse_roots`` ONCE for the whole batch.
      2. ``hold_protected_reuse_roots`` on whatever came back, so the
         30s sweep stops re-selecting rows we are refusing to reap.
      3. ``stream_sweep_blob_candidates`` ONCE for the whole batch, in
         ``_SWEEP_ROW_BATCH`` chunks. Per chunk:
         ``select_deletable_blob_paths`` (with the WHOLE batch as
         ``doomed_snapshot_ids``, or sibling doomed snapshots look like
         live referrers and nothing is ever reclaimed) →
         ``invalidate_chat_url_cache_paths`` BEFORE any delete → delete
         only ``plan.deletable``.
      4. ONE aggregated unkeyed pass at the end for the checksum-less
         residue collected in step 3.

    Bounded cost — why steps 3 and 4 are split
    ------------------------------------------
    This used to loop ``fetch_blob_candidates`` +
    ``select_deletable_blob_paths`` once per snapshot, and
    ``unkeyed_probe_budget`` bounds PATHS, not SCANS. So every snapshot
    that carried even one checksum-less candidate issued its own
    ``blob_path IN (...)`` statement — and on the real prod schema
    (``snapshot_items`` = PLAIN ~87GB heap, no index on blob_path) each of
    those is a full sequential scan of all 87GB. A ``LIMIT 200`` pass
    bought up to 200 of them, every 30 seconds, on the database the live
    Gold backups share. The keyed ``(tenant_id, content_checksum)`` probe
    fanned out the same way.

    Now one pass costs, regardless of how many snapshots it tears down:

      * 1 reuse-chain probe, 1 protective hold (only if there are roots),
      * 1 streamed candidate read (index-qualified on snapshot_id),
      * 1 keyed sharer probe per ``_SWEEP_ROW_BATCH`` rows — proportional
        to candidate VOLUME, never to snapshot count,
      * **at most 1** unindexed ``blob_path`` scan, for the whole pass,
      * 1 chat_url_cache UPDATE per batch that has deletable paths (one
        per snapshot only on the isolation retry after a failure).

    Bounded memory — why it streams
    -------------------------------
    backup-scheduler's container limit is 512Mi. Reading one snapshot's
    entire inventory into ``BlobCandidate`` objects and then building a
    whole-snapshot plan over it (``by_path`` = 3 sets per path, plus the
    keyed checksum sets, ``survivors`` and ``retained``) is ~350MB on a
    cancelled 200k-item snapshot — an OOMKill, not a slowdown. Peak here
    is one ``_SWEEP_ROW_BATCH`` of candidates plus its plan (~1MB), the
    unkeyed accumulator (≤ ``unkeyed_probe_budget`` candidates, ~0.1MB),
    and the per-pass id sets (≤ the candidate batch, ~200 UUIDs).

    Parameters
    ----------
    session_factory
        ``async_session_factory``. The streaming read holds one READ ONLY
        cursor; every write (chat_url_cache) and every probe runs in its
        own short session, and blob deletes hold no session at all.
    snapshot_ids
        The whole candidate batch for this pass.
    container_for
        ``snapshot_id`` → container name. Stays with the caller: the
        sweep derives it from the candidate row's resource type +
        tenant prefix.
    store_for
        ``backend_id`` → storage object exposing ``await
        delete(container, path)``. The sweep routes per row via
        ``shared.storage.router.get_store_by_id``, so deletes follow the
        backend the bytes were written to.
    unkeyed_probe_budget
        Ceiling on checksum-less paths the WHOLE pass may verify with the
        unindexed exact-path probe (see ``select_deletable_blob_paths``).
        One chunk = one sequential scan of the 87GB heap, so the default
        buys ONE per pass. Paths past it are RETAINED, never deleted
        unchecked.

    Fails CLOSED everywhere. If the protection set cannot be resolved or
    made durable this raises ``BlobCleanupGuardUnavailable`` having
    deleted NOTHING. If the candidate stream dies, every snapshot in the
    batch is ``blocked``. If a batch's probe or cache invalidation dies,
    that batch's snapshots are ``blocked``. If the pass budget runs out,
    the residue is RETAINED. Every step is idempotent, so the next pass
    retries for free.

    Non-overlapping: a pass that starts while another is still in flight
    returns ``SweepBlobTeardown(skipped=True)`` immediately.
    """
    doomed = list(dict.fromkeys(snapshot_ids))
    if not doomed:
        return SweepBlobTeardown()

    if _sweep_pass_in_flight:
        logger.warning(
            "[backup_cleanup] cancelled sweep — a previous pass is still "
            "running; skipping this one (%d candidate snapshot(s)). Nothing "
            "was deleted and nothing is reapable.", len(doomed),
        )
        return SweepBlobTeardown(skipped=True)

    with _sweep_pass_slot():
        return await _run_cancelled_sweep_pass(
            session_factory,
            doomed=doomed,
            container_for=container_for,
            store_for=store_for,
            unkeyed_probe_budget=unkeyed_probe_budget,
        )


async def _run_cancelled_sweep_pass(
    session_factory: Callable[[], AsyncSession],
    *,
    doomed: List[UUID],
    container_for: Callable[[UUID], str],
    store_for: Callable[[Any], Any],
    unkeyed_probe_budget: int,
) -> SweepBlobTeardown:
    """One pass body. See ``teardown_cancelled_sweep_blobs`` for contract."""
    # --- 1. Protection set, once per pass, fail closed ----------------
    async with session_factory() as session:
        try:
            roots = await resolve_protected_reuse_roots(session, doomed)
        except Exception as exc:
            logger.error(
                "[backup_cleanup] cancelled sweep — reuse-chain protection "
                "could not be resolved for %d candidate snapshot(s); NOTHING "
                "was deleted this pass (no blobs, no snapshot_items rows, no "
                "snapshots rows): %s", len(doomed), exc,
            )
            raise BlobCleanupGuardUnavailable(
                f"reuse-chain protection unresolved for {len(doomed)} "
                f"cancelled snapshot(s); nothing was deleted: {exc}"
            ) from exc

    # --- 2. Make the protection durable BEFORE any byte goes ----------
    if roots:
        async with session_factory() as session:
            try:
                held_n = await hold_protected_reuse_roots(
                    session, sorted(roots, key=str),
                )
            except Exception as exc:
                logger.error(
                    "[backup_cleanup] cancelled sweep — could not hold %d "
                    "protected reuse root(s) out of this sweep's own "
                    "populations; NOTHING was deleted: %s", len(roots), exc,
                )
                raise BlobCleanupGuardUnavailable(
                    f"could not protect {len(roots)} reuse root(s); "
                    f"nothing was deleted: {exc}"
                ) from exc
        logger.warning(
            "[backup_cleanup] cancelled sweep — %d snapshot(s) still back a "
            "live reuse chain; held intact (blobs, snapshot_items rows and "
            "counters all preserved; rehydrate the heir to reclaim): %s",
            held_n, ", ".join(sorted(str(x) for x in roots)),
        )

    stats = _SweepPassStats()
    budget = max(0, int(unkeyed_probe_budget))
    targets = [s for s in doomed if s not in roots]
    if not targets:
        return SweepBlobTeardown(held=frozenset(roots))

    # Checksum-less candidates are held back for ONE aggregated probe at
    # the end of the pass — that probe is the 87GB sequential scan, and
    # the whole point of this rewrite is that a pass buys at most one.
    # Capped at the pass budget, so this list is ≤500 entries.
    unkeyed_hold: List[BlobCandidate] = []
    unkeyed_seen: Set[str] = set()

    # --- 3. Stream the whole batch's candidates in bounded chunks -----
    try:
        async with session_factory() as read_session, contextlib.aclosing(
            stream_sweep_blob_candidates(
                read_session, snapshot_ids=targets,
                batch_rows=_SWEEP_ROW_BATCH,
            )
        ) as candidate_stream:
            async for batch in candidate_stream:
                stats.candidate_rows += len(batch)
                keyed: List[BlobCandidate] = []
                for cand in batch:
                    if cand.tenant_id is not None and cand.content_checksum:
                        keyed.append(cand)
                        continue
                    if cand.blob_path in unkeyed_seen:
                        continue
                    if len(unkeyed_seen) >= budget:
                        stats.note_unkeyed_overflow(cand.blob_path)
                        continue
                    unkeyed_seen.add(cand.blob_path)
                    unkeyed_hold.append(cand)
                if keyed:
                    await _apply_sweep_batch(
                        session_factory, doomed=doomed, roots=roots,
                        candidates=keyed, unkeyed_probe_budget=0,
                        container_for=container_for, store_for=store_for,
                        stats=stats,
                    )
        stream_ok = True
    except Exception as exc:
        # No inventory ⇒ no verdict ⇒ nothing may be reaped. Whatever was
        # already deleted stays deleted (idempotent), but every snapshot
        # keeps its rows so the next pass can finish the job.
        stream_ok = False
        stats.blocked.update(targets)
        logger.error(
            "[backup_cleanup] cancelled sweep — candidate stream failed for "
            "%d snapshot(s); no snapshot is reapable this pass (rows kept so "
            "the next pass retries): %s", len(targets), exc,
        )

    # --- 4. ONE aggregated unkeyed probe for the whole pass -----------
    # Skipped outright when the stream died: every snapshot is blocked, so
    # the probe could not authorise a single delete and would only spend
    # the pass's one 87GB sequential scan for nothing.
    if unkeyed_hold and stream_ok:
        await _apply_sweep_batch(
            session_factory, doomed=doomed, roots=roots,
            candidates=unkeyed_hold, unkeyed_probe_budget=budget,
            container_for=container_for, store_for=store_for, stats=stats,
        )
    if stats.unkeyed_overflow_paths or stats.unkeyed_overflow_extra:
        logger.warning(
            "[backup_cleanup] cancelled sweep — unkeyed probe budget spent: "
            "%d checksum-less path(s) RETAINED unverified this pass (each "
            "chunk of %d is one sequential scan of the 87GB snapshot_items "
            "heap; reconcile against orphan GC, or build "
            "ix_snapshot_items_blob_path)",
            len(stats.unkeyed_overflow_paths) + stats.unkeyed_overflow_extra,
            _PROBE_CHUNK,
        )

    reapable = {s for s in targets if s not in stats.blocked}
    if stats.paths_retained or stats.blocked or roots:
        logger.info(
            "[backup_cleanup] cancelled sweep — candidate_rows=%d held=%d "
            "reapable=%d blocked=%d blobs_deleted=%d blob_errors=%d "
            "blob_paths_retained=%d reasons=%s cache_rows_invalidated=%d "
            "unkeyed_probed=%d",
            stats.candidate_rows, len(roots), len(reapable),
            len(stats.blocked), stats.blobs_deleted, stats.blob_errors,
            stats.paths_retained, stats.reasons, stats.cache_cleared,
            stats.probed_total,
        )
    return SweepBlobTeardown(
        held=frozenset(roots),
        reapable=frozenset(reapable),
        blocked=frozenset(stats.blocked),
        blobs_deleted=stats.blobs_deleted,
        blob_errors=stats.blob_errors,
        blobs_retained=stats.blobs_retained,
        blob_paths_retained=stats.paths_retained,
        retained_reasons=stats.reasons,
        cache_rows_invalidated=stats.cache_cleared,
        unkeyed_probed=stats.probed_total,
        candidate_rows=stats.candidate_rows,
    )


@dataclass
class _SweepPassStats:
    """Mutable accumulator for one sweep pass. Every field is O(1) or
    explicitly capped, so the pass's own bookkeeping cannot be the thing
    that OOMs the 512Mi pod."""
    blocked: Set[UUID] = field(default_factory=set)
    blobs_deleted: int = 0
    blob_errors: int = 0
    blobs_retained: int = 0
    paths_retained: int = 0
    cache_cleared: int = 0
    probed_total: int = 0
    candidate_rows: int = 0
    reasons: Dict[str, int] = field(default_factory=dict)
    unkeyed_overflow_paths: Set[str] = field(default_factory=set)
    unkeyed_overflow_extra: int = 0

    def note_unkeyed_overflow(self, path: str) -> None:
        """A checksum-less path past the pass budget: RETAINED, unverified."""
        if path in self.unkeyed_overflow_paths:
            return
        self.blobs_retained += 1
        if len(self.unkeyed_overflow_paths) < _UNKEYED_OVERFLOW_TRACK:
            self.unkeyed_overflow_paths.add(path)
        else:
            # Past the tracking cap the histogram degrades from distinct
            # paths to occurrences. Telemetry only.
            self.unkeyed_overflow_extra += 1
        self.paths_retained += 1
        self.reasons[RETAIN_UNVERIFIABLE] = (
            self.reasons.get(RETAIN_UNVERIFIABLE, 0) + 1
        )


async def _apply_sweep_batch(
    session_factory: Callable[[], AsyncSession],
    *,
    doomed: Sequence[UUID],
    roots: Set[UUID],
    candidates: List[BlobCandidate],
    unkeyed_probe_budget: int,
    container_for: Callable[[UUID], str],
    store_for: Callable[[Any], Any],
    stats: _SweepPassStats,
) -> None:
    """Plan → invalidate → delete, for one bounded slice of a sweep pass.

    ``doomed`` is always the WHOLE pass's id list even though
    ``candidates`` is a slice: a sibling doomed snapshot that shares a
    path must not look like a live referrer, or nothing is ever
    reclaimed.

    Failure isolation is per SNAPSHOT, not per batch, wherever it can be:
    a probe failure blocks every snapshot represented in the slice
    (that is the only honest answer — we have no verdict for any of
    them), but a chat_url_cache failure retries per snapshot so one bad
    snapshot cannot cost the pass its other reclamations.
    """
    if not candidates:
        return
    try:
        async with session_factory() as session:
            plan = await select_deletable_blob_paths(
                session,
                doomed_snapshot_ids=doomed,
                candidates=candidates,
                protected_roots=roots,
                unkeyed_probe_budget=unkeyed_probe_budget,
            )
    except Exception as exc:
        # Keep the bytes AND the rows — see SweepBlobTeardown.blocked.
        hit = {c.snapshot_id for c in candidates if c.snapshot_id is not None}
        stats.blocked.update(hit)
        stats.blobs_retained += len(candidates)
        logger.warning(
            "[backup_cleanup] cancelled sweep — reference check failed for a "
            "%d-candidate slice covering %d snapshot(s); their blobs AND rows "
            "are left for the next pass: %s", len(candidates), len(hit), exc,
        )
        return

    stats.probed_total += plan.unkeyed_probed
    stats.paths_retained += len(plan.retained)
    for reason, n in plan.retained_reasons.items():
        stats.reasons[reason] = stats.reasons.get(reason, 0) + n

    by_snapshot: Dict[Optional[UUID], List[BlobCandidate]] = {}
    for cand in candidates:
        if cand.blob_path in plan.deletable:
            by_snapshot.setdefault(cand.snapshot_id, []).append(cand)
        else:
            stats.blobs_retained += 1
    if not by_snapshot:
        return

    # Clear cross-snapshot chat_url_cache pointers BEFORE the bytes go: a
    # leaked blob is storage, a live pointer to a dead blob is a future
    # unrestorable backup. One UPDATE for the slice; only on failure do we
    # pay a per-snapshot retry, and only to find out which snapshot to
    # block.
    cleared: Set[Optional[UUID]] = set()
    try:
        async with session_factory() as session:
            stats.cache_cleared += await invalidate_chat_url_cache_paths(
                session,
                blob_paths=[c.blob_path for cs in by_snapshot.values()
                            for c in cs],
                tenant_ids=[c.tenant_id for cs in by_snapshot.values()
                            for c in cs if c.tenant_id is not None],
            )
        cleared.update(by_snapshot)
    except Exception as exc:
        logger.warning(
            "[backup_cleanup] cancelled sweep — batched chat_url_cache "
            "invalidation failed; retrying per snapshot to isolate: %s", exc,
        )
        for sid, cands in by_snapshot.items():
            try:
                async with session_factory() as session:
                    stats.cache_cleared += (
                        await invalidate_chat_url_cache_paths(
                            session,
                            blob_paths=[c.blob_path for c in cands],
                            tenant_ids=[c.tenant_id for c in cands
                                        if c.tenant_id is not None],
                        )
                    )
                cleared.add(sid)
            except Exception as inner:
                stats.blocked.add(sid)
                stats.blobs_retained += len(cands)
                logger.warning(
                    "[backup_cleanup] cancelled sweep — chat_url_cache "
                    "invalidation failed for snapshot %s; its blobs AND rows "
                    "are left for the next pass: %s", sid, inner,
                )

    # --- Deletes. No DB session held here. ---------------------------
    for sid, cands in by_snapshot.items():
        if sid not in cleared or sid in stats.blocked:
            continue
        container = container_for(sid)
        for cand in cands:
            try:
                store = store_for(cand.backend_id)
                await store.delete(container, cand.blob_path)
                stats.blobs_deleted += 1
                # This process's blob_dedup TTL-LRU may still hand the
                # just-deleted path to the next backup as a dedup hit.
                _evict_dedup_cache(cand.tenant_id, cand.content_checksum)
            except Exception as exc:
                stats.blob_errors += 1
                logger.warning(
                    "[backup_cleanup] cancelled sweep — blob delete failed "
                    "(snapshot=%s container=%s path=%s): %s",
                    sid, container, cand.blob_path, exc,
                )


async def cleanup_cancelled_snapshots(
    *,
    job_id: UUID,
    session_factory: Callable[[], AsyncSession],
    shard: Any,
    container_resolver: Callable[[SnapshotItem], str],
    delete_db_rows: bool = True,
    batch_size: int = 500,
    unkeyed_probe_budget: int = _UNKEYED_RUN_BUDGET,
) -> Dict[str, int]:
    """Delete partial backup artifacts for a cancelled job.

    Parameters
    ----------
    job_id
        The cancelled Job's UUID. Snapshots are matched via
        ``Snapshot.job_id`` — populated by create_snapshot on every
        worker write. (``last_backup_job_id`` is the Resource field;
        Snapshot's own FK column is ``job_id``.)
    session_factory
        ``async_session_factory`` — opens a fresh DB session per batch
        so the cleanup doesn't hold one connection for the whole run.
    shard
        Storage shard (Azure / SeaweedFS) with an async
        ``delete_blob(container, path)`` method.
    container_resolver
        Callable that turns a ``SnapshotItem`` into the container the
        item's blob lives in. Workers know this mapping (mail container
        vs files container vs sharepoint container vs exports).
    delete_db_rows
        When True (default) snapshot_items rows are deleted after their
        blobs. Set False if you want to keep audit history.
    batch_size
        Number of items processed per session — caps memory + bounds
        DB transaction size.
    unkeyed_probe_budget
        Ceiling on candidate paths this RUN may check with the exact
        blob_path probe — the fallback for candidates that carry no
        content_checksum (see ``select_deletable_blob_paths``). Each
        chunk of 500 such paths is one unindexed sequential scan of the
        87GB ``snapshot_items`` heap, so the default caps a whole run at
        ONE. Paths past the budget are RETAINED, never deleted
        unchecked.

    Returns
    -------
    dict with counts. ``blobs_deleted`` / ``blobs_retained`` are
    occurrence-shaped (delete calls made / skipped);
    ``blob_paths_retained`` is the DISTINCT retained path count and
    ``retained_reasons`` its reason histogram — those are the numbers
    to reconcile against orphan GC, since one path can back many items.
    ``cache_rows_invalidated`` counts chat_url_cache rows whose dead
    ``blob_path`` pointer was cleared. ``snapshots_skipped_reuse``
    counts target snapshots whose blobs, rows and counters were left
    intact because a live reuse-chain descendant still reads their
    inventory; those are also HELD out of the backup-scheduler sweep's
    reach (``hold_protected_reuse_roots``) and marked
    ``extra_data.reuse_protected`` for the operator who reclaims them.

    Raises ``BlobCleanupGuardUnavailable`` — having deleted nothing — if
    the reuse-chain protection can't be resolved or can't be made
    durable.
    """
    stats: Dict[str, Any] = {
        "snapshots": 0, "items": 0, "blobs_deleted": 0, "blob_errors": 0,
        "blobs_retained": 0, "blob_paths_retained": 0,
        "cache_rows_invalidated": 0, "retained_reasons": {},
        "snapshots_skipped_reuse": 0,
    }

    # --- 1. Find target snapshots --------------------------------------
    async with session_factory() as session:
        stmt = (
            select(Snapshot.id)
            .where(Snapshot.job_id == job_id)
            .where(Snapshot.status.in_([
                SnapshotStatus.IN_PROGRESS,
                SnapshotStatus.COMPLETED,
            ]))
        )
        snapshot_ids: List[UUID] = list(
            (await session.execute(stmt)).scalars().all()
        )

    if not snapshot_ids:
        logger.info("[backup_cleanup] no snapshots to clean for job %s", job_id)
        return stats

    stats["snapshots"] = len(snapshot_ids)
    logger.info(
        "[backup_cleanup] job %s — cleaning %d snapshot(s)",
        job_id, len(snapshot_ids),
    )

    # Reuse-chain descendants that outlive this teardown. Resolved once
    # per run (not per batch) — it depends only on the snapshot set.
    #
    # FAIL CLOSED. Without this set we cannot tell whether a target
    # snapshot is the chain root a LIVE descendant reads its inventory
    # out of, which makes step 3's DELETE FROM snapshot_items and the
    # bytes_total/item_count zeroing every bit as destructive as a blob
    # delete — retaining only the bytes would leave them present but
    # unreachable. So: delete NOTHING and surface the failure. Both
    # callers (job-service's post-cancel task, backup-worker's
    # CANCELLED-intake task) log the traceback, and every teardown here
    # is idempotent, so a retry loses nothing.
    protected_roots: Set[UUID]
    async with session_factory() as session:
        try:
            protected_roots = await resolve_protected_reuse_roots(
                session, snapshot_ids,
            )
        except Exception as exc:
            logger.error(
                "[backup_cleanup] job %s — reuse-chain protection could not "
                "be resolved; NOTHING was deleted (no blobs, no "
                "snapshot_items rows, no status flip): %s", job_id, exc,
            )
            raise BlobCleanupGuardUnavailable(
                f"reuse-chain protection unresolved for job {job_id}; "
                f"no blobs or rows were deleted: {exc}"
            ) from exc
    if protected_roots:
        logger.warning(
            "[backup_cleanup] job %s — %d doomed snapshot(s) still back a "
            "live reuse chain; their blobs are RETAINED (rehydrate the heir "
            "to reclaim): %s",
            job_id, len(protected_roots),
            ", ".join(sorted(str(x) for x in protected_roots)),
        )
        # Make the protection DURABLE before anything is deleted.
        # Leaving these rows IN_PROGRESS / cancel-marked would hand them
        # to the unguarded 30s backup-scheduler sweep, which deletes the
        # very blobs and rows we just refused to touch — see
        # hold_protected_reuse_roots. Fail closed if it doesn't stick:
        # a run that can't protect them must not start deleting.
        async with session_factory() as session:
            try:
                held = await hold_protected_reuse_roots(
                    session, sorted(protected_roots, key=str),
                )
            except Exception as exc:
                logger.error(
                    "[backup_cleanup] job %s — could not hold %d protected "
                    "reuse root(s) out of the cancelled-snapshot sweep's "
                    "reach; NOTHING was deleted: %s",
                    job_id, len(protected_roots), exc,
                )
                raise BlobCleanupGuardUnavailable(
                    f"could not protect reuse roots for job {job_id}; "
                    f"no blobs or rows were deleted: {exc}"
                ) from exc
        logger.warning(
            "[backup_cleanup] job %s — held %d reuse-protected snapshot(s) "
            "(status flipped out of IN_PROGRESS, cancelled_at moved to "
            "reuse_protected_cancelled_at, rows + counters intact)",
            job_id, held,
        )

    # --- 2. Stream-delete blobs in batches -----------------------------
    # Keyset pagination on item id so a worker crash mid-cleanup can
    # resume cheaply on the next call (idempotent: missing blobs = no-op).
    #
    # The unkeyed (checksum-less) probe budget is spent across the WHOLE
    # run, not per batch: batch_size and the guard's per-call cap are
    # both 500, so a per-batch cap would never trip and a 200k-item
    # teardown would still issue ~400 unindexed sequential scans of the
    # 87GB snapshot_items heap. Paths past the budget are retained.
    unkeyed_remaining = max(0, int(unkeyed_probe_budget))
    last_id: Optional[UUID] = None
    while True:
        async with session_factory() as session:
            stmt = (
                select(SnapshotItem)
                .where(SnapshotItem.snapshot_id.in_(snapshot_ids))
                .order_by(SnapshotItem.id)
                .limit(batch_size)
            )
            if last_id is not None:
                stmt = stmt.where(SnapshotItem.id > last_id)
            items: List[SnapshotItem] = list(
                (await session.execute(stmt)).scalars().all()
            )
            for it in items:
                session.expunge(it)

            deletable: Set[str] = set()
            batch: List[BlobCandidate] = []
            guard_ok = False
            if items:
                # Reference-safety gate, resolved in the same session that
                # read the batch. On failure we retain every path in the
                # batch: an orphan blob is recoverable, a deleted shared
                # blob is not.
                batch = blob_candidates_from_items(items)
                try:
                    plan = await select_deletable_blob_paths(
                        session,
                        doomed_snapshot_ids=snapshot_ids,
                        candidates=batch,
                        protected_roots=protected_roots,
                        unkeyed_probe_budget=unkeyed_remaining,
                    )
                    unkeyed_remaining -= plan.unkeyed_probed
                    # Clear cross-snapshot chat_url_cache pointers BEFORE
                    # the bytes go. If that fails we keep the bytes: a
                    # leaked blob is storage, a live pointer to a dead
                    # blob is a future unrestorable backup.
                    stats["cache_rows_invalidated"] += (
                        await invalidate_chat_url_cache_paths(
                            session,
                            blob_paths=plan.deletable,
                            tenant_ids=[
                                c.tenant_id for c in batch
                                if c.tenant_id is not None
                            ],
                        )
                    )
                    deletable = set(plan.deletable)
                    guard_ok = True
                    stats["blob_paths_retained"] += len(plan.retained)
                    for reason, n in plan.retained_reasons.items():
                        stats["retained_reasons"][reason] = (
                            stats["retained_reasons"].get(reason, 0) + n
                        )
                except Exception as exc:
                    deletable = set()
                    logger.warning(
                        "[backup_cleanup] reference check failed for job %s "
                        "— retaining this batch's blobs: %s",
                        job_id, exc,
                    )

        if not items:
            break

        if not guard_ok:
            # Whole batch retained — the sharer probe or the cache
            # invalidation raised. Account the paths anyway so the leak
            # is visible rather than silent. (Reaping these snapshots'
            # rows is still safe: they are doomed, they are not
            # protected roots, and any live referrer of a retained path
            # has its own snapshot_items row.)
            n_unverified = len({
                c.blob_path for c in blob_candidates_from_items(items)
            })
            if n_unverified:
                stats["blob_paths_retained"] += n_unverified
                stats["retained_reasons"][RETAIN_UNVERIFIABLE] = (
                    stats["retained_reasons"].get(RETAIN_UNVERIFIABLE, 0)
                    + n_unverified
                )

        for it in items:
            stats["items"] += 1

            # Primary item blob
            blob_path = getattr(it, "blob_path", None)
            if blob_path and blob_path not in deletable:
                stats["blobs_retained"] += 1
            elif blob_path:
                container = container_resolver(it)
                try:
                    await shard.delete_blob(container, blob_path)
                    stats["blobs_deleted"] += 1
                    # This process's blob_dedup TTL-LRU may still be
                    # holding (tenant, checksum) -> this path for up to
                    # BLOB_DEDUP_LRU_HIT_TTL_S (5 min default) and would
                    # hand it to the next backup as a dedup hit. Evicting
                    # only fixes THIS process — see the module docstring
                    # for the cross-process window that remains.
                    _evict_dedup_cache(
                        getattr(it, "tenant_id", None),
                        getattr(it, "content_checksum", None),
                    )
                except Exception as exc:
                    stats["blob_errors"] += 1
                    logger.warning(
                        "[backup_cleanup] blob delete failed (%s/%s): %s",
                        container, blob_path, exc,
                    )

            # Attachment blobs (stored separately for mail items)
            extra = getattr(it, "extra_data", None) or {}
            att_paths = (
                extra.get("attachment_blob_paths") if isinstance(extra, dict) else None
            ) or []
            if att_paths:
                container = container_resolver(it)
                for ap in att_paths:
                    if ap not in deletable:
                        stats["blobs_retained"] += 1
                        continue
                    try:
                        await shard.delete_blob(container, ap)
                        stats["blobs_deleted"] += 1
                    except Exception as exc:
                        stats["blob_errors"] += 1
                        logger.warning(
                            "[backup_cleanup] attachment delete failed (%s/%s): %s",
                            container, ap, exc,
                        )

        last_id = items[-1].id

    # --- 3. Delete snapshot_items + mark snapshots CANCELLED -----------
    # A doomed snapshot that is still the reuse-chain root for a LIVE
    # descendant is skipped WHOLESALE (and was HELD above). Retaining its
    # blobs is only half the job: the descendant reads its inventory out
    # of the root's snapshot_items rows (shared.snapshot_reuse), so
    # wiping the rows or zeroing the counters leaves the bytes present
    # but unreachable — the same unrestorable backup by another route.
    # Reclaiming these properly means rehydrating the heir first
    # (shared.retention_cleanup._rehydrate_reuse_heir, as job-service's
    # _revert_snapshot_storage and the retention sweep both do); that is
    # an operator/owning-path action, not something to bolt onto a
    # best-effort reaper.
    reapable_ids = [s for s in snapshot_ids if s not in protected_roots]
    stats["snapshots_skipped_reuse"] = len(snapshot_ids) - len(reapable_ids)
    if not reapable_ids:
        logger.warning(
            "[backup_cleanup] job %s — every target snapshot still backs a "
            "live reuse chain; left fully intact (rehydrate the heir to "
            "reclaim)", job_id,
        )
    else:
        async with session_factory() as session:
            if delete_db_rows:
                await session.execute(sa_delete(SnapshotChatInventory).where(
                    SnapshotChatInventory.snapshot_id.in_(reapable_ids)))
                await session.execute(
                    sa_delete(SnapshotItem).where(
                        SnapshotItem.snapshot_id.in_(reapable_ids)
                    )
                )
            await session.execute(
                sa_update(Snapshot)
                .where(Snapshot.id.in_(reapable_ids))
                .values(
                    status=SnapshotStatus.CANCELLED
                    if hasattr(SnapshotStatus, "CANCELLED")
                    else SnapshotStatus.FAILED,
                    bytes_total=0,
                    item_count=0,
                )
            )
            await session.commit()

    logger.info(
        "[backup_cleanup] job %s — done: snapshots=%d items=%d blobs_deleted=%d "
        "blob_errors=%d blobs_retained=%d blob_paths_retained=%d reasons=%s "
        "cache_rows_invalidated=%d snapshots_skipped_reuse=%d",
        job_id,
        stats["snapshots"], stats["items"],
        stats["blobs_deleted"], stats["blob_errors"], stats["blobs_retained"],
        stats["blob_paths_retained"], stats["retained_reasons"],
        stats["cache_rows_invalidated"], stats["snapshots_skipped_reuse"],
    )
    return stats


def default_container_resolver(item: SnapshotItem) -> str:
    """Best-effort container picker by item type.

    Workers call this when they don't have explicit container context
    (e.g. cleanup-from-CLI). Production callers should pass a
    tenant-scoped resolver that knows the actual container names from
    ``azure_storage_manager.get_container_name``.
    """
    t = (getattr(item, "item_type", "") or "").upper()
    if t.startswith("EMAIL"):
        return "email"
    if t in {"USER_CONTACT", "USER_CALENDAR", "CALENDAR_EVENT"}:
        return "mailbox"
    if t.startswith("SHAREPOINT"):
        return "sharepoint"
    if t in {"FILE", "ONEDRIVE_FILE", "FILE_VERSION"}:
        return "files"
    return "files"
