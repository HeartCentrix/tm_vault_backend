"""Saved protected-size reads and bounded, restartable retained-reference work.

The journal is a set of committed invalidations, never a commit-order watermark.
Every page and its winner deltas commit together. Source writes only append
journal batches; they never lock the working resource counter.
"""

import json
import hashlib
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

METRIC_VERSION = "retained-logical-v1.1"
ELIGIBLE = "('COMPLETED','PARTIAL','PENDING_DELETION')"
optimized_rollup = f"""
SELECT COALESCE(sum(size),0) FROM (
 SELECT DISTINCT ON (COALESCE(i.external_id,i.id::text)) COALESCE(i.content_size,0) size
 FROM snapshot_items i JOIN snapshots s ON s.id=i.snapshot_id
 WHERE s.resource_id=:rid AND s.status IN {ELIGIBLE} AND NOT COALESCE(i.is_deleted,false)
 ORDER BY COALESCE(i.external_id,i.id::text),i.created_at DESC,i.id DESC
) winners
"""


def _route_pending(resource):
    return f"""EXISTS(SELECT 1 FROM protected_size_snapshots registry
      JOIN protected_size_event_snapshots route ON route.snapshot_id=registry.snapshot_id
      WHERE registry.resource_id={resource}
      AND NOT EXISTS(SELECT 1 FROM protected_size_retired retired WHERE retired.event_id=route.event_id)
      AND NOT EXISTS(SELECT 1 FROM protected_size_targets ack WHERE ack.event_id=route.event_id
        AND ack.resource_id={resource} AND ack.acknowledged_at IS NOT NULL))"""


SUMMARY_SQL = f"""
      WITH scoped AS (
        SELECT r.id,p.bytes,p.computed_at,p.ledger_published,
          EXISTS(SELECT 1 FROM protected_size_targets t WHERE t.resource_id=r.id AND t.acknowledged_at IS NULL)
          OR EXISTS(SELECT 1 FROM protected_size_jobs j WHERE j.resource_id=r.id) OR {_route_pending('r.id')} pending
        FROM resources r LEFT JOIN resource_protected_bytes p ON p.resource_id=r.id
        WHERE (:all_tenants OR r.tenant_id=ANY(CAST(:tenants AS uuid[])))
          AND (:all_types OR r.type::text=ANY(CAST(:types AS text[])))
          AND EXISTS(SELECT 1 FROM protected_size_snapshots s WHERE s.resource_id=r.id AND s.eligible)
      ) SELECT count(*) eligible,
        count(*) FILTER(WHERE ledger_published AND NOT pending) fresh,
        count(*) FILTER(WHERE ledger_published AND pending) stale,
        count(*) FILTER(WHERE NOT COALESCE(ledger_published,false)) missing,
        sum(bytes) FILTER(WHERE ledger_published) bytes,
        min(computed_at) FILTER(WHERE ledger_published) as_of,
        (SELECT NOT discovery_done FROM protected_size_control WHERE singleton) initializing
      FROM scoped

"""


async def read_summary(
    db,
    *,
    tenant_ids: tuple[UUID, ...] | None = None,
    service_types: tuple[str, ...] | None = None,
) -> dict:
    """One compact query. None tenant scope is explicitly privileged global.

    totalBytes sums only published values; None means an uncertain baseline.
    A partial saved total can shrink, so it is never advertised as a lower bound.
    """
    row = (
        (
            await db.execute(
                text(SUMMARY_SQL),
                {
                    "all_tenants": tenant_ids is None,
                    "tenants": list(tenant_ids or ()),
                    "all_types": service_types is None,
                    "types": list(service_types or ()),
                },
            )
        )
        .mappings()
        .one()
    )
    eligible, fresh, stale, missing = [
        int(row[k]) for k in ("eligible", "fresh", "stale", "missing")
    ]
    initializing = (
        bool(row["initializing"]) and tenant_ids != () and service_types != ()
    )
    freshness = (
        "initializing"
        if initializing
        else "missing"
        if missing
        else "stale"
        if stale
        else "fresh"
    )
    return {
        "totalBytes": None
        if (initializing or missing) and not (fresh or stale)
        else int(row["bytes"] or 0),
        "eligibleCount": eligible,
        "freshCount": fresh,
        "staleCount": stale,
        "missingCount": missing,
        "partial": bool(initializing or missing or stale),
        "stale": bool(stale),
        "freshness": freshness,
        "asOf": row["as_of"].isoformat() if row["as_of"] else None,
        "initializingScope": initializing,
        "metricVersion": METRIC_VERSION,
    }


async def _timeout(db, ms):
    await db.execute(text(f"SET LOCAL statement_timeout={int(ms)}"))
    await db.execute(text(f"SET LOCAL lock_timeout={min(1000,int(ms))}"))


async def _claim(factory, owner, ms):
    async with factory.begin() as db:
        await _timeout(db, ms)
        control = (
            (
                await db.execute(
                    text(
                        "SELECT * FROM protected_size_control WHERE singleton FOR UPDATE SKIP LOCKED"
                    )
                )
            )
            .mappings()
            .first()
        )
        if not control:
            return {"status": "busy"}
        if control["owner"] and await db.scalar(
            text("SELECT :until > clock_timestamp()"), {"until": control["lease_until"]}
        ):
            return {"status": "busy"}
        if not control["discovery_done"]:
            work = {
                "kind": "discovery",
                "cursor": control["discovery_cursor"],
                "discovery_epoch": control["discovery_epoch"],
                "discovery_phase": control["discovery_phase"],
            }
        else:
            rid = await db.scalar(
                text(f"""SELECT r.id FROM resources r LEFT JOIN resource_protected_bytes p ON p.resource_id=r.id
              LEFT JOIN protected_size_tenant_turns turn ON turn.tenant_id=r.tenant_id
              WHERE COALESCE(p.next_attempt,'epoch')<=clock_timestamp() AND (
                EXISTS(SELECT 1 FROM protected_size_targets t WHERE t.resource_id=r.id AND t.acknowledged_at IS NULL)
                OR EXISTS(SELECT 1 FROM protected_size_jobs j WHERE j.resource_id=r.id)
                OR {_route_pending('r.id')} OR (p.resource_id IS NOT NULL AND NOT p.ledger_published))
              ORDER BY COALESCE(turn.last_turn,'epoch'),COALESCE(p.last_turn,'epoch'),r.id LIMIT 1""")
            )
            if rid is None:
                work = {"kind": "maintenance"}
            else:
                await db.execute(
                    text(
                        "INSERT INTO resource_protected_bytes(resource_id) VALUES(:r) ON CONFLICT DO NOTHING"
                    ),
                    {"r": rid},
                )
                await db.execute(
                    text("""INSERT INTO protected_size_targets(event_id,resource_id)
                  SELECT route.event_id,:r FROM protected_size_snapshots registry
                  JOIN protected_size_event_snapshots route ON route.snapshot_id=registry.snapshot_id
                  WHERE registry.resource_id=:r
                    AND NOT EXISTS(SELECT 1 FROM protected_size_retired retired WHERE retired.event_id=route.event_id)
                    AND NOT EXISTS(SELECT 1 FROM protected_size_targets t WHERE t.event_id=route.event_id AND t.resource_id=:r)
                  GROUP BY route.event_id ORDER BY route.event_id LIMIT 1 ON CONFLICT DO NOTHING"""),
                    {"r": rid},
                )
                rf = await db.scalar(
                    text(
                        "UPDATE resource_protected_bytes SET fence=fence+1 WHERE resource_id=:r RETURNING fence"
                    ),
                    {"r": rid},
                )
                event = (
                    (
                        await db.execute(
                            text("""SELECT j.*,t.processed_offset FROM protected_size_targets t JOIN protected_size_journal j ON j.id=t.event_id
                   WHERE t.resource_id=:r AND t.acknowledged_at IS NULL ORDER BY j.id LIMIT 1"""),
                            {"r": rid},
                        )
                    )
                    .mappings()
                    .first()
                )
                job = (
                    (
                        await db.execute(
                            text(
                                "SELECT * FROM protected_size_jobs WHERE resource_id=:r ORDER BY snapshot_id LIMIT 1"
                            ),
                            {"r": rid},
                        )
                    )
                    .mappings()
                    .first()
                )
                prefer = await db.scalar(
                    text(
                        "SELECT prefer_build FROM resource_protected_bytes WHERE resource_id=:r"
                    ),
                    {"r": rid},
                )
                if job and (prefer or not event):
                    event = None
                elif event:
                    job = None
                work = {
                    "kind": "event" if event else "job" if job else "publish",
                    "resource_id": rid,
                    "resource_fence": rf,
                    "event": dict(event) if event else None,
                    "job": dict(job) if job else None,
                }
        fence = await db.scalar(
            text("""UPDATE protected_size_control SET owner=:owner,
          lease_until=clock_timestamp()+(:lease_ms * interval '1 millisecond'),fence=fence+1 WHERE singleton RETURNING fence"""),
            {"owner": owner, "lease_ms": ms * 4 + 5000},
        )
        work.update(
            owner=owner,
            fence=fence,
            instance_id=control["instance_id"],
            barrier=str(
                await db.scalar(text("SELECT pg_current_wal_insert_lsn()::text"))
            ),
        )
        return work


async def _source_page(factory, work, page_size, ms, replica):
    async with factory.begin() as db:
        # The MVCC-visible claim fence proves prior commits are in this source
        # snapshot; a volatile replay LSN alone can advance after snapshot creation.
        await db.execute(
            text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        )
        await _timeout(db, ms)
        if replica:
            ready = await db.scalar(
                text("""SELECT instance_id=CAST(:instance AS uuid) AND fence>=:fence AND (NOT pg_is_in_recovery() OR
                pg_last_wal_replay_lsn() >= CAST(CAST(:lsn AS text) AS pg_lsn)) FROM protected_size_control WHERE singleton"""),
                {
                    "lsn": work["barrier"],
                    "instance": work["instance_id"],
                    "fence": work["fence"],
                },
            )
            if not ready:
                return None
        if work["kind"] == "discovery":
            if work["discovery_phase"] == "registry":
                return list(
                    (
                        await db.execute(
                            text(
                                """SELECT p.snapshot_id id,p.resource_id,p.revision,
                  s.id IS NOT NULL present,COALESCE(s.status IN {ELIGIBLE},false) eligible
                  FROM protected_size_snapshots p LEFT JOIN snapshots s ON s.id=p.snapshot_id
                  WHERE (CAST(:cursor AS uuid) IS NULL OR p.snapshot_id>CAST(:cursor AS uuid))
                  ORDER BY p.snapshot_id LIMIT :n""".replace("{ELIGIBLE}", ELIGIBLE)
                            ),
                            {"cursor": work["cursor"], "n": page_size},
                        )
                    ).mappings()
                )
            return list(
                (
                    await db.execute(
                        text(f"""SELECT s.id,s.resource_id,s.status IN {ELIGIBLE} eligible,
              p.revision FROM snapshots s LEFT JOIN protected_size_snapshots p ON p.snapshot_id=s.id
              WHERE (CAST(:cursor AS uuid) IS NULL OR s.id>CAST(:cursor AS uuid)) ORDER BY s.id LIMIT :n"""),
                        {"cursor": work["cursor"], "n": page_size},
                    )
                ).mappings()
            )
        if work["kind"] == "event":
            event = work["event"]
            key = "item_ids" if event["kind"] == "items" else "snapshot_ids"
            ids = event[key][
                event["processed_offset"] : event["processed_offset"] + page_size
            ]
            if event["kind"] == "snapshots":
                return ids
            snapshots = event["snapshot_ids"][
                event["processed_offset"] : event["processed_offset"] + page_size
            ]
            keys = list(zip(snapshots, ids))
            return {
                "ids": ids,
                "keys": keys,
                "rows": list(
                    (
                        await db.execute(
                            text(f"""SELECT i.id item_id,i.snapshot_id,
              COALESCE(i.external_id,i.id::text) identity,i.created_at,COALESCE(i.content_size,0) size
              FROM unnest(CAST(:snapshots AS uuid[]),CAST(:ids AS uuid[])) k(snapshot_id,item_id)
              JOIN snapshot_items i ON i.snapshot_id=k.snapshot_id AND i.id=k.item_id
              JOIN snapshots s ON s.id=i.snapshot_id WHERE s.resource_id=:r AND s.status IN {ELIGIBLE}
              AND NOT COALESCE(i.is_deleted,false)"""),
                            {
                                "snapshots": snapshots,
                                "ids": ids,
                                "r": work["resource_id"],
                            },
                        )
                    ).mappings()
                ),
            }
        if work["kind"] == "job":
            job = work["job"]
            current = await db.scalar(
                text(
                    f"SELECT resource_id=:r AND status IN {ELIGIBLE} FROM snapshots WHERE id=:s"
                ),
                {"r": work["resource_id"], "s": job["snapshot_id"]},
            )
            if not current or job["phase"] == "prune":
                refs = list(
                    (
                        await db.execute(
                            text("""SELECT item_id FROM protected_size_refs WHERE resource_id=:r AND snapshot_id=:s
                  AND (CAST(:cursor AS uuid) IS NULL OR item_id>CAST(:cursor AS uuid)) ORDER BY item_id LIMIT :n"""),
                            {
                                "r": work["resource_id"],
                                "s": job["snapshot_id"],
                                "cursor": job["cursor"],
                                "n": page_size,
                            },
                        )
                    ).mappings()
                )
                existing = []
                if current and refs:
                    existing = list(
                        (
                            await db.execute(
                                text(f"""SELECT i.id item_id,i.snapshot_id,
                      COALESCE(i.external_id,i.id::text) identity,i.created_at,COALESCE(i.content_size,0) size
                      FROM snapshot_items i JOIN snapshots s ON s.id=i.snapshot_id WHERE i.snapshot_id=:s
                      AND i.id=ANY(CAST(:ids AS uuid[])) AND s.resource_id=:r AND s.status IN {ELIGIBLE}
                      AND NOT COALESCE(i.is_deleted,false)"""),
                                {
                                    "s": job["snapshot_id"],
                                    "ids": [r["item_id"] for r in refs],
                                    "r": work["resource_id"],
                                },
                            )
                        ).mappings()
                    )
                return {"remove": True, "rows": refs, "current": existing}
            rows = list(
                (
                    await db.execute(
                        text("""SELECT id item_id,snapshot_id,COALESCE(external_id,id::text) identity,
               created_at,COALESCE(content_size,0) size,COALESCE(is_deleted,false) deleted
               FROM snapshot_items WHERE snapshot_id=:s AND (CAST(:cursor AS uuid) IS NULL OR id>CAST(:cursor AS uuid)) ORDER BY id LIMIT :n"""),
                        {
                            "s": job["snapshot_id"],
                            "cursor": job["cursor"],
                            "n": page_size,
                        },
                    )
                ).mappings()
            )
            return {"remove": False, "rows": rows}
        return []


def _identity_hash(identity):
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


async def _references(db, rid, ids, rows, snapshot_id=None, item_keys=None):
    """Narrow hash probes resolve exact identities, including hash collisions."""
    keys = {r["identity"] for r in rows}
    hashes = {key: _identity_hash(key) for key in keys}
    identities = {}
    if hashes:
        known = (
            await db.execute(
                text("""SELECT id,identity FROM protected_size_identities
          WHERE resource_id=:r AND identity_hash=ANY(CAST(:hashes AS text[]))"""),
                {"r": rid, "hashes": list(set(hashes.values()))},
            )
        ).mappings()
        identities = {r["identity"]: r["id"] for r in known if r["identity"] in keys}
        missing = [
            {"identity": key, "identity_hash": hashes[key]}
            for key in keys
            if key not in identities
        ]
        if missing:
            inserted = (
                await db.execute(
                    text("""INSERT INTO protected_size_identities(resource_id,identity_hash,identity)
              SELECT :r,x.identity_hash,x.identity FROM jsonb_to_recordset(CAST(:rows AS jsonb))
              x(identity_hash text,identity text) RETURNING id,identity"""),
                    {"r": rid, "rows": json.dumps(missing)},
                )
            ).mappings()
            identities.update({r["identity"]: r["id"] for r in inserted})
    pairs = item_keys if item_keys is not None else [(snapshot_id, iid) for iid in ids]
    scope = {
        "r": rid,
        "keys": json.dumps(
            [{"snapshot_id": str(sid), "item_id": str(iid)} for sid, iid in pairs]
        ),
    }
    old = list(
        (
            await db.execute(
                text("""SELECT DISTINCT f.identity_id FROM
      jsonb_to_recordset(CAST(:keys AS jsonb)) k(snapshot_id uuid,item_id uuid)
      JOIN protected_size_refs f ON f.resource_id=:r AND f.snapshot_id=k.snapshot_id AND f.item_id=k.item_id"""),
                scope,
            )
        ).scalars()
    )
    affected = list(set(old) | set(identities.values()))
    if not affected:
        return 0
    args = {"r": rid, "keys": affected}
    before = await db.scalar(
        text(
            "SELECT COALESCE(sum(size),0) FROM protected_size_winners WHERE resource_id=:r AND identity_id=ANY(CAST(:keys AS bigint[]))"
        ),
        args,
    )
    await db.execute(
        text("""DELETE FROM protected_size_refs f USING
      jsonb_to_recordset(CAST(:keys AS jsonb)) k(snapshot_id uuid,item_id uuid)
      WHERE f.resource_id=:r AND f.snapshot_id=k.snapshot_id AND f.item_id=k.item_id"""),
        scope,
    )
    if rows:
        payload = [dict(r, identity_id=identities[r["identity"]]) for r in rows]
        await db.execute(
            text("""INSERT INTO protected_size_refs(resource_id,snapshot_id,item_id,identity_id,created_at,size)
          SELECT :r,x.snapshot_id,x.item_id,x.identity_id,x.created_at,x.size FROM jsonb_to_recordset(CAST(:rows AS jsonb))
          AS x(snapshot_id uuid,item_id uuid,identity_id bigint,created_at timestamp,size bigint)
          ON CONFLICT(resource_id,snapshot_id,item_id) DO UPDATE SET identity_id=EXCLUDED.identity_id,created_at=EXCLUDED.created_at,size=EXCLUDED.size"""),
            {"r": rid, "rows": json.dumps(payload, default=str)},
        )
    await db.execute(
        text(
            "DELETE FROM protected_size_winners WHERE resource_id=:r AND identity_id=ANY(CAST(:keys AS bigint[]))"
        ),
        args,
    )
    await db.execute(
        text("""INSERT INTO protected_size_winners(resource_id,identity_id,size)
      SELECT :r,key.identity_id,w.size FROM unnest(CAST(:keys AS bigint[])) key(identity_id)
      CROSS JOIN LATERAL (SELECT size FROM protected_size_refs WHERE resource_id=:r AND identity_id=key.identity_id
      ORDER BY created_at DESC,item_id DESC LIMIT 1) w"""),
        args,
    )
    after = await db.scalar(
        text(
            "SELECT COALESCE(sum(size),0) FROM protected_size_winners WHERE resource_id=:r AND identity_id=ANY(CAST(:keys AS bigint[]))"
        ),
        args,
    )
    await db.execute(
        text(
            "UPDATE resource_protected_bytes SET working_bytes=working_bytes+:delta WHERE resource_id=:r"
        ),
        {"r": rid, "delta": int(after) - int(before)},
    )
    await db.execute(
        text("""DELETE FROM protected_size_identities i WHERE resource_id=:r AND id=ANY(CAST(:keys AS bigint[]))
      AND NOT EXISTS(SELECT 1 FROM protected_size_refs f WHERE f.resource_id=:r AND f.identity_id=i.id)
      AND NOT EXISTS(SELECT 1 FROM protected_size_winners w WHERE w.resource_id=:r AND w.identity_id=i.id)"""),
        args,
    )
    return len(ids)


async def _apply(factory, work, page, page_size, ms):
    async with factory.begin() as db:
        await _timeout(db, ms)
        valid = await db.scalar(
            text("""SELECT singleton FROM protected_size_control WHERE singleton AND owner=:owner AND fence=:fence
          AND lease_until>clock_timestamp() FOR UPDATE"""),
            work,
        )
        if not valid:
            return {"status": "lost_lease", "referencesTouched": 0}
        rid = work.get("resource_id")
        if rid:
            valid = await db.scalar(
                text(
                    "SELECT fence=:resource_fence FROM resource_protected_bytes WHERE resource_id=:resource_id FOR UPDATE"
                ),
                work,
            )
            if not valid:
                return {"status": "lost_lease", "referencesTouched": 0}
        touched = 0
        if work["kind"] == "discovery":
            epoch = await db.scalar(
                text(
                    "SELECT discovery_epoch FROM protected_size_control WHERE singleton"
                )
            )
            if epoch != work["discovery_epoch"]:
                await db.execute(
                    text(
                        "UPDATE protected_size_control SET owner=NULL,lease_until=NULL WHERE singleton"
                    )
                )
                return {"status": "discovery_changed", "referencesTouched": 0}
            for row in page:
                if row["resource_id"] is None:
                    continue
                if work["discovery_phase"] == "registry":
                    if not row["present"]:
                        await db.execute(
                            text("""UPDATE protected_size_snapshots SET eligible=false,present=false,revision=revision+1
                          WHERE snapshot_id=:id AND revision=:revision"""),
                            dict(row),
                        )
                else:
                    # A trigger revision/tombstone wins over a stale source page.
                    await db.execute(
                        text("""INSERT INTO protected_size_snapshots(snapshot_id,resource_id,eligible)
                      SELECT :id,:resource_id,:eligible WHERE EXISTS(SELECT 1 FROM resources WHERE id=:resource_id)
                      ON CONFLICT(snapshot_id) DO UPDATE SET resource_id=EXCLUDED.resource_id,eligible=EXCLUDED.eligible,
                      present=true,revision=protected_size_snapshots.revision+1
                      WHERE protected_size_snapshots.revision=:revision"""),
                        dict(row),
                    )
                await db.execute(
                    text(
                        "INSERT INTO resource_protected_bytes(resource_id) SELECT id FROM resources WHERE id=:r ON CONFLICT DO NOTHING"
                    ),
                    {"r": row["resource_id"]},
                )
                await db.execute(
                    text("""INSERT INTO protected_size_jobs(resource_id,snapshot_id)
                  SELECT :r,:s WHERE EXISTS(SELECT 1 FROM resources WHERE id=:r)
                  ON CONFLICT(resource_id,snapshot_id) DO UPDATE SET cursor=NULL,phase='source' """),
                    {"r": row["resource_id"], "s": row["id"]},
                )
            finished = len(page) < page_size
            next_phase = (
                "registry"
                if finished and work["discovery_phase"] == "source"
                else work["discovery_phase"]
            )
            await db.execute(
                text("""UPDATE protected_size_control SET discovery_cursor=:cursor,
              discovery_phase=:phase,discovery_done=:done WHERE singleton"""),
                {
                    "cursor": None
                    if finished and work["discovery_phase"] == "source"
                    else page[-1]["id"]
                    if page
                    else work["cursor"],
                    "phase": next_phase,
                    "done": finished and work["discovery_phase"] == "registry",
                },
            )
        elif work["kind"] == "event":
            event = work["event"]
            ids = page if event["kind"] == "snapshots" else page["ids"]
            if event["kind"] == "snapshots":
                for sid in ids:
                    await db.execute(
                        text("""INSERT INTO protected_size_jobs(resource_id,snapshot_id) VALUES(:r,:s)
                      ON CONFLICT(resource_id,snapshot_id) DO UPDATE SET cursor=NULL,phase='source' """),
                        {"r": rid, "s": sid},
                    )
            else:
                touched = await _references(
                    db, rid, ids, page["rows"], item_keys=page["keys"]
                )
            total = len(
                event["snapshot_ids"]
                if event["kind"] == "snapshots"
                else event["item_ids"]
            )
            offset = event["processed_offset"] + len(ids)
            await db.execute(
                text("""UPDATE protected_size_targets SET processed_offset=:offset,
              acknowledged_at=CASE WHEN :done THEN clock_timestamp() ELSE NULL END WHERE resource_id=:r AND event_id=:e"""),
                {"offset": offset, "done": offset >= total, "r": rid, "e": event["id"]},
            )
            if offset >= total:
                # Acknowledged history leaves active routing immediately. A later
                # parent change schedules its own snapshot reconciliation.
                await db.execute(
                    text("""DELETE FROM protected_size_event_snapshots WHERE event_id=:e
                  AND NOT EXISTS(SELECT 1 FROM protected_size_targets t WHERE t.event_id=:e AND t.acknowledged_at IS NULL)
                  AND NOT EXISTS(SELECT 1 FROM protected_size_event_snapshots e
                    JOIN protected_size_snapshots registry ON registry.snapshot_id=e.snapshot_id WHERE e.event_id=:e
                    AND NOT EXISTS(SELECT 1 FROM protected_size_targets ack WHERE ack.event_id=:e
                      AND ack.resource_id=registry.resource_id AND ack.acknowledged_at IS NOT NULL))"""),
                    {"e": event["id"]},
                )

        elif work["kind"] == "job":
            job = work["job"]
            if page["remove"]:
                rows = page["rows"]
                touched = await _references(
                    db,
                    rid,
                    [r["item_id"] for r in rows],
                    page["current"],
                    job["snapshot_id"],
                )
            else:
                rows = page["rows"]
                touched = await _references(
                    db,
                    rid,
                    [r["item_id"] for r in rows],
                    [r for r in rows if not r["deleted"]],
                    job["snapshot_id"],
                )
            if len(rows) < page_size and not page["remove"]:
                await db.execute(
                    text(
                        "UPDATE protected_size_jobs SET cursor=NULL,phase='prune' WHERE resource_id=:r AND snapshot_id=:s"
                    ),
                    {"r": rid, "s": job["snapshot_id"]},
                )
            elif len(rows) < page_size:
                await db.execute(
                    text(
                        "DELETE FROM protected_size_jobs WHERE resource_id=:r AND snapshot_id=:s"
                    ),
                    {"r": rid, "s": job["snapshot_id"]},
                )
            else:
                await db.execute(
                    text(
                        "UPDATE protected_size_jobs SET cursor=:cursor WHERE resource_id=:r AND snapshot_id=:s"
                    ),
                    {"r": rid, "s": job["snapshot_id"], "cursor": rows[-1]["item_id"]},
                )
        else:
            await db.execute(
                text("""UPDATE resource_protected_bytes p SET bytes=working_bytes,computed_at=clock_timestamp(),ledger_published=true,
              latest_snapshot_id=(SELECT snapshot_id FROM protected_size_snapshots s WHERE s.resource_id=p.resource_id AND s.eligible ORDER BY snapshot_id DESC LIMIT 1)
              WHERE resource_id=:r AND (SELECT discovery_done FROM protected_size_control WHERE singleton)
              AND NOT EXISTS(SELECT 1 FROM protected_size_jobs WHERE resource_id=:r)
              AND NOT EXISTS(SELECT 1 FROM protected_size_targets WHERE resource_id=:r AND acknowledged_at IS NULL)"""),
                {"r": rid},
            )
        if rid:
            await db.execute(
                text("""INSERT INTO protected_size_tenant_turns(tenant_id,last_turn)
              SELECT tenant_id,clock_timestamp() FROM resources WHERE id=:r ON CONFLICT(tenant_id)
              DO UPDATE SET last_turn=EXCLUDED.last_turn"""),
                {"r": rid},
            )
            # Each successful page yields the resource's turn to the oldest waiter.
            await db.execute(
                text(
                    "UPDATE resource_protected_bytes SET last_turn=clock_timestamp(),next_attempt='epoch',prefer_build=:prefer WHERE resource_id=:r"
                ),
                {"r": rid, "prefer": work["kind"] == "event"},
            )
            # Existing publication remains stale until there are no jobs/events.
            pending = await db.scalar(
                text(f"""SELECT EXISTS(SELECT 1 FROM protected_size_jobs WHERE resource_id=:r)
              OR EXISTS(SELECT 1 FROM protected_size_targets WHERE resource_id=:r AND acknowledged_at IS NULL) OR {_route_pending(':r')}"""),
                {"r": rid},
            )
            if not pending and await db.scalar(
                text(
                    "SELECT discovery_done FROM protected_size_control WHERE singleton"
                )
            ):
                await db.execute(
                    text(
                        "UPDATE resource_protected_bytes SET bytes=working_bytes,computed_at=clock_timestamp(),ledger_published=true WHERE resource_id=:r"
                    ),
                    {"r": rid},
                )
        cleanup = await _cleanup(db, page_size)
        await db.execute(
            text(
                "UPDATE protected_size_control SET owner=NULL,lease_until=NULL WHERE singleton"
            )
        )
        return {
            "status": "idle"
            if work["kind"] == "maintenance" and not cleanup
            else "progress",
            "cleanupRows": cleanup,
            "phase": work["kind"],
            "resourceId": str(rid) if rid else None,
            "referencesTouched": touched,
            "journalRows": 1 if work["kind"] == "event" else 0,
            "sourceMode": "replica" if work.get("replica") else "primary",
        }


async def _cleanup(db, page_size):
    """Bound orphan removal and acknowledged journal retention independently."""
    removed = 0
    rid = await db.scalar(
        text(
            "SELECT resource_id FROM protected_size_orphans ORDER BY resource_id LIMIT 1"
        )
    )
    if rid:
        for table in (
            "protected_size_refs",
            "protected_size_winners",
            "protected_size_jobs",
            "protected_size_snapshots",
            "protected_size_targets",
            "protected_size_identities",
        ):
            result = await db.execute(
                text(f"""DELETE FROM {table} WHERE ctid IN (
              SELECT ctid FROM {table} WHERE resource_id=:r LIMIT :n)"""),
                {"r": rid, "n": page_size},
            )
            removed += result.rowcount
        remains = await db.scalar(
            text("""SELECT
          EXISTS(SELECT 1 FROM protected_size_refs WHERE resource_id=:r) OR
          EXISTS(SELECT 1 FROM protected_size_winners WHERE resource_id=:r) OR
          EXISTS(SELECT 1 FROM protected_size_jobs WHERE resource_id=:r) OR
          EXISTS(SELECT 1 FROM protected_size_snapshots WHERE resource_id=:r) OR
          EXISTS(SELECT 1 FROM protected_size_targets WHERE resource_id=:r) OR
          EXISTS(SELECT 1 FROM protected_size_identities WHERE resource_id=:r)"""),
            {"r": rid},
        )
        if not remains:
            await db.execute(
                text("DELETE FROM protected_size_orphans WHERE resource_id=:r"),
                {"r": rid},
            )
    result = await db.execute(
        text("""DELETE FROM protected_size_snapshots WHERE ctid IN (
      SELECT p.ctid FROM (SELECT ctid,snapshot_id,resource_id FROM protected_size_snapshots WHERE NOT present ORDER BY snapshot_id LIMIT :n) p
      WHERE NOT EXISTS(SELECT 1 FROM protected_size_jobs j WHERE j.resource_id=p.resource_id AND j.snapshot_id=p.snapshot_id))"""),
        {"n": page_size},
    )
    removed += result.rowcount
    cutoff = datetime.now(timezone.utc) - timedelta(days=1)
    control = (
        (
            await db.execute(
                text(
                    "SELECT retirement_at,retirement_id FROM protected_size_control WHERE singleton"
                )
            )
        )
        .mappings()
        .one()
    )
    candidates = list(
        (
            await db.execute(
                text("""SELECT id,created_at FROM protected_size_journal
      WHERE created_at<:cutoff AND (created_at,id)>(:at,:id)
      ORDER BY created_at,id LIMIT :n"""),
                {
                    "cutoff": cutoff,
                    "at": control["retirement_at"],
                    "id": control["retirement_id"],
                    "n": page_size,
                },
            )
        ).mappings()
    )
    if candidates:
        await db.execute(
            text("""INSERT INTO protected_size_retired(event_id) SELECT id FROM protected_size_journal j
          WHERE id=ANY(CAST(:ids AS bigint[]))
            AND NOT EXISTS(SELECT 1 FROM protected_size_targets t WHERE t.event_id=j.id AND t.acknowledged_at IS NULL)
            AND NOT EXISTS(SELECT 1 FROM protected_size_event_snapshots e
              JOIN protected_size_snapshots registry ON registry.snapshot_id=e.snapshot_id WHERE e.event_id=j.id
              AND NOT EXISTS(SELECT 1 FROM protected_size_targets t WHERE t.event_id=j.id
                AND t.resource_id=registry.resource_id AND t.acknowledged_at IS NOT NULL))
          ON CONFLICT DO NOTHING"""),
            {"ids": [r["id"] for r in candidates]},
        )
        cursor = candidates[-1]
        await db.execute(
            text(
                "UPDATE protected_size_control SET retirement_at=:created_at,retirement_id=:id WHERE singleton"
            ),
            dict(cursor),
        )
    else:
        await db.execute(
            text(
                "UPDATE protected_size_control SET retirement_at='epoch',retirement_id=0 WHERE singleton"
            )
        )
    retired_cursor = await db.scalar(
        text("SELECT retired_cursor FROM protected_size_control WHERE singleton")
    )
    retired = list(
        (
            await db.execute(
                text(
                    "SELECT event_id FROM protected_size_retired WHERE event_id>:cursor ORDER BY event_id LIMIT :n"
                ),
                {"cursor": retired_cursor, "n": page_size},
            )
        ).scalars()
    )
    await db.execute(
        text(
            "UPDATE protected_size_control SET retired_cursor=:cursor WHERE singleton"
        ),
        {"cursor": retired[-1] if retired else 0},
    )
    if not retired:
        return removed
    for table in ("protected_size_targets", "protected_size_event_snapshots"):
        result = await db.execute(
            text(f"""DELETE FROM {table} WHERE ctid IN (
          SELECT ctid FROM {table} WHERE event_id=ANY(CAST(:ids AS bigint[])) LIMIT :n)"""),
            {"ids": retired, "n": page_size},
        )
        removed += result.rowcount
    result = await db.execute(
        text("""WITH ready AS (
      SELECT event_id FROM unnest(CAST(:ids AS bigint[])) page(event_id)
      WHERE NOT EXISTS(SELECT 1 FROM protected_size_targets t WHERE t.event_id=page.event_id)
      AND NOT EXISTS(SELECT 1 FROM protected_size_event_snapshots e WHERE e.event_id=page.event_id)
    ), dropped AS (DELETE FROM protected_size_retired r USING ready
       WHERE r.event_id=ready.event_id RETURNING r.event_id)
    DELETE FROM protected_size_journal WHERE id IN (SELECT event_id FROM dropped)"""),
        {"ids": retired},
    )
    return removed + result.rowcount


async def _release(factory, work, ms):
    async with factory.begin() as db:
        await _timeout(db, ms)
        await db.execute(
            text(
                "UPDATE protected_size_control SET owner=NULL,lease_until=NULL WHERE singleton AND owner=:owner AND fence=:fence"
            ),
            work,
        )
        if work.get("resource_id"):
            await db.execute(
                text(
                    "UPDATE resource_protected_bytes SET next_attempt=clock_timestamp()+interval '1 second' WHERE resource_id=:resource_id AND fence=:resource_fence"
                ),
                work,
            )


async def refresh_once(
    factory,
    *,
    source_factory=None,
    page_size=1000,
    statement_timeout_ms=5000,
    owner=None,
) -> dict:
    """One fleet-wide fenced chunk; no pool connection is retained between phases.

    A replica must pass a WAL visibility barrier inside its page transaction.
    Lag/errors defer work; they never cause a historical primary fallback.
    """
    page_size = max(1, min(1000, int(page_size)))
    ms = max(1, min(5000, int(statement_timeout_ms)))
    owner = UUID(str(owner)) if owner is not None else uuid4()
    try:
        work = await _claim(factory, owner, ms)
    except DBAPIError as exc:
        code = getattr(exc.orig, "sqlstate", None) or getattr(exc.orig, "pgcode", None)
        if code in ("40001", "40P01", "57014", "55P03"):
            return {"status": "retry", "sqlstate": code, "referencesTouched": 0}
        raise
    if "status" in work:
        return work
    work["replica"] = source_factory is not None
    try:
        page = await _source_page(
            source_factory or factory, work, page_size, ms, source_factory is not None
        )
        if page is None:
            await _release(factory, work, ms)
            return {"status": "replica_deferred", "referencesTouched": 0}
        return await _apply(factory, work, page, page_size, ms)
    except DBAPIError as exc:
        await _release(factory, work, ms)
        code = getattr(exc.orig, "sqlstate", None) or getattr(exc.orig, "pgcode", None)
        if source_factory is not None:
            return {
                "status": "replica_deferred",
                "sqlstate": code,
                "referencesTouched": 0,
            }
        if code in ("40001", "40P01", "57014", "55P03"):
            return {"status": "retry", "sqlstate": code, "referencesTouched": 0}
        raise
    except BaseException:
        await _release(factory, work, ms)
        raise
