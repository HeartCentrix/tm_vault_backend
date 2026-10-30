"""Routing fence — defend against wrong-queue delivery.

Every work message carries an envelope:

    {"expected_workload": "USER_CHATS",
     "route_hops":        0,
     "payload":           {...}}

The worker checks ``expected_workload`` against the workloads its
current consumer handles. On mismatch:

  * re-publish to the correct queue (up to 2 hops)
  * if hops exceeds the cap, log + insert into ``work_dead_letter``
    and ACK the message so it doesn't ping-pong forever.

This is layer 4 of the reconciliation design.
"""
from __future__ import annotations

import os
from typing import Any, Dict, Iterable, Optional

from sqlalchemy import text


ROUTING_FENCE_ENABLED = os.getenv("ROUTING_FENCE_ENABLED", "true").lower() in ("true", "1", "yes")
ROUTING_MAX_HOPS = int(os.getenv("ROUTING_MAX_HOPS", "2"))


# Mirror of the queue-by-workload map. Add to this if a new workload
# is introduced; the publisher in shared/message_bus.py already
# routes by workload + tier today.
_QUEUE_BY_WORKLOAD = {
    # Tier-1 (ENTRA fan-out)
    "ENTRA_USER":   "backup.urgent",
    # Tier-2 single-shot
    "USER_MAIL":         "backup.urgent",
    "USER_CALENDAR":     "backup.urgent",
    "USER_CONTACTS":     "backup.urgent",
    # Tier-2 heavy / partitioned
    "USER_CHATS":        "backup.heavy",
    "USER_ONEDRIVE":     "backup.heavy",
    "USER_ARCHIVE":      "backup.heavy",
    # Partitions (shard-level)
    "ONEDRIVE_PARTITION":     "backup.onedrive_partition",
    "CHATS_PARTITION":        "backup.chats_partition",
    "MAIL_PARTITION":         "backup.mail_partition",
    "SHAREPOINT_PARTITION":   "backup.sharepoint_partition",
}


def wrap_outgoing(payload: Dict[str, Any], *, expected_workload: str) -> Dict[str, Any]:
    """Wrap a worker-bound message with the routing fence envelope.

    Idempotent — if the message is already wrapped (i.e. carries
    ``expected_workload``), just bumps hops if necessary.
    """
    if not ROUTING_FENCE_ENABLED:
        return payload
    if "expected_workload" in payload:
        # Already wrapped; leave alone.
        return payload
    return {
        "expected_workload": expected_workload,
        "route_hops": 0,
        "payload": payload,
    }


def unwrap_incoming(
    raw: Dict[str, Any],
    *,
    handled_workloads: Iterable[str],
) -> Optional[Dict[str, Any]]:
    """Validate routing on an incoming message.

    Returns the inner payload if this worker handles it. Returns
    ``None`` if the message must be re-routed by the caller (caller
    should publish to the workload's correct queue + ACK) or
    dead-lettered.

    Callers handle the re-routing themselves (they own the AMQP
    handle); this helper just decides.
    """
    if not ROUTING_FENCE_ENABLED:
        # Backwards-compat path — assume any unwrapped message belongs.
        return raw
    if "expected_workload" not in raw or "payload" not in raw:
        # Legacy unwrapped message — accept (no enforcement).
        return raw
    exp = raw.get("expected_workload")
    if exp in set(handled_workloads):
        return raw["payload"]
    return None  # caller re-routes / DLQs


def correct_queue_for(expected_workload: str) -> Optional[str]:
    return _QUEUE_BY_WORKLOAD.get(expected_workload)


async def reroute_or_dlq(
    raw: Dict[str, Any],
    *,
    message_bus,
    session_factory,
    work_kind: str = "unknown",
    work_id: Optional[str] = None,
) -> str:
    """Either re-publish ``raw`` to the correct queue (bumping hops)
    or insert a DLQ row if the hop cap is hit.

    Returns 'rerouted' or 'dlq' or 'unroutable'.
    """
    if not ROUTING_FENCE_ENABLED:
        return "unroutable"
    if "expected_workload" not in raw:
        return "unroutable"
    hops = int(raw.get("route_hops") or 0)
    exp = raw["expected_workload"]
    queue = correct_queue_for(exp)
    if queue is None:
        # No routing target — DLQ.
        await _dlq(
            session_factory,
            work_kind=work_kind,
            work_id=work_id,
            reason="unroutable",
            payload=raw,
        )
        return "dlq"
    if hops >= ROUTING_MAX_HOPS:
        await _dlq(
            session_factory,
            work_kind=work_kind,
            work_id=work_id,
            reason="route_loop",
            payload=raw,
        )
        return "dlq"
    rewrapped = {
        "expected_workload": exp,
        "route_hops": hops + 1,
        "payload": raw.get("payload", {}),
    }
    await message_bus.publish(queue, rewrapped)
    return "rerouted"


async def _dlq(
    session_factory,
    *,
    work_kind: str,
    work_id: Optional[str],
    reason: str,
    payload: Dict[str, Any],
) -> None:
    import json
    try:
        async with session_factory() as session:
            await session.execute(
                text(
                    """
                    INSERT INTO work_dead_letter
                        (work_kind, work_id, reason, last_payload)
                    VALUES
                        (:kind, cast(:wid AS uuid), :reason, cast(:body AS json))
                    """
                ),
                {
                    "kind": work_kind,
                    "wid": work_id or "00000000-0000-0000-0000-000000000000",
                    "reason": reason,
                    "body": json.dumps(payload, default=str),
                },
            )
            await session.commit()
    except Exception as exc:
        print(f"[ROUTING_FENCE] DLQ insert failed: {exc}")
