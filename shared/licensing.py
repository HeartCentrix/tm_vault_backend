"""License-entitlement helpers derived from Microsoft Graph ``user.assignedPlans``.

The point of this module is the "quick hack": Tier-1 user discovery already
makes ONE bulk ``GET /users`` call at tenant onboarding. Adding ``assignedPlans``
to that call's ``$select`` costs ZERO extra requests — it's a directory property
returned inline — and tells us, per user, which service families are licensed.
That lets us mark a workload ``license_missing`` up front (and skip its Graph
probe) instead of waiting for a per-workload 403/404 near backup time.

Everything here is pure so it's unit-testable without a network.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

# Graph ``assignedPlans[].service`` strings, grouped into the three families we
# back up. Values are matched case-insensitively. Teams/Skype have shipped
# under several service names over the years, so we accept all known aliases.
_EXCHANGE_SERVICES = {"exchange"}
_SHAREPOINT_SERVICES = {"sharepoint"}  # OneDrive for Business rides the SharePoint plan
_TEAMS_SERVICES = {
    "teamspaceapi", "microsoftcommunicationsonline", "teams", "mcolite", "mcostandard",
}

# capabilityStatus values that mean the service is usable. "Warning" is a grace
# period (license expiring) — the mailbox/drive still works, so we treat it as
# licensed. "Suspended"/"Deleted"/"LockedOut" mean it does not.
_ACTIVE_STATUSES = {"enabled", "warning"}

# Which license family each Tier-2 discovery category depends on — but ONLY the
# categories that genuinely have NO data without the license. These are the only
# workloads it is safe to entitlement-skip. Confirmed by a live READ-ONLY Graph
# probe of a 0-license user (Amit Kumar, 2026-07-09):
#   mail/calendar/contacts → HTTP 404 MailboxNotEnabledForRESTAPI (all three live
#                            in the Exchange mailbox — no mailbox, no data)
#   onedrive               → HTTP 423 notAllowed/resourceLocked (no SharePoint)
#   chats                  → HTTP 200 with data  ← backs up WITHOUT a license
#
# "chats" is deliberately EXCLUDED: Teams chat access is chat-MEMBERSHIP based,
# not license based. A user with zero assigned licenses can still be a member of
# thousands of 1:1/group chats (Amit Kumar: 27k messages, 0 licenses). Gating
# chats on a Teams license silently drops real, recoverable data. It must always
# be probed and captured.
CATEGORY_SERVICE: Dict[str, str] = {
    "mail": "exchange",
    "calendar": "exchange",
    "contacts": "exchange",
    "onedrive": "sharepoint",
}


def derive_licensed_services(assigned_plans: Optional[List[Dict[str, Any]]]) -> Dict[str, bool]:
    """Collapse Graph ``assignedPlans`` into ``{exchange, sharepoint, teams}`` booleans.

    A family is True when the user has at least one active (Enabled/Warning)
    plan for it. Always returns all three keys so callers can safely do
    ``services.get("exchange") is False``.
    """
    svc = {"exchange": False, "sharepoint": False, "teams": False}
    for p in assigned_plans or []:
        if not isinstance(p, dict):
            continue
        if (p.get("capabilityStatus") or "").lower() not in _ACTIVE_STATUSES:
            continue
        s = (p.get("service") or "").lower()
        if s in _EXCHANGE_SERVICES:
            svc["exchange"] = True
        elif s in _SHAREPOINT_SERVICES:
            svc["sharepoint"] = True
        elif s in _TEAMS_SERVICES:
            svc["teams"] = True
    return svc


def category_unlicensed(licensed_services: Optional[Dict[str, bool]], category: str) -> bool:
    """True only when we have POSITIVE evidence the user lacks the license for
    this Tier-2 category. Returns False when entitlement data is missing/unknown
    (``None`` / empty dict) so the caller falls back to the live probe — we
    never skip a probe on a guess.
    """
    svc = CATEGORY_SERVICE.get(category)
    if svc is None or not isinstance(licensed_services, dict) or not licensed_services:
        return False
    return licensed_services.get(svc) is False
