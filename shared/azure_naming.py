"""Naming for Azure data sources.

A data source must be named after the ORGANIZATION / subscription it backs up,
never after the admin who happened to authorize the OAuth connection. This
helper centralizes that rule so the onboarding callback cannot regress to the
user's JWT `name` claim (the "Amit Mishra" instead of "QFION Software" bug).
"""
from __future__ import annotations

from typing import Any, List, Optional


def derive_azure_datasource_name(
    arm_subscriptions: Optional[List[dict[str, Any]]],
    external_tenant_id: Optional[str],
    existing_org_name: Optional[str] = None,
) -> str:
    """Pick a display name for an Azure data source.

    Priority (never the signed-in user's name):
      1. ``existing_org_name`` — e.g. the org name from the M365 tenant row for
         the same Azure AD tenant ("QFION Software"). Most accurate.
      2. The first Azure subscription's ``displayName`` → ``"Azure - <name>"``.
      3. A tenant-id-keyed generic ``"Azure Tenant <8-char prefix>"``.
      4. ``"Azure Tenant"`` when nothing is known.

    There is deliberately no parameter for the user's name — the rule is
    enforced structurally.
    """
    if existing_org_name and existing_org_name.strip():
        return existing_org_name.strip()

    if arm_subscriptions:
        first = arm_subscriptions[0] or {}
        sub_name = (first.get("displayName") or "").strip() or "Subscription"
        return f"Azure - {sub_name}"

    if external_tenant_id:
        return f"Azure Tenant {str(external_tenant_id)[:8]}"

    return "Azure Tenant"
