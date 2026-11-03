"""Tenant entitlement — which tenants may a given platform user act in?

Single source of truth shared by auth-service (stamping the JWT `tenantIds`
claim) and job-service's chat-export authorization gate, so the claim and the
enforcement can never drift apart.

Why not `PlatformUser.tenant_id` on its own: that column exists on the model
but nothing in the product ever writes it — `oauth_callback` inserts
email/name/external_user_id/org_id only, and there is no admin endpoint or
migration that assigns it. Deriving entitlement from it alone yielded an
empty set for every real user, which silently turned every tenant-scoping
check in the product into dead code. Entitlement is therefore derived from
the org membership that IS populated (`PlatformUser.org_id` ->
`Tenant.org_id`), with `PlatformUser.tenant_id` honoured as an explicit
narrowing pin when an operator does set it.

Archived (soft-deleted) tenants are never entitled. A user with neither a
pin nor an org gets an empty list — callers must treat that as "deny".

The returned order is deterministic (M365 tenants first) — see `_CLAIM_ORDER`.
"""
import uuid
from typing import Any, List, Optional

from sqlalchemy import case, select

from shared.models import PlatformUser, Tenant, TenantType

# Claim order matters because two consumers take `[0]` from it:
# `alert-service.create_access_group` files new groups in the first tenant,
# and a job-service still running the pre-c8fa014 build compares the
# resource's tenant against `tenant_ids[0]`. Unordered, Postgres row order
# decided both. M365 first because every mailbox / OneDrive / chat resource
# lives in the M365 tenant row, so that is the useful default when an org has
# both an M365 and an AZURE tenant; created_at/id are stable tiebreakers.
_M365_FIRST = case((Tenant.type == TenantType.M365, 0), else_=1)
_CLAIM_ORDER = (_M365_FIRST, Tenant.created_at, Tenant.id)


async def entitled_tenant_ids_for_user(db: Any, user: PlatformUser) -> List[uuid.UUID]:
    """Entitled tenant ids for an already-loaded PlatformUser."""
    q = select(Tenant.id).where(Tenant.archived_at.is_(None)).order_by(*_CLAIM_ORDER)
    if user.tenant_id:
        q = q.where(Tenant.id == user.tenant_id)
    elif user.org_id:
        q = q.where(Tenant.org_id == user.org_id)
    else:
        return []
    return list((await db.execute(q)).scalars().all())


async def resolve_entitled_tenant_ids(db: Any, user_id: Optional[uuid.UUID]) -> List[uuid.UUID]:
    """Entitled tenant ids for a user id. Unknown user -> empty (deny)."""
    if user_id is None:
        return []
    user = (
        await db.execute(select(PlatformUser).where(PlatformUser.id == user_id))
    ).scalar_one_or_none()
    if user is None:
        return []
    return await entitled_tenant_ids_for_user(db, user)
