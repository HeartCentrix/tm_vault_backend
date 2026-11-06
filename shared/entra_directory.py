"""The tenant's singleton "Azure Active Directory" resource (ENTRA_DIRECTORY): one row per M365 tenant,
holding all Entra-wide content (users / groups / roles / applications / audit / security / intune /
admin units) as snapshot items."""
from uuid import uuid4

from sqlalchemy import select

from shared.models import Resource, ResourceStatus, ResourceType, TenantType


async def ensure_entra_directory(db, tenant) -> bool:
    """Create the tenant's directory resource when missing (M365 only). True when created."""
    if tenant.type != TenantType.M365:
        return False
    existing = await db.scalar(select(Resource.id).where(
        Resource.tenant_id == tenant.id, Resource.type == ResourceType.ENTRA_DIRECTORY).limit(1))
    if existing is not None:
        return False
    db.add(Resource(id=uuid4(), tenant_id=tenant.id, type=ResourceType.ENTRA_DIRECTORY,
                    external_id=tenant.external_tenant_id or str(tenant.id), display_name="Azure Active Directory",
                    email=None, extra_data={"source": "tenant_singleton"}, status=ResourceStatus.DISCOVERED))
    await db.flush()
    return True
