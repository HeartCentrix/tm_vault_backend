"""
Afi-style Azure SQL Provisioning

During discovery, automatically assigns our SP as Entra admin of each SQL server.
This is what makes SQL backup "just work" with no customer script or credentials.

Uses Directory.ReadWrite.All (app perm) to create the Entra admin mapping.
"""
import httpx
import json
from typing import Dict, List, Optional

from shared.config import settings
from shared.database import async_session_factory
from shared.models import Tenant


async def ensure_sql_server_backup_ready(tenant: Tenant, server: Dict, arm_token: str) -> bool:
    """
    For a newly-discovered SQL server, assign our SP as Entra admin.
    This is what makes SQL backup "just work" with no customer script.

    Uses Directory.ReadWrite.All (app perm) to create the Entra admin mapping.
    """
    server_fqdn = server.get("properties", {}).get("fullyQualifiedDomainName", "")
    sql_configured = getattr(tenant, 'azure_sql_servers_configured', None) or {}
    if isinstance(sql_configured, dict):
        server_list = sql_configured.get("servers", [])
    else:
        server_list = sql_configured if sql_configured else []

    if server_fqdn in server_list:
        return True  # already done

    our_sp_object_id = await _get_our_sp_object_id_in_tenant(tenant)

    # PUT /subscriptions/.../servers/{name}/administrators/ActiveDirectory
    async with httpx.AsyncClient() as client:
        url = (
            f"https://management.azure.com{server['id']}"
            f"/administrators/ActiveDirectory?api-version=2022-05-01-preview"
        )
        payload = {
            "properties": {
                "administratorType": "ActiveDirectory",
                "login": settings.MICROSOFT_CLIENT_ID or settings.AZURE_AD_CLIENT_ID or "tm-vault-app",
                "sid": our_sp_object_id,
                "tenantId": tenant.external_tenant_id,
            }
        }
        resp = await client.put(url, json=payload,
                                 headers={"Authorization": f"Bearer {arm_token}"})
        if resp.status_code not in (200, 201, 202):
            # Log but don't fail — SQL backup can still use ARM API fallback
            print(f"[AzureProvisioning] Failed to set SQL Entra admin for {server_fqdn}: {resp.status_code} {resp.text[:200]}")
            return False

    # Track so we don't retry
    async with async_session_factory() as session:
        t = await session.get(Tenant, tenant.id)
        if t:
            current = getattr(t, 'azure_sql_servers_configured', None) or {}
            if isinstance(current, dict):
                server_list = current.get("servers", [])
            else:
                server_list = current if current else []
            server_list.append(server_fqdn)
            t.azure_sql_servers_configured = {"servers": server_list}
            await session.commit()

    print(f"[AzureProvisioning] ✓ SQL server {server_fqdn} configured for backup (SP as Entra admin)")
    return True


async def ensure_pg_server_backup_ready(tenant: Tenant, server: Dict, arm_token: str) -> bool:
    """
    For a newly-discovered PostgreSQL Flexible Server, assign our SP as Azure AD admin.
    This makes PostgreSQL backup "just work" with no customer credentials.

    API: PUT /subscriptions/{subId}/resourceGroups/{rg}/providers/Microsoft.DBforPostgreSQL/
         flexibleServers/{serverName}/administrators/{objectId}?api-version=2025-08-01
    
    The {objectId} in the URL is the SP's object ID (GUID), not a literal string.
    """
    server_name = server.get("name", "")
    pg_configured = getattr(tenant, 'azure_pg_servers_configured', None) or {}
    if isinstance(pg_configured, dict):
        server_list = pg_configured.get("servers", [])
    else:
        server_list = pg_configured if pg_configured else []

    if server_name in server_list:
        return True  # already done

    our_sp_object_id = await _get_our_sp_object_id_in_tenant(tenant)

    # PUT /.../administrators/{objectId} — objectId is the SP's GUID
    async with httpx.AsyncClient() as client:
        url = (
            f"https://management.azure.com{server['id']}"
            f"/administrators/{our_sp_object_id}?api-version=2025-08-01"
        )
        payload = {
            "properties": {
                "principalName": settings.MICROSOFT_CLIENT_ID or settings.AZURE_AD_CLIENT_ID,
                "principalType": "ServicePrincipal",
                "tenantId": tenant.external_tenant_id,
            }
        }
        resp = await client.put(url, json=payload,
                                 headers={"Authorization": f"Bearer {arm_token}"})
        if resp.status_code not in (200, 201, 202):
            print(f"[AzureProvisioning] Failed to set PostgreSQL Entra admin for {server_name}: {resp.status_code} {resp.text[:300]}")
            return False

    # Track so we don't retry
    async with async_session_factory() as session:
        t = await session.get(Tenant, tenant.id)
        if t:
            current = getattr(t, 'azure_pg_servers_configured', None) or {}
            if isinstance(current, dict):
                server_list = current.get("servers", [])
            else:
                server_list = current if current else []
            server_list.append(server_name)
            t.azure_pg_servers_configured = {"servers": server_list}
            await session.commit()

    print(f"[AzureProvisioning] ✓ PostgreSQL server {server_name} configured for backup (SP as Entra admin)")
    return True


async def _get_our_sp_object_id_in_tenant(tenant: Tenant) -> str:
    """Find OUR app's SP object ID inside the customer's tenant.
    Each tenant has its own object ID for the same multi-tenant app."""
    graph_token = await _get_graph_app_token(tenant)

    app_id = settings.MICROSOFT_CLIENT_ID or settings.AZURE_AD_CLIENT_ID
    async with httpx.AsyncClient() as raw:
        from types import SimpleNamespace
        from shared.provider_http import download_client
        client = download_client(raw, SimpleNamespace(tenant_id=str(tenant.external_tenant_id), client_id=app_id))
        resp = await client.get(
            f"https://graph.microsoft.com/v1.0/servicePrincipals"
            f"?$filter=appId eq '{app_id}'",
            headers={"Authorization": f"Bearer {graph_token}"},
        )
        resp.raise_for_status()
        sps = resp.json()["value"]
        if not sps:
            raise RuntimeError("Our SP not found in tenant — consent may have failed")
        return sps[0]["id"]


async def _get_graph_app_token(tenant: Tenant) -> str:
    """App-only Graph token for Directory.ReadWrite.All operations."""
    client_id = settings.MICROSOFT_CLIENT_ID or settings.AZURE_AD_CLIENT_ID
    client_secret = settings.MICROSOFT_CLIENT_SECRET or settings.AZURE_AD_CLIENT_SECRET

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"https://login.microsoftonline.com/{tenant.external_tenant_id}/oauth2/v2.0/token",
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": "https://graph.microsoft.com/.default",
                "grant_type": "client_credentials",
            },
        )
        resp.raise_for_status()
        return resp.json()["access_token"]
