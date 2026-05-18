"""Auth Service - Handles authentication and user management"""
from contextlib import asynccontextmanager
from typing import Optional
from uuid import UUID, uuid4
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
import secrets

import httpx
from fastapi import FastAPI, Depends, HTTPException, Query, Request, Response
from sqlalchemy import select, String

from shared.config import settings
from shared.database import get_db, init_db, close_db, AsyncSession, async_session_factory
from shared.models import PlatformUser, UserRoleMapping, Organization, UserRole, Tenant, TenantType, TenantStatus, AdminConsentToken, Resource, ResourceType
from shared.power_bi_client import PowerBIClient
from shared.security import (
    create_access_token,
    create_refresh_token,
    decode_token,
    get_current_user_from_token,
    is_refresh_token_revoked,
    revoke_refresh_token,
)
from shared.schemas import (
    UserResponse, LoginResponse, RefreshTokenRequest, RefreshTokenResponse,
    MicrosoftAuthUrlResponse, OAuthCallbackRequest,
    DatasourceConsentRequest, DatasourceCallbackResponse,
    AdminConsentResponse, AdminConsentTokenResponse,
    PowerBIOAuthCallbackRequest,
    PowerBIReadinessResponse,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    from shared import core_metrics
    core_metrics.init()
    await init_db()
    yield
    await close_db()


app = FastAPI(title="Auth Service", version="1.0.0", lifespan=lifespan)


def _power_bi_error_detail(exc: Exception) -> str:
    message = str(exc)
    if "AADSTS" in message or "access token" in message.lower():
        return "The configured Power BI app could not get an access token. Check the client ID, client secret, and tenant ID."
    if "403" in message:
        return (
            "Power BI or Fabric denied access. Enable service principal access in the Fabric admin portal "
            "and add the app or its security group to the target workspaces."
        )
    if "401" in message:
        return "Power BI authentication failed. Recheck the app credentials and tenant selection."
    if "404" in message:
        return "Power BI API access was attempted, but the tenant or workspace endpoint was not found."
    return message or "Power BI readiness check failed."


def _power_bi_authorize_scopes() -> str:
    return " ".join(dict.fromkeys((
        f"openid profile email {PowerBIClient.POWER_BI_DELEGATED_SCOPE} {PowerBIClient.FABRIC_DELEGATED_SCOPE}"
    ).split()))


def _power_bi_code_redeem_scopes() -> str:
    # The token endpoint can only redeem scopes from a single resource at a time.
    # We redeem the code for a Power BI token + refresh token, then use that refresh
    # token to mint Fabric tokens later when the worker needs them.
    return " ".join(dict.fromkeys(PowerBIClient.POWER_BI_DELEGATED_SCOPE.split()))


async def _load_token_claims_from_db(db: AsyncSession, user_id: UUID) -> Optional[dict]:
    # Single source of truth for the claims we stamp into access/refresh tokens.
    # /refresh and /me both go through this so role grants/revokes and tenant
    # reassignments take effect on the next refresh instead of being frozen
    # into the user's original login token for its full lifetime.
    user = (
        await db.execute(select(PlatformUser).where(PlatformUser.id == user_id))
    ).scalar_one_or_none()
    if user is None:
        return None
    roles = (
        await db.execute(
            select(UserRoleMapping).where(UserRoleMapping.user_id == user.id)
        )
    ).scalars().all()
    return {
        "sub": str(user.id),
        "email": user.email,
        "roles": [r.role.value for r in roles],
        "orgId": str(user.org_id) if user.org_id else None,
        "tenantIds": [str(user.tenant_id)] if user.tenant_id else [],
    }


@app.get("/health")
async def health():
    return {"status": "ok", "service": "auth"}


def _set_auth_cookies(response: Response, access_token: str, refresh_token: str) -> None:
    """Issue the HttpOnly access/refresh cookies that the SPA uses for auth.

    HttpOnly = JS can't read them, so XSS can't exfiltrate. SameSite + Secure
    block the obvious CSRF / network-leak paths. The browser auto-attaches
    them to fetch() calls when `credentials: 'include'` is set.

    A third, non-HttpOnly companion cookie ``access_token_expires_at`` carries
    the access-token's expiry as an epoch-ms integer. JS reads it to schedule
    a proactive /refresh ~60s before expiry — saves the user from the 401 +
    retry round-trip on the first request after a long idle. The breadcrumb
    is just a timestamp; it leaks nothing useful to XSS that wasn't already
    visible in the network tab.
    """
    access_max_age = settings.JWT_EXPIRATION_HOURS * 3600
    refresh_max_age = settings.JWT_REFRESH_EXPIRATION_DAYS * 86400

    common = {
        "httponly": True,
        "secure": settings.COOKIE_SECURE,
        "samesite": settings.COOKIE_SAMESITE,
        "domain": settings.COOKIE_DOMAIN,
        "path": "/",
    }
    response.set_cookie("access_token", access_token, max_age=access_max_age, **common)
    response.set_cookie("refresh_token", refresh_token, max_age=refresh_max_age, **common)

    # Non-HttpOnly breadcrumb so the SPA can schedule proactive refresh.
    expires_at_ms = int(
        (datetime.now(timezone.utc) + timedelta(seconds=access_max_age)).timestamp() * 1000
    )
    response.set_cookie(
        "access_token_expires_at",
        str(expires_at_ms),
        max_age=access_max_age,
        httponly=False,                       # JS-readable on purpose
        secure=settings.COOKIE_SECURE,
        samesite=settings.COOKIE_SAMESITE,
        domain=settings.COOKIE_DOMAIN,
        path="/",
    )


def _clear_auth_cookies(response: Response) -> None:
    common = {
        "domain": settings.COOKIE_DOMAIN,
        "path": "/",
        "secure": settings.COOKIE_SECURE,
        "samesite": settings.COOKIE_SAMESITE,
        "httponly": True,
    }
    response.delete_cookie("access_token", **common)
    response.delete_cookie("refresh_token", **common)
    # Breadcrumb is non-HttpOnly; clear with a matching attribute set.
    response.delete_cookie(
        "access_token_expires_at",
        domain=settings.COOKIE_DOMAIN,
        path="/",
        secure=settings.COOKIE_SECURE,
        samesite=settings.COOKIE_SAMESITE,
        httponly=False,
    )


@app.get("/api/v1/auth/microsoft/url", response_model=MicrosoftAuthUrlResponse)
async def get_microsoft_login_url(state: Optional[str] = Query(None)):
    csrf_state = state or secrets.token_urlsafe(32)
    params = {
        "client_id": settings.MICROSOFT_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": f"{settings.FRONTEND_URL}/auth/callback",
        # Fragment delivery keeps the auth code (and any error params) out of
        # server access logs and Referer headers — fragments are never sent
        # over the wire. The SPA reads window.location.hash on landing.
        "response_mode": "fragment",
        "scope": "openid profile email offline_access User.Read",
        "state": csrf_state,
    }
    auth_url = f"{settings.MICROSOFT_AUTH_URL}?{urlencode(params)}"
    return MicrosoftAuthUrlResponse(url=auth_url, state=csrf_state)


@app.get("/api/v1/auth/microsoft/datasource/url", response_model=MicrosoftAuthUrlResponse)
async def get_datasource_url(state: Optional[str] = Query(None)):
    """
    Initiate admin-consent flow for M365 datasource onboarding.
    Redirects to /adminconsent endpoint — no user login required.
    After admin grants consent, redirects to /datasource-callback with ?tenant=...&admin_consent=True.
    """
    csrf_state = state or secrets.token_urlsafe(32)
    params = {
        "client_id": settings.MICROSOFT_CLIENT_ID,
        "redirect_uri": f"{settings.FRONTEND_URL}/datasource-callback",
        "state": csrf_state,
    }
    # Use admin-consent endpoint — grants app-level (not user-delegated) permissions
    auth_url = f"https://login.microsoftonline.com/organizations/adminconsent?{urlencode(params)}"
    return MicrosoftAuthUrlResponse(url=auth_url, state=csrf_state)


@app.get("/api/v1/auth/azure/datasource/url", response_model=MicrosoftAuthUrlResponse)
async def get_azure_datasource_url(state: Optional[str] = Query(None)):
    csrf_state = state or secrets.token_urlsafe(32)
    params = {
        "client_id": settings.MICROSOFT_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": f"{settings.FRONTEND_URL}/azure-datasource-callback",
        # Fragment delivery — see /microsoft/url for rationale.
        "response_mode": "fragment",
        "scope": "https://management.azure.com/.default openid",
        "state": csrf_state,
    }
    # CRITICAL: Use /organizations for multi-tenant Azure datasource onboarding.
    # The MICROSOFT_AUTH_URL uses the specific tenant ID which only works for
    # users within that tenant. For external Azure tenants connecting as datasources,
    # we need /organizations to accept any Azure AD tenant.
    auth_url = f"https://login.microsoftonline.com/organizations/oauth2/v2.0/authorize?{urlencode(params)}"
    return MicrosoftAuthUrlResponse(url=auth_url, state=csrf_state)


_POWER_BI_STATE_COOKIE = "power_bi_oauth_state"
# 10 min — long enough for the user to complete the MS sign-in but short
# enough that abandoned flows don't leave dead nonces lying around.
_POWER_BI_STATE_TTL_SECONDS = 600


@app.get("/api/v1/auth/power-bi/url", response_model=MicrosoftAuthUrlResponse)
async def get_power_bi_url(
    response: Response,
    tenant_id: str = Query(..., alias="tenantId"),
    state: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db),
):
    """Initiate delegated Power BI/Fabric onboarding similar to AFI's service-user flow."""
    org_id = UUID(current_user["org_id"]) if current_user.get("org_id") else None
    stmt = select(Tenant).where(Tenant.id == UUID(tenant_id))
    if org_id:
        stmt = stmt.where(Tenant.org_id == org_id)
    tenant = (await db.execute(stmt)).scalar_one_or_none()
    if tenant is None:
        raise HTTPException(404, "Tenant not found")
    if not settings.EFFECTIVE_POWER_BI_CLIENT_ID or not settings.EFFECTIVE_POWER_BI_CLIENT_SECRET:
        raise HTTPException(400, "Power BI onboarding is not configured on this deployment.")

    csrf_state = state or secrets.token_urlsafe(32)
    params = {
        "client_id": settings.EFFECTIVE_POWER_BI_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": f"{settings.FRONTEND_URL}/power-bi-callback",
        # Fragment delivery — see /microsoft/url for rationale.
        "response_mode": "fragment",
        "scope": _power_bi_authorize_scopes(),
        "state": csrf_state,
        "prompt": "consent",
    }
    auth_url = f"https://login.microsoftonline.com/organizations/oauth2/v2.0/authorize?{urlencode(params)}"

    # Stash the CSRF nonce in an HttpOnly cookie so JS (and therefore XSS)
    # can't read or forge it. The callback handler compares this cookie to
    # the `state` echoed back through the OAuth flow; equality is the only
    # acceptance criterion. Server-side validation makes the check
    # authoritative — even if the SPA loses or skips its own state check,
    # the backend rejects mismatches.
    response.set_cookie(
        _POWER_BI_STATE_COOKIE,
        csrf_state,
        max_age=_POWER_BI_STATE_TTL_SECONDS,
        httponly=True,
        secure=settings.COOKIE_SECURE,
        samesite=settings.COOKIE_SAMESITE,
        domain=settings.COOKIE_DOMAIN,
        path="/",
    )

    return MicrosoftAuthUrlResponse(url=auth_url, state=csrf_state)


@app.post("/api/v1/auth/callback", response_model=LoginResponse)
async def oauth_callback(callback: OAuthCallbackRequest, response: Response, db: AsyncSession = Depends(get_db)):
    async with httpx.AsyncClient() as client:
        token_response = await client.post(
            settings.MICROSOFT_TOKEN_URL,
            data={
                "client_id": settings.MICROSOFT_CLIENT_ID,
                "client_secret": settings.MICROSOFT_CLIENT_SECRET,
                "code": callback.code,
                "redirect_uri": f"{settings.FRONTEND_URL}/auth/callback",
                "grant_type": "authorization_code",
            },
        )
        token_response.raise_for_status()
        tokens = token_response.json()
        
        graph_response = await client.get(
            "https://graph.microsoft.com/v1.0/me",
            headers={"Authorization": f"Bearer {tokens['access_token']}"},
        )
        graph_response.raise_for_status()
        profile = graph_response.json()
    
    email = profile.get("mail") or profile.get("userPrincipalName")
    name = profile.get("displayName", email.split("@")[0])
    external_id = profile.get("id")
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    
    stmt = select(PlatformUser).where(PlatformUser.email == email)
    result = await db.execute(stmt)
    user = result.scalar_one_or_none()
    
    if user is None:
        org_stmt = select(Organization).limit(1)
        org_result = await db.execute(org_stmt)
        org = org_result.scalar_one_or_none()
        
        if org is None:
            org = Organization(
                id=UUID("00000000-0000-0000-0000-000000000001"),
                name="Taylor Morrison",
                slug="taylor-morrison",
            )
            db.add(org)
            await db.flush()
        
        user = PlatformUser(
            id=uuid4(),
            email=email,
            name=name,
            external_user_id=external_id,
            org_id=org.id,
        )
        db.add(user)
        db.add(UserRoleMapping(user_id=user.id, role=UserRole.USER))
        await db.flush()
    
    user.last_login_at = now
    user.updated_at = now
    await db.flush()

    token_data = await _load_token_claims_from_db(db, user.id)
    if token_data is None:
        raise HTTPException(status_code=500, detail="Failed to load user claims after login")
    user_roles = token_data["roles"]

    access_token = create_access_token(token_data)
    refresh_token = create_refresh_token(token_data)
    expires_in = settings.JWT_EXPIRATION_HOURS * 3600

    _set_auth_cookies(response, access_token, refresh_token)

    return LoginResponse(
        accessToken=access_token,
        refreshToken=refresh_token,
        expiresIn=expires_in,
        user=UserResponse(
            id=str(user.id),
            email=user.email,
            name=user.name,
            roles=user_roles,
            organizationId=str(user.org_id) if user.org_id else "",
            tenantId=str(user.tenant_id) if user.tenant_id else None,
        ),
    )


@app.post("/api/v1/auth/microsoft/datasource/callback", response_model=DatasourceCallbackResponse)
async def datasource_callback(
    callback: DatasourceConsentRequest,
    current_user: dict = Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db),
):
    """
    Handle Microsoft admin-consent callback for M365 datasource onboarding.
    
    Flow:
    1. Validate CSRF state
    2. Verify admin consent was granted
    3. Test client-credentials flow against the newly-consented tenant
    4. Store encrypted Graph API credentials on the Tenant row
    5. Publish discovery.m365 message to RabbitMQ
    """
    # 1. Validate CSRF state
    from shared.config import settings as app_settings
    if app_settings.REDIS_ENABLED:
        try:
            import redis.asyncio as aioredis
            redis_client = aioredis.from_url(
                app_settings.REDIS_URL_FULL,
                decode_responses=True,
            )
            expected_state = await redis_client.get(f"oauth_state:{current_user['id']}")
            await redis_client.delete(f"oauth_state:{current_user['id']}")
            await redis_client.aclose()
            if not expected_state or expected_state != callback.state:
                raise HTTPException(400, "Invalid or expired state token")
        except Exception:
            # Redis unavailable — skip CSRF check (dev mode)
            pass

    if not callback.admin_consent:
        raise HTTPException(400, "Admin consent was not granted")

    external_tenant_id = callback.external_tenant_id

    # 2. Test client-credentials flow against the newly-consented tenant
    async with httpx.AsyncClient(timeout=30.0) as client:
        token_url = f"https://login.microsoftonline.com/{external_tenant_id}/oauth2/v2.0/token"
        token_resp = await client.post(token_url, data={
            "client_id": settings.MICROSOFT_CLIENT_ID,
            "client_secret": settings.MICROSOFT_CLIENT_SECRET,
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials",
        })
        if token_resp.status_code != 200:
            raise HTTPException(400,
                f"Consent verification failed: {token_resp.text}. "
                "Confirm the app has required application permissions and the admin granted consent.")
        app_token = token_resp.json()["access_token"]

        # 3. Fetch organization info using the app token
        org_resp = await client.get(
            "https://graph.microsoft.com/v1.0/organization",
            headers={"Authorization": f"Bearer {app_token}"},
        )
        org_resp.raise_for_status()
        org_data = org_resp.json()
        display_name = (org_data["value"][0]["displayName"]
                        if org_data.get("value") else "M365 Tenant")

    # 4. Upsert tenant with CREDENTIALS STORED (encrypted)
    from shared.security import encrypt_secret
    encrypted_secret = encrypt_secret(settings.MICROSOFT_CLIENT_SECRET)

    # Scope by type=M365 so that an M365 onboarding against a Microsoft tenant
    # that was already onboarded as AZURE creates a SEPARATE M365 tenant row
    # (mirrors the Azure callback at line ~512). Before this, the M365 flow
    # silently merged its creds onto an existing AZURE row, leaving the
    # Tenants page showing only one row with the wrong badge.
    stmt = select(Tenant).where(
        Tenant.external_tenant_id == external_tenant_id,
        Tenant.type.cast(String) == "M365",
    )
    tenant = (await db.execute(stmt)).scalar_one_or_none()

    if tenant is None:
        org_id = UUID(current_user["org_id"]) if current_user.get("org_id") else None

        # Ensure org exists
        if org_id:
            org_stmt = select(Organization).where(Organization.id == org_id)
            org_result = await db.execute(org_stmt)
            org = org_result.scalar_one_or_none()
            if org is None:
                org = Organization(
                    id=org_id,
                    name="Default Organization",
                    slug="default-org",
                )
                db.add(org)
                await db.flush()

        tenant = Tenant(
            id=uuid4(),
            org_id=org_id,
            type=TenantType.M365,
            display_name=display_name,
            external_tenant_id=external_tenant_id,
            graph_client_id=settings.MICROSOFT_CLIENT_ID,
            graph_client_secret_encrypted=encrypted_secret,
            graph_delta_tokens={},
            status=TenantStatus.ACTIVE,
        )
        db.add(tenant)
        await db.flush()
        try:
            from shared.sla_presets import seed_preset_policies
            n = await seed_preset_policies(db, tenant.id, "M365")
            print(f"[auth-service] Seeded {n} preset SLA policies for new M365 tenant {tenant.id}")
        except Exception as exc:
            print(f"[auth-service] WARN preset SLA seeding failed: {exc}")
    else:
        tenant.graph_client_id = settings.MICROSOFT_CLIENT_ID
        tenant.graph_client_secret_encrypted = encrypted_secret
        tenant.status = TenantStatus.ACTIVE

    await db.flush()
    tenant_id = tenant.id

    # 4b. Also store in admin_consent_tokens table for settings page tracking
    from datetime import timedelta
    encrypted_access_token = encrypt_secret(app_token)
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=token_resp.json().get("expires_in", 3600))
    
    # Deactivate previous consent tokens for this tenant/type
    deactivate_stmt = select(AdminConsentToken).where(
        AdminConsentToken.tenant_id == tenant.id,
        AdminConsentToken.consent_type == "M365",
        AdminConsentToken.is_active == True,
    )
    old_tokens = (await db.execute(deactivate_stmt)).scalars().all()
    for old_token in old_tokens:
        old_token.is_active = False

    consent_token = AdminConsentToken(
        id=uuid4(),
        org_id=tenant.org_id,
        tenant_id=tenant.id,
        consent_type="M365",
        access_token_encrypted=encrypted_access_token,
        token_type="Bearer",
        expires_at=expires_at.replace(tzinfo=None),
        granted_by=current_user.get("email"),
        scope="https://graph.microsoft.com/.default",
        is_active=True,
    )
    db.add(consent_token)
    await db.commit()

    # 5. PUBLISH DISCOVERY JOB — critical missing step
    # Uses canonical signature: publish(routing_key: str, message: Dict, priority: int = 5)
    # Matching: services/job-service/main.py:200, services/backup-scheduler/main.py:226
    from shared.message_bus import message_bus as msg_bus
    if not msg_bus.connection:
        await msg_bus.connect()

    discovery_status = "queued"
    try:
        discovery_message = {
            "jobId": str(uuid4()),
            "tenantId": str(tenant_id),
            "externalTenantId": external_tenant_id,
            # Per-user OneDrive is a Tier 2 USER_ONEDRIVE row under each
            # ENTRA_USER, materialised on demand via discover_user_content.
            # Listing "onedrive" here would emit a duplicate Tier 1 ONEDRIVE
            # row per user and double the backup walk.
            "discoveryScope": ["users", "groups", "mailboxes", "shared_mailboxes",
                               "sharepoint", "teams"],
            "triggeredBy": str(current_user["id"]),
            "triggeredAt": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
        }
        print(f"[auth-service] Publishing discovery.m365 for tenant {tenant_id} ({display_name})")
        await msg_bus.publish("discovery.m365", discovery_message, priority=5)
        print(f"[auth-service] Published discovery.m365 successfully for tenant {tenant_id}")
    except Exception as e:
        import logging
        logging.getLogger("auth-service").exception(
            "Failed to publish discovery.m365 for tenant %s", tenant_id
        )
        # Mark tenant so a reconciler job picks it up
        async with async_session_factory() as retry_session:
            retry_tenant = await retry_session.get(Tenant, tenant_id)
            if retry_tenant:
                retry_tenant.status = TenantStatus.PENDING_DISCOVERY
                await retry_session.commit()
        discovery_status = "queue_failed_will_retry"

    return DatasourceCallbackResponse(
        tenantId=str(tenant_id),
        tenantName=display_name,
        discoveryStatus=discovery_status,
    )


@app.post("/api/v1/auth/azure/datasource/callback")
async def azure_datasource_callback(
    callback: OAuthCallbackRequest,
    current_user: dict = Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db),
):
    # Exchange code for tokens (Azure ARM scope)
    # CRITICAL: Use /organizations for token exchange since the auth URL used /organizations
    async with httpx.AsyncClient() as client:
        token_response = await client.post(
            "https://login.microsoftonline.com/organizations/oauth2/v2.0/token",
            data={
                "client_id": settings.MICROSOFT_CLIENT_ID,
                "client_secret": settings.MICROSOFT_CLIENT_SECRET,
                "code": callback.code,
                "redirect_uri": f"{settings.FRONTEND_URL}/azure-datasource-callback",
                "grant_type": "authorization_code",
            },
        )
        token_response.raise_for_status()
        tokens = token_response.json()

        # Get user info from Graph API (need to use the same token - if it has Graph scopes it will work)
        # If the token is only for ARM, we need to decode the JWT to get user info
        from jose import jwt
        from jose.exceptions import JWTError
        
        # Try to decode the access token to get user info
        display_name = "Azure Tenant"
        external_tenant_id = None
        
        try:
            # Decode JWT without verification to extract claims
            decoded = jwt.decode(tokens.get("access_token", ""), key="", algorithms=["RS256", "HS256"], options={"verify_signature": False, "verify_aud": False})
            display_name = decoded.get("name") or decoded.get("unique_name") or decoded.get("email") or "Azure Tenant"
            external_tenant_id = decoded.get("tid")  # Azure AD tenant ID
            
            if not external_tenant_id:
                # Try calling Azure ARM to get subscription info
                arm_response = await client.get(
                    "https://management.azure.com/subscriptions?api-version=2022-12-01",
                    headers={"Authorization": f"Bearer {tokens['access_token']}"},
                )
                if arm_response.status_code == 200:
                    arm_data = arm_response.json()
                    if arm_data.get("value"):
                        # Use first subscription's tenant ID
                        external_tenant_id = arm_data["value"][0].get("tenantId")
                        display_name = f"Azure - {arm_data['value'][0].get('displayName', 'Subscription')}"
        except JWTError:
            # If JWT decode fails, try ARM API
            try:
                arm_response = await client.get(
                    "https://management.azure.com/subscriptions?api-version=2022-12-01",
                    headers={"Authorization": f"Bearer {tokens['access_token']}"},
                )
                if arm_response.status_code == 200:
                    arm_data = arm_response.json()
                    if arm_data.get("value"):
                        external_tenant_id = arm_data["value"][0].get("tenantId")
                        display_name = f"Azure - {arm_data['value'][0].get('displayName', 'Subscription')}"
            except Exception:
                pass

    # Check if tenant already exists for this org
    org_id = UUID(current_user["org_id"]) if current_user.get("org_id") else None

    # The same Azure AD tenant ID is used for both M365 and Azure
    # datasources. Scope the lookup by type so onboarding Azure on a
    # tenant that was already onboarded as M365 creates a SEPARATE
    # AZURE tenant row — matches AFI's model and keeps the Tenants page
    # able to list the Azure datasource independently (was a bug where
    # Azure onboarding silently reused the M365 row, leaving the
    # Tenants page's Azure section empty while discovery ran).
    if external_tenant_id:
        stmt = select(Tenant).where(
            Tenant.external_tenant_id == external_tenant_id,
            Tenant.type.cast(String) == "AZURE",
        )
    else:
        stmt = select(Tenant).where(
            Tenant.type.cast(String) == "AZURE",
            Tenant.org_id == org_id,
        )

    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()

    if tenant is None:
        # Ensure org exists
        if org_id:
            org_stmt = select(Organization).where(Organization.id == org_id)
            org_result = await db.execute(org_stmt)
            org = org_result.scalar_one_or_none()
            if org is None:
                org = Organization(
                    id=org_id,
                    name="Taylor Morrison",
                    slug="taylor-morrison",
                )
                db.add(org)
                await db.flush()

        tenant = Tenant(
            id=uuid4(),
            org_id=org_id,
            type=TenantType.AZURE,
            display_name=display_name,
            external_tenant_id=external_tenant_id,
            status=TenantStatus.ACTIVE,
        )
        db.add(tenant)
        await db.flush()
        try:
            from shared.sla_presets import seed_preset_policies
            n = await seed_preset_policies(db, tenant.id, "AZURE")
            print(f"[auth-service] Seeded {n} preset SLA policies for new Azure tenant {tenant.id}")
        except Exception as exc:
            print(f"[auth-service] WARN preset SLA seeding failed: {exc}")
        await db.commit()
        print(f"[auth-service] Created Azure tenant: {tenant.id} ({display_name}), external_tenant_id={external_tenant_id}")
    else:
        print(f"[auth-service] Azure tenant already exists: {tenant.id} ({tenant.display_name}), type={tenant.type}")
        # Existing tenant keeps its original type. To onboard Azure on an M365
        # tenant (or vice versa), create a separate tenant row for the other side.

    # Store an active AZURE consent token so the Settings status card reflects
    # a successful Azure connect instead of remaining "Not granted".
    from shared.security import encrypt_secret

    deactivate_stmt = select(AdminConsentToken).where(
        AdminConsentToken.org_id == tenant.org_id,
        AdminConsentToken.consent_type == "AZURE",
        AdminConsentToken.is_active == True,
    )
    old_tokens = (await db.execute(deactivate_stmt)).scalars().all()
    for old_token in old_tokens:
        old_token.is_active = False

    encrypted_access_token = encrypt_secret(tokens["access_token"]) if tokens.get("access_token") else None
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=tokens.get("expires_in", 3600))
    consent_token = AdminConsentToken(
        id=uuid4(),
        org_id=tenant.org_id,
        tenant_id=tenant.id,
        consent_type="AZURE",
        access_token_encrypted=encrypted_access_token,
        refresh_token_encrypted=encrypt_secret(tokens["refresh_token"]) if tokens.get("refresh_token") else None,
        token_type=tokens.get("token_type", "Bearer"),
        expires_at=expires_at.replace(tzinfo=None),
        granted_by=current_user.get("email"),
        scope=tokens.get("scope") or "https://management.azure.com/.default",
        is_active=True,
    )
    db.add(consent_token)
    await db.commit()

    # Publish discovery.azure message so the discovery worker picks up Azure resources
    from shared.message_bus import message_bus as msg_bus
    if not msg_bus.connection:
        await msg_bus.connect()

    discovery_status = "queued"
    try:
        discovery_message = {
            "jobId": str(uuid4()),
            "tenantId": str(tenant.id),
            "externalTenantId": external_tenant_id or "",
            "discoveryScope": ["azure_vms", "azure_sql", "azure_postgresql"],
            "triggeredBy": str(current_user["id"]) if current_user.get("id") else "system",
            "triggeredAt": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
        }
        print(f"[auth-service] Publishing discovery.azure for tenant {tenant.id} ({tenant.display_name})")
        await msg_bus.publish("discovery.azure", discovery_message, priority=5)
        print(f"[auth-service] Published discovery.azure successfully for tenant {tenant.id}")
    except Exception as e:
        import logging
        logging.getLogger("auth-service").exception(
            "Failed to publish discovery.azure for tenant %s", tenant.id
        )
        discovery_status = "queue_failed_will_retry"

    return {"tenantId": str(tenant.id), "tenantName": tenant.display_name, "discoveryStatus": discovery_status}


@app.post("/api/v1/auth/power-bi/callback", response_model=AdminConsentTokenResponse)
async def power_bi_callback(
    callback: PowerBIOAuthCallbackRequest,
    response: Response,
    http_request: Request,
    current_user: dict = Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db),
):
    """Store delegated Power BI/Fabric refresh token for AFI-style service-user onboarding."""
    # CSRF check: the state nonce was issued as an HttpOnly cookie when the
    # SPA called /power-bi/url. JS (and therefore XSS) can't read or forge
    # the cookie, so an attacker who controls the SPA can't supply a
    # pre-known state value. Compare with constant time, then clear the
    # cookie so the same nonce can't be replayed.
    expected_state = http_request.cookies.get(_POWER_BI_STATE_COOKIE)
    if not expected_state or not callback.state or not secrets.compare_digest(
        expected_state, callback.state
    ):
        # Clear any stale cookie before rejecting so the next attempt starts
        # clean instead of reusing a leaked nonce.
        response.delete_cookie(
            _POWER_BI_STATE_COOKIE,
            path="/",
            domain=settings.COOKIE_DOMAIN,
            secure=settings.COOKIE_SECURE,
            samesite=settings.COOKIE_SAMESITE,
            httponly=True,
        )
        raise HTTPException(
            status_code=401,
            detail="Power BI sign-in state did not match. Please try again.",
        )
    response.delete_cookie(
        _POWER_BI_STATE_COOKIE,
        path="/",
        domain=settings.COOKIE_DOMAIN,
        secure=settings.COOKIE_SECURE,
        samesite=settings.COOKIE_SAMESITE,
        httponly=True,
    )

    org_id = UUID(current_user["org_id"]) if current_user.get("org_id") else None
    stmt = select(Tenant).where(Tenant.id == UUID(callback.tenantId))
    if org_id:
        stmt = stmt.where(Tenant.org_id == org_id)
    tenant = (await db.execute(stmt)).scalar_one_or_none()
    if tenant is None:
        raise HTTPException(404, "Tenant not found")

    async with httpx.AsyncClient(timeout=30.0) as client:
        token_response = await client.post(
            "https://login.microsoftonline.com/organizations/oauth2/v2.0/token",
            data={
                "client_id": settings.EFFECTIVE_POWER_BI_CLIENT_ID,
                "client_secret": settings.EFFECTIVE_POWER_BI_CLIENT_SECRET,
                "code": callback.code,
                "redirect_uri": f"{settings.FRONTEND_URL}/power-bi-callback",
                "grant_type": "authorization_code",
                "scope": _power_bi_code_redeem_scopes(),
            },
        )
        if token_response.status_code != 200:
            raise HTTPException(400, f"Power BI sign-in failed: {token_response.text}")
        tokens = token_response.json()

    refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        raise HTTPException(400, "Power BI onboarding did not return a refresh token. Make sure offline access was granted.")

    delegated_client = PowerBIClient(
        tenant.external_tenant_id or settings.EFFECTIVE_POWER_BI_TENANT_ID,
        client_id=settings.EFFECTIVE_POWER_BI_CLIENT_ID,
        client_secret=settings.EFFECTIVE_POWER_BI_CLIENT_SECRET,
        refresh_token=refresh_token,
    )
    try:
        workspaces = await delegated_client.list_workspaces()
    except Exception as exc:
        raise HTTPException(400, f"Power BI access validation failed: {_power_bi_error_detail(exc)}")

    from shared.security import encrypt_secret
    encrypted_access_token = encrypt_secret(tokens["access_token"]) if tokens.get("access_token") else None
    encrypted_refresh_token = encrypt_secret(refresh_token)

    deactivate_stmt = select(AdminConsentToken).where(
        AdminConsentToken.tenant_id == tenant.id,
        AdminConsentToken.consent_type == "POWER_BI",
        AdminConsentToken.is_active == True,
    )
    old_tokens = (await db.execute(deactivate_stmt)).scalars().all()
    for old_token in old_tokens:
        old_token.is_active = False

    consent_token = AdminConsentToken(
        id=uuid4(),
        org_id=tenant.org_id,
        tenant_id=tenant.id,
        consent_type="POWER_BI",
        access_token_encrypted=encrypted_access_token,
        refresh_token_encrypted=encrypted_refresh_token,
        token_type=tokens.get("token_type", "Bearer"),
        expires_at=(datetime.now(timezone.utc) + timedelta(seconds=tokens.get("expires_in", 3600))).replace(tzinfo=None),
        granted_by=current_user.get("email"),
        scope=tokens.get("scope"),
        is_active=True,
    )
    db.add(consent_token)

    tenant.extra_data = tenant.extra_data or {}
    tenant.extra_data["power_bi_auth_mode"] = "DELEGATED_SERVICE_USER"
    tenant.extra_data["power_bi_consented_at"] = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    tenant.extra_data["power_bi_workspace_count_hint"] = len(workspaces)
    tenant.extra_data["power_bi_uses_dedicated_app"] = bool(settings.POWER_BI_CLIENT_ID)

    id_token = tokens.get("id_token")
    if id_token:
        try:
            from jose import jwt
            claims = jwt.get_unverified_claims(id_token)
            tenant.extra_data["power_bi_service_user_email"] = claims.get("preferred_username") or claims.get("email")
            tenant.extra_data["power_bi_service_user_name"] = claims.get("name")
        except Exception:
            pass

    await PowerBIClient.persist_refresh_token(db, tenant, delegated_client.refresh_token or refresh_token)

    await db.commit()

    from shared.message_bus import message_bus as msg_bus
    if not msg_bus.connection:
        await msg_bus.connect()
    try:
        await msg_bus.publish(
            "discovery.m365",
            {
                "jobId": str(uuid4()),
                "tenantId": str(tenant.id),
                "externalTenantId": tenant.external_tenant_id,
                "discoveryScope": ["power_platform"],
                "triggeredBy": str(current_user["id"]),
                "triggeredAt": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
            },
            priority=5,
        )
    except Exception:
        # Best-effort only; user can still run discovery manually.
        pass

    return AdminConsentTokenResponse(
        tenantId=str(tenant.id),
        consentType="POWER_BI",
        message="Power BI service user connected successfully.",
        consentedAt=consent_token.consented_at.isoformat() if consent_token.consented_at else datetime.now(timezone.utc).isoformat(),
    )


# ============ Admin Consent Status Endpoints ============

@app.get("/api/v1/admin-consent/m365/status", response_model=Optional[AdminConsentResponse])
async def get_m365_admin_consent_status(
    current_user: dict = Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db),
):
    """Get the current M365 admin consent status for the organization."""
    org_id = UUID(current_user["org_id"]) if current_user.get("org_id") else None
    
    stmt = select(AdminConsentToken).where(
        AdminConsentToken.org_id == org_id,
        AdminConsentToken.consent_type == "M365",
        AdminConsentToken.is_active == True,
    ).order_by(AdminConsentToken.consented_at.desc()).limit(1)
    
    result = await db.execute(stmt)
    token = result.scalar_one_or_none()
    
    if token is None:
        return None
    
    return AdminConsentResponse.model_validate(token)


@app.get("/api/v1/admin-consent/azure/status", response_model=Optional[AdminConsentResponse])
async def get_azure_admin_consent_status(
    current_user: dict = Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db),
):
    """Get the current Azure admin consent status for the organization."""
    org_id = UUID(current_user["org_id"]) if current_user.get("org_id") else None
    
    stmt = select(AdminConsentToken).where(
        AdminConsentToken.org_id == org_id,
        AdminConsentToken.consent_type == "AZURE",
        AdminConsentToken.is_active == True,
    ).order_by(AdminConsentToken.consented_at.desc()).limit(1)
    
    result = await db.execute(stmt)
    token = result.scalar_one_or_none()
    
    if token is None:
        tenant_stmt = select(Tenant).where(
            Tenant.org_id == org_id,
            Tenant.type == TenantType.AZURE,
        ).order_by(Tenant.updated_at.desc()).limit(1)
        tenant = (await db.execute(tenant_stmt)).scalar_one_or_none()
        if tenant is None:
            return None

        fallback_timestamp = tenant.updated_at or tenant.created_at or datetime.now(timezone.utc).replace(tzinfo=None)
        return AdminConsentResponse(
            id=str(tenant.id),
            consentType="AZURE",
            grantedBy=current_user.get("email"),
            consentedAt=fallback_timestamp.isoformat(),
            lastUsedAt=tenant.last_discovery_at.isoformat() if tenant.last_discovery_at else None,
            isActive=True,
            scope="https://management.azure.com/.default",
        )

    return AdminConsentResponse.model_validate(token)


@app.get("/api/v1/admin-consent/power-bi/readiness", response_model=PowerBIReadinessResponse)
async def get_power_bi_readiness(
    tenant_id: str = Query(..., alias="tenantId"),
    current_user: dict = Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db),
):
    """Summarize whether Power BI backup onboarding is ready for this tenant."""
    org_id = UUID(current_user["org_id"]) if current_user.get("org_id") else None

    tenant_stmt = select(Tenant).where(Tenant.id == UUID(tenant_id))
    if org_id:
        tenant_stmt = tenant_stmt.where(Tenant.org_id == org_id)
    tenant = (await db.execute(tenant_stmt)).scalar_one_or_none()
    if tenant is None:
        raise HTTPException(404, "Tenant not found")

    checks = []
    recommended_actions = []

    creds_configured = bool(settings.EFFECTIVE_POWER_BI_CLIENT_ID and settings.EFFECTIVE_POWER_BI_CLIENT_SECRET)
    if creds_configured:
        checks.append({
            "key": "credentials",
            "label": "App credentials",
            "status": "ready",
            "detail": "A Power BI-capable app registration is configured for this deployment.",
        })
    else:
        checks.append({
            "key": "credentials",
            "label": "App credentials",
            "status": "action_required",
            "detail": "No Power BI app credentials are configured. Add POWER_BI_* values or reuse the primary Microsoft app credentials.",
        })
        recommended_actions.append("Configure POWER_BI_CLIENT_ID / POWER_BI_CLIENT_SECRET / POWER_BI_TENANT_ID, or make sure the primary Microsoft app credentials are present.")

    consent_stmt = select(AdminConsentToken).where(
        AdminConsentToken.org_id == org_id,
        AdminConsentToken.consent_type == "M365",
        AdminConsentToken.is_active == True,
    ).order_by(AdminConsentToken.consented_at.desc()).limit(1)
    m365_token = (await db.execute(consent_stmt)).scalar_one_or_none()
    if m365_token:
        checks.append({
            "key": "m365_consent",
            "label": "Microsoft 365 admin consent",
            "status": "ready",
            "detail": "Tenant admin consent is already stored for the Microsoft 365 datasource.",
        })
    else:
        checks.append({
            "key": "m365_consent",
            "label": "Microsoft 365 admin consent",
            "status": "action_required",
            "detail": "Grant Microsoft 365 admin consent first so the tenant is connected before Power BI discovery runs.",
        })
        recommended_actions.append("Grant Microsoft 365 admin consent from Settings before testing Power BI discovery.")

    power_bi_consent_stmt = select(AdminConsentToken).where(
        AdminConsentToken.tenant_id == tenant.id,
        AdminConsentToken.consent_type == "POWER_BI",
        AdminConsentToken.is_active == True,
    ).order_by(AdminConsentToken.consented_at.desc()).limit(1)
    power_bi_token = (await db.execute(power_bi_consent_stmt)).scalar_one_or_none()
    delegated_refresh_token = PowerBIClient.get_refresh_token_from_tenant(tenant)
    auth_mode = "DELEGATED_SERVICE_USER" if delegated_refresh_token else "APP_ONLY"

    if power_bi_token and delegated_refresh_token:
        service_user = (tenant.extra_data or {}).get("power_bi_service_user_email") or power_bi_token.granted_by or "service user"
        checks.append({
            "key": "power_bi_connection",
            "label": "Power BI service-user connection",
            "status": "ready",
            "detail": f"Connected as {service_user}. TMVault will prefer delegated Power BI auth for discovery and backup.",
        })
    else:
        checks.append({
            "key": "power_bi_connection",
            "label": "Power BI service-user connection",
            "status": "warning",
            "detail": "No delegated Power BI service user is connected yet. TMVault will fall back to the app-only setup.",
        })
        recommended_actions.append("Click 'Connect service user' to use the simpler AFI-style Power BI onboarding flow.")

    discovered_power_bi = (await db.execute(
        select(Resource).where(
            Resource.tenant_id == tenant.id,
            Resource.type == ResourceType.POWER_BI,
        )
    )).scalars().all()
    discovered_workspace_count = len(discovered_power_bi)

    accessible_workspace_count = 0
    api_access_ok = False
    admin_api_ok = False

    if creds_configured:
        client = PowerBIClient(
            tenant.external_tenant_id or settings.EFFECTIVE_POWER_BI_TENANT_ID,
            refresh_token=delegated_refresh_token,
        )
        try:
            workspaces = await client.list_workspaces()
            accessible_workspace_count = len(workspaces)
            api_access_ok = True
            mode_label = "service user" if delegated_refresh_token else "app"
            detail = f"The connected {mode_label} can list {accessible_workspace_count} Power BI workspace(s) in this tenant."
            if accessible_workspace_count == 0:
                if delegated_refresh_token:
                    detail = "The connected service user authenticated successfully, but it does not currently have access to any Power BI workspaces."
                    recommended_actions.append("Grant the connected service user access to at least one Power BI workspace, or create a shared workspace for backup testing.")
                else:
                    detail = "The app can talk to Power BI, but it does not currently have access to any workspaces."
                    recommended_actions.append("Add the app or its security group to at least one Power BI workspace as Member or Admin.")
            checks.append({
                "key": "workspace_api",
                "label": "Workspace API access",
                "status": "ready" if accessible_workspace_count > 0 else "action_required",
                "detail": detail,
            })
        except Exception as exc:
            checks.append({
                "key": "workspace_api",
                "label": "Workspace API access",
                "status": "action_required",
                "detail": _power_bi_error_detail(exc),
            })
            recommended_actions.append(
                "Verify the Power BI connection has the right Fabric role and workspace access, then reconnect if needed."
                if delegated_refresh_token
                else "In the Fabric admin portal, allow the service principal to use Fabric public APIs and add it to the target workspaces."
            )

        if api_access_ok:
            try:
                await client.list_modified_workspace_ids(datetime.utcnow() - timedelta(days=1))
                admin_api_ok = True
                checks.append({
                    "key": "admin_api",
                    "label": "Admin API access",
                    "status": "ready",
                    "detail": "Read-only admin APIs are available for richer discovery and faster change detection.",
                })
            except Exception as exc:
                checks.append({
                    "key": "admin_api",
                    "label": "Admin API access",
                    "status": "warning",
                    "detail": _power_bi_error_detail(exc),
                })
                recommended_actions.append(
                    "Enable read-only admin APIs in the Fabric admin portal so TMVault can do richer discovery and faster incremental checks."
                )

    discovery_status = "ready" if discovered_workspace_count > 0 else "warning"
    discovery_detail = (
        f"{discovered_workspace_count} Power BI workspace resource(s) are already discovered in TMVault."
        if discovered_workspace_count > 0
        else "No Power BI workspace resources have been discovered in TMVault yet. Run discovery after access is ready."
    )
    checks.append({
        "key": "discovery",
        "label": "TMVault discovery",
        "status": discovery_status,
        "detail": discovery_detail,
    })
    if discovered_workspace_count == 0:
        recommended_actions.append("Run Power Platform discovery after the Power BI checks above are green.")

    status = "ready"
    if any(check["status"] == "action_required" for check in checks):
        status = "action_required"
    elif any(check["status"] == "warning" for check in checks):
        status = "warning"

    summary = "Power BI backup is ready."
    if status == "action_required":
        summary = "Power BI still needs setup before discovery and backup will work cleanly."
    elif status == "warning":
        summary = "Power BI is partially ready, but some capabilities are limited."
    if auth_mode == "DELEGATED_SERVICE_USER" and status == "ready":
        summary = "Power BI backup is ready with AFI-style service-user onboarding."

    return PowerBIReadinessResponse(
        tenantId=str(tenant.id),
        status=status,
        summary=summary,
        authMode=auth_mode,
        usesDedicatedApp=bool(settings.POWER_BI_CLIENT_ID),
        accessibleWorkspaceCount=accessible_workspace_count,
        discoveredWorkspaceCount=discovered_workspace_count,
        checks=checks,
        recommendedActions=list(dict.fromkeys(recommended_actions)),
    )


@app.post("/api/v1/auth/refresh", response_model=RefreshTokenResponse)
async def refresh_token(
    response: Response,
    http_request: Request,
    request: Optional[RefreshTokenRequest] = None,
    db: AsyncSession = Depends(get_db),
):
    # Cookie-first: the SPA no longer holds the refresh token in JS, so the
    # request body is empty. Fall back to the body for non-browser callers.
    cookie_token = http_request.cookies.get("refresh_token")
    body_token = request.refreshToken if request is not None else None
    raw_token = cookie_token or body_token
    if not raw_token:
        raise HTTPException(status_code=401, detail="Missing refresh token")

    payload = decode_token(raw_token, expected_type="refresh")

    # Revocation check: a stolen refresh token replayed after the legitimate
    # user already rotated it (or after explicit logout) lands here.
    old_jti = payload.get("jti")
    if old_jti and await is_refresh_token_revoked(old_jti):
        raise HTTPException(status_code=401, detail="Refresh token revoked")

    # Re-read claims from the DB rather than echoing the previous payload — an
    # admin role grant/revoke or tenant reassignment between login and refresh
    # would otherwise stay frozen into the user's session for the refresh
    # token's full lifetime.
    sub = payload.get("sub")
    if not sub:
        raise HTTPException(status_code=401, detail="Refresh token missing subject")
    try:
        user_uuid = UUID(sub)
    except (TypeError, ValueError):
        raise HTTPException(status_code=401, detail="Refresh token has invalid subject")
    token_data = await _load_token_claims_from_db(db, user_uuid)
    if token_data is None:
        raise HTTPException(status_code=401, detail="User no longer exists")

    access_token = create_access_token(token_data)
    new_refresh_token = create_refresh_token(token_data)
    expires_in = settings.JWT_EXPIRATION_HOURS * 3600

    # Refresh-token rotation: revoke the just-used jti so the same token
    # can't be used twice. TTL = remaining lifetime of the old token, so the
    # denylist entry expires naturally and Redis doesn't grow unbounded.
    if old_jti:
        old_exp = payload.get("exp")
        if isinstance(old_exp, (int, float)):
            now_ts = int(datetime.now(timezone.utc).timestamp())
            remaining = max(1, int(old_exp) - now_ts)
        else:
            remaining = settings.JWT_REFRESH_EXPIRATION_DAYS * 86400
        await revoke_refresh_token(old_jti, remaining)

    _set_auth_cookies(response, access_token, new_refresh_token)

    return RefreshTokenResponse(accessToken=access_token, refreshToken=new_refresh_token, expiresIn=expires_in)


@app.post("/api/v1/auth/logout")
async def logout(response: Response, http_request: Request):
    # Revoke the refresh token's jti so a copy stolen before logout can't be
    # used to mint new sessions. Best-effort — if Redis is down or the cookie
    # is malformed, still clear the cookies and return success so the client
    # transitions to the signed-out state.
    cookie_token = http_request.cookies.get("refresh_token")
    if cookie_token:
        try:
            payload = decode_token(cookie_token, expected_type="refresh")
            jti = payload.get("jti")
            exp = payload.get("exp")
            if jti:
                if isinstance(exp, (int, float)):
                    now_ts = int(datetime.now(timezone.utc).timestamp())
                    remaining = max(1, int(exp) - now_ts)
                else:
                    remaining = settings.JWT_REFRESH_EXPIRATION_DAYS * 86400
                await revoke_refresh_token(jti, remaining)
        except HTTPException:
            # Already-expired or tampered token — nothing to revoke.
            pass

    _clear_auth_cookies(response)
    return {"message": "Logged out successfully"}


@app.get("/api/v1/auth/me", response_model=UserResponse)
async def get_me(
    user: dict = Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(PlatformUser).where(PlatformUser.id == UUID(user["id"]))
    result = await db.execute(stmt)
    platform_user = result.scalar_one_or_none()

    if not platform_user:
        raise HTTPException(status_code=404, detail="User not found")

    # Don't echo `user["roles"]` from the JWT — those are frozen at login time
    # and stay stale until refresh. The UI calls /me to learn the current
    # permission set, so it must come from UserRoleMapping.
    roles_result = await db.execute(
        select(UserRoleMapping).where(UserRoleMapping.user_id == platform_user.id)
    )
    fresh_roles = [r.role.value for r in roles_result.scalars().all()]

    return UserResponse(
        id=str(platform_user.id),
        email=platform_user.email,
        name=platform_user.name,
        roles=fresh_roles,
        organizationId=str(platform_user.org_id) if platform_user.org_id else "",
        tenantId=str(platform_user.tenant_id) if platform_user.tenant_id else None,
    )
