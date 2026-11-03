"""Microsoft sign-in exchange with safe, actionable provider failures."""
import logging

import httpx
from fastapi import HTTPException

from shared.config import settings

log = logging.getLogger("tmvault.auth.microsoft_oauth")


async def microsoft_login_profile(code: str) -> dict:
    stage = "code_exchange"
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            token_response = await client.post(settings.MICROSOFT_TOKEN_URL, data={
                "client_id": settings.MICROSOFT_CLIENT_ID,
                "client_secret": settings.MICROSOFT_CLIENT_SECRET,
                "code": code,
                "redirect_uri": f"{settings.FRONTEND_URL}/auth/callback",
                "grant_type": "authorization_code",
            })
            token_response.raise_for_status()
            tokens = token_response.json()
            if not isinstance(tokens, dict) or not isinstance(tokens.get("access_token"), str) or not tokens["access_token"]:
                raise ValueError("Missing access token")

            stage = "profile"
            profile_response = await client.get("https://graph.microsoft.com/v1.0/me", headers={
                "Authorization": f"Bearer {tokens['access_token']}",
            })
            profile_response.raise_for_status()
            profile = profile_response.json()
            if not isinstance(profile, dict):
                raise ValueError("Invalid profile")
            email = profile.get("mail") or profile.get("userPrincipalName")
            if not isinstance(email, str) or not email.strip() or not isinstance(profile.get("id"), str) or not profile["id"]:
                raise ValueError("Incomplete profile")
            return profile
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        error = "unknown"
        codes = []
        if stage == "code_exchange":
            try:
                body = exc.response.json()
            except ValueError:
                body = None
            if isinstance(body, dict):
                if body.get("error") in ("invalid_client", "invalid_grant", "access_denied", "temporarily_unavailable", "server_error"):
                    error = body["error"]
                raw_codes = body.get("error_codes")
                if isinstance(raw_codes, list):
                    codes = [c for c in raw_codes[:3] if type(c) is int and 0 < c < 1_000_000_000]
        # Do not log provider bodies/descriptions, codes, tokens, or request data.
        log.warning("Microsoft SSO rejected stage=%s status=%s error=%s aadsts=%s", stage, status, error, codes)
        if error == "invalid_client":
            raise HTTPException(503, "Microsoft sign-in is unavailable. Please contact your administrator.") from None
        if error in ("invalid_grant", "access_denied"):
            raise HTTPException(401, "Microsoft sign-in expired or was rejected. Please sign in again.") from None
        if status == 429 or status >= 500:
            raise HTTPException(503, "Microsoft sign-in is temporarily unavailable. Please try again shortly.") from None
        raise HTTPException(502, "Microsoft sign-in could not be completed. Please try signing in again.") from None
    except httpx.TimeoutException:
        log.warning("Microsoft SSO timed out stage=%s", stage)
        raise HTTPException(504, "Microsoft sign-in timed out. Please try signing in again.") from None
    except httpx.RequestError:
        log.warning("Microsoft SSO connection failed stage=%s", stage)
        raise HTTPException(503, "Microsoft sign-in is temporarily unavailable. Please try again shortly.") from None
    except ValueError:
        log.warning("Microsoft SSO returned an invalid response stage=%s", stage)
        raise HTTPException(502, "Microsoft sign-in returned an incomplete response. Please try signing in again.") from None
