"""
Report Service - Scheduled Report Configuration & History
Port: 8014

Responsibilities:
- Manage report configuration (CRUD)
- Track report sending history
- Generate and send scheduled reports (called by scheduler)
- Support multiple notification endpoints (email, Slack, Teams, Google Chat)
"""
import uuid
import httpx
import smtplib
import asyncio
import csv
import io
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from email.utils import parseaddr, formataddr
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Query, Depends
from pydantic import BaseModel, Field
from sqlalchemy import select, desc, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from shared.database import async_session_factory, init_db
from shared.models import (
    ReportConfig, ReportHistory,
    Job, Resource, Tenant, Snapshot,
    JobType, JobStatus, ResourceStatus, ResourceType
)
from shared.config import settings
from shared.security import get_current_user_from_token
from shared.storage_rollup import exclude_tier2_storage_dupes_clause

app = FastAPI(title="Report Service", version="1.0.0")

# SMTP config from environment
SMTP_ENABLED = os.getenv("NOTIFICATION_EMAIL_ENABLED", "false").lower() in ("true", "1", "yes")
SMTP_HOST = os.getenv("NOTIFICATION_EMAIL_SMTP_HOST", "smtp.office365.com")
SMTP_PORT = int(os.getenv("NOTIFICATION_EMAIL_SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("NOTIFICATION_EMAIL_SMTP_USERNAME", "")
SMTP_PASSWORD = os.getenv("NOTIFICATION_EMAIL_SMTP_PASSWORD", "")
SMTP_FROM = os.getenv("NOTIFICATION_EMAIL_FROM", "noreply@tm-vault.io")


# ==================== Pydantic Schemas ====================

class WebhookConfig(BaseModel):
    name: str
    url: str
    enabled: bool = True


class ReportConfigCreate(BaseModel):
    enabled: bool = False
    schedule_type: str = Field(default="daily")
    send_empty_report: bool = True
    empty_message: Optional[str] = "No updates. No backups occurred."
    send_detailed_report: bool = False
    email_recipients: List[str] = []
    slack_webhooks: List[WebhookConfig] = []
    teams_webhooks: List[WebhookConfig] = []


class ReportConfigUpdate(BaseModel):
    enabled: Optional[bool] = None
    schedule_type: Optional[str] = None
    send_empty_report: Optional[bool] = None
    empty_message: Optional[str] = None
    send_detailed_report: Optional[bool] = None
    email_recipients: Optional[List[str]] = None
    slack_webhooks: Optional[List[WebhookConfig]] = None
    teams_webhooks: Optional[List[WebhookConfig]] = None


class ReportConfigResponse(BaseModel):
    id: str
    org_id: str
    enabled: bool
    schedule_type: str
    send_empty_report: bool
    empty_message: Optional[str]
    send_detailed_report: bool
    email_recipients: List[str]
    slack_webhooks: List[WebhookConfig]
    teams_webhooks: List[WebhookConfig]
    created_at: str
    updated_at: str

    class Config:
        from_attributes = True


class ReportHistoryResponse(BaseModel):
    id: str
    org_id: Optional[str]
    report_config_id: Optional[str]
    report_type: str
    period_start: Optional[str]
    period_end: Optional[str]
    generated_at: str
    total_backups: int
    successful_backups: int
    failed_backups: int
    success_rate: Optional[str]
    coverage_rate: Optional[str]
    is_empty: bool
    delivery_status: Optional[dict]
    error_message: Optional[str]
    created_at: str

    class Config:
        from_attributes = True


# ==================== Helper Functions ====================


def _require_org_id(current_user: dict) -> str:
    org_id = current_user.get("org_id")
    if not org_id:
        raise HTTPException(status_code=403, detail="User is not bound to an organization")
    return org_id


async def save_report_history(session, config, report_type, period_start, period_end,
                              total_backups, successful_backups, failed_backups,
                              is_empty=False, report_data=None, delivery_status=None):
    """Save report to history table"""
    success_rate = f"{(successful_backups / total_backups * 100) if total_backups > 0 else 0:.1f}%"
    
    org_id = config.org_id if hasattr(config, 'org_id') else None
    
    history_record = ReportHistory(
        org_id=org_id,
        report_config_id=config.id,
        report_type=report_type,
        period_start=period_start,
        period_end=period_end,
        total_backups=total_backups,
        successful_backups=successful_backups,
        failed_backups=failed_backups,
        success_rate=success_rate,
        is_empty=is_empty,
        report_data=report_data,
        delivery_status=delivery_status,
    )
    session.add(history_record)
    await session.commit()
    print(f"[REPORT] Saved {report_type} report to history (empty={is_empty})")


# ==================== Report Generation ====================

async def send_report_to_channels(
    report: Dict[str, Any],
    config: ReportConfig,
    is_empty: bool = False,
    csv_bytes: Optional[bytes] = None,
    idempotency_key: Optional[str] = None,
):
    """Fan out a report to configured notification channels.
    idempotency_key flows through to SMTP as Resend-Idempotency-Key."""
    delivery_status = {"email": "skipped", "slack": "skipped", "teams": "skipped"}

    if config.teams_webhooks:
        for webhook in config.teams_webhooks:
            if webhook.get("enabled"):
                result = await send_teams_webhook_notification(report, webhook["url"], is_empty, config.empty_message)
                delivery_status["teams"] = "sent" if result else "failed"

    if config.slack_webhooks:
        for webhook in config.slack_webhooks:
            if webhook.get("enabled"):
                result = await send_slack_webhook_notification(report, webhook["url"], is_empty, config.empty_message)
                delivery_status["slack"] = "sent" if result else "failed"

    if config.email_recipients:
        result = await send_email_notification(
            report, config.email_recipients, is_empty, config.empty_message,
            csv_bytes, idempotency_key=idempotency_key,
        )
        delivery_status["email"] = "sent" if result else "failed"

    return delivery_status


async def send_teams_webhook_notification(report: Dict[str, Any], webhook_url: str, is_empty: bool, empty_message: str):
    """Send report to Teams webhook"""
    try:
        period = report.get("period", "")
        if is_empty:
            message = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "summary": "Backup Report",
                "themeColor": "FFA500",
                "title": f"Backup report ({period}): No backups occurred",
                "sections": [{"text": empty_message or "No updates. No backups occurred."}],
            }
        else:
            summary = report.get("summary", {})
            breakdown = report.get("resource_breakdown", {})
            domain = report.get("tenant_domain", "")

            breakdown_facts = [{"name": label, "value": f"protected {v['protected']} out of {v['total']}"} for label, v in breakdown.items()]
            breakdown_facts.append({"name": "Total", "value": f"{summary.get('protected_resources', 0)} out of {summary.get('total_resources', 0)} resources protected"})
            breakdown_facts.append({"name": "Backup storage size", "value": f"{summary.get('storage_gb', 0)} GB"})

            message = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "summary": "Backup Report",
                "themeColor": "0078D4",
                "title": f"Backup report ({period}): Everything looks good",
                "sections": [
                    {
                        "activityTitle": "Overview",
                        "facts": [{"name": "Microsoft 365 domain", "value": domain}] + breakdown_facts,
                    },
                    {
                        "activityTitle": "Backup and recovery details",
                        "facts": [
                            {"name": "Backup", "value": f"{summary.get('successful_backups', 0)} succeeded"},
                            {"name": "Discover", "value": f"{summary.get('successful_discoveries', 0)} succeeded"},
                        ],
                    },
                ],
            }

        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(webhook_url, json=message)
            return True
    except Exception as e:
        print(f"[REPORT] Failed to send Teams notification: {e}")
        return False


async def send_slack_webhook_notification(report: Dict[str, Any], webhook_url: str, is_empty: bool, empty_message: str):
    """Send report to Slack webhook"""
    try:
        period = report.get("period", "")
        if is_empty:
            blocks = [
                {"type": "header", "text": {"type": "plain_text", "text": f"Backup report ({period}): No backups occurred"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": empty_message or "No updates. No backups occurred."}},
            ]
        else:
            summary = report.get("summary", {})
            breakdown = report.get("resource_breakdown", {})
            domain = report.get("tenant_domain", "")

            breakdown_lines = "\n".join([f"• {label}: protected {v['protected']} out of {v['total']}" for label, v in breakdown.items()])
            breakdown_lines += f"\n• Total: {summary.get('protected_resources', 0)} out of {summary.get('total_resources', 0)} resources protected"
            breakdown_lines += f"\n• Backup storage size: {summary.get('storage_gb', 0)} GB"

            blocks = [
                {"type": "header", "text": {"type": "plain_text", "text": f"Backup report ({period}): Everything looks good"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Overview*\n*Microsoft 365 domain:* {domain}\n{breakdown_lines}"}},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Backup and recovery details*\n• Backup: {summary.get('successful_backups', 0)} succeeded\n• Discover: {summary.get('successful_discoveries', 0)} succeeded"}},
            ]

        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(webhook_url, json={"blocks": blocks})
            return True
    except Exception as e:
        print(f"[REPORT] Failed to send Slack notification: {e}")
        return False


def _parse_from_header() -> tuple[str, str]:
    """Split NOTIFICATION_EMAIL_FROM into (display_header_value, envelope_addr).

    Resend (and all SMTP servers) require the envelope-level MAIL FROM to be a
    bare address — the `Name <addr@host>` display-name form is only valid in
    the message header. We accept either:
      - bare form:   "report@codereport.com"
      - display form: '"TM Vault Reports" <report@codereport.com>'
                 or: 'TM Vault Reports <report@codereport.com>'
    and produce both variants.

    Fallback: if the env value is empty/invalid, use a sensible default."""
    raw = SMTP_FROM.strip()
    if not raw:
        return ("TM Vault Reports <noreply@tm-vault.io>", "noreply@tm-vault.io")
    display_name, addr = parseaddr(raw)
    if not addr:
        # parseaddr returned no address — treat entire string as a bare address.
        return (raw, raw)
    if display_name:
        return (formataddr((display_name, addr)), addr)
    # No display name — use a tenant-friendly default for the header only.
    return (formataddr(("TM Vault Reports", addr)), addr)


def _build_email_subject_and_body(report: Dict[str, Any], is_empty: bool, empty_message: str) -> tuple[str, str]:
    """Compose subject + HTML body. Shared between the SMTP and Resend HTTP
    code paths so changes to formatting hit both at once."""
    period = report.get("period", "")
    if is_empty:
        subject = f"Backup report ({period}): No backups occurred"
        body_html = f"<p style='font-family:sans-serif'>{empty_message or 'No updates. No backups occurred.'}</p>"
        return subject, body_html

    summary = report.get("summary", {})
    breakdown = report.get("resource_breakdown", {})
    domain = report.get("tenant_domain", "")
    subject = f"Backup report ({period}): Everything looks good"

    breakdown_rows = "".join([
        f"<tr><td style='padding:4px 12px;color:#555'>{label}</td>"
        f"<td style='padding:4px 12px'>protected {v['protected']} out of {v['total']}</td></tr>"
        for label, v in breakdown.items()
    ])
    breakdown_rows += (
        f"<tr><td style='padding:4px 12px;font-weight:bold'>Total</td>"
        f"<td style='padding:4px 12px;font-weight:bold'>{summary.get('protected_resources', 0)} out of {summary.get('total_resources', 0)} resources protected</td></tr>"
        f"<tr><td style='padding:4px 12px;color:#555'>Backup storage size</td>"
        f"<td style='padding:4px 12px'>{summary.get('storage_gb', 0)} GB</td></tr>"
    )

    body_html = f"""
<div style='font-family:sans-serif;font-size:14px;color:#1a1a1a;max-width:600px'>
  <h2 style='color:#0d9488'>Backup report ({period}): Everything looks good</h2>

  <h3 style='margin-top:24px;border-bottom:1px solid #e2e8f0;padding-bottom:6px'>Overview</h3>
  <table style='border-collapse:collapse;width:100%'>
    <tr><td style='padding:4px 12px;color:#555'>Microsoft 365 domain</td><td style='padding:4px 12px'>{domain}</td></tr>
    {breakdown_rows}
  </table>

  <h3 style='margin-top:24px;border-bottom:1px solid #e2e8f0;padding-bottom:6px'>Backup and recovery details</h3>
  <table style='border-collapse:collapse;width:100%'>
    <tr><td style='padding:4px 12px;color:#555'>Backup</td><td style='padding:4px 12px'>{summary.get('successful_backups', 0)} succeeded</td></tr>
    <tr><td style='padding:4px 12px;color:#555'>Discover</td><td style='padding:4px 12px'>{summary.get('successful_discoveries', 0)} succeeded</td></tr>
  </table>

  <p style='margin-top:32px;color:#555'>Sincerely,<br><strong>The TM Vault Team</strong></p>
</div>
"""
    return subject, body_html


async def _send_via_resend_http_api(
    from_header: str,
    recipients: List[str],
    subject: str,
    body_html: str,
    csv_bytes: Optional[bytes],
    idempotency_key: Optional[str],
) -> bool:
    """POST to Resend's HTTPS API at api.resend.com — only path that works
    from hosting providers that block outbound SMTP (Railway hobby tier,
    Heroku, AWS Lambda by default, etc.).

    Auth: Bearer <SMTP_PASSWORD> — Resend reuses the same `re_…` API key for
    SMTP auth and HTTP API auth, so we don't need a separate env var.
    """
    import base64
    payload: Dict[str, Any] = {
        "from": from_header,
        "to": recipients,
        "subject": subject,
        "html": body_html,
    }
    if csv_bytes:
        # HTTP API accepts attachments as base64-encoded strings
        payload["attachments"] = [{
            "filename": "backup_details.csv",
            "content": base64.b64encode(csv_bytes).decode("ascii"),
            "content_type": "text/csv",
        }]
    headers = {"Authorization": f"Bearer {SMTP_PASSWORD}"}
    if idempotency_key:
        # Resend supports this on both SMTP (as a message header) and HTTP (as an HTTP header)
        headers["Idempotency-Key"] = str(idempotency_key)

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            r = await client.post("https://api.resend.com/emails", json=payload, headers=headers)
        except httpx.RequestError as e:
            print(f"[REPORT] Resend HTTP API connection error: {type(e).__name__}: {e}")
            return False

    if r.status_code in (200, 202):
        try:
            data = r.json()
            email_id = data.get("id") if isinstance(data, dict) else None
        except Exception:
            email_id = None
        print(f"[REPORT] Email sent via Resend HTTP API to {len(recipients)} recipient(s) id={email_id} from={from_header}")
        return True

    # Non-2xx: Resend returns a structured JSON error like
    #   {"statusCode":403,"name":"validation_error","message":"From email is invalid"}
    try:
        body = r.json()
    except Exception:
        body = r.text[:300]
    print(f"[REPORT] Resend HTTP API rejected (status={r.status_code}): {body}")
    return False


async def send_email_notification(
    report: Dict[str, Any],
    recipients: List[str],
    is_empty: bool,
    empty_message: str,
    csv_bytes: Optional[bytes] = None,
    idempotency_key: Optional[str] = None,
):
    """Send report via the most appropriate transport for the configured host.

    Auto-routing:
      - If SMTP_HOST is on resend.com → use Resend's HTTPS API (port 443).
        Required for Railway hobby and any cloud where outbound SMTP is
        blocked. Resend reuses the SMTP_PASSWORD as the HTTP API bearer
        token so no extra env var is needed.
      - Otherwise → fall through to standard SMTP (works locally, on VPSes,
        and on cloud providers without SMTP egress restrictions).

    `idempotency_key`, when supplied, becomes Resend's `Idempotency-Key`
    header so retries don't double-send. Pass the report_id for that."""
    if not recipients:
        return False

    if not SMTP_ENABLED:
        print(f"[REPORT] Email notification skipped (NOTIFICATION_EMAIL_ENABLED=false)")
        return False

    if not SMTP_USERNAME or not SMTP_PASSWORD:
        print(f"[REPORT] Email notification skipped (SMTP credentials not configured)")
        return False

    from_header, envelope_from = _parse_from_header()

    # Auto-route to HTTPS for Resend hosts (covers both prod hobby-Railway and
    # any other deployment behind an SMTP-blocking egress).
    if "resend.com" in (SMTP_HOST or "").lower():
        subject, body_html = _build_email_subject_and_body(report, is_empty, empty_message)
        return await _send_via_resend_http_api(
            from_header=from_header,
            recipients=recipients,
            subject=subject,
            body_html=body_html,
            csv_bytes=csv_bytes,
            idempotency_key=idempotency_key,
        )

    subject, body_html = _build_email_subject_and_body(report, is_empty, empty_message)

    def _send():
        msg = MIMEMultipart("mixed")
        msg["Subject"] = subject
        msg["From"] = from_header
        msg["To"] = ", ".join(recipients)
        # Resend-specific: dedup identical retries
        if idempotency_key:
            msg["Resend-Idempotency-Key"] = str(idempotency_key)
        msg.attach(MIMEText(body_html, "html"))

        if csv_bytes:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(csv_bytes)
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f'attachment; filename="backup_details.csv"')
            msg.attach(part)

        if SMTP_PORT in (465, 2465):
            # Implicit SSL/TLS — connect encrypted from the start
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=15) as server:
                server.ehlo()
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
                server.sendmail(envelope_from, recipients, msg.as_string())
        else:
            # STARTTLS — connect plaintext then upgrade
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as server:
                server.ehlo()
                server.starttls()
                server.ehlo()
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
                server.sendmail(envelope_from, recipients, msg.as_string())

    try:
        await asyncio.to_thread(_send)
        print(f"[REPORT] Email sent to {len(recipients)} recipient(s) via {SMTP_HOST}:{SMTP_PORT} from={envelope_from}")
        return True
    except smtplib.SMTPResponseException as e:
        # Resend returns structured codes — surface them so "domain not verified" etc. is visible.
        print(f"[REPORT] SMTP refused (code={e.smtp_code}, msg={e.smtp_error!r}) from={envelope_from} to={recipients}")
        return False
    except smtplib.SMTPException as e:
        print(f"[REPORT] SMTP error: {type(e).__name__}: {e}")
        return False
    except Exception as e:
        print(f"[REPORT] Unexpected email failure: {type(e).__name__}: {e}")
        return False


# ==================== API Endpoints ====================

@app.get("/health")
async def health():
    return {"status": "ok", "service": "report-service"}


@app.get("/api/v1/reports/config", response_model=ReportConfigResponse)
async def get_report_config(current_user: dict = Depends(get_current_user_from_token)):
    """Get the current report configuration"""
    org_id = _require_org_id(current_user)
    async with async_session_factory() as session:
        result = await session.execute(
            select(ReportConfig).where(ReportConfig.org_id == org_id)
        )
        config = result.scalar_one_or_none()

        if not config:
            config = ReportConfig(org_id=org_id)
            session.add(config)
            await session.commit()
            await session.refresh(config)

        return ReportConfigResponse(
            id=str(config.id),
            org_id=str(config.org_id) if config.org_id else "",
            enabled=config.enabled,
            schedule_type=config.schedule_type,
            send_empty_report=config.send_empty_report,
            empty_message=config.empty_message,
            send_detailed_report=config.send_detailed_report,
            email_recipients=config.email_recipients or [],
            slack_webhooks=[WebhookConfig(**w) for w in (config.slack_webhooks or [])],
            teams_webhooks=[WebhookConfig(**w) for w in (config.teams_webhooks or [])],
            created_at=config.created_at.isoformat() if config.created_at else "",
            updated_at=config.updated_at.isoformat() if config.updated_at else "",
        )


@app.post("/api/v1/reports/config", response_model=ReportConfigResponse)
async def create_report_config(
    config_data: ReportConfigCreate,
    current_user: dict = Depends(get_current_user_from_token),
):
    """Create a new report configuration"""
    org_id = _require_org_id(current_user)
    async with async_session_factory() as session:
        result = await session.execute(
            select(ReportConfig).where(ReportConfig.org_id == org_id)
        )
        existing = result.scalar_one_or_none()

        if existing:
            raise HTTPException(status_code=400, detail="Configuration already exists. Use PUT to update.")

        config = ReportConfig(
            org_id=org_id,
            enabled=config_data.enabled,
            schedule_type=config_data.schedule_type,
            send_empty_report=config_data.send_empty_report,
            empty_message=config_data.empty_message,
            send_detailed_report=config_data.send_detailed_report,
            email_recipients=config_data.email_recipients,
            slack_webhooks=[w.model_dump() for w in config_data.slack_webhooks],
            teams_webhooks=[w.model_dump() for w in config_data.teams_webhooks],
        )
        session.add(config)
        await session.commit()
        await session.refresh(config)

        return ReportConfigResponse(
            id=str(config.id),
            org_id=str(config.org_id) if config.org_id else "",
            enabled=config.enabled,
            schedule_type=config.schedule_type,
            send_empty_report=config.send_empty_report,
            empty_message=config.empty_message,
            send_detailed_report=config.send_detailed_report,
            email_recipients=config.email_recipients or [],
            slack_webhooks=[WebhookConfig(**w) for w in (config.slack_webhooks or [])],
            teams_webhooks=[WebhookConfig(**w) for w in (config.teams_webhooks or [])],
            created_at=config.created_at.isoformat() if config.created_at else "",
            updated_at=config.updated_at.isoformat() if config.updated_at else "",
        )


@app.put("/api/v1/reports/config", response_model=ReportConfigResponse)
async def update_report_config(
    config_data: ReportConfigUpdate,
    current_user: dict = Depends(get_current_user_from_token),
):
    """Update the report configuration"""
    org_id = _require_org_id(current_user)
    async with async_session_factory() as session:
        result = await session.execute(
            select(ReportConfig).where(ReportConfig.org_id == org_id)
        )
        config = result.scalar_one_or_none()

        if not config:
            raise HTTPException(status_code=404, detail="Configuration not found")

        if config_data.enabled is not None:
            config.enabled = config_data.enabled
        if config_data.schedule_type is not None:
            config.schedule_type = config_data.schedule_type
        if config_data.send_empty_report is not None:
            config.send_empty_report = config_data.send_empty_report
        if config_data.empty_message is not None:
            config.empty_message = config_data.empty_message
        if config_data.send_detailed_report is not None:
            config.send_detailed_report = config_data.send_detailed_report
        if config_data.email_recipients is not None:
            config.email_recipients = config_data.email_recipients
        if config_data.slack_webhooks is not None:
            config.slack_webhooks = [w.model_dump() for w in config_data.slack_webhooks]
        if config_data.teams_webhooks is not None:
            config.teams_webhooks = [w.model_dump() for w in config_data.teams_webhooks]

        await session.commit()
        await session.refresh(config)

        return ReportConfigResponse(
            id=str(config.id),
            org_id=str(config.org_id) if config.org_id else "",
            enabled=config.enabled,
            schedule_type=config.schedule_type,
            send_empty_report=config.send_empty_report,
            empty_message=config.empty_message,
            send_detailed_report=config.send_detailed_report,
            email_recipients=config.email_recipients or [],
            slack_webhooks=[WebhookConfig(**w) for w in (config.slack_webhooks or [])],
            teams_webhooks=[WebhookConfig(**w) for w in (config.teams_webhooks or [])],
            created_at=config.created_at.isoformat() if config.created_at else "",
            updated_at=config.updated_at.isoformat() if config.updated_at else "",
        )


@app.get("/api/v1/reports/history", response_model=List[ReportHistoryResponse])
async def get_report_history(
    limit: int = Query(default=50, le=200),
    offset: int = 0,
    report_type: Optional[str] = None,
    current_user: dict = Depends(get_current_user_from_token),
):
    """Get report sending history"""
    org_id = _require_org_id(current_user)
    async with async_session_factory() as session:
        query = (
            select(ReportHistory)
            .where(ReportHistory.org_id == org_id)
            .order_by(desc(ReportHistory.generated_at))
        )

        if report_type:
            query = query.where(ReportHistory.report_type == report_type.upper())

        query = query.limit(limit).offset(offset)
        result = await session.execute(query)
        reports = result.scalars().all()

        return [
            ReportHistoryResponse(
                id=str(r.id),
                org_id=str(r.org_id) if r.org_id else None,
                report_config_id=str(r.report_config_id) if r.report_config_id else None,
                report_type=r.report_type,
                period_start=r.period_start.isoformat() if r.period_start else None,
                period_end=r.period_end.isoformat() if r.period_end else None,
                generated_at=r.generated_at.isoformat() if r.generated_at else "",
                total_backups=r.total_backups,
                successful_backups=r.successful_backups,
                failed_backups=r.failed_backups,
                success_rate=r.success_rate,
                coverage_rate=r.coverage_rate,
                is_empty=r.is_empty,
                delivery_status=r.delivery_status,
                error_message=r.error_message,
                created_at=r.created_at.isoformat() if r.created_at else "",
            )
            for r in reports
        ]


@app.get("/api/v1/reports/history/{report_id}", response_model=ReportHistoryResponse)
async def get_report_history_detail(
    report_id: str,
    current_user: dict = Depends(get_current_user_from_token),
):
    """Get detailed information about a specific report"""
    org_id = _require_org_id(current_user)
    async with async_session_factory() as session:
        result = await session.execute(
            select(ReportHistory).where(
                ReportHistory.id == report_id,
                ReportHistory.org_id == org_id,
            )
        )
        report = result.scalar_one_or_none()

        if not report:
            raise HTTPException(status_code=404, detail="Report not found")

        return ReportHistoryResponse(
            id=str(report.id),
            org_id=str(report.org_id) if report.org_id else None,
            report_config_id=str(report.report_config_id) if report.report_config_id else None,
            report_type=report.report_type,
            period_start=report.period_start.isoformat() if report.period_start else None,
            period_end=report.period_end.isoformat() if report.period_end else None,
            generated_at=report.generated_at.isoformat() if report.generated_at else "",
            total_backups=report.total_backups,
            successful_backups=report.successful_backups,
            failed_backups=report.failed_backups,
            success_rate=report.success_rate,
            coverage_rate=report.coverage_rate,
            is_empty=report.is_empty,
            delivery_status=report.delivery_status,
            error_message=report.error_message,
            created_at=report.created_at.isoformat() if report.created_at else "",
        )


# ==================== Report Generation Endpoints (called by scheduler) ====================

class GenerateReportRequest(BaseModel):
    report_type: str  # DAILY, WEEKLY, MONTHLY


class GenerateReportResponse(BaseModel):
    success: bool
    report_id: Optional[str]
    message: str


@app.post("/api/v1/reports/send-test")
async def send_test_email(to: Optional[str] = Query(None)):
    """Send a small test email using the current SMTP config. Defaults to the
    recipient list on the first report config; accept a `?to=addr` override
    for one-off checks. Returns the envelope/header used and the actual send
    outcome so operators can debug config without generating a full report."""
    # Resolve target recipients
    if to:
        recipients = [to]
    else:
        async with async_session_factory() as session:
            result = await session.execute(select(ReportConfig).limit(1))
            cfg = result.scalar_one_or_none()
            if not cfg or not cfg.email_recipients:
                raise HTTPException(
                    status_code=400,
                    detail="No report config or no recipients set. Pass ?to=addr explicitly.",
                )
            recipients = cfg.email_recipients

    if not SMTP_ENABLED:
        return {
            "sent": False,
            "reason": "NOTIFICATION_EMAIL_ENABLED is false — flip it to 'true' in .env then restart report-service.",
            "smtp_host": SMTP_HOST, "smtp_port": SMTP_PORT,
        }

    from_header, envelope_from = _parse_from_header()
    test_report = {
        "period": "Test email",
        "summary": {
            "successful_backups": 1, "failed_backups": 0, "successful_discoveries": 1,
            "total_resources": 1, "protected_resources": 1,
            "coverage_rate": "100.0%", "storage_gb": 0.01,
        },
        "resource_breakdown": {"Test": {"protected": 1, "total": 1}},
        "tenant_domain": "test",
    }
    ok = await send_email_notification(
        test_report, recipients, is_empty=False,
        empty_message="", csv_bytes=None,
        idempotency_key=f"test-{uuid.uuid4()}",
    )
    return {
        "sent": ok,
        "smtp_host": SMTP_HOST, "smtp_port": SMTP_PORT,
        "from_header": from_header, "envelope_from": envelope_from,
        "recipients": recipients,
        "hint": None if ok else "Check report-service logs for the SMTP response code (e.g. 550 = domain not verified on Resend).",
    }


@app.post("/api/v1/reports/generate", response_model=GenerateReportResponse)
async def generate_report(request: GenerateReportRequest):
    """Generate and send a report (called by scheduler on schedule)"""
    print(f"[REPORT] Generating {request.report_type} report...")

    async with async_session_factory() as session:
        config_result = await session.execute(select(ReportConfig).limit(1))
        config = config_result.scalar_one_or_none()
        
        if not config or not config.enabled:
            print("[REPORT] Reports are not enabled. Skipping.")
            return GenerateReportResponse(success=False, report_id=None, message="Reports not enabled")

        now = datetime.utcnow()
        
        if request.report_type == "DAILY":
            period_start = now - timedelta(days=1)
            period_end = now
        elif request.report_type == "WEEKLY":
            period_start = now - timedelta(days=7)
            period_end = now
        elif request.report_type == "MONTHLY":
            period_start = now - timedelta(days=30)
            period_end = now
        else:
            return GenerateReportResponse(success=False, report_id=None, message=f"Unknown report type: {request.report_type}")

        total_backups_result = await session.execute(
            select(func.count(Job.id)).where(
                and_(
                    Job.type == JobType.BACKUP,
                    Job.created_at >= period_start,
                    Job.created_at <= period_end,
                )
            )
        )
        total_backups = total_backups_result.scalar() or 0

        successful_backups_result = await session.execute(
            select(func.count(Job.id)).where(
                and_(
                    Job.type == JobType.BACKUP,
                    Job.status == JobStatus.COMPLETED,
                    Job.created_at >= period_start,
                    Job.created_at <= period_end,
                )
            )
        )
        successful_backups = successful_backups_result.scalar() or 0

        failed_backups = total_backups - successful_backups
        is_empty = total_backups == 0
        
        if is_empty and not config.send_empty_report:
            await save_report_history(
                session, config, request.report_type, period_start, period_end,
                total_backups, successful_backups, failed_backups,
                is_empty=True, report_data={"message": config.empty_message}
            )
            return GenerateReportResponse(success=True, report_id=None, message="Empty report skipped")

        # Per-resource-type breakdown
        resource_type_groups = {
            "Users": [ResourceType.MAILBOX, ResourceType.SHARED_MAILBOX],
            "SharePoint Sites": [ResourceType.SHAREPOINT_SITE],
            "Entra ID": [ResourceType.ENTRA_USER, ResourceType.ENTRA_GROUP, ResourceType.ENTRA_APP, ResourceType.ENTRA_SERVICE_PRINCIPAL, ResourceType.ENTRA_DEVICE],
            "Microsoft 365 Groups & Teams": [ResourceType.TEAMS_CHANNEL, ResourceType.TEAMS_CHAT],
        }

        resource_breakdown = {}
        total_resources = 0
        protected_resources = 0
        for label, types in resource_type_groups.items():
            total_q = await session.execute(
                select(func.count(Resource.id)).where(
                    and_(Resource.status == ResourceStatus.ACTIVE, Resource.type.in_(types))
                )
            )
            protected_q = await session.execute(
                select(func.count(Resource.id)).where(
                    and_(
                        Resource.status == ResourceStatus.ACTIVE,
                        Resource.type.in_(types),
                        Resource.last_backup_at >= period_start,
                        Resource.last_backup_status == "COMPLETED",
                    )
                )
            )
            t = total_q.scalar() or 0
            p = protected_q.scalar() or 0
            resource_breakdown[label] = {"protected": p, "total": t}
            total_resources += t
            protected_resources += p

        coverage_pct = (protected_resources / total_resources * 100) if total_resources > 0 else 0

        # Total backup storage. PostgreSQL returns SUM(BigInteger) as NUMERIC
        # (Decimal) to avoid overflow — cast to int here so downstream math +
        # JSON serialization into report_data don't hit
        # "Object of type Decimal is not JSON serializable".
        # Dedup Tier-1 ONEDRIVE/MAILBOX + Tier-2 USER_ONEDRIVE/USER_MAIL — both
        # walk the same content. See shared.storage_rollup.
        storage_result = await session.execute(
            select(func.sum(Resource.storage_bytes))
            .where(exclude_tier2_storage_dupes_clause())
        )
        total_storage_bytes = int(storage_result.scalar() or 0)
        storage_gb = round(total_storage_bytes / (1024 ** 3), 2)

        # Discovery job count
        discovery_result = await session.execute(
            select(func.count(Job.id)).where(
                and_(
                    Job.type == JobType.DISCOVERY,
                    Job.status == JobStatus.COMPLETED,
                    Job.created_at >= period_start,
                    Job.created_at <= period_end,
                )
            )
        )
        successful_discoveries = discovery_result.scalar() or 0

        # Tenant domain from resource email
        domain_result = await session.execute(
            select(Resource.email).where(Resource.email.isnot(None), Resource.email.contains("@")).limit(1)
        )
        email = domain_result.scalar() or ""
        tenant_domain = email.split("@")[1] if "@" in email else ""

        period_label = f"{period_start.strftime('%b %-d')} - {period_end.strftime('%b %-d, %Y')}"

        report = {
            "report_type": f"{request.report_type}_BACKUP_REPORT",
            "generated_at": now.isoformat(),
            "period": period_label,
            "tenant_domain": tenant_domain,
            "summary": {
                "total_backups": total_backups,
                "successful_backups": successful_backups,
                "failed_backups": failed_backups,
                "successful_discoveries": successful_discoveries,
                "total_resources": total_resources,
                "protected_resources": protected_resources,
                "coverage_rate": f"{coverage_pct:.1f}%",
                "storage_gb": storage_gb,
            },
            "resource_breakdown": resource_breakdown,
        }

        # Pre-mint the history row id so we can use it as the SMTP idempotency
        # key. Sends coming from retries of the same generate request then
        # carry the same key and Resend dedups them at the broker.
        history_id = uuid.uuid4()

        if not is_empty and config.send_detailed_report:
            detail_result = await session.execute(
                select(Resource.display_name, Job.completed_at, Snapshot.bytes_total)
                .join(Resource, Job.resource_id == Resource.id)
                .outerjoin(Snapshot, Job.snapshot_id == Snapshot.id)
                .where(
                    and_(
                        Job.type == JobType.BACKUP,
                        Job.status == JobStatus.COMPLETED,
                        Job.created_at >= period_start,
                        Job.created_at <= period_end,
                    )
                )
                .order_by(Job.completed_at)
            )
            detail_rows = detail_result.all()
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(["Resource Name", "Backup Time", "Size (bytes)"])
            for row in detail_rows:
                writer.writerow([row[0], row[1].isoformat() if row[1] else "", row[2] or 0])
            csv_bytes = buf.getvalue().encode("utf-8")
            delivery_status = await send_report_to_channels(report, config, is_empty, csv_bytes, idempotency_key=str(history_id))
        else:
            delivery_status = await send_report_to_channels(report, config, is_empty, idempotency_key=str(history_id))

        history_record = ReportHistory(
            id=history_id,
            org_id=config.org_id,
            report_config_id=config.id,
            report_type=request.report_type,
            period_start=period_start,
            period_end=period_end,
            total_backups=total_backups,
            successful_backups=successful_backups,
            failed_backups=failed_backups,
            success_rate=f"{(successful_backups / total_backups * 100) if total_backups > 0 else 0:.1f}%",
            coverage_rate=f"{coverage_pct:.1f}%",
            is_empty=is_empty,
            report_data=report,
            delivery_status=delivery_status,
        )
        session.add(history_record)
        await session.commit()

        print(f"[REPORT] {request.report_type} report generated: {history_record.id}")
        return GenerateReportResponse(success=True, report_id=str(history_record.id), message="Report generated successfully")


@app.on_event("startup")
async def startup():
    from shared import core_metrics
    core_metrics.init()
    await init_db()
    print("[REPORT] Report service started")
