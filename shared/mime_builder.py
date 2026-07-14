"""Build RFC 5322 MIME multipart EML bytes from a Graph API message dict.

Used by the mail-export pipeline (restore-worker) to convert each backed-up
message blob into a standards-compliant email that Outlook, Thunderbird, Apple
Mail and eDiscovery tooling can parse without loss.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass
from email.message import EmailMessage
from email.policy import SMTP
from email.utils import format_datetime, formataddr, parsedate_to_datetime
from typing import AsyncIterator, Iterable, List, Optional


@dataclass
class AttachmentRef:
    """Streaming attachment reference: name, content-type, and a bytes source.

    `data_stream` is an async generator that yields raw chunks; use when the
    attachment lives in Azure blob and we want to avoid loading it whole.
    `data_bytes` is the non-streaming alternative for tests and small blobs.
    """
    name: str
    content_type: str
    data_bytes: Optional[bytes] = None
    data_stream: Optional[AsyncIterator[bytes]] = None


def _format_addr(entry: dict) -> str:
    """Graph's emailAddress shape → RFC 5322 address with display-name encoded."""
    addr = entry.get("emailAddress", {}) or {}
    return formataddr((addr.get("name") or "", addr.get("address") or ""))


def _format_addr_list(entries) -> str:
    return ", ".join(_format_addr(e) for e in (entries or []))


def _parse_date(value: Optional[str]):
    if not value:
        return None
    try:
        return parsedate_to_datetime(value)
    except Exception:
        from datetime import datetime
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None


def build_eml(graph_msg: dict, attachments: Iterable[AttachmentRef]) -> bytes:
    """Build a fully RFC 5322 compliant EML. Attachments are base64-encoded inline.

    For streaming attachments use build_eml_streaming instead — this function
    materializes attachment bytes in memory and is intended for tests or small
    messages only.
    """
    msg = EmailMessage(policy=SMTP)

    msg["Subject"] = graph_msg.get("subject") or ""
    sender = graph_msg.get("from") or graph_msg.get("sender") or {}
    msg["From"] = _format_addr(sender)
    msg["To"] = _format_addr_list(graph_msg.get("toRecipients"))
    cc = _format_addr_list(graph_msg.get("ccRecipients"))
    if cc:
        msg["Cc"] = cc
    bcc = _format_addr_list(graph_msg.get("bccRecipients"))
    if bcc:
        msg["Bcc"] = bcc

    sent = _parse_date(graph_msg.get("sentDateTime") or graph_msg.get("receivedDateTime"))
    if sent:
        msg["Date"] = format_datetime(sent)

    imi = graph_msg.get("internetMessageId")
    if imi:
        msg["Message-ID"] = imi

    for h in graph_msg.get("internetMessageHeaders") or []:
        name, value = h.get("name"), h.get("value")
        if name and value and name.lower() in {"in-reply-to", "references", "return-path", "received"}:
            msg[name] = value

    body = graph_msg.get("body") or {}
    body_content = body.get("content") or ""
    body_type = (body.get("contentType") or "text").lower()
    if body_type == "html":
        import re
        plain = re.sub(r"<[^>]+>", "", body_content)
        msg.set_content(plain, subtype="plain", charset="utf-8")
        msg.add_alternative(body_content, subtype="html", charset="utf-8")
    else:
        msg.set_content(body_content, subtype="plain", charset="utf-8")

    for att in attachments:
        data = att.data_bytes
        if data is None:
            raise ValueError(
                f"AttachmentRef {att.name} has no data_bytes; "
                "use build_eml_streaming for streaming attachments."
            )
        maintype, _, subtype = (att.content_type or "application/octet-stream").partition("/")
        msg.add_attachment(
            data,
            maintype=maintype or "application",
            subtype=subtype or "octet-stream",
            filename=att.name,
        )

    return bytes(msg)


def _inline_attachment_refs(attachments) -> List[AttachmentRef]:
    """AttachmentRefs from a Graph message's inline ``fileAttachment`` dicts that
    already carry ``contentBytes`` — used when re-serializing a nested email that
    Graph expanded inline. Non-file / bytes-less entries are skipped."""
    refs: List[AttachmentRef] = []
    for a in attachments or []:
        cb = a.get("contentBytes") if isinstance(a, dict) else None
        if not cb:
            continue
        odata = (a.get("@odata.type") or "").lower()
        if odata and "fileattachment" not in odata:
            continue  # itemAttachment/referenceAttachment can't ride as bytes here
        try:
            refs.append(AttachmentRef(
                name=a.get("name") or "attachment",
                content_type=a.get("contentType") or "application/octet-stream",
                data_bytes=base64.b64decode(cb),
            ))
        except Exception:
            continue
    return refs


def build_nested_message_attachment(nested_msg: dict) -> AttachmentRef:
    """Convert an ``itemAttachment``'s nested Graph message (mail-in-mail) into a
    ``message/rfc822`` :class:`AttachmentRef` carrying the nested email's full
    ``.eml`` bytes. Attaching this through the ordinary file-attachment path makes
    mail-in-mail embed natively in EML, MBOX **and** PST — for every mailbox type
    (primary / shared / archive), which all share this export layer. The nested
    email's own inline file attachments ride inside its ``.eml``.
    """
    nested = nested_msg or {}
    eml = build_eml(nested, _inline_attachment_refs(nested.get("attachments")))
    subject = (nested.get("subject") or "").strip() or "attached-message"
    safe = "".join(c if (c.isalnum() or c in " -._") else "_" for c in subject).strip()
    safe = safe[:120].strip() or "attached-message"
    return AttachmentRef(name=f"{safe}.eml", content_type="message/rfc822", data_bytes=eml)


async def build_eml_streaming(
    graph_msg: dict,
    attachments: Iterable[AttachmentRef],
) -> AsyncIterator[bytes]:
    """Stream EML bytes chunk-by-chunk. Attachments with a `data_stream` are
    base64-encoded on the fly — no attachment materializes fully in RAM."""

    headers_msg = EmailMessage(policy=SMTP)
    headers_msg["Subject"] = graph_msg.get("subject") or ""
    sender = graph_msg.get("from") or graph_msg.get("sender") or {}
    headers_msg["From"] = _format_addr(sender)
    headers_msg["To"] = _format_addr_list(graph_msg.get("toRecipients"))
    cc = _format_addr_list(graph_msg.get("ccRecipients"))
    if cc:
        headers_msg["Cc"] = cc
    sent = _parse_date(graph_msg.get("sentDateTime") or graph_msg.get("receivedDateTime"))
    if sent:
        headers_msg["Date"] = format_datetime(sent)
    imi = graph_msg.get("internetMessageId")
    if imi:
        headers_msg["Message-ID"] = imi

    import uuid as _uuid
    boundary = f"----=_Part_{_uuid.uuid4().hex}"
    headers_msg["MIME-Version"] = "1.0"
    headers_msg["Content-Type"] = f'multipart/mixed; boundary="{boundary}"'

    header_bytes = headers_msg.as_bytes(policy=SMTP).rstrip(b"\r\n") + b"\r\n\r\n"
    yield header_bytes

    body = graph_msg.get("body") or {}
    body_content = (body.get("content") or "").encode("utf-8")
    body_type = (body.get("contentType") or "text").lower()
    ctype = "text/html" if body_type == "html" else "text/plain"

    yield (
        f"--{boundary}\r\n"
        f"Content-Type: {ctype}; charset=utf-8\r\n"
        f"Content-Transfer-Encoding: base64\r\n\r\n"
    ).encode("ascii")
    yield base64.b64encode(body_content) + b"\r\n"

    for att in attachments:
        maintype_sub = att.content_type or "application/octet-stream"
        yield (
            f"--{boundary}\r\n"
            f"Content-Type: {maintype_sub}\r\n"
            f"Content-Transfer-Encoding: base64\r\n"
            f'Content-Disposition: attachment; filename="{att.name}"\r\n\r\n'
        ).encode("ascii")

        if att.data_bytes is not None:
            yield base64.b64encode(att.data_bytes) + b"\r\n"
        elif att.data_stream is not None:
            buf = bytearray()
            async for chunk in att.data_stream:
                buf.extend(chunk)
                cut = (len(buf) // 57) * 57
                if cut:
                    yield base64.b64encode(bytes(buf[:cut])) + b"\r\n"
                    del buf[:cut]
            if buf:
                yield base64.b64encode(bytes(buf)) + b"\r\n"
        else:
            raise ValueError(f"AttachmentRef {att.name} has no data")

    yield f"--{boundary}--\r\n".encode("ascii")
