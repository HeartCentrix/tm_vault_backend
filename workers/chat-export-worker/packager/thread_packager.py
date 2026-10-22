"""Stream ZIP — v1 one ZIP per job. Layouts: single_thread | per_message."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import AsyncIterator, Protocol
import io, re, zipfile

from workers.chat_export_worker.render.normalizer import RenderMessage, AttachmentRef
from workers.chat_export_worker.render.html_renderer import render_thread_html, render_message_html
from workers.chat_export_worker.render.json_renderer import render_thread_json, render_message_json
from workers.chat_export_worker.render.activity_log import (
    build_activity_text, build_activity_json, build_manifest,
)

# PDF renderer is imported lazily because WeasyPrint needs system deps.
_STYLES = Path(__file__).resolve().parents[1] / "templates" / "styles.css"


def _safe_name(name: str, fallback: str = "item") -> str:
    s = re.sub(r'[\\/:*?"<>|]+', "_", (name or fallback).strip())
    s = re.sub(r"\s+", " ", s)
    return (s[:120] or fallback)


def _unique_in_dir(name: str, used: set[str]) -> str:
    """Uniquify a sanitised filename within one ZIP directory.

    Two attachments named 'contract.pdf' in the same thread used to become
    two ZIP entries at the identical path — extractors keep only one, and
    both messages' HTML chips then link to whichever file survived, silently
    misrepresenting which document was attached to which message. Comparison
    is case-insensitive because most extraction targets (Windows/macOS) are.
    """
    candidate = name
    n = 2
    while candidate.lower() in used:
        stem, dot, ext = name.rpartition(".")
        if dot and stem:
            candidate = f"{stem} ({n}).{ext}"
        else:
            candidate = f"{name} ({n})"
        n += 1
    used.add(candidate.lower())
    return candidate


class AttachmentSource(Protocol):
    async def open(self, blob_path: str) -> tuple[AsyncIterator[bytes], str, int]: ...


@dataclass
class ThreadPackager:
    layout: str
    thread_name: str
    thread_path: str
    format: str
    attachment_source: AttachmentSource

    async def write(self, messages: list[RenderMessage], *, attachment_map: dict,
                    hosted_map: dict, out: io.BufferedIOBase, context: dict | None = None) -> dict:
        ctx = context or {}
        zf = zipfile.ZipFile(out, mode="w", compression=zipfile.ZIP_DEFLATED, allowZip64=True)
        per_file: list[dict] = []
        total_bytes = 0
        tsafe = _safe_name(self.thread_name)

        def _add(path: str, data: bytes):
            """Write small in-memory artifacts (HTML, JSON, CSS, manifest).

            For large attachments use _add_stream instead so we don't
            buffer the whole blob in RAM.
            """
            nonlocal total_bytes
            zf.writestr(path, data)
            per_file.append({"path": path, "sha256": sha256(data).hexdigest(), "bytes": len(data)})
            total_bytes += len(data)

        async def _add_stream(path: str, src_path: str) -> tuple[int, bool]:
            """Stream bytes from blob storage straight into a ZIP entry.

            Computes sha256 + total bytes on the fly so per_file stays
            consistent with _add. Returns (bytes_written, missing).
            Memory footprint per call is bounded to one chunk
            (~4 MiB by default) plus zipfile's per-entry buffer.
            """
            nonlocal total_bytes
            h = sha256()
            total = 0
            try:
                ait, _ct, _sz = await self.attachment_source.open(src_path)
                with zf.open(path, mode="w", force_zip64=True) as entry:
                    async for chunk in ait:
                        if not chunk:
                            continue
                        entry.write(chunk)
                        h.update(chunk)
                        total += len(chunk)
                per_file.append({
                    "path": path, "sha256": h.hexdigest(), "bytes": total,
                })
                total_bytes += total
                return total, False
            except Exception as e:
                placeholder = (
                    f"[ATTACHMENT_MISSING]\nblob_path: {src_path}\nerror: {e!r}\n"
                    "This attachment was not available at export time.\n"
                ).encode()
                # Caller picks the .MISSING.txt path; we just record
                # the placeholder bytes against THIS entry.
                with zf.open(path, mode="w") as entry:
                    entry.write(placeholder)
                per_file.append({
                    "path": path,
                    "sha256": sha256(placeholder).hexdigest(),
                    "bytes": len(placeholder),
                })
                total_bytes += len(placeholder)
                return len(placeholder), True

        inline_count = 0; attachment_count = 0
        used_att_names: dict[str, set] = {}  # att_dir -> lowercased names taken
        for m in messages:
            att_dir = (f"{tsafe}-attachments" if self.layout == "single_thread"
                       else f"per-message/{tsafe}/{_safe_name(m.external_id)}/attachments")
            new_atts = []
            for att in attachment_map.get(m.external_id, []):
                # Pick the target name BEFORE streaming. We need to
                # decide .MISSING.txt vs the real name up-front
                # because zipfile.open() commits the entry path the
                # moment it's opened. If the stream fails mid-flight
                # we keep the real name and just emit a short
                # placeholder under it — the operator can tell from
                # the file size (and from manifest.json) that it's
                # not the real attachment.
                clean_name = _unique_in_dir(
                    _safe_name(att["name"]),
                    used_att_names.setdefault(att_dir, set()),
                )
                target = f"{att_dir}/{clean_name}"
                written, missing = await _add_stream(target, att["blob_path"])
                if missing:
                    # Append a .MISSING marker file next to it so
                    # operators can spot the gap at a glance — the
                    # zero-byte original is kept under the real name
                    # for path consistency with the HTML <a href>.
                    marker_text = (
                        f"Attachment file {clean_name!r} could not be retrieved "
                        f"from storage at export time.\nblob_path: {att['blob_path']}\n"
                    ).encode()
                    _add(f"{target}.MISSING.txt", marker_text)
                if self.layout == "single_thread":
                    href = f"./{target}"
                else:
                    href = f"./attachments/{clean_name}"
                new_atts.append(AttachmentRef(
                    name=clean_name, local_path=href,
                    content_type="text/plain" if missing else att.get("content_type"),
                    size=written,
                ))
                attachment_count += 1
            m.attachments = new_atts
            for hc in hosted_map.get(m.external_id, []):
                ext = hc.get("ext", ".bin")
                target = f"{att_dir}/inline/{hc['hc_id']}{ext}"
                written, missing = await _add_stream(target, hc["blob_path"])
                if missing:
                    _add(
                        f"{att_dir}/inline/{hc['hc_id']}.MISSING.txt",
                        (
                            f"Inline image {hc['hc_id']} could not be retrieved.\n"
                            f"blob_path: {hc['blob_path']}\n"
                        ).encode(),
                    )
                inline_count += 1

        # styles.css is only used by HTML exports — the rendered HTML
        # references it via <link rel="stylesheet">. PDF embeds the CSS
        # at render time inside the PDF stream (see pdf_renderer.py),
        # and JSON is a pure data file. Including styles.css in PDF/JSON
        # zips was dead weight + confusing ("why is there a CSS file in
        # my PDF export?"). Add it only when it will actually be used.
        if self.format == "HTML":
            _add("styles.css", _STYLES.read_bytes())

        if self.layout == "single_thread":
            if self.format == "HTML":
                _add(f"{tsafe}.html", render_thread_html(messages, self.thread_name).encode())
            elif self.format == "JSON":
                _add(f"{tsafe}.json", render_thread_json(messages, thread_path=self.thread_path).encode())
            elif self.format == "PDF":
                from workers.chat_export_worker.render.pdf_renderer import render_thread_pdf
                _add(f"{tsafe}.pdf", render_thread_pdf(messages, self.thread_name))
        else:
            for m in messages:
                folder = f"per-message/{tsafe}/{_safe_name(m.external_id)}"
                base = _safe_name(m.external_id)
                if self.format == "HTML":
                    _add(f"{folder}/{base}.html", render_message_html(m, self.thread_name).encode())
                elif self.format == "JSON":
                    _add(f"{folder}/{base}.json",
                         render_message_json(m, thread_path=self.thread_path).encode())
                elif self.format == "PDF":
                    from workers.chat_export_worker.render.pdf_renderer import render_message_pdf
                    _add(f"{folder}/{base}.pdf", render_message_pdf(m, self.thread_name))

        # Compute the Merkle-style zip sha256 over every content file
        # FIRST, so activity.log / activity.json can carry the real
        # value instead of a "(pending)" placeholder. The manifest is
        # then written including itself in `files` but its own row
        # contains the final per-file sha256 — only the manifest's
        # zip_sha is excluded from its own hash (chicken-and-egg).
        gen_at = datetime.now(timezone.utc)
        zip_sha = sha256(
            b"".join(f["sha256"].encode() for f in per_file)
        ).hexdigest()
        kwargs = dict(
            messages=messages,
            user_email=ctx.get("user_email", ""),
            tenant_name=ctx.get("tenant_name", ""),
            resource_name=ctx.get("resource_name", self.thread_name),
            scope=ctx.get("scope", self.thread_path),
            snapshot_at=ctx.get("snapshot_at"),
            format=self.format,
            attachments=attachment_count, inline_images=inline_count,
            size_bytes=total_bytes, sha256=zip_sha, generated_at=gen_at,
        )
        _add("_meta/activity.log", build_activity_text(**kwargs).encode())
        _add("_meta/activity.json", build_activity_json(**kwargs).encode())
        _add("_meta/manifest.json", build_manifest(
            job_id=ctx.get("job_id", "unknown"),
            generated_at=gen_at, files=per_file, zip_sha256=zip_sha,
        ).encode())
        zf.close()

        return {"attachment_count": attachment_count, "inline_image_count": inline_count,
                "total_bytes": total_bytes, "sha256": zip_sha}
