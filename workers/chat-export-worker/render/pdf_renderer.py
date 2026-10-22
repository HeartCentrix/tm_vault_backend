"""PDF rendering via WeasyPrint. Uses the same HTML template; pulls styles.css
from the templates dir as base_url so relative assets resolve."""
from pathlib import Path
from datetime import datetime
from weasyprint import HTML, CSS, default_url_fetcher
from workers.chat_export_worker.render.html_renderer import render_thread_html, render_message_html

_TEMPLATES = Path(__file__).resolve().parents[1] / "templates"
_STYLES_PATH = _TEMPLATES / "styles.css"


def _no_remote_fetch(url, *a, **k):
    # Deny-by-default resource fetcher: only local file: and inline data:
    # URIs resolve. Everything else (http/https/etc.) is blocked so a
    # chat <img src="http://internal"> can never trigger an in-pod fetch
    # during PDF render (CVE-2025-68616 SSRF hardening, defence in depth).
    if url.startswith(("data:",)) or url.startswith("file:"):
        return default_url_fetcher(url, *a, **k)
    raise ValueError(f"remote resource fetch blocked in PDF render: {url[:80]}")


def render_thread_pdf(messages, thread_name: str, exported_at: datetime | None = None) -> bytes:
    html = render_thread_html(messages, thread_name, exported_at)
    return HTML(
        string=html, base_url=str(_TEMPLATES), url_fetcher=_no_remote_fetch,
    ).write_pdf(
        stylesheets=[CSS(filename=str(_STYLES_PATH))],
    )


def render_message_pdf(message, thread_name: str, exported_at: datetime | None = None) -> bytes:
    html = render_message_html(message, thread_name, exported_at)
    return HTML(
        string=html, base_url=str(_TEMPLATES), url_fetcher=_no_remote_fetch,
    ).write_pdf(
        stylesheets=[CSS(filename=str(_STYLES_PATH))],
    )
