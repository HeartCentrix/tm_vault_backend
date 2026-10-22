"""USER_MAIL per-folder delta bootstrap must not send ``$top`` — Graph treats
``$top`` on ``/mailFolders/{id}/messages/delta`` as a cap on the WHOLE delta
round, then hands back an ``@odata.deltaLink``. The worker persists that
deltaLink as the folder cursor, so every message past the cap is never
enumerated again: no PARTIAL, no log line, no retry.

Proven live 2026-09-04 (Jason: "Dallas - Love the Customer", Graph user
cc43d12d…), worker APP_1 identity:

    request shape                                    result
    /messages/delta?$top=999 (+Prefer maxpagesize)   512 + 487 = 999, then deltaLink
    /messages/delta  (Prefer: odata.maxpagesize=999) 3,095 over 7 pages, then deltaLink
    resume the SAVED deltaLink from the capped round  0 items

Stored per-folder rows for that mailbox's USER_MAIL child: /Sent Items 999
(Graph 3,095), /Deleted Items 889 (Graph 1,227). Fleet scan the same day:
every Silver USER_MAIL folder over 999 messages sits at exactly 999
(Portland Sent 10,354 → 999; Charlotte Sent 5,184 → 999; Piedmont Inbox
1,247 → 999). The ROOM_MAILBOX / SHARED_MAILBOX row path is a plain
``/users/{id}/messages?$top=999`` walk (page size, not a round cap) and is
complete — which is how the truncation was noticed.

The "rescan net" (``USER_MAIL_FULL_RESCAN_DAYS``) only force-drains the SAVED
token once a day, so it cannot heal a truncated baseline.

Fix: page size belongs in the ``Prefer: odata.maxpagesize`` header, never in
``$top``; and that header must ride every page request (nextLinks included).
Same family as tests/workers/test_contacts_delta_no_top.py — a query option
that is legal on the plain collection but changes meaning on ``/delta``.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import re
import sys

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_SRC = _MAIN_PATH.read_text()

_spec = importlib.util.spec_from_file_location("bw_mail_delta_no_top", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_mail_delta_no_top"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )

SELECT = "id,subject,receivedDateTime"


def test_mail_delta_initial_params_never_sends_top():
    params = _bw.mail_delta_initial_params(SELECT)
    assert "$top" not in params, (
        "$top on /messages/delta caps the whole delta round at that many "
        "messages and the saved deltaLink skips the rest forever"
    )
    assert params["$select"] == SELECT


def test_mail_delta_initial_params_is_only_select():
    assert _bw.mail_delta_initial_params(SELECT) == {"$select": SELECT}


def test_mail_delta_request_headers_ask_for_max_page_size():
    headers = _bw.mail_delta_request_headers()
    assert headers == {"Prefer": "odata.maxpagesize=999"}


def _drain_one_folder_src() -> str:
    start = _SRC.index("async def _drain_one_folder(")
    # The helper is nested; stop at the next sibling-level definition.
    end = _SRC.find("\n                    async def ", start + 10)
    return _SRC[start: end if end > 0 else start + 20000]


def test_drain_one_folder_bootstrap_uses_the_params_helper():
    body = _drain_one_folder_src()
    assert "_bootstrap_params = mail_delta_initial_params(" in body
    assert '"$top": "999", "$select": mail_select' not in body, (
        "literal $top=999 bootstrap params are back in _drain_one_folder"
    )


def test_drain_one_folder_passes_prefer_header_to_every_page():
    body = _drain_one_folder_src()
    calls = re.findall(r"graph_client\._iter_pages\((.*?)\)\s*:", body, flags=re.S)
    assert calls, "expected the folder drain to iterate pages via graph_client._iter_pages"
    for call in calls:
        compact = re.sub(r"\s+", "", call)
        assert "headers=mail_delta_request_headers()" in compact, (
            "the Prefer: odata.maxpagesize header must ride every delta page request; "
            f"call site: {call.strip()[:120]}"
        )
