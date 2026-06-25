"""Regression: /contacts/delta must NOT be called with $top.

Verified LIVE against Graph (2026-06-25, app-only): GET
/users/{id}/contacts/delta?$top=50 returns HTTP 400 ErrorInvalidUrlQuery
("parameters are not supported with change tracking over the 'Contacts'
resource"), while bare /contacts/delta and /contacts/delta?$select=... both
return 200 with the contacts + an @odata.deltaLink. The drain passed
{"$top": ..., "$select": ...} on the initial (non-resume) delta call, so every
contacts backup 400'd and the except-handler swallowed it -> 0 contacts
captured for every user, on every run, with the snapshot still COMPLETED.

Pins the contract: the initial /contacts/delta params omit $top (page size
falls back to the Graph default + @odata.nextLink paging).
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

_MAIN_PATH = (
    pathlib.Path(__file__).resolve().parents[2]
    / "workers" / "backup-worker" / "main.py"
)
_spec = importlib.util.spec_from_file_location("bw_main_contacts_delta", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_main_contacts_delta"] = _bw
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )


def test_contacts_delta_initial_params_omit_top():
    p = _bw._contacts_delta_initial_params("displayName,emailAddresses,parentFolderId")
    assert "$top" not in p, "Graph 400s on $top for /contacts/delta change-tracking"
    assert p.get("$select") == "displayName,emailAddresses,parentFolderId"


def test_contacts_delta_initial_params_select_only():
    # Only $select (and nothing that Graph rejects on the delta endpoint).
    p = _bw._contacts_delta_initial_params("displayName")
    assert set(p.keys()) <= {"$select"}
