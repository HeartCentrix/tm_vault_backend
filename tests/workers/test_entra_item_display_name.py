"""Regression: the generic Entra / legacy persist path must never use
Graph's opaque immutable id as an item's display name.

Bug (found 2026-08-11, reported as "contacts are showing weird characters
for the names"): the inline name chain ended on the Graph id —

    item_name = (item_data.get("displayName") or item_data.get("subject")
                 or item_data.get("mail") or item_data.get("userPrincipalName")
                 or item_data.get("mailNickname") or item_data.get("id", "unknown"))

A Graph contact whose displayName is "" carries no subject / mail /
userPrincipalName / mailNickname either, so the chain fell through to `id`
and Recovery rendered

    AAMkAGQ4OTc2YjdkLWY0NTItNDU4NC05NmZiLTk4Nzk3MGIzNDRhYQBGAAAA...

where a person's name belongs. 308 of jrue@'s 4,727 USER_CONTACT rows were
in this state; zero rows contained non-ASCII, so this was never an encoding
problem. `emailAddresses` — the field contacts actually carry, as opposed to
the directory-object field `mail` — was missing from the chain entirely.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _load_helper():
    """Import just the helper out of the 25k-line worker module.

    Importing the whole module pulls in settings/clients, so lift the
    function's source instead — it is intentionally self-contained.
    """
    src = (_REPO_ROOT / "workers" / "backup-worker" / "main.py").read_text()
    marker = "def _entra_item_display_name("
    start = src.index(marker)
    end = src.index("\n# Planning estimate for a mail folder", start)
    ns: dict = {}
    exec(
        "from typing import Any, Dict\n" + src[start:end],
        ns,
    )
    return ns["_entra_item_display_name"]


_entra_item_display_name = _load_helper()

# The real shape, taken verbatim from prod snapshot_items.metadata->'raw'.
_GRAPH_ID = (
    "AAMkAGQ4OTc2YjdkLWY0NTItNDU4NC05NmZiLTk4Nzk3MGIzNDRhYQBGAAAAAABPMX2j"
    "LOCrTKtD_Uy_vmTbBwCRs-WEcTYSTJmsjz735olpAAAAAAEOAACRs-WEcTYSTJmsjz73"
    "5olpAAT1kqgBAAA="
)


def test_graph_id_is_never_used_as_a_name():
    """The exact prod row that produced the bug report."""
    contact = {
        "@odata.type": "#microsoft.graph.contact",
        "id": _GRAPH_ID,
        "displayName": "",
        "givenName": None,
        "surname": None,
        "jobTitle": "IT Field Systems Infrastructure",
        "companyName": "Corporate",
        "emailAddresses": [],
        "imAddresses": [],
    }
    name = _entra_item_display_name(contact)
    assert name == "(unnamed)"
    assert _GRAPH_ID not in name


def test_email_address_is_preferred_over_the_id():
    """Contacts carry emailAddresses[], not `mail`. The old chain skipped it
    and fell through to the id."""
    contact = {
        "id": _GRAPH_ID,
        "displayName": "",
        "emailAddresses": [{"name": "M Ali", "address": "marali@microsoft.com"}],
    }
    assert _entra_item_display_name(contact) == "marali@microsoft.com"


def test_display_name_still_wins():
    contact = {
        "id": _GRAPH_ID,
        "displayName": "Boris, Victoria",
        "emailAddresses": [{"address": "vboris@example.com"}],
    }
    assert _entra_item_display_name(contact) == "Boris, Victoria"


def test_whitespace_only_display_name_is_not_a_name():
    contact = {"id": _GRAPH_ID, "displayName": "   ", "emailAddresses": []}
    assert _entra_item_display_name(contact) == "(unnamed)"


def test_directory_object_fields_still_resolve():
    """USER_PROFILE / group objects flow through the same helper."""
    assert _entra_item_display_name({"displayName": "Jason Rue"}) == "Jason Rue"
    assert _entra_item_display_name({"mail": "jrue@taylormorrison.com"}) == (
        "jrue@taylormorrison.com"
    )
    assert _entra_item_display_name({"userPrincipalName": "jrue@twc.pvt"}) == "jrue@twc.pvt"
    assert _entra_item_display_name({"mailNickname": "jrue"}) == "jrue"
    assert _entra_item_display_name({"subject": "Q3 planning"}) == "Q3 planning"


def test_malformed_email_addresses_do_not_raise():
    """Graph payloads are not trusted to be well-shaped."""
    for bad in ([], [None], ["not-a-dict"], [{}], None):
        contact = {"id": _GRAPH_ID, "displayName": "", "emailAddresses": bad}
        assert _entra_item_display_name(contact) == "(unnamed)"


def test_name_is_truncated_to_the_column_width():
    long_name = "x" * 400
    assert len(_entra_item_display_name({"displayName": long_name})) == 255


def test_empty_payload_is_unnamed_not_unknown():
    assert _entra_item_display_name({}) == "(unnamed)"
