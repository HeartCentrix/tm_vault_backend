"""Regression: an EMPTY selection must not reach a restore/export path.

TMV-001 (QA sweep 2026-08-11, build v0.1.371), the report's highest-risk
item. On Recovery › Calendar the toolbar's Download and Recover stay
enabled at zero selection — verified in the DOM,
``document.querySelectorAll('.recovery-toolbar-btn-download,
.recovery-toolbar-btn-recover')`` both report ``disabled === false`` while
the counter reads "0 selected". RestoreModal defaults to
``scope = 'selected'`` and forwards ``itemIds: scope === 'selected' ?
itemIds : []``, so a click at zero selection posts::

    {"restoreType": "IN_PLACE", "snapshotIds": [snap], "itemIds": []}

restore-worker reads an empty item list as "everything in this snapshot",
so that empty selection replays the WHOLE calendar (5,418 events on the
test resource, measured 2026-08-11) into the live mailbox.

The subtlety this file exists to pin: an empty ``itemIds`` is ALSO how a
legitimate whole-snapshot restore is expressed. RestoreModal's
``scope='full'`` radio and DownloadModal's ``scope='all'`` send exactly the
same keys, and tests/services/test_restore_destination_authz.py plus
tests/services/test_export_scope_authz.py post that bare shape on purpose
(16 cases). The two intents are byte-identical on the wire — the server
cannot tell them apart, only the client can. So the contract is: the caller
DECLARES its scope and the server enforces the declaration.

  selectionScope=SELECTED + nothing picked  → 400 SELECTION_EMPTY
  selectionScope=ALL                        → allowed
  no declaration                            → today's contract, unchanged,
                                              unless RESTORE_REQUIRE_EXPLICIT_SCOPE

These tests exercise the module-level guard directly rather than the
router, so they pin the decision itself and stay independent of the authz
fixtures the sibling files build.
"""
from __future__ import annotations

import importlib.util
import inspect
import os
import pathlib
import sys

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

import pytest
from fastapi import HTTPException

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
_SVC_DIR = _REPO_ROOT / "services" / "job-service"
if str(_SVC_DIR) not in sys.path:
    sys.path.insert(0, str(_SVC_DIR))

_spec = importlib.util.spec_from_file_location(
    "js_main_empty_selection", _SVC_DIR / "main.py",
)
_js = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["js_main_empty_selection"] = _js
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_js)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import-env guard
    pytest.skip(f"job-service/main.py failed to import: {exc}",
                allow_module_level=True)


def _guard(body, item_ids=(), action="recover"):
    return _js.assert_selection_not_empty(body, list(item_ids), action=action)


def _rejects(body, item_ids=()):
    with pytest.raises(HTTPException) as exc:
        _guard(body, item_ids)
    assert exc.value.status_code == 400
    assert exc.value.detail["error"] == "SELECTION_EMPTY"
    return exc.value


def _allows(body, item_ids=()):
    _guard(body, item_ids)


# ── the reported bug ─────────────────────────────────────────────────────


def test_declared_selection_with_no_items_is_rejected():
    """The Calendar toolbar case, once the client declares its scope."""
    _rejects({
        "restoreType": "IN_PLACE",
        "snapshotIds": ["11111111-1111-1111-1111-111111111111"],
        "itemIds": [],
        "selectionScope": "SELECTED",
    })


def test_declared_selection_rejects_regardless_of_spelling():
    for key in ("selectionScope", "restoreScope", "exportScope"):
        for value in ("SELECTED", "selected", " Selection ", "items"):
            _rejects({key: value, "itemIds": []})


def test_the_400_names_the_way_out():
    exc = _rejects({"selectionScope": "SELECTED", "itemIds": []})
    assert "selectionScope=ALL" in exc.detail["message"], (
        "a 400 on a destructive path must tell the caller what a legitimate "
        "whole-resource request looks like"
    )


def test_the_action_word_reaches_the_message():
    with pytest.raises(HTTPException) as exc:
        _guard({"selectionScope": "SELECTED"}, [], action="download")
    assert "download" in exc.value.detail["message"]


# ── legitimate scopes keep working ───────────────────────────────────────


def test_a_real_selection_is_allowed():
    _allows({"selectionScope": "SELECTED"},
            ["22222222-2222-2222-2222-222222222222"])


def test_declared_whole_resource_is_allowed():
    for value in ("ALL", "all", "FULL", "Everything"):
        _allows({"selectionScope": value, "itemIds": []})


def test_folder_selection_without_item_ids_is_allowed():
    """Files folder-select v2: the server expands folderPaths itself, so an
    empty itemIds is the normal shape for a folder restore."""
    _allows({"selectionScope": "SELECTED", "folderPaths": ["/Documents"]})


def test_undeclared_whole_snapshot_stays_allowed_by_default():
    """The existing contract, exercised by 16 cases in the authz suites.

    `{restoreType, snapshotIds, targetUserId}` with no itemIds is how a
    whole-snapshot restore is expressed today. Blocking it by default would
    break RestoreModal's "Recover the entire site" before the frontend can
    declare its scope.
    """
    _allows({
        "restoreType": "ONEDRIVE",
        "snapshotIds": ["11111111-1111-1111-1111-111111111111"],
        "targetUserId": "33333333-3333-3333-3333-333333333333",
    })


# ── the fail-closed switch ───────────────────────────────────────────────


def _with_strict(monkeypatch, enabled: bool):
    monkeypatch.setattr(_js, "_REQUIRE_EXPLICIT_SCOPE", enabled)


def test_strict_mode_rejects_the_undeclared_empty_selection(monkeypatch):
    """What flips on once every client sends selectionScope."""
    _with_strict(monkeypatch, True)
    _rejects({
        "restoreType": "IN_PLACE",
        "snapshotIds": ["11111111-1111-1111-1111-111111111111"],
        "itemIds": [],
    })


@pytest.mark.parametrize("marker", [
    {"workloads": ["Mail", "Calendar"]},
    {"entraSections": ["users"]},
    {"recoverMode": "directory"},
    {"azureRestoreMode": "FULL"},
    {"azureRestoreParams": {"targetResourceGroup": "rg-1"}},
    {"newSiteName": "Recovered Site"},
    {"scope": "site"},
    {"folderPaths": ["/Shared Documents"]},
])
def test_strict_mode_still_allows_the_existing_whole_resource_markers(
    monkeypatch, marker,
):
    """Every mode that legitimately carries no item ids already marks itself.

    If strict mode broke these it would break Entra directory restore, Azure
    SQL/VM restore, PST "all workloads" export, SharePoint recover-to-new-site
    and the /sharepoint/{id}/download site wrapper — none of which have a
    per-item selection to make.
    """
    _with_strict(monkeypatch, True)
    _allows({"snapshotIds": ["11111111-1111-1111-1111-111111111111"], **marker})


def test_whole_resource_markers_win_over_a_mis_declared_selected_scope(
    monkeypatch,
):
    """A whole-resource marker is honoured even when the body ALSO declares
    SELECTED — in both modes.

    This assertion was inverted when first written ("a declared SELECTED is
    rejected in BOTH modes"), and review caught that it would break two real,
    shipping flows. The cross-agent handoff told the frontend to add
    `selectionScope: scope === 'selected' ? 'SELECTED' : 'ALL'` to EVERY
    restore/export body — but RestoreModal's Entra branch returns early and
    never touches `scope`, and DownloadModal's Entra export posts itemIds: []
    with no scope at all. Following that rule literally would therefore have
    sent `selectionScope: SELECTED` alongside entraSections / recoverMode=
    directory and 400'd Entra directory restore AND export.

    Refusing to restore is the second-worst outcome for a backup product,
    behind losing the data. So the markers win: they are positive evidence
    that the request IS whole-resource, and they cannot be produced by the
    zero-selection path this guard exists to stop (an empty Recover click
    carries no workloads / entraSections / recoverMode / azureRestoreMode /
    newSiteName). The guard still rejects the dangerous shape — declared
    SELECTED, no ids, no folderPaths, no markers — which is exactly what an
    empty Recover produces.
    """
    for enabled in (False, True):
        _with_strict(monkeypatch, enabled)
        _allows({"selectionScope": "SELECTED", "workloads": ["Mail"]})
        # …but a bare declared SELECTED with nothing else is still refused.
        _rejects({"selectionScope": "SELECTED"})


# ── the guard is actually wired into both queueing routes ────────────────


def test_both_queueing_routes_call_the_guard():
    for func, action in (
        (_js.trigger_restore, "recover"),
        (_js.trigger_export, "download"),
    ):
        src = inspect.getsource(func)
        assert (
            f'assert_selection_not_empty(request, item_ids, action="{action}")'
            in src
        ), (
            f"{func.__name__} must run the empty-selection guard — it is the "
            "function that persists the Job row and publishes to the restore "
            "queues"
        )


# ── the exact payloads the shipping client now sends ─────────────────────


def test_the_real_restoremodal_empty_selection_body_is_refused(monkeypatch):
    """The concrete shape RestoreModal posts when Recover is clicked with
    nothing ticked, now that it declares its scope (src/components/
    RestoreModal.tsx). Before the client sent selectionScope this body was
    indistinguishable from a deliberate whole-snapshot restore and the guard
    was inert in production — reviewer's blocking finding on the highest-risk
    item in the QA sweep."""
    _with_strict(monkeypatch, False)
    _rejects({
        "restoreType": "IN_PLACE",
        "snapshotIds": ["6c726320-cc25-4ffa-8787-c760c8396294"],
        "selectionScope": "SELECTED",
        "itemIds": [],
    })


def test_the_real_restoremodal_whole_snapshot_body_still_works(monkeypatch):
    """The same modal with the 'entire snapshot' radio picked. Must NOT be
    refused — inability to restore is the second-worst outcome here."""
    _with_strict(monkeypatch, False)
    _allows({
        "restoreType": "IN_PLACE",
        "snapshotIds": ["6c726320-cc25-4ffa-8787-c760c8396294"],
        "selectionScope": "ALL",
        "itemIds": [],
    })


def test_entra_directory_restore_is_unaffected(monkeypatch):
    """RestoreModal's Entra branch returns early and never sets `scope`, so it
    posts no selectionScope at all — and DownloadModal's Entra export posts
    itemIds: [] with none either. Both must keep working in both modes."""
    for enabled in (False, True):
        _with_strict(monkeypatch, enabled)
        _allows({"restoreType": "IN_PLACE", "entraSections": ["users"], "itemIds": []})
        _allows({"restoreType": "IN_PLACE", "recoverMode": "directory", "itemIds": []})
