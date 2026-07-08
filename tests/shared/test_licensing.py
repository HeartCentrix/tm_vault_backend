"""Unit tests for license-entitlement parsing from Graph user.assignedPlans.

Pure functions — no network. This is the 'quick hack' that lets Tier-1 user
discovery (a single bulk GET /users call) know each user's license coverage
inline, instead of waiting for per-workload 403/404 probes near backup time.
"""
from __future__ import annotations

from shared.licensing import (
    derive_licensed_services,
    category_unlicensed,
    CATEGORY_SERVICE,
)


def _plan(service, status="Enabled"):
    return {"service": service, "capabilityStatus": status, "servicePlanId": "x"}


# ─── derive_licensed_services ──────────────────────────────────────────

def test_full_e3_user_has_all_services():
    plans = [_plan("exchange"), _plan("SharePoint"), _plan("MicrosoftCommunicationsOnline"),
             _plan("TeamspaceAPI")]
    svc = derive_licensed_services(plans)
    assert svc == {"exchange": True, "sharepoint": True, "teams": True}


def test_no_plans_means_all_unlicensed():
    assert derive_licensed_services([]) == {"exchange": False, "sharepoint": False, "teams": False}
    assert derive_licensed_services(None) == {"exchange": False, "sharepoint": False, "teams": False}


def test_teams_only_user():
    svc = derive_licensed_services([_plan("TeamspaceAPI")])
    assert svc == {"exchange": False, "sharepoint": False, "teams": True}


def test_suspended_and_deleted_plans_do_not_count():
    plans = [_plan("exchange", "Suspended"), _plan("SharePoint", "Deleted")]
    svc = derive_licensed_services(plans)
    assert svc == {"exchange": False, "sharepoint": False, "teams": False}


def test_warning_status_still_counts_as_licensed():
    # capabilityStatus "Warning" = grace period; the mailbox/drive still works.
    svc = derive_licensed_services([_plan("exchange", "Warning")])
    assert svc["exchange"] is True


def test_service_match_is_case_insensitive():
    svc = derive_licensed_services([_plan("EXCHANGE"), _plan("sharepoint")])
    assert svc["exchange"] is True and svc["sharepoint"] is True


# ─── category_unlicensed (drives probe short-circuit) ──────────────────

def test_category_unlicensed_true_when_service_absent():
    svc = {"exchange": False, "sharepoint": True, "teams": True}
    assert category_unlicensed(svc, "mail") is True
    assert category_unlicensed(svc, "calendar") is True
    assert category_unlicensed(svc, "contacts") is True
    assert category_unlicensed(svc, "onedrive") is False
    assert category_unlicensed(svc, "chats") is False


def test_category_unlicensed_false_when_services_unknown():
    # No entitlement data → never short-circuit; fall back to the live probe.
    assert category_unlicensed(None, "mail") is False
    assert category_unlicensed({}, "mail") is False


def test_category_map_covers_all_five_workloads():
    assert set(CATEGORY_SERVICE) == {"mail", "onedrive", "contacts", "calendar", "chats"}
