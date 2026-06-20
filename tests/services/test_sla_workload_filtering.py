from __future__ import annotations

from dataclasses import dataclass

from shared.sla_workloads import filter_resource_map_by_policy_flags, resource_type_enabled


@dataclass
class Policy:
    id: str = "policy-1"
    name: str = "Selective"
    backup_exchange: bool = False
    backup_onedrive: bool = False
    backup_sharepoint: bool = False
    backup_teams: bool = False
    backup_teams_chats: bool = False
    backup_entra_id: bool = False
    contacts: bool = False
    calendars: bool = False
    group_mailbox: bool = False


@dataclass
class Resource:
    id: str
    type: str
    sla_policy_id: str = "policy-1"


def test_resource_type_enabled_allows_chat_without_other_m365_workloads():
    policy = Policy(backup_teams_chats=True)

    assert resource_type_enabled("USER_CHATS", policy) is True
    assert resource_type_enabled("TEAMS_CHAT_EXPORT", policy) is True
    assert resource_type_enabled("USER_MAIL", policy) is False
    assert resource_type_enabled("USER_ONEDRIVE", policy) is False


def test_filter_resource_map_keeps_only_workloads_enabled_by_sla():
    policy = Policy(backup_exchange=True)
    resources = {
        "user": Resource("user", "ENTRA_USER"),
        "mail": Resource("mail", "USER_MAIL"),
        "drive": Resource("drive", "USER_ONEDRIVE"),
        "contacts": Resource("contacts", "USER_CONTACTS"),
        "calendar": Resource("calendar", "USER_CALENDAR"),
        "chats": Resource("chats", "USER_CHATS"),
    }

    filtered, skipped = filter_resource_map_by_policy_flags(resources, {"policy-1": policy})

    assert list(filtered) == ["mail"]
    assert {item.resource_id for item in skipped} == {"user", "drive", "contacts", "calendar", "chats"}


def test_filter_resource_map_supports_chat_only_policy():
    policy = Policy(backup_teams_chats=True)
    resources = {
        "mail": Resource("mail", "USER_MAIL"),
        "chats": Resource("chats", "USER_CHATS"),
    }

    filtered, skipped = filter_resource_map_by_policy_flags(resources, {"policy-1": policy})

    assert list(filtered) == ["chats"]
    assert [item.resource_id for item in skipped] == ["mail"]
