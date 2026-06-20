"""SLA workload-to-resource filtering.

Pure helpers shared by the scheduler and manual job trigger path. A policy's
workload checkboxes should mean the same thing whether backup starts from the
cron scheduler, a selected user, a bulk click, or datasource-wide trigger.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


# Resource type to SLA flag mapping. Determines which workload checkbox covers
# each catalog/resource row.
RESOURCE_TYPE_TO_SLA_FLAG: dict[str, str] = {
    "MAILBOX": "backup_exchange",
    "SHARED_MAILBOX": "backup_exchange",
    "ROOM_MAILBOX": "backup_exchange",
    "ONEDRIVE": "backup_onedrive",
    "SHAREPOINT_SITE": "backup_sharepoint",
    "TEAMS_CHANNEL": "backup_teams",
    "TEAMS_CHAT": "backup_teams_chats",
    "TEAMS_CHAT_EXPORT": "backup_teams_chats",
    "USER_MAIL": "backup_exchange",
    "USER_ONEDRIVE": "backup_onedrive",
    "USER_CONTACTS": "contacts",
    "USER_CALENDAR": "calendars",
    "USER_CHATS": "backup_teams_chats",
    "ENTRA_USER": "backup_entra_id",
    "ENTRA_GROUP": "backup_entra_id",
    "ENTRA_APP": "backup_entra_id",
    "ENTRA_SERVICE_PRINCIPAL": "backup_entra_id",
    "ENTRA_DEVICE": "backup_entra_id",
    "ENTRA_ROLE": "backup_entra_id",
    "ENTRA_ADMIN_UNIT": "backup_entra_id",
    "ENTRA_AUDIT_LOG": "backup_entra_id",
    "INTUNE_MANAGED_DEVICE": "backup_entra_id",
    "POWER_BI": "backup_power_platform",
    "POWER_APPS": "backup_power_platform",
    "POWER_AUTOMATE": "backup_power_platform",
    "POWER_DLP": "backup_power_platform",
    "COPILOT": "backup_copilot",
    "PLANNER": "planner",
    "TODO": "tasks",
    "ONENOTE": "backup_onedrive",
    "AZURE_VM": "backup_azure_vm",
    "AZURE_SQL_DB": "backup_azure_sql",
    "AZURE_POSTGRESQL": "backup_azure_postgresql",
    "AZURE_POSTGRESQL_SINGLE": "backup_azure_postgresql",
}


# Resource types intentionally excluded from dispatch even when an SLA policy
# would otherwise cover them. TEAMS_CHAT rows are the user-facing catalog entity;
# actual chat-message backup runs through USER_CHATS / TEAMS_CHAT_EXPORT.
SCHEDULER_IGNORED_TYPES: set[str] = {"TEAMS_CHAT"}


@dataclass(frozen=True)
class SlaWorkloadSkip:
    resource_id: str
    resource_type: str
    policy_id: str | None
    flag_name: str | None
    reason: str


def _resource_type_name(resource: Any) -> str:
    resource_type = getattr(resource, "type", resource)
    return resource_type.value if hasattr(resource_type, "value") else str(resource_type)


def _policy_key(policy_id: Any) -> str | None:
    return str(policy_id) if policy_id is not None else None


def resource_type_enabled(resource_type: str, policy: Any) -> bool:
    """Check if a resource type is enabled in an SLA policy's backup flags."""
    if resource_type in SCHEDULER_IGNORED_TYPES:
        return False

    if resource_type == "ENTRA_USER":
        return bool(
            getattr(policy, "backup_entra_id", False)
            or getattr(policy, "contacts", False)
            or getattr(policy, "calendars", False)
        )

    if resource_type in {"ENTRA_GROUP", "DYNAMIC_GROUP"}:
        return bool(
            getattr(policy, "backup_entra_id", False)
            or getattr(policy, "group_mailbox", False)
        )

    flag_name = RESOURCE_TYPE_TO_SLA_FLAG.get(resource_type)
    if not flag_name:
        return False
    return bool(getattr(policy, flag_name, True))


def filter_resource_map_by_policy_flags(
    resources_map: Mapping[str, Any],
    policies_by_id: Mapping[Any, Any],
) -> tuple[dict[str, Any], list[SlaWorkloadSkip]]:
    """Return resources allowed by their assigned SLA plus skip details.

    `resources_map` keys are preserved so callers can keep their existing
    resource-id strings. `policies_by_id` may be keyed by UUIDs or strings.
    """
    normalized_policies = {
        str(policy_id): policy
        for policy_id, policy in policies_by_id.items()
        if policy_id is not None
    }
    filtered: dict[str, Any] = {}
    skipped: list[SlaWorkloadSkip] = []

    for resource_id, resource in resources_map.items():
        policy_id = _policy_key(getattr(resource, "sla_policy_id", None))
        policy = normalized_policies.get(policy_id or "")
        resource_type = _resource_type_name(resource)
        flag_name = RESOURCE_TYPE_TO_SLA_FLAG.get(resource_type)

        if policy is None:
            skipped.append(SlaWorkloadSkip(
                resource_id=str(resource_id),
                resource_type=resource_type,
                policy_id=policy_id,
                flag_name=flag_name,
                reason="policy_not_found",
            ))
            continue

        if resource_type_enabled(resource_type, policy):
            filtered[str(resource_id)] = resource
        else:
            skipped.append(SlaWorkloadSkip(
                resource_id=str(resource_id),
                resource_type=resource_type,
                policy_id=policy_id,
                flag_name=flag_name,
                reason="workload_disabled",
            ))

    return filtered, skipped
