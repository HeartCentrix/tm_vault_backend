"""EntraExportPipeline — section-by-section CSV/JSON + ZIP assembler.

Section UI label → CSV column contract defined here. Sections whose
raw shape is deeply nested (conditional-access policies, intune
settings trees, auth strengths) are JSON-only regardless of the
requested format."""
from __future__ import annotations

import csv
from shared.csv_safe import SafeCsvWriter  # Check 17: CSV formula-injection
import io
import json
import zipfile
from datetime import datetime, timezone
from typing import Any, Dict, List


# Sections that can be meaningfully flattened into CSV. Others always
# emit JSON inside the ZIP even if the user picked "csv".
CSV_CAPABLE_SECTIONS: set = {
    "users", "groups", "applications", "service_principals",
    "admin_units", "intune_devices", "audit_logs", "sign_in_logs",
}


# Per-section CSV column lists. Lives next to the code that uses them.
_CSV_COLUMNS: Dict[str, List[str]] = {
    "users": [
        "id", "userPrincipalName", "displayName", "mail", "jobTitle",
        "department", "officeLocation", "accountEnabled", "userType",
        "externalUserState", "createdDateTime", "proxyAddresses",
    ],
    "groups": [
        "id", "displayName", "description", "mailNickname", "mailEnabled",
        "securityEnabled", "groupTypes", "visibility", "membershipRule",
        "createdDateTime",
    ],
    "applications": [
        "id", "appId", "displayName", "signInAudience", "createdDateTime",
        "tags", "requiredResourceAccess",
    ],
    "service_principals": [
        "id", "appId", "displayName", "servicePrincipalType",
        "accountEnabled", "tags", "appRoleAssignments",
    ],
    "admin_units": [
        "id", "displayName", "description", "visibility",
    ],
    "intune_devices": [
        "id", "deviceName", "operatingSystem", "osVersion",
        "userPrincipalName", "complianceState", "lastSyncDateTime",
        "serialNumber",
    ],
    "audit_logs": [
        "id", "activityDateTime", "category", "result",
        "initiatedBy_displayName", "targetResources_0_displayName",
        "correlationId", "additionalDetails",
    ],
    "sign_in_logs": [
        "id", "createdDateTime", "userDisplayName", "userPrincipalName",
        "appDisplayName", "ipAddress", "clientAppUsed",
        "conditionalAccessStatus", "status_errorCode", "status_failureReason",
    ],
}


# List-valued fields that should be semicolon-joined into a single CSV cell.
_JOINED_FIELDS: set = {"proxyAddresses", "groupTypes", "tags"}

# Nested-dict fields that should be JSON-stringified into a single cell.
_JSON_CELL_FIELDS: set = {
    "requiredResourceAccess", "appRoleAssignments", "additionalDetails",
}


_MEMBERS_COMPANION = {
    "groups": ("groups_members.csv", "group_id"),
    "admin_units": ("admin_units_members.csv", "au_id"),
}


def _flatten_members_rows(parent_id_key: str, items: List[Any]) -> List[List[str]]:
    """Expand each item's `members` list into flat CSV rows. Returns
    header + data rows. Accepts either {"id": ..., "displayName": ...,
    "@odata.type": "..."} dicts or bare ids."""
    header = [parent_id_key, "member_id", "member_type", "member_displayName"]
    out: List[List[str]] = [header]
    for it in items:
        raw = (getattr(it, "extra_data", None) or {}).get("raw") or {}
        parent_id = raw.get("id", "")
        for m in (raw.get("members") or []):
            if isinstance(m, dict):
                mid = m.get("id", "")
                mtype = (m.get("@odata.type") or "").split(".")[-1] or ""
                mname = m.get("displayName", "") or ""
            elif isinstance(m, str):
                mid = m.rsplit("/", 1)[-1]
                mtype = ""
                mname = ""
            else:
                continue
            out.append([parent_id, mid, mtype, mname])
    return out


def _flatten_path(raw: Dict[str, Any], path: str) -> Any:
    """Resolve a dotted/underscored path like `initiatedBy_displayName`
    or `targetResources_0_displayName` against a nested raw dict. Keys
    are flattened with `_` as separator; numeric segments index into
    lists. Unknown paths return empty string."""
    parts = path.split("_")
    cur: Any = raw
    i = 0
    while i < len(parts):
        if cur is None:
            return ""
        key = parts[i]
        if key.isdigit() and isinstance(cur, list):
            idx = int(key)
            cur = cur[idx] if 0 <= idx < len(cur) else None
            i += 1
            continue
        if isinstance(cur, dict):
            if key in cur:
                cur = cur[key]
                i += 1
                continue
            joined = "_".join(parts[i:])
            if joined in cur:
                return cur[joined]
            return ""
        return ""
    return cur if cur is not None else ""


def _cell_value(col: str, raw: Dict[str, Any]) -> str:
    if col in _JOINED_FIELDS:
        v = raw.get(col) or []
        return ";".join(str(x) for x in v)
    if col in _JSON_CELL_FIELDS:
        v = raw.get(col)
        if v is None:
            return ""
        return json.dumps(v, separators=(",", ":"), default=str)
    if col in raw:
        v = raw[col]
    else:
        v = _flatten_path(raw, col)
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, separators=(",", ":"), default=str)
    return str(v)


def format_section_csv(section: str, items: List[Any]) -> str:
    """Render items as a CSV string using the section's column contract.
    Raises KeyError for sections that aren't CSV-capable so callers
    catch config mistakes early."""
    if section not in _CSV_COLUMNS:
        raise KeyError(f"no CSV columns defined for section {section!r}")
    cols = _CSV_COLUMNS[section]
    buf = io.StringIO()
    writer = SafeCsvWriter(csv.writer(buf))
    writer.writerow(cols)
    for it in items:
        raw = (getattr(it, "extra_data", None) or {}).get("raw") or {}
        writer.writerow([_cell_value(c, raw) for c in cols])
    return buf.getvalue()


def format_section_json(section: str, items: List[Any]) -> str:
    """Raw-verbatim JSON dump. Works for every section; the output is
    the list of `extra_data.raw` dicts."""
    rows = []
    for it in items:
        raw = (getattr(it, "extra_data", None) or {}).get("raw") or {}
        rows.append(raw)
    return json.dumps(rows, default=str, indent=2)


# ---- ZIP assembler ----

class EntraExportPipeline:
    """Build a single ZIP from a `{section_label: [items]}` mapping."""

    def __init__(self, *, snapshot_id: str, format: str, include_nested_detail: bool = False):
        if format not in ("csv", "json"):
            format = "json"
        self.snapshot_id = snapshot_id
        self.format = format
        self.include_nested_detail = include_nested_detail

    def _filename_for(self, section: str) -> str:
        if self.format == "csv" and section in CSV_CAPABLE_SECTIONS:
            return f"{section}.csv"
        return f"{section}.json"

    def build_zip(self, out_buffer, items_by_section: Dict[str, List[Any]]) -> Dict[str, Any]:
        """Write the ZIP into `out_buffer`. Returns the manifest dict."""
        manifest = {
            "snapshot_id": self.snapshot_id,
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "format": self.format,
            "include_nested_detail": self.include_nested_detail,
            "counts": {s: len(v) for s, v in items_by_section.items()},
        }
        with zipfile.ZipFile(out_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("MANIFEST.json", json.dumps(manifest, indent=2))
            for section, items in items_by_section.items():
                target = self._filename_for(section)
                if target.endswith(".csv"):
                    zf.writestr(target, format_section_csv(section, items))
                else:
                    zf.writestr(target, format_section_json(section, items))
                # Companion member CSV emitted only when the nested
                # toggle is on AND the section has one configured.
                if self.include_nested_detail and section in _MEMBERS_COMPANION:
                    companion_name, parent_key = _MEMBERS_COMPANION[section]
                    rows = _flatten_members_rows(parent_key, items)
                    buf = io.StringIO()
                    writer = SafeCsvWriter(csv.writer(buf))
                    for r in rows:
                        writer.writerow(r)
                    zf.writestr(companion_name, buf.getvalue())
                    manifest["counts"][companion_name] = len(rows) - 1  # minus header
        return manifest
