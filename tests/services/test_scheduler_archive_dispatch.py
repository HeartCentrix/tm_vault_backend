"""Task 2.3: scheduler dispatch + heavy-queue routing for USER_ARCHIVE.

USER_ARCHIVE Tier-2 children (Task 1.2, jrue-only pilot) are created by
discovery but the scheduler had no SLA-flag mapping for them, so
``resource_type_enabled`` fell through to the "unknown resource type"
branch and refused to dispatch every archive child regardless of the
SLA policy's ``backup_exchange_archive`` checkbox. Separately,
``shared.export_routing.pick_backup_queue`` — the REAL production
dispatch gate (``shared.routing_fence``'s queue-by-workload map is
inert; nothing consults it) — had no entry for USER_ARCHIVE, so even a
dispatched archive job would land on the shared backup.normal lane
instead of backup.heavy alongside the other big-content workloads
(OneDrive / SharePoint / Power BI).

``services/backup-scheduler/main.py`` imports fastapi at module level;
this dev environment has no fastapi installed (no network access to
pip), so — mirroring the existing convention in
tests/services/test_scheduler_activity_finalize.py and
test_scheduler_sweep_lease_aware.py — the map/gating pins here are
source-text assertions against the file, not a live import. The
heavy-queue routing pins DO live-import ``shared.export_routing``,
which has no fastapi dependency.

Pins:
  * RESOURCE_TYPE_TO_SLA_FLAG["USER_ARCHIVE"] == "backup_exchange_archive"
    (the UI checkbox's own flag — NOT backup_exchange, whose old archive
    walk is a confirmed dead no-op per reviewer findings).
  * RESOURCE_TYPE_DISPLAY_NAMES has a human label for USER_ARCHIVE.
  * pick_backup_queue(resource_type="USER_ARCHIVE") routes to the heavy
    queue, same as USER_ONEDRIVE / ONEDRIVE / SHAREPOINT_SITE / POWER_BI.
"""
from __future__ import annotations

import os
import pathlib
import re
import types

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret-value")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

from shared.export_routing import pick_backup_queue, _HEAVY_BACKUP_TYPES  # noqa: E402
from shared import config  # noqa: E402

_MAIN = (
    pathlib.Path(__file__).resolve().parents[2]
    / "services" / "backup-scheduler" / "main.py"
).read_text()


def _block(anchor: str, span: int = 4000) -> str:
    idx = _MAIN.find(anchor)
    assert idx != -1, f"anchor {anchor!r} not found"
    return _MAIN[max(0, idx - span): idx + span]


# The scheduler and manual trigger path share the same workload contract.
from shared import sla_workloads as _gating


# ─── RESOURCE_TYPE_TO_SLA_FLAG / display-name maps (source pins) ───────

def test_user_archive_maps_to_its_own_sla_flag():
    assert _gating.RESOURCE_TYPE_TO_SLA_FLAG["USER_ARCHIVE"] == "backup_exchange_archive"
    assert "from shared.sla_workloads import RESOURCE_TYPE_TO_SLA_FLAG, resource_type_enabled" in _MAIN


def test_user_archive_has_display_name():
    blk = _block("RESOURCE_TYPE_DISPLAY_NAMES: Dict[str, str] = {")
    m = re.search(r'"USER_ARCHIVE"\s*:\s*"([^"]+)"', blk)
    assert m and m.group(1), "USER_ARCHIVE missing a human display name"


# ─── resource_type_enabled gating (behavioral, via exec'd source slice) ─

def _policy(**flags) -> types.SimpleNamespace:
    return types.SimpleNamespace(**flags)


def test_archive_dispatch_enabled_when_flag_on():
    policy = _policy(backup_exchange_archive=True, backup_exchange=False)
    assert _gating.resource_type_enabled("USER_ARCHIVE", policy) is True


def test_archive_dispatch_disabled_when_flag_off():
    policy = _policy(backup_exchange_archive=False, backup_exchange=True)
    assert _gating.resource_type_enabled("USER_ARCHIVE", policy) is False


def test_archive_dispatch_is_independent_of_backup_exchange_flag():
    # The old backup_mailbox archive walk that backup_exchange used to
    # feed is a confirmed dead no-op — USER_ARCHIVE dispatch must be
    # driven only by backup_exchange_archive, not by backup_exchange.
    off = _policy(backup_exchange_archive=False, backup_exchange=True)
    assert _gating.resource_type_enabled("USER_ARCHIVE", off) is False

    on = _policy(backup_exchange_archive=True, backup_exchange=False)
    assert _gating.resource_type_enabled("USER_ARCHIVE", on) is True


# ─── heavy-queue routing (shared/export_routing.py — the real gate) ────

def test_user_archive_is_in_heavy_backup_types_set():
    assert "USER_ARCHIVE" in _HEAVY_BACKUP_TYPES


def test_user_archive_routes_to_heavy_queue():
    assert config.settings.BACKUP_HEAVY_ENABLED
    assert (
        pick_backup_queue(resource_type="USER_ARCHIVE")
        == config.settings.BACKUP_HEAVY_QUEUE
    )


def test_other_heavy_types_still_route_to_heavy_queue():
    # Regression guard: adding USER_ARCHIVE must not disturb the existing set.
    for rtype in ("USER_ONEDRIVE", "ONEDRIVE", "SHAREPOINT_SITE", "POWER_BI"):
        assert (
            pick_backup_queue(resource_type=rtype)
            == config.settings.BACKUP_HEAVY_QUEUE
        )


def test_non_heavy_type_routes_to_default_queue():
    assert pick_backup_queue(resource_type="USER_MAIL", default_queue="backup.urgent") == "backup.urgent"
