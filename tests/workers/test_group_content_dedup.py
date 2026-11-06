"""Group and channel backups skip content another protected resource owns (see
tests/integration/test_owned_elsewhere_postgres.py)."""
import pathlib
import re

_SRC = (pathlib.Path(__file__).resolve().parents[2] / "workers" / "backup-worker" / "main.py").read_text()
_FLAT = re.sub(r"\(\s+", "(", re.sub(r"\s+", " ", _SRC))


def _before(owner_check, guarded_call, start_marker):
    start = _FLAT.index(start_marker)
    check = _FLAT.index(owner_check, start)
    call = _FLAT.index(guarded_call, start)
    return check < call


def test_group_backup_skips_a_protected_team_site_and_protected_channels():
    start = 'elif resource_type in ("ENTRA_GROUP", "DYNAMIC_GROUP", "M365_GROUP"):'
    assert _before('await self._owned_elsewhere(tenant, "TEAMS_CHANNEL", obj_id', "await self._backup_team_channels(", start)
    assert _before('await self._owned_elsewhere(tenant, "SHAREPOINT_SITE", site_ext_id', "sp_result = await self.backup_sharepoint(", start)


def test_channel_backup_skips_a_protected_group_mailbox_and_team_site():
    start = "# A TEAMS_CHANNEL row's external_id is the team (M365 group) id."
    assert _before('await self._owned_elsewhere(tenant, "M365_GROUP", team_id', "await self.backup_group_mailbox_content(", start)
    assert _before('await self._owned_elsewhere(tenant, "SHAREPOINT_SITE", site_ext_id', "sp_result = await self.backup_sharepoint(", start)
