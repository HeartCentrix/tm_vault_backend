from __future__ import annotations

import os
import re
from pathlib import Path


def test_entra_user_warning_allows_any_user_workload_toggle():
    repo = Path(__file__).resolve().parents[2]
    frontend = Path(os.environ.get("TM_VAULT_FRONTEND_PATH", repo.parent / "tm_vault"))
    if not frontend.exists():
        import pytest
        pytest.skip("set TM_VAULT_FRONTEND_PATH to the frontend checkout for cross-repo validation")
    source = (frontend / "src/pages/Protection.tsx").read_text()

    match = re.search(r"entra_user:\s*{(?P<body>.*?)},\n\s*entra_group:", source, re.S)
    assert match, "Could not find entra_user SLA coverage rule"
    body = match.group("body")

    for flag in (
        "backupExchange",
        "backupOneDrive",
        "backupTeamsChats",
        "contacts",
        "calendars",
        "backupEntraId",
    ):
        assert f"policy.{flag}" in body
