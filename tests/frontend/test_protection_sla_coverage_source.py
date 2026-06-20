from __future__ import annotations

import re
from pathlib import Path


def test_entra_user_warning_allows_any_user_workload_toggle():
    source = (
        Path(__file__).resolve().parents[2]
        / ".."
        / "tm_vault"
        / "src"
        / "pages"
        / "Protection.tsx"
    ).resolve().read_text()

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
