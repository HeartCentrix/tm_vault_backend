"""Regression: GraphClient._get must actually send its query params.

Bug (2026-07-14): the request line guarded params with
``if not next_url.startswith("http")`` — always false for a full URL —
so every $filter/$top/$select passed to _get() was silently dropped.
discover_teams' resourceProvisioningOptions filter never reached Graph
and all ~10k tenant groups (including plain security groups with
has_team=false) were persisted as phantom TEAMS_CHANNEL resources,
which made the Recovery UI render security groups as M365-group
surfaces (site/mail/channel tabs).
"""
from __future__ import annotations

import pathlib
import re

_SRC = (
    pathlib.Path(__file__).resolve().parents[2] / "shared" / "graph_client.py"
).read_text()


def test_get_sends_params_unconditionally():
    assert 'params=params if not next_url.startswith("http")' not in _SRC
    # the legacy pager still nulls params after page 1 (nextLink embeds them)
    assert "params = None  # params only on first request" in _SRC


def test_discover_teams_filter_still_present():
    idx = _SRC.find("async def discover_teams")
    assert idx != -1
    blk = _SRC[idx: idx + 1200]
    assert "resourceProvisioningOptions/Any(x:x eq 'Team')" in blk
