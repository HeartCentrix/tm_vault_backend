"""Unit tests for the global app-settings store validation.

Covers `shared.app_settings`:
  - VALID_TIME regex (HH:MM 00:00–23:59)
  - timezone validation via zoneinfo.ZoneInfo
  - update_app_settings: good values pass, bad time / bad tz raise ValueError

Hermetic: no real DB/network. A tiny FakeDB records the issued statements and
returns canned rows for the SELECT, so update_app_settings exercises its real
validation + SQL-building path without a Postgres connection.
"""
from __future__ import annotations

import pytest

from shared.app_settings import (
    VALID_TIME,
    get_app_settings,
    update_app_settings,
)


class _FakeResult:
    """Mimics the .first() seam used by get_app_settings."""

    def __init__(self, row):
        self._row = row

    def first(self):
        return self._row


class _Row:
    def __init__(self, discovery_time, timezone,
                 job_retry_max=3, job_retry_delay_minutes=5):
        self.discovery_time = discovery_time
        self.timezone = timezone
        self.job_retry_max = job_retry_max
        self.job_retry_delay_minutes = job_retry_delay_minutes


class FakeDB:
    """In-memory stand-in for the async session.

    Holds a single settings row; .execute(stmt, params) inspects the SQL
    string to decide whether it is a SELECT (return the row) or a mutation
    (apply params to the row). Async to match the real session API.
    """

    def __init__(self, row=None):
        self.row = row  # _Row or None
        self.statements: list[str] = []

    async def execute(self, stmt, params=None):
        sql = str(stmt)
        self.statements.append(sql)
        if sql.strip().upper().startswith("SELECT"):
            return _FakeResult(self.row)
        if "INSERT" in sql.upper():
            self.row = _Row(params["t"], params["tz"],
                            params.get("rm", 3), params.get("rd", 5))
            return _FakeResult(None)
        if "UPDATE" in sql.upper():
            if self.row is None:
                self.row = _Row("02:00", "America/Phoenix", 3, 5)
            if params and "t" in params:
                self.row.discovery_time = params["t"]
            if params and "tz" in params:
                self.row.timezone = params["tz"]
            return _FakeResult(None)
        return _FakeResult(None)


# ─── VALID_TIME regex ─────────────────────────────────────────────────────

@pytest.mark.parametrize("good", ["00:00", "02:00", "09:59", "13:45", "23:59"])
def test_valid_time_accepts(good):
    assert VALID_TIME.match(good)


@pytest.mark.parametrize("bad", ["25:99", "24:00", "2:00", "02:60", "0200", "ab:cd", ""])
def test_valid_time_rejects(bad):
    assert not VALID_TIME.match(bad)


# ─── update_app_settings validation ───────────────────────────────────────

async def test_update_accepts_good_time_and_tz():
    db = FakeDB(_Row("02:00", "America/Phoenix"))
    result = await update_app_settings(
        db, discovery_time="03:30", timezone="America/New_York"
    )
    assert result == {"discovery_time": "03:30", "timezone": "America/New_York",
                      "job_retry_max": 3, "job_retry_delay_minutes": 5}


async def test_update_rejects_bad_time():
    db = FakeDB(_Row("02:00", "America/Phoenix"))
    with pytest.raises(ValueError):
        await update_app_settings(db, discovery_time="25:99")


async def test_update_rejects_bad_timezone():
    db = FakeDB(_Row("02:00", "America/Phoenix"))
    with pytest.raises(ValueError):
        await update_app_settings(db, timezone="Not/AZone")


async def test_update_partial_only_time():
    db = FakeDB(_Row("02:00", "America/Phoenix"))
    result = await update_app_settings(db, discovery_time="06:15")
    assert result["discovery_time"] == "06:15"
    assert result["timezone"] == "America/Phoenix"


async def test_get_seeds_defaults_when_missing():
    db = FakeDB(row=None)
    result = await get_app_settings(db)
    assert result == {"discovery_time": "02:00", "timezone": "America/Phoenix",
                      "job_retry_max": 3, "job_retry_delay_minutes": 5}
