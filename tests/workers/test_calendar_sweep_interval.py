"""The calendar /events sweep (series masters + events outside the calendarView window) re-listed
every master and single event of every calendar daily — O(calendar) Graph reads per user per day.
It only adds masters and out-of-window events (deletions come from the delta's @removed), so it
runs weekly by default (CALENDAR_EVENTS_SWEEP_HOURS overrides). The default calendar's delta still
runs every run; sub-calendars (no delta) keep their 24 h refresh."""
import pathlib
import re

_SRC = (pathlib.Path(__file__).resolve().parents[2] / "workers" / "backup-worker" / "main.py").read_text()
_FLAT = re.sub(r"\s+", " ", _SRC)


def test_events_sweep_defaults_to_weekly_with_env_override():
    assert 'SWEEP_HOURS = float(os.environ.get( "CALENDAR_EVENTS_SWEEP_HOURS", "168", ))' in _FLAT


def test_events_sweep_uses_its_own_interval():
    assert ("last_fetched_iso=swept_map.get(cal_id), now_dt=now_utc, refresh_hours=SWEEP_HOURS," in _FLAT)


def test_sub_calendars_keep_the_daily_refresh():
    assert '"CALENDAR_SUBCALENDAR_REFRESH_HOURS", "24",' in _FLAT
    assert "last_fetched_iso=last_fetched_iso, now_dt=now_utc, refresh_hours=SUB_REFRESH_HOURS," in _FLAT
