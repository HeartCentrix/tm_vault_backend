"""Global app-settings store (daily discovery time, application timezone,
and the automatic job-retry policy).

Single-row table `app_settings` (id = 1). These helpers read/seed/update that
row using async SQLAlchemy Core text() to match the rest of the codebase.

The daily-schedule worker consumes `discovery_time` + `timezone` via
`get_app_settings(...)`; this module only owns the store + validation.
"""
import re
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import text

# HH:MM, 00:00–23:59
VALID_TIME = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")

_DEFAULT_TIME = "02:00"
_DEFAULT_TZ = "America/Phoenix"
# Automatic retry of failed backup jobs (2026-08-18): after a resource's run
# fails, the scheduler re-dispatches it up to job_retry_max times, waiting
# job_retry_delay_minutes after each failure. 0 disables the sweep. Bounds
# are deliberately tight — retries multiply load, and the 2026-08-18 DB
# convoy was a retry-amplified write pile-up.
_DEFAULT_RETRY_MAX = 3
_DEFAULT_RETRY_DELAY_MIN = 5
RETRY_MAX_CEILING = 10
RETRY_DELAY_MIN_FLOOR = 1
RETRY_DELAY_MIN_CEILING = 1440


def _validate_time(discovery_time: str) -> None:
    if not VALID_TIME.match(discovery_time or ""):
        raise ValueError(
            f"discovery_time must be HH:MM (00:00–23:59), got {discovery_time!r}"
        )


def _validate_timezone(timezone: str) -> None:
    try:
        ZoneInfo(timezone)
    except (ZoneInfoNotFoundError, ValueError) as e:
        raise ValueError(f"invalid timezone {timezone!r}: {e}") from e


def _validate_retry_max(value) -> int:
    try:
        v = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"job_retry_max must be an integer, got {value!r}")
    if not (0 <= v <= RETRY_MAX_CEILING):
        raise ValueError(
            f"job_retry_max must be between 0 (disabled) and "
            f"{RETRY_MAX_CEILING}, got {v}"
        )
    return v


def _validate_retry_delay(value) -> int:
    try:
        v = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"job_retry_delay_minutes must be an integer, got {value!r}")
    if not (RETRY_DELAY_MIN_FLOOR <= v <= RETRY_DELAY_MIN_CEILING):
        raise ValueError(
            f"job_retry_delay_minutes must be between {RETRY_DELAY_MIN_FLOOR} "
            f"and {RETRY_DELAY_MIN_CEILING}, got {v}"
        )
    return v


async def get_app_settings(db) -> dict:
    """Return the singleton settings row, seeding defaults if it is missing."""
    row = (
        await db.execute(
            text(
                "SELECT discovery_time, timezone, "
                "       job_retry_max, job_retry_delay_minutes "
                "  FROM app_settings WHERE id = 1"
            )
        )
    ).first()
    if row is None:
        await db.execute(
            text(
                """
                INSERT INTO app_settings
                    (id, discovery_time, timezone,
                     job_retry_max, job_retry_delay_minutes)
                VALUES (1, :t, :tz, :rm, :rd)
                ON CONFLICT (id) DO NOTHING
                """
            ),
            {"t": _DEFAULT_TIME, "tz": _DEFAULT_TZ,
             "rm": _DEFAULT_RETRY_MAX, "rd": _DEFAULT_RETRY_DELAY_MIN},
        )
        return {"discovery_time": _DEFAULT_TIME, "timezone": _DEFAULT_TZ,
                "job_retry_max": _DEFAULT_RETRY_MAX,
                "job_retry_delay_minutes": _DEFAULT_RETRY_DELAY_MIN}
    return {"discovery_time": row.discovery_time, "timezone": row.timezone,
            "job_retry_max": row.job_retry_max,
            "job_retry_delay_minutes": row.job_retry_delay_minutes}


async def update_app_settings(db, *, discovery_time=None, timezone=None,
                              job_retry_max=None,
                              job_retry_delay_minutes=None) -> dict:
    """Validate + update the provided fields on the singleton row.

    Raises ValueError on a bad discovery_time (regex) or timezone (ZoneInfo).
    Returns the full updated settings dict.
    """
    sets = []
    params: dict = {}
    if discovery_time is not None:
        _validate_time(discovery_time)
        sets.append("discovery_time = :t")
        params["t"] = discovery_time
    if timezone is not None:
        _validate_timezone(timezone)
        sets.append("timezone = :tz")
        params["tz"] = timezone
    if job_retry_max is not None:
        params["rm"] = _validate_retry_max(job_retry_max)
        sets.append("job_retry_max = :rm")
    if job_retry_delay_minutes is not None:
        params["rd"] = _validate_retry_delay(job_retry_delay_minutes)
        sets.append("job_retry_delay_minutes = :rd")

    # Ensure the row exists before updating (seeds defaults if absent).
    await get_app_settings(db)

    if sets:
        sets.append("updated_at = now()")
        await db.execute(
            text(f"UPDATE app_settings SET {', '.join(sets)} WHERE id = 1"),
            params,
        )

    return await get_app_settings(db)
