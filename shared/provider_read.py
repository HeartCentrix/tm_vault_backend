"""Safe provider read failures and finite retry budgets for streaming callers."""
from __future__ import annotations

from dataclasses import dataclass
import re
import time

import httpx

from shared.graph_ratelimit import parse_retry_after


def _identifier(value):
    value = str(value or "")
    return value if re.fullmatch(r"[A-Za-z0-9_.:\-]{1,128}", value) else None


class ProviderReadError(RuntimeError):
    """Never include provider messages, resource URLs or credentials in logs."""

    def __init__(self, stage, *, status=None, code=None, request_id=None,
                 retry_after=None, attempts=1, response=None):
        self.stage = stage
        self.status_code = status
        self.code = _identifier(code)
        self.request_id = _identifier(request_id)
        self.retry_after = retry_after
        self.attempts = attempts
        self.response = response
        super().__init__(f"provider read {stage}: status={status} code={self.code or 'unknown'} attempts={attempts}")

    @classmethod
    def from_response(cls, response, stage, *, attempts=1):
        try:
            error = response.json().get("error", {})
            if not isinstance(error, dict):
                error = {}
        except (ValueError, httpx.ResponseNotRead):
            error = {}
        return cls(stage, status=response.status_code, code=error.get("code"),
                   request_id=response.headers.get("request-id") or response.headers.get("SPRequestGuid"),
                   retry_after=parse_retry_after(response.headers.get("Retry-After")),
                   attempts=attempts, response=response)


@dataclass
class ProviderReadBudget:
    """Long cooldowns are returned to the durable owner, never shortened."""
    attempts: int = 0
    waited: float = 0.0
    started: float = 0.0

    def __post_init__(self):
        self.started = time.monotonic()

    def retry_delay(self, error: ProviderReadError) -> float:
        delay = error.retry_after
        if delay is None:
            delay = float(2 ** min(self.attempts, 5))
        delay = max(0.0, delay)
        if (self.attempts >= 4 or self.waited + delay > 30
                or time.monotonic() - self.started + delay > 120):
            error.retry_after = delay
            raise error
        self.waited += delay
        return delay
