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
                 retry_after=None, attempts=1, response=None, inner_code=None):
        self.stage = stage
        self.status_code = status
        self.code = _identifier(code)
        self.request_id = _identifier(request_id)
        self.inner_code = _identifier(inner_code)
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
        inner = error.get("innerError") or error.get("innererror") or {}
        if not isinstance(inner, dict):
            inner = {}
        return cls(stage, status=response.status_code, code=error.get("code"), inner_code=inner.get("code"),
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


def sharepoint_file_identity(metadata):
    """Require a length and source-version proof before publishing file bytes."""
    if not isinstance(metadata, dict) or "Length" not in metadata:
        raise ProviderReadError("invalid_file_metadata")
    try:
        size = int(metadata["Length"])
    except (TypeError, ValueError):
        raise ProviderReadError("invalid_file_length") from None
    if size < 0:
        raise ProviderReadError("invalid_file_length")
    etag = metadata.get("ETag") or metadata.get("@odata.etag") or metadata.get("odata.etag")
    if not etag and isinstance(metadata.get("__metadata"), dict):
        etag = metadata["__metadata"].get("etag")
    if etag is not None and (not isinstance(etag, str) or not etag or "\r" in etag or "\n" in etag):
        raise ProviderReadError("invalid_file_version")
    version = tuple(metadata.get(key) for key in ("UniqueId", "UIVersionLabel", "TimeLastModified"))
    if not etag and not all(version):
        raise ProviderReadError("missing_file_version")
    return size, etag, version
