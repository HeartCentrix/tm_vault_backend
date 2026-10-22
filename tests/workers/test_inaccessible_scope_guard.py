"""A nested 404 inside a walk must not quarantine the whole resource.

Live incident (2026-08-06 → 2026-08-07). ``Archive — Jason Rue``
(USER_ARCHIVE 78e71aa6) was flipped to ``status=INACCESSIBLE`` because ONE
child folder answered 404 mid-enumeration. The handler catch in
``_process_single_backup`` lowercased the whole exception string and
substring-matched "404"/"not found":

    error_str = str(e).lower()
    is_inaccessible = any(kw in error_str for kw in [
        "not found", "404", "resource_not_found", ...])

``EnumerationFailed`` embeds the failing child's status text by
construction (archive_backup.py:238-243 and :762-765 interpolate ``{e}``
from an httpx error whose message is ``Client error '404 Not Found' for
url ...``), so the match was guaranteed for the one exception class this
path is most likely to see.

The consequence is silent and permanent: backup-scheduler dispatches only
DISCOVERED|ACTIVE (services/backup-scheduler/main.py:2126/2464/2674), so
the resource left every cycle, and nothing re-probes an existing tier-2
row — it was still INACCESSIBLE 26h later, having self-healed never.

Quarantine now requires proof that the RESOURCE ITSELF is gone or locked:
a real HTTP status on a resource-level error. Scope is never inferred
from rendered text.

NB there are TWO distinct ``EnumerationFailed`` classes — main.py:1625
(chats/mail) and archive_backup.py:70 (archive) — and main.py does not
import the latter. An ``isinstance`` check against either one alone
misses the other, which is exactly the case that caused this incident.
Both must be recognised.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys

import httpx
import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_inaccessible_scope", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_inaccessible_scope"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )

quarantines = _bw.is_resource_level_inaccessible
MainEnumerationFailed = _bw.EnumerationFailed


class _ArchiveEnumerationFailed(Exception):
    """Stand-in for archive_backup.EnumerationFailed — a SEPARATE class
    object from main.py's, matching the real two-class layout."""


_ArchiveEnumerationFailed.__name__ = "EnumerationFailed"


def _http_error(status: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://graph.microsoft.com/v1.0/users/x/messages")
    response = httpx.Response(status_code=status, request=request, text="")
    return httpx.HTTPStatusError(
        f"Client error '{status}' for url '{request.url}'",
        request=request,
        response=response,
    )


class TestEnumerationFailuresNeverQuarantine:
    def test_the_exact_archive_exception_from_the_incident(self):
        exc = _ArchiveEnumerationFailed(
            "archive enumeration/export failed for MBX:6193533a: "
            "HTTPStatusError: Client error '404 Not Found' for url "
            "'https://graph.microsoft.com/v1.0/users/x/mailFolders/AAA/messages'"
        )
        assert quarantines(exc) is False

    def test_the_other_enumeration_failed_class_is_also_recognised(self):
        """main.py's own class — the chats/mail enumeration path."""
        exc = MainEnumerationFailed(
            "USER_CHATS: 20 of 20 chat drain(s) failed — 404 not found"
        )
        assert quarantines(exc) is False

    @pytest.mark.parametrize(
        "text",
        [
            "404 ErrorItemNotFound",
            "one folder returned 423 locked",
            "access_denied on /Recoverable Items",
            "authorization_failed for child folder",
            "resource_not_found: subfolder",
        ],
    )
    def test_no_keyword_in_an_enumeration_message_can_quarantine(self, text):
        assert quarantines(_ArchiveEnumerationFailed(text)) is False
        assert quarantines(MainEnumerationFailed(text)) is False

    def test_run_abandoned_never_quarantines(self):
        """A swept run is not a missing mailbox."""
        assert quarantines(_bw.RunAbandoned("snapshot went terminal")) is False


class TestResourceLevelErrorsStillQuarantine:
    @pytest.mark.parametrize("status", [403, 404, 423])
    def test_resource_level_http_status_quarantines(self, status):
        assert quarantines(_http_error(status)) is True

    @pytest.mark.parametrize("status", [400, 429, 500, 502, 503])
    def test_transient_and_malformed_never_quarantine(self, status):
        assert quarantines(_http_error(status)) is False

    def test_an_enumeration_failure_wrapping_a_404_still_does_not_quarantine(self):
        """Class beats status: the 404 belonged to a child, not the box."""
        exc = _ArchiveEnumerationFailed("wrapped")
        exc.__cause__ = _http_error(404)
        assert quarantines(exc) is False


class TestTextIsNeverConsulted:
    def test_a_bare_exception_mentioning_404_does_not_quarantine(self):
        assert quarantines(
            RuntimeError("upload failed: got '404 Not Found' from storage")
        ) is False

    def test_unrelated_exceptions_do_not_quarantine(self):
        assert quarantines(ValueError("boom")) is False
        assert quarantines(MemoryError()) is False

    def test_a_worker_kill_does_not_quarantine(self):
        """Corp_Esplanade's OOM must not also get it quarantined."""
        assert quarantines(ConnectionResetError("connection lost")) is False
