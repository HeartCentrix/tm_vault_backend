"""/contacts/delta rejects $top outright — the baseline must not send it.

Live incident (2026-08-07). Jason: "there are no contacts showing as
backing up for jlucas". True fleet-wide: 465 contacts existed in Exchange
and 2 were captured.

    user              in Graph   captured
    John Lucas             250          2
    Jason Rue              163          0
    Bryan Roeben            37          0
    Riley Stillman          12          0
    Orlando Garcia           4          0
    Carlos Romero            1          0

Every USER_CONTACTS snapshot reported COMPLETED with item_count 0, three
times a day, for weeks.

Root cause is the asymmetry between the two branches of
_fetch_contacts_delta: the RESUME branch passes the saved deltaLink
verbatim with ``params = None``, but the BASELINE branch sent
``{"$top": ..., "$select": ...}``. Graph answers the latter with

    400 Bad Request — "The following parameters are not supported with
    change tracking over the 'Contacts' resource: '$top'"

Proven against live Graph on Bryan Roeben (37 contacts):

    no params                        -> 200
    $top=200 alone                   -> 400
    $select alone                    -> 200
    $top=200 + $select               -> 400   <- what the code sent
    $top=50  + $select               -> 400   (value is irrelevant)

So only the resume path ever worked. A first/baseline walk could never
succeed for anyone: the fetch 400s, _fetch_contacts_delta returns [] and
never sets new_deltas, so no cursor is stored and the next run repeats
the same failing call. Users who DO hold a cursor got it under older
code and are now pinned to incrementals over a baseline that was never
captured — jlucas's 2 rows are simply the two contacts created since.

$top is not needed: Graph pages /contacts/delta via @odata.nextLink on
its own (248 contacts came back over 26 pages with no $top), and the
walk already follows nextLink. $select is supported and is kept — it is
what keeps the payload small now that page size is not ours to set.

This is the same family as the chats $orderby defect: a query option that
is legal on the plain collection but rejected by its /delta sibling.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_contacts_delta", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_contacts_delta"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )

build = _bw.contacts_delta_initial_params

SELECT = "id,displayName,givenName,surname,emailAddresses"


class TestTopIsNeverSent:
    def test_top_is_absent(self):
        """The whole defect in one assertion."""
        assert "$top" not in build(SELECT)

    def test_no_top_under_any_alias_or_casing(self):
        params = build(SELECT)
        assert not any(k.lower().lstrip("$") == "top" for k in params)

    @pytest.mark.parametrize("sel", [SELECT, "id", "", None])
    def test_top_absent_regardless_of_select(self, sel):
        assert "$top" not in build(sel)


class TestSelectIsPreserved:
    def test_select_is_passed_through(self):
        """$select IS supported on /contacts/delta (probed 200) and keeps
        the payload small now that page size is Graph's to choose."""
        assert build(SELECT)["$select"] == SELECT

    def test_empty_select_yields_no_select_key(self):
        """Never send an empty $select — that is its own 400 risk."""
        for empty in ("", None):
            assert "$select" not in build(empty)

    def test_result_is_a_plain_dict_safe_to_pass_as_params(self):
        p = build(SELECT)
        assert isinstance(p, dict)
        assert set(p) == {"$select"}

    def test_callers_cannot_mutate_a_shared_default(self):
        """Two calls must not share one dict — the walk sets params=None
        after page 1, and a shared object would leak across users."""
        a, b = build(SELECT), build(SELECT)
        assert a == b and a is not b
        a["$top"] = "200"
        assert "$top" not in build(SELECT)
