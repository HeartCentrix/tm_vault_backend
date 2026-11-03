"""Reaction presentation fields rotate between Graph reads (observed on a real tenant,
2026-11-04: ``reactions[].user.user.tenantId`` null vs the tenant id, and
``reactions[].displayName`` changing) and must not make two observation passes of an
unchanged message disagree. Stored payloads stay raw; only the observation set is normalized."""
import os
import copy

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

from shared.chat_catalogue_identity import observation_payload


def _message(tenant, display):
    return {"id": "1", "body": {"content": "hi"},
            "from": {"user": {"id": "sender", "tenantId": "t-sender", "displayName": "Sender"}},
            "reactions": [{"reactionType": "like", "createdDateTime": "2025-11-13T17:20:41.575Z",
                           "displayName": display,
                           "user": {"application": None, "device": None,
                                    "user": {"id": "u1", "userIdentityType": "aadUser", "tenantId": tenant,
                                             "displayName": display}}}]}


def test_reaction_tenant_and_display_name_do_not_change_observation_identity():
    a, b = _message("t-1", "Alice"), _message(None, None)
    assert observation_payload(a, 2) == observation_payload(b, 2)


def test_reaction_identity_and_sender_tenant_still_matter():
    a = _message("t-1", "Alice")
    other_user = copy.deepcopy(a)
    other_user["reactions"][0]["user"]["user"]["id"] = "u2"
    assert observation_payload(a, 2) != observation_payload(other_user, 2)
    other_sender_tenant = copy.deepcopy(a)
    other_sender_tenant["from"]["user"]["tenantId"] = "t-other"
    assert observation_payload(a, 2) != observation_payload(other_sender_tenant, 2)


def test_raw_payload_is_not_mutated_and_v1_stays_raw():
    a = _message("t-1", "Alice")
    before = copy.deepcopy(a)
    observation_payload(a, 2)
    assert a == before
    assert observation_payload(a, 1)["reactions"][0]["user"]["user"]["tenantId"] == "t-1"
