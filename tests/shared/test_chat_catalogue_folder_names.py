"""Catalogue chat folders must carry the people, not a hash. The legacy chat path named
topic-less chats from members; the catalogue path only had `topic` and fell back to
"Chat <sha8>" (Railway, 2026-11-04: 'chats/Chat 175b7c6b'). Members come from the
principal-scoped chat list ($expand=members)."""
import os

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

from shared.chat_catalogue_entry import chat_folder

ME = "11111111-1111-1111-1111-111111111111"


def member(name, uid):
    return {"@odata.type": "#microsoft.graph.aadUserConversationMember", "displayName": name, "userId": uid}


def test_topic_wins():
    chat = {"id": "19:a@thread.v2", "topic": " TM Devs ", "chatType": "group", "members": [member("Me", ME), member("Rohit", "2")]}
    assert chat_folder(chat, ME) == "chats/TM Devs"


def test_one_on_one_is_named_after_the_other_person():
    chat = {"id": "19:b@unq.gbl.spaces", "topic": None, "chatType": "oneOnOne", "members": [member("Me", ME), member("Rohit Sharma", "2")]}
    assert chat_folder(chat, ME) == "chats/Rohit Sharma"


def test_group_without_topic_lists_the_other_members():
    chat = {"id": "19:c@thread.v2", "topic": None, "chatType": "group",
            "members": [member("Me", ME), member("Akshat", "2"), member("Harsh", "3"), member("Rashmi", "4")]}
    assert chat_folder(chat, ME) == "chats/Akshat, Harsh, Rashmi"


def test_long_member_lists_are_truncated_with_a_count():
    names = [member(f"Person Number {i:02d}", str(i)) for i in range(1, 12)]
    chat = {"id": "19:d@thread.v2", "topic": None, "chatType": "group", "members": [member("Me", ME), *names]}
    folder = chat_folder(chat, ME)
    assert folder.startswith("chats/Person Number 01, ") and folder.endswith(" more") and len(folder) <= 96


def test_no_members_falls_back_to_the_hash_and_slashes_are_not_paths():
    assert chat_folder({"id": "19:e@thread.v2", "topic": None, "chatType": "oneOnOne"}, ME).startswith("chats/Chat ")
    chat = {"id": "19:f@unq.gbl.spaces", "topic": None, "chatType": "oneOnOne", "members": [member("Me", ME), member("A/B", "2")]}
    assert chat_folder(chat, ME) == "chats/A-B"


def test_visible_history_start_reads_the_principal_membership():
    from datetime import datetime, timezone
    from shared.chat_catalogue_entry import visible_history_start
    chat = {"members": [member("Other", "2") | {"visibleHistoryStartDateTime": "2030-01-01T00:00:00Z"},
                        member("Me", ME) | {"visibleHistoryStartDateTime": "2026-09-01T16:07:24.118Z"}]}
    assert visible_history_start(chat, ME.upper()) == datetime(2026, 9, 1, 16, 7, 24, 118000, tzinfo=timezone.utc)
    assert visible_history_start({"members": [member("Me", ME) | {"visibleHistoryStartDateTime": "0001-01-01T00:00:00Z"}]}, ME) is None
    assert visible_history_start({"members": [member("Other", "2")]}, ME) is None
    assert visible_history_start({"members": [member("Me", ME) | {"visibleHistoryStartDateTime": "garbage"}]}, ME) is None
