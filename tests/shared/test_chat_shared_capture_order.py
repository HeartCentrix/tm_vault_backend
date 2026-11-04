from shared.chat_catalogue_entry import _capture_order


def test_members_of_shared_chats_start_on_different_chats():
    """Railway 2026-11-04: every member walked chats in the same sorted order, so the members
    of a group chat captured it simultaneously and each uploaded the same 180 objects."""
    chats = [f"19:chat-{n}@thread.v2" for n in range(20)]
    alpha, beta = _capture_order(chats, "principal-alpha"), _capture_order(chats, "principal-beta")
    assert sorted(alpha) == sorted(beta) == sorted(chats)
    assert alpha != beta and alpha[0] != beta[0]
    assert alpha == _capture_order(list(reversed(chats)), "principal-alpha")  # stable per member


def group(*members):
    return {"id": "19:group@thread.v2", "chatType": "group", "members": [
        {"userId": user, "visibleHistoryStartDateTime": start} for user, start in members]}


def test_member_who_sees_the_most_history_captures_a_shared_chat():
    """Railway 2026-11-04: 71 of 77 repeat chat captures were a member who joined earlier
    re-capturing a chat a later member had already captured (a later member's capture
    lacks the earlier history, so it cannot be copied). Every member's worker picks the
    same capturer from the chat's member list: the earliest join among members backed up
    in this run."""
    from shared.chat_catalogue_entry import _capture_owner
    chat = group(("X", "0001-01-01T00:00:00Z"), ("Y", "2026-03-01T00:00:00Z"), ("Z", "2026-06-01T00:00:00Z"))
    run = {"x", "y", "z"}
    assert [_capture_owner(chat, me, run - {me.lower()}) for me in ("X", "Y", "Z")] == ["x", "x", "x"]
    assert _capture_owner(chat, "Z", {"y"}) == "y"      # X not backed up in this run
    assert _capture_owner(chat, "Z", set()) == "z"      # nobody else: capture it yourself


def test_members_with_the_same_history_agree_on_one_capturer():
    from shared.chat_catalogue_entry import _capture_owner
    chat = group(("X", "0001-01-01T00:00:00Z"), ("W", "0001-01-01T00:00:00Z"))
    assert _capture_owner(chat, "X", {"w"}) == _capture_owner(chat, "W", {"x"})


def test_a_member_captures_the_shared_chats_others_wait_for_first():
    chats = [f"19:chat-{n}@thread.v2" for n in range(20)]
    owned = {chats[7], chats[13]}
    order = _capture_order(chats, "principal-alpha", first=owned)
    assert set(order[:2]) == owned and sorted(order) == sorted(chats)
    assert order[2:] == [c for c in _capture_order(chats, "principal-alpha") if c not in owned]


def test_someone_who_left_the_chat_never_captures_it_for_members():
    """Railway 2026-11-04: a chat still in Narendra's list no longer listed him as a member. His
    worker picked him by the tie hash, Ranjeet's worker (not seeing him) picked Ranjeet, and the
    copy check refuses a non-member donor, so both captured. A current member captures; the
    former member copies from it."""
    from shared.chat_catalogue_entry import _capture_owner
    chat = group(("R", "0001-01-01T00:00:00Z"), ("Q", "0001-01-01T00:00:00Z"))
    for left in ("n1", "n2", "n3", "n4"):  # whatever the tie hash would say
        assert _capture_owner(chat, left, {"r"}) == "r" == _capture_owner(chat, "R", {left})
    assert _capture_owner(chat, "N1", set()) == "n1"  # no member in this run: capture it yourself
