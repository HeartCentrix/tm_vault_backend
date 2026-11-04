from shared.chat_catalogue_entry import _capture_order


def test_members_of_shared_chats_start_on_different_chats():
    """Railway 2026-11-04: every member walked chats in the same sorted order, so the members
    of a group chat captured it simultaneously and each uploaded the same 180 objects."""
    chats = [f"19:chat-{n}@thread.v2" for n in range(20)]
    alpha, beta = _capture_order(chats, "principal-alpha"), _capture_order(chats, "principal-beta")
    assert sorted(alpha) == sorted(beta) == sorted(chats)
    assert alpha != beta and alpha[0] != beta[0]
    assert alpha == _capture_order(list(reversed(chats)), "principal-alpha")  # stable per member
