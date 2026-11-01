"""Naming uses already captured user-scoped evidence, never activity timestamps."""
def test_current_topic_wins_over_cached_name_and_only_owned_chats_emit_labels():
    from shared.chat_presentation import label_observations
    labels = label_observations({'a': {'displayName': 'Old', 'topic': 'Old'},
        'b': {'displayName': 'Another user chat'}}, owned_chat_ids={'a'},
        current_chats=[{'id': 'a', 'topic': 'New', 'lastUpdatedDateTime': None}])
    assert labels == [{'chat_id': 'a', 'folder_path': 'chats/New',
        'quality': 3, 'provenance': 'current_topic'}]


def test_unknown_topic_keeps_known_name_but_explicit_clear_does_not_repeat_old_topic():
    from shared.chat_presentation import label_observations
    info = {'a': {'displayName': 'Old topic', 'topic': 'Old topic'}}
    unknown = label_observations(info, owned_chat_ids={'a'}, current_chats=[{'id': 'a'}])[0]
    assert unknown['folder_path'] == 'chats/Old topic'
    assert unknown['provenance'] == 'cached_topic'
    cleared = label_observations(info, owned_chat_ids={'a'}, current_chats=[{'id': 'a', 'topic': None}])[0]
    assert cleared['folder_path'] != 'chats/Old topic'
    assert cleared['provenance'] == 'current_topic_cleared'


def test_already_resolved_user_specific_and_late_author_names_stay_separate():
    from shared.chat_presentation import label_observations
    info = {'alice': {'displayName': 'Bob', 'topic': None, 'memberNames': ['Alice', 'Bob']},
        'bob': {'displayName': 'Alice', 'topic': None, 'memberNames': ['Alice', 'Bob']},
        'late': {'displayName': 'Former colleague', 'nameSource': 'message_author'}}
    labels = {l['chat_id']: l for l in label_observations(info, owned_chat_ids=set(info))}
    assert labels['alice']['folder_path'] == 'chats/Bob'
    assert labels['bob']['folder_path'] == 'chats/Alice'
    assert labels['late']['quality'] == 1
