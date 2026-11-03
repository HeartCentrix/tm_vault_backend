"""Naming uses already captured user-scoped evidence, never activity timestamps."""
def test_legacy_producers_require_rollout_enable_but_readers_do_not(monkeypatch):
    from shared.chat_presentation import chat_presentation_enabled
    monkeypatch.delenv('CHAT_PRESENTATION_ENABLED', raising=False)
    assert not chat_presentation_enabled()
    monkeypatch.setenv('CHAT_PRESENTATION_ENABLED', 'true')
    assert chat_presentation_enabled()
    monkeypatch.setenv('CHAT_PRESENTATION_ENABLED', 'false')
    assert not chat_presentation_enabled()


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


def test_refresh_missing_topic_keeps_cache_while_explicit_clear_replaces_it():
    from shared.chat_presentation import captured_topic_observation
    assert captured_topic_observation({'id': 'a'}, {'topic': 'Planning'}) == 'Planning'
    assert captured_topic_observation({'id': 'a', 'topic': None}, {'topic': 'Planning'}) is None
    assert captured_topic_observation({'id': 'a', 'topic': ''}, {'topic': 'Planning'}) == ''
    assert captured_topic_observation({'id': 'a', 'topic': {}}, {'topic': 'Planning'}) == 'Planning'
    assert captured_topic_observation({'id': 'a', 'topic': 'New'}, {'topic': 'Planning'}) == 'New'
