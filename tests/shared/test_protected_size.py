"""Database-global capture names must remain distinct for isolated schemas."""

from shared.protected_size_schema import event_trigger_names


def test_event_trigger_names_fit_postgres_limit_for_max_length_schema():
    names = event_trigger_names("x" * 63)
    assert len(set(names)) == 2
    assert all(len(name.encode()) <= 63 for name in names)
    assert names == event_trigger_names("x" * 63)


def test_schema_suffix_changes_global_event_trigger_names():
    first = event_trigger_names("x" * 62 + "a")
    second = event_trigger_names("x" * 62 + "b")
    assert not set(first).intersection(second)
