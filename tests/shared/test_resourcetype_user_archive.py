"""Test USER_ARCHIVE resource type enum and Tier-2 classification"""
from shared.models import ResourceType, UI_HIDDEN_TYPES


def test_user_archive_enum_and_tier2():
    """USER_ARCHIVE enum member exists and is classified as Tier-2 UI-hidden"""
    assert ResourceType.USER_ARCHIVE.value == "USER_ARCHIVE"
    assert "USER_ARCHIVE" in UI_HIDDEN_TYPES
