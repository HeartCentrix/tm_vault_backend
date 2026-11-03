import pytest
from shared.sharepoint_scope import owns_site_inventory


def test_each_site_has_one_inventory_owner_across_drive_shards():
    assert owns_site_inventory({})
    shards = [{'partitionIndex': i, 'partitionFilter': {'driveIds': [str(i)]}} for i in range(7)]
    assert [owns_site_inventory(s) for s in shards] == [True, False, False, False, False, False, False]
    for message in shards:
        message['partitionFilter']['ownsSiteInventory'] = message['partitionIndex'] == 0
    assert sum(owns_site_inventory(s) for s in reversed(shards)) == 1


def test_unknown_shard_does_not_repeat_or_silently_skip_inventory():
    with pytest.raises(ValueError):
        owns_site_inventory({'partitionFilter': {'driveIds': ['a']}})
