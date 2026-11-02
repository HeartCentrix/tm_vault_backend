"""Persisted ownership for site-wide work alongside drive shards."""

def owns_site_inventory(message):
    message = message or {}
    part = message.get('partitionFilter') or {}
    if part.get('driveIds') is None:
        return True
    if 'ownsSiteInventory' in part:
        if type(part['ownsSiteInventory']) is not bool:
            raise ValueError('invalid SharePoint inventory owner')
        return part['ownsSiteInventory']
    index = message.get('partitionIndex')
    if type(index) is not int or index < 0:
        raise ValueError('SharePoint shard requires its persisted index')
    # Compatibility for queued shards created before explicit ownership.
    return index == 0
