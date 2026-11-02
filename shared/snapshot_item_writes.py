"""Ordering and descriptors shared by direct chat snapshot writers."""
from uuid import UUID, uuid4

from sqlalchemy import inspect
from sqlalchemy.orm.attributes import NO_VALUE

CHAT_ITEM_TYPES = frozenset({"TEAMS_CHAT_MESSAGE", "TEAMS_MESSAGE", "TEAMS_MESSAGE_REPLY",
                             "CHAT_MESSAGE", "CHAT_ATTACHMENT", "CHAT_HOSTED_CONTENT"})


def ordered_snapshot_item_rows(rows):
    """Stable conflict-key order; matches PostgreSQL UUID and C text ordering."""
    return sorted(rows, key=lambda row: (
        UUID(str(row["snapshot_id"])).bytes,
        row["external_id"].encode("utf-8"), row["item_type"].encode("utf-8")))


def snapshot_item_rows(items):
    """Preserve every assigned mapped field without replacing omitted defaults."""
    rows = []
    for item in items:
        state = inspect(item)
        row = {attribute.key: state.attrs[attribute.key].loaded_value
               for attribute in state.mapper.column_attrs
               if state.attrs[attribute.key].loaded_value is not NO_VALUE}
        if row.get("id") is None:
            item.id = row["id"] = uuid4()
        rows.append(row)
    return rows
