"""Bound optional mail-body catalogue writes; primary snapshot data is untouched."""
import json
import logging

logger = logging.getLogger(__name__)


def mail_body_batches(rows, *, max_rows=128, max_bytes=1024 * 1024):
    if max_rows <= 0 or max_bytes <= 0:
        raise ValueError('mail catalogue batch limits must be positive')
    batch, size = [], 0
    for row in rows:
        # Count body_content AND metadata_raw, including JSON escaping.
        row_size = len(json.dumps(row, default=str, separators=(',', ':')).encode('utf-8'))
        if row_size > max_bytes:
            logger.warning('mail_catalogue_optional_row_skipped bytes=%s limit=%s', row_size, max_bytes)
            continue
        if batch and (len(batch) >= max_rows or size + row_size > max_bytes):
            yield batch
            batch, size = [], 0
        batch.append(row)
        size += row_size
    if batch:
        yield batch
