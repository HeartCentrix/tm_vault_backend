"""Bounded lexical extraction of exportItems base64 data into temporary files.

The complete remaining JSON envelope still passes the standard JSON decoder.
Only string values following the exact JSON key `data` are replaced. Duplicate
keys, malformed escapes, invalid base64 and truncated envelopes fail closed.
The caller owns a temporary directory for the whole upload/receipt lifetime.
"""
from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
import shutil
from uuid import uuid4

from shared.provider_read import ProviderReadError

_SPECIAL = re.compile(rb'["\\]')


@dataclass(frozen=True)
class SpooledArchiveData:
    path: str
    size: int
    sha256: str


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate export property')
        result[key] = value
    return result


class ExportSpoolDecoder:
    def __init__(self, directory, *, metadata_limit=4 * 1024 * 1024):
        self.directory = Path(directory)
        self.metadata_limit = metadata_limit
        self.envelope, self.between, self.string, self.pending = bytearray(), bytearray(), bytearray(), b''
        self.in_string, self.data_string, self.last_data_key = False, False, False
        self.escape, self.file = None, None
        self.records, self.marker = {}, None
        self.size, self.padded, self.closed = 0, False, False
        self.peak_buffer_bytes = 0
        self._space_check = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        if self.file is not None:
            self.file.close()
            self.file = None
        self.closed = True

    def _metadata(self, data):
        if len(self.envelope) + len(data) > self.metadata_limit:
            raise ProviderReadError('archive_export_metadata_limit')
        self.envelope.extend(data)

    def _data(self, data):
        if not data:
            return
        if self.padded:
            raise ProviderReadError('archive_export_invalid_base64')
        block = self.pending + data
        self.peak_buffer_bytes = max(self.peak_buffer_bytes, len(self.envelope) + len(self.string) + len(block))
        end = len(block) // 4 * 4
        self.pending = block[end:]
        if not end:
            return
        try:
            decoded = base64.b64decode(block[:end], validate=True)
        except (ValueError, binascii.Error):
            raise ProviderReadError('archive_export_invalid_base64') from None
        if b'=' in block[:end]:
            self.padded = True
            if self.pending:
                raise ProviderReadError('archive_export_invalid_base64')
        # Preserve working space; no content-size cap is imposed. Concurrency
        # is bounded by the owning export pool, and each write is one chunk.
        if self.size >= self._space_check:
            if shutil.disk_usage(self.directory).free < len(decoded) + 256 * 1024 * 1024:
                raise ProviderReadError('archive_spool_capacity', retry_after=30)
            self._space_check = self.size + 4 * 1024 * 1024
        try:
            self.file.write(decoded)
        except OSError:
            raise ProviderReadError('archive_spool_write_failed', retry_after=30) from None
        self.digest.update(decoded)
        self.size += len(decoded)

    def _piece(self, data):
        if self.data_string:
            self._data(data)
        else:
            self._metadata(data)
            self.string.extend(data)

    def feed(self, chunk):
        if self.closed:
            raise ProviderReadError('archive_export_decoder_closed')
        # An adversarial caller cannot cause a second whole-response allocation.
        for start in range(0, len(chunk), 65536):
            self._feed(chunk[start:start+65536])

    def _feed(self, chunk):
        pos = 0
        while pos < len(chunk):
            if not self.in_string:
                quote = chunk.find(b'"', pos)
                end = len(chunk) if quote < 0 else quote
                segment = chunk[pos:end]
                self._metadata(segment)
                self.between.extend(segment)
                if quote < 0:
                    return
                self.data_string = self.last_data_key and self.between.strip() == b':'
                self.between.clear()
                self.string.clear()
                self.in_string = True
                if self.data_string:
                    if len(self.records) >= 20:
                        raise ProviderReadError('archive_export_item_limit')
                    self.marker = 'archive-spool-' + uuid4().hex
                    self.file = (self.directory / self.marker).open('xb')
                    self.digest, self.size, self.padded, self.pending = hashlib.sha256(), 0, False, b''
                    self._space_check = 0
                    self._metadata(json.dumps(self.marker).encode())
                else:
                    self._metadata(b'"')
                pos = quote + 1
                continue
            if self.escape is not None:
                self.escape += bytes([chunk[pos]])
                pos += 1
                needed = 6 if self.escape.startswith(b'\\u') else 2
                if len(self.escape) < needed:
                    continue
                try:
                    decoded = json.loads(b'"' + self.escape + b'"')
                    if self.data_string:
                        self._data(decoded.encode('ascii'))
                    else:
                        self._piece(self.escape)
                except (ValueError, UnicodeError):
                    raise ProviderReadError('archive_export_invalid_escape') from None
                self.escape = None
                continue
            match = _SPECIAL.search(chunk, pos)
            if match is None:
                self._piece(chunk[pos:])
                return
            self._piece(chunk[pos:match.start()])
            pos = match.end()
            if match.group() == b'\\':
                self.escape = b'\\'
                continue
            if self.data_string:
                if self.pending:
                    raise ProviderReadError('archive_export_invalid_base64')
                self.file.close()
                self.file = None
                self.records[self.marker] = SpooledArchiveData(
                    str(self.directory / self.marker), self.size, self.digest.hexdigest())
                self.last_data_key = False
            else:
                self._metadata(b'"')
                try:
                    self.last_data_key = json.loads(b'"' + self.string + b'"') == 'data'
                except (ValueError, UnicodeError):
                    raise ProviderReadError('archive_export_invalid_string') from None
            self.in_string = False
            self.between.clear()
            self.string.clear()

    def finish(self):
        if self.in_string or self.escape is not None:
            raise ProviderReadError('archive_export_truncated')
        try:
            envelope = json.loads(self.envelope, object_pairs_hook=_unique_object)
            if not isinstance(envelope, dict) or not isinstance(envelope.get('value'), list):
                raise ValueError
            if len(envelope['value']) > 20:
                raise ValueError
            used = set()
            for entry in envelope['value']:
                if not isinstance(entry, dict):
                    raise ValueError
                data = entry.get('data')
                if isinstance(data, str) and data in self.records:
                    used.add(data)
                    entry['data'] = self.records[data]
            if used != set(self.records):
                raise ValueError
            return envelope
        except (ValueError, UnicodeError):
            raise ProviderReadError('archive_export_malformed_envelope') from None
