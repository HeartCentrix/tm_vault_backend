"""Large archive opaque data never needs a whole base64 string in memory."""
import base64
import hashlib
import json
from pathlib import Path
import pytest


def parse(tmp_path, raw, step):
    from shared.archive_spool import ExportSpoolDecoder
    with ExportSpoolDecoder(tmp_path, metadata_limit=1024 * 1024) as parser:
        for offset in range(0, len(raw), step):
            parser.feed(raw[offset:offset + step])
        return parser.finish()


@pytest.mark.parametrize('step', [1, 2, 3, 4, 7, 64, 65536])
def test_every_boundary_and_escaped_base64_preserves_exact_bytes(tmp_path, step):
    payload = bytes(range(256)) * 7
    encoded = base64.b64encode(payload).decode().replace('/', '\\/').replace('+', '\\u002b')
    raw = ('{"value":[{"id":"item", "changeKey":"v1", "data":"' + encoded + '"}]}').encode()
    result = parse(tmp_path, raw, step)
    data = result['value'][0]['data']
    assert Path(data.path).read_bytes() == payload
    assert data.size == len(payload)
    assert data.sha256 == hashlib.sha256(payload).hexdigest()


@pytest.mark.parametrize('raw', [
    b'{"value":[{"data":"YWJj',
    b'{"value":[{"data":"YQ==Yg=="}]}',
    b'{"value":[{"data":"Y!Jj"}]}',
    b'{"value":[{"data":"YQ"}]}',
    b'{"value":[{"data":"YWJj","data":"ZA=="}]}',
    b'{"value":[{"data":"YWJj"}] trailing',
    b'{"value":[{"data":"YWJj\\n"}]}',
])
def test_malformed_export_cannot_publish_a_spooled_receipt(tmp_path, raw):
    from shared.provider_read import ProviderReadError
    with pytest.raises(ProviderReadError):
        parse(tmp_path, raw, 1)


def test_large_data_does_not_use_metadata_budget(tmp_path):
    from shared.archive_spool import ExportSpoolDecoder
    block = b'abc' * 16384
    chunk = base64.b64encode(block)
    expected = hashlib.sha256()
    with ExportSpoolDecoder(tmp_path, metadata_limit=128) as parser:
        parser.feed(b'{"value":[{"id":"x","data":"')
        for _ in range(512):
            parser.feed(chunk)
            expected.update(block)
        parser.feed(b'"}]}')
        result = parser.finish()['value'][0]['data']
        assert result.size == len(block) * 512
        assert result.sha256 == expected.hexdigest()
        assert parser.peak_buffer_bytes < 140000


def test_non_data_metadata_limit_still_applies(tmp_path):
    from shared.provider_read import ProviderReadError
    with pytest.raises(ProviderReadError):
        from shared.archive_spool import ExportSpoolDecoder
        with ExportSpoolDecoder(tmp_path, metadata_limit=128) as parser:
            parser.feed(b'{"message":"' + b'x' * 500 + b'"}')
            parser.finish()
