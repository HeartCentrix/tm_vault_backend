"""Archive→EML/MBOX/PST EXPORT staging engine
(workers/restore-worker/archive_export.py).

Opaque archive blobs can't be parsed, so export stages them: import into a temp
folder of the user's PRIMARY mailbox → read back as normal Graph JSON → empty →
repeat in bounded batches → GUARANTEED delete of the temp folder at the end
(never leave archive data in the live mailbox). The live Graph ops are injected
so the batching loop + the cleanup guarantee are unit-tested with fakes.

workers/restore-worker has a hyphen → put on sys.path (same as other worker tests)."""
from __future__ import annotations

import pathlib
import sys

import pytest

_ROOT = pathlib.Path(__file__).resolve().parents[2]
_WORKER = _ROOT / "workers" / "restore-worker"
for _p in (str(_ROOT), str(_WORKER)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import archive_export as ae  # noqa: E402


class FakeOps:
    """Records every live op the engine performs; canned message reads."""

    def __init__(self, messages_per_batch=None, fail_read_on=None):
        self.calls = []
        self._msgs = messages_per_batch or {}   # batch_index -> [graph msgs]
        self._fail_read_on = fail_read_on
        self._read_idx = 0
        self._import_n = 0

    async def create_temp_folder(self):
        self.calls.append(("create_temp_folder",))
        return "STD-TMP"

    async def resolve_ie_folder_id(self, std_id):
        self.calls.append(("resolve_ie", std_id))
        return "IE-TMP"

    async def create_import_session(self):
        self.calls.append(("import_session",))
        return ("https://o/import", "2999-01-01T00:00:00Z")

    async def import_blob(self, url, folder_ie_id, blob):
        self.calls.append(("import", folder_ie_id, blob))
        self._import_n += 1
        return f"NEW{self._import_n}"  # scratch-mailbox item id assigned on import

    async def read_messages(self, std_id):
        idx = self._read_idx
        self._read_idx += 1
        self.calls.append(("read", std_id, idx))
        if self._fail_read_on is not None and idx == self._fail_read_on:
            raise RuntimeError("graph read failed")
        for m in self._msgs.get(idx, []):
            yield m

    async def empty_folder(self, std_id):
        self.calls.append(("empty", std_id))

    async def delete_folder(self, std_id):
        self.calls.append(("delete", std_id))


class _Item:
    def __init__(self, ext_id, blob=b"OPAQUE"):
        self.external_id = ext_id
        self._blob = blob


async def _dl(it):
    return it._blob


def _collect(engine, items):
    async def _run():
        return [m async for m in engine.stage_and_read(items)]
    return _run


@pytest.mark.asyncio
async def test_happy_path_batches_stage_read_empty_and_cleanup():
    ops = FakeOps(messages_per_batch={0: [{"id": "a"}, {"id": "b"}], 1: [{"id": "c"}]})
    eng = ae.ArchiveExportStager(ops=ops, download_blob=_dl, batch_size=2)
    msgs = [m async for m in eng.stage_and_read([_Item("a"), _Item("b"), _Item("c")])]

    assert [m["id"] for m in msgs] == ["a", "b", "c"]
    kinds = [c[0] for c in ops.calls]
    assert kinds.count("create_temp_folder") == 1
    assert kinds.count("resolve_ie") == 1
    assert kinds.count("import_session") == 2      # one per batch (2 batches)
    assert kinds.count("import") == 3              # one per item
    assert kinds.count("read") == 2                # one per batch
    assert kinds.count("empty") == 2               # footprint bounded per batch
    assert kinds.count("delete") == 1              # temp folder removed at end
    # delete is the LAST op (cleanup after everything)
    assert ops.calls[-1][0] == "delete"


@pytest.mark.asyncio
async def test_temp_folder_is_deleted_even_when_a_read_fails_midway():
    # The security-critical guarantee: an error mid-stream must NOT leave the
    # imported archive data sitting in the user's live mailbox.
    ops = FakeOps(messages_per_batch={0: [{"id": "a"}]}, fail_read_on=0)
    eng = ae.ArchiveExportStager(ops=ops, download_blob=_dl, batch_size=2)

    with pytest.raises(RuntimeError):
        [m async for m in eng.stage_and_read([_Item("a"), _Item("b")])]

    assert ("delete", "STD-TMP") in ops.calls          # cleanup still happened
    assert ops.calls[-1] == ("delete", "STD-TMP")      # and it was the final act


@pytest.mark.asyncio
async def test_items_with_no_blob_are_skipped_and_empty_batch_skips_read():
    ops = FakeOps(messages_per_batch={})
    eng = ae.ArchiveExportStager(ops=ops, download_blob=_dl, batch_size=5)
    # both items download to empty -> nothing imported -> no read/empty for the batch
    out = [m async for m in eng.stage_and_read([_Item("a", blob=b""), _Item("b", blob=None)])]

    assert out == []
    kinds = [c[0] for c in ops.calls]
    assert kinds.count("import") == 0
    assert kinds.count("read") == 0        # empty batch -> no read
    assert kinds.count("empty") == 0
    assert kinds.count("delete") == 1      # still cleaned up


def test_chunks_helper():
    assert list(ae._chunks([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]]
    assert list(ae._chunks([], 3)) == []


# ── stage_and_map: enrichment needs (source_item -> graph_json) matching ──

@pytest.mark.asyncio
async def test_stage_and_map_matches_staged_json_back_to_source_item():
    # imports assign NEW1/NEW2; the temp-folder read returns messages carrying
    # those ids, so each staged message must map back to its ORIGINAL item.
    ops = FakeOps(messages_per_batch={
        0: [{"id": "NEW1", "subject": "first"}, {"id": "NEW2", "subject": "second"}],
    })
    eng = ae.ArchiveExportStager(ops=ops, download_blob=_dl, batch_size=10)
    a, b = _Item("A"), _Item("B")
    pairs = [(src.external_id, msg) async for src, msg in eng.stage_and_map([a, b])]

    # source A imported first -> NEW1 -> "first"; B -> NEW2 -> "second"
    assert pairs == [("A", {"id": "NEW1", "subject": "first"}),
                     ("B", {"id": "NEW2", "subject": "second"})]
    assert ("delete", "STD-TMP") == ops.calls[-1]  # cleanup still guaranteed


@pytest.mark.asyncio
async def test_stage_and_map_skips_unmatched_and_no_blob_items():
    # C downloads empty -> not imported; a stray staged msg with unknown id is dropped
    ops = FakeOps(messages_per_batch={
        0: [{"id": "NEW1", "subject": "ok"}, {"id": "GHOST", "subject": "orphan"}],
    })
    eng = ae.ArchiveExportStager(ops=ops, download_blob=_dl, batch_size=10)
    got = [(s.external_id, m["subject"]) async for s, m in eng.stage_and_map([_Item("A"), _Item("C", blob=b"")])]
    assert got == [("A", "ok")]  # C skipped (no blob); GHOST dropped (no source)


def test_message_to_eml_reuses_shared_builders_with_file_and_nested():
    import base64
    import email

    msg = {
        "subject": "STAGED",
        "from": {"emailAddress": {"address": "a@x.com"}},
        "body": {"contentType": "text", "content": "hello from archive"},
        "attachments": [
            {
                "@odata.type": "#microsoft.graph.fileAttachment",
                "name": "doc.pdf", "contentType": "application/pdf",
                "contentBytes": base64.b64encode(b"%PDF-1.4 x").decode(),
            },
            {
                "@odata.type": "#microsoft.graph.itemAttachment",
                "name": "nested",
                "item": {
                    "subject": "INNER-MAIL",
                    "from": {"emailAddress": {"address": "b@y.com"}},
                    "body": {"contentType": "text", "content": "inner"},
                },
            },
        ],
    }
    eml = ae.message_to_eml(msg)
    parsed = email.message_from_bytes(eml)
    assert parsed["subject"] == "STAGED"
    assert b"hello from archive" in eml
    # inline file attachment rode through
    assert b"doc.pdf" in eml
    # mail-in-mail: nested email embedded as message/rfc822 with its own subject
    assert any(p.get_content_type() == "message/rfc822" for p in parsed.walk())
    assert b"INNER-MAIL" in eml
