"""Blob shard selection for chat exports.

(Browser-facing SAS signing was removed — chat export downloads stream through the
api-gateway server-side endpoint, which decrypts; a raw SAS URL would bypass that
and could serve ciphertext for an encrypted blob.)
"""
import zlib
from typing import Sequence

from shared.config import settings


def pick_shard(job_id: str, n: int | None = None) -> int:
    n = n if n is not None else settings.chat_export_blob_account_shards
    return zlib.crc32(job_id.encode("utf-8")) % n


def shard_account(idx: int, accounts: Sequence[str] | None = None) -> str:
    accs = list(accounts or settings.chat_export_blob_accounts)
    return accs[idx]
