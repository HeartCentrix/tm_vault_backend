"""Execute the actual nested parallel drain at its provider deferral boundary."""
import ast
import asyncio
from pathlib import Path

import pytest
from shared.provider_admission import ProviderDeferred, ProviderRequestContext
from shared.provider_runtime import ProviderTurnDeferred


async def test_parallel_mail_drain_propagates_deferral_instead_of_reporting_folder_failure():
    tree = ast.parse((Path(__file__).resolve().parents[2] / 'workers/backup-worker/main.py').read_text())
    node = next(n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef)
                and n.name == '_drain_mail_parallel')
    deferred = ProviderTurnDeferred(ProviderDeferred(
        ProviderRequestContext('tenant', 'app', 'mail'), 1))
    async def drain(fid):
        if fid == 'waiting':
            raise deferred
        return ['saved-message']
    namespace = {'asyncio': asyncio, 'mail_folder_ids': ['done', 'waiting'],
                 '_drain_one_folder': drain, 'out': []}
    exec(compile(ast.Module(body=[node], type_ignores=[]), '<mail-drain>', 'exec'), namespace)
    with pytest.raises(ProviderTurnDeferred):
        await namespace['_drain_mail_parallel']()
