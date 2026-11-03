"""Exercise real nested chat pipelines at their Graph, storage, and DB seams."""
from __future__ import annotations

import ast
import copy
import importlib.util
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

os.environ.setdefault('ALLOW_DEV_JWT_SECRETS', 'true')

MAIN = Path(__file__).resolve().parents[2] / 'workers/backup-worker/main.py'


@pytest.fixture(scope='module')
def worker():
    spec = importlib.util.spec_from_file_location('bw_interleave_persistence_honesty', MAIN)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def pipeline(worker):
    tree = ast.parse(MAIN.read_text())
    find = lambda name: copy.deepcopy(next(n for n in ast.walk(tree)
        if isinstance(n, ast.AsyncFunctionDef) and n.name == name))
    kick_hc, kick_att, finalizer = map(find, (
        '_kick_hosted_content_for_chat', '_kick_attachments_for_chat', '_hc_drain_finalizer'))
    outer = find('_backup_user_content_parallel')
    phase2 = next(n for n in ast.walk(outer) if isinstance(n, ast.If) and any(
        isinstance(s, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'chats_with_att'
        for t in s.targets) for s in n.body))
    fetch = next(n for n in ast.walk(outer) if isinstance(n, ast.AsyncFunctionDef) and n.name == '_fetch_for')
    barrier = next(n.orelse for n in ast.walk(fetch) if isinstance(n, ast.If) and any(
        isinstance(s, ast.AsyncFunctionDef) and s.name == '_hc_drain_finalizer' for s in n.body))
    env = vars(worker).copy()
    snapshot = SimpleNamespace(id=uuid4(), extra_data={}, status=worker.SnapshotStatus.COMPLETED)
    resource = SimpleNamespace(type=worker.ResourceType.USER_CHATS, external_id='user', extra_data={})
    tenant = SimpleNamespace(id=uuid4())
    observed = {'writes': [], 'hosted': [], 'attachments': []}

    async def hosted(**kwargs):
        observed['hosted'].append(kwargs['message_id'])
        return 1, 7

    async def attachments(graph, resource, snapshot, tenant, messages, **kwargs):
        observed['attachments'].extend(m['id'] for m in messages)
        return [], 0

    class Session:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def get(self, *args): return snapshot
        async def execute(self, statement): observed['writes'].append(statement.compile().params)
        async def commit(self): pass

    bw = SimpleNamespace(worker_id='test', _userchats_backup_hosted_contents=hosted,
        _userchats_backup_attachments=attachments, _stamp_snapshot_degraded=AsyncMock())
    env.update(self=bw, snapshot=snapshot, resource=resource, tenant=tenant, user_id='user',
        graph_client=SimpleNamespace(batch=AsyncMock(return_value={}), GRAPH_URL='https://graph.invalid',
            _get=AsyncMock(return_value={'value': []})), items_data=[],
        _hc_sem=worker.asyncio.Semaphore(1), _att_chat_sem=worker.asyncio.Semaphore(1),
        _hc_failed_msgs=worker.BoundedInterleaveFailureLog(2),
        _att_failed_msgs=worker.BoundedInterleaveFailureLog(2),
        _hc_interleaved_msg_ids=set(), _att_interleaved_msg_ids=set(), _att_dedup=None,
        _detached_hc_tasks=[], _detached_att_tasks=[], _hc_tasks=[], _att_tasks=[],
        _interleave_hc=True, _interleave_att=True, _snap_id_for_finalize=snapshot.id,
        _worker_id_for_finalize='test', _hc_bytes_baseline=0, _hc_items_baseline=0,
        _att_bytes_baseline=0, _att_items_baseline=0, async_session_factory=Session,
        _persist_chat_snapshot_items=AsyncMock(return_value=1))
    wrapper = ast.parse('''def build():
    _hc_items_total = _hc_bytes_total = _att_items_total_count = _att_bytes_total = 0
''').body[0]
    phase_fn = ast.parse('''async def phase2():
    att_items, att_bytes, bytes_total, total_hosted_items = [], 0, 0, 0
''').body[0]
    phase_fn.body.extend(copy.deepcopy(phase2.body))
    barrier_fn = ast.parse('async def attached_barrier():\n    pass\n').body[0]
    barrier_fn.body = copy.deepcopy(barrier)
    wrapper.body.extend([kick_hc, kick_att, finalizer, phase_fn, barrier_fn,
        ast.parse('return locals()').body[0]])
    exec(compile(ast.fix_missing_locations(ast.Module(body=[wrapper], type_ignores=[])),
        str(MAIN), 'exec'), env)
    funcs = env['build']()
    return SimpleNamespace(worker=worker, env=env, funcs=funcs, observed=observed, bw=bw,
        snapshot=snapshot, resource=resource, tenant=tenant)


def raw_message(name='message'):
    return {'id': name, 'chatId': 'chat', 'hostedContents': [{'id': 'inline'}],
            'attachments': [{'id': 'attachment'}]}


def persistence_error():
    error = RuntimeError('pointer transaction exhausted')
    error._chat_snapshot_persistence_failed = True
    return error


def set_phase2_messages(pipeline, messages):
    pipeline.env['items_data'] = [('TEAMS_CHAT_MESSAGE', 'name', m['id'], {'raw': m}) for m in messages]


async def test_phase2_hosted_persistence_exception_prevents_clean_finalize(pipeline):
    message = raw_message()
    error = persistence_error()
    pipeline.bw._userchats_backup_hosted_contents = AsyncMock(side_effect=error)
    set_phase2_messages(pipeline, [message])
    with pytest.raises(RuntimeError, match='pointer transaction exhausted'):
        await pipeline.funcs['phase2']()
    assert message['_hc_persistence_failed'] is True
    assert pipeline.bw._stamp_snapshot_degraded.await_args.args[0] == pipeline.snapshot.id


@pytest.mark.parametrize('kind', ['ChatCatalogueWorkLeaseMoved', 'SnapshotVanishedError'])
async def test_phase2_stale_or_deleted_owner_propagates_without_stamping(pipeline, kind):
    error = getattr(pipeline.worker, kind)('owner moved')
    pipeline.bw._userchats_backup_hosted_contents = AsyncMock(side_effect=error)
    set_phase2_messages(pipeline, [raw_message()])
    with pytest.raises(type(error)):
        await pipeline.funcs['phase2']()
    assert pipeline.bw._stamp_snapshot_degraded.await_count == 0


@pytest.mark.parametrize('lane', ['hc', 'att'])
async def test_exhausted_interleave_persistence_is_not_redownloaded_in_phase2(pipeline, lane):
    message = raw_message()
    if lane == 'hc':
        pipeline.bw._userchats_backup_hosted_contents = AsyncMock(side_effect=persistence_error())
        source = pipeline.bw._userchats_backup_hosted_contents
    else:
        item = pipeline.worker.SnapshotItem(snapshot_id=pipeline.snapshot.id,
            external_id='attachment', item_type='CHAT_ATTACHMENT', name='attachment')
        pipeline.bw._userchats_backup_attachments = AsyncMock(return_value=([item], 7))
        pipeline.env['_persist_chat_snapshot_items'] = AsyncMock(side_effect=persistence_error())
        source = pipeline.bw._userchats_backup_attachments
    await pipeline.funcs['_kick_hosted_content_for_chat' if lane == 'hc' else '_kick_attachments_for_chat']('chat', [message])
    assert message[f'_{lane}_persistence_failed'] is True
    assert len(pipeline.env[f'_{lane}_failed_msgs']) == 1
    set_phase2_messages(pipeline, [message])
    with pytest.raises(Exception, match='persistence'):
        await pipeline.funcs['phase2']()
    assert source.await_count == 1
    assert pipeline.bw._stamp_snapshot_degraded.await_count == 1


@pytest.mark.parametrize('overflow', [False, True])
async def test_finalizer_retries_network_only_and_preserves_persistence_failure_evidence(pipeline, overflow):
    for lane in ('hc', 'att'):
        log = pipeline.env[f'_{lane}_failed_msgs']
        failed = raw_message(f'{lane}-database')
        failed[f'_{lane}_persistence_failed'] = True
        log.append(('chat', failed))
        log.append(('chat', raw_message(f'{lane}-network')))
        if overflow:
            log.append(('chat', raw_message(f'{lane}-dropped')))
    await pipeline.funcs['_hc_drain_finalizer']()
    assert pipeline.observed['hosted'] == ['hc-network']
    assert pipeline.observed['attachments'] == ['att-network']
    written, = pipeline.observed['writes']
    assert written['hc_drain_status'] == 'FAILED'
    assert written['status'] == pipeline.worker.SnapshotStatus.PARTIAL
    reason = written['extra_data']['failure_reason']
    assert 'lost hostedContent' in reason
    assert 'lost attachments' in reason
    if overflow:
        assert 'CHAT_INTERLEAVE_MAX_RETAINED_FAILURES' in reason


@pytest.mark.parametrize('lane', ['hc', 'att'])
async def test_interleave_stale_owner_is_not_retained_as_network_failure(pipeline, lane):
    error = pipeline.worker.ChatCatalogueWorkLeaseMoved('owner moved')
    if lane == 'hc':
        pipeline.bw._userchats_backup_hosted_contents = AsyncMock(side_effect=error)
    else:
        pipeline.bw._userchats_backup_attachments = AsyncMock(side_effect=error)
    with pytest.raises(type(error)):
        await pipeline.funcs['_kick_hosted_content_for_chat' if lane == 'hc' else '_kick_attachments_for_chat']('chat', [raw_message()])
    assert len(pipeline.env[f'_{lane}_failed_msgs']) == 0


@pytest.mark.parametrize('barrier,lane', [('_hc_drain_finalizer', 'hc'), ('_hc_drain_finalizer', 'att'),
                                        ('attached_barrier', 'hc'), ('attached_barrier', 'att')])
async def test_barrier_stale_owner_propagates_without_writing_new_owner(pipeline, barrier, lane):
    async def stale():
        raise pipeline.worker.ChatCatalogueWorkLeaseMoved('owner moved')
    pipeline.env[f'_{"detached_" if barrier == "_hc_drain_finalizer" else ""}{lane}_tasks'].append(stale())
    with pytest.raises(pipeline.worker.ChatCatalogueWorkLeaseMoved):
        await pipeline.funcs[barrier]()
    assert pipeline.observed['writes'] == []


async def test_ordinary_hosted_network_failure_keeps_one_source_retry(pipeline):
    messages = [raw_message()]
    calls = []

    async def network_then_success(**kwargs):
        calls.append(kwargs['message_id'])
        if len(calls) == 1:
            raise ConnectionError('Graph unavailable')
        return 1, 7

    pipeline.bw._userchats_backup_hosted_contents = network_then_success
    await pipeline.funcs['_kick_hosted_content_for_chat']('chat', messages)
    assert not messages[0].get('_hc_persistence_failed')
    await pipeline.funcs['_hc_drain_finalizer']()
    assert calls == ['message', 'message']
    assert pipeline.observed['writes'][0]['hc_drain_status'] == 'COMPLETE'
