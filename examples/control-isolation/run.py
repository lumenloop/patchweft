#!/usr/bin/env python3
"""Finite localhost control-contention probe using disposable synthetic data."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
import time

from owned_process import run as own_process

ENV = {'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8', 'LC_ALL': 'C.UTF-8',
       'GIT_CONFIG_NOSYSTEM': '1', 'GIT_CONFIG_GLOBAL': '/dev/null',
       'GIT_TERMINAL_PROMPT': '0', 'PYTHONDONTWRITEBYTECODE': '1'}
HERE = Path(__file__).resolve().parent


def save(path, value):
    temporary = path.with_name(path.name + '.partial')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    temporary.replace(path)


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def wait_for(predicate, seconds=20):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(.025)
    raise TimeoutError('fixture condition did not complete')


def job(mode):
    state = Path(os.environ['PATCHWEFT_STATE'])
    save(state / (mode + '.started'), {'attempt': os.environ['PATCHWEFT_ATTEMPT'],
                                     'mode': mode, 'at': time.monotonic()})
    wait_for(lambda: (state / (mode + '.release')).exists(), seconds=90)
    Path('result.json').write_text(json.dumps({'mode': mode, 'passed': True}))


def child(binary, root):
    root.mkdir(parents=True, exist_ok=False)
    state, repo = root / 'state', root / 'source'
    processes, wire, logical = [], [], []
    lock, stop = threading.Lock(), threading.Event()
    monitor = None
    began = time.monotonic()
    result = {'binary_sha256': digest(binary), 'source_sha256': digest(Path(__file__)),
              'started_at': time.time(), 'started_monotonic': began,
              'cpu_affinity': sorted(os.sched_getaffinity(0)),
              'load_average': os.getloadavg(), 'requests': wire, 'logical': logical,
              'completed': False, 'error': None}
    sequence = 0

    def persist():
        with lock:
            save(root / 'observations.json', result)

    def trace(kind, **fields):
        nonlocal sequence
        with lock:
            sequence += 1
            with (root / 'trace.jsonl').open('a') as stream:
                stream.write(json.dumps({'seq': sequence, 'kind': kind, **fields}, sort_keys=True) + '\n')
            return sequence

    persist()

    def execute(argv):
        run = subprocess.run(list(map(str, argv)), env=ENV, capture_output=True, text=True, timeout=30)
        if run.returncode:
            raise RuntimeError(f'fixture command failed: {run.stderr[:2000]}')
        return run.stdout

    def api(*args):
        return json.loads(execute([binary, '--state', state, *args]))

    def sql(query, args=()):
        with sqlite3.connect(state / 'state.sqlite', timeout=5) as db:
            return db.execute(query, args).fetchall()

    def spawn(name, *args):
        with (root / (name + '.log')).open('w') as log:
            process = subprocess.Popen([str(binary), '--state', str(state), *map(str, args)],
                                       env=ENV, stdout=log, stderr=log)
        processes.append(process)
        return process

    def request(who, path, body, key=None):
        # Published literal fixture credentials; no credential store is read.
        token = 'public-control-load-fixture-' + who
        connection = http.client.HTTPConnection(address[0], int(address[1]), timeout=5)
        started = time.monotonic()
        status, value = 0, None
        request_id = trace('wire.started', principal=who, path=path, key=key,
                           request=body, start=started - began)
        try:
            headers = {'authorization': 'Bearer ' + token, 'content-type': 'application/json'}
            if key is not None:
                headers['idempotency-key'] = key
            connection.request('POST', path, json.dumps(body), headers)
            response = connection.getresponse()
            status, value = response.status, json.loads(response.read())
        except (OSError, http.client.HTTPException, ValueError) as error:
            status = 0
            value = {'transport_error': type(error).__name__}
        finally:
            connection.close()
        row = {'id': request_id, 'principal': who, 'path': path, 'key': key, 'request': body,
               'start': started - began, 'end': time.monotonic() - began,
               'status': status, 'response': value}
        with lock:
            wire.append(row)
        trace('wire.completed', **row)
        return status, value

    def command(who, body, key):
        started = time.monotonic()
        trace('logical.started', principal=who, key=key, request=body, start=started - began)
        for retry in range(60):
            if time.monotonic() - started > 60:
                break
            status, value = request(who, '/v1/commands', body, key)
            if status == 200:
                row = {'principal': who, 'key': key, 'start': started - began,
                       'end': time.monotonic() - began, 'response': value, 'tries': retry + 1}
                with lock:
                    logical.append(row)
                trace('logical.completed', **row)
                return value
            if status not in (0, 429, 503, 504):
                trace('logical.failed', principal=who, key=key, status=status, response=value)
                raise RuntimeError(f'command failed: {status}: {value}')
            jitter = int(hashlib.sha256(key.encode()).hexdigest()[:4], 16) % 30 / 1000
            time.sleep(min(.03 * 2 ** retry, 1) + jitter)
        trace('logical.failed', principal=who, key=key, reason='retry budget exhausted')
        raise TimeoutError('logical request retry budget exhausted')

    try:
        api('init', '--slots', 2, '--memory-mb', 512)
        repo.mkdir()
        (repo / 'README').write_text('Synthetic control-contention source.\n')
        def git(*args):
            return execute(['/usr/bin/git', '-C', repo, '-c', 'user.name=Fixture',
                            '-c', 'user.email=fixture@local.invalid', '-c', 'commit.gpgsign=false',
                            '-c', 'core.hooksPath=/dev/null', *args]).strip()
        git('init', '--quiet'); git('add', 'README'); git('commit', '--quiet', '-m', 'fixture')
        result['source_tree'] = git('rev-parse', 'HEAD^{tree}')
        revisions = {who: api('import', '--name', who, '--repo', repo, '--owner', 'fixture')
                     for who in ('noisy', 'quiet')}
        definition = {'version': 1, 'name': 'control-contention', 'certifying': False,
                      'tools': {'python': '/usr/bin/python3', 'fixture': str(Path(__file__).resolve()),
                                'ownership': str(HERE / 'owned_process.py')},
                      'jobs': [{'name': who, 'command': ['python', str(Path(__file__).resolve()), '--job', who],
                                'reuse': 'inflight', 'slots': 1, 'memory_mb': 128,
                                'timeout_seconds': 90, 'artifacts': ['result.json'],
                                'report': {'kind': 'command'}} for who in ('noisy', 'quiet')]}
        save(root / 'profile.json', definition)
        profile = api('register', root / 'profile.json')
        result['profile'] = profile
        result['revisions'] = revisions
        persist()
        # Provision only this new disposable store with public synthetic inputs.
        with sqlite3.connect(state / 'state.sqlite') as db:
            db.executescript('CREATE TABLE IF NOT EXISTS api_principals(id TEXT PRIMARY KEY,body TEXT NOT NULL);'
                             'CREATE TABLE IF NOT EXISTS api_credentials(digest TEXT PRIMARY KEY,principal TEXT NOT NULL);')
            for who in ('noisy', 'quiet'):
                policy = {'id': who, 'projects': [revisions[who]['project']], 'profiles': [profile['id']],
                          'roles': ['read', 'write'], 'max_pending': 1024}
                db.execute('INSERT INTO api_principals VALUES (?,?)', (who, json.dumps(policy)))
                db.execute('INSERT INTO api_credentials VALUES (?,?)',
                           (hashlib.sha256(('public-control-load-fixture-' + who).encode()).hexdigest(), who))
        ready = root / 'ready.json'
        spawn('server', 'serve', '--listen', '127.0.0.1:0', '--ready-file', ready)
        wait_for(ready.exists)
        address = json.loads(ready.read_text())['address'].rsplit(':', 1)
        for i in range(2):
            spawn('worker-' + str(i), 'worker')
        bodies = {who: {'op': 'request', 'revision': revisions[who]['id'],
                        'profile': profile['id'], 'job': who} for who in ('noisy', 'quiet')}
        actions = {who: command(who, bodies[who], who + '-prime')['result']['id']
                   for who in ('quiet', 'noisy')}
        result['actions'] = actions
        wait_for(lambda: sql("SELECT COUNT(*) FROM attempts WHERE state='running'")[0][0] == 2)
        wait_for(lambda: all((state / (who + '.started')).exists() for who in ('noisy', 'quiet')))
        result['job_starts'] = {who: json.loads((state / (who + '.started')).read_text())
                                for who in ('noisy', 'quiet')}
        persist()

        def observe():
            try:
                while not stop.is_set():
                    request('quiet', '/v1/query', {'op': 'projects'})
                    stop.wait(.05)
            except BaseException as error:
                result['observer_error'] = repr(error)
                trace('observer.failed', error=repr(error))
                persist()
        monitor = threading.Thread(target=observe)
        monitor.start()
        time.sleep(.5)
        barrier = threading.Barrier(129)
        def noisy(i):
            barrier.wait(timeout=20)
            return command('noisy', bodies['noisy'], 'flood-' + str(i))
        with ThreadPoolExecutor(max_workers=129) as pool:
            calls = [pool.submit(noisy, i) for i in range(128)]
            result['burst_start'] = time.monotonic() - began
            persist()
            barrier.wait(timeout=20)
            time.sleep(.1)
            result['cancel_start'] = time.monotonic() - began
            persist()
            cancel = pool.submit(command, 'quiet', {'op': 'cancel', 'action': actions['quiet']}, 'quiet-cancel')
            replies = [future.result() for future in calls]
            cancel.result()
            result['burst_end'] = time.monotonic() - began
            persist()
        assert all(reply['result']['id'] == actions['noisy'] for reply in replies)
        wait_for(lambda: sql('SELECT state FROM actions WHERE id=?', (actions['quiet'],))[0][0] == 'canceled')
        # The quiet gate stays closed until explicit cancellation has drained it.
        assert not (state / 'quiet.release').exists()
        wait_for(lambda: sql('SELECT COUNT(*) FROM attempts WHERE action=? AND drained=0', (actions['quiet'],))[0][0] == 0)
        assert not (state / 'quiet.release').exists()
        trace('quiet.drained', at=time.monotonic() - began, gate_closed=True)
        (state / 'noisy.release').touch()
        trace('noisy.released', at=time.monotonic() - began)
        wait_for(lambda: sql('SELECT COUNT(*) FROM attempts WHERE drained=0')[0][0] == 0)
        assert sql('SELECT state FROM actions WHERE id=?', (actions['noisy'],))[0][0] == 'passed'
        assert sql('SELECT COUNT(*) FROM attempts')[0][0] == 2
        time.sleep(.5)
        assert 'observer_error' not in result, 'quiet observer failed'
        result['completed'] = True
    except BaseException as error:
        result['error'] = type(error).__name__ + ': ' + str(error)
    finally:
        persist()
        stop.set()
        cleanup_errors = []
        if monitor is not None:
            monitor.join(timeout=10)
            if monitor.is_alive():
                cleanup_errors.append('quiet observer did not stop')
        if state.exists():
            for who in ('noisy', 'quiet'):
                (state / (who + '.release')).touch()
            try:
                wait_for(lambda: sql('SELECT COUNT(*) FROM attempts WHERE drained=0')[0][0] == 0)
                result['drained'] = True
            except Exception as error:
                result['drained'] = False
                cleanup_errors.append(repr(error))
        for process in reversed(processes):
            try:
                if process.poll() is None:
                    process.terminate()
                try:
                    process.wait(timeout=6)
                except subprocess.TimeoutExpired:
                    process.kill(); process.wait(timeout=5)
            except Exception as error:
                cleanup_errors.append(repr(error))
        result['cleanup_errors'] = cleanup_errors
        persist()
        if (state / 'state.sqlite').exists():
            with sqlite3.connect(state / 'state.sqlite') as db:
                db.row_factory = sqlite3.Row
                tables = ['revisions', 'profiles', 'actions', 'attempts', 'events']
                save(root / 'database.json', {table: [dict(row) for row in db.execute('SELECT * FROM ' + table)]
                                             for table in tables})
        result['elapsed'] = time.monotonic() - began
        persist()
    return 0 if result['completed'] and result.get('drained') and not cleanup_errors else 1


def main():
    if not __debug__:
        raise SystemExit('Optimized Python is unsupported')
    if len(sys.argv) == 3 and sys.argv[1] == '--job':
        job(sys.argv[2]); return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--child', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args()
    binary, root = args.binary.resolve(strict=True), args.root.resolve()
    assert not root.exists(), 'refusing to reuse a fixture directory'
    if args.child:
        raise SystemExit(child(binary, root))
    os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[:2])
    result = own_process([sys.executable, str(Path(__file__).resolve()), '--child',
                          '--binary', str(binary), '--root', str(root)],
                         cwd=HERE, env=ENV, timeout=120)
    root.mkdir(parents=True, exist_ok=True)
    save(root / 'capsule.json', result)
    print(json.dumps({'code': result['code'], 'error': result['error'],
                      'surviving_owned_pids': result['surviving_owned_pids']}))
    raise SystemExit(0 if result['code'] == 0 and result['error'] is None
                     and not result['surviving_owned_pids'] else 1)


if __name__ == '__main__':
    main()
