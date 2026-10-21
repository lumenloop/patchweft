"""Finite local exercise: two agents use native source commands and scoped remote CLI.

The gateway is a cooperative harness, not a sandbox for hostile code or agents.
Only the parent uses setup/host/seal. Agents send one JSON packet on stdin to act.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from fixture import BASE


def save(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def execute(argv, timeout=45):
    result = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(result.stderr + result.stdout)
    return json.loads(result.stdout)


class Session:
    def __init__(self, root):
        self.root = root.resolve(strict=True)
        self.config = json.loads((root / 'config.json').read_text())
        self.binary = self.config['binary']
        assert hashlib.sha256(Path(self.binary).read_bytes()).hexdigest() == self.config['binary_sha256']

    def call(self, *args):
        return execute([self.binary, '--state', str(self.root / 'state'), *args])

    def remote(self, role, operation, data, key=None):
        ready = json.loads((self.root / 'ready.json').read_text())
        argv = [self.binary, 'remote', '--endpoint', 'http://' + ready['address'],
                '--credential-file', str(self.root / (role + '.credential')),
                operation, '--data', json.dumps(data)]
        if key is not None:
            argv += ['--key', key]
        return execute(argv)

    def act(self, role, packet):
        assert role in {'provider', 'consumer'}
        assert (self.root / 'clock.json').exists(), 'host not started'
        clock = json.loads((self.root / 'clock.json').read_text())
        assert time.monotonic() < clock['deadline_monotonic'], 'exercise deadline passed'
        assert not (self.root / 'seal.json').exists(), 'exercise sealed'
        operation = packet['op']
        if operation == 'info':
            return {key: self.config[key] for key in ['base', 'profile', 'owned_files']}
        if operation == 'query':
            assert packet['data']['op'] in {'projects', 'profiles', 'revisions', 'actions',
                'action', 'candidate', 'repairs', 'repair', 'logs', 'artifacts', 'events'}
            return self.remote(role, 'query', packet['data'])
        if operation == 'command':
            assert packet['data']['op'] in {'request', 'submit', 'compose', 'cancel'}
            return self.remote(role, 'command', packet['data'], packet['key'])
        if operation == 'workspace':
            result = self.call('workspace', '--revision', packet['revision'], '--owner', role)
            catalog = self.catalog()
            catalog[result['id']] = result
            save(self.root / 'workspaces.json', catalog)
            return result
        catalog = self.catalog()
        workspace = catalog[packet['workspace']]
        assert workspace['owner'] == role, 'workspace belongs to another agent'
        if operation in {'read', 'write'}:
            filename = packet['file']
            assert filename in BASE, 'file not part of the synthetic fixture'
            path = Path(workspace['path']) / filename
            assert not path.is_symlink() and path.resolve().parent == Path(workspace['path']).resolve()
            if operation == 'read':
                return {'file': filename, 'text': path.read_text()}
            assert filename in self.config['owned_files'][role], 'file belongs to another role'
            assert isinstance(packet['text'], str) and len(packet['text'].encode()) < 16000
            path.write_text(packet['text'])
            return {'file': filename, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
        if operation == 'checkpoint':
            return self.call('checkpoint', workspace['id'], '--message', packet['message'])
        raise ValueError('unsupported operation')

    def catalog(self):
        path = self.root / 'workspaces.json'
        return json.loads(path.read_text()) if path.exists() else {}


def setup(root, binary):
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    binary = binary.resolve(strict=True)
    save(root / 'config.json', {'binary': str(binary),
        'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest()})
    session = Session(root)
    seed = root / 'seed'
    seed.mkdir()
    for name, content in BASE.items():
        (seed / name).write_text(content)
    # The parent alone creates the seed repository; no agent Git command is exposed.
    for argv in [['init', '--quiet'], ['add', '.'], ['commit', '--quiet', '-m', 'Synthetic base']]:
        subprocess.run(['/usr/bin/git', '-C', str(seed), '-c', 'user.name=Example',
            '-c', 'user.email=example@local.invalid', '-c', 'commit.gpgsign=false',
            '-c', 'core.hooksPath=/dev/null', *argv], check=True, timeout=20)
    session.call('init', '--slots', '2', '--memory-mb', '512')
    base = session.call('import', '--name', 'direct-agents', '--repo', str(seed), '--owner', 'parent')
    profile = {'version': 1, 'name': 'fixed-eight', 'certifying': True,
        'tools': {'python': str(Path(sys.executable).resolve())},
        'jobs': [{'name': 'contract', 'command': ['python', 'contract.py'],
            'slots': 1, 'memory_mb': 256, 'timeout_seconds': 20,
            'artifacts': ['result.json'], 'report': {'kind': 'node-tap', 'minimum': 8}}]}
    save(root / 'profile.json', profile)
    registered = session.call('register', str(root / 'profile.json'))
    for role in ['provider', 'consumer']:
        save(root / (role + '-policy.json'), {'id': role, 'projects': [base['project']],
            'profiles': [registered['id']], 'roles': ['read', 'write'], 'max_pending': 16})
        session.call('grant', '--policy', str(root / (role + '-policy.json')),
                     '--credential-file', str(root / (role + '.credential')))
    session.config.update(base=base, profile=registered,
        owned_files={'provider': ['quote.py'], 'consumer': ['invoice.py', 'ledger.py']})
    save(root / 'config.json', session.config)
    return session.config


def host(session):
    root = session.root
    clock = {'started_at': time.time(), 'started_monotonic': time.monotonic()}
    clock['deadline_monotonic'] = clock['started_monotonic'] + 900
    assert not (root / 'clock.json').exists()
    save(root / 'clock.json', clock)
    processes = []
    handles = []
    try:
        for label, args in [('server', ['serve', '--listen', '127.0.0.1:0',
                           '--ready-file', str(root / 'ready.json')]), ('worker', ['worker'])]:
            stream = (root / (label + '.log')).open('x')
            handles.append(stream)
            processes.append(subprocess.Popen([session.binary, '--state', str(root / 'state'), *args],
                stdout=stream, stderr=subprocess.STDOUT))
        while time.monotonic() < clock['deadline_monotonic'] and not (root / 'seal.json').exists():
            assert all(process.poll() is None for process in processes), 'service exited early'
            time.sleep(.1)
        save(root / 'host-ended.json', {'ended_at': time.time(), 'sealed': (root / 'seal.json').exists()})
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)
        for stream in handles:
            stream.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=['setup', 'host', 'act', 'seal'])
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--binary', type=Path)
    parser.add_argument('--role', choices=['provider', 'consumer'])
    args = parser.parse_args()
    if args.operation == 'setup':
        result = setup(args.root.resolve(), args.binary)
    else:
        session = Session(args.root)
        if args.operation == 'host':
            host(session)
            return
        with (session.root / 'gateway.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if args.operation == 'seal':
                assert not (session.root / 'seal.json').exists(), 'already sealed'
                result = {'sealed_at': time.time(), 'sealed_monotonic': time.monotonic()}
                save(session.root / 'seal.json', result)
            else:
                packet = json.loads(sys.stdin.readline(100000))
                row = {'role': args.role, 'packet': packet, 'started_at': time.time()}
                try:
                    result = session.act(args.role, packet)
                    row['result'] = result
                except Exception as error:
                    row['error'] = type(error).__name__ + ': ' + str(error)
                    raise
                finally:
                    row['ended_at'] = time.time()
                    with (session.root / 'calls.jsonl').open('a') as stream:
                        stream.write(json.dumps(row) + '\n')
    print(json.dumps(result))


if __name__ == '__main__':
    if not __debug__:
        raise SystemExit('Optimized Python unsupported')
    main()
