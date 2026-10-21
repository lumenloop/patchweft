"""Bound one command and its descendants, retaining partial timeout output."""
import ctypes
import os
from pathlib import Path
import signal
import subprocess
import time


def identity(pid):
    try:
        fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        return fields[19], fields[0]
    except (FileNotFoundError, ProcessLookupError):
        return None


def run(argv, *, cwd, env, timeout):
    if not __debug__:
        raise RuntimeError('Optimized Python is unsupported')
    libc = ctypes.CDLL(None, use_errno=True)
    assert libc.prctl(36, 1, 0, 0, 0) == 0, 'cannot establish child subreaper'

    def children(pid):
        try:
            return list(map(int, Path(f'/proc/{pid}/task/{pid}/children').read_text().split()))
        except (FileNotFoundError, ProcessLookupError):
            return []

    baseline = set(children(os.getpid()))
    process = subprocess.Popen(argv, cwd=cwd, env=env, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True, start_new_session=True)
    owned = {}
    started, error, stdout, stderr = time.monotonic(), None, '', ''
    captured = False

    def collect(pid):
        value = identity(pid)
        if value is None or (pid in owned and owned[pid] != value[0]):
            return
        owned[pid] = value[0]
        for child in children(pid):
            collect(child)

    def collect_all():
        collect(process.pid)
        for pid in children(os.getpid()):
            if pid not in baseline:
                collect(pid)

    def live(pid, start):
        value = identity(pid)
        return value is not None and value[0] == start and value[1] != 'Z'

    def kill_owned():
        collect_all()
        for pid, start in reversed(list(owned.items())):
            if live(pid, start):
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

    try:
        while True:
            collect_all()
            remaining = timeout - (time.monotonic() - started)
            if remaining <= 0:
                raise TimeoutError('owned command deadline')
            try:
                stdout, stderr = process.communicate(timeout=min(.05, remaining))
                captured = True
                break
            except subprocess.TimeoutExpired:
                continue
    except BaseException as exc:
        error = type(exc).__name__ + ': ' + str(exc)
    finally:
        # Cleanup precedes any second pipe-drain attempt and never depends on
        # its success. Adoption catches double-forked children.
        collect_all()
        grace = time.monotonic() + (.25 if error is None else 0)
        while any(live(pid, start) for pid, start in owned.items()) and time.monotonic() < grace:
            time.sleep(.01)
            collect_all()
        if any(live(pid, start) for pid, start in owned.items()):
            error = error or 'owned descendants survived command completion'
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                kill_owned()
                if not any(live(pid, start) for pid, start in owned.items()):
                    break
                time.sleep(.01)
        if not captured:
            try:
                stdout, stderr = process.communicate(timeout=2)
            except subprocess.TimeoutExpired as exc:
                error = error or 'output pipes did not close after cleanup'
                def text(value):
                    return value.decode(errors='replace') if isinstance(value, bytes) else value or ''
                stdout, stderr = text(exc.output), text(exc.stderr)
                kill_owned()
                process.stdout.close()
                process.stderr.close()
        try:
            process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            error = error or 'direct child survived cleanup'
            kill_owned()
        collect_all()
        for pid in owned:
            if pid != process.pid:
                try:
                    os.waitpid(pid, os.WNOHANG)
                except ChildProcessError:
                    pass
    survivors = [pid for pid, start in owned.items() if live(pid, start)]
    return {'stdout': stdout, 'stderr': stderr, 'code': process.returncode, 'error': error,
            'owned_pids': sorted(owned), 'surviving_owned_pids': survivors,
            'elapsed': time.monotonic() - started}
