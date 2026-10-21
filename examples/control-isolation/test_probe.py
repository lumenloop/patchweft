"""Failure-path checks that need no listener or production service."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import owned_process


class ProbeGuards(unittest.TestCase):
    def test_timeout_retains_output_and_reaps_detached_descendants(self):
        code = ('import os,time\n'
                'if os.fork()==0:\n os.setsid();print("detached-ready",flush=True);time.sleep(30)\n'
                'else:\n print("parent-ready",flush=True);time.sleep(30)\n')
        result = owned_process.run([sys.executable, '-c', code], cwd='/tmp',
                                   env={'PATH': '/usr/bin:/bin'}, timeout=.4)
        self.assertIn('deadline', result['error'])
        self.assertIn('parent-ready', result['stdout'])
        self.assertIn('detached-ready', result['stdout'])
        self.assertGreaterEqual(len(result['owned_pids']), 2)
        self.assertEqual(result['surviving_owned_pids'], [])

    def test_second_pipe_timeout_cannot_skip_cleanup(self):
        real_popen = subprocess.Popen
        def popen(*args, **kwargs):
            process = real_popen(*args, **kwargs)
            def timeout(*args, **kwargs):
                raise subprocess.TimeoutExpired('synthetic pipe stall', .1,
                                                output=b'partial-output\n', stderr=b'partial-error\n')
            process.communicate = timeout
            return process
        with patch.object(owned_process.subprocess, 'Popen', popen):
            result = owned_process.run([sys.executable, '-c', 'import time;time.sleep(30)'],
                                       cwd='/tmp', env={'PATH': '/usr/bin:/bin'}, timeout=.1)
        self.assertIn('deadline', result['error'])
        self.assertEqual(result['stdout'], 'partial-output\n')
        self.assertEqual(result['stderr'], 'partial-error\n')
        self.assertEqual(result['surviving_owned_pids'], [])
        self.assertIsNotNone(result['code'])

    def test_early_setup_failure_keeps_observations_and_capsule(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'failed-run'
            result = subprocess.run([sys.executable, str(Path(__file__).with_name('run.py')),
                                     '--binary', '/usr/bin/false', '--root', str(root)],
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 1, result.stderr)
            observations = json.loads((root / 'observations.json').read_text())
            capsule = json.loads((root / 'capsule.json').read_text())
            self.assertFalse(observations['completed'])
            self.assertIn('fixture command failed', observations['error'])
            self.assertEqual(capsule['surviving_owned_pids'], [])
            self.assertEqual(capsule['code'], 1)


if __name__ == '__main__':
    unittest.main()
