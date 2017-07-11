import os
import sys
import subprocess
import unittest

import jobman

JOBMAN_PARENT_DIR = os.path.dirname(os.path.dirname(jobman.__file__))


class TestExample(unittest.TestCase):
    def test_example(self):
        cmd = 'PYTHONPATH={PYTHONPATH} python {this_dir}/entrypoint.py'.format(
            PYTHONPATH=(JOBMAN_PARENT_DIR + ':$PYTHONPATH'),
            this_dir=os.path.dirname(os.path.abspath(__file__))
        )
        proc = subprocess.run(
            cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        stdout = proc.stdout.decode()
        stderr = proc.stderr.decode()
        print(stdout, file=sys.stdout)
        print(stderr, file=sys.stderr)
        expected_stdout = ''
        expected_stderr = ''
        self.assertEqual(stdout, expected_stdout)
        self.assertEqual(stderr, expected_stderr)
