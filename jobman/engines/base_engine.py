import logging
import subprocess
import types

class BaseEngine(object):
    DEFAULT_ENTRYPOINT_NAME ='job.sh'

    class JOB_STATUSES(object):
        RUNNING = 'RUNNING'
        EXECUTED = 'EXECUTED'
        FAILED = 'FAILED'
        UNKNOWN = 'UNKNOWN'

    class SubmissionError(Exception): pass

    def __init__(self, process_runner=None, logger=None, cfg=None):
        self.process_runner = process_runner or \
                self.generate_default_process_runner()
        self.logger = logger or logging
        self.cfg = cfg or {}

    def generate_default_process_runner(self):
        process_runner = types.SimpleNamespace()
        def run_process(cmd=None, **kwargs):
            return subprocess.run(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                universal_newlines=True, **kwargs)
        process_runner.run_process = run_process
        process_runner.CalledProcessError = subprocess.CalledProcessError
        return process_runner
