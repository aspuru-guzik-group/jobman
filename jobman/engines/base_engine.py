import logging
import subprocess
import types

from .. import debug_utils
from ..batch_jobdir_builders.bash_batch_jobdir_builder import (
    BashBatchJobdirBuilder)

class BaseEngine(object):
    DEFAULT_ENTRYPOINT_NAME ='job.sh'

    class JOB_STATUSES(object):
        RUNNING = 'RUNNING'
        EXECUTED = 'EXECUTED'
        FAILED = 'FAILED'
        UNKNOWN = 'UNKNOWN'

    class SubmissionError(Exception): pass

    def __init__(self, process_runner=None, logger=None, debug=None, cfg=None,
                 scratch_dir=None, build_batch_jobdir_fn=None):
        self.process_runner = process_runner or \
                self.generate_default_process_runner()
        self.debug = debug
        self.logger = self._setup_logger(logger=logger)
        self.cfg = cfg or {}
        self.scratch_dir = scratch_dir
        self.build_batch_jobdir_fn = build_batch_jobdir_fn or \
                self.default_build_batch_jobdir

    def _setup_logger(self, logger=None):
        if not logger:
            logger = logging.getLogger(__name__)
            if self.debug:
                logger.addHandler(logging.StreamHandler())
                logger.setLevel(logging.DEBUG)
        return logger

    def _debug_locals(self):
        if self.debug: debug_utils.debug_locals(logger=self.logger)

    def default_build_batch_jobdir(self, *args, **kwargs):
        return BashBatchJobdirBuilder().build_batch_jobdir(*args, **kwargs)

    def generate_default_process_runner(self):
        process_runner = types.SimpleNamespace()
        def run_process(cmd=None, **kwargs):
            return subprocess.run(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                universal_newlines=True, **kwargs)
        process_runner.run_process = run_process
        process_runner.CalledProcessError = subprocess.CalledProcessError
        return process_runner

    def build_batch_jobdir(self, batch_job=None, subjobs=None, dest=None):
        jobdir_meta = self.build_batch_jobdir_fn(
            batch_job=batch_job, subjobs=subjobs, dest=dest)
        return jobdir_meta
