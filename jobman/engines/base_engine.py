import logging
import subprocess
import tempfile
import types

from ..batch_builders.bash_batch_builder import BashBatchBuilder

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
        self.debug = debug
        self.logger = self._setup_logger(logger=logger)
        self.process_runner = process_runner or \
                self.generate_default_process_runner()
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

    def generate_default_process_runner(self):
        process_runner = types.SimpleNamespace()
        def run_process(cmd=None, **kwargs):
            return subprocess.run(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                universal_newlines=True, **kwargs)
        process_runner.run_process = run_process
        process_runner.CalledProcessError = subprocess.CalledProcessError
        return process_runner

    def default_build_batch_jobdir(self, *args, **kwargs):
        return BashBatchBuilder().build_batch_jobdir(*args, **kwargs)

    def submit_job(self, job=None): raise NotImplementedError

    def submit_batch_job(self, batch_job=None, subjobs=None):
        batch_job_spec = self.build_batch_jobdir(
            batch_job=batch_job, subjobs=subjobs,
            dest=tempfile.mkdtemp(dir=self.scratch_dir, prefix='batch.')
        )
        return self.submit_job(job={'job_spec': batch_job_spec})

    def build_batch_jobdir(self, batch_job=None, subjobs=None, dest=None):
        job_spec = self.build_batch_jobdir_fn(
            batch_job=batch_job, subjobs=subjobs, dest=dest)
        return job_spec
