import os
import logging
import subprocess
import tempfile
import types

from jobman.batch_builders.bash_batch_builder import BashBatchBuilder
from jobman.utils import dot_spec_loader
from jobman import constants


class BaseEngine(object):
    DEFAULT_JOB_ENTRYPOINT_NAME = 'job.sh'
    CHECKPOINT_FILE_NAMES = constants.CHECKPOINT_FILE_NAMES

    JOB_STATUSES = constants.JOB_STATUSES

    class SubmissionError(Exception):
        pass

    class CfgItemResolutionError(Exception):
        def __init__(self, msg=None, key=None, spec=None, *args, **kwargs):
            msg = msg or ''
            msg += "key:'{key}'; spec: '{spec}'".format(key=key, spec=spec)
            super().__init__(msg, *args, **kwargs)

    def __init__(self, key=None, process_runner=None, logger=None, debug=None,
                 cfg=None, scratch_dir=None, build_batch_jobdir_fn=None):
        self.key = key
        self.debug = debug
        self.logger = self._setup_logger(logger=logger)
        self.process_runner = (process_runner or
                               self.generate_default_process_runner())
        self.cfg = cfg or {}
        self.scratch_dir = scratch_dir
        self.build_batch_jobdir_fn = (build_batch_jobdir_fn or
                                      self.default_build_batch_jobdir)

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

    def initialize(self):
        raise NotImplementedError

    def tick(self):
        raise NotImplementedError

    def submit_job(self, job=None, extra_cfgs=None):
        raise NotImplementedError

    def get_keyed_states(self, keyed_metas=None):
        raise NotImplementedError

    def submit_batch_job(self, batch_job=None, subjobs=None, extra_cfgs=None):
        batch_job_spec = self.build_batch_jobdir(
            batch_job=batch_job, subjobs=subjobs,
            dest=tempfile.mkdtemp(dir=self.scratch_dir, prefix='batch.'),
            extra_cfgs=extra_cfgs
        )
        return self.submit_job(
            job={'job_spec': batch_job_spec},
            extra_cfgs=extra_cfgs
        )

    def build_batch_jobdir(self, batch_job=None, subjobs=None, dest=None,
                           extra_cfgs=None):
        job_spec = self.build_batch_jobdir_fn(
            batch_job=batch_job, subjobs=subjobs, dest=dest,
            extra_cfgs=extra_cfgs)
        return job_spec

    def resolve_job_cfg_specs(self, job=None, extra_cfgs=None):
        cfgs = [job['job_spec'].get('cfg', {}), *self._get_default_cfgs(),
                *(extra_cfgs or [])]
        resolved_cfg_items = {}
        for key, spec in job['job_spec'].get('cfg_specs', {}).items():
            output_key = spec.get('output_key') or key
            resolved_cfg_items[output_key] = self.resolve_cfg_item(
                key=key, spec=spec, cfgs=cfgs)
        return resolved_cfg_items

    def _get_default_cfgs(self): return [self.cfg, os.environ]

    def resolve_cfg_item(self, key=None, spec=None, cfgs=None):
        cfgs = cfgs or self._get_default_cfgs()
        try:
            get_kwargs = {'key': key, 'srcs': cfgs}
            if 'default' in spec:
                get_kwargs['default'] = spec['default']
            return self._get_from_first_matching_src(**get_kwargs)
        except KeyError:
            raise self.CfgItemResolutionError(key=key, spec=spec)

    def _get_from_first_matching_src(self, key=None, srcs=None, default=...):
        for src in srcs:
            try:
                return self._get_key_or_attr(src=src, key=key)
            except KeyError:
                pass
        if default is not ...:
            return default
        raise KeyError(key)

    def _get_key_or_attr(self, src=None, key=None):
        return dot_spec_loader.get_attr_or_item(obj=src, key=key)
