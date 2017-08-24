from pathlib import Path
import tempfile


class BaseBatchBuilder(object):
    ENTRYPOINT_NAME = 'entrypoint.sh'
    SUBJOBS_DIR_NAME = 'subjobs'
    std_log_file_names = {logname: logname for logname in ['stdout', 'stderr']}

    def build_batch_jobdir(self, batch_job=None, subjobs=None, dest=None,
                           extra_cfgs=None, **kwargs):
        self.jobdir_path = Path(dest or tempfile.mkdtemp())
        self.jobdir_path.mkdir(parents=True, exist_ok=True)
        self.batch_job = batch_job
        self.subjobs = subjobs
        self.extra_cfgs = extra_cfgs
        job_spec = self._build_batch_jobdir(**kwargs)
        return job_spec

    def _build_batch_jobdir(self, **kwargs): raise NotImplementedError

    @property
    def entrypoint_path(self): return (self.jobdir_path / self.ENTRYPOINT_NAME)
