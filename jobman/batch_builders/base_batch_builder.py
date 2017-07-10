import os
import tempfile


class BaseBatchBuilder(object):
    ENTRYPOINT_NAME = 'entrypoint.sh'
    SUBJOBS_DIR_NAME = 'subjobs'
    std_log_file_names = {logname: logname for logname in ['stdout', 'stderr']}

    def __init__(self, batch_job=None, subjobs=None, dest=None):
        self.jobdir = dest or tempfile.mkdtemp()
        os.makedirs(self.jobdir, exist_ok=True)
        self.entrypoint_path = os.path.join(self.jobdir, self.ENTRYPOINT_NAME)


    def build_batch_jobdir(self, batch_job=None, subjobs=None, dest=None,
                           **kwargs):
        self.batch_job = batch_job
        self.subjobs = subjobs
        self.dest = dest
        job_spec = self._build_batch_jobdir(**kwargs)
        del self.batch_job
        del self.subjobs
        del self.dest
        return job_spec

    def _build_batch_jobdir(self, **kwargs): raise NotImplementedError
        
