import os
import tempfile


class BaseBatchJobdirBuilder(object):
    ENTRYPOINT_NAME = 'entrypoint.sh'
    SUBJOBS_DIR_NAME = 'subjobs'
    std_log_file_names = {logname: logname for logname in ['stdout', 'stderr']}

    def __init__(self, batch_job=None, subjobs=None, dest=None):
        self.batch_job = batch_job
        self.subjobs = subjobs
        self.jobdir = dest or tempfile.mkdtemp()
        os.makedirs(self.jobdir, exist_ok=True)
        self.entrypoint_path = os.path.join(self.jobdir, self.ENTRYPOINT_NAME)

    @classmethod
    def build_batch_jobdir(cls, batch_job=None, subjobs=None, dest=None):
        instance = cls(batch_job=batch_job, subjobs=subjobs, dest=dest)
        jobdir_meta = instance._build_batch_jobdir()
        return jobdir_meta

    def _build_batch_jobdir(self): raise NotImplementedError()
