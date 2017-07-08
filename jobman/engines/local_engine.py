import os
import sqlite3
import tempfile
import uuid

from .base_engine import BaseEngine
from .batch_jobdir_builders.serial_bash_batch_jobdir_builder import (
    SerialBashBatchJobdirBuilder)


class LocalEngine(BaseEngine):
    DEFAULT_ENTRYPOINT_NAME ='job.sh'

    def __init__(self, *args, db_uri=None, sqlite=sqlite3, scratch_dir=None,
                 batch_jobdir_builder=None, **kwargs):
        super().__init__(*args, **kwargs)
        db_uri = db_uri or ':memory:'
        self.scratch_dir = scratch_dir
        self.batch_jobdir_builder = batch_jobdir_builder or \
                self._get_default_batch_jobdir_builder()

        self.conn = sqlite.connect(db_uri)
        self.conn.row_factory = sqlite.Row
        self.ensure_db()

    def _get_default_batch_jobdir_builder(self):
        return SerialBashBatchJobdirBuilder

    def ensure_db(self):
        self.conn.execute('''CREATE TABLE IF NOT EXISTS jobs
                          (job_id text, status text)''')

    def submit_job(self, job=None):
        self._debug_locals()
        return self._submit_jobdir(jobdir_meta=job['jobdir_meta'])

    def _submit_jobdir(self, jobdir_meta=None):
        self._debug_locals()
        workdir = jobdir_meta['dir']
        entrypoint_name = jobdir_meta.get('entrypoint') or \
                self.DEFAULT_ENTRYPOINT_NAME
        entrypoint_path = os.path.join(workdir, entrypoint_name)
        cmd = 'pushd {workdir}; {entrypoint_path}; popd;'.format(
            workdir=workdir, entrypoint_path=entrypoint_path)
        std_log_files = {
            log_key: os.path.join(workdir, log_file_name)
            for log_key, log_file_name in jobdir_meta.get(
                'std_log_file_names', {}).items()
        }
        stdout_path = std_log_files.get('stdout')
        if stdout_path: cmd += ' >> "%s"' % std_log_files['stdout']
        stderr_path = std_log_files.get('stderr')
        if stderr_path: cmd += ' 2>> "%s"' % stderr_path
        try:
            self.process_runner.run_process(cmd=cmd, check=True, shell=True)
            job_id = str(uuid.uuid4())
            self.conn.cursor().execute(
                "INSERT INTO jobs VALUES (?, ?)",
                (job_id, self.JOB_STATUSES.EXECUTED,)
            )
            engine_meta = {'job_id': job_id}
            return engine_meta
        except self.process_runner.CalledProcessError as called_proc_err:
            if stdout_path: stdout = open(stdout_path).read()
            else: stdout = called_proc_err.stdout
            if stderr_path: stderr = open(stderr_path).read()
            else: stderr = called_proc_err.stderr
            error_msg = ("Submission error:\n"
                         "\tstdout: {stdout}\n"
                         "\tstderr: {stderr}\n").format(
                             stdout=stdout, stderr=stderr
                         )
            raise self.SubmissionError(error_msg) from called_proc_err

    def get_keyed_engine_states(self, keyed_engine_metas=None):
        self._debug_locals()
        keyed_job_ids = {
            key: engine_meta['job_id']
            for key, engine_meta in keyed_engine_metas.items()
        }
        local_jobs_by_id = self.get_local_jobs_by_id(
            job_ids=keyed_job_ids.values())
        keyed_engine_states = {}
        for key, job_id in keyed_job_ids.items():
            local_job = local_jobs_by_id.get(job_id)
            engine_state = self.local_job_to_engine_state(local_job=local_job)
            keyed_engine_states[key] = engine_state
        return keyed_engine_states

    def get_local_jobs_by_id(self, job_ids=None):
        self._debug_locals()
        local_jobs = {}
        rows = self.conn.cursor().execute('SELECT job_id, status from jobs')
        for row in rows:
            local_jobs[row['job_id']] = {k: row[k] for k in row.keys()}
        return local_jobs

    def local_job_to_engine_state(self, local_job=None):
        self._debug_locals()
        engine_state = {'engine_job_state': local_job}
        if local_job is not None:
            engine_state['status'] = self.local_job_to_status(
                local_job=local_job)
        return engine_state

    def local_job_to_status(self, local_job=None):
        self._debug_locals()
        return self.JOB_STATUSES.EXECUTED

    def submit_batch_job(self, batch_job=None, subjobs=None):
        self._debug_locals()
        batch_jobdir_meta = self._build_batch_jobdir(
            batch_job=batch_job, subjobs=subjobs,
            dest=tempfile.mkdtemp(dir=self.scratch_dir, prefix='batch.')
        )
        return self._submit_jobdir(jobdir_meta=batch_jobdir_meta)

    def _build_batch_jobdir(self, batch_job=None, subjobs=None, dest=None):
        self._debug_locals()
        jobdir_meta = self.batch_jobdir_builder.build_batch_jobdir(
            batch_job=batch_job, subjobs=subjobs, dest=dest)
        return jobdir_meta
