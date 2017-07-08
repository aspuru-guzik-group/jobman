import os
import sqlite3
import uuid

from .base_engine import BaseEngine


class LocalEngine(BaseEngine):
    DEFAULT_ENTRYPOINT_NAME ='job.sh'

    def __init__(self, *args, db_uri=None, sqlite=sqlite3, **kwargs):
        super().__init__(*args, **kwargs)
        db_uri = db_uri or ':memory:'
        self.conn = sqlite.connect(db_uri)
        self.conn.row_factory = sqlite.Row
        self.ensure_db()

    def ensure_db(self):
        self.conn.execute('''CREATE TABLE IF NOT EXISTS jobs
                          (job_id text, status text)''')

    def submit_job(self, job=None):
        jobdir_meta = job['jobdir_meta']
        workdir = jobdir_meta['dir']
        entrypoint_name = jobdir_meta.get('entrypoint') or \
                self.DEFAULT_ENTRYPOINT_NAME
        entrypoint_path = os.path.join(workdir, entrypoint_name)
        cmd = entrypoint_path
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
        local_jobs = {}
        rows = self.conn.cursor().execute('SELECT job_id, status from jobs')
        for row in rows:
            local_jobs[row['job_id']] = {k: row[k] for k in row.keys()}
        return local_jobs

    def local_job_to_engine_state(self, local_job=None):
        engine_state = {'engine_job_state': local_job}
        if local_job is not None:
            engine_state['status'] = self.local_job_to_status(
                local_job=local_job)
        return engine_state

    def local_job_to_status(self, local_job=None):
        return self.JOB_STATUSES.EXECUTED
