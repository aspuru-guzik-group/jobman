import os
import sqlite3
import uuid

from .base_bash_engine import BaseBashEngine


class LocalEngine(BaseBashEngine):
    def __init__(self, *args, db_uri=':memory:', sqlite=sqlite3, **kwargs):
        super().__init__(*args, **kwargs)
        if db_uri == 'sqlite://':
            db_uri = ':memory:'
        elif db_uri.startswith('sqlite:///'):
            db_uri = db_uri.replace('sqlite:///', '')
        self.conn = sqlite.connect(db_uri)
        self.conn.row_factory = sqlite.Row
        self.ensure_db()

    def ensure_db(self):
        self.conn.execute('''CREATE TABLE IF NOT EXISTS jobs
                          (job_id text, status text)''')

    def submit_job(self, job=None, extra_cfgs=None):
        job_id = self._generate_job_id()
        entrypoint_path = self._write_engine_entrypoint(
            job=job, extra_cfgs=extra_cfgs)
        self._execute_engine_entrypoint(entrypoint_path=entrypoint_path,
                                        job=job, job_id=job_id,
                                        extra_cfgs=extra_cfgs)
        engine_meta = {'job_id': job_id}
        return engine_meta

    def _generate_job_id(self): return str(uuid.uuid4())

    def _execute_engine_entrypoint(self, entrypoint_path=None, job=None,
                                   job_id=None, extra_cfgs=None):
        self._execute_engine_entrypoint_cmd(
            entrypoint_cmd=self._generate_engine_entrypoint_cmd(
                entrypoint_path=entrypoint_path,
                job=job,
                job_id=job_id,
                extra_cfgs=extra_cfgs
            ),
            job=job,
            job_id=job_id
        )

    def _generate_engine_entrypoint_cmd(self, entrypoint_path=None, job=None,
                                        job_id=None, extra_cfgs=None):
        cmd = (
            'pushd {entrypoint_dir} > /dev/null &&'
            ' {entrypoint_path} {stdout_redirect} {stderr_redirect};'
            ' popd > /dev/null;'
        ).format(
            entrypoint_dir=os.path.dirname(entrypoint_path),
            entrypoint_path=entrypoint_path,
            **self._get_std_log_redirects(job=job)
        )
        return cmd

    def _get_std_log_redirects(self, job=None):
        std_log_paths = self._get_std_log_paths(job=job)
        stdout_redirect = ''
        if 'stdout' in std_log_paths:
            stdout_redirect = '>> %s' % std_log_paths['stdout']
        stderr_redirect = ''
        if 'stderr' in std_log_paths:
            stderr_redirect = '2>> %s' % std_log_paths['stderr']
        return {'stdout_redirect': stdout_redirect,
                'stderr_redirect': stderr_redirect}

    def _execute_engine_entrypoint_cmd(self, entrypoint_cmd=None, job=None,
                                       job_id=None):
        try:
            self.process_runner.run_process(cmd=entrypoint_cmd, check=True,
                                            shell=True)
            self.conn.cursor().execute(
                "INSERT INTO jobs VALUES (?, ?)",
                (job_id, self.JOB_STATUSES.EXECUTED,)
            )
        except self.process_runner.CalledProcessError as called_proc_exc:
            self._handle_engine_entrypoint_called_proc_exc(
                called_proc_exc=called_proc_exc, job=job)

    def _handle_engine_entrypoint_called_proc_exc(self, called_proc_exc=None,
                                                  job=None):
        error_msg_lines = ["Submission error:"]
        std_log_contents = self._get_std_log_contents(job=job)
        for stream_name in ['stdout', 'stderr']:
            error_msg_lines.append(
                "\tprocess.{stream_name}: {content}".format(
                    stream_name=stream_name,
                    content=getattr(called_proc_exc, stream_name)
                )
            )
            error_msg_lines.append(
                "\tlogs.{stream_name}: {content}".format(
                    stream_name=stream_name,
                    content=std_log_contents.get(stream_name)
                )
            )
        error_msg = "\n".join(error_msg_lines)
        raise self.SubmissionError(error_msg) from called_proc_exc

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
