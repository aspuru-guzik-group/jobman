import os
import sqlite3
import textwrap
import uuid

from .base_engine import BaseEngine


class LocalEngine(BaseEngine):
    ENGINE_ENTRYPOINT_TPL ='jobman_entrypoint.{job_id}.sh'

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
        job_id = self._generate_job_id()
        entrypoint_path = self._write_engine_entrypoint(job=job, job_id=job_id)
        self._execute_engine_entrypoint(entrypoint_path=entrypoint_path,
                                        job=job, job_id=job_id)
        engine_meta = {'job_id': job_id}
        return engine_meta

    def _generate_job_id(self): return str(uuid.uuid4())

    def _write_engine_entrypoint(self, job=None, job_id=None):
        entrypoint_content = self._generate_engine_entrypoint_content(
            job=job, job_id=job_id)
        entrypoint_path = os.path.join(
            job['job_spec']['dir'],
            self.ENGINE_ENTRYPOINT_TPL.format(job_id=job_id)
        )
        with open(entrypoint_path, 'w') as f: f.write(entrypoint_content)
        return entrypoint_path

    def _generate_engine_entrypoint_content(self, job=None, job_id=None):
        return textwrap.dedent(
            '''
            #!/bin/bash
            {preamble}
            pushd {jobdir} && {job_entrypoint}; popd;
            '''
        ).format(
            preamble=self._generate_engine_entrypoint_preamble(job=job,
                                                               job_id=job_id),
            jobdir=job['job_spec']['dir'],
            job_entrypoint=job['job_spec']['entrypoint'],
        )

    def _generate_engine_entrypoint_preamable(self, job=None, job_id=None):
        return self._generate_env_vars_for_cfg_specs(job=job)

    def _generate_env_vars_for_cfg_specs(self, job=None):
        resolved_cfgs = self.resolve_job_cfg_specs(job=job)
        return "\n".join([
            self._kvp_to_env_var_block(kvp={'key': k, 'value': v})
            for k, v in resolved_cfgs.items()
        ])

    def _kvp_to_env_var_block(self, kvp=None):
        return textwrap.dedent(
            '''
            read -d '' {key} << EOF
            {value}
            EOF
            '''
        ).lstrip().format(key=kvp['key'], value=kvp['value'].lstrip())

    def _execute_engine_entrypoint(self, entrypoint_path=None, job=None,
                                   job_id=None):
        self._execute_engine_entrypoint_cmd(
            entrypoint_cmd=self._generate_engine_entrypoint_cmd(
                entrypoint_path=entrypoint_path,
                job=job,
                job_id=job_id
            ),
            job=job,
            job_id=job_id
        )

    def _generate_engine_entrypoint_cmd(self, entrypoint_path=None, job=None,
                                        job_id=None):
        cmd = (
            'pushd {entrypoint_dir}' 
            ' && {entrypoint_path} {stdout_redirect} {stderr_redirect}'
            'popd;'
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

    def _get_std_log_paths(self, job=None):
        return {
            log_key: os.path.join(job['job_spec']['dir'], log_file_name)
            for log_key, log_file_name in job['job_spec'].get(
                'std_log_file_names', {}).items()
        }

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

    def _get_std_log_contents(self, job=None):
        std_log_contents = {}
        for log_name, log_path in self._get_std_log_paths(job=job):
            log_content = ''
            try:
                with open(log_path) as f: log_content = f.read()
            except Exception as exc:
                log_content = "COULD NOT READ LOG '{log_name}': {exc}'".format(
                    log_name=log_name, exc=exc)
            std_log_contents[log_name] = log_content
        return std_log_contents
                
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

