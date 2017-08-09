import os

from jobman.dao.engine_sqlite_dao import EngineSqliteDAO
from .base_bash_engine import BaseBashEngine


class LocalEngine(BaseBashEngine):
    def __init__(self, *args, db_uri=None, ensure_db=None, cache_ttl=1,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.cache_ttl = cache_ttl
        self.dao = EngineSqliteDAO(db_uri=db_uri,
                                   table_prefix='engine_%s_' % self.key)

    def tick(self):
        pass

    def submit_job(self, job=None, extra_cfgs=None):
        entrypoint_path = self._write_engine_entrypoint(
            job=job, extra_cfgs=extra_cfgs)
        self._execute_engine_entrypoint(entrypoint_path=entrypoint_path,
                                        extra_cfgs=extra_cfgs)
        engine_job = self.dao.create_job(
            job={'status': self.JOB_STATUSES.EXECUTED})
        engine_meta = {'key': engine_job['key']}
        return engine_meta

    def _execute_engine_entrypoint(self, entrypoint_path=None, job=None,
                                   job_key=None, extra_cfgs=None):
        self._execute_engine_entrypoint_cmd(
            entrypoint_cmd=self._generate_engine_entrypoint_cmd(
                entrypoint_path=entrypoint_path,
                job=job,
                extra_cfgs=extra_cfgs
            ),
            job=job,
        )

    def _generate_engine_entrypoint_cmd(self, entrypoint_path=None, job=None,
                                        extra_cfgs=None):
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

    def _execute_engine_entrypoint_cmd(self, entrypoint_cmd=None, job=None):
        try:
            self.process_runner.run_process(cmd=entrypoint_cmd, check=True,
                                            shell=True)
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
        engine_job_keys_by_external_keys = {
            external_key: engine_meta['key']
            for external_key, engine_meta in keyed_engine_metas.items()
        }
        engine_job_keys = list(engine_job_keys_by_external_keys.values())
        engine_jobs = self.dao.get_jobs(query={
            'filters': [{'field': 'key', 'op': 'IN', 'arg': engine_job_keys}]
        })
        engine_jobs_by_engine_job_keys = {
            engine_job['key']: engine_job
            for engine_job in engine_jobs
        }
        engine_states_by_external_keys = {}
        for external_key in engine_job_keys_by_external_keys.keys():
            engine_job_key = engine_job_keys_by_external_keys[external_key]
            engine_job = engine_jobs_by_engine_job_keys.get(engine_job_key)
            engine_state = self._engine_job_to_engine_state(engine_job)
            engine_states_by_external_keys[external_key] = engine_state
        return engine_states_by_external_keys

    def _engine_job_to_engine_state(self, engine_job=None):
        engine_state = {}
        if engine_job:
            engine_state['status'] = engine_job['status']
        return engine_state
