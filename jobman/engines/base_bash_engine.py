import os
import textwrap

from .base_engine import BaseEngine


class BaseBashEngine(BaseEngine):
    ENGINE_ENTRYPOINT_TPL = 'JOBMAN.ENTRYPOINT.sh'

    def _write_engine_entrypoint(self, job=None, extra_cfgs=None):
        entrypoint_content = self._generate_engine_entrypoint_content(
            job=job, extra_cfgs=extra_cfgs)
        entrypoint_path = os.path.join(job['job_spec']['dir'],
                                       self.ENGINE_ENTRYPOINT_TPL)
        with open(entrypoint_path, 'w') as f:
            f.write(entrypoint_content)
        os.chmod(entrypoint_path, 0o755)
        return entrypoint_path

    def _generate_engine_entrypoint_content(self, job=None, extra_cfgs=None):
        return textwrap.dedent(
            '''
            #!/bin/bash
            {preamble}
            pushd "{jobdir}" > /dev/null && ./{job_entrypoint}
            RESULT=$?
            if [ $RESULT -eq 0 ]; then
                touch {completed_checkpoint}
            else
                touch {failed_checkpoint}
            fi
            popd > /dev/null
            '''
        ).lstrip().format(
            preamble=self._generate_engine_entrypoint_preamble(
                job=job, extra_cfgs=extra_cfgs),
            jobdir=job['job_spec']['dir'],
            job_entrypoint=job['job_spec']['entrypoint'],
            completed_checkpoint=self.CHECKPOINT_FILE_NAMES['completed'],
            failed_checkpoint=self.CHECKPOINT_FILE_NAMES['failed'],
        )

    def _generate_engine_entrypoint_preamble(self, job=None, extra_cfgs=None):
        preamble = textwrap.dedent(
            '''
            {engine_preamble}
            {env_vars_for_cfg_specs}
            '''
        ).lstrip().format(
            engine_preamble=self.resolve_cfg_item(key='ENGINE_PREAMBLE',
                                                  spec={'default': ''}),
            env_vars_for_cfg_specs=self._generate_env_vars_for_cfg_specs(
                job=job, extra_cfgs=extra_cfgs)
        )
        return preamble

    def _generate_env_vars_for_cfg_specs(self, job=None, extra_cfgs=None):
        resolved_cfgs = self.resolve_job_cfg_specs(
            job=job, extra_cfgs=extra_cfgs)
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
            export {key}=${key}
            '''
        ).lstrip().format(key=kvp['key'], value=kvp['value'].lstrip())

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

    def _get_std_log_contents(self, job=None):
        std_log_contents = {}
        for log_name, log_path in self._get_std_log_paths(job=job).items():
            log_content = ''
            try:
                with open(log_path) as f:
                    log_content = f.read()
            except Exception as exc:
                log_content = "COULD NOT READ LOG '{log_name}': {exc}'".format(
                    log_name=log_name, exc=exc)
            std_log_contents[log_name] = log_content
        return std_log_contents
