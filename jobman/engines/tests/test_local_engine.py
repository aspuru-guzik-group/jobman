import os
import textwrap
import unittest
from unittest.mock import call, MagicMock, patch

from .. import local_engine


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self.engine = local_engine.LocalEngine(
            process_runner=MagicMock(),
            sqlite=MagicMock()
        )
        self.job = MagicMock()

    def mockify_engine_attrs(self, attrs=None):
        for attr in attrs: setattr(self.engine, attr, MagicMock())

    def mockify_module_attrs(self, attrs=None, module=local_engine):
        mocks = {}
        for attr in attrs:
            patcher = patch.object(module, attr)
            self.addCleanup(patcher.stop)
            mocks[attr] = patcher.start()
        return mocks

class SubmitJobTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_engine_attrs(attrs=['_generate_job_id',
                                         '_write_engine_entrypoint',
                                         '_execute_engine_entrypoint'])
        self.result = self.engine.submit_job(job=self.job)

    def test_generates_job_id(self):
        self.assertEqual(self.engine._generate_job_id.call_args, call())

    def test_writes_engine_entrypoint(self):
        self.assertEqual(
            self.engine._write_engine_entrypoint.call_args,
            call(job=self.job, job_id=self.engine._generate_job_id.return_value)
        )

    def test_executes_engine_entrypoint(self):
        self.assertEqual(
            self.engine._execute_engine_entrypoint.call_args,
            call(entrypoint_path=(self.engine._write_engine_entrypoint
                                  .return_value),
                 job=self.job,
                 job_id=self.engine._generate_job_id.return_value)
        )

    def test_returns_engine_meta(self):
        self.assertEqual(self.result,
                         {'job_id': self.engine._generate_job_id.return_value})

class _WriteEngineEntrypointTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_engine_attrs(attrs=['_generate_engine_entrypoint_content'])
        self.module_mocks = self.mockify_module_attrs(attrs=['open', 'os'])
        self.job_id = MagicMock()
        self.result = self.engine._write_engine_entrypoint(job=self.job,
                                                           job_id=self.job_id)
        self.expected_path = self.module_mocks['os'].path.join.return_value

    def test_generates_content(self):
        self.assertEqual(
            self.engine._generate_engine_entrypoint_content.call_args,
            call(job=self.job, job_id=self.job_id)
        )

    def test_generates_expected_path(self):
        self.assertEqual(
            self.module_mocks['os'].path.join.call_args,
            call(self.job['job_spec']['dir'],
                 self.engine.ENGINE_ENTRYPOINT_TPL.format(job_id=self.job_id))
        )

    def test_writes_content_to_entrypoint_path(self):
        self.assertEqual(self.module_mocks['open'].call_args,
                         call(self.expected_path, 'w'))
        self.assertEqual(
            (self.module_mocks['open'].return_value.__enter__.return_value
             .write.call_args),
            call(self.engine._generate_engine_entrypoint_content.return_value)
        )

    def test_returns_path(self):
        self.assertEqual(self.result, self.expected_path)

class _GenerateEngineEntrypointContentTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_engine_attrs(attrs=[
            '_generate_engine_entrypoint_preamble'])
        self.job_id = MagicMock()
        self.result = self.engine._generate_engine_entrypoint_content(
            job=self.job, job_id=self.job_id)

    def test_generates_preamble(self):
        self.assertEqual(
            self.engine._generate_engine_entrypoint_preamble.call_args,
            call(job=self.job, job_id=self.job_id)
        )

    def test_returns_expected_content(self):
        expected_content = textwrap.dedent(
            '''
            #!/bin/bash
            {preamble}
            pushd {jobdir} && {job_entrypoint}; popd;
            '''
        ).format(
            preamble=(self.engine._generate_engine_entrypoint_preamble
                      .return_value),
            jobdir=self.job['job_spec']['dir'],
            job_entrypoint=self.job['job_spec']['entrypoint'],
        )
        self.assertEqual(self.result, expected_content)

class _GenerateEngineEntrypointPreambleTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.job_id = MagicMock()
        self.mockify_engine_attrs(attrs=['_generate_env_vars_for_cfg_specs'])
        self.result = self.engine._generate_engine_entrypoint_preamable(
            job=self.job, job_id=self.job_id)

    def test_generates_env_vars_for_cfg_specs(self):
        self.assertEqual(self.engine._generate_env_vars_for_cfg_specs.call_args,
                         call(job=self.job))

    def test_returns_expected_preamable_content(self):
        self.assertEqual(
            self.result,
            self.engine._generate_env_vars_for_cfg_specs.return_value
        )

class _GenerateEnvVarsForCfgSpecsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_engine_attrs(attrs=['resolve_job_cfg_specs',
                                         '_kvp_to_env_var_block'])
        self.engine.resolve_job_cfg_specs.return_value = {'key_i': MagicMock()
                                                          for i in range(3)}
        self.engine._kvp_to_env_var_block.return_value = 'some_block'
        self.result = self.engine._generate_env_vars_for_cfg_specs(job=self.job)

    def test_resolves_cfg_specs(self):
        self.assertEqual(self.engine.resolve_job_cfg_specs.call_args,
                         call(job=self.job))

    def test_writes_resolved_specs_as_env_vars(self):
        expected_resolved_cfgs = self.engine.resolve_job_cfg_specs.return_value
        self.assertEqual(
            self.engine._kvp_to_env_var_block.call_args_list,
            [call(kvp={'key': k, 'value': v})
             for k, v in expected_resolved_cfgs.items()]
        )
        self.assertEqual(
            self.result,
            "\n".join([self.engine._kvp_to_env_var_block.return_value
                       for cfg_item in expected_resolved_cfgs.items()])
        )

class _KvpToEnvVarBlock(BaseTestCase):
    def test_generates_expected_block(self):
        kvp = {'key': 'SOME_KEY', 'value': 'SOME_VALUE'}
        result = self.engine._kvp_to_env_var_block(kvp=kvp)
        expected_block = textwrap.dedent(
            '''
            read -d '' {key} << EOF
            {value}
            EOF
            '''
        ).lstrip().format(key=kvp['key'], value=kvp['value'].lstrip())
        self.assertEqual(result, expected_block)

class _ExecuteEngineEntrypointTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_engine_attrs(attrs=['_generate_engine_entrypoint_cmd',
                                         '_execute_engine_entrypoint_cmd'])
        self.job_id = MagicMock()
        self.entrypoint_path = MagicMock()
        self.engine._execute_engine_entrypoint(
            entrypoint_path=self.entrypoint_path,
            job=self.job,
            job_id=self.job_id
        )

    def test_generates_entrypoint_cmd(self):
        self.assertEqual(
            self.engine._generate_engine_entrypoint_cmd.call_args,
            call(entrypoint_path=self.entrypoint_path,
                 job=self.job,
                 job_id=self.job_id
                )
        )

    def test_executes_entrypoint_cmd(self):
        self.assertEqual(
            self.engine._execute_engine_entrypoint_cmd.call_args,
            call(
                entrypoint_cmd=(self.engine._generate_engine_entrypoint_cmd
                                .return_value),
                job=self.job,
                job_id=self.job_id
            )
        )

class _GenerateEngineEntrypointCmdTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.entrypoint_path = '/some/dir/some/entrypoint'
        self.job_id = MagicMock()
        self.result = self.engine._generate_engine_entrypoint_cmd(
            entrypoint_path=self.entrypoint_path, job=self.job,
            job_id=self.job_id)

    def test_returns_expected_cmd(self):
        expected_cmd = (
            'pushd {entrypoint_dir}' 
            ' && {entrypoint_path} {stdout_redirect} {stderr_redirect}'
            'popd;'
        ).format(
            entrypoint_dir=os.path.dirname(self.entrypoint_path),
            entrypoint_path=self.entrypoint_path,
            **self.engine._get_std_log_redirects(job=self.job)
        )
        self.assertEqual(self.result, expected_cmd)

class _GetStdLogRedirectsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_engine_attrs(attrs=['_get_std_log_paths'])
        self.engine._get_std_log_paths.return_value = {
            'stdout': '/some/path/for/stdout',
            'stderr': '/some/path/for/stderr',
        }
        self.result = self.engine._get_std_log_redirects(job=self.job)

    def test_returns_expected_redirects(self):
        expected_std_log_paths = self.engine._get_std_log_paths(job=self.job)
        expected_redirects = {
            'stdout_redirect': '>> %s' % expected_std_log_paths['stdout'],
            'stderr_redirect': '2>> %s' % expected_std_log_paths['stderr'],
        }
        self.assertEqual(self.result, expected_redirects)

class _GetStdLogPathsestCase(BaseTestCase):
    def test_returns_expected_paths(self):
        self.job = {
            'job_spec': {
                'dir': 'some_dir',
                'std_log_file_names': {
                    'log_%s' % i: 'log_%s_file_name_' % i for i in range(3)
                }
            }
        }
        result = self.engine._get_std_log_paths(job=self.job)
        expected_paths = {
            log_key: os.path.join(self.job['job_spec']['dir'], log_file_name)
            for log_key, log_file_name in self.job['job_spec'].get(
                'std_log_file_names', {}).items()
        }
        self.assertEqual(result, expected_paths)

class _ExecuteEngineEntrypointCmdTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.entrypoint_cmd = MagicMock()
        self.job_id = MagicMock()

    def _execute_engine_entrypoint_cmd(self):
        self.engine._execute_engine_entrypoint_cmd(
            entrypoint_cmd=self.entrypoint_cmd, 
            job=self.job,
            job_id=self.job_id
        )

    def test_runs_cmd(self):
        self._execute_engine_entrypoint_cmd()
        self.assertEqual(self.engine.process_runner.run_process.call_args,
                         call(cmd=self.entrypoint_cmd, check=True, shell=True))

    def test_inserts_job_id_into_db(self):
        self._execute_engine_entrypoint_cmd()
        self.assertEqual(
            self.engine.conn.cursor.return_value.execute.call_args,
            call("INSERT INTO jobs VALUES (?, ?)",
                 (self.job_id, self.engine.JOB_STATUSES.EXECUTED,))
        )

    def test_raises_submission_error_for_execution_error(self):
        class MockProcessError(Exception):
            stdout = 'some stdout'
            stderr = 'some stderr'

        self.engine.process_runner.CalledProcessError = MockProcessError
        self.engine.process_runner.run_process.side_effect = \
                self.engine.process_runner.CalledProcessError
        with self.assertRaises(self.engine.SubmissionError):
            self._execute_engine_entrypoint_cmd()
