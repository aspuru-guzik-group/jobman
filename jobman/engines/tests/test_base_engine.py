import unittest
from unittest.mock import call, MagicMock, patch

from .. import base_engine


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.engine = base_engine.BaseEngine()

    def mockify_engine_attrs(self, attrs=None):
        for attr in attrs: setattr(self.engine, attr, MagicMock())

    def mockify_module_attrs(self, attrs=None, module=base_engine):
        mocks = {}
        for attr in attrs:
            patcher = patch.object(module, attr)
            self.addCleanup(patcher.stop)
            mocks[attr] = patcher.start()
        return mocks

class SubmitBatchJobTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_engine_attrs(attrs=['build_batch_jobdir',
                                         'submit_job'])
        self.module_mocks = self.mockify_module_attrs(attrs=['tempfile'])
        self.batch_job = MagicMock()
        self.subjobs = MagicMock()
        self.extra_cfgs = MagicMock()
        self.result = self.engine.submit_batch_job(batch_job=self.batch_job,
                                                   subjobs=self.subjobs,
                                                   extra_cfgs=self.extra_cfgs)

    def test_makes_batch_jobdir(self):
        self.assertEqual(
            self.engine.build_batch_jobdir.call_args,
            call(batch_job=self.batch_job, subjobs=self.subjobs,
                 dest=self.module_mocks['tempfile'].mkdtemp.return_value,
                 extra_cfgs=self.extra_cfgs)
        )

    def test_submits_batch_jobdir(self):
        expected_job_spec = self.engine.build_batch_jobdir.return_value
        self.assertEqual(self.engine.submit_job.call_args,
                         call(job={'job_spec': expected_job_spec},
                              extra_cfgs=self.extra_cfgs))

    def test_returns_submission_result(self):
        self.assertEqual(self.result, self.engine.submit_job.return_value)

class ResolveJobCfgSpecsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_engine_attrs(attrs=['resolve_cfg_item'])
        self.module_mocks = self.mockify_module_attrs(attrs=['os'])
        self.cfg_specs = {'key_%s' % i: MagicMock() for i in range(3)}
        self.cfg_specs['sans_output_key'] = {'required': False}
        self.cfg_specs['with_output_key'] = {'output_key': 'some_output_key'}
        self.job = {
            'job_spec': {
                'cfg': MagicMock(), 'cfg_specs': self.cfg_specs
            }
        }
        self.extra_cfgs = [MagicMock() for i in range(3)]
        self.result = self.engine.resolve_job_cfg_specs(
            job=self.job, extra_cfgs=self.extra_cfgs)

    def test_dispatches_to_resolve_cfg_item(self):
        expected_cfgs = [self.job['job_spec'].get('cfg', {}), self.engine.cfg,
                         self.module_mocks['os'].environ, *(self.extra_cfgs)]
        self.assertEqual(
            self.engine.resolve_cfg_item.call_args_list,
            [
                call(key=key, spec=spec, cfgs=expected_cfgs)
                for key, spec in self.cfg_specs.items()
            ]
        )
        expected_resolved_cfg_specs = {}
        for key, spec in self.cfg_specs.items():
            output_key = spec.get('output_key') or key
            expected_resolved_cfg_specs[output_key] = \
                self.engine.resolve_cfg_item.return_value
        self.assertEqual(self.result, expected_resolved_cfg_specs)

class ResolveCfgItemTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.cfg_key = MagicMock()
        self.cfg_spec = MagicMock()
        self.cfgs = [MagicMock() for i in range(3)]
        self.mockify_engine_attrs(attrs=['_get_from_first_matching_src'])

    def _resolve_cfg_spec(self):
        return self.engine.resolve_cfg_item(
            key=self.cfg_key, spec=self.cfg_spec, cfgs=self.cfgs)

    def test_dispatches_to_get_from_first_matching_src(self):
        result = self._resolve_cfg_spec()
        self.assertEqual(
            self.engine._get_from_first_matching_src.call_args,
            call(key=self.cfg_key, srcs=self.cfgs)
        )
        self.assertEqual(
            result, self.engine._get_from_first_matching_src.return_value)

    def test_raises_if_key_error(self):
        self.engine._get_from_first_matching_src.side_effect = KeyError
        with self.assertRaises(self.engine.CfgItemResolutionError):
            self._resolve_cfg_spec()

class _GetFromFirstMatchingSourceTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_engine_attrs(attrs=['_get_key_or_attr'])
        self.key = 'some_key'
        self.srcs = [MagicMock() for i in range(3)]
        def _mock_get_key_or_attr(src=None, key=None): raise KeyError
        self.mock_get_key_or_attr = _mock_get_key_or_attr

    def _get_from_first_matching_src(self, **kwargs):
        self.engine._get_key_or_attr.side_effect = self.mock_get_key_or_attr
        return self.engine._get_from_first_matching_src(
            key=self.key, srcs=self.srcs, **kwargs)

    def test_returns_from_first_matching_src(self):
        def _mock_get_key_or_attr(src=None, key=None):
            if src is self.srcs[-2]: return src[key]
            else: raise KeyError
        self.mock_get_key_or_attr = _mock_get_key_or_attr
        self.assertEqual(self._get_from_first_matching_src(),
                         self.srcs[-2][self.key])

    def test_returns_default_if_no_matching_src(self):
        default = MagicMock()
        self.assertEqual(self._get_from_first_matching_src(default=default),
                         default)

    def test_raises_if_no_matching_src_and_no_default(self):
        with self.assertRaises(KeyError): self._get_from_first_matching_src()

