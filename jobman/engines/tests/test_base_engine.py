import types
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
        self.cfg_specs = {'key_%s' % i: MagicMock() for i in range(3)}
        self.job = {
            'job_spec': {
                'cfg': MagicMock(), 'cfg_specs': self.cfg_specs
            }
        }
        self.extra_cfgs = [MagicMock() for i in range(3)]
        self.result = self.engine.resolve_job_cfg_specs(
            job=self.job, extra_cfgs=self.extra_cfgs)

    def test_dispatches_to_resolve_cfg_item(self):
        expected_extra_cfgs = [*self.extra_cfgs,
                               self.job['job_spec'].get('cfg')]
        self.assertEqual(
            self.engine.resolve_cfg_item.call_args_list,
            [
                call(key=key, spec=spec, extra_cfgs=expected_extra_cfgs)
                for key, spec in self.cfg_specs.items()
            ]
        )
        self.assertEqual(
            self.result,
            {
                key: self.engine.resolve_cfg_item.return_value
                for key in self.cfg_specs
            }
        )

    def resolve_job_cfg_specs(self, job=None, extra_cfgs=None):
        extra_cfgs = ((extra_cfgs or []) + job['job_spec'].get('cfg', []))
        resolved_cfg_items = {}
        for key, spec in job['job_spec'].get('cfg_specs', {}).items():
            resolved_cfg_items[key] = self.resolve_cfg_item(
                key=key, spec=spec, extra_cfgs=extra_cfgs)
        return resolved_cfg_items

class ResolveCfgItemTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.cfg_key = MagicMock()
        self.cfg_spec = MagicMock()
        self.extra_cfgs = [MagicMock() for i in range(3)]
        self.module_mocks = self.mockify_module_attrs(attrs=['os'])
        self.mockify_engine_attrs(attrs=['_get_from_first_matching_src'])

    def _resolve_cfg_spec(self):
        return self.engine.resolve_cfg_item(
            key=self.cfg_key, spec=self.cfg_spec, extra_cfgs=self.extra_cfgs)

    def test_dispatches_to_get_from_first_matching_src(self):
        result = self._resolve_cfg_spec()
        expected_srcs = self.extra_cfgs + [
            self.engine.cfg, self.module_mocks['os'].environ]
        self.assertEqual(
            self.engine._get_from_first_matching_src.call_args,
            call(key=self.cfg_key, srcs=expected_srcs)
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
        self.key = 'some_key'
        self.dict_srcs = [{} for i in range(3)]
        self.attr_srcs = [types.SimpleNamespace() for i in range(3)]
        self.srcs = self.dict_srcs + self.attr_srcs

    def _get_from_first_matching_src(self, **kwargs):
        return self.engine._get_from_first_matching_src(
            key=self.key, srcs=self.srcs, **kwargs)

    def test_returns_from_first_matching_src(self):
        self.dict_srcs[-2][self.key] = MagicMock()
        self.assertEqual(self._get_from_first_matching_src(),
                         self.dict_srcs[-2][self.key])

    def test_matches_for_attr_srcs(self):
        setattr(self.attr_srcs[-2], self.key, MagicMock())
        self.assertEqual(self._get_from_first_matching_src(),
                         getattr(self.attr_srcs[-2], self.key))

    def test_returns_default_if_no_matching_src(self):
        default = MagicMock()
        self.assertEqual(
            self._get_from_first_matching_src(default=default),
            default
        )

    def test_raises_if_no_matching_src_and_no_default(self):
        with self.assertRaises(KeyError): self._get_from_first_matching_src()

