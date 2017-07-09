import unittest
from unittest.mock import call, MagicMock, patch

from .. import local_engine


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.engine = local_engine.LocalEngine()

    def mockify_engine_attrs(self, attrs=None):
        for attr in attrs: setattr(self.engine, attr, MagicMock())

    def mockify_module_attrs(self, attrs=None, module=local_engine):
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
                                         '_submit_jobdir'])
        self.module_mocks = self.mockify_module_attrs(attrs=['tempfile'])
        self.batch_job = MagicMock()
        self.subjobs = MagicMock()
        self.result = self.engine.submit_batch_job(batch_job=self.batch_job,
                                                   subjobs=self.subjobs)

    def test_makes_batch_jobdir(self):
        self.assertEqual(
            self.engine.build_batch_jobdir.call_args,
            call(batch_job=self.batch_job, subjobs=self.subjobs,
                 dest=self.module_mocks['tempfile'].mkdtemp.return_value)
        )

    def test_submits_batch_jobdir(self):
        expected_jobdir_meta = self.engine.build_batch_jobdir.return_value
        self.assertEqual(self.engine._submit_jobdir.call_args,
                         call(jobdir_meta=expected_jobdir_meta))

    def test_returns_submission_result(self):
        self.assertEqual(self.result, self.engine._submit_jobdir.return_value)
