import unittest
from unittest.mock import call, MagicMock

from ..local_engine import LocalEngine


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.engine = LocalEngine()

    def mockify_engine_attrs(self, attrs=None):
        for attr in attrs: setattr(self.engine, attr, MagicMock())

class SubmitBatchJobTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_engine_attrs(attrs=['_build_batch_jobdir',
                                         '_submit_jobdir'])
        self.batch_job = MagicMock()
        self.subjobs = MagicMock()
        self.result = self.engine.submit_batch_job(batch_job=self.batch_job,
                                                   subjobs=self.subjobs)

    def test_makes_batch_jobdir(self):
        self.assertEqual(self.engine._build_batch_jobdir.call_args,
                         call(batch_job=self.batch_job, subjobs=self.subjobs))

    def test_submits_batch_jobdir(self):
        expected_jobdir_meta = self.engine._build_batch_jobdir.return_value
        self.assertEqual(self.engine._submit_jobdir.call_args,
                         call(jobdir_meta=expected_jobdir_meta))

    def test_returns_submission_result(self):
        self.assertEqual(self.result, self.engine._submit_jobdir.return_value)

class _BuildBatchJobDir(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.engine.batch_jobdir_builder = MagicMock()
        self.batch_job = MagicMock()
        self.subjobs = MagicMock()
        self.result = self.engine._build_batch_jobdir(batch_job=self.batch_job,
                                                      subjobs=self.subjobs)

    def test_dispatches_to_batch_jobdir_builder(self):
        self.assertEqual(
            self.engine.batch_jobdir_builder.build_batch_jobdir.call_args,
            call(batch_job=self.batch_job, subjobs=self.subjobs)
        )
        self.assertEqual(
            self.result,
            self.engine.batch_jobdir_builder.build_batch_jobdir.return_value
        )
