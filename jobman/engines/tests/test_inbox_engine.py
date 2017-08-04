import unittest

from .. import jobman_inbox_engine


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self.engine = jobman_inbox_engine.JobManInboxEngine()


class SubmitJobTestCase(BaseTestCase):
    def test_copies_job_dir_to_inbox_dir(self):
        self.fail()


class GetKeyedEngineStatesTestCase(BaseTestCase):
    def test_queries_jobman_db_in_inbox_root(self):
        self.fail()
