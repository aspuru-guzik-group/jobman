import tempfile
import unittest

from .. import inbox_engine


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self.skipTest('todo')
        self.root_dir = tempfile.mkdtemp()
        self.engine = inbox_engine.InboxEngine(
            root_dir=self.root_dir,
            db_uri='sqlite://'
        )


class SubmitJobTestCase(BaseTestCase):
    def test_copies_job_dir_to_inbox_dir(self):
        self.fail()


class GetKeyedEngineStatesTestCase(BaseTestCase):
    def test_queries_jobman_db_in_inbox_root(self):
        self.fail()
