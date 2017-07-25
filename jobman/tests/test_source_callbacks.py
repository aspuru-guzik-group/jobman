import unittest
from unittest.mock import MagicMock

from .. import jobman
from ..dao import sqlite_dao
from .mock_engine import MockEngine


class OnJobExecutedCallbackTestCase(unittest.TestCase):
    def setUp(self):
        self.dao = sqlite_dao.SqliteDAO()
        self.engine = MockEngine()
        self.source_key = 'my_source_key'
        self.source_cfg = {'on_job_executed': MagicMock()}
        self.jobman = jobman.JobMan(
            dao=self.dao,
            engine=self.engine,
            job_engine_states_ttl=10,
            source_cfgs={
                self.source_key: self.source_cfg
            }
        )

    def test_calls_callback(self):
        job_spec = {'some': 'job_spec'}
        job = self.jobman.submit_job_spec(job_spec=job_spec,
                                          source_key=self.source_key,
                                          submit_to_engine_immediately=True)
        self.engine.complete_job(engine_meta=job['engine_meta'])
        self.jobman._update_job_engine_states(jobs=[job])
        self.jobman.tick()
        self.assertEqual(
            self.source_cfg['on_job_executed'].call_args[1]['job']['key'],
            job['key']
        )
