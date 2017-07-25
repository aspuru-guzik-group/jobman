import unittest

from .. import jobman
from ..dao import sqlite_dao
from .mock_engine import MockEngine


class JobManE2ETest(unittest.TestCase):
    def setUp(self):
        self.dao = sqlite_dao.SqliteDAO()
        self.engine = MockEngine()
        self.jobman = jobman.JobMan(
            dao=self.dao,
            engine=self.engine,
            job_engine_states_ttl=10
        )

    def test_job_completions(self):
        job_specs = [{'some': 'job_spec'} for i in range(3)]
        jobs = [
            self.jobman.submit_job_spec(job_spec=job_spec,
                                        submit_to_engine_immediately=True)
            for job_spec in job_specs
        ]
        for i, job in enumerate(jobs):
            self.assertEqual(
                set(self._get_keys(self.jobman.get_running_jobs())),
                set(self._get_keys(jobs[i:]))
            )
            self.engine.complete_job(engine_meta=job['engine_meta'])
            self.jobman._update_job_engine_states(
                jobs=self.jobman.get_running_jobs())
        self.assertEqual(self._get_keys(self.jobman.get_running_jobs()), [])

    def _get_keys(self, items): return [item['key'] for item in items]

    def test_orphaned_job(self):
        self.jobman.submission_grace_period = 0
        job_specs = [{'some': 'job_spec'} for i in range(3)]
        jobs = [
            self.jobman.submit_job_spec(job_spec=job_spec,
                                        submit_to_engine_immediately=True)
            for job_spec in job_specs
        ]
        for i, job in enumerate(jobs):
            self.assertEqual(self._get_keys(self.jobman.get_running_jobs()),
                             self._get_keys(jobs[i:]))
            self.engine.unregister_job(engine_meta=job['engine_meta'])
            self.jobman._update_job_engine_states(
                jobs=self.jobman.get_running_jobs())
        self.assertEqual(self._get_keys(self.jobman.get_running_jobs()), [])
