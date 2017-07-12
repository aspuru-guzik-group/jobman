import unittest
import uuid

from .. import jobman
from ..dao import sqlite_dao


class MockEngine(object):
    def __init__(self):
        self.jobs = {}

    def submit_job(self, job=None, extra_cfgs=None):
        engine_meta = str(uuid.uuid4())
        self.jobs[engine_meta] = {'status': 'RUNNING'}
        return engine_meta

    def get_keyed_engine_states(self, keyed_engine_metas=None):
        return {
            key: self.jobs.get(engine_meta)
            for key, engine_meta in keyed_engine_metas.items()
        }

    def complete_job(self, engine_meta=None):
        self.jobs[engine_meta]['status'] = 'COMPLETED'

    def unregister_job(self, engine_meta=None):
        del self.jobs[engine_meta]

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
