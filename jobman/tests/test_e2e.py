import unittest
import uuid

from .. import jobman
from ..dao import sqlite_dao


class MockEngine(object):
    def __init__(self):
        self.jobs = {}

    def submit(self, submission=None):
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
        submissions = [{'some': 'submission'} for i in range(3)]
        jobs = [self.jobman.submit_job(submission=submission)
                for submission in submissions]
        for i, job in enumerate(jobs):
            self.assertEqual(
                set(self._get_job_keys(self.jobman.get_running_jobs())),
                set(self._get_job_keys(jobs[i:]))
            )
            self.engine.complete_job(engine_meta=job['engine_meta'])
            self.jobman.update_job_engine_states(jobs=jobs, force=True)
        self.assertEqual(self._get_job_keys(self.jobman.get_running_jobs()), [])

    def _get_job_keys(self, jobs):
        return [job['job_key'] for job in jobs]

    def test_orphaned_job(self):
        self.jobman.submission_grace_period = 0
        submissions = [{'some': 'submission'} for i in range(3)]
        jobs = [self.jobman.submit_job(submission=submission)
                for submission in submissions]
        for i, job in enumerate(jobs):
            self.assertEqual(self._get_job_keys(self.jobman.get_running_jobs()),
                             self._get_job_keys(jobs[i:]))
            self.engine.unregister_job(engine_meta=job['engine_meta'])
            self.jobman.update_job_engine_states(jobs=jobs, force=True)
        self.assertEqual(self._get_job_keys(self.jobman.get_running_jobs()), [])
