import logging
import os
import time


class JobMan(object):
    class SubmissionError(Exception): pass

    def __init__(self, dao=None, engine=None, logger=None,
                 job_engine_states_ttl=120, submission_grace_period=None):
        self.dao = dao or self._generate_dao()
        self.engine = engine or self._generate_engine()
        self.logger = logger or logging
        self.job_engine_states_ttl = job_engine_states_ttl
        self.submission_grace_period = submission_grace_period or \
                (2 * self.job_engine_states_ttl)
        self.ensure_db()
        self._kvps = {}

    def _generate_dao(self):
        from .dao.sqlite_dao import SqliteDAO
        db_uri = os.path.join(os.path.expanduser('~'), '.jobman.sqlite.db')
        return SqliteDAO(db_uri=db_uri)

    def _generate_engine(self):
        from .engines.slurm_engine import SlurmEngine
        return SlurmEngine()

    def ensure_db(self):
        self.dao.ensure_db()

    def submit_job(self, submission=None):
        self.log_submission(submission=submission)
        try:
            engine_meta = self.engine.submit(submission=submission)
            job = self.create_job(job_kwargs={
                'submission': submission,
                'engine_meta': engine_meta,
                'status': 'RUNNING',
            })
            return job
        except Exception as exc:
            raise self.SubmissionError("Bad submission") from exc

    def log_submission(self, submission=None):
        self.logger.info("submit_job, {submission}".format(
            submission=submission))

    def create_job(self, job_kwargs=None):
        return self.dao.create_job(job_kwargs=job_kwargs)

    def get_jobs(self, job_keys=None):
        return self.dao.get_jobs(query={
            'filters': [
                {'field': 'job_key', 'operator': 'in', 'value': job_keys}
            ]
        })

    def update_jobs(self, force=False):
        if self.job_engine_states_are_stale() or force:
            self.update_job_engine_states(jobs=self.get_running_jobs())

    def get_running_jobs(self):
        return self.dao.get_jobs(query={
            'filters': [
                {'field': 'status', 'operator': '=', 'value': 'RUNNING'}
            ]
        })

    def job_engine_states_are_stale(self):
        job_engine_states_age = self.get_job_engine_states_age()
        return (job_engine_states_age is None) or \
                (job_engine_states_age >= self.job_engine_states_ttl)

    def get_job_engine_states_age(self):
        try:
            age = time.time() - self.get_kvp(key='job_engine_states_modified')
        except KeyError: age = None
        return age

    def get_kvp(self, key=None):
        # fallback to dao if not in _kvps.
        try:
            if key not in self._kvps:
                self._kvps[key] = self.dao.get_kvps(query={
                    'filters': [{'field': 'key', 'operator': '=', 'value': key}]
                })[0]
            return self._kvps[key]
        except Exception as exc:
            raise KeyError(key) from exc

    def set_kvp(self, key=None, value=None):
        self.dao.save_kvps(kvps=[{'key': key, 'value': value}])
        self._kvps[key] = value

    def update_job_engine_states(self, jobs=None):
        keyed_engine_states = self.engine.get_keyed_engine_states(
            keyed_engine_metas=self.get_keyed_engine_metas(jobs=jobs))
        for job in jobs:
            self.set_job_engine_state(
                job=job,
                job_engine_state=keyed_engine_states.get(job['job_key'])
            )
        self.dao.save_jobs(jobs=jobs)
        self.set_kvp(key='job_engine_states_modified', value=time.time())

    def get_keyed_engine_metas(self, jobs=None):
        keyed_engine_metas = {
            job['job_key']: job.get('engine_meta') for job in jobs
        }
        return keyed_engine_metas

    def set_job_engine_state(self, job=None, job_engine_state=None):
        if job_engine_state is None:
            if self.job_is_orphaned(job=job): job['status'] = 'COMPLETED'
        else:
            job['engine_state'] = job_engine_state
            job['status'] = job_engine_state['status']

    def job_is_orphaned(self, job=None):
        return (self.get_job_age(job=job) > self.submission_grace_period)

    def get_job_age(self, job=None):
        return time.time() - job['created']
