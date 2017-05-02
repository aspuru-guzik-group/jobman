import logging
import time


class JobMan(object):

    class SubmissionError(Exception): pass

    def __init__(self, dao=None, engine=None, logger=None,
                 job_engine_states_ttl=120, submission_grace_period=None):
        self.dao = dao
        self.engine = engine
        self.logger = logger or logging
        self.job_engine_states_ttl = job_engine_states_ttl
        self.submission_grace_period = submission_grace_period or \
                (2 * self.job_engine_states_ttl)
        self._kvps = {}

    def submit_job(self, submission=None):
        self.log_submission(submission=submission)
        try:
            submission_meta = self.engine.submit(submission=submission)
            job = self.create_job(job_kwargs={
                'submission_meta': submission_meta
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

    def update_jobs(self):
        if self.job_engine_states_are_stale():
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
        return time.time() - self.get_kvp(key='job_engine_states_modified')

    def get_kvp(self, key=None):
        # fallback to dao if not in _kvps.
        if key not in self._kvps: self._kvps[key] = self.dao.get_kvp(key=key)
        return self._kvps[key]

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
