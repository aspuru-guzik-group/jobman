import logging
import time


class JobMan(object):

    class SubmissionError(Exception): pass

    def __init__(self, db=None, engine=None, logger=None, job_records_ttl=120,
                 submission_grace_period=None):
        self.db = db
        self.engine = engine
        self.logger = logger or logging
        self.job_records_ttl = job_records_ttl
        self.submission_grace_period = submission_grace_period or \
                (2 * self.job_records_ttl)

    def submit_job(self, submission=None):
        self.log_submission(submission=submission)
        try:
            submission_meta = self.engine.submit(submission=submission)
            job_record = self.create_job_record(job_kwargs={
                'submission_meta': submission_meta
            })
            return job_record
        except Exception as exc:
            raise self.SubmissionError("Bad submission") from exc

    def log_submission(self, submission=None):
        self.logger.info("submit_job, {submission}".format(
            submission=submission))

    def create_job_record(self, job_kwargs=None):
        return self.db.create_job_record(job_kwargs=job_kwargs)

    def get_job_record(self, job_key=None):
        self.update_job_records()
        return self.db.get_job_record(job_key=job_key)

    def update_job_records(self):
        if self.job_records_are_stale(): self._update_job_records()

    def job_records_are_stale(self):
        job_records_age = self.get_job_records_age()
        return (job_records_age is None) or \
                (job_records_age >= self.job_records_ttl)

    def get_job_records_age(self):
        return time.time() - self.get_job_records_modified_time()

    def get_job_records_modified_time(self):
        if not hasattr(self, '_job_records_modified_time'):
            self._job_records_modified_time = \
                    self.db.get_job_records_modified_time()
        return self._job_records_modified_time

    def _update_job_records(self):
        running_job_records = self.db.get_running_job_records()
        engine_job_states = self.engine.get_job_states(
            job_records=running_job_records)
        for job_record in running_job_records:
            self.update_job_record_from_engine_job_states(
                job_record=job_record,
                engine_job_states=engine_job_states)
        self.set_job_records_modified_time(value=time.time())

    def set_job_records_modified_time(self, value=None):
        self._job_records_modified_time = value
        self.db.set_job_records_modified_time(value=value)

    def update_job_record_from_engine_job_states(self, job_record=None,
                                                 engine_job_states=None):
        engine_job_state = engine_job_states.get(job_record['job_key'])
        if engine_job_state:
            job_record['status'] = engine_job_state['status']
            job_record['engine_job_state'] = engine_job_state
            self.db.save_job_record(job_record=job_record)
        else: self.process_orphan_job_record(job_record=job_record)

    def process_orphan_job_record(self, job_record=None):
        job_record_age = self.get_job_record_age(job_record=job_record)
        if job_record_age > self.submission_grace_period:
            job_record['status'] = 'COMPLETED'
            self.db.save_job_record(job_record=job_record)

    def get_job_record_age(self, job_record=None):
        return time.time() - job_record['created']
