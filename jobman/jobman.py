import logging


class JobMan(object):

    class SubmissionError(Exception): pass

    def __init__(self, engine=None, logger=None):
        self.engine = engine
        self.logger = logger or logging

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
