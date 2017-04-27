class BaseEngine(object):
    class JOB_STATUSES(object):
        RUNNING = 'RUNNING'
        COMPLETED = 'COMPLETED'
        FAILED = 'FAILED'
        UNKNOWN = 'UNKNOWN'

    class SubmissionError(Exception): pass

    def submit(self, submission=None):
        raise NotImplementedError

    def get_keyed_job_states(self, keyed_engine_metas=None):
        raise NotImplementedError
