import logging
import os
import time


class JobMan(object):
    class SubmissionError(Exception): pass

    CFG_PARAMS = ['dao', 'jobman_db_uri', 'engine', 'max_running_jobs',
                  'job_engine_states_ttl', 'submission_grace_period']

    @classmethod
    def from_cfg(cls, cfg=None):
        params_from_cfg = {param: getattr(cfg, param, None)
                           for param in cls.CFG_PARAMS}
        return JobMan(**params_from_cfg)

    def __init__(self, dao=None, jobman_db_uri=None, engine=None,
                 logging_cfg=None, max_running_jobs=None,
                 job_engine_states_ttl=None, submission_grace_period=None):
        self.logger = self._generate_logger(logging_cfg=logging_cfg)
        self.dao = dao or self._generate_dao(jobman_db_uri=jobman_db_uri)
        self.engine = engine or self._generate_engine()
        if max_running_jobs is None: max_running_jobs = 50
        self.max_running_jobs = max_running_jobs
        if job_engine_states_ttl is None: job_engine_states_ttl = 120
        self.job_engine_states_ttl = job_engine_states_ttl
        self.submission_grace_period = submission_grace_period or \
                (2 * self.job_engine_states_ttl)
        self.ensure_db()
        self._kvps = {}

    def _generate_logger(self, logging_cfg=None):
        logging_cfg = logging_cfg or {}
        logger = logging_cfg.get('logger') or \
                logging.getLogger('jobman_%s' % id(self))

        log_file = logging_cfg.get('log_file')
        if log_file: logger.addHandler(
            logging.FileHandler(os.path.expanduser(log_file)))

        level = logging_cfg.get('level')
        if level:
            logger.setLevel(getattr(logging, level))

        fmt = logging_cfg.get('fmt') or \
                '| %(asctime)s | %(name)s | %(levelname)s |\n%(message)s\n'
        formatter = logging.Formatter(fmt)
        for handler in logger.handlers: handler.setFormatter(formatter)

        return logger

    def _generate_dao(self, jobman_db_uri=None):
        jobman_db_uri = jobman_db_uri or os.path.expanduser(
            '~/jobman.sqlite.db')
        from .dao.sqlite_dao import SqliteDAO
        return SqliteDAO(db_uri=jobman_db_uri, logger=self.logger)

    def _generate_engine(self):
        from .engines.slurm_engine import SlurmEngine
        return SlurmEngine(logger=self.logger)

    def ensure_db(self):
        self.dao.ensure_db()

    def submit_job(self, submission=None, source=None, source_meta=None):
        self.log_submission(submission=submission)
        try:
            engine_meta = self.engine.submit(submission=submission)
            job = self.create_job(job_kwargs={
                'submission': submission,
                'engine_meta': engine_meta,
                'source': source,
                'source_meta': source_meta,
                'status': 'RUNNING',
            })
            return job
        except Exception as exc:
            raise self.SubmissionError("Bad submission") from exc

    def log_submission(self, submission=None):
        self.logger.info("submit_job, {submission}".format(
            submission=submission))

    @property
    def num_free_slots(self):
        return self.max_running_jobs - len(self.get_running_jobs())

    def create_job(self, job_kwargs=None):
        return self.dao.create_job(job_kwargs=job_kwargs)

    def get_jobs(self, query=None): return self.dao.get_jobs(query=query)

    def save_jobs(self, jobs=None): return self.dao.save_jobs(jobs=jobs)

    def get_running_jobs(self):
        return self.get_jobs(query={
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
                })[0]['value']
            return self._kvps[key]
        except Exception as exc:
            raise KeyError(key) from exc

    def set_kvp(self, key=None, value=None):
        self.dao.save_kvps(kvps=[{'key': key, 'value': value}])
        self._kvps[key] = value

    def update_job_engine_states(self, jobs=None, force=False, query=None):
        if not jobs and query:
            jobs = self.filter_for_incomplete_status(
                items=self.get_jobs(query=query))
        if not jobs: return
        if force or self.job_engine_states_are_stale():
            self._update_job_engine_states(jobs=jobs)

    def filter_for_incomplete_status(self, items=None):
        if not items: return
        def incomplete_status_filter(item):
            return item.get('status') != 'COMPLETED'
        return list(filter(incomplete_status_filter, items))

    def _update_job_engine_states(self, jobs=None):
        keyed_engine_states = self.engine.get_keyed_engine_states(
            keyed_engine_metas=self.get_keyed_engine_metas(jobs=jobs))
        for job in jobs:
            self.set_job_engine_state(
                job=job,
                job_engine_state=keyed_engine_states.get(job['key'])
            )
        self.dao.save_jobs(jobs=jobs)
        self.set_kvp(key='job_engine_states_modified', value=time.time())

    def get_keyed_engine_metas(self, jobs=None):
        jobs = jobs or []
        keyed_engine_metas = {
            job['key']: job.get('engine_meta') for job in jobs
        }
        return keyed_engine_metas

    def set_job_engine_state(self, job=None, job_engine_state=None):
        if job_engine_state is None:
            if self.job_is_orphaned(job=job): job['status'] = 'EXECUTED'
        else:
            job['engine_state'] = job_engine_state
            job['status'] = job_engine_state.get('status')

    def job_is_orphaned(self, job=None):
        return (self.get_job_age(job=job) > self.submission_grace_period)

    def get_job_age(self, job=None):
        return time.time() - job['created']

    def flush(self):
        self.dao.flush()
