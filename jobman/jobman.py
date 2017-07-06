import logging
import os
import time

from . import debug_utils


class JobMan(object):
    class SubmissionError(Exception): pass

    CFG_PARAMS = ['dao', 'jobman_db_uri', 'engine', 'max_running_jobs',
                  'job_engine_states_ttl', 'submission_grace_period',
                  'batchable_filters']

    @classmethod
    def from_cfg(cls, cfg=None):
        params_from_cfg = {param: getattr(cfg, param, None)
                           for param in cls.CFG_PARAMS}
        return JobMan(**params_from_cfg)

    def __init__(self, logger=None, logging_cfg=None, debug=None,
                 setup=True, **kwargs):
        self.logger = self._generate_logger(logging_cfg=logging_cfg)
        self.debug = debug
        if setup: self._setup(**kwargs)

    def _setup(self, dao=None, jobman_db_uri=None, engine=None,
               max_running_jobs=None, batchable_filters=...,
               job_engine_states_ttl=None, submission_grace_period=None,
               **kwargs):
        self.dao = dao or self._generate_dao(jobman_db_uri=jobman_db_uri)
        self.engine = engine or self._generate_engine()
        if max_running_jobs is None: max_running_jobs = 50
        self.max_running_jobs = max_running_jobs
        if job_engine_states_ttl is None: job_engine_states_ttl = 120
        self.job_engine_states_ttl = job_engine_states_ttl
        self.submission_grace_period = submission_grace_period or \
                (2 * self.job_engine_states_ttl)
        if batchable_filters is ...: batchable_filters = []
        self.batchable_filters = (batchable_filters
                                  + self._get_default_batchable_filters())
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

    def _debug_locals(self):
        if self.debug: debug_utils.debug_locals(logger=self.logger)

    def _get_default_batchable_filters(self):
        def submission_has_batchable(job=None):
            return bool(job.get('submission', {}).get('batchable'))
        return [submission_has_batchable]

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

    def submit(self, submission=None, source=None, source_meta=None):
        self._debug_locals()
        try:
            job = self._submission_to_job(submission=submission,
                                          source=source,
                                          source_meta=source_meta)
            job['batchable'] = self._job_is_batchable(job=job)
            created_job = self._create_job(job=job)
            return created_job['key']
        except Exception as exc:
            raise self.SubmissionError() from exc

    def _submission_to_job(self, submission=None, source=None,
                           source_meta=None):
        return {
            'submission': submission,
            'source': source,
            'source_meta': source_meta,
            'status': 'PENDING',
        }

    def _job_is_batchable(self, job=None):
        for filter_ in self.batchable_filters:
            if filter_(job): return True
        return False

    def _create_job(self, job=None): return self.dao.create_job(job=job)

    @property
    def num_free_slots(self):
        return self.max_running_jobs - len(self.get_running_jobs())

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
        try: age = time.time() - self.get_kvp(key='job_engine_states_modified')
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

    def tick(self):
        self._update_stale_engine_states_for_running_jobs()
        self._process_executed_jobs()
        self._process_batchable_jobs()
        self._process_submittable_jobs()

    def _update_job_engine_states(self, jobs=None):
        keyed_engine_states = self.engine.get_keyed_engine_states(
            keyed_engine_metas=self._get_keyed_engine_metas(jobs=jobs))
        for job in jobs:
            self._set_job_engine_state(
                job=job,
                job_engine_state=keyed_engine_states.get(job['key'])
            )
        self.dao.save_jobs(jobs=jobs)
        self.set_kvp(key='job_engine_states_modified', value=time.time())

    def _get_keyed_engine_metas(self, jobs=None):
        jobs = jobs or []
        keyed_engine_metas = {
            job['key']: job.get('engine_meta') for job in jobs
        }
        return keyed_engine_metas

    def _set_job_engine_state(self, job=None, job_engine_state=None):
        if job_engine_state is None:
            if self._job_is_orphaned(job=job): job['status'] = 'EXECUTED'
        else:
            job['engine_state'] = job_engine_state
            job['status'] = job_engine_state.get('status')

    def _job_is_orphaned(self, job=None):
        return (self._get_job_age(job=job) > self.submission_grace_period)

    def _get_job_age(self, job=None):
        return time.time() - job['created']

    def _process_executed_jobs(self):
        pass

    def _process_batchable_jobs(self):
        pass

    def _process_submittable_jobs(self):
        pass

    def flush(self):
        self.dao.flush()
