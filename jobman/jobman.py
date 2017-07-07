import contextlib
import logging
import os
import time

from . import debug_utils


class JobMan(object):
    class LockError(Exception): pass
    class SubmissionError(Exception): pass

    LOCK_KEY = '_JOBMAN_LOCK'

    CFG_PARAMS = ['dao', 'jobman_db_uri', 'engine', 'max_running_jobs',
                  'job_engine_states_ttl', 'submission_grace_period',
                  'batchable_filters', 'max_batchable_wait',
                  'target_batch_time', 'default_estimated_job_run_time',
                  'lock_timeout']

    @classmethod
    def from_cfg(cls, cfg=None):
        params_from_cfg = {}
        for param in cls.CFG_PARAMS:
            try: params_from_cfg[param] = getattr(cfg, param)
            except AttributeError: pass
        return JobMan(**params_from_cfg)

    def __init__(self, logger=None, logging_cfg=None, debug=None,
                 setup=True, **kwargs):
        self.logger = self._generate_logger(logging_cfg=logging_cfg)
        self.debug = debug
        if setup: self._setup(**kwargs)

    def _setup(self, dao=None, jobman_db_uri=None, engine=None,
               max_running_jobs=50, batchable_filters=...,
               job_engine_states_ttl=120, submission_grace_period=None,
               max_batchable_wait=120, target_batch_time=(60 * 60),
               default_estimated_job_run_time=(5 * 60),
               lock_timeout=30, **kwargs):
        self.dao = dao or self._generate_dao(jobman_db_uri=jobman_db_uri)
        self.engine = engine or self._generate_engine()
        self.max_running_jobs = max_running_jobs
        self.job_engine_states_ttl = job_engine_states_ttl
        self.submission_grace_period = submission_grace_period or \
                (2 * self.job_engine_states_ttl)
        if batchable_filters is ...: batchable_filters = []
        self.batchable_filters = (batchable_filters
                                  + self._get_default_batchable_filters())
        self.max_batchable_wait = max_batchable_wait
        self.target_batch_time = target_batch_time
        self.default_estimated_job_run_time = default_estimated_job_run_time
        self.lock_timeout = lock_timeout

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
        self._ensure_lock_kvp()

    def _ensure_lock_kvp(self):
        lock_kvp = {'key': self.LOCK_KEY, 'value': 'UNLOCKED'}
        try: self.dao.save_kvps(kvps=[lock_kvp], replace=False)
        except self.dao.InsertError: pass

    def submit_submission(self, submission=None, source=None, source_meta=None,
                          submit_to_engine_immediately=False):
        self._debug_locals()
        try:
            job = self._submission_to_job(submission=submission,
                                          source=source,
                                          source_meta=source_meta)
            job['batchable'] = self._job_is_batchable(job=job)
            created_job = self._create_job(job=job)
            if submit_to_engine_immediately:
                created_job = self._submit_job(job=created_job)
            return created_job
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

    def get_num_free_slots(self):
        return self.max_running_jobs - len(self.get_running_jobs())

    def get_jobs(self, query=None): return self.dao.get_jobs(query=query)

    def get_job_for_key(self, key=None):
        return self.dao.get_jobs(query={
            'filters': [{'field': 'key', 'operator': '=', 'value': key}]
        })[0]

    def save_jobs(self, jobs=None): return self.dao.save_jobs(jobs=jobs)

    def get_running_jobs(self, include_batch_subjobs=False):
        filters = [{'field': 'status', 'operator': '=', 'value': 'RUNNING'}]
        if not include_batch_subjobs:
            filters.append({'field': 'parent_batch_key', 'operator': 'IS',
                            'value': None})
        return self.dao.get_jobs(query={'filters': filters})

    def get_jobs_for_status(self, status=None):
        return self.get_jobs(query={
            'filters': [
                {'field': 'status', 'operator': '=', 'value': status}
            ]
        })

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
        if self._job_engine_states_are_stale():
            self._update_job_engine_states(jobs=self.get_running_jobs())
        self._process_executed_jobs()
        self._process_batchable_jobs()
        self._process_submittable_jobs()

    def _job_engine_states_are_stale(self):
        job_engine_states_age = self._get_job_engine_states_age()
        return (job_engine_states_age is None) or \
                (job_engine_states_age >= self.job_engine_states_ttl)

    def _get_job_engine_states_age(self):
        try: age = time.time() - self.get_kvp(key='job_engine_states_modified')
        except KeyError: age = None
        return age

    def _update_job_engine_states(self, jobs=None):
        if not jobs: return
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
        for executed_job in self.get_jobs_for_status(status='EXECUTED'):
            self._process_executed_job(executed_job=executed_job)

    def _process_executed_job(self, executed_job=None):
        process_fn = self._process_executed_single_job
        if executed_job.get('is_batch'):
            process_fn = self._process_executed_batch_job
        process_fn(executed_job=executed_job)

    def _process_executed_batch_job(self, executed_job=None):
        for subjob in self._get_batch_subjobs(batch_job=executed_job):
            self._process_executed_job(executed_job=subjob)
        self._complete_job(job=executed_job)

    def _get_batch_subjobs(self, batch_job=None):
        subjob_keys = batch_job['batch_meta']['subjob_keys']
        return self.dao.get_jobs(query={
            'filters': [
                {'field': 'key', 'operator': 'IN', 'value': subjob_keys}
            ]
        })

    def _complete_job(self, job=None):
        job['status'] = 'COMPLETED'
        self.save_jobs(jobs=[job])

    def _process_executed_single_job(self, executed_job=None):
        self._complete_job(job=executed_job)

    def _process_batchable_jobs(self):
        with self._lock():
            batchable_jobs = self._get_batchable_jobs()
            self._batchify_jobs(batchable_jobs=batchable_jobs)

    def _get_batchable_jobs(self):
        age_threshold = time.time() - self.max_batchable_wait
        return self.dao.get_jobs(query={
            'filters': [
                {'field': 'batchable', 'operator': '=', 'value': 1},
                {'field': 'status', 'operator': '=', 'value': 'PENDING'},
                {'field': 'modified', 'operator': '>=', 'value': age_threshold}
            ]
        })

    def _batchify_jobs(self, batchable_jobs=None):
        subjob_partitions = self._make_batch_subjob_partitions(
            batchable_jobs=batchable_jobs)
        for subjob_partition in subjob_partitions:
            self._make_batch_job(subjobs=subjob_partition)

    def _make_batch_subjob_partitions(self, batchable_jobs=None):
        partitions = []
        current_partition = []
        current_partition_time = 0
        for batchable_job in batchable_jobs:
            current_partition.append(batchable_job)
            current_partition_time += self._get_estimated_job_run_time(
                job=batchable_job)
            if current_partition_time >= self.target_batch_time:
                partitions.append(current_partition)
                current_partition = []
                current_partition_time = 0
        if current_partition: partitions.append(current_partition)
        return partitions

    def _get_estimated_job_run_time(self, job=None):
        estimated_job_run_time = job.get('submission_meta', {}).get(
            'estimated_run_time')
        if estimated_job_run_time is None:
            estimated_job_run_time = self.default_estimated_job_run_time
        return estimated_job_run_time

    def _make_batch_job(self, subjobs=None):
        batch_key = self.dao.generate_key()
        patched_subjobs = [
            {**subjob, 'status': 'PENDING', 'parent_batch_key': batch_key}
            for subjob in subjobs
        ]
        self.save_jobs(jobs=patched_subjobs)
        self._create_job(job={
            'key': batch_key,
            'batch_meta': {
                'subjob_keys': [subjob['key'] for subjob in subjobs]
            },
            'is_batch': 1,
            'status': 'PENDING'
        })

    def _process_submittable_jobs(self):
        with self._lock():
            submittable_jobs = self._get_submittable_jobs()
            num_submissions = 0
            num_slots = self.get_num_free_slots() 
            for job in submittable_jobs:
                if num_submissions > num_slots: break
                try:
                    self._submit_job(job=job)
                    num_submissions += 1
                except self.SubmissionError:
                    self.logger.exception('SubmissionError')

    @contextlib.contextmanager
    def _lock(self):
        self._acquire_lock()
        try: yield
        finally: self._release_lock()

    def _acquire_lock(self):
        start_time = time.time()
        while time.time() - start_time < self.lock_timeout:
            try:
                self.dao.update_kvp(key=self.LockError, new_value='LOCKED',
                                    where_prev_value='UNLOCKED')
            except self.dao.UpdateError: pass
            if self.debug: self.logger.debug("waiting for lock")
            time.sleep(1)
        raise self.LockError("Could not acquire lock within timeout window")

    def _release_lock(self):
        self.dao.update_kvp(key=self.LockError, new_value='UNLOCKED',
                            where_prev_value='LOCKED')

    def _get_submittable_jobs(self):
        return self.dao.get_jobs(query={
            'filters': [
                {'field': 'batchable', 'operator': '! =', 'value': 1},
                {'field': 'status', 'operator': '=', 'value': 'PENDING'},
            ]
        })

    def _submit_job(self, job=None):
        try:
            engine_meta = self.engine.submit(submission=job['submission'])
            job.update({'engine_meta': engine_meta, 'status': 'RUNNING'})
            return self.save_jobs(jobs=[job])[0]
        except Exception as exc:
            job.update({'status': 'FAILED'})
            self.save_jobs(jobs=[job])
            raise self.SubmissionError() from exc

    def flush(self):
        self.dao.flush()
