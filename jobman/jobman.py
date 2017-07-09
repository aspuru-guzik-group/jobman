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
                  'target_batch_time', 'default_job_time',
                  'lock_timeout', 'use_batching']

    @classmethod
    def from_cfg(cls, cfg=None):
        params_from_cfg = {}
        for param in cls.CFG_PARAMS:
            try: params_from_cfg[param] = getattr(cfg, param)
            except AttributeError: pass
        return JobMan(**params_from_cfg)

    def __init__(self, logger=None, logging_cfg=None, debug=None,
                 setup=True, **kwargs):
        self.debug = debug
        self.logger = self._generate_logger(logging_cfg=logging_cfg)
        if setup: self._setup(**kwargs)

    def _setup(self, dao=None, jobman_db_uri=None, engine=None,
               max_running_jobs=50, batchable_filters=...,
               job_engine_states_ttl=120, submission_grace_period=None,
               max_batchable_wait=120, target_batch_time=(60 * 60),
               default_job_time=(5 * 60),
               lock_timeout=30, use_batching=False, **kwargs):
        self._debug_locals()
        self.dao = dao or self._generate_dao(jobman_db_uri=jobman_db_uri)
        self.engine = engine or self._generate_engine()
        self.max_running_jobs = max_running_jobs
        self.job_engine_states_ttl = job_engine_states_ttl
        self.submission_grace_period = submission_grace_period or \
                (2 * self.job_engine_states_ttl)
        self.default_job_time = default_job_time
        self.lock_timeout = lock_timeout

        self.use_batching = use_batching
        if batchable_filters is ...: batchable_filters = []
        self.batchable_filters = (batchable_filters
                                  + self._get_default_batchable_filters())
        self.max_batchable_wait = max_batchable_wait
        self.target_batch_time = target_batch_time

        self.ensure_db()
        self._kvps = {}

    def _generate_logger(self, logging_cfg=None):
        logging_cfg = logging_cfg or {}
        logger = logging_cfg.get('logger') or \
                logging.getLogger('jobman_%s' % id(self))

        log_file = logging_cfg.get('log_file')
        if log_file:
            logger.addHandler(logging.FileHandler(os.path.expanduser(log_file)))
        if self.debug: logger.addHandler(logging.StreamHandler())

        level = logging_cfg.get('level')
        if level: logger.setLevel(getattr(logging, level))
        elif self.debug: logger.setLevel(logging.DEBUG)

        fmt = logging_cfg.get('fmt') or \
                '| %(asctime)s | %(name)s | %(levelname)s |\n%(message)s\n'
        formatter = logging.Formatter(fmt)
        for handler in logger.handlers: handler.setFormatter(formatter)

        return logger

    def _debug_locals(self):
        if self.debug: debug_utils.debug_locals(logger=self.logger)

    def _get_default_batchable_filters(self):
        def jobdir_meta_has_batchable(job=None):
            return bool(job.get('jobdir_meta', {}).get('batchable'))
        return [jobdir_meta_has_batchable]

    def _generate_dao(self, jobman_db_uri=None):
        self._debug_locals()
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
        self._debug_locals()
        lock_kvp = {'key': self.LOCK_KEY, 'value': 'UNLOCKED'}
        try: self.dao.save_kvps(kvps=[lock_kvp], replace=False)
        except self.dao.InsertError as exc:
            if self.debug:
                self.logger.debug(("_ensure_lock_kvp exc: ", exc))

    def submit_jobdir_meta(self, jobdir_meta=None, source=None,
                           source_meta=None,
                           submit_to_engine_immediately=False):
        self._debug_locals()
        try:
            job = self._jobdir_meta_to_job(jobdir_meta=jobdir_meta,
                                           source=source,
                                           source_meta=source_meta)
            created_job = self._create_job(job=job)
            if submit_to_engine_immediately:
                created_job = self._submit_job_to_engine(job=created_job)
            return created_job
        except Exception as exc:
            raise self.SubmissionError() from exc

    def _jobdir_meta_to_job(self, jobdir_meta=None, source=None,
                            source_meta=None):
        return {
            'jobdir_meta': jobdir_meta,
            'source': source,
            'source_meta': source_meta,
            'status': 'PENDING',
        }

    def _create_job(self, job=None): return self.dao.create_job(job=job)

    def get_num_free_slots(self):
        return self.max_running_jobs - len(self.get_running_jobs())

    def get_jobs(self, query=None): return self.dao.get_jobs(query=query)

    def get_job_for_key(self, key=None):
        return self.dao.get_jobs(query={
            'filters': [{'field': 'key', 'op': '=', 'arg': key}]
        })[0]

    def save_jobs(self, jobs=None): return self.dao.save_jobs(jobs=jobs)

    def get_running_jobs(self, include_batch_subjobs=False):
        filters = [{'field': 'status', 'op': '=', 'arg': 'RUNNING'}]
        if not include_batch_subjobs:
            filters.append({'field': 'parent_batch_key', 'op': 'IS',
                            'arg': None})
        return self.dao.get_jobs(query={'filters': filters})

    def get_jobs_for_status(self, status=None):
        return self.get_jobs(query={
            'filters': [{'field': 'status', 'op': '=', 'arg': status}]
        })

    def get_kvp(self, key=None):
        # fallback to dao if not in _kvps.
        try:
            if key not in self._kvps:
                self._kvps[key] = self.dao.get_kvps(query={
                    'filters': [{'field': 'key', 'op': '=', 'arg': key}]
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
        if self.use_batching: self._tick_batching()
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
            'filters': [{'field': 'key', 'op': 'IN', 'arg': subjob_keys}]
        })

    def _complete_job(self, job=None):
        job['status'] = 'COMPLETED'
        self.save_jobs(jobs=[job])

    def _process_executed_single_job(self, executed_job=None):
        self._complete_job(job=executed_job)

    def _tick_batching(self):
        self._mark_jobs_as_batchable()
        self._process_batchable_jobs()

    def _mark_jobs_as_batchable(self):
        with self._get_lock():
            candidate_batchable_jobs = self._get_candidate_batchable_jobs()
            for job in candidate_batchable_jobs:
                job['batchable'] = self._job_is_batchable(job=job)
            self.save_jobs(jobs=candidate_batchable_jobs)

    def _get_candidate_batchable_jobs(self):
        return self.dao.get_jobs(query={
            'filters': [
                {'field': 'status', 'op': '=', 'arg': 'PENDING'},
                {'field': 'batchable', 'op': 'IS', 'arg': None}
            ]
        })

    def _job_is_batchable(self, job=None):
        for filter_ in self.batchable_filters:
            if filter_(job): return True
        return False

    def _process_batchable_jobs(self):
        with self._get_lock():
            batchable_jobs = self._get_batchable_jobs()
            self._batchify_jobs(batchable_jobs=batchable_jobs)

    def _get_batchable_jobs(self):
        age_threshold = time.time() - self.max_batchable_wait
        return self.dao.get_jobs(query={
            'filters': [
                {'field': 'batchable', 'op': '=', 'arg': 1},
                {'field': 'status', 'op': '=', 'arg': 'PENDING'},
                {'field': 'modified', 'op': '>=', 'arg': age_threshold}
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
            current_partition_time += self._get_job_time(job=batchable_job)
            if current_partition_time >= self.target_batch_time:
                partitions.append(current_partition)
                current_partition = []
                current_partition_time = 0
        if current_partition: partitions.append(current_partition)
        return partitions

    def _get_job_time(self, job=None):
        try: job_time = job['jobdir_meta']['resources']['time']
        except KeyError: job_time = None
        return job_time or self.default_job_time

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
        with self._get_lock():
            submittable_jobs = self._get_submittable_jobs(
                exclude_batchable_jobs=(not self.use_batching))
            num_submissions = 0
            num_slots = self.get_num_free_slots() 
            for job in submittable_jobs:
                if num_submissions > num_slots: break
                try:
                    self._submit_job_to_engine(job=job)
                    num_submissions += 1
                except self.SubmissionError:
                    self.logger.exception('SubmissionError')

    @contextlib.contextmanager
    def _get_lock(self):
        self._debug_locals()
        self._acquire_lock()
        try: yield
        finally: self._release_lock()

    def _acquire_lock(self):
        self._debug_locals()
        start_time = time.time()
        while time.time() - start_time < self.lock_timeout:
            try:
                self.dao.update_kvp(key=self.LOCK_KEY, new_value='LOCKED',
                                    where_prev_value='UNLOCKED')
                return
            except self.dao.UpdateError as exc:
                if self.debug:
                    self.logger.exception('UpdateError')
                    self.logger.debug("waiting for lock")
                time.sleep(1)
        raise self.LockError("Could not acquire lock within timeout window")

    def _release_lock(self):
        self._debug_locals()
        self.dao.update_kvp(key=self.LOCK_KEY, new_value='UNLOCKED',
                            where_prev_value='LOCKED')

    def _get_submittable_jobs(self, exclude_batchable_jobs=True):
        filters = [{'field': 'status', 'op': '=', 'arg': 'PENDING'}]
        if exclude_batchable_jobs:
            filters.append({'field': 'batchable', 'op': '! =', 'arg': 1})
        return self.dao.get_jobs(query={'filters': filters})

    def _submit_job_to_engine(self, job=None):
        try:
            submit_to_engine_fn = self._get_submit_to_engine_fn_for_job(job=job)
            engine_meta = submit_to_engine_fn(job=job)
            job.update({'engine_meta': engine_meta, 'status': 'RUNNING'})
            return self.save_jobs(jobs=[job])[0]
        except Exception as exc:
            job.update({'status': 'FAILED'})
            self.save_jobs(jobs=[job])
            raise self.SubmissionError() from exc

    def _get_submit_to_engine_fn_for_job(self, job=None):
        if job.get('is_batch'): return self._submit_batch_job_to_engine
        return self._submit_single_job_to_engine

    def _submit_single_job_to_engine(self, job=None):
        engine_meta = self.engine.submit_job(job=job)
        return engine_meta

    def _submit_batch_job_to_engine(self, job=None):
        subjobs = self._get_batch_subjobs(batch_job=job)
        return self.engine.submit_batch_job(batch_job=job, subjobs=subjobs)

    def flush(self):
        self.dao.flush()
