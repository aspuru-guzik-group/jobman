import collections
import contextlib
import json
import os
from pathlib import Path
import time
import traceback

from . import constants
from . import debug_utils
from . import utils


class JobMan(object):
    class LockError(Exception):
        pass

    class SubmissionError(Exception):
        pass

    LOCK_KEY = '_JOBMAN_LOCK'

    CFG_PARAMS = ['dao', 'jobman_db_uri', 'job_sources', 'engines',
                  'batchable_filters', 'max_batchable_wait',
                  'target_batch_time', 'default_job_time', 'lock_timeout',
                  'use_batching']

    job_spec_defaults = {'entrypoint': 'entrypoint.sh'}

    @classmethod
    def from_cfg(cls, cfg=None):
        params_from_cfg = utils.get_keys_or_attrs(src=cfg, keys=cls.CFG_PARAMS)
        return JobMan(**params_from_cfg, cfg=cfg)

    def __init__(self, logging_cfg=None, debug=None, setup=True, cfg=None,
                 **kwargs):
        self.debug = debug or os.environ.get('JOBMAN_DEBUG')
        self.logger = self._generate_logger(logging_cfg=logging_cfg)
        self.cfg = cfg
        if setup:
            self._setup(**kwargs)

    def _generate_logger(self, logging_cfg=None):
        logging_cfg = {**(logging_cfg or {})}
        logging_cfg.setdefault('name', (__name__ + ':' + str(id(self))))
        if self.debug:
            logging_cfg['add_stream_handler'] = True
            logging_cfg['level'] = 'DEBUG'
        return utils.generate_logger(logging_cfg)

    def _setup(self, dao=None, jobman_db_uri=None, job_sources=None,
               engines=None, batchable_filters=None, max_batchable_wait=120,
               target_batch_time=(60 * 60), default_job_time=(5 * 60),
               lock_timeout=30, use_batching=False, job_spec_defaults=None):
        self.dao = dao or self._generate_dao(jobman_db_uri=jobman_db_uri)
        self.job_sources = job_sources or {}
        for job_source in self.job_sources.values():
            job_source.jobman = self
        self.engines = engines or {}
        self.default_job_time = default_job_time
        self.lock_timeout = lock_timeout
        self.use_batching = use_batching
        self.job_spec_defaults = job_spec_defaults or self.job_spec_defaults
        if batchable_filters is ...:
            batchable_filters = []
        self.batchable_filters = [
            *(batchable_filters or []),
            *self._get_default_batchable_filters()
        ]
        self.max_batchable_wait = max_batchable_wait
        self.target_batch_time = target_batch_time
        self.ensure_db()
        self._kvps = {}

    def _debug_locals(self):
        if self.debug: debug_utils.debug_locals(logger=self.logger)  # noqa

    def _generate_dao(self, jobman_db_uri=None):
        jobman_db_uri = (jobman_db_uri or
                         str(Path('~/jobman.sqlite.db').expanduser()))
        from .dao.jobman_sqlite_dao import JobmanSqliteDAO
        return JobmanSqliteDAO(db_uri=jobman_db_uri, logger=self.logger)

    def _get_default_batchable_filters(self):
        def job_spec_has_batchable(job=None):
            return bool(job.get('job_spec', {}).get('batchable'))
        return [job_spec_has_batchable]

    def ensure_db(self):
        self.dao.ensure_tables()
        self._ensure_lock_kvp()

    def _ensure_lock_kvp(self):
        lock_kvp = {'key': self.LOCK_KEY, 'value': 'UNLOCKED'}
        try:
            self.dao.save_kvps(kvps=[lock_kvp], replace=False)
        except self.dao.InsertError as exc:
            if self.debug:
                self.logger.debug(("_ensure_lock_kvp exc: ", exc))

    def submit_job_spec(self, job_spec=None, source_key=None,
                        source_meta=None):
        try:
            return self.dao.create_job(job={
                'job_spec': job_spec,
                'source_key': source_key,
                'source_meta': source_meta,
                'status': 'PENDING',
            })
        except Exception as exc:
            raise self.SubmissionError() from exc

    def get_num_free_slots(self):
        # return self.max_running_jobs - len(self.get_running_jobs())
        return 999

    def query_jobs(self, query=None):
        return self.dao.query_jobs(query=query)

    def get_job(self, key=None):
        return self.dao.get_job(key=key)

    def save_jobs(self, jobs=None):
        return self.dao.save_jobs(jobs=jobs)

    def get_jobs_for_status(self, status=None):
        return self.query_jobs(query={
            'filters': [self.generate_status_filter(status=status)]
        })

    def generate_status_filter(self, status=None):
        return {'field': 'status', 'op': '=', 'arg': status}

    def generate_source_key_filter(self, source_key=None):
        return {'field': 'source_key', 'op': '=', 'arg': source_key}

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
        self._tick_engines()
        self._update_running_jobs()
        self._process_executed_jobs()
        if self.use_batching:
            self._tick_batching()
        self._process_submittable_jobs()
        self._tick_job_sources()

    def _tick_engines(self):
        for engine in self.engines.values():
            engine.tick()

    def _update_running_jobs(self):
        running_jobs = self.get_running_jobs()
        self._update_job_engine_states(jobs=running_jobs)
        self.dao.save_jobs(running_jobs)

    def get_running_jobs(self, include_batch_subjobs=False):
        filters = [self.generate_status_filter(status='RUNNING')]
        if not include_batch_subjobs:
            filters.append({'field': 'parent_batch_key', 'op': 'IS',
                            'arg': None})
        return self.dao.query_jobs(query={'filters': filters})

    def _update_job_engine_states(self, jobs=None):
        if not jobs: return  # noqa
        jobs_by_engine_key = self._group_jobs_by_engine_key(jobs=jobs)
        for engine_key, jobs_for_engine_key in jobs_by_engine_key.items():
            self._update_job_engine_states_for_engine(
                jobs=jobs_for_engine_key, engine=self.engines[engine_key])

    def _group_jobs_by_engine_key(self, jobs=None):
        jobs_by_engine = collections.defaultdict(list)
        for job in jobs:
            jobs_by_engine[job['engine_key']].append(job)
        return jobs_by_engine

    def _update_job_engine_states_for_engine(self, jobs=None, engine=None):
        engine_metas = {job['key']: job.get('engine_meta') for job in jobs}
        engine_states = engine.get_keyed_engine_states(engine_metas)
        for job in jobs:
            engine_state = engine_states.get(job['key'])
            if engine_state is None:
                if self._job_is_orphaned(job=job):
                    job['status'] = 'EXECUTED'
                else:
                    job['engine_state'] = engine_state
                    job['status'] = engine_state.get('status')

    def _job_is_orphaned(self, job=None):
        return (self._get_job_age(job=job) > self.submission_grace_period)

    def _get_job_age(self, job=None):
        return time.time() - job['created']

    def _process_executed_jobs(self):
        for executed_job in self.get_jobs_for_status(status='EXECUTED'):
            self._process_executed_job(executed_job=executed_job)

    def _process_executed_job(self, executed_job=None):
        if executed_job.get('is_batch'):
            process_fn = self._process_executed_batch_job
        else:
            process_fn = self._process_executed_single_job
        process_fn(executed_job=executed_job)

    def _process_executed_batch_job(self, executed_job=None):
        for subjob in self._get_batch_subjobs(batch_job=executed_job):
            self._process_executed_job(executed_job=subjob)
        self._complete_job(job=executed_job)

    def _get_batch_subjobs(self, batch_job=None):
        subjob_keys = batch_job['batch_meta']['subjob_keys']
        return self.dao.query_jobs(query={
            'filters': [{'field': 'key', 'op': 'IN', 'arg': subjob_keys}]
        })

    def _complete_job(self, job=None):
        job['status'] = 'COMPLETED'
        self.save_jobs(jobs=[job])

    def _process_executed_single_job(self, executed_job=None):
        self._evaluate_job(job=executed_job)

    def _evaluate_job(self, job=None):
        try:
            if self._job_has_completed_checkpoint(job=job):
                self._complete_job(job=job)
            else:
                raise Exception("No completed checkpoint file found for job")
        except Exception as exc:
            error = traceback.format_exc()
            self.logger.warning("warning: %s" % error)
            self._fail_job(job=job, errors=[error])

    def _job_has_completed_checkpoint(self, job=None):
        return Path(
            job['job_spec']['dir'],
            constants.CHECKPOINT_FILE_NAMES['completed']
        ).exists()

    def _fail_job(self, job=None, errors=None):
        job['status'] = 'FAILED'
        job['errors'] = errors
        self.save_jobs(jobs=[job])

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
        return self.dao.query_jobs(query={
            'filters': [
                self.generate_status_filter(status='PENDING'),
                {'field': 'batchable', 'op': 'IS', 'arg': None}
            ]
        })

    def _job_is_batchable(self, job=None):
        return any(filter_(job) for filter_ in self.batchable_filters)

    def _process_batchable_jobs(self):
        with self._get_lock():
            batchable_jobs = self._get_batchable_jobs()
            self._batchify_jobs(batchable_jobs=batchable_jobs)

    def _get_batchable_jobs(self):
        age_threshold = time.time() - self.max_batchable_wait
        return self.dao.query_jobs(query={
            'filters': [
                {'field': 'batchable', 'op': '=', 'arg': 1},
                self.generate_status_filter(status='PENDING'),
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
        if current_partition:
            partitions.append(current_partition)
        return partitions

    def _get_job_time(self, job=None):
        try:
            job_time = job['job_spec']['resources']['time']
        except KeyError:
            job_time = None
        return job_time or self.default_job_time

    def _make_batch_job(self, subjobs=None):
        batch_key = self.dao.generate_key()
        patched_subjobs = [
            {**subjob, 'status': 'PENDING', 'parent_batch_key': batch_key}
            for subjob in subjobs
        ]
        self.save_jobs(jobs=patched_subjobs)
        self.dao.create_job(job={
            'key': batch_key,
            'batch_meta': {'subjob_keys': [job['key'] for job in subjobs]},
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
                if num_submissions > num_slots:
                    break
                try:
                    engine = list(self.engines.values())[0]
                    self._submit_job_to_engine(job=job, engine=engine)
                    num_submissions += 1
                except self.SubmissionError:
                    self.logger.exception('SubmissionError')

    @contextlib.contextmanager
    def _get_lock(self):
        self._acquire_lock()
        try:
            yield
        finally:
            self._release_lock()

    def _acquire_lock(self):
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
        self.dao.update_kvp(key=self.LOCK_KEY, new_value='UNLOCKED',
                            where_prev_value='LOCKED')

    def _get_submittable_jobs(self, exclude_batchable_jobs=True):
        filters = [self.generate_status_filter(status='PENDING')]
        if exclude_batchable_jobs:
            filters.append({'field': 'batchable', 'op': '! =', 'arg': 1})
        return self.dao.query_jobs(query={'filters': filters})

    def _submit_job_to_engine(self, job=None, engine=None):
        try:
            submit_to_engine_fn = self._get_submit_to_engine_fn_for_job(
                job=job)
            engine_meta = submit_to_engine_fn(job=job, engine=engine)
            job.update({
                'engine_key': engine.key,
                'engine_meta': engine_meta,
                'status': 'RUNNING'
            })
            return self.save_jobs(jobs=[job])[0]
        except Exception as exc:
            job.update({'status': 'FAILED'})
            self.save_jobs(jobs=[job])
            raise self.SubmissionError() from exc

    def _get_submit_to_engine_fn_for_job(self, job=None):
        if job.get('is_batch'):
            return self._submit_batch_job_to_engine
        return self._submit_single_job_to_engine

    def _submit_single_job_to_engine(self, job=None, engine=None):
        engine_meta = engine.submit_job(job=job, extra_cfgs=[self.cfg])
        return engine_meta

    def _submit_batch_job_to_engine(self, job=None, engine=None):
        subjobs = self._get_batch_subjobs(batch_job=job)
        return engine.submit_batch_job(batch_job=job, subjobs=subjobs,
                                       extra_cfgs=[self.cfg])

    def flush(self):
        self.dao.flush()

    def _tick_job_sources(self):
        for source_key, job_source in self.job_sources.items():
            job_source.tick()

    def submit_job_dir(self, job_dir=None, source_key=None, source_meta=None,
                       job_spec_defaults=None, job_spec_overrides=None):
        return self.submit_job_spec(
            job_spec=self._generate_job_spec_for_job_dir(
                job_dir=job_dir,
                defaults=job_spec_defaults,
                overrides=job_spec_overrides
            ),
            source_key=source_key,
            source_meta=source_meta,
        )

    def _generate_job_spec_for_job_dir(self, job_dir=None, defaults=None,
                                       overrides=None):
        return {
            'dir': str(job_dir),
            **{**self.job_spec_defaults, **(defaults or {})},
            **self._load_job_spec_from_job_dir(job_dir=job_dir),
            **(overrides or {})
        }

    def _load_job_spec_from_job_dir(self, job_dir=None):
        job_spec = {}
        job_spec_path = Path(job_dir) / constants.JOB_SPEC_FILE_NAME
        if job_spec_path.exists():
            with open(job_spec_path) as f:
                job_spec = json.load(f)
        return job_spec
