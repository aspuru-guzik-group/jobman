import collections
import collections.abc
import json
import os
from pathlib import Path
import time
import traceback

from . import constants
from .utils import debug_utils
from .utils import dot_spec_loader
from .utils import logging_utils
from .worker import Worker


class JobMan(object):
    class LockError(Exception):
        pass

    class SubmissionError(Exception):
        pass

    CFG_PARAMS = [
        'dao', 'jobman_db_uri', 'source_specs', 'worker_specs',
        'default_job_time',
    ]

    job_spec_defaults = {'entrypoint': 'entrypoint.sh'}

    @classmethod
    def from_cfg(cls, cfg=None):
        params_from_cfg = dot_spec_loader.get_attrs_or_items(
            obj=cfg, keys=cls.CFG_PARAMS)
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
        return logging_utils.generate_logger(logging_cfg)

    def _setup(self, dao=None, jobman_db_uri=None, source_specs=None,
               worker_specs=None, job_spec_defaults=None):
        self.dao = dao or self._generate_dao(jobman_db_uri=jobman_db_uri)
        self.sources = self._source_specs_to_sources(source_specs)
        self.workers = self._worker_specs_to_workers(worker_specs)
        self.job_spec_defaults = job_spec_defaults or self.job_spec_defaults
        self.dao.ensure_db()
        self._kvps = {}

    def _debug_locals(self):
        if self.debug: debug_utils.debug_locals(logger=self.logger)  # noqa

    def _generate_dao(self, jobman_db_uri=None):
        jobman_db_uri = (
            jobman_db_uri or
            os.path.expanduser('~/jobman.sqlite.db')
        )
        from .dao.jobman_sqlite_dao import JobmanSqliteDAO
        return JobmanSqliteDAO(db_uri=jobman_db_uri, logger=self.logger)

    def _source_specs_to_sources(self, source_specs=None):
        sources = {}
        for source_key, source_spec in (source_specs or {}).items():
            sources[source_key] = self._source_spec_to_source(
                source_key=source_key, source_spec=source_spec)
        return sources

    def _source_spec_to_source(self, source_key=None, source_spec=None):
        if isinstance(source_spec, collections.abc.Mapping):
            source_class = source_spec['source_class']
            if isinstance(source_class, str):
                source_class = dot_spec_loader.load_from_dot_spec(source_class)
            source = source_class(**{
                'key': source_key,
                'jobman': self,
                **(source_spec.get('source_params') or {}),
            })
        else:
            source = source_spec
            source.key = source_key
            source.jobman = self
        return source

    def _worker_specs_to_workers(self, worker_specs=None):
        workers = {}
        for worker_key, worker_spec in (worker_specs or {}).items():
            workers[worker_key] = self._worker_spec_to_worker(
                worker_key=worker_key, worker_spec=worker_spec)
        return workers

    def _worker_spec_to_worker(self, worker_key=None, worker_spec=None):
        if isinstance(worker_spec, collections.abc.Mapping):
            worker_class = worker_spec.get('worker_class') or Worker
            if isinstance(worker_class, str):
                worker_class = dot_spec_loader.load_from_dot_spec(worker_class)
            worker = worker_class(**{
                'key': worker_key,
                **(worker_spec.get('worker_params') or {})
            })
        else:
            worker = worker_spec
            worker.key = worker_key
        return worker

    def tick(self):
        self._tick_workers()
        self._update_running_jobs()
        self._process_executed_jobs()
        self._submit_pending_jobs()
        self._tick_sources()

    def _tick_workers(self):
        for worker in self.workers.values(): worker.tick()  # noqa

    def _update_running_jobs(self):
        running_jobs = self.dao.get_jobs_for_status(status='RUNNING')
        if not running_jobs: return  # noqa
        self._update_job_worker_states(jobs=running_jobs)
        self.dao.save_jobs(running_jobs)

    def _update_job_worker_states(self, jobs=None):
        if not jobs: return  # noqa
        jobs_by_worker_key = self._group_jobs_by_worker_key(jobs=jobs)
        for worker_key, jobs_for_worker_key in jobs_by_worker_key.items():
            self._update_job_worker_states_for_worker(
                jobs=jobs_for_worker_key, worker=self.workers[worker_key])

    def _group_jobs_by_worker_key(self, jobs=None):
        jobs_by_worker = collections.defaultdict(list)
        for job in jobs:
            jobs_by_worker[job['worker_key']].append(job)
        return jobs_by_worker

    def _update_job_worker_states_for_worker(self, jobs=None, worker=None):
        worker_metas = {job['key']: job.get('worker_meta') for job in jobs}
        worker_states = worker.get_keyed_states(keyed_metas=worker_metas)
        for job in jobs:
            worker_state = worker_states.get(job['key'])
            if not worker_state:
                if self._job_is_orphaned(job=job):
                    job['status'] = 'EXECUTED'
            else:
                job['worker_state'] = worker_state
                job['status'] = worker_state.get('status')

    def _job_is_orphaned(self, job=None):
        return (self._get_job_age(job=job) > self.submission_grace_period)

    def _get_job_age(self, job=None):
        return time.time() - job['created']

    def _process_executed_jobs(self):
        for executed_job in self.dao.get_jobs_for_status(status='EXECUTED'):
            self._process_executed_job(executed_job=executed_job)

    def _process_executed_job(self, executed_job=None):
        try:
            if self._job_has_completed_checkpoint(executed_job):
                self._complete_job(executed_job)
            else:
                raise Exception("No completed checkpoint file found for job")
        except Exception as exc:
            error = traceback.format_exc()
            self.logger.warning("warning: %s" % error)
            self._fail_job(executed_job, errors=[error])

    def _job_has_completed_checkpoint(self, job=None):
        return Path(
            job['job_spec']['dir'],
            constants.CHECKPOINT_FILE_NAMES['completed']
        ).exists()

    def _complete_job(self, job=None):
        job['status'] = 'COMPLETED'
        self.dao.save_jobs(jobs=[job])

    def _fail_job(self, job=None, errors=None):
        job['status'] = 'FAILED'
        job['errors'] = errors
        self.dao.save_jobs(jobs=[job])

    def _submit_pending_jobs(self):
        tallies = collections.defaultdict(int)
        with self.dao.get_lock():
            pending_jobs = self.dao.get_jobs_for_status(status='PENDING')
            for job in pending_jobs:
                tallies['visited'] += 1
                try:
                    for worker in self.workers.values():
                        try:
                            if worker.can_accept_job(job=job):
                                self._submit_job_to_worker(
                                    job=job, worker=worker)
                                tallies['submitted'] += 1
                        except worker.IncompatibleJobError:
                            continue
                except self.SubmissionError:
                    tallies['errors'] += 1
                    self.logger.exception('SubmissionError')
        return tallies

    def _submit_job_to_worker(self, job=None, worker=None):
        try:
            worker_meta = worker.submit_job(job=job, extra_cfgs=[self.cfg])
            job.update({
                'worker_key': worker.key,
                'worker_meta': worker_meta,
                'status': 'RUNNING'
            })
            return self.dao.save_jobs(jobs=[job])[0]
        except Exception as exc:
            job.update({'status': 'FAILED'})
            self.dao.save_jobs(jobs=[job])
            raise self.SubmissionError() from exc

    def _tick_sources(self):
        for source in self.sources.values(): source.tick()  # noqa

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
