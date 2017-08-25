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
from .workers.base_worker import BaseWorker


class CfgParams:
    """JobMan Config Parameters"""

    auto_initialize = True
    """If True then create jobman db and initialize sources/engines."""

    dao = ...
    """An instance of a JobManDAO. Usually only used for testing."""

    db_uri = ...
    """A database uri.

    Currently can only use Sqlite uris. For example,
    'sqlite://' for an in-memory db, or 'sqlite:////absolute/path/to/db.sqlite'
    for a file-based db.
    """

    debug = ...
    """set to True to operate in debug mode. Logs more verbose statements."""

    label = ...
    """A label to identify this jobman instance. Useful for logging."""

    logging_cfg = ...
    """logging config, per :meth:`jobman.utils.logging_utils.generate_logger`
    """

    source_specs = ...
    """A dict of keyed source specs.

    A source spec is a dict with this shape:

        ::

           {
               # a dot_spec path to a source class
               'source_class': 'jobman.sources.dir_source:DirSource'
               # a dict of params to pass as kwargs when instantiating the
               # source.
               'source_params': {
                   'root_path': self.root_path
               }
           }

    """

    worker_specs = ...
    """A dict of keyed worker specs.

    A worker spec is a dict with this shape:

        ::

           {
               # a dot_spec path to a worker class
               'worker_class': 'jobman.workers.base_worker:BaseWorker'
               # a dict of params to pass as kwargs when instantiating the
               # worker.
               'worker_params': {
                    # a spec for the worker's engine.
                    'engine_spec': {
                        'engine_class': (
                            'jobman.engines.local_engine:LocalEngine'
                        ),
                        'engine_params': {
                            'scratch_dir': self.scratch_dir,
                            # This is usually where engine configs go.
                            'cfg': {
                                'MY_EXE': '/my/cluster/path/to/my_exe',
                                # other cluster-specific params...
                            }
                        }
                    },
                    # other worker params...
               }
           }

    """

    @classmethod
    def get_cfg_infos(cls):
        infos = {
            k: v for k, v in cls.__dict__.items()
            if not k.startswith('_') and k != 'get_cfg_infos'
        }
        return infos


class JobMan(object):

    class SubmissionError(Exception):
        pass

    job_spec_defaults = {'entrypoint': './entrypoint.sh'}
    """Default job_spec values."""

    JOB_STATUSES = constants.JOB_STATUSES

    @classmethod
    def from_cfg(cls, cfg=None, overrides=None):
        """Create a JobMan instance from a cfg.

        Args:
            cfg: can be an object or dict. Keys or attrs per
                 :class:`CfgParams`.
            overrides: dict of cfg overrides

        Returns:
            JobMan instance
        """

        kwargs_from_cfg = {}
        for param, default in CfgParams.get_cfg_infos().items():
            try:
                kwargs_from_cfg[param] = dot_spec_loader.get_attr_or_item(
                    obj=cfg, key=param, default=default)
            except KeyError:
                pass
        kwargs = {
            **kwargs_from_cfg,
            **(overrides or {}),
            'cfg': cfg
        }
        kwargs = {k: v for k, v in kwargs.items() if v is not ...}
        return JobMan(**kwargs)

    def __init__(self, label=None, cfg=None, source_specs=None,
                 job_spec_defaults=None, worker_specs=None, dao=None,
                 db_uri=None, auto_initialize=True, logging_cfg=None,
                 debug=None):
        """Central JobMan object.

        Args:
            label: per :attr:`CfgParams.label` .
            cfg: cfg object or dict.
            source_specs: per :attr:`CfgParams.source_specs` .
            job_spec_defaults: per :attr:`job_spec_defaults` .
            worker_specs: per :attr:`CfgParams.worker_specs` .
            dao: per :attr:`CfgParams.dao` .
            db_uri: per :attr:`CfgParams.db_uri` .
            auto_initialize: per :attr:`CfgParams.auto_initialize` .
            logging_cfg: per :attr:`CfgParams.logging_cfg` .
            debug: per :attr:`CfgParams.debug` .
        """
        self.label = label
        self.debug = debug or os.environ.get('JOBMAN_DEBUG')
        self.logger = self._generate_logger(logging_cfg=logging_cfg)
        self.cfg = cfg
        self.source_specs = source_specs
        self.worker_specs = worker_specs
        self.dao = dao
        self.db_uri = self.dao.db_uri if self.dao else db_uri
        self.job_spec_defaults = job_spec_defaults or self.job_spec_defaults
        self._kvps = {}
        if auto_initialize:
            self.initialize()

    def _generate_logger(self, logging_cfg=None):
        logging_cfg = {**(logging_cfg or {})}
        default_name = (
            __name__ + ':' + (self.label if self.label else str(id(self)))
        )
        logging_cfg.setdefault('name', default_name)
        if self.debug:
            logging_cfg['add_stream_handler'] = True
            logging_cfg['level'] = 'DEBUG'
        return logging_utils.generate_logger(logging_cfg)

    def initialize(self):
        """Initialize dao, sources, and workers."""
        self.dao = self.dao or self._generate_dao(
            db_uri=self.db_uri,
            initialize=True
        )
        self.sources = self._source_specs_to_sources(
            source_specs=self.source_specs,
            initialize=True
        )
        self.workers = self._worker_specs_to_workers(
            worker_specs=self.worker_specs,
            initialize=True
        )

    def _debug_locals(self):
        if self.debug: debug_utils.debug_locals(logger=self.logger)  # noqa

    def _generate_dao(self, db_uri=None, initialize=...):
        db_uri = (
            db_uri or
            os.path.expanduser('~/jobman.sqlite.db')
        )
        from .dao.jobman_sqlite_dao import JobmanSqliteDAO
        dao_kwargs = {
            'db_uri': db_uri,
            'logger': self.logger
        }
        if initialize is not ...:
            dao_kwargs['initialize'] = initialize
        return JobmanSqliteDAO(**dao_kwargs)

    def _source_specs_to_sources(self, source_specs=None, initialize=...):
        sources = {}
        for source_key, source_spec in (source_specs or {}).items():
            sources[source_key] = self._source_spec_to_source(
                source_key=source_key,
                source_spec=source_spec,
                initialize=initialize
            )
        return sources

    def _source_spec_to_source(self, source_key=None, source_spec=None,
                               initialize=...):
        if isinstance(source_spec, collections.abc.Mapping):
            source_class = source_spec['source_class']
            if isinstance(source_class, str):
                source_class = dot_spec_loader.load_from_dot_spec(source_class)
            source_kwargs = {
                'key': source_key,
                'jobman': self,
                **(source_spec.get('source_params') or {}),
            }
            if initialize is not ...:
                source_kwargs['initialize'] = initialize
            source = source_class(**source_kwargs)
        else:
            source = source_spec
            source.key = source_key
            source.jobman = self
            if initialize is not ...:
                source.initialize()
        return source

    def _worker_specs_to_workers(self, worker_specs=None, initialize=...):
        workers = {}
        for worker_key, worker_spec in (worker_specs or {}).items():
            workers[worker_key] = self._worker_spec_to_worker(
                worker_key=worker_key,
                worker_spec=worker_spec,
                initialize=initialize
            )
        return workers

    def _worker_spec_to_worker(self, worker_key=None, worker_spec=None,
                               initialize=...):
        if isinstance(worker_spec, collections.abc.Mapping):
            worker_class = worker_spec.get('worker_class') or BaseWorker
            if isinstance(worker_class, str):
                worker_class = dot_spec_loader.load_from_dot_spec(worker_class)
            worker_kwargs = {
                'key': worker_key,
                'db_uri': self.dao.db_uri,
                **(worker_spec.get('worker_params') or {})
            }
            if initialize is not ...:
                worker_kwargs['initialize'] = initialize
            worker = worker_class(**worker_kwargs)
        else:
            worker = worker_spec
            worker.key = worker_key
            if initialize is not ...:
                worker.initialize()
        if self.cfg:
            worker.cfgs.extend([self.cfg])
        worker.jobman = self
        return worker

    def tick(self):
        """Tick JobMan components."""
        self._tick_workers()
        self._update_running_jobs()
        self._process_executed_jobs()
        self._submit_pending_jobs()
        self._tick_sources()

    def _tick_workers(self):
        for worker in self.workers.values(): worker.tick()  # noqa

    def _update_running_jobs(self):
        running_jobs = self.dao.get_jobs_for_status(
            status=self.JOB_STATUSES.RUNNING)
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
                    job['status'] = self.JOB_STATUSES.EXECUTED
            else:
                job['worker_state'] = worker_state
                job['status'] = worker_state.get('status')

    def _job_is_orphaned(self, job=None):
        return (self._get_job_age(job=job) > self.submission_grace_period)

    def _get_job_age(self, job=None):
        return time.time() - job['created']

    def _process_executed_jobs(self):
        claimed_executed_jobs = self.dao.claim_jobs(query={
            'filters': [
                self.dao.generate_status_filter(
                    status=self.JOB_STATUSES.EXECUTED),
            ],
            'limit': 100
        })
        for executed_job in claimed_executed_jobs:
            self._process_executed_job(executed_job=executed_job, save=False)
        self.dao.save_jobs(claimed_executed_jobs)

    def _process_executed_job(self, executed_job=None, save=True):
        try:
            if self._job_has_completed_checkpoint(executed_job):
                self._complete_job(executed_job, save=save)
            else:
                raise Exception(
                    (
                        "No completed checkpoint file found for job."
                        " job_dir was: '{job_dir}'."
                    ).format(
                        job_dir=(executed_job.get('job_spec', {}).get('dir'))
                    )
                )
        except Exception as exc:
            error = traceback.format_exc()
            self.logger.warning("warning: %s" % error)
            self._fail_job(executed_job, errors=[error], save=save)

    def _job_has_completed_checkpoint(self, job=None):
        return Path(
            job['job_spec']['dir'],
            constants.CHECKPOINT_FILE_NAMES['completed']
        ).exists()

    def _complete_job(self, job=None, save=True):
        job['status'] = self.JOB_STATUSES.COMPLETED
        if save:
            self.dao.save_jobs(jobs=[job])

    def _fail_job(self, job=None, errors=None, save=True):
        job['status'] = self.JOB_STATUSES.FAILED
        job['errors'] = errors
        if save:
            self.dao.save_jobs(jobs=[job])

    def _submit_pending_jobs(self):
        tallies = collections.defaultdict(int)
        claimed_pending_jobs = self.dao.claim_jobs(query={
            'filters': [
                self.dao.generate_status_filter(
                    status=self.JOB_STATUSES.PENDING)
            ],
            'limit': 100
        })
        unsubmittable_jobs = []
        for job in claimed_pending_jobs:
            tallies['visited'] += 1
            try:
                submitted = False
                for worker in self.workers.values():
                    try:
                        if worker.can_accept_job(job=job):
                            self._submit_job_to_worker(
                                job=job, worker=worker)
                            submitted = True
                            break
                    except worker.IncompatibleJobError:
                        continue
                if submitted:
                    tallies['submitted'] += 1
                else:
                    unsubmittable_jobs.append(job)
            except self.SubmissionError:
                tallies['errors'] += 1
                self.logger.exception('SubmissionError')
        tallies['unsubmittable'] = len(unsubmittable_jobs)
        unsubmittable_job_keys = [job['key'] for job in unsubmittable_jobs]
        self.dao.update_jobs(
            updates={'status': self.JOB_STATUSES.PENDING},
            query={
                'filters': [
                    {'field': 'key', 'op': 'IN', 'arg': unsubmittable_job_keys}
                ]
            }
        )
        return tallies

    def _submit_job_to_worker(self, job=None, worker=None):
        try:
            worker_meta = worker.submit_job(job=job)
            job.update({
                'worker_key': worker.key,
                'worker_meta': worker_meta,
                'status': self.JOB_STATUSES.RUNNING,
            })
            return self.dao.save_jobs(jobs=[job])[0]
        except Exception as exc:
            job.update({'status': self.JOB_STATUSES.FAILED})
            self.dao.save_jobs(jobs=[job])
            raise self.SubmissionError() from exc

    def _tick_sources(self):
        for source in self.sources.values(): source.tick()  # noqa

    def submit_job_dir(self, job_dir=None, source_key=None, source_meta=None,
                       job_spec_defaults=None, job_spec_overrides=None):
        """Submit a job dir.

        Creates a job_spec for dir. Attempts to read job_spec file from dir
        if provided.

        Args:
            job_dir: absolute path to job_dir
            source_key: per
                :attr:`jobman.dao.jobman_sqlite_dao.JobmanSqliteDAO.JobSchema.source_key`
            source_meta: per
                :attr:`jobman.dao.jobman_sqlite_dao.JobmanSqliteDAO.JobSchema.source_meta`
            job_spec_defaults: default values for job_spec that will be
                generated for dir.
            job_spec_overrides: job_spec value overrides. Will be set after any
                job_spec values are read from job_dir.

        Returns:
            return per :meth:`submit_job_spec`.
        """
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
        """Submit a job spec.

        Args:
            job_spec: per
                :attr:`jobman.dao.jobman_sqlite_dao.JobmanSqliteDAO.JobSchema.job_spec`
            source_key: per
                :attr:`jobman.dao.jobman_sqlite_dao.JobmanSqliteDAO.JobSchema.source_key`
            source_meta: per
                :attr:`jobman.dao.jobman_sqlite_dao.JobmanSqliteDAO.JobSchema.source_meta`

        Returns:
            return job record per
                :class:`jobman.dao.jobman_sqlite_dao.JobmanSqliteDAO.JobSchema`
        """
        try:
            return self.dao.create_job(job={
                'job_spec': job_spec,
                'source_key': source_key,
                'source_meta': source_meta,
                'status': self.JOB_STATUSES.PENDING,
            })
        except Exception as exc:
            raise self.SubmissionError() from exc
