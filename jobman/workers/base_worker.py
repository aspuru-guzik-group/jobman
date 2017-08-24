import collections
import time

from jobman.utils import dot_spec_loader
from jobman.dao import worker_sqlite_dao
from jobman import constants


class BaseWorker(object):
    JOB_STATUSES = constants.JOB_STATUSES

    class IncompatibleJobError(Exception):
        pass

    def __init__(self, key=None, initialize=True, db_uri=None, dao=None,
                 engine_spec=None, acceptance_fn_spec=None):
        self.key = key
        self.db_uri = db_uri
        if dao:
            self.dao = dao  # noqa
            self.db_uri = self.dao.db_uri
        if initialize:
            self.initialize()
        self.engine = self._engine_spec_to_engine(engine_spec)
        self.acceptance_fn = self._acceptance_fn_spec_to_acceptance_fn(
            acceptance_fn_spec)
        self.cfgs = []

    def initialize(self):
        if not hasattr(self, 'dao'):
            self.dao = self.generate_dao(db_uri=self.db_uri)
        self.dao.ensure_tables()

    def _engine_spec_to_engine(self, engine_spec=None):
        engine_class = engine_spec['engine_class']
        if isinstance(engine_class, str):
            engine_class = dot_spec_loader.load_from_dot_spec(engine_class)
        engine_key = self.key + '_engine'
        engine = engine_class(**{
            'key': engine_key,
            'db_uri': self.db_uri,
            **(engine_spec.get('engine_params') or {})
        })
        return engine

    def _acceptance_fn_spec_to_acceptance_fn(self, acceptance_fn_spec=None):
        if not acceptance_fn_spec:
            acceptance_fn = self.default_acceptance_fn
        if isinstance(acceptance_fn_spec, str):
            acceptance_fn = dot_spec_loader.load_from_dot_spec(
                acceptance_fn_spec)
        elif callable(acceptance_fn_spec):
            acceptance_fn = acceptance_fn_spec
        if not acceptance_fn:
            raise Exception("Could not parse acceptance_fn_spec")
        return acceptance_fn

    def default_acceptance_fn(self, job=None, **kwargs): return True

    @property
    def dao(self):
        if not hasattr(self, '_dao'):
            self._dao = self.generate_dao(db_uri=self.db_uri)
        return self._dao

    @dao.setter
    def dao(self, value): self._dao = value

    def generate_dao(self, db_uri=None):
        return worker_sqlite_dao.WorkerSqliteDAO(
            db_uri=db_uri,
            table_prefix=('_worker_' + self.key + '_')
        )

    def can_accept_job(self, job=None):
        try:
            return self.acceptance_fn(job=job)
        except Exception as exc:
            raise self.IncompatibleJobError() from exc

    def submit_job(self, job=None):
        worker_job = self.dao.create_job(job={
            'key': job['key'],
            'status': self.JOB_STATUSES.PENDING,
        })
        worker_meta = {'key': worker_job['key']}
        return worker_meta

    def tick(self):
        self._tick_engine()
        self._update_job_engine_states()
        self._process_executed_jobs()
        self._submit_pending_jobs()

    def _tick_engine(self):
        if hasattr(self.engine, 'tick'): self.engine.tick()  # noqa

    def _update_job_engine_states(self):
        jobs = self.dao.get_jobs_for_status(
            status=self.JOB_STATUSES.RUNNING)
        if not jobs: return  # noqa
        keyed_engine_states = self.engine.get_keyed_states(
            keyed_metas=self._get_keyed_engine_metas(jobs=jobs))
        for job in jobs:
            self._set_job_engine_state(
                job=job,
                job_engine_state=keyed_engine_states.get(job['key'])
            )
        self.dao.save_jobs(jobs=jobs)

    def _get_keyed_engine_metas(self, jobs=None):
        jobs = jobs or []
        keyed_engine_metas = {
            job['key']: job.get('engine_meta') for job in jobs
        }
        return keyed_engine_metas

    def _set_job_engine_state(self, job=None, job_engine_state=None):
        if job_engine_state is None:
            if self._job_is_orphaned(job=job):
                job['status'] = self.JOB_STATUSES.EXECUTED
        else:
            job['engine_state'] = job_engine_state
            job['status'] = job_engine_state.get('status')

    def _job_is_orphaned(self, job=None):
        return (self._get_job_age(job=job) > self.submission_grace_period)

    def _get_job_age(self, job=None):
        return time.time() - job['created']

    def get_keyed_states(self, keyed_metas=None):
        worker_jobs = self.dao.query_jobs(query={
            'filters': [
                {
                    'field': 'key', 'op': 'IN',
                    'arg': [meta['key'] for meta in keyed_metas.values()]
                }
            ]
        })
        return {
            worker_job['key']: self.worker_job_to_worker_state(worker_job)
            for worker_job in worker_jobs
        }

    def worker_job_to_worker_state(self, worker_job=None):
        status = worker_job['status']
        if status == self.JOB_STATUSES.COMPLETED:
            status = self.JOB_STATUSES.EXECUTED
        elif status != self.JOB_STATUSES.FAILED:
            status = self.JOB_STATUSES.RUNNING
        return {'status': status}

    def _process_executed_jobs(self):
        claimed_executed_jobs = self.dao.claim_jobs(query={
            'filters': [
                self.dao.generate_status_filter(
                    status=self.JOB_STATUSES.EXECUTED),
            ],
            'limit': 100,
        })
        for executed_job in claimed_executed_jobs:
            self._process_executed_job(executed_job=executed_job, save=False)
        self.dao.save_jobs(jobs=claimed_executed_jobs)

    def _process_executed_job(self, executed_job=None, save=True):
        self._complete_job(executed_job, save=save)

    def _complete_job(self, job=None, save=True):
        job['status'] = self.JOB_STATUSES.COMPLETED
        if save: self.dao.save_jobs(jobs=[job])  # noqa

    def _submit_pending_jobs(self):
        tallies = collections.defaultdict(int)
        claimed_pending_jobs = self._claim_pending_jobs()
        jobman_jobs = self._get_related_jobman_jobs(jobs=claimed_pending_jobs)
        worker_jobs_to_update = []
        for job in claimed_pending_jobs:
            tallies['visited'] += 1
            try:
                engine_meta = self._submit_jobman_job_to_engine(
                    jobman_jobs[job['key']])
                job['engine_meta'] = engine_meta
                job['status'] = self.JOB_STATUSES.RUNNING
                tallies['submitted'] += 1
            except self.engine.SubmissionError:
                job['status'] = self.JOB_STATUSES.FAILED
                tallies['errors'] += 1
                self.logger.exception('SubmissionError')
            worker_jobs_to_update.append(job)
        self.dao.save_jobs(worker_jobs_to_update)
        return tallies

    def _claim_pending_jobs(self):
        return self.dao.claim_jobs(query={
            'filters': [
                self.dao.generate_status_filter(
                    status=self.JOB_STATUSES.PENDING)
            ],
            'limit': 100
        })

    def _get_related_jobman_jobs(self, jobs=None):
        jobman_jobs = self.jobman.dao.query_jobs(query={
            'filters': [
                {'field': 'key', 'op': 'IN',
                 'arg': [job['key'] for job in jobs]}
            ]
        })
        keyed_jobman_jobs = {
            jobman_job['key']: jobman_job
            for jobman_job in jobman_jobs
        }
        return keyed_jobman_jobs

    def _submit_jobman_job_to_engine(self, jobman_job=None):
        engine_meta = self.engine.submit_job(
            job=jobman_job, extra_cfgs=self.cfgs)
        return engine_meta
