import time

from jobman.utils import dot_spec_loader
from jobman.dao import worker_sqlite_dao


class Worker(object):
    class IncompatibleJobError(Exception):
        pass

    def __init__(self, key=None, db_uri=None, engine_spec=None,
                 acceptance_fn_spec=None,
                 use_batching=False, batchable_filters=None,
                 max_batchable_wait=120, target_batch_time=(60 * 60),
                 default_job_time=(5 * 60)):
        self.key = key
        self.engine = self._engine_spec_to_engine(engine_spec)
        self.acceptance_fn = self._acceptance_fn_spec_to_acceptance_fn(
            acceptance_fn_spec)
        self.use_batching = use_batching
        self.batchable_filters = (
            (batchable_filters or [])
            + self._get_default_batchable_filters()
        )
        self.max_batchable_wait = max_batchable_wait
        self.target_batch_time = target_batch_time
        self.default_job_time = default_job_time
        self.cfgs = []

    def _engine_spec_to_engine(self, engine_spec=None):
        engine_class = engine_spec['engine_class']
        if isinstance(engine_class, str):
            engine_class = dot_spec_loader.load_from_dot_spec(engine_class)
        engine = engine_class(**(engine_spec.get('engine_params') or {}))
        engine.key = self.key + '_engine'
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

    def generate_dao(self, db_uri=None):
        return worker_sqlite_dao.WorkerSqliteDAO(
            db_uri=db_uri,
            table_prefix=('_worker_' + self.key)
        )

    def can_accept_job(self, job=None):
        try:
            return self.acceptance_fn(job=job)
        except Exception as exc:
            raise self.IncompatibleJobError() from exc

    def submit_job(self, job=None):
        return self.dao.create_job(job={
            'key': job['key'],
            'status': 'PENDING',
        })

    def get_keyed_states(self, keyed_metas=None):
        return self.engine.get_keyed_states(keyed_metas)

    def tick(self):
        if hasattr(self.engine, 'tick'):
            self.engine.tick()
        self._update_job_engine_states(jobs=self.dao.get_running_jobs())
        self._process_executed_jobs()
        if self.use_batching:
            self._batch_tick()
        self._process_submittable_jobs()

    def _update_job_engine_states(self, jobs=None):
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

    def _process_executed_jobs(self):
        for executed_job in self.dao.get_jobs_for_status(status='EXECUTED'):
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

    def _batch_tick(self):
        self._mark_jobs_as_batchable()
        self._process_batchable_jobs()

    def _mark_jobs_as_batchable(self):
        with self.dao.get_lock():
            batch_candidates = self._get_batch_candidates()
            worker_jobs_to_update = []
            for key, candidate in batch_candidates.items():
                worker_job = candidate['worker_job']
                worker_job['batchable'] = self._job_is_batchable(
                    job=candidate['jobman_job'])
                worker_jobs_to_update.append(worker_job)
            self.save_jobs(jobs=worker_jobs_to_update)

    def _get_candidate_batchable_jobs(self):
        candidate_worker_jobs = self.dao.query_jobs(query={
            'filters': [
                {'field': 'status', 'op': '=', 'arg': 'PENDING'},
                {'field': 'batchable', 'op': 'IS', 'arg': None}
            ]
        })
        jobman_jobs = self._get_related_jobman_jobs(jobs=candidate_worker_jobs)
        return {
            worker_job['key']: {
                'worker_job': worker_job,
                'jobman_job': jobman_jobs[worker_job['key']]
            }
            for worker_job in candidate_worker_jobs
        }

    def _get_related_jobman_jobs(self, jobs=None):
        related_jobman_jobs = self.jobman.dao.query_jobs(query={
            'filters': [
                {'field': 'key', 'op': 'IN',
                 'arg': [job['key'] for job in jobs]}
            ]
        })
        keyed_jobman_jobs = {
            jobman_job['key']: jobman_job
            for jobman_job in related_jobman_jobs
        }
        return keyed_jobman_jobs

    def _job_is_batchable(self, job=None):
        return any(filter_(job) for filter_ in self.batchable_filters)

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
        keyed_jobman_jobs = self._get_related_jobman_jobs(jobs=batchable_jobs)
        for batchable_job in batchable_jobs:
            jobman_job = keyed_jobman_jobs[batchable_job['key']]
            current_partition.append(batchable_job)
            current_partition_time += self._get_job_time(job=jobman_job)
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
                if num_submissions > num_slots: break  # noqa
                try:
                    self._submit_job_to_engine(job=job)
                    num_submissions += 1
                except self.SubmissionError:
                    self.logger.exception('SubmissionError')

    def _get_submittable_jobs(self, exclude_batchable_jobs=True):
        filters = [{'field': 'status', 'op': '=', 'arg': 'PENDING'}]
        if exclude_batchable_jobs:
            filters.append({'field': 'batchable', 'op': '! =', 'arg': 1})
        return self.dao.get_jobs(query={'filters': filters})

    def _submit_job_to_engine(self, job=None):
        try:
            submit_to_engine_fn = self._get_submit_to_engine_fn_for_job(job)
            engine_meta = submit_to_engine_fn(job)
            job.update({'engine_meta': engine_meta, 'status': 'RUNNING'})
            return self.save_jobs(jobs=[job])[0]
        except Exception as exc:
            job.update({'status': 'FAILED'})
            self.save_jobs(jobs=[job])
            raise self.SubmissionError() from exc

    def _get_submit_to_engine_fn_for_job(self, job=None):
        if job.get('is_batch'):
            return self._submit_batch_job_to_engine
        return self._submit_single_job_to_engine

    def _submit_single_job_to_engine(self, job=None):
        engine_meta = self.engine.submit_job(job=job, extra_cfgs=self.cfgs)
        return engine_meta

    def _submit_batch_job_to_engine(self, job=None):
        worker_subjobs = self._get_batch_subjobs(batch_job=job)
        jobman_subjobs = self._get_related_jobman_jobs(jobs=worker_subjobs)
        return self.engine.submit_batch_job(
            batch_job=job,
            subjobs=jobman_subjobs.values(),
            extra_cfgs=self.cfgs
        )
