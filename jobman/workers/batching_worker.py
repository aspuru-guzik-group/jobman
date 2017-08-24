import time

from .base_worker import BaseWorker


class BatchingWorker(BaseWorker):
    def __init__(self, *args, batchable_filters=None,
                 max_batchable_wait=120, target_batch_time=(60 * 60),
                 default_job_time=(5 * 60), **kwargs):
        super().__init__(*args, **kwargs)
        self.batchable_filters = (
            (batchable_filters or [])
            + self._get_default_batchable_filters()
        )
        self.max_batchable_wait = max_batchable_wait
        self.target_batch_time = target_batch_time
        self.default_job_time = default_job_time

    def _get_default_batchable_filters(self):
        def job_spec_has_batchable(job=None):
            return bool(job.get('job_spec', {}).get('batchable'))
        return [job_spec_has_batchable]

    def tick(self):
        self._tick_engine()
        self._update_job_engine_states()
        self._process_executed_jobs()
        self._mark_jobs_as_batchable()
        self._process_batchable_jobs()
        self._submit_pending_batch_jobs()
        self._submit_pending_jobs()

    def _process_executed_job(self, executed_job=None, save=True):
        process_fn = super()._process_executed_job
        if executed_job.get('is_batch'):
            process_fn = self._process_executed_batch_job
        process_fn(executed_job=executed_job, save=save)

    def _process_executed_batch_job(self, executed_job=None, save=True):
        for subjob in self._get_batch_subjobs(batch_job=executed_job):
            self._process_executed_job(executed_job=subjob)
        self._complete_job(job=executed_job, save=save)

    def _get_batch_subjobs(self, batch_job=None):
        subjob_keys = batch_job['batch_meta']['subjob_keys']
        return self.dao.query_jobs(query={
            'filters': [{'field': 'key', 'op': 'IN', 'arg': subjob_keys}]
        })

    def _mark_jobs_as_batchable(self):
        with self.dao.get_lock():
            batch_candidates = self._get_batch_candidates()
            worker_jobs_to_update = []
            for key, candidate in batch_candidates.items():
                worker_job = candidate['worker_job']
                worker_job['batchable'] = self._job_is_batchable(
                    job=candidate['jobman_job'])
                worker_jobs_to_update.append(worker_job)
            self.dao.save_jobs(jobs=worker_jobs_to_update)

    def _get_batch_candidates(self):
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

    def _job_is_batchable(self, job=None):
        return any(filter_(job) for filter_ in self.batchable_filters)

    def _process_batchable_jobs(self):
        with self.dao.get_lock():
            batchable_jobs = self._get_batchable_jobs()
            self._batchify_jobs(batchable_jobs=batchable_jobs)

    def _get_batchable_jobs(self):
        age_threshold = time.time() - self.max_batchable_wait
        return self.dao.query_jobs(query={
            'filters': [
                {'field': 'batchable', 'op': '=', 'arg': 1},
                self.dao.generate_status_filter(status='PENDING'),
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
            {
                **subjob,
                'status': 'PENDING_BATCH',
                'parent_batch_key': batch_key,
            }
            for subjob in subjobs
        ]
        self.dao.save_jobs(jobs=patched_subjobs)
        self.dao.create_job(job={
            'key': batch_key,
            'batch_meta': {
                'subjob_keys': [subjob['key'] for subjob in subjobs]
            },
            'is_batch': 1,
            'status': 'PENDING'
        })

    def _submit_pending_batch_jobs(self):
        pending_batch_jobs = self._claim_pending_batch_jobs()
        for job in pending_batch_jobs:
            try:
                engine_meta = self._submit_batch_job_to_engine(job)
                job['engine_meta'] = engine_meta
                job['status'] = self.JOB_STATUSES.RUNNING
            except self.engine.SubmissionError:
                job['status'] = self.JOB_STATUSES.FAILED
                self.logger.exception('SubmissionError')
        self.dao.save_jobs(pending_batch_jobs)

    def _claim_pending_batch_jobs(self):
        return self.dao.claim_jobs(query={
            'filters': [
                self.dao.generate_status_filter(
                    status=self.JOB_STATUSES.PENDING),
                {'field': 'is_batch', 'op': '=', 'arg': 1},
            ]
        })

    def _submit_batch_job_to_engine(self, job=None):
        worker_subjobs = self._get_batch_subjobs(batch_job=job)
        jobman_subjobs = self._get_related_jobman_jobs(jobs=worker_subjobs)
        return self.engine.submit_batch_job(
            batch_job=job,
            subjobs=jobman_subjobs.values(),
            extra_cfgs=self.cfgs
        )

    def _claim_pending_jobs(self, exclude_batchable_jobs=True):
        filters = [self.dao.generate_status_filter(status='PENDING')]
        if exclude_batchable_jobs:
            filters.append({'field': 'batchable', 'op': '! =', 'arg': 1})
        return self.dao.claim_jobs(query={
            'filters': filters,
            'limit': 100,
        })
