from collections import defaultdict
import unittest
from unittest.mock import call, MagicMock, patch


from .. import jobman as _jobman

class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self.engine = MagicMock()
        self.dao = MagicMock()
        self.jobman = self.generate_jobman()

    def generate_jobman(self, **kwargs):
        default_kwargs = {
            'dao': self.dao,
            'engine': self.engine
        }
        jobman = _jobman.JobMan(**{**default_kwargs, **kwargs})
        return jobman

    def mockify_jobman_attrs(self, attrs=None):
        for attr in attrs: setattr(self.jobman, attr, MagicMock())

    def mockify_module_attrs(self, attrs=None, module=_jobman):
        patchers = {attr: patch.object(module, attr)
                    for attr in attrs}
        mocks = {}
        for attr, patcher in patchers.items():
            self.addCleanup(patcher.stop)
            mocks[attr] = patcher.start()
        return mocks

class _SetupTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.jobman = self.generate_jobman(setup=False)

    def test_calls_get_default_batchable_filters(self):
        self.mockify_jobman_attrs(attrs=['_get_default_batchable_filters'])
        self.jobman._setup()
        self.assertEqual(self.jobman._get_default_batchable_filters.call_args,
                         call())

class EnsureDbTestCase(BaseTestCase):
    def test_dispatches_to_dao(self):
        self.jobman.ensure_db()
        self.assertEqual(self.jobman.dao.ensure_db.call_args, call())

class SubmitJobDir_MetaTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.jobdir_meta = MagicMock()
        self.source = MagicMock()
        self.source_meta = MagicMock()
        self.mockify_jobman_attrs(attrs=['_jobdir_meta_to_job', '_create_job'])
        self.jobman._jobdir_meta_to_job.return_value = defaultdict(MagicMock)
        self.expected_job = self.jobman._jobdir_meta_to_job.return_value
        self.result = self.jobman.submit_jobdir_meta(
            jobdir_meta=self.jobdir_meta, source=self.source,
            source_meta=self.source_meta)

    def test_assembles_job_from_jobdir_meta(self):
        self.assertEqual(
            self.jobman._jobdir_meta_to_job.call_args,
            call(jobdir_meta=self.jobdir_meta, source=self.source,
                 source_meta=self.source_meta)
        )

    def test_creates_job(self):
        self.assertEqual(self.jobman._create_job.call_args,
                         call(job=self.expected_job))

    def test_returns_created_job(self):
        self.assertEqual(self.result, self.jobman._create_job.return_value)

class _SubmissionToJobTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.jobdir_meta = MagicMock()
        self.source = MagicMock()
        self.source_meta = MagicMock()
        self.result = self.jobman._jobdir_meta_to_job(
            jobdir_meta=self.jobdir_meta, source=self.source,
            source_meta=self.source_meta
        )

    def test_returns_expected_job(self):
        expected_result = {
            'jobdir_meta': self.jobdir_meta,
            'source': self.source,
            'source_meta': self.source_meta,
            'status': 'PENDING',
        }
        self.assertEqual(self.result, expected_result)

class _JobIsBatchableTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.job = MagicMock()
        self.filters = self._generate_filters()

    def _generate_filters(self):
        def passing_filter(*args, **kwargs): return True
        def failing_filter(*args, **kwargs): return False
        return {
            'passing': passing_filter,
            'failing': failing_filter,
        }

    def _job_is_batchable(self):
        return self.jobman._job_is_batchable(job=self.job)

    def test_returns_true_if_any_filters_match(self):
        self.jobman.batchable_filters = [
            self.filters['failing'], self.filters['passing']
        ]
        self.assertEqual(self._job_is_batchable(), True)

    def test_returns_false_if_no_filters_match(self):
        self.jobman.batchable_filters = [
            self.filters['failing'] for i in range(3)
        ]
        self.assertEqual(self._job_is_batchable(), False)

class TickTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_jobman_attrs(attrs=[
            '_job_engine_states_are_stale',
            'get_running_jobs',
            '_update_job_engine_states',
            '_process_executed_jobs',
            '_tick_batching',
            '_process_submittable_jobs',
        ])

    def _tick(self): self.jobman.tick()

    def test_updates_engine_states_if_stale(self):
        self.jobman._job_engine_states_are_stale.return_value = True
        self._tick()
        self.assertEqual(self.jobman.get_running_jobs.call_args, call())
        self.assertEqual(self.jobman._update_job_engine_states.call_args,
                         call(jobs=self.jobman.get_running_jobs.return_value))

    def test_does_not_update_engine_states_if_not_stale(self):
        self.jobman._job_engine_states_are_stale.return_value = False
        self._tick()
        self.assertEqual(self.jobman.get_running_jobs.call_args, None)
        self.assertEqual(self.jobman._update_job_engine_states.call_args, None)

    def test_processes_executed_jobs(self):
        self._tick()
        self.assertEqual(self.jobman._process_executed_jobs.call_args,
                         call())

    def test_calls_tick_batching_if_use_batching(self):
        self.jobman.use_batching = True
        self._tick()
        self.assertEqual(self.jobman._tick_batching.call_args, call())

    def test_does_not_call_tick_batching_if_not_use_batching(self):
        self.jobman.use_batching = False
        self._tick()
        self.assertEqual(self.jobman._tick_batching.call_args, None)

    def test_processes_submittable_jobs(self):
        self._tick()
        self.assertEqual(self.jobman._process_submittable_jobs.call_args,
                         call())

class _ProcessExecutedJobsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_jobman_attrs(attrs=['get_jobs_for_status',
                                         '_process_executed_job'])
        self.jobman.get_jobs_for_status.return_value = \
                [MagicMock() for i in range(3)]
        self.jobman._process_executed_jobs()

    def test_gets_executed_jobs(self):
        self.assertEqual(self.jobman.get_jobs_for_status.call_args,
                         call(status='EXECUTED'))

    def test_dispatches_to_process_executed_job(self):
        self.assertEqual(
            self.jobman._process_executed_job.call_args_list,
            [call(executed_job=executed_job)
             for executed_job in self.jobman.get_jobs_for_status.return_value]
        )

class _ProcessExecutedJobTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.executed_job = defaultdict(MagicMock)

    def _process_executed_job(self):
        self.jobman._process_executed_job(executed_job=self.executed_job)

    def test_dispatches_for_batch_job(self):
        self.jobman._process_executed_batch_job = MagicMock()
        self.executed_job['is_batch'] = 1
        self._process_executed_job()
        self.assertEqual(self.jobman._process_executed_batch_job.call_args,
                         call(executed_job=self.executed_job))

    def test_dispatches_for_single_job(self):
        self.jobman._process_executed_single_job = MagicMock()
        self.executed_job['is_batch'] = 0
        self._process_executed_job()
        self.assertEqual(self.jobman._process_executed_single_job.call_args,
                         call(executed_job=self.executed_job))

class _ProcessExecutedBatchJobTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_jobman_attrs(attrs=['_get_batch_subjobs',
                                         '_process_executed_job',
                                         '_complete_job'])
        self.executed_job = defaultdict(MagicMock)
        self.jobman._process_executed_batch_job(executed_job=self.executed_job)

    def test_gets_subjobs(self):
        self.assertEqual(self.jobman._get_batch_subjobs.call_args,
                         call(batch_job=self.executed_job))

    def test_processes_subjobs(self):
        self.assertEqual(
            self.jobman._process_executed_job.call_args_list,
            [call(executed_job=subjob)
             for subjob in self.jobman._get_batch_subjobs.return_value]
        )

    def test_completes_batch_job(self):
        self.assertEqual(self.jobman._complete_job.call_args,
                         call(job=self.executed_job))

class _GetBatchSubjobsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.batch_job = defaultdict(MagicMock)
        self.batch_job['batch_meta'] = {
            'subjob_keys': [MagicMock() for i in range(3)]
        }
        self.result = self.jobman._get_batch_subjobs(batch_job=self.batch_job)

    def test_queries_for_subjob_keys_in_batch_meta(self):
        self.assertEqual(
            self.jobman.dao.get_jobs.call_args,
            call(query={
                'filters': [
                    {'field': 'key', 'op': 'IN',
                     'arg': self.batch_job['batch_meta']['subjob_keys']}
                ]
            })
        )
        self.assertEqual(self.result, self.jobman.dao.get_jobs.return_value)

class _CompleteJobTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.job = defaultdict(MagicMock)
        self.mockify_jobman_attrs(attrs=['save_jobs'])
        self.jobman._complete_job(job=self.job)

    def test_marks_job_as_completed(self):
        self.assertEqual(self.job['status'], 'COMPLETED')

    def test_saves_job(self):
        self.assertEqual(self.jobman.save_jobs.call_args, call(jobs=[self.job]))

class _ProcessExecutedSingleJobTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.executed_job = defaultdict(MagicMock)
        self.mockify_jobman_attrs(attrs=['_complete_job'])
        self.jobman._process_executed_single_job(executed_job=self.executed_job)

    def test_completes_job(self):
        self.assertEqual(self.jobman._complete_job.call_args,
                         call(job=self.executed_job))

class _TickBatchingTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_jobman_attrs(attrs=['_mark_jobs_as_batchable',
                                         '_process_batchable_jobs'])
        self.jobman._tick_batching()

    def test_mark_jobs_as_batchable(self):
        self.assertEqual(self.jobman._mark_jobs_as_batchable.call_args, call())

    def test_processes_batchable_jobs(self):
        self.assertEqual(self.jobman._process_batchable_jobs.call_args, call())

class _MarkJobsAsBatchableTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_jobman_attrs(attrs=['_get_candidate_batchable_jobs',
                                         '_job_is_batchable',
                                         '_get_lock', 'save_jobs'])
        self.jobman._get_candidate_batchable_jobs.return_value = [
            defaultdict(MagicMock) for i in range(3)
        ]
        self.expected_candidate_batchable_jobs = \
                self.jobman._get_candidate_batchable_jobs.return_value
        self.jobman._mark_jobs_as_batchable()

    def test_gets_lock(self):
        self.assertEqual(self.jobman._get_lock.call_args, call())

    def test_gets_candidate_batchable_jobs(self):
        self.assertEqual(self.jobman._get_candidate_batchable_jobs.call_args,
                         call())

    def test_marks_job_as_batchable(self):
        self.assertEqual(
            self.jobman._job_is_batchable.call_args_list,
            [call(job=job) for job in self.expected_candidate_batchable_jobs]
        )
        batchable_values = []
        expected_batchable_values = []
        for job in self.expected_candidate_batchable_jobs:
            batchable_values.append(job['batchable'])
            expected_batchable_values.append(
                self.jobman._job_is_batchable.return_value)
        self.assertEqual(batchable_values, expected_batchable_values)

    def test_saves_jobs(self):
        self.assertEqual(self.jobman.save_jobs.call_args,
                         call(jobs=self.expected_candidate_batchable_jobs))

class _ProcessBatchableJobsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_jobman_attrs(attrs=['_get_lock', '_get_batchable_jobs',
                                         '_batchify_jobs'])
        self.jobman._process_batchable_jobs()

    def test_gets_lock(self):
        self.assertEqual(self.jobman._get_lock.call_args, call())

    def test_gets_batchable_jobs(self):
        self.assertEqual(self.jobman._get_batchable_jobs.call_args, call())

    def test_batchifies_jobs(self):
        self.assertEqual(
            self.jobman._batchify_jobs.call_args,
            call(batchable_jobs=self.jobman._get_batchable_jobs.return_value)
        )

class _GetBatchableJobsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.module_mocks = self.mockify_module_attrs(attrs=['time'])
        self.result = self.jobman._get_batchable_jobs()

    def test_queries_dao(self):
        expected_age_threshold = (self.module_mocks['time'].time.return_value
                                  - self.jobman.max_batchable_wait)
        self.assertEqual(
            self.jobman.dao.get_jobs.call_args,
            call(query={
                'filters': [
                    {'field': 'batchable', 'op': '=', 'arg': 1},
                    {'field': 'status', 'op': '=', 'arg': 'PENDING'},
                    {'field': 'modified', 'op': '>=',
                     'arg': expected_age_threshold}
                ]
            })
        )
        self.assertEqual(self.result, self.dao.get_jobs.return_value)

class _BatchifyJobsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.batchable_jobs = [MagicMock() for i in range(3)]
        self.mockify_jobman_attrs(attrs=['_make_batch_subjob_partitions',
                                         '_make_batch_job'])
        self.jobman._batchify_jobs(batchable_jobs=self.batchable_jobs)

    def test_makes_batch_subjob_partitions(self):
        self.assertEqual(self.jobman._make_batch_subjob_partitions.call_args,
                         call(batchable_jobs=self.batchable_jobs))

    def test_makes_batch_jobs(self):
        expected_subjob_partitions = \
                self.jobman._make_batch_subjob_partitions.return_value
        self.assertEqual(
            self.jobman._make_batch_job.call_args_list,
            [call(subjobs=subjob_partition)
             for subjob_partition in expected_subjob_partitions]
        )

class _MakeBatchSubJobPartitionsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.jobman.target_batch_time = 3
        self.batchable_jobs = [
            self._generate_batchable_job(estimated_run_time=1)
            for i in range(self.jobman.target_batch_time * 2 + 1)
        ]
        self.result = self.jobman._make_batch_subjob_partitions(
            batchable_jobs=self.batchable_jobs)

    def _generate_batchable_job(self, estimated_run_time=1):
        batchable_job = defaultdict(MagicMock)
        batchable_job['jobdir_meta'] = defaultdict(MagicMock, **{
            'estimated_run_time': estimated_run_time
        })
        return batchable_job

    def test_makes_expected_partitions(self):
        expected_partitions = []
        current_partition = []
        current_partition_time = 0
        for batchable_job in self.batchable_jobs:
            current_partition.append(batchable_job)
            current_partition_time += self.jobman._get_estimated_job_run_time(
                job=batchable_job)
            if current_partition_time >= self.jobman.target_batch_time:
                expected_partitions.append(current_partition)
                current_partition_time = 0
                current_partition = []
        if current_partition: expected_partitions.append(current_partition)
        self.assertEqual(self.result, expected_partitions)

class _GetEstimatedJobRunTime(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.job = {'jobdir_meta': {'estimated_run_time': MagicMock()}}

    def _get_estimated_job_run_time(self):
        return self.jobman._get_estimated_job_run_time(job=self.job)

    def test_gets_from_jobdir_meta(self):
        self.assertEqual(self._get_estimated_job_run_time(),
                         self.job['jobdir_meta']['estimated_run_time'])

    def test_fallsback_to_default(self):
        del self.job['jobdir_meta']['estimated_run_time']
        self.assertEqual(self._get_estimated_job_run_time(),
                         self.jobman.default_estimated_job_run_time)

class _MakeBatchJobTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_jobman_attrs(attrs=['save_jobs', '_create_job'])
        self.subjobs = [defaultdict(MagicMock, **{'key': MagicMock()})
                        for i in range(3)]
        self.jobman._make_batch_job(subjobs=self.subjobs)

    def test_generates_batch_key(self):
        self.assertEqual(self.jobman.dao.generate_key.call_args, call())

    def test_patches_subjobs_with_batch_key_and_status(self):
        expected_patched_subjobs = [
            {**subjob, 'status': 'PENDING',
             'parent_batch_key': self.jobman.dao.generate_key.return_value}
            for subjob in self.subjobs
        ]
        self.assertEqual(self.jobman.save_jobs.call_args,
                         call(jobs=expected_patched_subjobs))

    def test_creates_batch_job(self):
        expected_batch_job = {
            'key': self.jobman.dao.generate_key.return_value,
            'batch_meta': {
                'subjob_keys': [subjob['key'] for subjob in self.subjobs]
            },
            'is_batch': 1,
            'status': 'PENDING'
        }
        self.assertEqual(self.jobman._create_job.call_args,
                         call(job=expected_batch_job))

class _ProcessSubmittableJobsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_jobman_attrs(attrs=['_get_lock', '_get_submittable_jobs',
                                         'get_num_free_slots',
                                         '_submit_job_to_engine'])
        self.jobman.use_batching = MagicMock()
        self.submittable_jobs = [MagicMock() for i in range(5)]
        self.jobman._get_submittable_jobs.return_value = self.submittable_jobs
        self.num_free_slots = len(self.submittable_jobs) - 2
        self.jobman.get_num_free_slots.return_value = self.num_free_slots
        self.jobman._process_submittable_jobs()

    def test_gets_lock(self):
        self.assertEqual(self.jobman._get_lock.call_args, call())

    def test_gets_submittable_jobs(self):
        self.assertEqual(
            self.jobman._get_submittable_jobs.call_args,
            call(exclude_batchable_jobs=(not self.jobman.use_batching))
        )

    def test_submits_until_free_slots_are_filled(self):
        self.assertEqual(
            self.jobman._submit_job_to_engine.call_args_list,
            [call(job=job)
             for job in self.submittable_jobs[:(self.num_free_slots + 1)]]
        )


class _GetSubmittableJobsTestCase(BaseTestCase):
    def _get_submittable_jobs(self, **kwargs):
        return self.jobman._get_submittable_jobs(**kwargs)

    def test_makes_expected_query_for_exclude_batchable_jobs(self):
        self._get_submittable_jobs(exclude_batchable_jobs=True)
        self.assertEqual(
            self.jobman.dao.get_jobs.call_args,
            call(query={
                'filters': [
                    {'field': 'status', 'op': '=', 'arg': 'PENDING'},
                    {'field': 'batchable', 'op': '! =', 'arg': 1},
                ]
            })
        )

    def test_makes_expected_query_for_include_batchable_jobs(self):
        self._get_submittable_jobs(exclude_batchable_jobs=False)
        self.assertEqual(
            self.jobman.dao.get_jobs.call_args,
            call(query={
                'filters': [
                    {'field': 'status', 'op': '=', 'arg': 'PENDING'},
                ]
            })
        )

    def test_returns_query_result(self):
        result = self._get_submittable_jobs()
        self.assertEqual(result, self.jobman.dao.get_jobs.return_value)

class _SubmitJobToEngineTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.job = MagicMock()
        self.mockify_jobman_attrs(attrs=['_get_submit_to_engine_fn_for_job',
                                         'save_jobs'])
        self.expected_submission_fn = \
                self.jobman._get_submit_to_engine_fn_for_job.return_value

    def _submit_job_to_engine(self):
        return self.jobman._submit_job_to_engine(job=self.job)

    def test_gets_submission_fn(self):
        self._submit_job_to_engine()
        self.assertEqual(self.jobman._get_submit_to_engine_fn_for_job.call_args,
                         call(job=self.job))

    def test_submits_via_submission_fn(self):
        self._submit_job_to_engine()
        self.assertEqual(self.expected_submission_fn.call_args,
                         call(job=self.job))

    def test_updates_engine_meta_and_status(self):
        self._submit_job_to_engine()
        self.assertEqual(
            self.job.update.call_args,
            call({'engine_meta': self.expected_submission_fn.return_value,
                  'status': 'RUNNING'})
        )
        self.assertEqual(self.jobman.save_jobs.call_args, call(jobs=[self.job]))

    def test_raises_exception_for_bad_submission(self):
        exception = Exception("bad submission")
        self.expected_submission_fn.side_effect = exception
        with self.assertRaises(self.jobman.SubmissionError):
            self._submit_job_to_engine()
            self.assertEqual(self.job.update.call_args,
                             call({'status': 'FAILED'}))
            self.assertEqual(self.jobman.save_jobs.call_args,
                             call(jobs=[self.job]))

class _GetSubmitToEngineFnForJobTestCase(BaseTestCase):
    def test_handles_batch_job(self):
        job = {'is_batch': 1}
        result = self.jobman._get_submit_to_engine_fn_for_job(job=job)
        self.assertEqual(result, self.jobman._submit_batch_job_to_engine)

    def test_defaults_to_basic_job(self):
        job = {'is_batch': None}
        result = self.jobman._get_submit_to_engine_fn_for_job(job=job)
        self.assertEqual(result, self.jobman._submit_single_job_to_engine)

class _SubmitSingleJobToEngine(BaseTestCase):
    def test_dispatches_to_engine_submit_job(self):
        job = MagicMock()
        result = self.jobman._submit_single_job_to_engine(job=job)
        self.assertEqual(self.jobman.engine.submit_job.call_args, call(job=job))
        self.assertEqual(result, self.jobman.engine.submit_job.return_value)

class _SubmitBatchJobToEngine(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_jobman_attrs(attrs=['_get_batch_subjobs'])
        self.job = MagicMock()
        self.result = self.jobman._submit_batch_job_to_engine(job=self.job)

    def test_gets_batch_subjobs(self):
        self.assertEqual(self.jobman._get_batch_subjobs.call_args,
                         call(batch_job=self.job))

    def test_dispatches_to_engine_submit_batch_job(self):
        expected_subjobs = self.jobman._get_batch_subjobs.return_value
        self.assertEqual(self.jobman.engine.submit_batch_job.call_args,
                         call(batch_job=self.job, subjobs=expected_subjobs))
        self.assertEqual(self.result,
                         self.jobman.engine.submit_batch_job.return_value)

class _CreateJobTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.job = MagicMock()

    def _create(self):
        return self.jobman._create_job(job=self.job)

    def test_dispatches_to_dao(self):
        self._create()
        self.assertEqual(self.jobman.dao.create_job.call_args,
                         call(job=self.job))

class GetJobsTestCase(BaseTestCase):
    def test_dispatches_to_dao(self):
        query = MagicMock()
        result = self.jobman.get_jobs(query=query)
        self.assertEqual(
            self.jobman.dao.get_jobs.call_args,
            call(query=query)
        )
        self.assertEqual(result, self.jobman.dao.get_jobs.return_value)

class GetNumFreeSlotsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.jobman.max_running_jobs = 3
        self.jobman.get_running_jobs = MagicMock(return_value=[1,2])

    def test_returns_max_running_jobs_less_running_jobs(self):
        self.assertEqual(
            self.jobman.get_num_free_slots(),
            (self.jobman.max_running_jobs - len(self.jobman.get_running_jobs()))
        )

class SaveJobsTestCase(BaseTestCase):
    def test_dispatches_to_dao(self):
        jobs = MagicMock()
        result = self.jobman.save_jobs(jobs=jobs)
        self.assertEqual(self.jobman.dao.save_jobs.call_args, call(jobs=jobs))
        self.assertEqual(result, self.jobman.dao.save_jobs.return_value)

class GetKvp(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.key = 'some_key'

    def test_returns_from__state_if_set(self):
        self.jobman._kvps[self.key] = MagicMock()
        self.assertEqual(self.jobman.get_kvp(key=self.key),
                         self.jobman._kvps[self.key])

    def test_returns_from_dao_and_sets_key_if_key_not_set(self):
        result = self.jobman.get_kvp(key=self.key)
        expected_result = self.jobman.dao.get_kvps.return_value[0]['value']
        self.assertEqual(
            self.jobman.dao.get_kvps.call_args,
            call(query={
                'filters': [
                    {'field': 'key', 'op': '=', 'arg': self.key}
                ]
            })
        )
        self.assertEqual(result, expected_result)
        self.assertEqual(self.jobman._kvps[self.key], expected_result)

class GetRunningJobsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.status_filter = {'field': 'status', 'op': '=', 'arg': 'RUNNING'}
        self.batch_subjob_filter = {'field': 'parent_batch_key', 'op': 'IS',
                                    'arg': None}

    def test_excludes_batch_subjobs_by_default(self):
        result = self.jobman.get_running_jobs()
        self.assertEqual(
            self.dao.get_jobs.call_args,
            call(query={
                'filters': [self.status_filter, self.batch_subjob_filter]
            })
        )
        self.assertEqual(result, self.dao.get_jobs.return_value)

    def test_includes_batch_subjobs_if_requested(self):
        result = self.jobman.get_running_jobs(include_batch_subjobs=True)
        self.assertEqual(
            self.dao.get_jobs.call_args,
            call(query={
                'filters': [self.status_filter]
            })
        )
        self.assertEqual(result, self.dao.get_jobs.return_value)

class _JobsEngineStatesAreStaleTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.jobman.jobs_ttl = 999
        self.mockify_jobman_attrs(attrs=['_get_job_engine_states_age'])

    def test_returns_true_age_too_old(self):
        self.jobman._get_job_engine_states_age.return_value = \
                self.jobman.job_engine_states_ttl + 1
        self.assertEqual(self.jobman._job_engine_states_are_stale(), True)

    def test_returns_true_if_age_is_none(self):
        self.jobman._get_job_engine_states_age.return_value = None
        self.assertEqual(self.jobman._job_engine_states_are_stale(), True)

    def test_returns_false_if_jobs_updated_timestamp_not_too_old(self):
        self.jobman._get_job_engine_states_age.return_value = \
                self.jobman.job_engine_states_ttl - 1
        self.assertEqual(self.jobman._job_engine_states_are_stale(), False)

class _GetJobEngineStatesAge(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_jobman_attrs(attrs=['get_kvp'])
        self.jobman.get_kvp.return_value = 999

    @patch.object(_jobman, 'time')
    def test_returns_age(self, mock_time):
        mock_time.time.return_value = 123
        age = self.jobman._get_job_engine_states_age()
        self.assertEqual(self.jobman.get_kvp.call_args,
                         call(key='job_engine_states_modified'))
        expected_age = mock_time.time() - \
                self.jobman.get_kvp.return_value
        self.assertEqual(age, expected_age)

class _UpdateJobEngineStatesTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.jobs = [MagicMock() for i in range(3)]
        self.mockify_jobman_attrs(attrs=['set_kvp', '_get_keyed_engine_metas',
                                         '_set_job_engine_state'])

    def _update(self):
        self.jobman._update_job_engine_states(jobs=self.jobs)

    def test_gets_keyed_engine_states(self):
        self._update()
        self.assertEqual(self.jobman._get_keyed_engine_metas.call_args,
                         call(jobs=self.jobs))
        expected_keyed_engine_metas = \
                self.jobman._get_keyed_engine_metas.return_value
        self.assertEqual(self.jobman.engine.get_keyed_engine_states.call_args,
                         call(keyed_engine_metas=expected_keyed_engine_metas))

    def test_sets_job_engine_states(self):
        self._update()
        keyed_engine_states = \
                self.jobman.engine.get_keyed_engine_states.return_value
        expected_call_args_list = [
            call(
                job=job,
                job_engine_state=keyed_engine_states.get(job['job_key'])
            ) for job in self.jobs
        ]
        self.assertEqual(self.jobman._set_job_engine_state.call_args_list,
                         expected_call_args_list)

    def test_saves_jobs(self):
        self._update()
        self.assertEqual(self.jobman.dao.save_jobs.call_args,
                         call(jobs=self.jobs))

    @patch.object(_jobman, 'time')
    def test_updates_jobs_modified_time(self, mock_time):
        self._update()
        self.assertEqual(
            self.jobman.set_kvp.call_args,
            call(key='job_engine_states_modified', value=mock_time.time()))

class _SetJobEngineState(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_jobman_attrs(attrs=['_get_job_age', '_job_is_orphaned'])
        self.job = defaultdict(MagicMock)
        self.job_engine_state = MagicMock()

    def _set(self):
        self.jobman._set_job_engine_state(
            job=self.job, job_engine_state=self.job_engine_state)

    def test_sets_engine_state_key(self):
        self._set()
        self.assertEqual(self.job['engine_state'], self.job_engine_state)

    def test_updates_job_status_for_non_null_engine_state(self):
        self._set()
        self.assertEqual(self.job['status'],
                         self.job_engine_state.get('status'))

    def test_state_marks_orphaned_job_as_executed(self):
        self.job_engine_state = None
        self.jobman._job_is_orphaned.return_value = True
        self._set()
        self.assertEqual(self.job['status'], 'EXECUTED')

    def test_for_null_engine_state_ignores_non_orphaned_jobs(self):
        self.job_engine_state = None
        self.jobman._job_is_orphaned.return_value = False
        orig_status = self.job['status']
        self._set()
        self.assertEqual(self.job['status'], orig_status)

class _JobIsOrphanedTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_jobman_attrs(attrs=['_get_job_age'])
        self.jobman.submission_grace_period = 999
        self.job = MagicMock()

    def test_returns_false_if_within_submission_grace_period(self):
        self.jobman._get_job_age.return_value = \
                self.jobman.submission_grace_period - 1
        self.assertEqual(self.jobman._job_is_orphaned(job=self.job), False)

    def test_returns_true_if_exceeds_submission_grace_period(self):
        self.jobman._get_job_age.return_value = \
                self.jobman.submission_grace_period + 1
        self.assertEqual(self.jobman._job_is_orphaned(job=self.job), True)

class GetJobAge(BaseTestCase):
    @patch.object(_jobman, 'time')
    def test_returns_delta_for_created_time(self, mock_time):
        job = MagicMock()
        self.assertEqual(self.jobman._get_job_age(job=job),
                         (mock_time.time() - job['created']))

class SetKvpTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.key = MagicMock()
        self.value = MagicMock()
        self._set_kvp()

    def _set_kvp(self):
        self.jobman.set_kvp(key=self.key, value=self.value)

    def test_saves_kvp_via_dao(self):
        self.assertEqual(self.jobman.dao.save_kvps.call_args,
                         call(kvps=[{'key': self.key, 'value': self.value}]))

    def test_saves_to_local_kvp(self):
        self.assertEqual(self.jobman._kvps[self.key], self.value)

class FromCfgTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.setup_mocks()
        self.cfg = MagicMock()
        self.result = _jobman.JobMan.from_cfg(cfg=self.cfg)

    def setup_mocks(self):
        patchers = {'JobMan.__init__': patch.object(_jobman.JobMan, '__init__'),
                    'getattr': patch.object(_jobman, 'getattr')}
        self.mocks = {}
        for key, patcher in patchers.items():
            self.addCleanup(patcher.stop)
            self.mocks[key] = patcher.start()
        self.mocks['JobMan.__init__'].return_value = None

    def test_gets_expected_attrs(self):
        expected_call_args_list = [call(self.cfg, attr)
                                   for attr in _jobman.JobMan.CFG_PARAMS]

        self.assertEqual(self.mocks['getattr'].call_args_list,
                         expected_call_args_list)

    def test_creates_jobman_with_kwargs_from_cfg(self):
        expected_kwargs = {attr: self.mocks['getattr'].return_value
                           for attr in _jobman.JobMan.CFG_PARAMS}
        self.assertEqual(self.mocks['JobMan.__init__'].call_args,
                         call(**expected_kwargs))

    def test_returns_jobman(self):
        self.assertTrue(isinstance(self.result, _jobman.JobMan))

if __name__ == '__main__': unittest.main()
