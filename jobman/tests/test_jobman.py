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

class SubmitTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.submission = MagicMock()
        self.source = MagicMock()
        self.source_meta = MagicMock()
        self.mockify_jobman_attrs(attrs=['_submission_to_job',
                                         '_job_is_batchable', '_create_job'])
        self.jobman._submission_to_job.return_value = defaultdict(MagicMock)
        self.expected_job = self.jobman._submission_to_job.return_value
        self.result = self.jobman.submit(submission=self.submission,
                                         source=self.source,
                                         source_meta=self.source_meta)

    def test_assembles_job_from_submission(self):
        self.assertEqual(
            self.jobman._submission_to_job.call_args,
            call(submission=self.submission, source=self.source,
                 source_meta=self.source_meta)
        )

    def test_marks_job_as_batchable(self):
        self.jobman._job_is_batchable.call_args(job=self.expected_job)
        self.assertEqual(self.expected_job['batchable'],
                         self.jobman._job_is_batchable.return_value)

    def test_creates_job(self):
        self.assertEqual(self.jobman._create_job.call_args,
                         call(job=self.expected_job))

    def test_returns_job_key(self):
        self.assertEqual(self.result,
                         self.jobman._create_job.return_value['key'])

class _SubmissionToJobTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.submission = MagicMock()
        self.source = MagicMock()
        self.source_meta = MagicMock()
        self.result = self.jobman._submission_to_job(
            submission=self.submission, source=self.source,
            source_meta=self.source_meta
        )

    def test_returns_expected_job(self):
        expected_result = {
            'submission': self.submission,
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
            'get_jobs_for_status',
            '_update_job_engine_states',
            '_process_executed_jobs',
            '_process_batchable_jobs',
            '_process_submittable_jobs',
        ])

    def _tick(self): self.jobman.tick()

    def test_updates_engine_states_if_stale(self):
        self.jobman._job_engine_states_are_stale.return_value = True
        self._tick()
        self.assertEqual(self.jobman.get_jobs_for_status.call_args,
                         call(status='RUNNING'))
        self.assertEqual(self.jobman._update_job_engine_states.call_args,
                         call(jobs=self.jobman.get_running_jobs.return_value))

    def test_does_not_update_engine_states_if_not_stale(self):
        self.jobman._job_engine_states_are_stale.return_value = False
        self._tick()
        self.assertEqual(self.jobman.get_jobs_for_status.call_args, None)
        self.assertEqual(self.jobman._update_job_engine_states.call_args, None)

    def test_processes_executed_jobs(self):
        self._tick()
        self.assertEqual(self.jobman._process_executed_jobs.call_args,
                         call())

    def test_processes_batchable_jobs(self):
        self._tick()
        self.assertEqual(self.jobman._process_batchable_jobs.call_args,
                         call())

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
        self.mockify_jobman_attrs(attrs=['_get_batch_job_subjobs',
                                         '_process_executed_job',
                                         '_complete_job'])
        self.executed_job = defaultdict(MagicMock)
        self.jobman._process_executed_batch_job(executed_job=self.executed_job)

    def test_gets_subjobs(self):
        self.assertEqual(self.jobman._get_batch_job_subjobs.call_args,
                         call(batch_job=self.executed_job))

    def test_processes_subjobs(self):
        self.assertEqual(
            self.jobman._process_executed_job.call_args_list,
            [call(executed_job=subjob)
             for subjob in self.jobman._get_batch_job_subjobs.return_value]
        )

    def test_completes_batch_job(self):
        self.assertEqual(self.jobman._complete_job.call_args,
                         call(job=self.executed_job))

class _GetBatchJobSubjobsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.batch_job = defaultdict(MagicMock)
        self.batch_job['batch_meta'] = {
            'subjob_keys': [MagicMock() for i in range(3)]
        }
        self.result = self._jobman._get_batch_job_subjobs(
            batch_job=self.batch_job)

    def test_queries_for_subjob_keys_in_batch_meta(self):
        self.assertEqual(
            self.jobman.dao.query.call_args,
            call(query={
                'filters': [
                    {'field': 'key', 'operator': 'IN',
                     'value': self.batch_job['batch_meta']['subjob_keys']}
                ]
            })
        )
        self.assertEqual(self.result, self.jobman.dao.query.return_value)

class _CompleteJobTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.job = defaultdict(MagicMock)
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

class _ProcessBatchableJobsTestCase(BaseTestCase):
    def test_something(self):
        self.fail()

class _ProcessSubmittableJobsTestCase(BaseTestCase):
    def test_something(self):
        self.fail()

class _SubmitJobTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.submission = MagicMock()
        self.source = MagicMock()
        self.source_meta = MagicMock()
        self.mockify_jobman_attrs(attrs=['create_job'])

    def _submit_job(self):
        return self.jobman._submit_job(submission=self.submission,
                                       source=self.source,
                                       source_meta=self.source_meta)

    def test_submits_via_engine(self):
        self._submit_job()
        self.assertEqual(self.engine.submit.call_args,
                         call(submission=self.submission))

    def test_raises_exception_for_bad_submission(self):
        exception = Exception("bad submission")
        self.engine.submit.side_effect = exception
        with self.assertRaises(self.jobman.SubmissionError): self._submit_job()

    def test_creates_job_w_metas(self):
        self._submit_job()
        self.assertEqual(
            self.jobman.create_job.call_args,
            call(
                job_kwargs={
                    'submission': self.submission,
                    'source': self.source,
                    'source_meta': self.source_meta,
                    'engine_meta': self.engine.submit.return_value,
                    'status': 'RUNNING',
                }
            )
        )

    def test_returns_job(self):
        result = self._submit()
        self.assertEqual(result, self.jobman.create_job.return_value)

class CreateJobTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.job_kwargs = MagicMock()

    def _create(self):
        return self.jobman.create_job(job_kwargs=self.job_kwargs)

    def test_dispatches_to_dao(self):
        self._create()
        self.assertEqual(self.jobman.dao.create_job.call_args,
                         call(job_kwargs=self.job_kwargs))

class GetJobsTestCase(BaseTestCase):
    def test_dispatches_to_dao(self):
        query = MagicMock()
        result = self.jobman.get_jobs(query=query)
        self.assertEqual(
            self.jobman.dao.get_jobs.call_args,
            call(query=query)
        )
        self.assertEqual(result, self.jobman.dao.get_jobs.return_value)

class NumFreeSlotsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.jobman.max_running_jobs = 3
        self.jobman.get_running_jobs = MagicMock(return_value=[1,2])

    def test_returns_max_running_jobs_less_running_jobs(self):
        self.assertEqual(
            self.jobman.num_free_slots,
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
                    {'field': 'key', 'operator': '=', 'value': self.key}
                ]
            })
        )
        self.assertEqual(result, expected_result)
        self.assertEqual(self.jobman._kvps[self.key], expected_result)

class GetRunningJobsTestCase(BaseTestCase):
    def test_dispatches_to_dao(self):
        result = self.jobman.get_running_jobs()
        self.assertEqual(
            self.dao.get_jobs.call_args,
            call(query={
                'filters': [
                    {'field': 'status', 'operator': '=', 'value': 'RUNNING'}
                ]
            })
        )
        self.assertEqual(result, self.dao.get_jobs.return_value)

class UpdateJobEngineStatesTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.jobs = MagicMock()
        self.mockify_jobman_attrs(attrs=['get_jobs',
                                         'job_engine_states_are_stale',
                                         '_update_job_engine_states',
                                         'filter_for_incomplete_status'])

    def test_gets_jobs_if_passed_query(self):
        query = MagicMock()
        self.jobman.update_job_engine_states(query=query)
        self.assertEqual(self.jobman.get_jobs.call_args, call(query=query))
        self.assertEqual(self.jobman.filter_for_incomplete_status.call_args,
                         call(items=self.jobman.get_jobs.return_value))
        self.assertEqual(
            self.jobman._update_job_engine_states.call_args,
            call(jobs=self.jobman.filter_for_incomplete_status.return_value)
        )

    def test_noop_if_jobs_are_empty(self):
        self.jobman.update_job_engine_states(jobs=None)
        self.assertEqual(self.jobman._update_job_engine_states.call_args, None)

    def test_noop_if_jobs_and_query_result_are_empty(self):
        self.jobman.filter_for_incomplete_status.return_value = None
        self.jobman.update_job_engine_states(jobs=None, query=MagicMock())
        self.assertEqual(self.jobman._update_job_engine_states.call_args, None)

    def test_noop_if_not_force_and_not_stale(self):
        self.jobman.job_engine_states_are_stale.return_value = False
        self.jobman.update_job_engine_states(jobs=self.jobs)
        self.assertEqual(self.jobman._update_job_engine_states.call_args, None)

    def test_updates_if_force_and_not_stale(self):
        self.jobman.job_engine_states_are_stale.return_value = False
        self.jobman.update_job_engine_states(jobs=self.jobs, force=True)
        self.assertEqual(self.jobman._update_job_engine_states.call_args,
                         call(jobs=self.jobs))

class JobsEngineStatesAreStaleTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.jobman.jobs_ttl = 999
        self.mockify_jobman_attrs(attrs=['get_job_engine_states_age'])

    def test_returns_true_age_too_old(self):
        self.jobman.get_job_engine_states_age.return_value = \
                self.jobman.job_engine_states_ttl + 1
        self.assertEqual(self.jobman.job_engine_states_are_stale(), True)

    def test_returns_true_if_age_is_none(self):
        self.jobman.get_job_engine_states_age.return_value = None
        self.assertEqual(self.jobman.job_engine_states_are_stale(), True)

    def test_returns_false_if_jobs_updated_timestamp_not_too_old(self):
        self.jobman.get_job_engine_states_age.return_value = \
                self.jobman.job_engine_states_ttl - 1
        self.assertEqual(self.jobman.job_engine_states_are_stale(), False)

class GetJobEngineStatesAge(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_jobman_attrs(attrs=['get_kvp'])
        self.jobman.get_kvp.return_value = 999

    @patch.object(_jobman, 'time')
    def test_returns_age(self, mock_time):
        mock_time.time.return_value = 123
        age = self.jobman.get_job_engine_states_age()
        self.assertEqual(self.jobman.get_kvp.call_args,
                         call(key='job_engine_states_modified'))
        expected_age = mock_time.time() - \
                self.jobman.get_kvp.return_value
        self.assertEqual(age, expected_age)

class _UpdateJobEngineStatesTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.jobs = [MagicMock() for i in range(3)]
        self.mockify_jobman_attrs(attrs=['set_kvp', 'get_keyed_engine_metas',
                                         'set_job_engine_state'])

    def _update(self):
        self.jobman._update_job_engine_states(jobs=self.jobs)

    def test_gets_keyed_engine_states(self):
        self._update()
        self.assertEqual(self.jobman.get_keyed_engine_metas.call_args,
                         call(jobs=self.jobs))
        expected_keyed_engine_metas = \
                self.jobman.get_keyed_engine_metas.return_value
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
        self.assertEqual(self.jobman.set_job_engine_state.call_args_list,
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

class SetJobEngineState(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_jobman_attrs(attrs=['get_job_age', 'job_is_orphaned'])
        self.job = defaultdict(MagicMock)
        self.job_engine_state = MagicMock()

    def _set(self):
        self.jobman.set_job_engine_state(job=self.job,
                                         job_engine_state=self.job_engine_state)

    def test_sets_engine_state_key(self):
        self._set()
        self.assertEqual(self.job['engine_state'], self.job_engine_state)

    def test_updates_job_status_for_non_null_engine_state(self):
        self._set()
        self.assertEqual(self.job['status'],
                         self.job_engine_state.get('status'))

    def test_state_marks_orphaned_job_as_executed(self):
        self.job_engine_state = None
        self.jobman.job_is_orphaned.return_value = True
        self._set()
        self.assertEqual(self.job['status'], 'EXECUTED')

    def test_for_null_engine_state_ignores_non_orphaned_jobs(self):
        self.job_engine_state = None
        self.jobman.job_is_orphaned.return_value = False
        orig_status = self.job['status']
        self._set()
        self.assertEqual(self.job['status'], orig_status)

class JobIsOrphanedTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_jobman_attrs(attrs=['get_job_age'])
        self.jobman.submission_grace_period = 999
        self.job = MagicMock()

    def test_returns_false_if_within_submission_grace_period(self):
        self.jobman.get_job_age.return_value = \
                self.jobman.submission_grace_period - 1
        self.assertEqual(self.jobman.job_is_orphaned(job=self.job), False)

    def test_returns_true_if_exceeds_submission_grace_period(self):
        self.jobman.get_job_age.return_value = \
                self.jobman.submission_grace_period + 1
        self.assertEqual(self.jobman.job_is_orphaned(job=self.job), True)

class GetJobAge(BaseTestCase):
    @patch.object(_jobman, 'time')
    def test_returns_delta_for_created_time(self, mock_time):
        job = MagicMock()
        self.assertEqual(self.jobman.get_job_age(job=job),
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
        expected_call_args_list = [call(self.cfg, attr, None)
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
