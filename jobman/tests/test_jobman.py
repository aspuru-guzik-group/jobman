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
        self.mockify_jobman_attrs(attrs=['log_submission', 'create_job'])

    def _submit(self):
        return self.jobman.submit_job(submission=self.submission,
                                      source=self.source,
                                      source_meta=self.source_meta)

    def test_logs_submission(self):
        self._submit()
        self.assertTrue(self.jobman.log_submission.call_args,
                        call(submission=self.submission))

    def test_submits_via_engine(self):
        self._submit()
        self.assertEqual(self.engine.submit.call_args,
                         call(submission=self.submission))

    def test_raises_exception_for_bad_submission(self):
        exception = Exception("bad submission")
        self.engine.submit.side_effect = exception
        with self.assertRaises(self.jobman.SubmissionError): self._submit()

    def test_creates_job_w_metas(self):
        self._submit()
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

if __name__ == '__main__': unittest.main()
