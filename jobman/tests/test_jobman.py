from collections import defaultdict
import unittest
from unittest.mock import call, MagicMock, patch


from .. import jobman as _jobman

class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self.engine = MagicMock()
        self.db = MagicMock()
        self.jobman = self.generate_jobman()

    def generate_jobman(self, **kwargs):
        default_kwargs = {
            'db': self.db,
            'engine': self.engine
        }
        jobman = _jobman.JobMan(**{**default_kwargs, **kwargs})
        return jobman

    def mock_jobman_attrs(self, attrs=None):
        for attr in attrs: setattr(self.jobman, attr, MagicMock())

class SubmitTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.submission = MagicMock()
        self.mock_jobman_attrs(attrs=['log_submission',
                                               'create_job_record'])

    def _submit(self):
        return self.jobman.submit_job(submission=self.submission)

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

    def test_creates_job_record_w_submission_meta(self):
        self._submit()
        self.assertEqual(
            self.jobman.create_job_record.call_args,
            call(
                job_kwargs={
                    'submission_meta': self.engine.submit.return_value
                }
            )
        )

    def test_returns_job_record(self):
        result = self._submit()
        self.assertEqual(result, self.jobman.create_job_record.return_value)

class CreateJobRecordTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.job_kwargs = MagicMock()

    def _create(self):
        return self.jobman.create_job_record(job_kwargs=self.job_kwargs)

    def test_dispatches_to_db(self):
        self._create()
        self.assertEqual(self.jobman.db.create_job_record.call_args,
                         call(job_kwargs=self.job_kwargs))

class UpdateJobRecordsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mock_jobman_attrs(attrs=['job_records_are_stale',
                                      '_update_job_records'])

    def test_does_not_call__update_job_records_if_not_stale(self):
        self.jobman.job_records_are_stale.return_value = False
        self.jobman.update_job_records()
        self.assertEqual(len(self.jobman._update_job_records.call_args_list), 0)

    def test_calls__update_job_records_if_stale(self):
        self.jobman.job_records_are_stale.return_value = True
        self.jobman.update_job_records()
        self.assertEqual(len(self.jobman._update_job_records.call_args_list), 1)

class JobRecordsAreStaleTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.jobman.job_records_ttl = 999
        self.mock_jobman_attrs(attrs=['get_job_records_age'])

    def test_returns_true_if_job_records_updated_timestamp_too_old(self):
        self.jobman.get_job_records_age.return_value = \
                self.jobman.job_records_ttl + 1
        self.assertEqual(self.jobman.job_records_are_stale(), True)

    def test_returns_true_if_job_records_updated_timestamp_is_none(self):
        self.jobman.get_job_records_age.return_value = None
        self.assertEqual(self.jobman.job_records_are_stale(), True)

    def test_returns_false_if_job_records_updated_timestamp_not_too_old(self):
        self.jobman.get_job_records_age.return_value = \
                self.jobman.job_records_ttl - 1
        self.assertEqual(self.jobman.job_records_are_stale(), False)

class GetJobRecordsAge(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mock_jobman_attrs(attrs=['get_job_records_modified_time'])
        self.jobman.get_job_records_modified_time.return_value = 999

    @patch.object(_jobman, 'time')
    def test_returns_age(self, mock_time):
        mock_time.time.return_value = 123
        expected_age = \
                mock_time.time() - self.jobman.get_job_records_modified_time()
        self.assertEqual(self.jobman.get_job_records_age(), expected_age)

class GetJobRecordsModifiedTime(BaseTestCase):
    def test_returns_from_attr_if_set(self):
        self.jobman._job_records_modified_time = MagicMock()
        self.assertEqual(self.jobman.get_job_records_modified_time(),
                         self.jobman._job_records_modified_time)

    def test_returns_from_db_and_sets_attr_if_attr_not_set(self):
        self.assertEqual(self.jobman.get_job_records_modified_time(),
                         self.jobman.db.get_job_records_modified_time())

class _UpdateJobRecordsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mock_jobman_attrs(attrs=[
            'update_job_record_from_engine_job_states',
            'set_job_records_modified_time'])
        self.job_records = [MagicMock() for i in range(3)]
        self.jobman.db.get_running_job_records.return_value = self.job_records

    def test_gets_running_job_records(self):
        self.jobman._update_job_records()
        self.assertEqual(self.jobman.db.get_running_job_records.call_args,
                         call())

    def test_get_engine_job_states(self):
        self.jobman._update_job_records()
        self.assertEqual(self.jobman.engine.get_job_states.call_args,
            call(job_records=self.jobman.db.get_running_job_records.return_value))

    def test_updates_running_jobs_from_engine_job_states(self):
        self.jobman._update_job_records()
        self.assertEqual(
            self.jobman.update_job_record_from_engine_job_states.call_args_list,
            [
                call(job_record=jrec,
                     engine_job_states=(
                         self.jobman.engine.get_job_states.return_value)
                    )
                for jrec in self.jobman.db.get_running_job_records.return_value
            ]
        )

    @patch.object(_jobman, 'time')
    def test_updates_job_records_modified_time(self, mock_time):
        self.jobman._update_job_records()
        self.assertEqual(self.jobman.set_job_records_modified_time.call_args,
                         call(value=mock_time.time()))

class GetJobRecord(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mock_jobman_attrs(attrs=['update_job_records'])

    def test_dispatches_to_db(self):
        job_key = MagicMock()
        job_record = self.jobman.get_job_record(job_key=job_key)
        self.assertEqual(self.jobman.db.get_job_record.call_args,
                         call(job_key=job_key))
        self.assertEqual(job_record, self.jobman.db.get_job_record.return_value)

class UpdateJobRecordFromEngineJobStates(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mock_jobman_attrs(attrs=[
            'update_job_record_from_engine_job_state',
            'process_orphan_job_record'])
        self.job_key = MagicMock()
        self.job_record = defaultdict(MagicMock, **{'job_key': self.job_key})
        self.engine_job_states = defaultdict(MagicMock)
        self.engine_job_state = MagicMock()

    def _update(self):
        self.jobman.update_job_record_from_engine_job_states(
            job_record=self.job_record,
            engine_job_states=self.engine_job_states)

    def test_handles_non_orphan_job(self):
        self.engine_job_states[self.job_key] = self.engine_job_state
        self._update()
        self.job_record['status'] = self.engine_job_state['status']
        self.job_record['engine_job_state'] = self.engine_job_state
        self.assertEqual(self.jobman.db.save_job_record.call_args,
                         call(job_record=self.job_record))

    def test_handles_orphan_job_record(self):
        self._update()
        self.assertEqual(self.jobman.process_orphan_job_record.call_args,
                         call(job_record=self.job_record))

class ProcessOrphanJobRecord(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mock_jobman_attrs(attrs=['get_job_record_age'])
        self.jobman.submission_grace_period = 999
        self.job_record = defaultdict(MagicMock)

    def _process(self):
        self.jobman.process_orphan_job_record(job_record=self.job_record)

    def test_does_not_complete_if_age_within_submission_grace_period(self):
        self.jobman.get_job_record_age.return_value = \
                self.jobman.submission_grace_period - 1
        orig_status = self.job_record['status']
        self._process()
        self.assertEqual(self.job_record['status'], orig_status)
        self.assertEqual(self.jobman.db.save_job_record.call_args, None)

    def test_completes_if_age_exceeds_submission_grace_period(self):
        self.jobman.get_job_record_age.return_value = \
                self.jobman.submission_grace_period + 1
        self._process()
        self.assertEqual(self.job_record['status'], 'COMPLETED')
        self.assertEqual(self.jobman.db.save_job_record.call_args,
                         call(job_record=self.job_record))

class GetJobRecordAge(BaseTestCase):
    @patch.object(_jobman, 'time')
    def test_returns_delta_for_created_time(self, mock_time):
        job_record = MagicMock()
        self.assertEqual(self.jobman.get_job_record_age(job_record=job_record),
                         (mock_time.time() - job_record['created']))

if __name__ == '__main__': unittest.main()
