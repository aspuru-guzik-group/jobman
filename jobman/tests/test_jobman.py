import unittest
from unittest.mock import call, MagicMock


from .. import jobman as _jobman

class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self.engine = MagicMock()
        self.jobman = self.generate_jobman()

    def generate_jobman(self, **kwargs):
        default_kwargs = {
            'engine': self.engine
        }
        jobman = _jobman.JobMan(**{**default_kwargs, **kwargs})
        return jobman

    def mock_jobman_methods(self, method_names=None):
        for method_name in method_names:
            setattr(self.jobman, method_name, MagicMock())

class SubmitTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.submission = MagicMock()
        self.mock_jobman_methods(method_names=['log_submission',
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
    def test_job_record_has_uuid(self):
        self.fail()

    def test_job_record_has_created_timestamp(self):
        self.fail()

class CheckRunningJobs(BaseTestCase):
    def test_gets_queue_state(self):
        self.fail()

    def test_checks_for_running_jobs_in_queue_state(self):
        self.fail()

    def test_processes_jobs_which_have_entries_in_queue_state(self):
        self.fail()

    def test_processes_orphan_jobs(self):
        self.fail()

class GetQueueStateTestCase(BaseTestCase):
    def test_updates_queue_state_if_stale(self):
        self.fail()

class UpdateJobFromQueueState(BaseTestCase):
    def test_handles_completed_job(self):
        self.fail()

    def test_handles_running_job(self):
        self.fail()

class ProcessOrphanJob(BaseTestCase):
    def test_ignores_if_within_grace_period(self):
        self.fail()

    def test_completes_if_exceeded_grace_period(self):
        self.fail()

class GetJobStateTestCase(BaseTestCase):
    def test_gets_job_state(self):
        self.fail()

class CreateDbTestCase(BaseTestCase):
    def test_creates_db(self):
        self.fail()

if __name__ == '__main__': unittest.main()
