import os
import unittest
from unittest.mock import call, MagicMock

from .. import slurm_engine


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self.process_runner = MagicMock()
        self.engine = self.generate_engine()

    def generate_engine(self, **kwargs):
        default_kwargs = {'process_runner': self.process_runner}
        return slurm_engine.SlurmEngine(**{**default_kwargs, **kwargs})

    def mockify_engine_attrs(self, attrs=None):
        for attr in attrs: setattr(self.engine, attr, MagicMock())

class SubmitTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.submission = MagicMock()

    def _submit(self):
        return self.engine.submit(submission=self.submission)

    def test_calls_sbatch(self):
        self.process_runner.run_process.return_value = \
                self.generate_successful_sbatch_proc()
        self._submit()
        workdir = self.submission['dir']
        entrypoint_path = os.path.join(workdir,
                                       self.submission.get('entrypoint'))
        expected_cmd = ['sbatch', '--workdir="%s"' % workdir, entrypoint_path]
        self.assertEqual(self.process_runner.run_process.call_args,
                         call(cmd=expected_cmd, check=True))

    def generate_successful_sbatch_proc(self, job_id='12345'):
        proc = MagicMock()
        proc.returncode = 0
        proc.stdout = 'Submitted batch job %s' % job_id
        return proc

    def test_returns_engine_meta_for_successful_submission(self):
        job_id = '12345'
        self.process_runner.run_process.return_value = \
                self.generate_successful_sbatch_proc(job_id=job_id)
        engine_meta = self._submit()
        expected_engine_meta = {'job_id': job_id}
        self.assertEqual(engine_meta, expected_engine_meta)

    def test_handles_failed_submission(self):
        self.process_runner.CalledProcessError = \
                slurm_engine.subprocess.CalledProcessError
        def simulate_failed_proc(cmd, *args, **kwargs):
            proc = MagicMock()
            proc.returncode = 1
            proc.stderr = 'some error'
            raise self.process_runner.CalledProcessError(
                proc.returncode, cmd)
        self.process_runner.run_process.side_effect = simulate_failed_proc
        with self.assertRaises(self.engine.SubmissionError):
            self._submit()

class GetKeyedJobStatesTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_engine_attrs(attrs=['get_slurm_jobs',
                                         'slurm_job_to_job_state'])
        self.keyed_engine_metas = {i: MagicMock() for i in range(3)}
        self.expected_job_ids = [
            engine_meta['job_id']
            for engine_meta in self.keyed_engine_metas.values()
        ]
        self.mock_slurm_jobs = {
            job_id: MagicMock() for job_id in self.expected_job_ids
        }
        self.engine.get_slurm_jobs.return_value = self.mock_slurm_jobs

    def _get(self):
        return self.engine.get_keyed_job_states(
            keyed_engine_metas=self.keyed_engine_metas)

    def test_gets_slurm_jobs(self):
        self._get()
        self.assertEqual(self.engine.get_slurm_jobs.call_args,
                         call(job_ids=self.expected_job_ids))

    def test_gets_job_states(self):
        self._get()
        self.assertEqual(
            self._get_sorted_slurm_job_calls(
                calls=self.engine.slurm_job_to_job_state.call_args_list),
            self._get_sorted_slurm_job_calls(
                calls=[call(slurm_job=slurm_job)
                       for slurm_job in self.mock_slurm_jobs.values()])
        )

    def _get_sorted_slurm_job_calls(self, calls=None):
        def _key_fn(call):
            if len(call) == 3: return id(call[2]['slurm_job'])
            elif len(call) == 2: return id(call[1]['slurm_job'])
        return sorted(calls, key=_key_fn)

    def test_returns_keyed_job_states(self):
        result = self._get()
        expected_result = {
            key: self.engine.slurm_job_to_job_state.return_value
            for key in self.keyed_engine_metas
        }
        self.assertEqual(result, expected_result)

class GetSlurmJobsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.job_ids = ["job_id_%s" % i for i in range(3)]
        self.mock_slurm_jobs = [MagicMock() for job_id in self.job_ids]
        self.mockify_engine_attrs(attrs=['parse_sacct_stdout'])
        self.engine.parse_sacct_stdout.return_value = self.mock_slurm_jobs

    def _get(self):
        return self.engine.get_slurm_jobs(job_ids=self.job_ids)

    def test_makes_expected_process_call(self):
        self._get()
        csv_job_ids = ",".join(self.job_ids)
        expected_cmd = ['sacct', '--jobs=%s' % csv_job_ids, '--long',
                        '--noconvert', 'parsable2', '--allocations']
        self.assertEqual(self.engine.process_runner.run_process.call_args,
                         call(cmd=expected_cmd, check=True))

    def test_returns_keyed_parsed_jobs(self):
        result = self._get()
        expected_result = {slurm_job['JobID']: slurm_job
                           for slurm_job in self.mock_slurm_jobs}
        self.assertEqual(result, expected_result)

class ParseSacctOutputTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.fields = ["field_%s" for i in range(5)]
        self.records = [
            ["record_%s__%s_value" % (i, field) for field in self.fields]
            for i in range(3)
        ]
        self.sacct_stdout = "\n".join([
            "|".join(self.fields),
            *["|".join(record) for record in self.records]
        ])

    def test_returns_fields_and_records(self):
        result = self.engine.parse_sacct_stdout(sacct_stdout=self.sacct_stdout)
        expected_result = {
            'fields': self.fields,
            'records': [
                {field: record[i] for i, field in enumerate(self.fields)}
                for record in self.records
            ]
        }
        self.assertEqual(result, expected_result)

class SlurmJobToJobStateTestCase(BaseTestCase):
    def test_generates_expected_job_state_for_non_null_slurm_job(self):
        self.engine.slurm_job_to_status = MagicMock()
        slurm_job = MagicMock()
        result = self.engine.slurm_job_to_job_state(slurm_job=slurm_job)
        expected_result = {
            'engine_job_state': slurm_job,
            'status': self.engine.slurm_job_to_status.return_value
        }
        self.assertEqual(result, expected_result)

    def test_generates_expectedd_job_state_for_null_slurm_job(self):
        result = self.engine.slurm_job_to_job_state(slurm_job=None)
        expected_result = {'engine_job_state': None}
        self.assertEqual(result, expected_result)

class SlurmJobToStatusTestCase(BaseTestCase):
    def test_handles_known_statuses(self):
        slurm_jobs = {}
        expected_mappings = {}
        for engine_job_status, slurm_states \
                in self.engine.SLURM_STATES_TO_ENGINE_JOB_STATUSES.items():
            for slurm_state in slurm_states:
                expected_mappings[slurm_state] = engine_job_status
                slurm_jobs[slurm_state] = {'JobState': slurm_state}
        actual_mappings = {
            slurm_state: self.engine.slurm_job_to_status(slurm_job=slurm_job)
            for slurm_state, slurm_job in slurm_jobs.items()
        }
        self.assertEqual(expected_mappings, actual_mappings)

    def test_handles_unknown_status(self):
        slurm_job = {'JobState': 'some_crazy_JobState'}
        self.assertEqual(self.engine.slurm_job_to_status(slurm_job=slurm_job),
                         self.engine.JOB_STATUSES.UNKNOWN)

if __name__ == '__main__':
    unittest.main()
