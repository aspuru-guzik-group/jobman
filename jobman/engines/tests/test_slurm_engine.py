import os
import re
import textwrap
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

    def generate_failed_proc(self):
        proc = MagicMock()
        proc.returncode = 1
        proc.stderr = 'some error'
        return proc

class SubmitTestCase(BaseTestCase):
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
        failed_proc = self.generate_failed_proc()
        self.process_runner.run_cmd.return_value = failed_proc
        with self.assertRaises(self.engine.SubmissionError) as context:
            self._submit()
            self.assertContains(str(context.exception), failed_proc.stderr)

class GetKeyedJobStatesTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.keyed_engine_metas = {i: MagicMock() for i in range(3)}

    def _get_job_state(self):
        self.engine.get_keyed_job_state(
            keyed_engine_metas=self.keyed_engine_metas)

    def test_calls_scontrol(self):
        self.process_runner.run_process.return_value = \
                self.generate_scontrol_proc()
        self._get_job_state()
        expected_cmd = ['scontrol', 'show', '--details', '--oneliner',
                       'job', self.engine_meta['job_id']]
        self.assertEqual(self.process_runner.run_process.call_args,
                         call(cmd=expected_cmd, check=True))

    def generate_scontrol_proc(self, overrides=None):
        proc = MagicMock()
        proc.returncode = 0
        proc.stdout = textwrap.dedent(
            """
            JobId=75945797 JobName=foo.sh UserId=user(5900165)
            GroupId=rc_admin(40273) MCS_label=N/A Priority=19999669
            Nice=0 Account=rc_admin QOS=normal JobState=COMPLETED
            Reason=None Dependency=(null) Requeue=1 Restarts=0
            BatchFlag=1 Reboot=0 ExitCode=0:0 RunTime=00:00:01
            TimeLimit=00:10:00 TimeMin=N/A
            SubmitTime=2016-11-23T13:44:31
            EligibleTime=2016-11-23T13:44:31
            StartTime=2016-11-23T13:44:32 EndTime=2016-11-23T13:44:33
            Deadline=N/A PreemptTime=None SuspendTime=None
            SecsPreSuspend=0 Partition=serial_requeue
            AllocNode:Sid=rclogin03:9811 ReqNodeList=(null)
            ExcNodeList=(null) NodeList=holy2a16206
            BatchHost=holy2a16206 NumNodes=1 NumCPUs=1 NumTasks=0
            CPUs/Task=1 ReqB:S:C:T=0:0:*:* TRES=cpu=1,mem=100M,node=1
            Socks/Node=* NtasksPerN:B:S:C=0:0:*:* CoreSpec=*
            MinCPUsNode=1 MinMemoryCPU=100M MinTmpDiskNode=0
            Features=(null) Gres=(null) Reservation=(null)
            OverSubscribe=OK Contiguous=0 Licenses=(null) Network=(null)
            Command=/home/user/foo.sh WorkDir=/home/user/foo
            StdErr=/home/user/slurm-75945797.out StdIn=/dev/null
            StdOut=/home/user/slurm-75945797.out Power=
            """)
        for key, value in (overrides or {}).items():
            proc.stdout = re.sub(r'{key}=(.*?)\s'.format(key=key),
                                 r'{key}={value}'.format(key=key, value=value),
                                 proc.stdout)
        return proc

    def test_returns_expected_execution_state(self):
        scontrol_proc = self.generate_scontrol_proc()
        self.process_runner.run_process.return_value = scontrol_proc
        job_state = self._get_job_state()
        expected_slurm_meta = self.slurm_client.parse_scontrol_output(
            scontrol_proc.stdout)
        self.assertEqual(job_state['slurm_job_meta'], expected_slurm_meta)
        self.assertEqual(
            job_state['status'], 
            self.engine.get_status_for_slurm_job_meta(expected_slurm_meta))

if __name__ == '__main__':
    unittest.main()
