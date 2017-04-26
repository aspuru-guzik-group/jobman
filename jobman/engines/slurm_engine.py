import logging
import os
import re
import subprocess

from . import base_engine


SLURM_JOB_STATES_TO_RUN_STATES = {
    'RUNNING': set(['CONFIGURING', 'COMPLETING', 'PENDING', 'RUNNING']),
    'COMPLETED': set(['COMPLETED']),
    'FAILED': set(['BOOT_FAIL', 'CANCELLED', 'FAILED', 'NODE_FAIL', 'PREEMPTED',
                   'TIMEOUT'])
}

class SlurmEngine(base_engine.BaseEngine):
    DEFAULT_ENTRYPOINT_NAME ='job.sh'

    class SubmissionError(Exception): pass

    def __init__(self, process_runner=None, logger=None):
        self.process_runner = process_runner or subprocess.run
        self.logger = logger or logging

    def submit(self, submission=None):
        workdir = submission['dir']
        entrypoint_name = submission.get('entrypoint',
                                         self.DEFAULT_ENTRYPOINT_NAME)
        entrypoint_path = os.path.join(workdir, entrypoint_name)
        cmd = ['sbatch', '--workdir="%s"' % workdir, entrypoint_path]
        try:
            completed_proc = self.process_runner.run_process(
                cmd=cmd, check=True)
        except self.process_runner.CalledProcessError as called_proc_err:
            error_msg = ("Submission error:\n"
                         "\tstdout: {stdout}\n"
                         "\tstderr: {stderr}\n").format(
                             stdout=called_proc_err.stdout,
                             stderr=called_proc_err.stderr)
            raise self.SubmissionError(error_msg) from called_proc_err
        slurm_job_id = self.parse_sbatch_output(completed_proc.stdout)
        engine_meta = {'job_id': slurm_job_id}
        return engine_meta

    def parse_sbatch_output(self, sbatch_output=None):
        match = re.match(r'Submitted batch job (\d+)', sbatch_output)
        if not match:
            raise Exception("Could not parse slurm job id."
                            " sbatch output was: '%s'" % sbatch_output)
        slurm_job_id = match.group(1)
        return slurm_job_id

    def get_keyed_job_states(self, keyed_engine_metas=None):
        job_id = engine_meta['job_id']
        cmd = ['scontrol', 'show', '--details', '--oneliner', 'job', job_id]
        completed_proc = self.process_runner.run_process(cmd=cmd, check=True)
        slurm_job_meta = self.parse_scontrol_output(completed_proc.stdout)
        run_status = self.get_run_status_from_slurm_job_meta(slurm_job_meta)
        execution_state = {
            'run_status': run_status,
            'slurm_job_meta': slurm_job_meta,
        }
        return execution_state

    def parse_scontrol_output(self, scontrol_output=None):
        parsed = {}
        for key_value_str in scontrol_output.split():
            key, value = key_value_str.split('=', 1)
            parsed[key] = value
        return parsed

    def get_run_status_from_slurm_job_meta(self, slurm_job_meta=None):
        job_state = slurm_job_meta['JobState']
        for run_status, job_states in SLURM_JOB_STATES_TO_RUN_STATES.items():
            if job_state in job_states: return run_status
        return 'UNKNOWN'
