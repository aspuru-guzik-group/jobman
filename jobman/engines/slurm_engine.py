import logging
import os
import re
import subprocess
import types

from .base_engine import BaseEngine



class SlurmEngine(BaseEngine):
    DEFAULT_ENTRYPOINT_NAME ='job.sh'
    SLURM_STATES_TO_ENGINE_JOB_STATUSES = {
        BaseEngine.JOB_STATUSES.RUNNING: set(['CONFIGURING', 'COMPLETING',
                                              'PENDING', 'RUNNING']),
        BaseEngine.JOB_STATUSES.COMPLETED: set(['COMPLETED']),
        BaseEngine.JOB_STATUSES.FAILED: set(['BOOT_FAIL', 'CANCELLED', 'FAILED',
                                             'NODE_FAIL', 'PREEMPTED',
                                             'TIMEOUT'])
    }

    def __init__(self, process_runner=None, logger=None):
        self.process_runner = process_runner or \
                self._generate_default_process_runner()
        self.logger = logger or logging

    def _generate_default_process_runner(self):
        process_runner = types.SimpleNamespace()
        def run_process(cmd=None, **kwargs):
            return subprocess.run(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                universal_newlines=True, **kwargs)
        process_runner.run_process = run_process
        process_runner.CalledProcessError = subprocess.CalledProcessError
        return process_runner

    def submit(self, submission=None):
        workdir = submission['dir']
        entrypoint_name = submission.get('entrypoint',
                                         self.DEFAULT_ENTRYPOINT_NAME)
        entrypoint_path = os.path.join(workdir, entrypoint_name)
        cmd = ['sbatch', '--workdir="%s"' % workdir, entrypoint_path]
        try:
            completed_proc = self.process_runner.run_process(
                cmd=cmd, check=True)
            slurm_job_id = self.parse_sbatch_stdout(completed_proc.stdout)
            engine_meta = {'job_id': slurm_job_id}
            return engine_meta
        except self.process_runner.CalledProcessError as called_proc_err:
            error_msg = ("Submission error:\n"
                         "\tstdout: {stdout}\n"
                         "\tstderr: {stderr}\n").format(
                             stdout=called_proc_err.stdout,
                             stderr=called_proc_err.stderr)
            raise self.SubmissionError(error_msg) from called_proc_err

    def parse_sbatch_stdout(self, sbatch_stdout=None):
        match = re.match(r'Submitted batch job (\d+)', sbatch_stdout)
        if not match:
            raise Exception("Could not parse slurm job id."
                            " sbatch stdout was: '%s'" % sbatch_stdout)
        slurm_job_id = match.group(1)
        return slurm_job_id

    def get_keyed_job_states(self, keyed_engine_metas=None):
        keyed_job_ids = {key: engine_meta['job_id']
                         for key, engine_meta in keyed_engine_metas.items()}
        job_ids = list(keyed_job_ids.values())
        slurm_jobs = self.get_slurm_jobs(job_ids=job_ids)
        keyed_job_states = {}
        for key, job_id in keyed_job_ids.items():
            slurm_job = slurm_jobs.get(job_id)
            job_state = self.slurm_job_to_job_state(slurm_job=slurm_job)
            keyed_job_states[key] = job_state
        return keyed_job_states

    def get_slurm_jobs(self, job_ids=None):
        csv_job_ids = ','.join(list(job_ids))
        cmd = ['sacct', '--jobs=%s' % csv_job_ids, '--long', '--noconvert',
               'parsable2', '--allocations']
        completed_proc = self.process_runner.run_process(cmd=cmd, check=True)
        slurm_jobs = self.parse_sacct_stdout(sacct_stdout=completed_proc.stdout)
        slurm_jobs_by_id = {slurm_job['JobID']: slurm_job
                            for slurm_job in slurm_jobs}
        return slurm_jobs_by_id

    def parse_sacct_stdout(self, sacct_stdout=None):
        sacct_lines = sacct_stdout.split("\n")
        fields = self.split_sacct_line(sacct_line=sacct_lines[0])
        records = [
            self.parse_sacct_line(sacct_line=sacct_line, fields=fields)
            for sacct_line in sacct_lines[1:]
        ]
        return {'fields': fields, 'records': records}

    def split_sacct_line(self, sacct_line=None):
        return sacct_line.split('|')

    def parse_sacct_line(self, sacct_line=None, fields=None):
        parsed = {}
        for i, value in enumerate(self.split_sacct_line(sacct_line=sacct_line)):
            parsed[fields[i]] = value
        return parsed

    def slurm_job_to_job_state(self, slurm_job=None):
        job_state = {'engine_job_state': slurm_job}
        if slurm_job is not None:
            job_state['status'] = self.slurm_job_to_status(slurm_job=slurm_job)
        return job_state

    def slurm_job_to_status(self, slurm_job=None):
        for engine_job_status, slurm_states \
                in self.SLURM_STATES_TO_ENGINE_JOB_STATUSES.items():
            if slurm_job['JobState'] in slurm_states:
                return engine_job_status
        return self.JOB_STATUSES.UNKNOWN
