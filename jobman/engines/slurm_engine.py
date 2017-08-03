import os
import re

from .base_engine import BaseEngine


class SlurmEngine(BaseEngine):
    SLURM_STATES_TO_ENGINE_JOB_STATUSES = {
        BaseEngine.JOB_STATUSES.RUNNING: set([
            'CONFIGURING', 'COMPLETING', 'PENDING', 'RUNNING']),
        BaseEngine.JOB_STATUSES.EXECUTED: set(['COMPLETED']),
        BaseEngine.JOB_STATUSES.FAILED: set([
            'BOOT_FAIL', 'CANCELLED', 'FAILED', 'NODE_FAIL', 'PREEMPTED',
            'TIMEOUT'])
    }

    DEFAULT_SLURM_COMMANDS = {
        slurm_command: slurm_command
        for slurm_command in ['sacct', 'sbatch', 'scontrol']
    }

    def __init__(self, *args, slurm_commands=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.slurm_commands = slurm_commands or self.DEFAULT_SLURM_COMMANDS

    def submit_job(self, job=None, cfg=None):
        job_spec = job['job_spec']
        workdir = job_spec['dir']
        entrypoint_name = (job_spec.get('entrypoint') or
                           self.DEFAULT_ENTRYPOINT_NAME)
        entrypoint_path = os.path.join(workdir, entrypoint_name)
        cmd = [self.slurm_commands['sbatch'],
               ('--workdir=%s' % workdir), entrypoint_path]
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

    def get_keyed_engine_states(self, keyed_engine_metas=None):
        keyed_job_ids = {key: engine_meta['job_id']
                         for key, engine_meta in keyed_engine_metas.items()}
        job_ids = list(keyed_job_ids.values())
        slurm_jobs_by_id = self.get_slurm_jobs_by_id(job_ids=job_ids)
        keyed_engine_states = {}
        for key, job_id in keyed_job_ids.items():
            slurm_job = slurm_jobs_by_id.get(job_id)
            engine_state = self.slurm_job_to_engine_state(slurm_job=slurm_job)
            keyed_engine_states[key] = engine_state
        return keyed_engine_states

    def get_slurm_jobs_by_id(self, job_ids=None):
        if self.cfg.get('use_sacct'):
            slurm_jobs = self.get_slurm_jobs_via_sacct(job_ids=job_ids)
        else:
            slurm_jobs = self.get_slurm_jobs_via_scontrol(job_ids=job_ids)
        slurm_jobs_by_id = {slurm_job['JobId']: slurm_job
                            for slurm_job in slurm_jobs}
        return slurm_jobs_by_id

    def get_slurm_jobs_via_scontrol(self, job_ids=None):
        if not job_ids:
            return {}
        all_slurm_jobs = self.get_all_slurm_jobs_via_scontrol()
        return [slurm_job for slurm_job in all_slurm_jobs
                if slurm_job['JobId'] in job_ids]

    def get_all_slurm_jobs_via_scontrol(self):
        cmd = [self.slurm_commands['scontrol'], 'show', '--all', '--details',
               '--oneliner', 'job']
        completed_proc = self.process_runner.run_process(cmd=cmd, check=True)
        scontrol_output = completed_proc.stdout
        slurm_jobs = []
        if 'No jobs in the system' not in scontrol_output:
            for scontrol_line in scontrol_output.split("\n"):
                scontrol_line = scontrol_line.strip()
                if not scontrol_line:
                    continue
                slurm_jobs.append(self.parse_scontrol_line(scontrol_line))
        return slurm_jobs

    def get_slurm_job_via_scontrol(self, job_id=None):
        cmd = [self.slurm_comamnds['scontrol'], 'show', '--details',
               '--oneliner', 'job', job_id]
        try:
            completed_proc = self.process_runner.run_process(
                cmd=cmd, check=True)
            slurm_job = self.parse_scontrol_line(completed_proc.stdout)
        except self.process_runner.CalledProcessError as exc:
            if 'Invalid job id specified' in exc.stderr:
                slurm_job = None
            else:
                raise exc
        return slurm_job

    def parse_scontrol_line(self, scontrol_line=None):
        parsed = {}
        for key_value_str in scontrol_line.split():
            key, value = key_value_str.split('=', 1)
            parsed[key] = value
        return parsed

    def get_slurm_jobs_via_sacct(self, job_ids=None):
        if not job_ids:
            return []
        csv_job_ids = ','.join(list(job_ids))
        cmd = [self.slurm_commands['sacct'], '--jobs=%s' % csv_job_ids,
               '--long', '--noconvert', '--parsable2', '--allocations']
        completed_proc = self.process_runner.run_process(cmd=cmd, check=True)
        slurm_jobs = self.parse_sacct_stdout(
            sacct_stdout=completed_proc.stdout)['records']
        return slurm_jobs

    def parse_sacct_stdout(self, sacct_stdout=None):
        sacct_lines = sacct_stdout.split("\n")
        fields = self.split_sacct_line(sacct_line=sacct_lines[0])
        records = [
            self.parse_sacct_line(sacct_line=sacct_line, fields=fields)
            for sacct_line in sacct_lines[1:]
            if sacct_line
        ]
        return {'fields': fields, 'records': records}

    def split_sacct_line(self, sacct_line=None):
        return sacct_line.split('|')

    def parse_sacct_line(self, sacct_line=None, fields=None):
        parsed = {}
        line_parts = self.split_sacct_line(sacct_line=sacct_line)
        for i, value in enumerate(line_parts):
            parsed[fields[i]] = value
        return parsed

    def slurm_job_to_engine_state(self, slurm_job=None):
        engine_state = {'engine_job_state': slurm_job}
        if slurm_job is not None:
            engine_state['status'] = self.slurm_job_to_status(
                slurm_job=slurm_job)
        return engine_state

    def slurm_job_to_status(self, slurm_job=None):
        for engine_job_status, slurm_states \
                in self.SLURM_STATES_TO_ENGINE_JOB_STATUSES.items():
            if slurm_job['JobState'] in slurm_states:
                return engine_job_status
        return self.JOB_STATUSES.UNKNOWN
