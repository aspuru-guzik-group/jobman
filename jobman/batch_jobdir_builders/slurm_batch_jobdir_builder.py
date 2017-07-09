import textwrap

from .bash_batch_jobdir_builder import BashBatchJobdirBuilder


class SlurmBatchJobdirBuilder(BashBatchJobdirBuilder):

    DEFAULT_PREAMBLE = textwrap.dedent(
        '''
        SRUN="srun --exclusive --nodes=1 --ntasks=1"
        # the --exclusive to srun make srun use distinct CPUs for each job step
        # --nodes=1 --ntasks=1 allocates a single core to each task

        PARALLEL="parallel --delay .2 -j $SLURM_NTASKS --joblog runtask.log \\
            --resume"
        # --delay .2 prevents overloading the controlling node
        # -j is the number of tasks parallel runs so we set it to $SLURM_NTASKS
        # --joblog makes parallel create a log of tasks that it has already run
        # --resume makes parallel use the joblog to resume from where it has
        #   left off
        # The combination of --joblog and --resume allow jobs to be resubmitted
        # if necessary and continue from where they left off.
        '''
    )

    def __init__(self, *args, time_fudge_factor=2.0,
                 default_subjob_time=(10 * 60), **kwargs):
        super().__init__(*args, **kwargs)
        self.time_fudge_factor = time_fudge_factor
        self.default_subjob_time = default_subjob_time

    def _build_batch_jobdir(self, *args, **kwargs):
        jobdir_meta = super()._build_batch_jobdir(*args, **kwargs)
        jobdir_meta['resource_params'] = self._generate_resource_params()
        return jobdir_meta

    def _generate_resource_params(self):
        resource_params = {
            'memory': self._get_max_subjob_memory(),
            'time': self._get_total_batch_time(),
        }
        return resource_params

    def _get_max_subjob_memory(self):
        max_mem = 0
        for subjob in self.subjobs:
            try: subjob_mem = subjob['jobdir_meta']['resources']['memory']
            except KeyError: subjob_mem = None
            if subjob_mem is not None and subjob_mem > max_mem:
                max_mem = subjob_mem
        if max_mem == 0: max_mem = None
        return max_mem

    def _get_total_batch_time(self):
        total_time = 0
        for subjob in self.subjobs:
            try: subjob_time = subjob['jobdir_meta']['resources']['time']
            except KeyError: subjob_time = None
            total_time += (subjob_time or self.default_subjob_time)
        return total_time * self.time_fudge_factor

    def _get_preamble_errors(self, preamble=None):
        errors = super()._get_preamble_errors(preamble=preamble) or []
        if 'SRUN=' not in preamble:
            errors.append("A line like 'SRUN=<your srun cmd>' must be present"
                          " in the preamble. It will be called like"
                          " 'pushd <subjob_dir>; $SRUN <entrypoint>' .")
        return errors

    def _generate_subjob_command(self, subjob=None):
        return "pushd {dir}; $SRUN {entrypoint}; popd".format(
            dir=subjob['jobdir_meta']['dir'],
            srun_cmd=self.srun_cmd,
            entrypoint=subjob['jobdir_meta']['entrypoint']
        )
