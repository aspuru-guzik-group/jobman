import os
import time
import tempfile
import textwrap

from jobman.jobman import JobMan
from jobman.engines.local_engine import LocalEngine
from jobman.batch_builders.bash_batch_builder import BashBatchBuilder


class Entrypoint(object):
    def run(self):
        self._setup()
        job_specs = self._generate_job_specs()
        for job_spec in job_specs:
            self.jobman.submit_job_spec(job_spec=job_spec)
        self._tick_jobman(num_ticks=3)

    def _setup(self):
        this_dir = os.path.abspath(os.path.dirname(__file__))
        self.scratch_dir = tempfile.mkdtemp(
            dir=os.path.join(this_dir, 'scratch'))
        batch_builder = BashBatchBuilder(
            default_preamble="PARALLEL=/bin/bash")
        self.jobman = JobMan(
            jobman_db_uri=':memory:',
            engines={
                'my_local_engine': LocalEngine(
                    key='my_local_engine',
                    db_uri=':memory:',
                    scratch_dir=self.scratch_dir,
                    build_batch_jobdir_fn=batch_builder.build_batch_jobdir
                )
            },
            use_batching=True,
            lock_timeout=2,
        )

    def _generate_job_specs(self, num_jobdirs=3):
        job_specs = [self._generate_job_spec(ctx={'key': i})
                     for i in range(num_jobdirs)]
        return job_specs

    def _generate_job_spec(self, ctx=None):
        jobdir = tempfile.mkdtemp(dir=self.scratch_dir)
        entrypoint_name = 'entrypoint.sh'
        entrypoint_content = textwrap.dedent(
            '''
            #!/bin/bash
            echo "ctx: {ctx}" > output
            '''
        ).lstrip().format(ctx=ctx)
        entrypoint_path = os.path.join(jobdir, entrypoint_name)
        with open(entrypoint_path, 'w') as f:
            f.write(entrypoint_content)
        os.chmod(entrypoint_path, 0o755)
        job_spec = {
            'dir': jobdir,
            'entrypoint': ('./' + entrypoint_name),
            'batchable': True
        }
        return job_spec

    def _tick_jobman(self, num_ticks=3):
        for i in range(num_ticks):
            self.jobman.tick()
            time.sleep(1)


if __name__ == '__main__':
    Entrypoint().run()
