import os
import time
import tempfile
import textwrap

from jobman.jobman import JobMan
from jobman.engines.local_engine import LocalEngine


class Entrypoint(object):
    @classmethod
    def run(cls): cls()._run()

    def _run(self):
        self._setup()
        jobdir_metas = self._generate_jobdirs()
        for jobdir_meta in jobdir_metas:
            self.jobman.submit_jobdir_meta(jobdir_meta=jobdir_meta)
        self._tick_jobman(num_ticks=3)

    def _setup(self):
        self.debug = os.environ.get('DEBUG')
        this_dir = os.path.abspath(os.path.dirname(__file__))
        self.scratch_dir = tempfile.mkdtemp(
            dir=os.path.join(this_dir, 'scratch'))
        self.engine = LocalEngine(debug=self.debug,
                                  scratch_dir=self.scratch_dir)
        self.jobman = JobMan(
            jobman_db_uri=':memory:',
            debug=self.debug,
            engine=self.engine,
            use_batching=True,
            lock_timeout=2,
        )

    def _generate_jobdirs(self, num_submissions=3):
        jobdir_metas = [self._generate_jobdir(ctx={'key': i})
                        for i in range(num_submissions)]
        return jobdir_metas

    def _generate_jobdir(self, ctx=None):
        jobdir = tempfile.mkdtemp(dir=self.scratch_dir)
        entrypoint_name = 'entrypoint.sh'
        entrypoint_content = textwrap.dedent(
            '''
            #!/bin/bash
            echo "ctx: {ctx}" > output
            '''
        ).lstrip().format(ctx=ctx)
        entrypoint_path = os.path.join(jobdir, entrypoint_name)
        with open(entrypoint_path, 'w') as f: f.write(entrypoint_content)
        os.chmod(entrypoint_path, 0o755)
        jobdir_meta = {'dir': jobdir, 'entrypoint': entrypoint_name,
                       'batchable': True}
        return jobdir_meta

    def _tick_jobman(self, num_ticks=3):
        for i in range(num_ticks):
            self.jobman.tick()
            time.sleep(1)

if __name__ == '__main__': Entrypoint.run()
