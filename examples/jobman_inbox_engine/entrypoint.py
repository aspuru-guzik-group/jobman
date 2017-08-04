from pathlib import Path
import tempfile
import textwrap
import time

from jobman.jobman import JobMan
from jobman.engines.local_engine import LocalEngine
from jobman.engines.inbox_engine import InboxEngine


class Entrypoint(object):
    def run(self):
        this_dir = Path(__file__).absolute().parent
        self.scratch_dir = tempfile.mkdtemp(dir=Path(this_dir, 'scratch'))
        self.inbox_root = Path(self.scratch_dir, 'inbox_root')
        self.inbox_root.mkdir(parents=True)
        self.upstream_jobman = JobMan(
            jobman_db_uri=':memory:',
            engine=InboxEngine(inbox_root=self.inbox_root)
        )
        self.downstream_jobman = JobMan(
            jobman_db_uri=':memory:',
            engine=LocalEngine(scratch_dir=self.scratch_dir),
            sources=[]
        )
        job_specs = [self._generate_job_spec(ctx={'key': i}) for i in range(3)]
        for job_spec in job_specs:
            self.jobman.submit_job_spec(job_spec=job_spec)
        for i in range(3):
            self.upstream_jobman.tick()
            self.downstream_jobman.tick()
            time.sleep(.1)

    def _generate_job_spec(self, ctx=None):
        jobdir = tempfile.mkdtemp(dir=self.scratch_dir)
        entrypoint_name = 'entrypoint.sh'
        entrypoint_content = textwrap.dedent(
            '''
            #!/bin/bash
            echo "ctx: {ctx}" > output
            '''
        ).lstrip().format(ctx=ctx)
        entrypoint_path = Path(jobdir, entrypoint_name)
        with open(entrypoint_path, 'w') as f:
            f.write(entrypoint_content)
        entrypoint_path.chmod(0o755)
        job_spec = {
            'dir': jobdir,
            'entrypoint': ('./' + entrypoint_name),
        }
        return job_spec


if __name__ == '__main__':
    Entrypoint().run()
