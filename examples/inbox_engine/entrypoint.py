from pathlib import Path
import tempfile
import textwrap
import time

from jobman.jobman import JobMan
from jobman.engines.local_engine import LocalEngine
from jobman.engines.inbox_engine import InboxEngine
from jobman.job_sources.dir_job_source import DirJobSource


class Entrypoint(object):
    def run(self):
        this_dir = Path(__file__).absolute().parent
        self.scratch_dir = tempfile.mkdtemp(dir=str(Path(this_dir, 'scratch')))
        self.root_path = Path(self.scratch_dir, 'inbox_engine_root')
        self.root_path.mkdir(parents=True)
        self.db_uri = 'sqlite://'
        self.upstream_jobman = JobMan(
            jobman_db_uri=self.db_uri,
            engines={
                'my_inbox': InboxEngine(
                    key='my_inbox',
                    db_uri=self.db_uri,
                    root_dir=str(self.root_path)
                ),
            },
        )
        self.downstream_jobman = JobMan(
            jobman_db_uri=':memory:',
            engines={
                'my_local': LocalEngine(
                    key='my_local',
                    scratch_dir=self.scratch_dir,
                    db_uri=self.db_uri
                ),
            },
            job_sources={
                'my_root': DirJobSource(
                    key='my_root', root_path=self.root_path)
            },
        )
        job_specs = [self._generate_job_spec(ctx={'key': i}) for i in range(3)]
        for job_spec in job_specs:
            self.upstream_jobman.submit_job_spec(job_spec=job_spec)
        for i in range(5):
            self.upstream_jobman.tick()
            self.downstream_jobman.tick()
            time.sleep(.1)
        jobs = self.upstream_jobman.query_jobs()
        job_statuses = [job['status'] for job in jobs]
        assert set(job_statuses) == set(['COMPLETED']), set(job_statuses)

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
        with entrypoint_path.open('w') as f:
            f.write(entrypoint_content)
        entrypoint_path.chmod(0o755)
        job_spec = {
            'dir': jobdir,
            'entrypoint': ('./' + entrypoint_name),
        }
        return job_spec


if __name__ == '__main__':
    Entrypoint().run()
