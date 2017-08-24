from pathlib import Path
import json
import tempfile
import textwrap
import time


def generate_echo_job_dir(job_dir_path=None, message=None):
    job_dir_path = Path(
        job_dir_path
        or tempfile.mkdtemp(prefix='echo_job.{timestamp:.0f}'.format(
            timestamp=time.time()))
    ).absolute()
    job_dir_path.mkdir(parents=True, exist_ok=True)
    entrypoint_content = textwrap.dedent(
        '''
        #!/bin/bash
        echo {message}
        '''
    ).lstrip().format(message=message)
    entrypoint_name = 'entrypoint.sh'
    entrypoint_path = (job_dir_path / 'entrypoint.sh')
    with entrypoint_path.open('w') as f:
        f.write(entrypoint_content)
    entrypoint_path.chmod(0o755)

    job_spec = {
        'entrypoint': './' + entrypoint_name,
    }
    job_spec_path = (job_dir_path / 'job_spec.json')
    with job_spec_path.open('w') as f:
        f.write(json.dumps(job_spec, indent=2))
    return {'job_dir': str(job_dir_path)}
