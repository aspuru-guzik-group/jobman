from pathlib import Path
import shutil
import uuid

from .base_engine import BaseEngine


class InboxEngine(BaseEngine):
    def __init__(self, root_dir=None, transfer_fn=shutil.move, **kwargs):
        super().__init__(**kwargs)
        self.root_dir = root_dir
        self.transfer_fn = transfer_fn

    def submit_job(self, job=None, extra_cfgs=None):
        dir_name = str(uuid.uuid4())
        dest = Path(self.root_dir, 'inbox', dir_name)
        self.transfer_fn(job['job_spec']['dir'], dest)
        engine_meta = {'dir_name': dir_name}
        return engine_meta

    def get_keyed_engine_states(self, keyed_engine_metas=None):
        lookup = self._generate_dir_lookup()
        return {
            key: lookup.get(
                engine_meta['dir_name'],
                {'status': self.JOB_STATUSES.UNKNOWN}
            )
            for key, engine_meta in keyed_engine_metas.items()
        }

    def _generate_dir_lookup(self):
        lookup = {}
        for parent_dir, status in [
            ('inbox', self.JOB_STATUSES.RUNNING),
            ('queued', self.JOB_STATUSES.RUNNING),
            ('executed', self.JOB_STATUSES.EXECUTED),
        ]:
            parent_path = Path(self.root_dir, parent_dir)
            for dir_ in parent_path.glob('*'):
                lookup[str(dir_.name)] = {
                    'path': str(Path(parent_path, dir_)),
                    'status': status,
                }
        return lookup
