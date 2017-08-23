from pathlib import Path
import shutil
import time
import uuid

from jobman.dao.engine_sqlite_dao import EngineSqliteDAO
from .base_engine import BaseEngine


class InboxEngine(BaseEngine):
    def __init__(self, db_uri=None, root_dir=None, transfer_fn=shutil.move,
                 sync_fn=None, throttling_specs=None, **kwargs):
        super().__init__(**kwargs)
        self.root_dir = root_dir
        self.transfer_fn = transfer_fn
        self.sync_fn = sync_fn or self._default_sync_fn
        self.dao = EngineSqliteDAO(
            db_uri=db_uri, table_prefix='engine_%s_' % self.key,
            extra_job_fields={
                'local_dir': {'type': 'TEXT'},
                'remote_status': {'type': 'TEXT'},
                'remote_dir': {'type': 'TEXT'},
            }
        )
        self.throttling_specs = {
            **self._get_default_throttling_specs(),
            **(throttling_specs or {})
        }

    def _default_sync_fn(self, src, dest):
        dest_path = Path(dest)
        if dest_path.exists():
            dest_path.rmdir()
        self.transfer_fn(src, dest)

    def _get_default_throttling_specs(self):
        return {
            key: {
                # 'wait': 120,
                'wait': .001,
                'storage_key': 'LAST_%s_TIME' % key.upper(),
                'fn': getattr(self, '_%s' % key)
            }
            for key in ['scan_remote_dirs', 'sync_dirs']
        }

    def tick(self):
        for throttling_spec in self.throttling_specs.values():
            try:
                self._run_throttling_spec(throttling_spec)
            except:
                self.logger.exception("Failed to run throttling spec")

    def _run_throttling_spec(self, throttling_spec=None):
        last_run_time = None
        kvp = self.dao.get_kvp(key=throttling_spec['storage_key'])
        if kvp:
            last_run_time = kvp['value']
        needs_run = (
            last_run_time is None or
            ((time.time() - last_run_time) > throttling_spec['wait'])
        )
        if needs_run:
            throttling_spec['fn']()
        self.dao.save_kvps(kvps=[{'key': throttling_spec['storage_key'],
                                  'value': time.time()}])

    def _scan_remote_dirs(self):
        running_engine_jobs = self.dao.query_jobs(query={
            'filters': [
                {'field': 'status', 'op': '=',
                 'arg': self.JOB_STATUSES.RUNNING}
            ]
        })
        if not running_engine_jobs:
            return
        remote_inventory = self._get_remote_dir_inventory()
        jobs_to_save = []
        for engine_job in running_engine_jobs:
            remote_meta = remote_inventory.get(engine_job['key'])
            if remote_meta:
                engine_job['remote_status'] = remote_meta['status']
                engine_job['remote_dir'] = remote_meta['dir']
                jobs_to_save.append(engine_job)
        self.dao.save_jobs(jobs_to_save)

    def _get_remote_dir_inventory(self):
        inventory = {}
        for subdir, status in [
            ('inbox', self.JOB_STATUSES.RUNNING),
            ('queued', self.JOB_STATUSES.RUNNING),
            ('completed', self.JOB_STATUSES.EXECUTED),
            ('failed', self.JOB_STATUSES.FAILED),
        ]:
            remote_subdir_path = Path(self.root_dir, subdir)
            for dir_ in remote_subdir_path.glob('*'):
                inventory[str(dir_.name)] = {
                    'status': status,
                    'dir': str(dir_),
                }
        return inventory

    def _sync_dirs(self):
        engine_jobs_to_sync = self._get_engine_jobs_to_sync()
        if not engine_jobs_to_sync:
            return
        for engine_job in engine_jobs_to_sync:
            self._sync_dir_for_engine_job(engine_job)
            engine_job['status'] = engine_job['remote_status']
        self.dao.save_jobs(jobs=engine_jobs_to_sync)

    def _get_engine_jobs_to_sync(self):
        return self.dao.query_jobs(query={
            'filters': [
                {'field': 'status', 'op': '=',
                 'arg': self.JOB_STATUSES.RUNNING},
                {'field': 'remote_status', 'op': 'IN',
                 'arg': self.JOB_STATUSES.FINISHED_STATUSES},
            ]
        })

    def _sync_dir_for_engine_job(self, engine_job=None):
        local_dir = engine_job['local_dir']
        remote_dir = engine_job['remote_dir']
        self.sync_fn(remote_dir, local_dir)

    def submit_job(self, job=None, extra_cfgs=None):
        engine_job_key = self.key + '__' + str(uuid.uuid4())
        remote_dest = Path(self.root_dir, 'inbox', engine_job_key)
        local_dir = job['job_spec']['dir']
        self.transfer_fn(local_dir, str(remote_dest))
        self.dao.create_job(job={
            'key': engine_job_key,
            'local_dir': local_dir,
            'status': self.JOB_STATUSES.RUNNING
        })
        engine_meta = {'key': engine_job_key}
        return engine_meta

    def get_keyed_states(self, keyed_metas=None):
        engine_job_keys_by_external_keys = {
            external_key: engine_meta['key']
            for external_key, engine_meta in keyed_metas.items()
        }
        engine_job_keys = list(engine_job_keys_by_external_keys.values())
        engine_jobs = self.dao.query_jobs(query={
            'filters': [{'field': 'key', 'op': 'IN', 'arg': engine_job_keys}]
        })
        engine_jobs_by_engine_job_keys = {
            engine_job['key']: engine_job
            for engine_job in engine_jobs
        }
        engine_states_by_external_keys = {}
        for external_key in engine_job_keys_by_external_keys.keys():
            engine_job_key = engine_job_keys_by_external_keys[external_key]
            engine_job = engine_jobs_by_engine_job_keys.get(engine_job_key)
            engine_state = self._engine_job_to_engine_state(engine_job)
            engine_states_by_external_keys[external_key] = engine_state
        return engine_states_by_external_keys

    def _engine_job_to_engine_state(self, engine_job=None):
        engine_state = {}
        if engine_job:
            engine_state['status'] = engine_job['status']
        return engine_state
