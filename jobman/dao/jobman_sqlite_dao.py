import contextlib
import time

from .sqlite_dao import SqliteDAO


class JobmanSqliteDAO(SqliteDAO):
    LOCK_KEY = '_JOBMAN_LOCK'

    def __init__(self, lock_timeout=30, debug=None, **kwargs):
        self.debug = debug
        self.lock_timeout = lock_timeout
        super().__init__(
            orm_specs=self._generate_orm_specs(),
            table_prefix='jobman_',
            **kwargs
        )

    def _generate_orm_specs(self):
        return [
            {'name': 'job', 'fields': self._generate_job_fields()},
        ]

    def _generate_job_fields(self):
        return {
            'key': {'type': 'TEXT', 'primary_key': True,
                    'default': self.generate_key},
            'job_spec': {'type': 'JSON'},
            'status': {'type': 'TEXT'},
            'batchable': {'type': 'INTEGER'},
            'engine_key': {'type': 'TEXT'},
            'engine_meta': {'type': 'JSON'},
            'engine_state': {'type': 'JSON'},
            'errors': {'type': 'JSON'},
            'source_key': {'type': 'TEXT'},
            'source_meta': {'type': 'JSON'},
            'source_tag': {'type': 'TEXT'},
            'purgeable': {'type': 'INTEGER'},
            **self._generate_timestamp_fields()
        }

    def ensure_db(self):
        self.ensure_tables()
        self._ensure_lock_kvp()

    def _ensure_lock_kvp(self):
        lock_kvp = {'key': self.LOCK_KEY, 'value': 'UNLOCKED'}
        self.save_kvps(kvps=[lock_kvp], replace=False)

    def create_job(self, job=None):
        return self.create_ent(ent_type='job', ent=job)

    def save_jobs(self, jobs=None, replace=True):
        return self.save_ents(ent_type='job', ents=jobs, replace=replace)

    def query_jobs(self, query=None):
        return self.query_ents(ent_type='job', query=query)

    def get_job(self, key=None):
        return self.get_ents(ent_type='job', key=key)

    def get_jobs_for_status(self, status=None):
        return self.query_jobs(query={
            'filters': [self.generate_status_filter(status=status)]
        })

    def generate_status_filter(self, status=None):
        return {'field': 'status', 'op': '=', 'arg': status}

    def generate_source_key_filter(self, source_key=None):
        return {'field': 'source_key', 'op': '=', 'arg': source_key}

    @property
    @contextlib.contextmanager
    def get_lock(self):
        self._acquire_lock()
        try: yield  # noqa
        finally: self._release_lock()  # noqa

    def _acquire_lock(self):
        start_time = time.time()
        while time.time() - start_time < self.lock_timeout:
            try:
                self.update_kvp(
                    key=self.LOCK_KEY,
                    new_value='LOCKED',
                    where_prev_value='UNLOCKED'
                )
                return
            except self.UpdateError as exc:
                if self.debug:
                    self.logger.exception('UpdateError')
                    self.logger.debug("waiting for lock")
                time.sleep(1)
        raise self.LockError("Could not acquire lock within timeout window")

    def _release_lock(self):
        self.update_kvp(
            key=self.LOCK_KEY, new_value='UNLOCKED', where_prev_value='LOCKED')
