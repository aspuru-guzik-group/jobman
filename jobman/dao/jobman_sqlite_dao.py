from .jobs_dao_mixin import JobsDaoMixin
from .sqlite_dao import SqliteDAO


class JobmanSqliteDAO(JobsDaoMixin, SqliteDAO):
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
            'worker_key': {'type': 'TEXT'},
            'worker_meta': {'type': 'JSON'},
            'errors': {'type': 'JSON'},
            'source_key': {'type': 'TEXT'},
            'source_meta': {'type': 'JSON'},
            'source_tag': {'type': 'TEXT'},
            'purgeable': {'type': 'INTEGER'},
            **self._generate_timestamp_fields()
        }

    def generate_source_key_filter(self, source_key=None):
        return {'field': 'source_key', 'op': '=', 'arg': source_key}
