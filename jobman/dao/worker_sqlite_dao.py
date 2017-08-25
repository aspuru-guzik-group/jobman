from .jobs_dao_mixin import JobsDaoMixin
from .sqlite_dao import SqliteDAO
from . import utils as _dao_utils


class WorkerSqliteDAO(JobsDaoMixin, SqliteDAO):
    def __init__(self, table_prefix=None, extra_job_fields=None, **kwargs):
        super().__init__(
            orm_specs=self._generate_orm_specs(
                extra_job_fields=extra_job_fields
            ),
            table_prefix=table_prefix,
            **kwargs
        )

    def _generate_orm_specs(self, extra_job_fields=None):
        return [
            {
                'name': 'job',
                'fields': self._generate_job_fields(
                    extra_job_fields=extra_job_fields)
            },
        ]

    def _generate_job_fields(self, extra_job_fields=None):
        return {
            'key': {'type': 'TEXT', 'primary_key': True,
                    'default': _dao_utils.generate_key},
            'status': {'type': 'TEXT'},
            'is_batch': {'type': 'INTEGER'},
            'batch_meta': {'type': 'JSON'},
            'batchable': {'type': 'INTEGER'},
            'parent_batch_key': {'type': 'TEXT'},
            'engine_meta': {'type': 'JSON'},
            'engine_state': {'type': 'JSON'},
            **_dao_utils.generate_timestamp_fields(),
            **(extra_job_fields or {}),
        }
