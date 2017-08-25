from .jobs_dao_mixin import JobsDaoMixin
from .sqlite_dao import SqliteDAO
from . import utils as _dao_utils


class WorkerSqliteDAO(JobsDaoMixin, SqliteDAO):
    def __init__(self, table_prefix=None, extra_job_fields=None, **kwargs):
        super().__init__(
            orm_specs=self._generate_orm_specs(extra_job_fields),
            table_prefix=table_prefix,
            **kwargs
        )

    class JobSchema(_dao_utils.TimestampedSchemaMixin, _dao_utils.Schema):
        """Fields for job records."""
        key = {'type': 'TEXT', 'primary_key': True,
               'default': _dao_utils.generate_key}
        status = {'type': 'TEXT'}
        batchable = {'type': 'INTEGER'}
        """Flag to indicate whether a job can be included in a batch."""
        batch_meta = {'type': 'JSON'}
        """Metadata for batch jobs, often includes list of subjobs"""
        is_batch = {'type': 'INTEGER'}
        """Flag to indicate whether a job is a batch job."""
        parent_batch_key = {'type': 'Text'}
        """If job is part of batch, this is the parent batch job's key."""
        engine_key = {'type': 'TEXT'}
        """Key for engine handling the job."""
        engine_meta = {'type': 'JSON'}
        """Metadata to retrieve related state from engine."""

    def _generate_orm_specs(self, extra_job_fields=None):
        return [{
            'name': 'job',
            'fields': {
                **self.JobSchema.get_field_infos(),
                **(extra_job_fields or {}),
            }
        }]
