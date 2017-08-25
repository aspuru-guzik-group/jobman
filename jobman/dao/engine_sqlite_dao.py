from .jobs_dao_mixin import JobsDaoMixin
from .sqlite_dao import SqliteDAO
from . import utils as _dao_utils


class EngineSqliteDAO(JobsDaoMixin, SqliteDAO):

    class JobSchema(_dao_utils.TimestampedSchemaMixin, _dao_utils.Schema):
        """Fields for job records."""

        key = {'type': 'TEXT', 'primary_key': True,
               'default': _dao_utils.generate_key}
        status = {'type': 'TEXT'}

    def __init__(self, table_prefix=None, extra_job_fields=None, **kwargs):
        super().__init__(
            orm_specs=self._generate_orm_specs(extra_job_fields),
            table_prefix=table_prefix,
            **kwargs
        )

    def _generate_orm_specs(self, extra_job_fields=None):
        return [{
            'name': 'job',
            'fields': {
                **self.JobSchema.get_field_infos(),
                **(extra_job_fields or {}),
            }
        }]
