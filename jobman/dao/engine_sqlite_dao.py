from .sqlite_dao import SqliteDAO


class EngineSqliteDAO(SqliteDAO):
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
                    'default': self.generate_key},
            'status': {'type': 'TEXT'},
            **self._generate_timestamp_fields(),
            **(extra_job_fields or {}),
        }

    def create_job(self, job=None):
        return self.create_ent(ent_type='job', ent=job)

    def save_jobs(self, jobs=None, replace=True):
        return self.save_ents(ent_type='job', ents=jobs, replace=replace)

    def query_jobs(self, query=None):
        return self.query_ents(ent_type='job', query=query)
