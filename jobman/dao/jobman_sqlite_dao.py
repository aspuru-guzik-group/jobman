from .sqlite_dao import SqliteDAO


class JobmanSqliteDAO(SqliteDAO):
    def __init__(self, **kwargs):
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
            'is_batch': {'type': 'INTEGER'},
            'batch_meta': {'type': 'JSON'},
            'batchable': {'type': 'INTEGER'},
            'parent_batch_key': {'type': 'TEXT'},
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

    def create_job(self, job=None):
        return self.create_ent(ent_type='job', ent=job)

    def save_jobs(self, jobs=None, replace=True):
        return self.save_ents(ent_type='job', ents=jobs, replace=replace)

    def query_jobs(self, query=None):
        return self.query_ents(ent_type='job', query=query)

    def get_job(self, key=None):
        return self.get_ents(ent_type='job', key=key)
