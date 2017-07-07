import logging
import os
import sqlite3
import time
import uuid

from . import orm as _orm


class SqliteDAO(object):
    class UpdateError(Exception): pass
    class InsertError(Exception): pass

    def __init__(self, db_uri=':memory:', logger=None, sqlite=sqlite3,
                 orm=_orm):
        self.logger = logger or logging
        if db_uri == 'sqlite://': db_uri = ':memory:'
        elif db_uri.startswith('sqlite:///'):
            db_uri = db_uri.replace('sqlite:///', '')
        self.db_uri = db_uri
        self.sqlite = sqlite

        self.orms = self._generate_orms(orm=orm)
        self._connection = None

    def _generate_orms(self, orm=None):
        return {
            'job': orm.ORM(name='job', fields=self._generate_job_fields(),
                           logger=self.logger),
            'kvp': orm.ORM(name='kvp', fields=self._generate_kvp_fields(),
                           logger=self.logger),
        }

    def _generate_job_fields(self):
        return {
            'key': {'type': 'TEXT', 'primary_key': True,
                    'default': self.generate_key},
            'status': {'type': 'TEXT'},
            'is_batch': {'type': 'INTEGER'},
            'batch_meta': {'type': 'JSON'},
            'batchable': {'type': 'INTEGER'},
            'parent_batch_key': {'type': 'TEXT'},
            'engine_meta': {'type': 'JSON'},
            'engine_state': {'type': 'JSON'},
            'source': {'type': 'TEXT'},
            'source_meta': {'type': 'JSON'},
            'source_tag': {'type': 'TEXT'},
            'submission': {'type': 'JSON'},
            **self._generate_timestamp_fields()
        }

    def _generate_kvp_fields(self):
        return {
            'key': {'type': 'TEXT', 'primary_key': True},
            'value': {'type': 'JSON'},
            **self._generate_timestamp_fields()
        }

    def _generate_timestamp_fields(self):
        return {
            'created': {'type': 'INTEGER', 'default': self._generate_timestamp},
            'modified': {'type': 'INTEGER',
                         'auto_update': self._generate_timestamp}
        }

    def generate_key(self): return self._generate_uuid()

    def _generate_uuid(self, *args, **kwargs):
        return str(uuid.uuid4())

    def _generate_timestamp(self, *args, **kwargs):
        return int(time.time())

    @property
    def connection(self):
        if not self._connection: self._connection = self.create_connection()
        return self._connection

    def create_connection(self):
        connection = self.sqlite.connect(self.db_uri)
        connection.row_factory = self.sqlite.Row
        return connection

    def ensure_db(self):
        should_create = False
        if self.db_uri == ':memory': should_create = True
        elif not os.path.exists(self.db_uri): should_create = True
        if should_create: self.create_db()

    def create_db(self):
        with self.connection:
            for orm in self.orms.values():
                orm.create_table(connection=self.connection)

    def create_job(self, job=None): return self.save_jobs(jobs=[job])[0]

    def save_jobs(self, jobs=None, replace=True):
        saved_jobs = []
        with self.connection:
            for job in jobs:
                try:
                    saved_job = self.orms['job'].save_object(
                        obj=job, replace=replace, connection=self.connection)
                except self.orms['job'].InsertError as exc:
                    raise self.InsertError() from exc
                saved_jobs.append(saved_job)
        return saved_jobs

    def get_jobs(self, query=None):
        return self.orms['job'].get_objects(query=query,
                                            connection=self.connection)

    def save_kvps(self, kvps=None, replace=True):
        with self.connection:
            for kvp in kvps:
                try:
                    self.orms['kvp'].save_object(obj=kvp,
                                                 connection=self.connection,
                                                 replace=replace)
                except self.orms['kvp'].InsertError as exc:
                    raise self.InsertError() from exc

    def get_kvps(self, query=None):
        return self.orms['kvp'].get_objects(query=query,
                                            connection=self.connection)

    def update_kvp(self, key=None, new_value=None, where_prev_value=...):
        filters = [{'field': 'key', 'operator': '=', 'value': key}]
        if where_prev_value is not ...:
            filters.append({'field': 'value', 'operator': '=',
                            'value': where_prev_value})
        try:
            update_result =  self.orms['kvp'].update_objects(
                query={'filters': filters}, updates={'value': new_value},
                connection=self.connection
            )
            assert update_result['rowcount'] == 1
        except Exception as exc: raise self.UpdateError() from exc

    def flush(self):
        os.remove(self.db_uri)
