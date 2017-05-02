import sqlite3 as sqlite3
import time
import uuid

from .base_dao import BaseDAO
from . import orm as _orm


class SqliteDAO(BaseDAO):
    def __init__(self, db_uri=None, sqlite=sqlite3, orm=_orm):
        self.db_uri = db_uri
        self.sqlite = sqlite

        self.orms = self._generate_orms(orm=orm)
        self._connection = None

    def _generate_orms(self, orm=None):
        return {
            'job': orm.ORM(name='job', fields=self._generate_job_fields()),
            'kvp': orm.ORM(name='kvp', fields=self._generate_kvp_fields()),
        }

    def _generate_job_fields(self):
        return {
            'job_key': {'type': 'TEXT', 'primary_key': True,
                        'default': self._generate_uuid},
            'status': {'type': 'TEXT'},
            'engine_meta': {'type': 'JSON'},
            'engine_state': {'type': 'JSON'},
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

    def _generate_uuid(self):
        return str(uuid.uuid4())

    def _generate_timestamp(self):
        return int(time.time())

    @property
    def connection(self):
        if not self._connection: self._connection = self.create_connection()
        return self._connection

    def create_connection(self):
        return self.sqlite.connect(self.db_uri)

    def create_db(self):
        with self.connection:
            for orm in self.orms.values():
                orm.create_table(connection=self.connection)

    def save_jobs(self, jobs=None):
        with self.connection:
            for job in jobs:
                self.orms['job'].save_obj(obj=job, connection=self.connection)

    def get_jobs(self, query=None):
        return self.orms['job'].get_objects(query=query,
                                            connection=self.connection)

    def save_kvps(self, kvps=None):
        with self.connection:
            for kvp in kvps:
                self.orms['kvp'].save_obj(obj=kvp, connection=self.connection)

    def get_kvps(self, query=None):
        return self.orms['kvp'].get_objects(query=query,
                                            connection=self.connection)
