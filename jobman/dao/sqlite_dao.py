import logging
import os
import sqlite3 as sqlite
import time
import uuid

from . import orm


class SqliteDAO(object):
    class UpdateError(Exception):
        pass

    class InsertError(Exception):
        pass

    def __init__(self, db_uri=':memory:', orm_specs=None, table_prefix=None,
                 ensure_tables=True, logger=None, include_kvp_orm=True):
        self.logger = logger or logging
        if db_uri == 'sqlite://':
            db_uri = ':memory:'
        elif db_uri.startswith('sqlite:///'):
            db_uri = db_uri.replace('sqlite:///', '')
        self.db_uri = db_uri
        self.orms = self._generate_orms(orm_specs=orm_specs,
                                        table_prefix=table_prefix,
                                        include_kvp_orm=include_kvp_orm)
        self._connection = None
        if ensure_tables:
            self.ensure_tables()

    def _generate_orms(self, orm_specs=None, table_prefix=None,
                       include_kvp_orm=None):
        common_orm_kwargs = {'logger': self.logger,
                             'table_prefix': table_prefix}
        if include_kvp_orm:
            orm_specs += [{'name': 'kvp',
                           'fields': self._generate_kvp_fields()}]
        orms = {
            orm_spec['name']: orm.ORM(**{**common_orm_kwargs, **orm_spec})
            for orm_spec in orm_specs
        }
        return orms

    def _generate_kvp_fields(self):
        return {
            'key': {'type': 'TEXT', 'primary_key': True},
            'value': {'type': 'JSON'},
            **self._generate_timestamp_fields()
        }

    def _generate_timestamp_fields(self):
        return {
            'created': {'type': 'INTEGER',
                        'default': self.generate_timestamp},
            'modified': {'type': 'INTEGER',
                         'auto_update': self.generate_timestamp}
        }

    @classmethod
    def generate_key(cls):
        return cls.generate_uuid()

    @classmethod
    def generate_uuid(cls, *args, **kwargs):
        return str(uuid.uuid4())

    @classmethod
    def generate_timestamp(cls, *args, **kwargs):
        return int(time.time())

    @property
    def connection(self):
        if not self._connection:
            self._connection = self.create_connection()
        return self._connection

    def create_connection(self):
        connection = sqlite.connect(self.db_uri)
        connection.row_factory = sqlite.Row
        return connection

    def ensure_tables(self):
        with self.connection:
            for orm_ in self.orms.values():
                orm_.create_table(connection=self.connection)

    def create_ent(self, ent_type=None, ent=None):
        return self.save_ents(ent_type=ent_type, ents=[ent])[0]

    def save_ents(self, ent_type=None, ents=None, replace=True):
        saved_ents = []
        ent_orm = self.orms[ent_type]
        with self.connection:
            for ent in ents:
                try:
                    saved_ent = ent_orm.save_object(
                        obj=ent, replace=replace, connection=self.connection)
                except ent_orm.InsertError as exc:
                    raise self.InsertError() from exc
                saved_ents.append(saved_ent)
        return saved_ents

    def get_ent(self, ent_type=None, key=None):
        ent_orm = self.orms[ent_type]
        query = {'filters': [{'field': 'key', 'op': '=', 'arg': key}]}
        try:
            return ent_orm.get_objects(query=query,
                                       connection=self.connection)[0]
        except IndexError:
            return None

    def query_ents(self, ent_type=None, query=None):
        ent_orm = self.orms[ent_type]
        return ent_orm.get_objects(query=query, connection=self.connection)

    def save_kvps(self, kvps=None, replace=True):
        return self.save_ents(ent_type='kvp', ents=kvps, replace=replace)

    def create_kvp(self, kvp=None):
        return self.create_ent(ent_type='kvp', ent=kvp)

    def query_kvps(self, query=None):
        return self.get_ents(ent_type='kvp', query=query)

    def get_kvp(self, key=None):
        return self.get_ent(ent_type='kvp', key=key)

    def update_kvp(self, key=None, new_value=None, where_prev_value=...):
        kvp_orm = self.orms['kvp']
        filters = [{'field': 'key', 'op': '=', 'arg': key}]
        if where_prev_value is not ...:
            filters.append({'field': 'value', 'op': '=',
                            'arg': where_prev_value})
        try:
            update_result = kvp_orm.update_objects(
                query={'filters': filters}, updates={'value': new_value},
                connection=self.connection
            )
            assert update_result['rowcount'] == 1
        except Exception as exc:
            raise self.UpdateError() from exc

    def flush(self):
        os.remove(self.db_uri)
