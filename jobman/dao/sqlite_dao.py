import json
import sqlite3 as sqlite3
import textwrap
import time
from uuid import uuid4

from .base_dao import BaseDAO
            '''


class SqliteDAO(BaseDAO):

    def __init__(self, db_uri=None, sqlite=sqlite3, json=json):
        self.db_uri = db_uri
        self.sqlite = sqlite
        self.json = json

        self._job_record_fields = self.generate_job_record_fields()
        self._kvp_record_fields = self.generate_kvp_record_fields()
        self._connection = None

    def _generate_job_record_fields(self):
            'job_key': {'copy': True, 'default': self.generate_key},
            'status': {'copy': True},
            'created': {'copy': True, 'default': self.generate_timestamp},
            'modified': {'auto_update': self.generate_timestamp},
            'engine_meta': {'serialize': True},
            'engine_state': {'serialize': True}
            'engine_state': {'type': 'TEXT', 'JSON': True}
        }

    def _generate_kvp_record_fields(self):
        return {
            'key': {'copy': True},
            'created': {'copy': True, 'default': self.generate_timestamp},
            'modified': {'auto_update': self.generate_timestamp},
        }

    @property
    def connection(self):
        if not self._connection: self._connection = self.create_connection()
        return self._connection

    def create_connection(self):
        return self.sqlite.connect(self.db_uri)

    def create_db(self):
        self.create_job_table()
        self.create_kvp_table()

    def create_job_table(self):
        create_statement = textwrap.dedent(
            '''
            CREATE TABLE jobs (
                job_key TEXT PRIMARY KEY,
                status TEXT,
                engine_state TEXT,
                engine_meta TEXT,
                created INTEGER,
                modified INTEGER
            )
            '''
        ).strip()
        with self.connection: self.connection.execute(create_statement)

    def create_kvp_table(self):
        create_statement = textwrap.dedent(
            '''
            CREATE TABLE kvp (
                key TEXT PRIMARY KEY,
                value TEXT,
                created INTEGER,
                modified INTEGER
            )
            '''
        ).strip()
        with self.connection: self.connection.execute(create_statement)

    def save_jobs(self, jobs=None):
        job_records = [self._job_to_job_record(job=job) for job in jobs]
        self._save_job_records(job_records=job_records)

    def _job_to_job_record(self, job=None):
        return self._obj_to_record(obj=job, fields=self._job_record_fields)

    def _job_record_to_job(self, job_record=None):
        return self._record_to_obj(record=job_record,
                                   fields=self._job_record_fields)

    def _serialize_value(self, value=None):
        return self.json.dumps(value)

    def _deserialize_value(self, serialized_value=None):
        if serialized_value is None or serialized_value == '': return None
        return self.json.loads(serialized_value)

    def _save_job_records(self, job_records=None):
        return [self._save_job_record(job_record=job_record)
                for job_record in job_records]

    def _save_job_record(self, job_record=None):
        return self._save_record(record=job_record, table='jobs',
                          fields=self._job_record_fields)

    def generate_key(self):
        return str(uuid4())

    def generate_timestamp(self):
        return int(time.time())

    def get_jobs(self, query=None):
        job_records = self._get_job_records(query=query)
        return [self._job_record_to_job(job_record=job_record)
                for job_record in job_records]

    def _get_job_records(self, query=None):
        query_spec = {'table': 'jobs', 'filters': []}
        for _filter in query.get('filters', []):
            if _filter['field'] in ['status']:
                query_spec['filters'].append(_filter)
            else:
                raise Exception("unknown filter field '{field}'".format(
                    _filter['field']))
        return self._execute_query(query_spec=query_spec)

    def _execute_query(self, query_spec=None):
        args = []
        statement = 'SELECT {fields} FROM {table}'.format( 
            fields=query_spec.get('fields', '*'),
            table=query_spec['table']
        )
        where_clauses = []
        for _filter in query_spec.get('filters', []):
            where_clauses.append(self._filter_to_where_clause(_filter=_filter))
            args.append(_filter['value'])
        if where_clauses:
            statement += '\nWHERE ' + ' AND '.join(where_clauses)
        return self.connection.execute(statement, args)

    def _filter_to_where_clause(self, _filter=None):
        return ''.join([_filter['field'], _filter['operator'], '?'])

    def save_kvps(self, kvps=None):
        kvp_records = [self._kvp_to_kvp_record(kvp=kvp) for kvp in kvps]
        self._save_kvp_records(kvp_records=kvp_records)

    def _kvp_to_kvp_record(self, kvp=None):
        return self._obj_to_record(obj=kvp, fields=self._kvp_record_fields)

    def _kvp_record_to_kvp(self, kvp_record=None):
        return self._record_to_obj(record=kvp_record,
                                   fields=self._kvp_record_fields)

    def _save_kvp_record(self, kvp_record=None):
        return self._save_record(record=kvp_record, table='kvps',
                                 fields=self._kvp_record_fields)

    def _obj_to_record(self, obj=None, fields=None):
        record = {}
        for field, field_def in fields.items():
            if field_def.get('copy'):
                record[field] = obj.get(field)
            if field_def.get('serialize'):
                record[field] = self._serialize_value(obj.get(field, {}))
        return record

    def _record_to_obj(self, record=None, fields=None):
        obj = {}
        for field, field_def in fields.items():
            if field_def.get('copy'):
                obj[field] = record.get(field)
            if field_def.get('serialize'):
                obj[field] = self._deserialize_value(record.get(field, {}))

    def _save_record(self, record=None, table=None, fields=None):
        field_names = []
        args = []
        for field, field_def in fields.items():
            if field_def.get('default') and record.get(field) is None:
                record[field] = field_def['default']()
            if field_def.get('auto_update'):
                record[field] = field_def['auto_update'](record=record)
            field_names.append(field)
            args.append(record[field])
        statement = textwrap.dedent(
            '''
            INSERT OR REPLACE INTO {table}
                ({csv_fields})
                VALUES ({csv_placeholders})
            '''
        ).strip().format(
            table=table,
            csv_fields=(','.join(field_names)),
            csv_placeholders=(','.join(['?' for field_name in field_names]))
        )
        with self.connection: self.connection.execute(statement, args)
        return record
