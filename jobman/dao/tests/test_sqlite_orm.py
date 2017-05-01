from collections import defaultdict
import textwrap
import unittest
from unittest.mock import call, MagicMock

from .. import sqlite_orm


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.connection = MagicMock()
        self.name = 'my_name'
        self.fields = defaultdict(MagicMock)
        self.orm = self.generate_orm()

    def generate_orm(self, **kwargs):
        merged_kwargs = {
            'name': self.name,
            'fields': self.fields,
            'json': MagicMock(),
            **kwargs
        }
        return sqlite_orm.SqliteORM(**merged_kwargs)

class CreateTableTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.orm._generate_column_def = MagicMock(return_value='column_def')
        self.orm.fields = {i: MagicMock() for i in range(3)}

    def test_creates_table(self):
        self.orm.create_table(connection=self.connection)
        expected_statement = 'CREATE TABLE {table} ({column_defs})'.format(
            table=self.name,
            column_defs=(
                ",\n".join([
                    self.orm._generate_column_def(field=field,
                                                  field_def=field_def)
                    for field, field_def in self.orm.fields.items()])
            )
        )
        self.assertEqual(self.connection.execute.call_args,
                         call(expected_statement))

class _GenerateColumnDefTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.orm._get_column_type = MagicMock()
        self.field = MagicMock()
        self.field_def = defaultdict(MagicMock)

    def _get_column_def(self):
        return self.orm._generate_column_def(field=self.field,
                                             field_def=self.field_def)

    def test_column_def_has_field_name_and_type(self):
        expected_column_def = '{field} {column_type}'.format(
            field=self.field,
            column_type=self.orm._get_column_type.return_value
        )
        column_def = self._get_column_def()
        self.assertEqual(self.orm._get_column_type.call_args,
                         call(field_type=self.field_def['type']))
        self.assertEqual(column_def, expected_column_def)

    def test_includes_pkey_def_if_specified(self):
        self.field_def['primary_key'] = True
        expected_column_def = '{field} {column_type} PRIMARY KEY'.format(
            field=self.field,
            column_type=self.orm._get_column_type.return_value
        )
        column_def = self._get_column_def()
        self.assertEqual(column_def, expected_column_def)

class _GetColumnTypeTestCase(BaseTestCase):
    def test_returns_field_type_for_non_json(self):
        field_type = MagicMock()
        column_type = self.orm._get_column_type(field_type=field_type)
        self.assertEqual(column_type, field_type)

    def test_returns_text_for_json(self):
        field_type = 'JSON'
        column_type = self.orm._get_column_type(field_type=field_type)
        self.assertEqual(column_type, 'TEXT')

class SaveObjTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        for attr in ['_obj_to_record', '_save_record']: 
            setattr(self.orm, attr, MagicMock())
        self.obj = MagicMock()
        self.orm.save_obj(obj=self.obj, connection=self.connection)

    def test_converts_to_record_and_saves(self):
        self.assertEqual(self.orm._obj_to_record.call_args,
                         call(obj=self.obj))
        self.assertEqual(self.orm._save_record.call_args,
                         call(record=self.orm._obj_to_record.return_value,
                              connection=self.connection))

class _ObjToRecordTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.obj = MagicMock()
        self.orm._serialize_json_value = MagicMock()

    def _obj_to_record(self):
        return self.orm._obj_to_record(obj=self.obj)

    def test_copies_values_to_record(self):
        self.orm.fields = {i: MagicMock() for i in range(3)}
        expected_record = {field: self.obj.get(field)
                           for field in self.orm.fields}
        record = self._obj_to_record()
        self.assertEqual(record, expected_record)

    def test_serializes_json_field_values(self):
        self.orm.fields = {
            'json_field': {'type': 'JSON'},
            'non_json_field': MagicMock()
        }
        expected_record = {
            'json_field': self.orm._serialize_json_value.return_value,
            'non_json_field': self.obj.get('non_json_field')
        }
        record = self._obj_to_record()
        self.assertEqual(self.orm._serialize_json_value.call_args,
                         call(self.obj.get('json_field')))
        self.assertEqual(record, expected_record)

class _SaveRecordTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.orm.execute_insert_or_replace = MagicMock()
        self.record = defaultdict(MagicMock)

    def _save_record(self):
        return self.orm._save_record(record=self.record,
                                     connection=self.connection)

    def test_sets_default_values_if_empty(self):
        self.orm.fields = {
            'unset_w_default': {'default': MagicMock()},
            'set_w_default': {'default': MagicMock()},
            'sans_default': {'default': None},
        }
        self.record['set_w_default'] = MagicMock()
        self._save_record()
        expected_modified_record = {
            **self.record,
            'unset_w_default': (
                self.orm.fields['unset_w_default']['default'].return_value)
        }
        expected_fields = sorted(self.orm.fields.keys())
        expected_values = [expected_modified_record.get(field)
                           for field in expected_fields]
        self.assertEqual(self.orm.execute_insert_or_replace.call_args,
                         call(fields=expected_fields, values=expected_values,
                              connection=self.connection))

    def test_sets_autoupdate_values(self):
        self.orm.fields = {
            'w_auto_update': {'auto_update': MagicMock()},
            'sans_auto_update': {'auto_update': None},
        }
        self._save_record()
        expected_modified_record = {
            **self.record,
            'w_auto_update': (
                self.orm.fields['w_auto_update']['auto_update'].return_value)
        }
        self.assertEqual(
            self.orm.fields['w_auto_update']['auto_update'].call_args,
            call(record=self.record)
        )
        expected_fields = sorted(self.orm.fields.keys())
        expected_values = [expected_modified_record.get(field)
                           for field in expected_fields]
        self.assertEqual(self.orm.execute_insert_or_replace.call_args,
                         call(fields=expected_fields, values=expected_values,
                              connection=self.connection))

    def test_returns_record(self):
        result = self._save_record()
        self.assertEqual(result, self.record)

class ExecuteInsertOrReplaceTestCase(BaseTestCase):
    def test_executes_insert_or_replace_statement(self):
        fields = ["field_%s" % i for i in range(3)]
        values = [MagicMock() for i in range(3)]
        self.orm.execute_insert_or_replace(fields=fields, values=values,
                                           connection=self.connection)
        expected_statement = textwrap.dedent(
            '''
            INSERT OR REPLACE INTO {table} ({csv_fields})
            VALUES ({csv_placeholders})
            '''
        ).strip().format(
            table=self.name,
            csv_fields=(','.join(fields)),
            csv_placeholders=(','.join(['?' for field in fields]))
        )
        self.assertEqual(self.connection.execute.call_args,
                         call(expected_statement, values))

class GetObjectsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        for attr in ['_validate_query', '_get_records', '_record_to_obj']:
            setattr(self.orm, attr, MagicMock())
        self.orm._get_records.return_value = [MagicMock() for i in range(3)]
        self.query = MagicMock()
        self.result = self.orm.get_objects(query=self.query,
                                           connection=self.connection)

    def test_validates_query(self):
        self.assertEqual(self.orm._validate_query.call_args,
                         call(query=self.query))

    def test_gets_records(self):
        self.assertEqual(self.orm._get_records.call_args,
                         call(query=self.query, connection=self.connection))

    def test_returns_converted_job_records(self):
        self.assertEqual(
            self.orm._record_to_obj.call_args_list,
            [call(record=record)
             for record in self.orm._get_records.return_value]
        )
        self.assertEqual(
            self.result,
            [self.orm._record_to_obj.return_value
             for record in self.orm._get_records.return_value]
        )

class ExecuteQueryTestCase(BaseTestCase):
    def _execute_query(self):
        self.result = self.orm._execute_query(query=self.query,
                                              connection=self.connection)

    def test_executes_query_w_filters(self):
        self.query = {'filters': [MagicMock() for i in range(3)]}
        expected_statement = textwrap.dedent(
            '''
            SELECT * FROM {table}
            WHERE status=?
            '''
        ).strip().format(
            fields=self.query.get('fields', '*'),
            table=self.orm.name
        )
        expected_values = [self.query['filters'][0]['value']]
        self.assertEqual(self.dao.connection.execute.call_args,
                         call(expected_statement, expected_values))

    def test_returns_query_results(self):
        self.assertEqual(self.result, self.dao.connection.execute.return_value)

class _JobRecordToJobTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.dao._deserialize_value = MagicMock()
        self.job_record = MagicMock()
        self.result = self.dao._job_record_to_job(job_record=self.job_record)

    def test_copies_expected_fielsd(self):
        expected_values = {field: self.job_record.get(field)
                           for field in self.dao.JOB_RECORD_FIELDS['to_copy']}
        actual_values = {field: self.result[field]
                         for field in self.dao.JOB_RECORD_FIELDS['to_copy']}
        self.assertEqual(actual_values, expected_values)

    def test_deserializes_expected_fields(self):
        expected_values = {
            field: self.dao._deserialize_value(self.job_record.get(field, {}))
            for field in self.dao.JOB_RECORD_FIELDS['to_serialize']
        }
        actual_values = {
            field: self.result[field]
            for field in self.dao.JOB_RECORD_FIELDS['to_serialize']
        }
        self.assertEqual(actual_values, expected_values)

class SaveKvpsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.kvps = [MagicMock() for i in range(3)]
        for attr in ['_kvp_to_kvp_record', '_save_kvp_records']:
            setattr(self.dao, attr, MagicMock())
        self.result = self.dao.save_kvps(kvps=self.kvps)

    def test_converts_kvps_to_kvp_records(self):
        self.assertEqual(self.dao._kvp_to_kvp_record.call_args_list,
                         [call(kvp=kvp) for kvp in self.kvps])

    def test_saves_kvp_records(self):
        self.assertEqual(
            self.dao._save_kvp_records.call_args,
            call(kvp_records=[self.dao._kvp_to_kvp_record.return_value
                              for kvp in self.kvps])
        )

class _KvpToKvpRecordTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.dao._serialize_value = MagicMock()
        self.kvp = MagicMock()
        self.result = self.dao._kvp_to_kvp_record(kvp=self.kvp)

    def test_copies_expected_fielsd(self):
        expected_values = {field: self.kvp.get(field)
                           for field in self.dao.KVP_RECORD_FIELDS['to_copy']}
        actual_values = {field: self.result[field]
                         for field in self.dao.KVP_RECORD_FIELDS['to_copy']}
        self.assertEqual(actual_values, expected_values)

    def test_serializes_expected_fields(self):
        expected_values = {
            field: self.dao._serialize_value(self.kvp.get(field, {}))
            for field in self.dao.KVP_RECORD_FIELDS['to_serialize']
        }
        actual_values = {
            field: self.result[field]
            for field in self.dao.KVP_RECORD_FIELDS['to_serialize']
        }
        self.assertEqual(actual_values, expected_values)

class SaveKvpRecordsTestCase(BaseTestCase):
    def test_dispatches_to__save_kvp_record(self):
        self.fail()

class _SaveKvpRecordTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.dao.generate_timestamp = MagicMock()

    def test_ensures_created_timestamp(self):
        record_w_created = defaultdict(MagicMock, {'created': 'some_created'})
        result_for_w_created = self.dao._save_kvp_record(
            kvp_record=record_w_created)
        self.assertEqual(record_w_created['created'],
                         result_for_w_created['created'])

        record_sans_created = defaultdict(MagicMock)
        result_for_sans_created = self.dao._save_kvp_record(
            kvp_record=record_sans_created)
        self.assertEqual(result_for_sans_created['created'],
                         self.dao.generate_timestamp.return_value)

    def test_updates_modified_timestamp(self):
        record = defaultdict(MagicMock)
        result = self.dao._save_kvp_record(kvp_record=record)
        self.assertEqual(result['modified'],
                         self.dao.generate_timestamp.return_value)

    def test_writes_record_to_db(self):
        kvp_record = defaultdict(MagicMock)
        result = self.dao._save_kvp_record(kvp_record=kvp_record)
        expected_statement = textwrap.dedent(
            '''
            INSERT OR REPLACE INTO kvps
                ({csv_fields})
                VALUES ({csv_placeholders})
            '''
        ).strip().format(
            csv_fields=(','.join(self.dao.KVP_RECORD_FIELDS['all'])),
            csv_placeholders=(','.join(['?' for field in
                                        self.dao.KVP_RECORD_FIELDS['all']])),
        )
        expected_statement_args = [result.get(field) for field in
                                   self.dao.KVP_RECORD_FIELDS['all']]
        self.assertEqual(self.dao.connection.execute.call_args,
                         call(expected_statement, expected_statement_args))

class GetKvpsTestCase(BaseTestCase):
    def test_gets_kvp_records(self):
        self.fail()

    def test_converts_kvp_records_to_kvps(self):
        self.fail()

class GetKvpRecordsTestCase(BaseTestCase):
    def test_executes_query(self):
        self.fail()

class _KvpRecordToKvpTestCase(BaseTestCase):
    def test_copies_expected_fields(self):
        self.fail()

    def test_serializes_expected_fields(self):
        self.fail()
