from collections import defaultdict
import textwrap
import unittest
from unittest.mock import call, MagicMock

from .. import orm


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
        return orm.ORM(**merged_kwargs)


class CreateTableTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.orm._generate_column_def = MagicMock(return_value='column_def')
        self.orm.fields = {i: MagicMock() for i in range(3)}

    def test_creates_table(self):
        self.orm.create_table(connection=self.connection)
        expected_statement = (
            'CREATE TABLE IF NOT EXISTS {table} ({column_defs})'
        ).format(
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


class SaveObjectTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        for attr in ['_obj_to_record', '_save_record']:
            setattr(self.orm, attr, MagicMock())
        self.obj = MagicMock()
        self.replace = MagicMock()
        self.orm.save_object(obj=self.obj, replace=self.replace,
                             connection=self.connection)

    def test_converts_to_record_and_saves(self):
        self.assertEqual(self.orm._obj_to_record.call_args,
                         call(obj=self.obj))
        self.assertEqual(
            self.orm._save_record.call_args,
            call(record=self.orm._obj_to_record.return_value,
                 connection=self.connection, replace=self.replace)
        )


class _ObjToRecordTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.obj = MagicMock()
        self.orm._obj_val_to_record_val = MagicMock()

    def _obj_to_record(self):
        return self.orm._obj_to_record(obj=self.obj)

    def test_transforms_values(self):
        self.orm.fields = {i: MagicMock() for i in range(3)}
        expected_record = {
            field: self.orm._obj_val_to_record_val(field_def=field_def,
                                                   value=self.obj.get(field))
            for field, field_def in self.orm.fields.items()
        }
        record = self._obj_to_record()
        self.assertEqual(record, expected_record)


class _ObjValToRecordValTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.field_def = defaultdict(MagicMock)
        self.value = MagicMock()

    def _obj_val_to_record_val(self):
        return self.orm._obj_val_to_record_val(field_def=self.field_def,
                                               value=self.value)

    def test_serializes_json_field_values(self):
        self.field_def['type'] = 'JSON'
        self.orm._serialize_json_value = MagicMock()
        val = self._obj_val_to_record_val()
        self.assertEqual(val, self.orm._serialize_json_value.return_value)

    def test_passes_through_other_fields(self):
        val = self._obj_val_to_record_val()
        self.assertEqual(val, self.value)


class _SaveRecordTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.orm.execute_insert_or_replace = MagicMock()
        self.replace = MagicMock()
        self.record = defaultdict(MagicMock)

    def _save_record(self):
        return self.orm._save_record(record=self.record, replace=self.replace,
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
        self.assertEqual(
            self.orm.execute_insert_or_replace.call_args,
            call(fields=expected_fields, values=expected_values,
                 replace=self.replace, connection=self.connection)
        )

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
        self.assertEqual(
            self.orm.execute_insert_or_replace.call_args,
            call(fields=expected_fields, values=expected_values,
                 replace=self.replace, connection=self.connection)
        )

    def test_returns_record(self):
        result = self._save_record()
        self.assertEqual(result, self.record)


class ExecuteInsertOrReplaceTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.fields = ["field_%s" % i for i in range(3)]
        self.values = [MagicMock() for i in range(3)]

    def _execute_insert_or_replace(self, replace=None):
        return self.orm.execute_insert_or_replace(
            fields=self.fields, values=self.values, replace=replace,
            connection=self.connection)

    def test_executes_insert_if_no_replace(self):
        replace = None
        self._execute_insert_or_replace(replace=replace)
        self._assert_execution(replace=replace)

    def _assert_execution(self, replace=None):
        replace_sql = ''
        if replace:
            replace_sql = 'OR REPLACE'
        expected_statement = textwrap.dedent(
            '''
            INSERT {replace_sql} INTO {table} ({csv_fields})
            VALUES ({csv_placeholders})
            '''
        ).strip().format(
            replace_sql=replace_sql,
            table=self.orm.name,
            csv_fields=(','.join(self.fields)),
            csv_placeholders=(','.join(['?' for field in self.fields]))
        )
        self.assertEqual(self.connection.execute.call_args,
                         call(expected_statement, self.values))

    def test_executes_insert_or_replace_if_replace(self):
        replace = True
        self._execute_insert_or_replace(replace=replace)
        self._assert_execution(replace=replace)


class QueryObjectsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        for attr in ['_validate_query', '_query_records', '_record_to_obj']:
            setattr(self.orm, attr, MagicMock())
        self.orm._query_records.return_value = [MagicMock() for i in range(3)]
        self.query = MagicMock()
        self.result = self.orm.query_objects(
            query=self.query, connection=self.connection)

    def test_validates_query(self):
        self.assertEqual(self.orm._validate_query.call_args,
                         call(query=self.query))

    def test_gets_records(self):
        self.assertEqual(self.orm._query_records.call_args,
                         call(query=self.query, connection=self.connection))

    def test_returns_converted_job_records(self):
        self.assertEqual(
            self.orm._record_to_obj.call_args_list,
            [call(record=record)
             for record in self.orm._query_records.return_value]
        )
        self.assertEqual(
            self.result,
            [self.orm._record_to_obj.return_value
             for record in self.orm._query_records.return_value]
        )


class _ExecuteQueryTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.orm._get_where_section = MagicMock(return_value={
            'content': 'some_where_content',
            'args': [MagicMock() for i in range(3)]
        })
        self.query = {'filters': [MagicMock() for i in range(3)]}

    def _execute_query(self):
        return self.orm._execute_query(query=self.query,
                                       connection=self.connection)

    def test_executes_query_w_filters(self):
        self._execute_query()
        expected_where_section = self.orm._get_where_section(query=self.query)
        expected_statement = textwrap.dedent(
            '''
            SELECT {fields} FROM {table}
            WHERE {where_content}
            '''
        ).strip().format(
            table=self.orm.name,
            fields=self.query.get('fields', '*'),
            where_content=expected_where_section['content']
        )
        expected_args = expected_where_section['args']
        self.assertEqual(self.connection.execute.call_args,
                         call(expected_statement, expected_args))

    def test_returns_query_results(self):
        result = self._execute_query()
        self.assertEqual(result, self.connection.execute.return_value)


class _GetWhereSectionTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.orm._filter_to_where_item = MagicMock(return_value={
            'clause': 'some_clause',
            'args': [1, 2, 3]
        })
        self.query = {'filters': [MagicMock() for i in range(3)]}

    def _get_where_section(self):
        return self.orm._get_where_section(query=self.query)

    def test_generates_expected_where_section(self):
        where_section = self._get_where_section()
        expected_clauses = []
        expected_args = []
        for _filter in self.query['filters']:
            where_item = self.orm._filter_to_where_item(_filter=_filter)
            expected_clauses.append(where_item['clause'])
            expected_args.extend(where_item['args'])
        expected_where_section = {
            'content': ' AND '.join(expected_clauses),
            'args': expected_args,
        }
        self.assertEqual(where_section, expected_where_section)


class _FilterToWhereItemTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.filter = {'field': 'field', 'op': 'op', 'arg': 'some_arg'}

    def _filter_to_where_item(self):
        return self.orm._filter_to_where_item(_filter=self.filter)

    def test_returns_expected_item(self):
        result = self._filter_to_where_item()
        expected_item = {
            'clause': '{field} {op} ?'.format(**self.filter),
            'args': [self.filter['arg']]
        }
        self.assertEqual(result, expected_item)

    def test_handles_negation(self):
        self.filter['op'] = '! ' + self.filter['op']
        result = self._filter_to_where_item()
        expected_item = {
            'clause': 'NOT {field} {op} ?'.format(
                field=self.filter['field'],
                op=self.filter['op'].lstrip('! '),
            ),
            'args': [self.filter['arg']]
        }
        self.assertEqual(result, expected_item)

    def test_handles_in_op(self):
        self.filter['op'] = 'IN'
        self.filter['arg'] = [1, 2, 3]
        result = self._filter_to_where_item()
        expected_item = {
            'clause': '{field} IN ({placeholders})'.format(
                field=self.filter['field'],
                placeholders=(', '.join(['?' for v in self.filter['arg']]))
            ),
            'args': self.filter['arg']
        }
        self.assertEqual(result, expected_item)

    def test_handles_negated_eq(self):
        self.filter['op'] = '! ='
        result = self._filter_to_where_item()
        expected_item = {
            'clause': '(NOT {field} {op} ? OR {field} IS NULL)'.format(
                field=self.filter['field'],
                op=self.filter['op'].lstrip('! '),
            ),
            'args': [self.filter['arg']]
        }
        self.assertEqual(result, expected_item)


class _RecordToObjTestCase(BaseTestCase):
    def test_transforms_values(self):
        self.orm._record_val_to_obj_val = MagicMock()
        self.orm.fields = {i: MagicMock() for i in range(3)}
        record = MagicMock()
        expected_obj = {
            field: self.orm._record_val_to_obj_val(field_def=field_def,
                                                   value=record[field])
            for field, field_def in self.orm.fields.items()
        }
        obj = self.orm._record_to_obj(record=record)
        self.assertEqual(obj, expected_obj)


class _RecordValToObjValTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.field_def = defaultdict(MagicMock)
        self.value = MagicMock()

    def _record_val_to_obj_val(self):
        return self.orm._record_val_to_obj_val(field_def=self.field_def,
                                               value=self.value)

    def test_deserializes_json_values(self):
        self.field_def['type'] = 'JSON'
        self.orm._deserialize_json_value = MagicMock()
        obj_val = self._record_val_to_obj_val()
        self.assertEqual(
            obj_val, self.orm._deserialize_json_value.return_value)

    def test_passes_through_other_values(self):
        obj_val = self._record_val_to_obj_val()
        self.assertEqual(obj_val, self.value)


class UpdateObjectsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        for attr in ['_validate_query', '_update_records']:
            setattr(self.orm, attr, MagicMock())
        self.updates = {'field_%s' % i: 'value_%s' % i for i in range(3)}
        self.query = MagicMock()
        self.result = self.orm.update_objects(updates=self.updates,
                                              query=self.query,
                                              connection=self.connection)

    def test_validates_query(self):
        self.assertEqual(self.orm._validate_query.call_args,
                         call(query=self.query))

    def test_updates_records(self):
        self.assertEqual(
            self.orm._update_records.call_args,
            call(updates=self.updates, query=self.query,
                 connection=self.connection)
        )

    def test_returns_update_result(self):
        self.assertEqual(self.result, self.orm._update_records.return_value)


class _UpdateRecordsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        for attr in ['_get_updates_section', '_get_where_section']:
            setattr(self.orm, attr, MagicMock())
        self.updates = MagicMock()
        self.query = MagicMock()
        self.connection = MagicMock()
        self.result = self._update_records()

    def _update_records(self):
        return self.orm._update_records(updates=self.updates,
                                        query=self.query,
                                        connection=self.connection)

    def test_gets_updates_section(self):
        self.assertEqual(self.orm._get_updates_section.call_args,
                         call(updates=self.updates))

    def test_gets_where_section(self):
        self.assertEqual(self.orm._get_where_section.call_args,
                         call(query=self.query))

    def test_executes_statement_w_args(self):
        updates_section = self.orm._get_updates_section.return_value
        where_section = self.orm._get_where_section.return_value
        where_content = ' WHERE ' + where_section['content']
        expected_statement = textwrap.dedent(
            '''
            UPDATE {table}
            SET {updates_content}
            {where_content}
            '''
        ).lstrip().format(
            table=self.orm.name,
            updates_content=updates_section['content'],
            where_content=where_content
        )
        expected_args = [*updates_section['args'], *where_section['args']]
        self.assertEqual(self.connection.execute.call_args,
                         call(expected_statement, expected_args))

    def test_returns_rowcount(self):
        self.assertEqual(
            self.result,
            {'rowcount': self.connection.execute.return_value.rowcount}
        )

    def test_raises_update_error_on_failure(self):
        self.connection.execute.side_effect = Exception
        with self.assertRaises(self.orm.UpdateError):
            self._update_records()


class _GetUpdatesSection(BaseTestCase):
    def test_returns_expected_result(self):
        updates = {'field_%s' % i: 'value_%s' % i for i in range(3)}
        expected_updates_section = {
            'content': ', '.join(['%s = ?' % k for k in updates]),
            'args': list(updates.values()),
        }
        self.assertEqual(self.orm._get_updates_section(updates=updates),
                         expected_updates_section)
