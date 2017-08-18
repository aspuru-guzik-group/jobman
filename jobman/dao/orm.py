import json
import logging
import textwrap


class ORM(object):
    class UpdateError(Exception):
        pass

    class InsertError(Exception):
        pass

    def __init__(self, name=None, fields=None, json=json, table_prefix=None,
                 logger=None):
        self.name = name
        self.fields = fields
        self.json = json
        self.table_prefix = table_prefix
        self.logger = logger or logging

    def create_table(self, connection=None):
        create_statement = (
            'CREATE TABLE IF NOT EXISTS {table} ({column_defs})'
        ).format(
            table=self.table,
            column_defs=(
                ",\n".join([
                    self._generate_column_def(field=field, field_def=field_def)
                    for field, field_def in self.fields.items()])
            )
        )
        connection.execute(create_statement)

    @property
    def table(self):
        table_prefix = self.table_prefix or ''
        return table_prefix + self.name

    def _generate_column_def(self, field=None, field_def=None):
        column_def = '{field} {type}'.format(
            field=field,
            type=self._get_column_type(field_type=field_def['type'])
        )
        if field_def.get('primary_key'):
            column_def += ' PRIMARY KEY'
        return column_def

    def _get_column_type(self, field_type=None):
        column_type = field_type
        if field_type == 'JSON':
            column_type = 'TEXT'
        return column_type

    def save_object(self, obj=None, connection=None, replace=True):
        saved_record = self._save_record(record=self._obj_to_record(obj=obj),
                                         connection=connection,
                                         replace=replace)
        return self._record_to_obj(record=saved_record)

    def _obj_to_record(self, obj=None, fields=None):
        record = {}
        for field, field_def in self.fields.items():
            record[field] = self._obj_val_to_record_val(
                field_def=field_def, value=obj.get(field))
        return record

    def _obj_val_to_record_val(self, field_def=None, value=None):
        if field_def.get('type') == 'JSON':
            value = self._serialize_json_value(value)
        return value

    def _serialize_json_value(self, value=None):
        return self.json.dumps(value)

    def _save_record(self, record=None, connection=None, replace=None):
        fields = sorted(self.fields.keys())
        values = []
        for field in fields:
            field_def = self.fields[field]
            if field_def.get('default') and record.get(field) is None:
                record[field] = field_def['default']()
            if field_def.get('auto_update'):
                record[field] = field_def['auto_update'](record=record)
            values.append(record.get(field))
        self.execute_insert_or_replace(fields=fields, values=values,
                                       replace=replace, connection=connection)
        return record

    def execute_insert_or_replace(self, fields=None, values=None, replace=None,
                                  connection=None):
        replace_sql = ''
        if replace:
            replace_sql = 'OR REPLACE'
        statement = textwrap.dedent(
            '''
            INSERT {replace_sql} INTO {table} ({csv_fields})
            VALUES ({csv_placeholders})
            '''
        ).strip().format(
            replace_sql=replace_sql,
            table=self.table,
            csv_fields=(','.join(fields)),
            csv_placeholders=(','.join(['?' for field in fields]))
        )
        try:
            connection.execute(statement, values)
        except Exception as exc:
            raise self.InsertError() from exc

    def query_objects(self, query=None, connection=None):
        query = query or {}
        self._validate_query(query=query)
        records = self._query_records(query=query, connection=connection)
        return [self._record_to_obj(record=record) for record in records]

    def _validate_query(self, query=None):
        filterable_fields = self._get_filterable_fields()
        for _filter in (query or {}).get('filters', []):
            if _filter['field'] not in filterable_fields:
                raise Exception("Unknown filter field '{field}'.".format(
                    field=_filter['field']))

    def _get_filterable_fields(self):
        return [field for field, field_def in self.fields.items()
                if not field_def.get('unfilterable')]

    def _query_records(self, query=None, connection=None):
        return self._execute_query(query=query, connection=connection)

    def _execute_query(self, query=None, connection=None):
        args = []
        statement = 'SELECT {fields} FROM {table}'.format(
            fields=query.get('fields', '*'),
            table=self.table
        )
        where_section = self._get_where_section(query=query)
        if where_section.get('content'):
            statement += '\nWHERE ' + where_section['content']
            args.extend(where_section['args'])
        return connection.execute(statement, args)

    def _get_where_section(self, query=None):
        clauses = []
        args = []
        for _filter in query.get('filters', []):
            where_item = self._filter_to_where_item(_filter=_filter)
            clauses.append(where_item['clause'])
            args.extend(where_item['args'])
        return {'content': ' AND '.join(clauses), 'args': args}

    def _filter_to_where_item(self, _filter=None):
        op = _filter['op']
        negation = ''
        if op.startswith('!'):
            negation = 'NOT'
            op = op.lstrip('! ')
        if op == 'IN':
            args = _filter.get('arg', [])
            clause_rhs = '({placeholders})'.format(
                placeholders=(', '.join(['?' for v in args]))
            )
        else:
            args = [_filter['arg']]
            clause_rhs = '?'
        where_item = {
            'clause': '{negation} {field} {op} {rhs}'.format(
                negation=negation,
                field=_filter['field'],
                op=op,
                rhs=clause_rhs,
            ).lstrip(),
            'args': self._format_args(args=args, _filter=_filter)
        }
        # Hack for handling != filters for null values.
        # NOT = value will return true if value is null :/
        if op == '=' and negation:
            where_item['clause'] = '({clause} OR {field} IS NULL)'.format(
                clause=where_item['clause'],
                field=_filter['field'])
        return where_item

    def _format_args(self, args=None, _filter=None):
        return [
            self._format_value_for_field(value=arg, field_key=_filter['field'])
            for arg in args
        ]

    def _format_value_for_field(self, value=None, field_key=None):
        field = self.fields[field_key]
        if field['type'] == 'JSON':
            value = self._serialize_json_value(value)
        return value

    def _get_field_for_filter(self, _filter=None):
        return self.fields[_filter['field']]

    def _record_to_obj(self, record=None):
        obj = {}
        for field, field_def in self.fields.items():
            obj[field] = self._record_val_to_obj_val(field_def=field_def,
                                                     value=record[field])
        return obj

    def _record_val_to_obj_val(self, field_def=None, value=None):
        if field_def.get('type') == 'JSON':
            value = self._deserialize_json_value(value)
        return value

    def _deserialize_json_value(self, serialized_value=None):
        if serialized_value is None or serialized_value == '':
            return None
        return self.json.loads(serialized_value)

    def update_objects(self, updates=None, query=None, connection=None):
        self._validate_query(query=query)
        result = self._update_records(updates=updates, query=query,
                                      connection=connection)
        return result

    def _update_records(self, updates=None, query=None, connection=None):
        args = []
        updates_section = self._get_updates_section(updates=updates)
        args.extend(updates_section['args'])
        where_section = self._get_where_section(query=query)
        args.extend(where_section['args'])
        where_content = where_section['content']
        if where_content:
            where_content = ' WHERE ' + where_content
        statement = textwrap.dedent(
            '''
            UPDATE {table}
            SET {updates_content}
            {where_content}
            '''
        ).lstrip().format(
            table=self.table,
            updates_content=updates_section['content'],
            where_content=where_content,
        )
        try:
            cursor = connection.execute(statement, args)
            return {'rowcount': cursor.rowcount}
        except Exception as exc:
            raise self.UpdateError() from exc

    def _get_updates_section(self, updates=None):
        set_items = []
        args = []
        for field_key, value in updates.items():
            set_items.append('%s = ?' % field_key)
            args.append(self._format_value_for_field(value=value,
                                                     field_key=field_key))
        return {'content': ', '.join(set_items), 'args': args}
