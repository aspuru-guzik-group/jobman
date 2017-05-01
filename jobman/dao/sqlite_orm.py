import json
import textwrap


class SqliteORM(object):
    def __init__(self, name=None, fields=None, json=json):
        self.name = name
        self.fields = fields

    def create_table(self, connection=None):
        create_statement = 'CREATE TABLE {table} ({column_defs})'.format(
            table=self.name,
            column_defs=(
                ",\n".join([
                    self._generate_column_def(field=field, field_def=field_def)
                    for field, field_def in self.fields.items()])
            )
        )
        connection.execute(create_statement)

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
        if field_type == 'JSON': column_type = 'TEXT'
        return column_type

    def save_obj(self, obj=None, connection=None):
        self._save_record(record=self._obj_to_record(obj=obj),
                          connection=connection)

    def _obj_to_record(self, obj=None, fields=None):
        record = {}
        for field, field_def in self.fields.items():
            value = obj.get(field)
            if field_def.get('type') == 'JSON':
                value = self._serialize_json_value(value)
            record[field] = value
        return record

    def _serialize_json_value(self, value=None):
        return self.json.dumps(value)

    def _save_record(self, record=None, connection=None):
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
                                       connection=connection)
        return record

    def execute_insert_or_replace(self, fields=None, values=None,
                                  connection=None):
        statement = textwrap.dedent(
            '''
            INSERT OR REPLACE INTO {table} ({csv_fields})
            VALUES ({csv_placeholders})
            '''
        ).strip().format(
            table=self.name,
            csv_fields=(','.join(fields)),
            csv_placeholders=(','.join(['?' for field in fields]))
        )
        connection.execute(statement, values)

    def get_objects(self, query=None, connection=None):
        self._validate_query(query=query)
        records = self._get_records(query=query, connection=connection)
        return [self._record_to_obj(record=record) for record in records]

    def _validate_query(self, query=None):
        filterable_fields = self._get_filterable_fields()
        for _filter in (query or {}).get('filters', []):
            if _filter['field'] not in filterable_fields:
                raise Exception(
                    "Unkown filter field '{field}'.".format(_filter['field']))

    def _get_records(self, query=None, connection=None):
        return self.execute_query(query=query, connection=connection)

    def execute_query(self, query=None, connection=None):
        args = []
        statement = 'SELECT {fields} FROM {table}'.format( 
            fields=query.get('fields', '*'),
            table=self.name
        )
        where_clauses = []
        for _filter in ('filters' or []):
            where_clauses.append(self._filter_to_where_clause(_filter=_filter))
            args.append(_filter['value'])
        if where_clauses:
            statement += '\nWHERE ' + ' AND '.join(where_clauses)
        return connection.execute(statement, args)

    def _get_filterable_fields(self):
        return [field for field, field_def in self.fields.items()
                if field_def.get('filterable')]

    
    def _filter_to_where_clause(self, _filter=None):
        return ''.join([_filter['field'], _filter['operator'], '?'])


    def _record_to_obj(self, record=None, fields=None):
        obj = {}
        for field, field_def in self.fields.items():
            value = record.get(field)
            if field_def['type'] == 'JSON':
                value = self._deserialize_json_value(value)
            obj[field] = value

    def _deserialize_json_value(self, serialized_value=None):
        if serialized_value is None or serialized_value == '': return None
        return self.json.loads(serialized_value)

