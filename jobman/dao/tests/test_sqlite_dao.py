import unittest
from unittest.mock import call, MagicMock

from .. import sqlite_dao


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.db_uri = ':memory:'
        self.dao = self._generate_dao()

    def _generate_dao(self, **kwargs):
        merged_kwargs = {
            'ensure_db': False,
            'db_uri': self.db_uri,
            'sqlite': MagicMock(),
            'orm': MagicMock(),
            'table_prefix': 'some_table_prefix',
            **kwargs
        }
        return sqlite_dao.SqliteDAO(**merged_kwargs)


class InitTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.orm = MagicMock()
        self.dao = self._generate_dao(orm=self.orm)

    def test_generates_orms(self):
        self.assertEqual(
            self.orm.ORM.call_args_list,
            [
                call(
                    name='kvp',
                    fields=self.dao._generate_kvp_fields(),
                    logger=self.dao.logger,
                    table_prefix=self.dao.table_prefix
                )
            ]
        )
        expected_orms = {'kvp': self.orm.ORM.return_value}
        self.assertEqual(self.dao.orms, expected_orms)


class CreateDbTestCase(BaseTestCase):
    def test_dispatches_to_orms(self):
        self.dao.ensure_db()
        for orm in self.dao.orms.values():
            self.assertEqual(orm.create_table.call_args,
                             call(connection=self.dao.connection))


class ConnectionGetterTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.dao.create_connection = MagicMock()

    def test_creates_connection_if_not_set(self):
        self.assertEqual(self.dao.connection,
                         self.dao.create_connection.return_value)

    def test_does_not_creates_connection_if_already_set(self):
        self.dao.connection
        self.dao.connection
        self.assertEqual(len(self.dao.create_connection.call_args_list), 1)


class CreateConnectionTestCase(BaseTestCase):
    def test_gets_connection(self):
        result = self.dao.create_connection()
        self.assertEqual(self.dao.sqlite.connect.call_args,
                         call(self.dao.db_uri))
        self.assertEqual(result, self.dao.sqlite.connect.return_value)

    def test_connection_has_row_factory(self):
        result = self.dao.create_connection()
        self.assertEqual(result.row_factory, self.dao.sqlite.Row)


class CreateEntTestCase(BaseTestCase):
    def test_dispatches_to_save_ents(self):
        self.dao.save_ents = MagicMock()
        ent = MagicMock()
        ent_type = MagicMock()
        result = self.dao.create_ent(ent_type=ent_type, ent=ent)
        self.assertEqual(
            self.dao.save_ents.call_args,
            call(ent_type=ent_type, ents=[ent])
        )
        self.assertEqual(result, self.dao.save_ents.return_value[0])


class SaveEntsTestCase(BaseTestCase):
    def test_dispatches_to_orm(self):
        ent_type = 'some_ent_type'
        self.dao.orms[ent_type] = MagicMock()
        ents = [MagicMock() for i in range(3)]
        self.dao.save_ents(ent_type=ent_type, ents=ents)
        self.assertEqual(
            self.dao.orms[ent_type].save_object.call_args_list,
            [call(obj=ent, connection=self.dao.connection, replace=True)
             for ent in ents]
        )


class QueryEntsTestCase(BaseTestCase):
    def test_dispatches_to_orm(self):
        ent_type = 'some_ent_type'
        query = MagicMock()
        self.dao.orms[ent_type] = MagicMock()
        result = self.dao.query_ents(ent_type=ent_type, query=query)
        self.assertEqual(self.dao.orms[ent_type].query_objects.call_args,
                         call(query=query, connection=self.dao.connection))
        self.assertEqual(
            result, self.dao.orms[ent_type].query_objects.return_value)


class SaveKvpsTestCase(BaseTestCase):
    def test_dispatches_to_orm(self):
        kvps = [MagicMock() for i in range(3)]
        self.dao.save_kvps(kvps=kvps)
        self.assertEqual(
            self.dao.orms['kvp'].save_object.call_args_list,
            [call(obj=kvp, connection=self.dao.connection, replace=True)
             for kvp in kvps]
        )


class QueryKvpsTestCase(BaseTestCase):
    def test_dispatches_to_orm(self):
        query = MagicMock()
        result = self.dao.query_kvps(query=query)
        self.assertEqual(self.dao.orms['kvp'].query_objects.call_args,
                         call(query=query, connection=self.dao.connection))
        self.assertEqual(
            result, self.dao.orms['kvp'].query_objects.return_value)
