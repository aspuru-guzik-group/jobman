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
            'db_uri': self.db_uri,
            'sqlite': MagicMock(),
            'orm': MagicMock(),
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
                call(name='job', fields=self.dao._generate_job_fields()),
                call(name='kvp', fields=self.dao._generate_kvp_fields()),
            ]
        )
        expected_orms = {'job': self.orm.ORM.return_value,
                         'kvp': self.orm.ORM.return_value}
        self.assertEqual(self.dao.orms, expected_orms)

class CreateDbTestCase(BaseTestCase):
    def test_dispatches_to_orms(self):
        self.dao.create_db()
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

class CreateJobTestCase(BaseTestCase):
    def test_dispatches_to_save_jobs(self):
        self.dao.save_jobs = MagicMock()
        job_kwargs = MagicMock()
        result = self.dao.create_job(job_kwargs=job_kwargs)
        self.assertEqual(self.dao.save_jobs.call_args, call(jobs=[job_kwargs]))
        self.assertEqual(result, self.dao.save_jobs.return_value[0])

class SaveJobsTestCase(BaseTestCase):
    def test_dispatches_to_orm(self):
        jobs = [MagicMock() for i in range(3)]
        self.dao.save_jobs(jobs=jobs)
        self.assertEqual(
            self.dao.orms['job'].save_object.call_args_list,
            [call(obj=job, connection=self.dao.connection)
             for job in jobs]
        )

class GetJobsTestCase(BaseTestCase):
    def test_dispatches_to_orm(self):
        query = MagicMock()
        result = self.dao.get_jobs(query=query)
        self.assertEqual(self.dao.orms['job'].get_objects.call_args,
                         call(query=query, connection=self.dao.connection))
        self.assertEqual(result, self.dao.orms['job'].get_objects.return_value)

class SaveKvpsTestCase(BaseTestCase):
    def test_dispatches_to_orm(self):
        kvps = [MagicMock() for i in range(3)]
        self.dao.save_kvps(kvps=kvps)
        self.assertEqual(
            self.dao.orms['kvp'].save_object.call_args_list,
            [call(obj=kvp, connection=self.dao.connection)
             for kvp in kvps]
        )

class GetKvpsTestCase(BaseTestCase):
    def test_dispatches_to_orm(self):
        query = MagicMock()
        result = self.dao.get_kvps(query=query)
        self.assertEqual(self.dao.orms['kvp'].get_objects.call_args,
                         call(query=query, connection=self.dao.connection))
        self.assertEqual(result, self.dao.orms['kvp'].get_objects.return_value)
