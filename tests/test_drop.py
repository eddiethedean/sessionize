import unittest

from sqlalchemy import inspect
from sqlalchemy.exc import NoSuchTableError

from sessionize.setup_test import sqlite_setup, postgres_setup
from sessionize.utils.drop import drop_table


class TestDropTable(unittest.TestCase):
    def drop_table(self, setup_function):
        engine, table = setup_function()

        drop_table(table.name, engine)

        table_names = inspect(engine).get_table_names()

        exists = table.name in table_names

        self.assertFalse(exists)

    def test_drop_table_sqlite(self):
        self.drop_table(sqlite_setup)

    def test_drop_table_postgres(self):
        self.drop_table(postgres_setup)

    def drop_table_fail(self, setup_function):
        engine, table = setup_function()
        with self.assertRaises(NoSuchTableError):
            drop_table('this_table_does_not_exist', engine, if_exists=False)

    def test_drop_table_fail_sqlite(self):
        self.drop_table_fail(sqlite_setup)

    def test_drop_table_fail_postgres(self):
        self.drop_table_fail(postgres_setup)

