import unittest

import sqlalchemy as sa
import sqlalchemy.exc as sa_exc

from setup_test import sqlite_setup, postgres_setup
from sessionize.utils.drop import drop_table
from sessionize.utils.features import get_table


class TestDropTable(unittest.TestCase):
    def drop_table(self, setup_function, schema=None):
        engine, tbl1, tbl2 = setup_function(schema=schema)
        table = get_table('people', engine, schema=schema)

        drop_table(table.name, engine, schema=schema)

        table_names = sa.inspect(engine).get_table_names(schema=schema)

        exists = table.name in table_names

        self.assertFalse(exists)

    def test_drop_table_sqlite(self):
        self.drop_table(sqlite_setup)

    def test_drop_table_postgres(self):
        self.drop_table(postgres_setup)

    def test_drop_table_schema(self):
        self.drop_table(postgres_setup, schema='local')

    def drop_table_fail(self, setup_function, schema=None):
        engine, tbl1, tbl2 = setup_function(schema=schema)
        with self.assertRaises(sa_exc.NoSuchTableError):
            drop_table('this_table_does_not_exist', engine, if_exists=False, schema=schema)

    def test_drop_table_fail_sqlite(self):
        self.drop_table_fail(sqlite_setup)

    def test_drop_table_fail_postgres(self):
        self.drop_table_fail(postgres_setup)

    def test_drop_table_fail_schema(self):
        self.drop_table_fail(postgres_setup, schema='local')

