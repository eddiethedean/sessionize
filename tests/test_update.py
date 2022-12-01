import unittest

import sqlalchemy.orm.session as sa_session

from setup_test import sqlite_setup, postgres_setup
from sessionize.utils.features import get_table
from sessionize.utils.select import select_records
from sessionize.exceptions import ForceFail
from sessionize.utils.update import update_records_session


# update_df_session
class TestUpdateRecords(unittest.TestCase):
    def update_records(self, setup_function, schema=None):
        """
        Test that update_record_sesssion works
        """
        engine, tbl1, tbl2 = setup_function(schema=schema)
        table = get_table('people', engine, schema=schema)

        new_ages = [
            {'id': 2, 'name': 'Liam', 'age': 19},
            {'id': 3, 'name': 'Emma', 'age': 20}
        ]
        
        session = sa_session.Session(engine)
        update_records_session(table, new_ages, session, schema=schema)
        session.commit()

        expected = [
            {'id': 1, 'name': 'Olivia', 'age': 17, 'address_id': 1},
            {'id': 2, 'name': 'Liam', 'age': 19, 'address_id': 1},
            {'id': 3, 'name': 'Emma', 'age': 20, 'address_id': 2},
            {'id': 4, 'name': 'Noah', 'age': 20, 'address_id': 2},
        ]

        results = select_records(table, engine, schema=schema, sorted=True)
        self.assertEqual(results, expected)

    def test_update_records_sqlite(self):
        self.update_records(sqlite_setup)

    def test_update_records_postgres(self):
        self.update_records(postgres_setup)

    def test_update_records_schema(self):
        self.update_records(postgres_setup, schema='local')