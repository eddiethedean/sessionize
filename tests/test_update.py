import unittest

from sqlalchemy.orm import Session

from sessionize.setup_test import sqlite_setup, postgres_setup
from sessionize.utils.select import select_records
from sessionize.exceptions import ForceFail
from sessionize.utils.update import update_records_session


# update_df_session
class TestUpdateRecords(unittest.TestCase):
    def update_records(self, setup_function):
        """
        Test that update_record_sesssion works
        """
        engine, table = setup_function()

        new_ages = [
            {'id': 2, 'name': 'Liam', 'age': 19},
            {'id': 3, 'name': 'Emma', 'age': 20}
        ]
        
        with Session(engine) as session, session.begin():
            update_records_session(table, new_ages, session)

        expected = [
            {'id': 1, 'name': 'Olivia', 'age': 17},
            {'id': 2, 'name': 'Liam', 'age': 19},
            {'id': 3, 'name': 'Emma', 'age': 20},
            {'id': 4, 'name': 'Noah', 'age': 20},
        ]

        results = select_records(table, engine)
        self.assertEqual(results, expected)

    def test_update_records_sqlite(self):
        self.update_records(sqlite_setup)

    def test_update_records_postgres(self):
        self.update_records(postgres_setup)