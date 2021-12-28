import unittest

from sqlalchemy.orm import Session

from sessionize.setup_test import sqlite_setup, postgres_setup
from sessionize.utils.select import select_records
from sessionize.exceptions import ForceFail
from sessionize.utils.insert import insert_records_session
from sessionize.utils.update import update_records_session
from sessionize.utils.delete import delete_records_session


# TODO: insert & update & delete
# TODO: insert & delete & fail
# TODO: insert & update & delete & fail


class TestCombined(unittest.TestCase):
    def insert_update(self, setup_function):
        engine, table = setup_function()

        new_people = [
            {'name': 'Odos', 'age': 35},
            {'name': 'Kayla', 'age': 28}
        ]

        new_ages = [
            {'id': 2, 'name': 'Liam', 'age': 19},
            {'id': 3, 'name': 'Emma', 'age': 20}
        ]
        
        with Session(engine) as session, session.begin():
            insert_records_session(table, new_people, session)
            update_records_session(table, new_ages, session)

        expected = [
            {'id': 1, 'name': 'Olivia', 'age': 17},
            {'id': 2, 'name': 'Liam', 'age': 19},
            {'id': 3, 'name': 'Emma', 'age': 20},
            {'id': 4, 'name': 'Noah', 'age': 20},
            {'id': 5, 'name': 'Odos', 'age': 35},
            {'id': 6, 'name': 'Kayla', 'age': 28}
        ]

        results = select_records(table, engine)

        self.assertEqual(results, expected)

    def test_insert_update_sqlite(self):
        self.insert_update(sqlite_setup)

    def test_insert_update_psotgres(self):
        self.insert_update(postgres_setup)

    def delete_update_fail(self, setup_function):
        engine, table = setup_function()

        new_ages = [
            {'id': 2, 'name': 'Liam', 'age': 19},
            {'id': 3, 'name': 'Emma', 'age': 20}
        ]

        try:
            with Session(engine) as session, session.begin():
                delete_records_session(table, 'id', [2, 3], session)
                update_records_session(table, new_ages, session)
        except:
            pass

        expected = [
            {'id': 1, 'name': 'Olivia', 'age': 17},
            {'id': 2, 'name': 'Liam', 'age': 18},
            {'id': 3, 'name': 'Emma', 'age': 19},
            {'id': 4, 'name': 'Noah', 'age': 20},
        ]

        results = select_records(table, engine)

        self.assertEqual(results, expected)

    def test_delete_update_fail_sqlite(self):
        self.delete_update_fail(sqlite_setup)

    def test_delete_update_fail_postgres(self):
        self.delete_update_fail(postgres_setup)

    def update_delete(self, setup_function):
        engine, table = setup_function()

        new_ages = [
            {'id': 2, 'name': 'Liam', 'age': 19},
            {'id': 3, 'name': 'Emma', 'age': 20}
        ]

        with Session(engine) as session, session.begin():
            update_records_session(table, new_ages, session)
            delete_records_session(table, 'id', [2, 3], session)
            
        expected = [
            {'id': 1, 'name': 'Olivia', 'age': 17},
            {'id': 4, 'name': 'Noah', 'age': 20},
        ]

        results = select_records(table, engine)

        self.assertEqual(results, expected)

    def test_update_delete_sqlite(self):
        self.update_delete(sqlite_setup)

    def test_update_delete_postgres(self):
        self.update_delete(postgres_setup)

    def delete_insert_update(self, setup_function):
        engine, table = setup_function()

        new_people = [
            {'name': 'Odos', 'age': 35},
            {'name': 'Kayla', 'age': 28}
        ]

        new_ages = [
            {'id': 1, 'name': 'Olivia', 'age': 18},
            {'id': 4, 'name': 'Noah', 'age': 21}
        ]

        with Session(engine) as session, session.begin():
            delete_records_session(table, 'id', [2, 3], session)
            insert_records_session(table, new_people, session)
            update_records_session(table, new_ages, session)
            
        expected = [
            {'id': 1, 'name': 'Olivia', 'age': 18},
            {'id': 4, 'name': 'Noah', 'age': 21},
            {'id': 5, 'name': 'Odos', 'age': 35},
            {'id': 6, 'name': 'Kayla', 'age': 28}
        ]

        results = select_records(table, engine)

        self.assertEqual(results, expected)

    def test_delete_insert_update_sqlite(self):
        self.delete_insert_update(sqlite_setup)

    def test_delete_insert_update_postgres(self):
        self.delete_insert_update(postgres_setup)
