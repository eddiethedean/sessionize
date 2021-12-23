import unittest

from sqlalchemy.orm import Session

from sessionize.setup_test import sqlite_setup, postgres_setup, mysql_setup
from sessionize.utils.delete import delete_records_session
from sessionize.utils.insert import insert_records_session
from sessionize.utils.update import update_records_session
from sessionize.utils.select import select_records


class ForceFail(Exception):
    pass

# delete_record_session
class TestDeleteRecords(unittest.TestCase):

    def delete_records(self, setup_function):
        engine, table = setup_function()
        
        with Session(engine) as session, session.begin():
            delete_records_session(table, 'id', [2, 3], session)

        expected = [
            {'id': 1, 'name': 'Olivia', 'age': 17},
            {'id': 4, 'name': 'Noah', 'age': 20}
        ]

        results = select_records(table, engine)

        self.assertEqual(results, expected)

    def test_delete_records_sqlite(self):
        self.delete_records(sqlite_setup)

    def test_delete_records_postgres(self):
        self.delete_records(postgres_setup)

    def test_delete_records_mysql(self):
        self.delete_records(mysql_setup)

    def delete_records_session_fails(self, setup_function):
        engine, table = setup_function()
        
        try:
            with Session(engine) as session, session.begin():
                delete_records_session(table, 'id', [1, 2], session)
                raise ForceFail
        except ForceFail:
            pass

        expected = [
            {'id': 1, 'name': 'Olivia', 'age': 17},
            {'id': 2, 'name': 'Liam', 'age': 18},
            {'id': 3, 'name': 'Emma', 'age': 19},
            {'id': 4, 'name': 'Noah', 'age': 20},
        ]

        results = select_records(table, engine)

        self.assertEqual(results, expected)

    def test_delete_records_session_fails_sqlite(self):
        self.delete_records_session_fails(sqlite_setup)

    def test_delete_records_session_fails_postgres(self):
        self.delete_records_session_fails(postgres_setup)

    def test_delete_records_session_fails_mysql(self):
        self.delete_records_session_fails(mysql_setup)

# insert_df_session
class TestInsertRecords(unittest.TestCase):

    def insert_records(self, setup_function):
        engine, table = setup_function()

        new_people = [
            {'name': 'Odos', 'age': 35},
            {'name': 'Kayla', 'age': 28}
        ]
        
        with Session(engine) as session, session.begin():
            insert_records_session(table, new_people, session)

        expected = [
            {'id': 1, 'name': 'Olivia', 'age': 17},
            {'id': 2, 'name': 'Liam', 'age': 18},
            {'id': 3, 'name': 'Emma', 'age': 19},
            {'id': 4, 'name': 'Noah', 'age': 20},
            {'id': 5, 'name': 'Odos', 'age': 35},
            {'id': 6, 'name': 'Kayla', 'age': 28}
        ]

        results = select_records(table, engine)

        self.assertEqual(results, expected)

    def test_insert_records_sqlite(self):
        self.insert_records(sqlite_setup)

    def test_insert_records_postgres(self):
        self.insert_records(postgres_setup)

    def test_insert_records_mysql(self):
        self.insert_records(mysql_setup)

    def insert_records_session_fails(self, setup_function):
        engine, table = setup_function()

        new_people = [
            {'name': 'Odos', 'age': 35},
            {'name': 'Kayla', 'age': 28}
        ]
        
        try:
            with Session(engine) as session, session.begin():
                insert_records_session(table, new_people, session)
                raise ForceFail
        except ForceFail:
            pass

        expected = [
            {'id': 1, 'name': 'Olivia', 'age': 17},
            {'id': 2, 'name': 'Liam', 'age': 18},
            {'id': 3, 'name': 'Emma', 'age': 19},
            {'id': 4, 'name': 'Noah', 'age': 20},
        ]

        results = select_records(table, engine)

        self.assertEqual(results, expected)

    def test_insert_records_session_fails_sqlite(self):
        self.insert_records_session_fails(sqlite_setup)
    
    def test_insert_records_session_fails_postgres(self):
        self.insert_records_session_fails(postgres_setup)

    def test_insert_records_session_fails_mysql(self):
        self.insert_records_session_fails(mysql_setup)

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

    def test_update_records_mysql(self):
        self.update_records(mysql_setup)

    def insert_records_session_fails(self, setup_function):
        """
        Test that update_records_sesssion fails with error
        before session commits.
        """
        engine, table = setup_function()

        new_ages = [
            {'name': 'Liam', 'age': 19},
            {'name': 'Emma', 'age': 20}
        ]
        
        try:
            with Session(engine) as session, session.begin():
                insert_records_session(table, new_ages, session)
                raise ForceFail
        except ForceFail:
            pass

        expected = [
            {'id': 1, 'name': 'Olivia', 'age': 17},
            {'id': 2, 'name': 'Liam', 'age': 18},
            {'id': 3, 'name': 'Emma', 'age': 19},
            {'id': 4, 'name': 'Noah', 'age': 20},
        ]

        results = select_records(table, engine)

        self.assertEqual(results, expected)

    def test_insert_records_session_fails_sqlite(self):
        self.insert_records_session_fails(sqlite_setup)

    def test_insert_records_session_fails_postgres(self):
        self.insert_records_session_fails(postgres_setup)

    def test_insert_records_session_fails_mysql(self):
        self.insert_records_session_fails(mysql_setup)


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

    def test_insert_update_mysql(self):
        self.insert_update(mysql_setup)

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

    def test_delete_update_fail_mysql(self):
        self.delete_update_fail(mysql_setup)

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

    def test_update_delete_mysql(self):
        self.update_delete(mysql_setup)

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

    def test_delete_insert_update_mysql(self):
        self.delete_insert_update(mysql_setup)
