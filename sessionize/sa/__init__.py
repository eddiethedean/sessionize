
from sqlalchemy import __version__

version = '.'.join(__version__.split('.')[:-1])

if version == '1.4':
    from sessionize.sa_versions.sa_1_4.sa import SqlAlchemy, Session, Record, Engine, Column, Table, SqlConnection, sql, inspect
    from sessionize.sa_versions.sa_1_4.sa import NoSuchTableError, OperationalError, ProgrammingError, VARCHAR, INTEGER
    from sessionize.sa_versions.sa_1_4.setup_test import sqlite_setup, postgres_setup
    from sessionize.sa_versions.sa_1_4.type_convert import _type_convert

else:
    raise Exception('Sessionize only works with SqlAlchemy==1.4')

