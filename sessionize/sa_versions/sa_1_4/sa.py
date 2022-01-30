"""
All SqlAlchemy functionality used in Sessionize is defined here.
"""
from typing import Any, Union

# External dependencies
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import Table, Column, MetaData
from sqlalchemy.exc import NoSuchTableError, OperationalError, ProgrammingError
from sqlalchemy import VARCHAR, INTEGER
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.orm.session import sessionmaker, Session
from sqlalchemy import sql, inspect


Record = dict[str, Any]
SqlConnection = Union[Engine, Session, Connection]

from sessionize.sa_versions.sa_1.sa import SqlAlchemy as SqlAlchemy1

class SqlAlchemy(SqlAlchemy1):
    __version__ = '1.4'

