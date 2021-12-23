from typing import Optional, Union

import sqlalchemy as sa
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.sql.schema import PrimaryKeyConstraint


SqlConnection = Union[Engine, Session, Connection]


def primary_keys(table: sa.Table) -> list[str]:
    """
    Given SqlAlchemy Table, query database for
    columns with primary key constraint.
    Returns a list of column names.
    
    Parameters
    ----------
    table: sa.Table
        SqlAlchemy table mapped to sql table.
    
    Returns
    -------
    list of primary key names.
    """
    return [c.name for c in table.primary_key.columns.values()]


def has_primary_key(table: sa.Table) -> bool:
    """
    Given a SqlAlchemy Table, query database to
    check for primary keys.
    Returns True if table has primary key,
    False if no primary key.
    
    Parameters
    ----------
    table: sa.Table
        SqlAlchemy table mapped to sql table.
    
    Returns
    -------
    bool
    """
    return len(primary_keys(table)) != 0


def get_table(
    name: str,
    connection: SqlConnection,
    schema: Optional[str] = None
) -> sa.Table:
    """
    Maps a SqlAlchemy Table to a sql table.
    Returns SqlAlchemy Table object.
    
    Parameters
    ----------
    name: str
        name of sql table to map.
    connection: sa.engine.Engine, sa.orm.Session, or sa.engine.Connection
        connection used to query database.
    schema: str, default None
        Database schema name.
    
    Returns
    -------
    A SqlAlchemy mapped Table object.
    """
    metadata = sa.MetaData(bind=connection, schema=schema)
    
    if isinstance(connection, sa.orm.Session):
        autoload_with = connection.connection()
    else:
        autoload_with = connection
    
    return sa.Table(name,
                    metadata,
                    autoload=True,
                    autoload_with=autoload_with,
                    extend_existing=True,
                    schema=schema)


def get_class(
    name: str,
    connection: SqlConnection,
    schema: Optional[str] = None
) -> DeclarativeMeta:
    """
    Maps a SqlAlchemy table class to a sql table.
    Returns the mapped class object.
    Some SqlAlchemy functions require the class
    instead of the table object.
    Will fail to map if sql table has no primary key.
    
    Parameters
    ----------
    name: str
        name of sql table to map.
    connection: sa.engine.Engine, sa.orm.Session, or sa.engine.Connection
        connection used to query database.
    schema: str, default None
        Database schema name.
    
    Returns
    -------
    A SqlAlchemy table class object.
    """
    metadata = sa.MetaData(connection, schema=schema)
    if isinstance(connection, sa.orm.Session):
        reflect = connection.connection()
    else:
        reflect = connection
        
    metadata.reflect(reflect, only=[name])
    Base = automap_base(metadata=metadata)
    Base.prepare()
    return Base.classes[name]


def get_column(table: sa.Table, column_name: str) -> sa.Column:
    return table.c[column_name]


def get_primary_key_constraints(
    table: sa.Table
) -> tuple[str, list[str]]:
    """
        Returns dictionary of primary key constraint names
        and list of column names per contraint.
    """
    cons = table.constraints
    for con in cons:
        if isinstance(con, PrimaryKeyConstraint):
            return con.name, [col.name for col in con.columns]