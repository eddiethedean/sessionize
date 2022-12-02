from typing import Optional, Union

# TODO: replace with interfaces
from sqlalchemy import Table, Column
from sqlalchemy.engine import Engine
from sqlalchemy.orm.session import Session

import sessionize.utils.types as types
import sqlalchemize.features as features


def _get_table_name(
    table_name: Union[Table, str]
) -> str:
    if isinstance(table_name, Table):
        return table_name.name
    if isinstance(table_name, str):
        return table_name


def _get_table(
    sa_table: Union[str, Table],
    engine: Engine | Session,
    schema: Optional[str] = None
) -> Table:
    if isinstance(sa_table, Table):
        return sa_table
    if isinstance(sa_table, str):
        return get_table(sa_table, engine, schema=schema)


def primary_keys(sa_table: Table) -> list[str]:
    """
    Given SqlAlchemy Table, query database for
    columns with primary key constraint.
    Returns a list of column names.
    
    Parameters
    ----------
    sa_table: sa.Table
        SqlAlchemy table mapped to sql table.
    
    Returns
    -------
    list of primary key names.
    """
    return features.primary_key_names(sa_table)


def has_primary_key(sa_table: Table) -> bool:
    """
    Given a SqlAlchemy Table, query database to
    check for primary keys.
    Returns True if table has primary key,
    False if no primary key.
    
    Parameters
    ----------
    sa_table: sa.Table
        SqlAlchemy table mapped to sql table.
    
    Returns
    -------
    bool
    """
    return len(primary_keys(sa_table)) != 0


def get_table(
    table_name: str,
    connection: types.SqlConnection,
    schema: Optional[str] = None
) -> Table:
    """
    Maps a SqlAlchemy Table to a sql table.
    Returns SqlAlchemy Table object.
    
    Parameters
    ----------
    table_name: str
        name of sql table to map.
    connection: sa.engine.Engine, sa.orm.Session, or sa.engine.Connection
        connection used to query database.
    schema: str, default None
        Database schema name.
    
    Returns
    -------
    A SqlAlchemy mapped Table object.
    """
    return features.get_table(table_name, connection, schema)


def get_class(
    table_name: str,
    connection: types.SqlConnection,
    schema: Optional[str] = None
):
    """
    Maps a SqlAlchemy table class to a sql table.
    Returns the mapped class object.
    Some SqlAlchemy functions require the class
    instead of the table object.
    Will fail to map if sql table has no primary key.
    
    Parameters
    ----------
    table_name: str
        name of sql table to map.
    connection: sa.engine.Engine, sa.orm.Session, or sa.engine.Connection
        connection used to query database.
    schema: str, default None
        Database schema name.
    
    Returns
    -------
    A SqlAlchemy table class object.
    """
    return features.get_class(table_name, connection, schema)


def get_column(
    sa_table: Table,
    column_name: str
) -> Column:
    return features.get_column(sa_table, column_name)


def get_primary_key_constraints(
    sa_table: Table
) -> tuple[str, list[str]]:
    """
        Returns dictionary of primary key constraint names
        and list of column names per contraint.
    """
    return features.get_primary_key_constraints(sa_table)


def get_column_types(sa_table: Table):
    """Returns dict of table column names:sql_type
    """
    return features.get_column_types(sa_table)


def get_column_names(sa_table: Table) -> list[str]:
    return features.get_column_names(sa_table)


def get_row_count(sa_table: Table, session: types.SqlConnection) -> int:
    return features.get_row_count(sa_table, session)


def get_schemas(engine: Engine):
    return features.get_schemas(engine)