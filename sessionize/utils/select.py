
from typing import Optional, Any, Union, Generator

import sqlalchemy as sa
from sqlalchemy.sql import select

from sessionize.utils.sa_orm import get_column, get_table
from sessionize.utils.types import Record, SqlConnection


def select_records(
    table: Union[sa.Table, str],
    connection: SqlConnection,
    chunksize: Optional[int] = None
) -> Union[list[Record], Generator[list[Record], None, None]]:
    """
    Queries database for records in table.
    Returns list of records in sql table.
    Returns a generator of lists of records if chunksize is not None.
    
    Parameters
    ----------
    table: sa.Table
        SqlAlchemy table mapped to sql table.
    connection: sa.engine.Engine, sa.orm.Session, or sa.engine.Connection
        connection used to query database.
    chunksize: int, default None
        if not None, returns generator of lists of records.
    
    Returns
    -------
    list of sql table records or generator of lists of records.
    """
    if isinstance(table, str):
        table = get_table(table, connection)
    if chunksize is None:
        return select_records_all(table, connection)
    else:
        return select_records_chunks(table, connection, chunksize)


def select_records_all(
    table: Union[sa.Table, str],
    connection: SqlConnection
) -> list[Record]:
    """
    Queries database for records in table.
    Returns list of records in sql table.
    
    Parameters
    ----------
    table: sa.Table
        SqlAlchemy table mapped to sql table.
    connection: sa.engine.Engine, sa.orm.Session, or sa.engine.Connection
        connection used to query database.
    
    Returns
    -------
    list of sql table records.
    """
    if isinstance(table, str):
        table = get_table(table, connection)
    query = select(table).order_by(*table.primary_key.columns.values())
    results = connection.execute(query)
    return [dict(r) for r in results]


def select_records_chunks(
    table: Union[sa.Table, str],
    connection: SqlConnection,
    chunksize: int = 2
) -> Generator[list[Record], None, None]:
    """
    Queries database for records in table.
    Returns a generator of chunksized lists of sql table records.
    
    Parameters
    ----------
    table: sa.Table
        SqlAlchemy table mapped to sql table.
    connection: sa.engine.Engine, sa.orm.Session, or sa.engine.Connection
        connection used to query database.
    chunksize: int
        size of lists of sql records generated.
    
    Returns
    -------
    Generator of lists of sql table records.
    """
    if isinstance(table, str):
        table = get_table(table, connection)
    query = select(table).order_by(*table.primary_key.columns.values())
    stream = connection.execute(query, execution_options={'stream_results': True})
    for results in stream.partitions(chunksize):
        yield [dict(r) for r in results]


def select_existing_values(
    table: Union[sa.Table, str],
    column_name: str,
    values: list,
    conection: SqlConnection
) -> list:
    """
    Queries database for existing values in table column.
    Returns list of matching values that exist in table column.
    
    Parameters
    ----------
    table: sa.Table
        SqlAlchemy table mapped to sql table.
    records: list[Record]
        list of records to select from.
        Use df.to_dict('records') to convert Pandas DataFrame to records.
    connection: sa.engine.Engine, sa.orm.Session, or sa.engine.Connection
        connection used to query database
    
    Returns
    -------
    List of matching values.
    """
    if isinstance(table, str):
        table = get_table(table, conection)
    column = get_column(table, column_name)
    query = select([column]).where(column.in_(values))
    return conection.execute(query).scalars().fetchall()


def select_column_values(
    table: Union[sa.Table, str],
    column_name: str,
    connection: SqlConnection,
    chunksize: Optional[int] = None
) -> Union[list, Generator[list, None, None]]:
    """
    Queries database for vaules in sql table column.
    Returns list of values in sql table column.
    Returns a lists of values.
    Returns a generator of lists of values if chunksize is not None.
    
    Parameters
    ----------
    table: sa.Table
        SqlAlchemy table mapped to sql table.
    column_name: str
        name of sql table column.
    connection: sa.engine.Engine, sa.orm.Session, or sa.engine.Connection
        connection used to query database.
    chunksize: int, default None
        if not None, returns generator of lists of values.
    
    Returns
    -------
    list of sql table column values or generator of lists of values.
    """
    if isinstance(table, str):
        table = get_table(table, connection)
    if chunksize is None:
        return select_column_values_all(table, column_name, connection)
    else:
        return select_column_values_chunks(table, column_name, connection, chunksize)


def select_column_values_all(
    table: Union[sa.Table, str],
    column_name: str,
    connection: SqlConnection,
) -> list:
    """
    Queries database for vaules in sql table column.
    Returns list of values in sql table column.
    Returns a lists of values.
    
    Parameters
    ----------
    table: sa.Table
        SqlAlchemy table mapped to sql table.
    column_name: str
        name of sql table column.
    connection: sa.engine.Engine, sa.orm.Session, or sa.engine.Connection
        connection used to query database.
    
    Returns
    -------
    list of sql table column values.
    """
    if isinstance(table, str):
        table = get_table(table, connection)
    query = select(table.c[column_name])
    return connection.execute(query).scalars().all()


def select_column_values_chunks(
    table: Union[sa.Table, str],
    column_name: str,
    connection: SqlConnection,
    chunksize: int
) -> Generator[list, None, None]:
    """
    Queries database for vaules in sql table column.
    Returns a generator of lists of values.
    
    Parameters
    ----------
    table: sa.Table
        SqlAlchemy table mapped to sql table.
    column_name: str
        name of sql table column.
    connection: sa.engine.Engine, sa.orm.Session, or sa.engine.Connection
        connection used to query database.
    chunksize: int, default None
        Returns generator of chunksized lists of values.
    
    Returns
    -------
    Generator of chunksized lists of sql table column values.
    """
    if isinstance(table, str):
        table = get_table(table, connection)
    query = select(table.c[column_name])
    stream = connection.execute(query, execution_options={'stream_results': True})
    for results in stream.scalars().partitions(chunksize):
        yield results