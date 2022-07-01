"""
All SqlAlchemy functionality used in Sessionize is defined here.
"""

from typing import Optional, Any, Generator

import sqlalchemy as sa
import sqlalchemy.schema as sa_schema
import sqlalchemy.ext.automap as sa_automap
import sqlalchemy.engine as sa_engine
import sqlalchemy.sql.elements as sa_elements
import sqlalchemy.orm.session as sa_session

from sessionize.exceptions import MissingPrimaryKey, SliceError
from sessionize.sa.type_convert import _type_convert


Record = dict[str, Any]
SqlConnection = sa_engine.Engine | sa_session.Session | sa_engine.Connection


def primary_key_columns(
    sa_table: sa.Table
) -> list[sa.Column]:
    return list(sa_table.primary_key.columns)


def primary_key_names(
    sa_table: sa.Table
) -> list[str]:
    return [c.name for c in primary_key_columns(sa_table)]


def get_connection(
    connection: SqlConnection | sa_session.Session
) -> SqlConnection:
    if isinstance(connection, sa_session.Session):
        return connection.connection()
    return connection


def get_metadata(
    connection,
    schema: Optional[str] = None
) -> sa.MetaData:
    return sa.MetaData(bind=connection, schema=schema)


def get_table(
    name: str,
    connection: SqlConnection,
    schema: Optional[str] = None
) -> sa.Table:
    metadata = get_metadata(connection, schema)
    autoload_with = get_connection(connection)

    return sa.Table(name,
                 metadata,
                 autoload=True,
                 autoload_with=autoload_with,
                 extend_existing=True,
                 schema=schema)


def get_class(
    name: str,
    connection: SqlConnection | sa_session.Session,
    schema: Optional[str] = None
):
    metadata = get_metadata(connection, schema)
    connection = get_connection(connection)

    metadata.reflect(connection, only=[name], schema=schema)
    Base = sa_automap.automap_base(metadata=metadata)
    Base.prepare()
    return Base.classes[name]


def get_column(
    sa_table: sa.Table,
    column_name: str
) -> sa.Column:
    return sa_table.c[column_name]


def get_table_constraints(sa_table: sa.Table):
    return sa_table.constraints


def get_primary_key_constraints(
    sa_table: sa.Table
) -> tuple[str, list[str]] | None:
    cons = get_table_constraints(sa_table)
    for con in cons:
        if isinstance(con, sa.PrimaryKeyConstraint):
            return con.name, [col.name for col in con.columns]


def get_column_types(sa_table: sa.Table) -> dict:
    return {c.name: c.type for c in sa_table.c}


def get_column_names(sa_table: sa.Table) -> list[str]:
    return [c.name for c in sa_table.columns]


def get_table_names(
    engine: sa_engine.Engine,
    schema: Optional[str] = None
) -> list[str]:
    return sa.inspect(engine).get_table_names(schema)


def get_row_count(
    sa_table: sa.Table,
    session: sa_session.Session
) -> int:
    col_name = get_column_names(sa_table)[0]
    col = get_column(sa_table, col_name)
    return session.execute(sa.func.count(col)).scalar()


def get_schemas(engine: sa_engine.Engine) -> list[str]:
    insp = sa.inspect(engine)
    return insp.get_schema_names()


def get_where_clause(
    sa_table: sa.Table,
    record: Record
) -> list[bool]:
    return [sa_table.c[key_name]==key_value for key_name, key_value in record.items()]


def delete_records_session(
    sa_table: sa.Table,
    col_name: str,
    values: list,
    session: sa_session.Session
) -> None:
    col = get_column(sa_table, col_name)
    session.query(sa_table).filter(col.in_(values)).delete(synchronize_session=False)


def delete_records(
    sa_table: sa.Table,
    col_name: str,
    values: list,
    engine: sa_engine.Engine
) -> None:
    try:
        session = sa_session.Session(engine)
        delete_records_session(sa_table, col_name, values, session)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e


def delete_all_records_session(
    table: sa.Table,
    session: sa_session.Session
) -> None:
    session.query(table).delete()


def delete_all_records(
    sa_table: sa.Table,
    engine: sa_engine.Engine
) -> None:
    try:
        session = sa_session.Session(engine)
        delete_all_records_session(sa_table, session)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e


def insert_from_table_session(
    sa_table1: sa.Table,
    sa_table2: sa.Table,
    session: sa_session.Session
) -> None:
    session.execute(sa_table2.insert().from_select(sa_table1.columns.keys(), sa_table1))


def insert_records_session(
    sa_table: sa.Table,
    records: list[Record],
    session: sa_session.Session
) -> None:
    table_class = get_class(sa_table.name, session, schema=sa_table.schema)
    mapper = sa.inspect(table_class)
    session.bulk_insert_mappings(mapper, records)


def insert_records(
    sa_table: sa.Table,
    records: list[Record],
    engine: sa_engine.Engine
) -> None:
    try:
        session = sa_session.Session(engine)
        insert_records_session(sa_table, records, session)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e


def select_records_all(
    sa_table: sa.Table,
    connection: SqlConnection,
    sorted: bool = False,
    include_columns: Optional[list[str]] = None
) -> list[Record]:
    if include_columns is not None:
        columns = [get_column(sa_table, column_name) for column_name in include_columns]
        query = sa.select(*columns)
    else:
        query = sa.select(sa_table)

    if sorted:
        query = query.order_by(*primary_key_columns(sa_table))
    results = connection.execute(query)
    return [dict(r) for r in results]


def select_records_chunks(
    sa_table: sa.Table,
    connection: SqlConnection,
    chunksize: int = 2,
    sorted: bool = False,
    include_columns: Optional[list[str]] = None
) -> Generator[list[Record], None, None]:
    if include_columns is not None:
        columns = [get_column(sa_table, column_name) for column_name in include_columns]
        query = sa.select(*columns)
    else:
        query = sa.select(sa_table)

    if sorted:
        query = query.order_by(*primary_key_columns(sa_table))
    stream = connection.execute(query, execution_options={'stream_results': True})
    for results in stream.partitions(chunksize):
        yield [dict(r) for r in results]


def select_existing_values(
    sa_table: sa.Table,
    connection: SqlConnection,
    column_name: str,
    values: list,
) -> list:
    column = get_column(sa_table, column_name)
    query = sa.select([column]).where(column.in_(values))
    return connection.execute(query).scalars().fetchall()


def select_column_values_all(
    sa_table: sa.Table,
    connection: SqlConnection,
    column_name: str
) -> list:
    query = sa.select(get_column(sa_table, column_name))
    return connection.execute(query).scalars().all()


def select_column_values_chunks(
    sa_table: sa.Table,
    connection: SqlConnection,
    column_name: str,
    chunksize: int
) -> Generator[list, None, None]:
    query = sa.select(get_column(sa_table, column_name))
    stream = connection.execute(query, execution_options={'stream_results': True})
    for results in stream.scalars().partitions(chunksize):
        yield results


def _convert_slice_indexes(
    sa_table: sa.Table,
    connection: SqlConnection,
    start: Optional[int] = None,
    stop: Optional[int] = None
) -> tuple[int, int]:
    # start index is 0 if None
    start = 0 if start is None else start
    row_count = get_row_count(sa_table, connection)
    
    # stop index is row count if None
    stop = row_count if stop is None else stop
    # convert negative indexes
    start = _calc_positive_index(start, row_count)
    start = _stop_underflow_index(start, row_count)
    stop = _calc_positive_index(stop, row_count)
    stop = _stop_overflow_index(stop, row_count)

    if row_count == 0:
        return 0, 0

    return start, stop


def select_records_slice(
    sa_table: sa.Table,
    connection: SqlConnection,
    start: Optional[int] = None,
    stop: Optional[int] = None,
    sorted: bool = False,
    include_columns: Optional[list[str]] = None
) -> list[Record]:
    start, stop = _convert_slice_indexes(sa_table, connection, start, stop)
    if stop < start:
        raise SliceError('stop cannot be less than start.')
    if include_columns is not None:
        columns = [get_column(sa_table, column_name) for column_name in include_columns]
        query = sa.select(*columns)
    else:
        query = sa.select(sa_table)
    if sorted:
        query = query.order_by(*primary_key_columns(sa_table))
    query = query.slice(start, stop)
    results = connection.execute(query)
    return [dict(r) for r in results]


def select_column_values_by_slice(
    sa_table: sa.Table,
    connection: SqlConnection,
    column_name: str,
    start: Optional[int] = None,
    stop: Optional[int] = None
) -> list:
    start, stop = _convert_slice_indexes(sa_table, connection, start, stop)
    if stop < start:
        raise SliceError('stop cannot be less than start.')
    query = sa.select(get_column(sa_table, column_name)).slice(start, stop)
    return connection.execute(query).scalars().all()


def select_column_value_by_index(
    sa_table: sa.Table,
    connection: SqlConnection,
    column_name: str,
    index: int
) -> Any:
    if index < 0:
        row_count = get_row_count(sa_table, connection)
        if index < -row_count:
            raise IndexError('Index out of range.') 
        index = _calc_positive_index(index, row_count)
    query = sa.select(get_column(sa_table, column_name)).slice(index, index+1)
    return connection.execute(query).scalars().all()[0]


def select_primary_key_records_by_slice(
    sa_table: sa.Table,
    connection: SqlConnection,
    _slice: slice,
    sorted: bool = False
) -> list[Record]:
    start = _slice.start
    stop = _slice.stop
    start, stop = _convert_slice_indexes(sa_table, connection, start, stop)
    if stop < start:
        raise SliceError('stop cannot be less than start.')
    primary_key_values = primary_key_columns(sa_table)
    if sorted:
        query = sa.select(primary_key_values).order_by(*primary_key_values).slice(start, stop)
    else:
        query = sa.select(primary_key_values).slice(start, stop)
    results = connection.execute(query)
    return [dict(r) for r in results]


def select_record_by_primary_key(
    sa_table: sa.Table,
    connection: SqlConnection,
    primary_key_value: Record,
    include_columns: Optional[list[str]] = None
) -> Record:
    # TODO: check if primary key values exist
    where_clause = get_where_clause(sa_table, primary_key_value)
    if len(where_clause) == 0:
        return []
    if include_columns is not None:
        columns = [get_column(sa_table, column_name) for column_name in include_columns]
        query = sa.select(*columns).where((sa_elements.and_(*where_clause)))
    else:
        query = sa.select(sa_table).where((sa_elements.and_(*where_clause)))
    results = connection.execute(query)
    results = [dict(x) for x in results]
    if len(results) == 0:
        raise MissingPrimaryKey('Primary key values missing in table.')
    return results[0]


def select_records_by_primary_keys(
    sa_table: sa.Table,
    connection: SqlConnection,
    primary_keys_values: list[Record],
    schema: Optional[str] = None,
    include_columns: Optional[list[str]] = None
) -> list[Record]:
    # TODO: check if primary key values exist
    where_clauses = []
    for record in primary_keys_values:
        where_clause = get_where_clause(sa_table, record)
        where_clauses.append(sa_elements.and_(*where_clause))
    if len(where_clauses) == 0:
        return []
    if include_columns is not None:
        columns = [get_column(sa_table, column_name) for column_name in include_columns]
        query = sa.select(*columns).where((sa_elements.or_(*where_clauses)))
    else:
        query = sa.select(sa_table).where((sa_elements.or_(*where_clauses)))
    results = connection.execute(query)
    return [dict(r) for r in results]


def select_column_values_by_primary_keys(
    sa_table: sa.Table,
    connection: SqlConnection,
    column_name: str,
    primary_keys_values: list[Record]
) -> list:
    # TODO: check if primary key values exist
    where_clauses = []
    for record in primary_keys_values:
        where_clause = get_where_clause(sa_table, record)
        where_clauses.append(sa_elements.and_(*where_clause))

    if len(where_clauses) == 0:
        return []
    query = sa.select(get_column(sa_table, column_name)).where((sa_elements.or_(*where_clauses)))
    results = connection.execute(query)
    return results.scalars().fetchall()


def select_value_by_primary_keys(
    sa_table: sa.Table,
    connection: SqlConnection,
    column_name: str,
    primary_key_value: Record,
    schema: Optional[str] = None
) -> Any:
    # TODO: check if primary key values exist
    where_clause = get_where_clause(sa_table, primary_key_value)
    if len(where_clause) == 0:
        raise KeyError('No such primary key values exist in table.')
    query = sa.select(get_column(sa_table, column_name)).where((sa_elements.and_(*where_clause)))
    return connection.execute(query).scalars().all()[0]


def update_records_session(
    sa_table: sa.Table,
    records: list[Record],
    session: sa_session.Session
) -> None:
    table_name = sa_table.name
    table_class = get_class(table_name, session, schema=sa_table.schema)
    mapper = sa.inspect(table_class)
    session.bulk_update_mappings(mapper, records)


def update_records(
    sa_table: sa.Table,
    records: list[Record],
    engine: sa_engine.Engine
) -> None:
    try:
        session = sa_session.Session(engine)
        update_records_session(sa_table, records, session)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e


def create_table(
    table_name: str,
    column_names: list[str],
    column_types: list[type],
    primary_key: str,
    engine: sa_engine.Engine,
    schema: Optional[str] = None,
    autoincrement: Optional[bool] = True,
    if_exists: Optional[str] = 'error'
) -> sa.Table:
    
    cols = []
    
    for name, python_type in zip(column_names, column_types):
        sa_type = _type_convert[python_type]
        if name == primary_key:
            col = sa.Column(name, sa_type,
                            primary_key=True,
                            autoincrement=autoincrement)
        else:
            col = sa.Column(name, sa_type)
        cols.append(col)

    metadata = sa.MetaData(engine)
    table = sa.Table(table_name, metadata, *cols, schema=schema)
    if if_exists == 'replace':
        drop_table_sql = sa_schema.DropTable(table, if_exists=True)
        engine.execute(drop_table_sql)
    table_creation_sql = sa_schema.CreateTable(table)
    engine.execute(table_creation_sql)
    return get_table(table_name, engine, schema=schema)


def drop_table(
    table: sa.Table | str,
    engine: sa_engine.Engine,
    if_exists: Optional[bool] = True,
    schema: Optional[str] = None
) -> None:
    if isinstance(table, str):
        if table not in sa.inspect(engine).get_table_names(schema=schema):
            if if_exists:
                return
        table = get_table(table, engine, schema=schema)
    sql = sa_schema.DropTable(table, if_exists=if_exists)
    engine.execute(sql)


def _calc_positive_index(
    index: int,
    row_count: int
) -> int:
    # convert negative index to real index
    if index < 0:
        index = row_count + index
    return index


def _stop_overflow_index(
    index: int,
    row_count: int
) -> int:
    if index > row_count - 1:
        return row_count
    return index

    
def _stop_underflow_index(
    index: int,
    row_count: int
) -> int:
    if index < 0 and index < -row_count:
        return 0
    return index