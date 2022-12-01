from typing import Any, Dict, Sequence, Union, Optional

from sessionize.utils.features import _get_table
import sessionize.utils.types as types

# TODO: replace with interfaces
from sqlalchemy import Table
from sqlalchemy.engine import Engine
from sqlalchemy.orm.session import Session
import sqlalchemize.delete as delete


def delete_records_session(
    sa_table: Union[Table, str],
    col_name: str,
    values: Sequence,
    session: Session,
    schema: Optional[str] = None
) -> None:
    """
    Given a SqlAlchemy Table, name of column to compare,
    list of values to match, and SqlAlchemy session object,
    deletes sql records where column values match given values.
    Only adds sql records deletions to session, does not commit session.
    
    Parameters
    ----------
    sa_table: sa.Table
        SqlAlchemy table mapped to sql table.
        Use sessionize.engine_utils.get_table to get table.
    col_name: str
        name of sql table column to compare to values.
    values: list
        list of values to match with column values.
    session: sa.orm.session.Session
        SqlAlchemy session to add sql deletes to.
    
    Returns
    -------
    None
    """
    table = _get_table(sa_table, session, schema=schema)
    delete.delete_records_session(table, col_name, values, session)


def delete_record_by_values_session(
    sa_table: Union[Table, str],
    record: types.Record,
    session: Session,
    schema: Optional[str] = None
) -> None:
    # Delete any records that match the given record values.
    table = _get_table(sa_table, session, schema=schema)
    delete.delete_record_by_values_session(table, record, session)


def delete_records_by_values_session(
    sa_table: Union[Table, str],
    records: Sequence[types.Record],
    session: Session,
    schema: Optional[str] = None
) -> None:
    # Delete any records that match the given records values.
    table = _get_table(sa_table, session, schema=schema)
    delete.delete_records_by_values_session(table, records, session)


def delete_records(
    sa_table: Union[Table, str],
    col_name: str,
    values: Sequence,
    engine: Engine,
    schema: Optional[str] = None
) -> None:
    table = _get_table(sa_table, engine, schema=schema)
    delete.delete_records(table, col_name, values, engine)


def delete_all_records_session(
    sa_table: Union[Table, str],
    session: Session,
    schema: Optional[str] = None
) -> None:
    table = _get_table(sa_table, session, schema=schema)
    delete.delete_all_records_session(table, session)


def delete_all_records(
    sa_table: Union[Table, str],
    engine: Engine,
    schema: Optional[str] = None
) -> None:
    table = _get_table(sa_table, engine, schema=schema)
    delete.delete_all_records(table, engine)
