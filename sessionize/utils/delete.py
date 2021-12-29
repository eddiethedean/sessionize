from typing import Union

import sqlalchemy as sa
from sqlalchemy.orm import Session

from sessionize.utils.sa_orm import _get_table


def delete_records_session(
    table: Union[sa.Table, str],
    col_name: str,
    values: list,
    session: Session
) -> None:
    """
    Given a SqlAlchemy Table, name of column to compare,
    list of values to match, and SqlAlchemy session object,
    deletes sql records where column values match given values.
    Only adds sql records deletions to session, does not commit session.
    
    Parameters
    ----------
    table: sa.Table
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
    table = _get_table(table, session)
    col = table.c[col_name]
    session.query(table).filter(col.in_(values)).delete(synchronize_session=False)


def delete_all_records_session(
    table: Union[sa.Table, str],
    session: Session
) -> None:
    table = _get_table(table, session)
    session.query(table).delete()
