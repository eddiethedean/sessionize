from typing import Optional, Union

import sqlalchemy as sa
from sqlalchemy.orm.decl_api import DeclarativeMeta

from sessionize.utils.custom_types import Record
from sessionize.utils.sa_orm import get_class, get_table


def update_records_session(
    table: Union[sa.Table, str],
    records: list[Record],
    session: sa.orm.session.Session,
    schema: Optional[str] = None,
    table_class: Optional[DeclarativeMeta] = None
) -> None:
    """
    Update sql table records from list records.
    Only adds sql records updates to session, does not commit session.
    Sql table must have primary key.
    Do not pass any records that do not already have primary key matches in table.
    
    Parameters
    ----------
    table: sa.Table
        SqlAlchemy table mapped to sql table.
        Use sessionize.engine_utils.get_table to get table.
    records: list[Record]
        list of records to update.
        Use df.to_dict('records') to convert Pandas DataFrame to records.
    session: sa.orm.session.Session
        SqlAlchemy session to add sql updates to.
    schema: str, default None
        Database schema name.
    table_class: DeclarativeMeta, default None
        pass in the table class if you already have it
        otherwise, will query sql for it each time.

    Returns
    -------
    None
    """
    engine = session.get_bind()
    if isinstance(table, str):
        table = get_table(table, engine)
    table_name = table.name
    if table_class is None:
        table_class = get_class(table_name, engine, schema=schema)
    mapper = sa.inspect(table_class)
    session.bulk_update_mappings(mapper, records)