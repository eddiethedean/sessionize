from typing import Optional
import decimal
import datetime

import sqlalchemy as sa
from sqlalchemy.schema import DropTable, CreateTable
from sqlalchemy.engine import Engine


_type_convert = {
    int: sa.sql.sqltypes.Integer,
    str: sa.sql.sqltypes.Unicode,
    float: sa.sql.sqltypes.Float,
    decimal.Decimal: sa.sql.sqltypes.Numeric,
    datetime.datetime: sa.sql.sqltypes.DateTime,
    bytes: sa.sql.sqltypes.LargeBinary,
    bool: sa.sql.sqltypes.Boolean,
    datetime.date: sa.sql.sqltypes.Date,
    datetime.time: sa.sql.sqltypes.Time,
    datetime.timedelta: sa.sql.sqltypes.Interval,
    list: sa.sql.sqltypes.ARRAY,
    dict: sa.sql.sqltypes.JSON
}


def create_table(
    table_name: str,
    column_names: list[str],
    column_types: list[type],
    primary_key: str,
    engine: Engine,
    schema: Optional[str] = None,
    autoincrement: Optional[bool] = True,
    if_exists: Optional[str] = 'replace'
) -> None:
    
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
        drop_table_sql = DropTable(table, if_exists=True)
        engine.execute(drop_table_sql)
    table_creation_sql = CreateTable(table)
    engine.execute(table_creation_sql)