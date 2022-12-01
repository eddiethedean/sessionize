from typing import Optional

# TODO: replace with interfaces
from sqlalchemy import Table
from sqlalchemy.engine import Engine
import sqlalchemize.create as create


def create_table(
    table_name: str,
    column_names: list[str],
    column_types: list[type],
    primary_key: str,
    engine: Engine,
    schema: Optional[str] = None,
    autoincrement: Optional[bool] = True,
    if_exists: Optional[str] = 'error'
) -> Table:
    return create.create_table(table_name, column_names, column_types, primary_key, engine, schema, autoincrement, if_exists)